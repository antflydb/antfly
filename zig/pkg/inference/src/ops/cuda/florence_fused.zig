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
    depthwise: driver.CUfunction = null,
    depthwise_calls: usize = 0,
    append_kv: driver.CUfunction = null,
    append_kv_calls: usize = 0,
    layer_norm: driver.CUfunction = null,
    gelu: driver.CUfunction = null,
    cross_q1: driver.CUfunction = null,
    cross_calls: usize = 0,
    window_norm: driver.CUfunction = null,
    split_qkv: driver.CUfunction = null,
    attention_input_calls: usize = 0,
    split_calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module != null) return;
        var module: driver.CUmodule = null;
        const image = @embedFile("florence_fused.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var function: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "florence_token_depthwise_residual"));
        var norm: driver.CUfunction = null;
        var gelu: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&norm, module, "florence_layer_norm_f16"));
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&gelu, module, "florence_exact_gelu_f16"));
        var cross: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&cross, module, "florence_cross_q1_f32"));
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&self.window_norm, module, "florence_window_norm_f16"));
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&self.split_qkv, module, "florence_split_qkv_f16"));
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&self.append_kv, module, "florence_append_self_kv_f32"));
        self.cross_q1 = cross;
        self.layer_norm = norm;
        self.gelu = gelu;
        self.module = module;
        self.depthwise = function;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }

    pub fn runNorm(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, rows: u32, dim: u32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var eps: f32 = 1e-5;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &r, &d, &eps };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.layer_norm, rows, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn runWindowNorm(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, rows: u32, dim: u32, height: u32, width: u32, window: u32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var eps: f32 = 1e-5;
        var h = height;
        var w = width;
        var ws = window;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &r, &d, &eps, &h, &w, &ws };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.window_norm, rows, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn runSplit(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, count: u32, dim: u32) !void {
        var p = pointers;
        var n = count;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &n, &d };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.split_qkv, (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.split_calls += 1;
    }

    pub fn runGelu(self: *Module, ctx: *context.CudaContext, output: driver.CUdeviceptr, input: driver.CUdeviceptr, count: u32) !void {
        var o = output;
        var i = input;
        var n = count;
        var args = [_]?*anyopaque{ &o, &i, &n };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.gelu, (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn runCross(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, batch: u32, sequence: u32) !void {
        var p = pointers;
        var b = batch;
        var s = sequence;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &b, &s };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.cross_q1, batch * 12, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.cross_calls += 1;
    }

    pub fn runAppendKv(self: *Module, ctx: *context.CudaContext, pointers: [6]driver.CUdeviceptr, count: u32, capacity: u32, old_len: u32, dim: u32) !void {
        var p = pointers;
        var n = count;
        var c = capacity;
        var old = old_len;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &p[5], &n, &c, &old, &d };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.append_kv, (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.append_kv_calls += 1;
    }

    pub fn runDepthwise(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, count: u32, height: u32, width: u32, channels: u32) !void {
        var p = pointers;
        var n = count;
        var h = height;
        var w = width;
        var c = channels;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &n, &h, &w, &c };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.depthwise, (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.depthwise_calls += 1;
    }
};
