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

//! Whisper-only CUDA primitives; launches join the owning compute stream.
const context = @import("context.zig");
const driver = @import("driver.zig");

pub const Module = struct {
    module: driver.CUmodule = null,
    im2col: driver.CUfunction = null,
    stats: driver.CUfunction = null,
    cross_partial: driver.CUfunction = null,
    cross_merge: driver.CUfunction = null,
    gemv: driver.CUfunction = null,
    stats_merge: driver.CUfunction = null,
    qkv: driver.CUfunction = null,
    add_norm_sum: driver.CUfunction = null,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        if (self.module != null) return;
        try ctx.makeCurrent();
        var module: driver.CUmodule = null;
        const artifact = @import("artifact.zig");
        const bytes = if (artifact.is_sm89)
            @embedFile("whisper_kernels_sm89.cubin")
        else if (artifact.is_fatbin)
            @embedFile("whisper_kernels.fatbin")
        else
            @embedFile("whisper_kernels.ptx");
        const image = bytes ++ if (artifact.uses_jit) "\x00" else "";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var function: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "antfly_whisper_im2col"));
        var stats: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&stats, module, "antfly_whisper_stats"));
        var cross_partial: driver.CUfunction = null;
        var cross_merge: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&cross_partial, module, "antfly_whisper_cross_partial"));
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&cross_merge, module, "antfly_whisper_cross_merge"));
        var gemv: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&gemv, module, "antfly_whisper_gemv"));
        var stats_merge: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&stats_merge, module, "antfly_whisper_stats_merge"));
        var qkv: driver.CUfunction = null;
        var add_norm_sum: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&qkv, module, "antfly_whisper_qkv"));
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_norm_sum, module, "antfly_whisper_add_norm_sum"));
        self.* = .{ .qkv = qkv, .add_norm_sum = add_norm_sum, .stats_merge = stats_merge, .module = module, .im2col = function, .stats = stats, .cross_partial = cross_partial, .cross_merge = cross_merge, .gemv = gemv };
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }

    pub fn qkvF16(self: *Module, ctx: *context.CudaContext, pointers: [10]driver.CUdeviceptr, dim: u32) !void {
        var ptrs = pointers;
        var inputs = dim;
        var outputs = dim;
        var args: [12]?*anyopaque = undefined;
        for (&ptrs, 0..) |*ptr, i| args[i] = ptr;
        args[10] = &inputs;
        args[11] = &outputs;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.qkv, (dim + 7) / 8, 3, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn addNormSum(self: *Module, ctx: *context.CudaContext, pointers: [6]driver.CUdeviceptr, rows: u32, dim: u32, eps: f32) !void {
        var ptrs = pointers;
        var rs = rows;
        var d = dim;
        var e = eps;
        var args: [9]?*anyopaque = undefined;
        for (&ptrs, 0..) |*ptr, i| args[i] = ptr;
        args[6] = &rs;
        args[7] = &d;
        args[8] = &e;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.add_norm_sum, rows, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn gemvF16(self: *Module, ctx: *context.CudaContext, output: driver.CUdeviceptr, input: driver.CUdeviceptr, weight: driver.CUdeviceptr, bias: driver.CUdeviceptr, inputs: u32, outputs: u32, activation: bool) !void {
        var o = output;
        var i = input;
        var w = weight;
        var b = bias;
        var ins = inputs;
        var outs = outputs;
        var act: u32 = @intFromBool(activation);
        var args = [_]?*anyopaque{ &o, &i, &w, &b, &ins, &outs, &act };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.gemv, (outputs + 7) / 8, 1, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn crossAttention(self: *Module, ctx: *context.CudaContext, output: driver.CUdeviceptr, partial: driver.CUdeviceptr, q: driver.CUdeviceptr, k: driver.CUdeviceptr, v: driver.CUdeviceptr, mask: driver.CUdeviceptr, batch: u32, queries: u32, keys: u32, heads: u32, half_kv: bool) !void {
        var o = output;
        var p = partial;
        var qp = q;
        var kp = k;
        var vp = v;
        var mp = mask;
        var qs = queries;
        var ks = keys;
        var hs = heads;
        var splits = (keys + 127) / 128;
        var half: u32 = @intFromBool(half_kv);
        var args = [_]?*anyopaque{ &p, &qp, &kp, &vp, &mp, &qs, &ks, &hs, &splits, &half };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.cross_partial, batch * queries * heads, splits, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        var merge_args = [_]?*anyopaque{ &o, &p, &splits };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.cross_merge, batch * queries * heads, 1, 1, 64, 1, 1, 0, ctx.stream, &merge_args, null));
        ctx.noteKernelLaunch();
    }

    pub fn logitsStats(self: *Module, ctx: *context.CudaContext, output: driver.CUdeviceptr, partial: driver.CUdeviceptr, logits: driver.CUdeviceptr, suppress: driver.CUdeviceptr, params: @import("../ops.zig").WhisperLogitsParams) !void {
        var o = partial;
        var l = logits;
        var s = suppress;
        var p = params;
        var args = [_]?*anyopaque{ &o, &l, &s, &p };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.stats, (params.out_dim + 2047) / 2048, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        var dst = output;
        var blocks = (params.out_dim + 2047) / 2048;
        var merge_args = [_]?*anyopaque{ &dst, &o, &blocks };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.stats_merge, 1, 1, 1, 32, 1, 1, 0, ctx.stream, &merge_args, null));
        ctx.noteKernelLaunch();
    }

    pub fn unfold(self: *Module, ctx: *context.CudaContext, output: driver.CUdeviceptr, input: driver.CUdeviceptr, channels: u32, time: u32, kernel: u32, stride: u32, padding: u32, out_time: u32, time_major: bool, count: u32) !void {
        var o = output;
        var i = input;
        var values = [_]u32{ channels, time, kernel, stride, padding, out_time, @intFromBool(time_major), count };
        var args = [_]?*anyopaque{ &o, &i, &values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7] };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.im2col, (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
};
