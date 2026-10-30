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

// Native export names remain the compatibility ABI of the qualified PTX.
const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("florence_fused.ptx") ++ "\x00", .{
    .token_depthwise_residual = "antfly_depthwise_3x3_residual_f32",
    .layer_norm_f16 = "antfly_layer_norm_warp_finish_f16",
    .exact_gelu_f16 = "florence_exact_gelu_f16",
    .cross_q1_h12_d64_f32 = "florence_cross_q1_f32",
    .window_norm_f16 = "antfly_window_layer_norm_warp_finish_f16",
    .split_qkv_f16 = "florence_split_qkv_f16",
    .append_self_kv_f32 = "florence_append_self_kv_f32",
});

pub const Module = struct {
    program: Program = .{},
    depthwise_calls: usize = 0,
    append_kv_calls: usize = 0,
    cross_calls: usize = 0,
    attention_input_calls: usize = 0,
    split_calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.program.deinit(ctx);
        self.* = .{};
    }

    pub fn runNorm(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, rows: u32, dim: u32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var eps: f32 = 1e-5;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &r, &d, &eps };
        try self.program.launch(ctx, .layer_norm_f16, rows, 1, 256, &args);
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
        try self.program.launch(ctx, .window_norm_f16, rows, 1, 256, &args);
    }

    pub fn runSplit(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, count: u32, dim: u32) !void {
        var p = pointers;
        var n = count;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &n, &d };
        try self.program.launch(ctx, .split_qkv_f16, (count + 255) / 256, 1, 256, &args);
        self.split_calls += 1;
    }

    pub fn runGelu(self: *Module, ctx: *context.CudaContext, output: driver.CUdeviceptr, input: driver.CUdeviceptr, count: u32) !void {
        var o = output;
        var i = input;
        var n = count;
        var args = [_]?*anyopaque{ &o, &i, &n };
        try self.program.launch(ctx, .exact_gelu_f16, (count + 255) / 256, 1, 256, &args);
    }

    pub fn runCross(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, batch: u32, sequence: u32) !void {
        var p = pointers;
        var b = batch;
        var s = sequence;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &b, &s };
        try self.program.launch(ctx, .cross_q1_h12_d64_f32, batch * 12, 1, 256, &args);
        self.cross_calls += 1;
    }

    pub fn runAppendKv(self: *Module, ctx: *context.CudaContext, pointers: [6]driver.CUdeviceptr, count: u32, capacity: u32, old_len: u32, dim: u32) !void {
        var p = pointers;
        var n = count;
        var c = capacity;
        var old = old_len;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &p[5], &n, &c, &old, &d };
        try self.program.launch(ctx, .append_self_kv_f32, (count + 255) / 256, 1, 256, &args);
        self.append_kv_calls += 1;
    }

    pub fn runDepthwise(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, count: u32, height: u32, width: u32, channels: u32) !void {
        var p = pointers;
        var n = count;
        var h = height;
        var w = width;
        var c = channels;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &n, &h, &w, &c };
        try self.program.launch(ctx, .token_depthwise_residual, (count + 255) / 256, 1, 256, &args);
        self.depthwise_calls += 1;
    }
};
