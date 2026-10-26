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
const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("lighton_vision.ptx") ++ "\x00", .{
    .layer_norm_bf16 = "lighton_vision_norm_bf16",
    .bias_gelu_bf16 = "lighton_vision_bias_gelu_bf16",
    .bias_residual_f32 = "lighton_vision_bias_residual_f32",
    .split_qkv_rope_2d_bf16 = "lighton_vision_qkv_rope_bf16",
});

pub const Module = struct {
    program: Program = .{},
    calls: usize = 0,
    qkv_calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.program.deinit(ctx);
        self.* = .{};
    }
    pub fn norm(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, rows: u32, dim: u32, epsilon: f32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var eps = epsilon;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &r, &d, &eps };
        try self.program.launch(ctx, .layer_norm_bf16, rows, 1, 256, &args);
    }
    pub fn gelu(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32, dim: u32) !void {
        var p = pointers;
        var n = count;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n, &d };
        try self.program.launch(ctx, .bias_gelu_bf16, (count + 255) / 256, 1, 256, &args);
    }
    pub fn residual(self: *Module, ctx: *context.CudaContext, pointers: [4]driver.CUdeviceptr, count: u32, dim: u32) !void {
        var p = pointers;
        var n = count;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &n, &d };
        try self.program.launch(ctx, .bias_residual_f32, (count + 255) / 256, 1, 256, &args);
    }
    pub fn qkvRope(self: *Module, ctx: *context.CudaContext, pointers: [5]driver.CUdeviceptr, rows: u32, heads: u32, dim: u32, theta: f32) !void {
        var p = pointers;
        var r = rows;
        var h = heads;
        var d = dim;
        var t = theta;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &r, &h, &d, &t };
        const pairs = rows * heads * (dim / 2);
        try self.program.launch(ctx, .split_qkv_rope_2d_bf16, (pairs + 255) / 256, 1, 256, &args);
        self.qkv_calls += 1;
    }
};
