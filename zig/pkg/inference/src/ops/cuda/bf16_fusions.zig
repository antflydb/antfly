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
const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("lighton_activation.ptx") ++ "\x00", .{
    .silu_product_f32 = "lighton_silu_up_bf16",
    .packed_silu_product_f32 = "lighton_packed_silu_up_bf16",
    .rms_f32 = "lighton_rms_input_bf16",
    .silu_product_bf16 = "lighton_silu_up_bf16_inputs",
    .residual_f32 = "lighton_bf16_residual_f32",
});

pub const Module = struct {
    program: Program = .{},
    calls: usize = 0,
    separate_calls: usize = 0,
    norm_calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.program.deinit(ctx);
        self.* = .{};
    }
    pub fn runSeparate(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        var p = pointers;
        var n = count;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n };
        try self.program.launch(ctx, .silu_product_f32, (count + 255) / 256, 1, 256, &args);
        self.calls += 1;
        self.separate_calls += 1;
    }
    pub fn runSeparateBf16Inputs(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        try self.runThreePointers(ctx, .silu_product_bf16, pointers, count);
        self.calls += 1;
        self.separate_calls += 1;
    }
    pub fn runBf16Residual(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        try self.runThreePointers(ctx, .residual_f32, pointers, count);
    }
    fn runThreePointers(self: *Module, ctx: *context.CudaContext, entry: Program.Entry, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        var p = pointers;
        var n = count;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n };
        try self.program.launch(ctx, entry, (count + 255) / 256, 1, 256, &args);
    }
    pub fn runJoined(self: *Module, ctx: *context.CudaContext, pointers: [2]driver.CUdeviceptr, rows: u32, dim: u32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &r, &d };
        try self.program.launch(ctx, .packed_silu_product_f32, (rows * dim + 255) / 256, 1, 256, &args);
        self.calls += 1;
    }
    pub fn runRms(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, rows: u32, dim: u32, eps: f32, offset: f32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var e = eps;
        var o = offset;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &r, &d, &e, &o };
        try self.program.launch(ctx, .rms_f32, rows, 1, 256, &args);
        self.norm_calls += 1;
    }
};
