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

const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("scalar_clamp.ptx"), .{ .clamp = "scalar_clamp", .widen = "widen_16bit", .split = "split_bf16_terms", .split_rounded = "split_rounded_terms", .finish_projection = "finish_projection" });

pub const Kernel = struct {
    program: Program = .{},

    pub fn deinit(self: *Kernel, ctx: *context.CudaContext) void {
        self.program.deinit(ctx);
    }

    pub fn prepare(self: *Kernel, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
    }

    pub fn finishProjection(self: *Kernel, ctx: *context.CudaContext, output: driver.CUdeviceptr, correction: driver.CUdeviceptr, input: driver.CUdeviceptr, weights: driver.CUdeviceptr, rows: usize, in_dim: usize, out_dim: usize, bf16: bool) !void {
        const count = try std.math.mul(usize, rows, out_dim);
        if (in_dim == 0 or in_dim > std.math.maxInt(u32) or count > std.math.maxInt(u32)) return error.InvalidTensorShape;
        if (count == 0) return;
        try self.prepare(ctx);
        var dst = output;
        var addend = correction;
        var src = input;
        var w = weights;
        var n: u32 = @intCast(count);
        var inner: u32 = @intCast(in_dim);
        var cols: u32 = @intCast(out_dim);
        var format: u32 = @intFromBool(bf16);
        var args = [_]?*anyopaque{ &dst, &addend, &src, &w, &n, &inner, &cols, &format };
        try ctx.launchKernel(self.program.function(.finish_projection), @intCast((count + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null);
        ctx.noteKernelLaunch();
    }

    pub fn splitProjector(self: *Kernel, ctx: *context.CudaContext, input: driver.CUdeviceptr, outputs: [3]driver.CUdeviceptr, count: usize, bf16: bool, nearest_even: bool) !void {
        if (count > std.math.maxInt(u32)) return error.InvalidTensorShape;
        if (count == 0) return;
        try self.prepare(ctx);
        var src = input;
        var planes = outputs;
        var n: u32 = @intCast(count);
        var format: u32 = @intFromBool(bf16);
        var args = [_]?*anyopaque{ &src, &planes[0], &planes[1], &planes[2], &n, &format };
        try ctx.launchKernel(if (nearest_even) self.program.function(.split_rounded) else self.program.function(.split), @intCast((count + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null);
        ctx.noteKernelLaunch();
    }

    pub fn widen16Bit(self: *Kernel, ctx: *context.CudaContext, input: driver.CUdeviceptr, output: driver.CUdeviceptr, count: usize, bf16: bool) !void {
        if (count > std.math.maxInt(u32)) return error.InvalidTensorShape;
        if (count == 0) return;
        try self.prepare(ctx);
        var src = input;
        var dst = output;
        var n: u32 = @intCast(count);
        var bf: u32 = @intFromBool(bf16);
        var args = [_]?*anyopaque{ &src, &dst, &n, &bf };
        try ctx.launchKernel(self.program.function(.widen), @intCast((count + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null);
        ctx.noteKernelLaunch();
    }

    pub fn launch(self: *Kernel, ctx: *context.CudaContext, input: driver.CUdeviceptr, output: driver.CUdeviceptr, count: usize, lower: ?f32, upper: ?f32) !void {
        if (count > std.math.maxInt(u32)) return error.InvalidTensorShape;
        if (count == 0) return;
        try self.prepare(ctx);
        var src = input;
        var dst = output;
        var n: u32 = @intCast(count);
        var lo = lower orelse -std.math.inf(f32);
        var hi = upper orelse std.math.inf(f32);
        var flags: u32 = @as(u32, @intFromBool(lower != null)) | (@as(u32, @intFromBool(upper != null)) << 1);
        var args = [_]?*anyopaque{ &src, &dst, &n, &lo, &hi, &flags };
        try ctx.launchKernel(self.program.function(.clamp), @intCast((count + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null);
        ctx.noteKernelLaunch();
    }
};
