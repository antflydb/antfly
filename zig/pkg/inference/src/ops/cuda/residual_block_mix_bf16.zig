// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const buffer = @import("buffer.zig");
const Context = @import("context.zig").CudaContext;
const embedded = @import("embedded_module.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const Program = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(catalog.residual_block_mix).image;
}, catalog.residual_block_mix_entries);
const block_dim = 128;

pub const Shape = struct {
    rows: u32 = 1,
    width: u32,

    fn values(self: Shape) !u32 {
        if (self.rows == 0 or self.width == 0) return error.InvalidShape;
        return std.math.mul(u32, self.rows, self.width);
    }
};

pub const RowGroup = struct {
    width: u32,
    broadcast_destination: bool,

    fn stride(self: RowGroup) u32 {
        return if (self.broadcast_destination) 0 else self.width;
    }
};

pub const Module = struct {
    program: Program = .{},

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.program.deinit(ctx);
    }

    pub fn mix(self: *Module, ctx: *Context, shape: Shape, blocks: DeviceBuffer, last: DeviceBuffer, addend: DeviceBuffer, sources: u32, query: DeviceBuffer, output: DeviceBuffer) !void {
        const count: usize = try shape.values();
        if (sources == 0 or sources > 8) return error.InvalidShape;
        try buffer.validateRegions(5, .{ blocks, last, addend, query, output }, .{
            (sources - 1) * count * 2, count * 2, if (addend.ptr == 0) 0 else count * 2, @as(usize, shape.width) * 4, count * 4,
        }, .{ 2, 2, 2, 4, 4 }, 4);
        try self.program.init(ctx);
        var addresses = [5]u64{ output.ptr, blocks.ptr, last.ptr, addend.ptr, query.ptr };
        var rows = shape.rows;
        var width = shape.width;
        var source_count = sources;
        var eps: f32 = 1e-6;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &addresses[4], &rows, &width, &source_count, &eps };
        try self.program.launch(ctx, .mix, rows, 1, block_dim, &args);
    }

    pub fn mixAndNorm(self: *Module, ctx: *Context, shape: Shape, blocks: DeviceBuffer, last: DeviceBuffer, addend: DeviceBuffer, sources: u32, query: DeviceBuffer, gamma: DeviceBuffer, epsilon: f32, output: DeviceBuffer) !void {
        const count: usize = try shape.values();
        if (sources == 0 or sources > 8 or shape.width > 4096 or !std.math.isFinite(epsilon) or epsilon <= 0) return error.InvalidShape;
        try buffer.validateRegions(6, .{ blocks, last, addend, query, gamma, output }, .{
            (sources - 1) * count * 2,   count * 2,                   if (addend.ptr == 0) 0 else count * 2,
            @as(usize, shape.width) * 4, @as(usize, shape.width) * 2, count * 2,
        }, .{ 2, 2, 2, 4, 2, 2 }, 5);
        try self.program.init(ctx);
        var ptrs = [_]u64{ output.ptr, blocks.ptr, last.ptr, addend.ptr, query.ptr, gamma.ptr };
        var rows = shape.rows;
        var width = shape.width;
        var source_count = sources;
        var mix_epsilon: f32 = 1e-6;
        var norm_epsilon = epsilon;
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &ptrs[4], &ptrs[5], &rows, &width, &source_count, &mix_epsilon, &norm_epsilon };
        try self.program.launch(ctx, .mix_norm, rows, 1, block_dim, &args);
    }

    pub fn mixPair(self: *Module, ctx: *Context, shape: Shape, first: DeviceBuffer, second: DeviceBuffer, query: DeviceBuffer, destination: DeviceBuffer, message: DeviceBuffer, pair_output: DeviceBuffer) !void {
        const count: usize = try shape.values();
        try buffer.validateRegions(6, .{ first, second, query, destination, message, pair_output }, .{
            count * 2, count * 2, @as(usize, shape.width) * 4, count * 4, count * 4, count * 4,
        }, .{ 2, 2, 4, 4, 4, 2 }, 4);
        try self.program.init(ctx);
        var ptrs = [_]u64{ pair_output.ptr, message.ptr, destination.ptr, first.ptr, second.ptr, query.ptr };
        var rows = shape.rows;
        var width = shape.width;
        var epsilon: f32 = 1e-6;
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &ptrs[4], &ptrs[5], &rows, &width, &epsilon };
        try self.program.launch(ctx, .mix_pair, rows, 1, block_dim, &args);
    }

    pub fn mixThreePairs(self: *Module, ctx: *Context, group: RowGroup, first: DeviceBuffer, second: DeviceBuffer, queries: [3]DeviceBuffer, destination: DeviceBuffer, messages: DeviceBuffer, pairs: DeviceBuffer) !void {
        if (group.width == 0) return error.InvalidShape;
        const row_values: usize = group.width;
        try buffer.validateRegions(8, .{ first, second, queries[0], queries[1], queries[2], destination, messages, pairs }, .{
            row_values * 2,                                    row_values * 2,  row_values * 4,  row_values * 4, row_values * 4,
            (@as(usize, group.stride()) * 2 + row_values) * 4, row_values * 12, row_values * 12,
        }, .{ 2, 2, 4, 4, 4, 4, 4, 2 }, 6);
        try self.program.init(ctx);
        var ptrs = [_]u64{ pairs.ptr, messages.ptr, destination.ptr, first.ptr, second.ptr };
        var query_ptrs = [_]u64{ queries[0].ptr, queries[1].ptr, queries[2].ptr };
        var dest_stride = group.stride();
        var width = group.width;
        var epsilon: f32 = 1e-6;
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &ptrs[2], &dest_stride, &ptrs[3], &ptrs[4], &query_ptrs, &width, &epsilon };
        try self.program.launch(ctx, .mix_three_pairs, 3, 1, block_dim, &args);
    }

    pub fn blend(self: *Module, ctx: *Context, shape: Shape, input: DeviceBuffer, message: DeviceBuffer, gain: DeviceBuffer, forget: DeviceBuffer, gamma: DeviceBuffer, epsilon: f32, output: DeviceBuffer) !void {
        const count: usize = try shape.values();
        if (!std.math.isFinite(epsilon) or epsilon <= 0) return error.InvalidShape;
        try buffer.validateRegions(6, .{ input, message, gain, forget, gamma, output }, .{
            count * 4, count * 4, count * 4, count * 4, @as(usize, shape.width) * 2, count * 2,
        }, .{ 4, 4, 4, 4, 2, 2 }, 5);
        try self.program.init(ctx);
        var addresses = [6]u64{ output.ptr, input.ptr, message.ptr, gain.ptr, forget.ptr, gamma.ptr };
        var width = shape.width;
        var eps = epsilon;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &addresses[4], &addresses[5], &width, &eps };
        try self.program.launch(ctx, .blend, shape.rows, 1, block_dim, &args);
    }

    pub fn blendThree(self: *Module, ctx: *Context, group: RowGroup, destination: DeviceBuffer, messages: DeviceBuffer, gates: DeviceBuffer, gammas: [3]DeviceBuffer, epsilon: f32, output: DeviceBuffer) !void {
        if (group.width == 0 or !std.math.isFinite(epsilon) or epsilon <= 0) return error.InvalidShape;
        const row_values: usize = group.width;
        try buffer.validateRegions(7, .{ destination, messages, gates, gammas[0], gammas[1], gammas[2], output }, .{
            (@as(usize, group.stride()) * 2 + row_values) * 4, row_values * 12, row_values * 24,
            row_values * 2,                                    row_values * 2,  row_values * 2,
            row_values * 6,
        }, .{ 4, 4, 4, 2, 2, 2, 2 }, 6);
        try self.program.init(ctx);
        var ptrs = [_]u64{ output.ptr, destination.ptr, messages.ptr, gates.ptr };
        var gamma_ptrs = [_]u64{ gammas[0].ptr, gammas[1].ptr, gammas[2].ptr };
        var dest_stride = group.stride();
        var width = group.width;
        var eps = epsilon;
        var args = [_]?*anyopaque{ &ptrs[0], &ptrs[1], &dest_stride, &ptrs[2], &ptrs[3], &gamma_ptrs, &width, &eps };
        try self.program.launch(ctx, .blend_three, 3, 1, block_dim, &args);
    }

    pub fn norm(self: *Module, ctx: *Context, shape: Shape, input: DeviceBuffer, dtype: enum { bf16, f32 }, gamma: DeviceBuffer, epsilon: f32, output: DeviceBuffer) !void {
        const count: usize = try shape.values();
        if (!std.math.isFinite(epsilon) or epsilon <= 0) return error.InvalidShape;
        const input_size: usize = if (dtype == .f32) 4 else 2;
        if (input.ptr % input_size != 0) return error.InvalidShape;
        try buffer.validateRegions(3, .{ input, gamma, output }, .{ count * input_size, @as(usize, shape.width) * 2, count * 2 }, @splat(2), 2);
        try self.program.init(ctx);
        var addresses = [3]u64{ output.ptr, input.ptr, gamma.ptr };
        var width = shape.width;
        var input_f32: u32 = @intFromBool(dtype == .f32);
        var eps = epsilon;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &width, &input_f32, &eps };
        try self.program.launch(ctx, .norm, shape.rows, 1, block_dim, &args);
    }

    pub fn sum(self: *Module, ctx: *Context, shape: Shape, previous: DeviceBuffer, a: DeviceBuffer, b: DeviceBuffer, output: DeviceBuffer) !void {
        var count = try shape.values();
        const bytes = @as(usize, count) * 2;
        // The kernel reads each element before storing it, including when the
        // destination is the previous partial block.
        const alias_previous = previous.ptr == output.ptr;
        if (alias_previous and previous.len < bytes) return error.InvalidShape;
        try buffer.validateRegions(4, .{ previous, a, b, output }, .{ if (previous.ptr == 0 or alias_previous) 0 else bytes, bytes, bytes, bytes }, @splat(2), 3);
        try self.program.init(ctx);
        var addresses = [4]u64{ output.ptr, previous.ptr, a.ptr, b.ptr };
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &count };
        try self.program.launch(ctx, .sum, std.math.divCeil(u32, count, block_dim) catch unreachable, 1, block_dim, &args);
    }

    pub fn softcap(self: *Module, ctx: *Context, logits: DeviceBuffer, values: u32, cap: f32) !void {
        if (values == 0 or !std.math.isFinite(cap) or cap <= 0) return error.InvalidShape;
        try buffer.validateRegions(1, .{logits}, .{@as(usize, values) * 4}, .{4}, 0);
        try self.program.init(ctx);
        var dst = logits.ptr;
        var count = values;
        var limit = cap;
        var args = [_]?*anyopaque{ &dst, &count, &limit };
        try self.program.launch(ctx, .softcap, std.math.divCeil(u32, count, block_dim) catch unreachable, 1, block_dim, &args);
    }
};
