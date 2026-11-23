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

//! Four-tap BF16 causal stencil with caller-owned committed and staged state.
const std = @import("std");
const buffer = @import("buffer.zig");
const Context = @import("context.zig").CudaContext;
const embedded = @import("embedded_module.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const Program = embedded.EmbeddedModule(embedded.profile(catalog.causal_depthwise4_bf16).image, catalog.causal_depthwise4_bf16_entries);

pub const Request = struct {
    input: DeviceBuffer,
    weight: DeviceBuffer,
    history: DeviceBuffer,
    dynamic_taps: DeviceBuffer = .{},
    output: DeviceBuffer,
    next_history: DeviceBuffer,
    rows: u32,
    channels: u32,
    head_size: u32,
    valid_history: u32,
    output_scale: f32,
};

fn stencilAddresses(ctx: *const Context, r: Request) ![6]u64 {
    if (r.rows == 0 or r.rows > 65535 or r.channels == 0 or r.channels > (1 << 20) or
        r.head_size == 0 or r.channels % r.head_size != 0 or r.valid_history > 3 or
        !std.math.isFinite(r.output_scale) or r.output_scale <= 0)
        return error.InvalidShape;
    const row_bytes = @as(usize, r.channels) * 2;
    const io_bytes = try std.math.mul(usize, r.rows, row_bytes);
    const dynamic_bytes = if (r.dynamic_taps.ptr != 0)
        try std.math.mul(usize, r.rows, @as(usize, r.channels / r.head_size) * 8)
    else
        0;
    const history_bytes = 3 * row_bytes;
    try buffer.validateRegions(6, .{
        r.input, r.weight, r.history, r.dynamic_taps, r.output, r.next_history,
    }, .{
        io_bytes, 4 * row_bytes, if (r.valid_history != 0) history_bytes else 0, dynamic_bytes, io_bytes, history_bytes,
    }, @splat(2), 4);
    if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
    return .{ r.output.ptr, r.next_history.ptr, r.input.ptr, r.history.ptr, r.weight.ptr, r.dynamic_taps.ptr };
}

pub const Module = struct {
    program: Program = .{},

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.program.deinit(ctx);
    }

    pub fn run(self: *Module, ctx: *Context, r: Request) !void {
        var addresses = try stencilAddresses(ctx, r);
        try self.program.init(ctx);
        var channels = r.channels;
        var head_size = r.head_size;
        var rows = r.rows;
        var valid_history = r.valid_history;
        var scale = r.output_scale;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &addresses[4], &addresses[5], &channels, &head_size, &rows, &valid_history, &scale };
        const threads = 128;
        try self.program.launch(ctx, .run, (r.channels - 1) / threads + 1, r.rows, threads, &args);
    }

    /// Apply L2 clipping or RMS normalization after rounding each stencil output to BF16.
    pub fn runHeadNorm(self: *Module, ctx: *Context, r: Request, clip: f32, eps: f32) !void {
        const dim = 128;
        if (r.head_size != dim or !std.math.isFinite(clip) or clip < 0 or
            !std.math.isFinite(eps) or eps <= 0)
            return error.InvalidShape;
        var addresses = try stencilAddresses(ctx, r);
        try self.program.init(ctx);
        var channels = r.channels;
        var rows = r.rows;
        var valid_history = r.valid_history;
        var scale = r.output_scale;
        var clip_threshold = clip;
        var epsilon = eps;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &addresses[3], &addresses[4], &addresses[5], &channels, &rows, &valid_history, &scale, &clip_threshold, &epsilon };
        try self.program.launch(ctx, .head_norm_d128, r.channels / dim, r.rows, dim, &args);
    }
};
