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
const buffer = @import("buffer.zig");
const Context = @import("context.zig").CudaContext;
const embedded = @import("embedded_module.zig");
const catalog = @import("../../tile/cuda_standalone.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const Program = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(catalog.head_norm_bf16).image;
}, catalog.head_norm_bf16_entries);

pub const Request = struct {
    input: DeviceBuffer,
    output: DeviceBuffer,
    gamma: DeviceBuffer = .{},
    rows: u32,
    heads: u32,
    dim: u32,
    position: u32,
    log_floor: u32 = 0,
    log_alpha: f32 = 0,
    /// A positive threshold selects L2 clipping. Zero selects unweighted RMS.
    clip: f32 = 0,
    eps: f32,
};

const Footprint = struct { items: u32, bytes: usize, gamma: usize };

fn footprint(r: Request) !Footprint {
    if (r.rows == 0 or r.heads == 0 or r.dim == 0 or r.dim > 128 or
        !std.math.isFinite(r.clip) or r.clip < 0 or !std.math.isFinite(r.eps) or r.eps <= 0 or
        !std.math.isFinite(r.log_alpha) or r.log_alpha < 0) return error.InvalidShape;
    _ = try std.math.add(u32, r.position, r.rows);
    const items = try std.math.mul(u32, r.rows, r.heads);
    if (items > 0x7fff_ffff) return error.InvalidShape;
    const dim_bytes = @as(usize, r.dim) * 2;
    return .{
        .items = items,
        .bytes = try std.math.mul(usize, items, dim_bytes),
        .gamma = if (r.gamma.ptr == 0) 0 else try std.math.mul(usize, r.heads, dim_bytes),
    };
}

const Plane = extern struct {
    output: u64,
    input: u64,
    gamma: u64,
    dim: u32,
    clip: f32,
};

const Pair = extern struct { planes: [2]Plane };

pub const Module = struct {
    program: Program = .{},

    pub fn deinit(self: *Module, ctx: *Context) void {
        self.program.deinit(ctx);
    }

    pub fn run(self: *Module, ctx: *Context, r: Request) !void {
        const f = try footprint(r);
        try buffer.validateRegions(3, .{ r.input, r.gamma, r.output }, .{ f.bytes, f.gamma, f.bytes }, @splat(2), 2);
        if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
        try self.program.init(ctx);
        var addresses = [3]u64{ r.output.ptr, r.input.ptr, r.gamma.ptr };
        var heads = r.heads;
        var dim = r.dim;
        var position = r.position;
        var log_floor = r.log_floor;
        var clip = r.clip;
        var eps = r.eps;
        var alpha = r.log_alpha;
        var args = [_]?*anyopaque{ &addresses[0], &addresses[1], &addresses[2], &heads, &dim, &position, &log_floor, &clip, &eps, &alpha };
        try self.program.launch(ctx, .run, f.items, 1, 32, &args);
    }

    /// Group independent head dimensions and scales on the same position schedule.
    pub fn runPair(self: *Module, ctx: *Context, a: Request, b: Request) !void {
        if (a.rows != b.rows or a.heads != b.heads or a.position != b.position or a.log_floor != b.log_floor or
            a.log_alpha != b.log_alpha or a.eps != b.eps)
            return error.InvalidShape;
        const fa = try footprint(a);
        const fb = try footprint(b);
        try buffer.validateRegions(6, .{ a.input, a.gamma, b.input, b.gamma, a.output, b.output }, .{
            fa.bytes, fa.gamma, fb.bytes, fb.gamma, fa.bytes, fb.bytes,
        }, @splat(2), 4);
        if (ctx.info.compute_major < 8) return error.UnsupportedCudaDevice;
        try self.program.init(ctx);
        var pair: Pair = .{ .planes = .{
            .{ .output = a.output.ptr, .input = a.input.ptr, .gamma = a.gamma.ptr, .dim = a.dim, .clip = a.clip },
            .{ .output = b.output.ptr, .input = b.input.ptr, .gamma = b.gamma.ptr, .dim = b.dim, .clip = b.clip },
        } };
        var heads = a.heads;
        var position = a.position;
        var log_floor = a.log_floor;
        var eps = a.eps;
        var alpha = a.log_alpha;
        var args = [_]?*anyopaque{ &pair, &heads, &position, &log_floor, &eps, &alpha };
        try self.program.launch(ctx, .pair, fa.items, 2, 32, &args);
    }
};
