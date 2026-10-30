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

//! Caller-owned F32 Q/K normalization, partial interleaved M-RoPE and query
//! gate preparation. This module owns no activation, position, KV or recurrent
//! storage, and launches only actual rows.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");

pub const Request = struct {
    query_gate: buffer.DeviceBuffer,
    key: buffer.DeviceBuffer,
    query_weight: buffer.DeviceBuffer,
    key_weight: buffer.DeviceBuffer,
    positions: buffer.DeviceBuffer,
    query_out: buffer.DeviceBuffer,
    key_out: buffer.DeviceBuffer,
    gate_out: buffer.DeviceBuffer,
    rows: usize,
    query_heads: usize,
    key_heads: usize,
    theta: f32,
    frequency_scale: f32,
    epsilon: f32,
};

const Params = extern struct {
    query_gate: u64,
    key: u64,
    query_weight: u64,
    key_weight: u64,
    positions: u64,
    query_out: u64,
    key_out: u64,
    gate_out: u64,
    rows: u32,
    query_heads: u32,
    key_heads: u32,
    dim: u32 = 256,
    rotary_dim: u32 = 64,
    theta: f32,
    frequency_scale: f32,
    epsilon: f32,
    section_height: u32 = 11,
    section_width: u32 = 10,
};

comptime {
    std.debug.assert(@sizeOf(Params) == 104);
    std.debug.assert(@offsetOf(Params, "rows") == 64);
}

/// Validate all bounds and output aliases before module initialization or launch.
pub fn validate(r: Request) !void {
    if (r.rows == 0 or r.rows > 8192 or
        !((r.query_heads == 8 and r.key_heads == 2) or (r.query_heads == 16 and r.key_heads == 4)) or
        r.theta != 10000000 or r.frequency_scale != 1 or r.epsilon != 1e-6) return error.InvalidShape;
    const q = try std.math.mul(usize, try std.math.mul(usize, r.rows, r.query_heads), 256 * 4);
    const k = try std.math.mul(usize, try std.math.mul(usize, r.rows, r.key_heads), 256 * 4);
    const regions = [_]buffer.DeviceBuffer{ r.query_gate, r.key, r.query_weight, r.key_weight, r.positions, r.query_out, r.key_out, r.gate_out };
    const lengths = [_]usize{ try std.math.mul(usize, q, 2), k, 256 * 4, 256 * 4, r.rows * 3 * 4, q, k, q };
    var ends: [8]u64 = undefined;
    for (regions, lengths, 0..) |region, length, index| {
        if (region.ptr == 0 or region.ptr & 3 != 0 or region.len < length) return error.InvalidShape;
        ends[index] = try std.math.add(u64, region.ptr, length);
    }
    // Read-only inputs may alias one another; each output must be disjoint
    // from every input and every other output.
    for (5..8) |i| for (0..i) |j| {
        if (regions[i].ptr < ends[j] and regions[j].ptr < ends[i]) return error.InvalidShape;
    };
}

pub const Module = struct {
    module: driver.CUmodule = null,
    function: driver.CUfunction = null,
    calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        if (self.module != null) return;
        try ctx.makeCurrent();
        var module: driver.CUmodule = null;
        const image = @embedFile("lighton_attention_inputs.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var function: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "lighton_qk_norm_partial_mrope_gate"));
        self.module = module;
        self.function = function;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch return;
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }

    pub fn run(self: *Module, ctx: *context.CudaContext, r: Request) !void {
        try validate(r);
        try self.init(ctx);
        var p = Params{
            .query_gate = r.query_gate.ptr,
            .key = r.key.ptr,
            .query_weight = r.query_weight.ptr,
            .key_weight = r.key_weight.ptr,
            .positions = r.positions.ptr,
            .query_out = r.query_out.ptr,
            .key_out = r.key_out.ptr,
            .gate_out = r.gate_out.ptr,
            .rows = @intCast(r.rows),
            .query_heads = @intCast(r.query_heads),
            .key_heads = @intCast(r.key_heads),
            .theta = r.theta,
            .frequency_scale = r.frequency_scale,
            .epsilon = r.epsilon,
        };
        var args = [_]?*anyopaque{&p};
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.function, @intCast(r.query_heads + r.key_heads), @intCast(r.rows), 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
};

test "LightOnOCR3 fused attention input preflight rejects truncated aliased and unsupported regions" {
    const valid = Request{
        .query_gate = .{ .ptr = 0x10000, .len = 8 * 512 * 4 },
        .key = .{ .ptr = 0x20000, .len = 2 * 256 * 4 },
        .query_weight = .{ .ptr = 0x30000, .len = 256 * 4 },
        .key_weight = .{ .ptr = 0x30000, .len = 256 * 4 },
        .positions = .{ .ptr = 0x40000, .len = 3 * 4 },
        .query_out = .{ .ptr = 0x50000, .len = 8 * 256 * 4 },
        .key_out = .{ .ptr = 0x60000, .len = 2 * 256 * 4 },
        .gate_out = .{ .ptr = 0x70000, .len = 8 * 256 * 4 },
        .rows = 1,
        .query_heads = 8,
        .key_heads = 2,
        .theta = 10000000,
        .frequency_scale = 1,
        .epsilon = 1e-6,
    };
    try validate(valid);
    inline for (.{ "query_gate", "key", "query_weight", "key_weight", "positions", "query_out", "key_out", "gate_out" }) |field| {
        var bad = valid;
        @field(bad, field).len -= 1;
        try std.testing.expectError(error.InvalidShape, validate(bad));
        bad = valid;
        @field(bad, field).ptr = 0;
        try std.testing.expectError(error.InvalidShape, validate(bad));
    }
    inline for (.{ "query_out", "key_out", "gate_out" }) |output| {
        inline for (.{ "query_gate", "key", "query_weight", "key_weight", "positions" }) |input| {
            var bad = valid;
            @field(bad, output).ptr = @field(bad, input).ptr;
            try std.testing.expectError(error.InvalidShape, validate(bad));
        }
    }
    var bad = valid;
    bad.gate_out.ptr = bad.query_out.ptr;
    try std.testing.expectError(error.InvalidShape, validate(bad));
    bad = valid;
    bad.rows = 8193;
    try std.testing.expectError(error.InvalidShape, validate(bad));
    bad = valid;
    bad.theta = std.math.nan(f32);
    try std.testing.expectError(error.InvalidShape, validate(bad));
}
