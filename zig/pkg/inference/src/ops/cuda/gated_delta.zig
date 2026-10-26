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
const buffer = @import("buffer.zig");

pub const State = struct {
    ctx: ?*context.CudaContext = null,
    history: [2]buffer.DeviceBuffer = .{ .{}, .{} },
    recurrent: buffer.DeviceBuffer = .{},
    weights: [4]buffer.DeviceBuffer = .{ .{}, .{}, .{}, .{} },
    owns_weights: [4]bool = @splat(true),
    // The compute/session owner already outlives ctx and every page state.
    // Its ordinary admitted cache can retain exact persistent allocations.
    release_context: ?*anyopaque = null,
    release_buffer: ?*const fn (*anyopaque, *buffer.DeviceBuffer) void = null,
    weight_sources: [4]driver.CUdeviceptr = @splat(0),
    key_heads: usize = 0,
    value_heads: usize = 0,
    initialized: bool = false,
    failed: bool = false,
    admission: ?@import("../../runtime/tier/memory.zig").AdmissionLease = null,

    pub fn reset(self: *State) void {
        self.initialized = false;
        self.failed = false;
    }

    fn releaseOwned(self: *State, ctx: *context.CudaContext, value: *buffer.DeviceBuffer) void {
        if (!self.failed) if (self.release_buffer) |release| {
            release(self.release_context.?, value);
            return;
        };
        value.free(ctx);
    }

    pub fn deinit(self: *State) void {
        if (self.ctx) |ctx| {
            for (&self.history) |*value| self.releaseOwned(ctx, value);
            self.releaseOwned(ctx, &self.recurrent);
            for (&self.weights, self.owns_weights) |*value, owned| if (owned) self.releaseOwned(ctx, value);
        }
        if (self.admission) |*lease| lease.release();
        self.* = .{};
    }
};

pub const BatchPage = extern struct {
    history: driver.CUdeviceptr = 0,
    next_history: driver.CUdeviceptr = 0,
    state: driver.CUdeviceptr = 0,
};
pub const BatchParams = extern struct {
    mixed: driver.CUdeviceptr = 0,
    conv_weight: driver.CUdeviceptr = 0,
    conv: driver.CUdeviceptr = 0,
    z: driver.CUdeviceptr = 0,
    beta: driver.CUdeviceptr = 0,
    a: driver.CUdeviceptr = 0,
    a_log: driver.CUdeviceptr = 0,
    dt_bias: driver.CUdeviceptr = 0,
    core: driver.CUdeviceptr = 0,
    norm_weight: driver.CUdeviceptr = 0,
    out: driver.CUdeviceptr = 0,
    key_heads: u32 = 0,
    value_heads: u32 = 0,
    items: u32 = 0,
    eps: f32 = 0,
    pages: [64]BatchPage = @splat(.{}),
};
comptime {
    if (@sizeOf(BatchPage) != 24 or @sizeOf(BatchParams) != 1640 or @offsetOf(BatchParams, "pages") != 104)
        @compileError("LightOn recurrent batch ABI changed");
}

pub const PrefixBatchParams = extern struct {
    base: BatchParams,
    seq: u32,
    reserved: u32 = 0,
};
comptime {
    if (@sizeOf(PrefixBatchParams) != 1648 or @offsetOf(PrefixBatchParams, "seq") != 1640)
        @compileError("LightOn prefix batch ABI changed");
}

pub const RaggedParams = extern struct {
    base: BatchParams,
    offsets: [64]u32 = @splat(0),
    lengths: [64]u32 = @splat(0),
    initialized: [64]u32 = @splat(0),
};
comptime {
    if (@sizeOf(RaggedParams) != 2408 or @offsetOf(RaggedParams, "offsets") != 1640 or
        @offsetOf(RaggedParams, "lengths") != 1896 or @offsetOf(RaggedParams, "initialized") != 2152)
        @compileError("LightOn ragged batch ABI changed");
}

// Native export names remain the compatibility ABI of the qualified PTX.
const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("lighton_recurrent.ptx") ++ "\x00", .{
    .causal_conv = "lighton_conv",
    .normalize_qk = "lighton_normalize",
    .recurrent = "lighton_recurrent",
    .gated_norm = "lighton_gated_norm",
    .batch_causal_conv = "lighton_batch_conv",
    .batch_recurrent = "lighton_batch_recurrent",
    .batch_conv_normalize = "lighton_batch_conv_normalize",
    .batch_recurrent_norm = "lighton_batch_recurrent_norm",
    .batch_conv_normalize_cached_gates = "lighton_batch_conv_normalize_cached_gates",
    .batch_recurrent_norm_cached_gates = "lighton_batch_recurrent_norm_cached_gates",
    .conv_cached_gates = "lighton_conv_cached_gates",
    .recurrent_cached_gates = "lighton_recurrent_cached_gates",
    .split_projections = "lighton_split_projections",
    .prefix_conv_normalize_cached_gates = "lighton_prefix_conv_normalize_cached_gates",
    .prefix_batch_conv_normalize = "lighton_prefix_batch_conv_normalize",
    .prefix_batch_recurrent = "lighton_prefix_batch_recurrent",
    .prefix_batch_recurrent_shared_qk = "lighton_prefix_batch_recurrent_shared_qk",
    .ragged_conv_normalize_cached_gates = "lighton_ragged_conv_normalize_cached_gates",
    .ragged_recurrent_shared_qk = "lighton_ragged_recurrent_shared_qk",
    .ragged_prefix_conv = "lighton_ragged_prefix_conv",
    .ragged_recurrent_prefix_only = "lighton_ragged_recurrent_prefix_only",
    .ragged_prefix_gated_norm = "lighton_ragged_prefix_gated_norm",
    .decode_fused_p2 = "lighton_decode_fused_p2",
});

pub const Module = struct {
    program: Program = .{},

    prefix_shared_qk_calls: usize = 0,
    ragged_batch_calls: usize = 0,
    fused_decode_calls: usize = 0,
    fused_decode_pages: usize = 0,
    ragged_query_rows: usize = 0,
    prefix_batch_calls: usize = 0,
    parallel_prefix_calls: usize = 0,
    cached_single_calls: usize = 0,
    cached_batch_calls: usize = 0,

    pub fn splitProjections(self: *Module, ctx: *context.CudaContext, pointers: [5]driver.CUdeviceptr, count: u32, mixed_dim: u32, z_dim: u32, heads: u32) !void {
        var p = pointers;
        var n = count;
        var m = mixed_dim;
        var z = z_dim;
        var h = heads;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &n, &m, &z, &h };
        try self.program.launch(ctx, .split_projections, (count + 255) / 256, 1, 256, &args);
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.program.module != null) {
            self.program.deinit(ctx);
            self.* = .{};
        }
    }

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        const loaded = self.program.module != null;
        try self.program.init(ctx);
        if (!loaded) self.* = .{ .program = self.program };
    }

    fn launch(self: *Module, ctx: *context.CudaContext, entry: Program.Entry, grid_x: usize, grid_y: usize, args: []?*anyopaque) !void {
        try self.program.launch(ctx, entry, @intCast(grid_x), @intCast(grid_y), 128, args);
    }

    pub fn runFusedDecode(self: *Module, ctx: *context.CudaContext, params: *RaggedParams) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        try self.launchFusedDecode(ctx, params, &packet, params.base.items);
        self.cached_batch_calls += 1;
    }

    fn launchFusedDecode(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, packet: []?*anyopaque, pages: usize) !void {
        try self.program.launch(ctx, .decode_fused_p2, 1, params.base.items * params.base.value_heads, 256, packet);
        self.fused_decode_calls += 1;
        self.fused_decode_pages += pages;
    }

    pub fn runRagged(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, rows: u32) !void {
        return self.runRaggedWithDecodeFusion(ctx, params, rows, false);
    }

    pub fn runRaggedWithDecodeFusion(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, rows: u32, fused_decode: bool) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        const channels: usize = (2 * params.base.key_heads + params.base.value_heads) * 128;
        var decode_pages: usize = 0;
        if (fused_decode) for (params.lengths[0..params.base.items]) |n| {
            decode_pages += @intFromBool(n == 1);
        };
        if (decode_pages > 0) {
            if (decode_pages < params.base.items) {
                try self.launch(ctx, .ragged_prefix_conv, channels / 128, rows, &packet);
                try self.launch(ctx, .ragged_recurrent_prefix_only, 4, params.base.items * params.base.value_heads, &packet);
                try self.launch(ctx, .ragged_prefix_gated_norm, rows * params.base.value_heads, 1, &packet);
            }
            try self.launchFusedDecode(ctx, params, &packet, decode_pages);
        } else {
            try self.launch(ctx, .ragged_conv_normalize_cached_gates, channels / 128, rows, &packet);
            try self.launch(ctx, .ragged_recurrent_shared_qk, 4, params.base.items * params.base.value_heads, &packet);
            var gate = [_]?*anyopaque{ &params.base.core, &params.base.z, &params.base.norm_weight, &params.base.out, &params.base.value_heads, &params.base.eps };
            try self.launch(ctx, .gated_norm, rows * params.base.value_heads, 1, &gate);
        }
        self.ragged_batch_calls += 1;
        self.cached_batch_calls += 1;
        self.ragged_query_rows += rows;
    }

    pub fn runPrefixBatch(self: *Module, ctx: *context.CudaContext, params: *PrefixBatchParams) !void {
        return self.runPrefixBatchWithSharedQk(ctx, params, false);
    }

    pub fn runPrefixBatchWithSharedQk(self: *Module, ctx: *context.CudaContext, params: *PrefixBatchParams, shared_qk: bool) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        const channels: usize = (2 * params.base.key_heads + params.base.value_heads) * 128;
        try self.launch(ctx, .prefix_batch_conv_normalize, channels / 128, params.base.items * params.seq, &packet);
        try self.launch(ctx, if (shared_qk) .prefix_batch_recurrent_shared_qk else .prefix_batch_recurrent, 4, params.base.items * params.base.value_heads, &packet);
        var gate = [_]?*anyopaque{ &params.base.core, &params.base.z, &params.base.norm_weight, &params.base.out, &params.base.value_heads, &params.base.eps };
        try self.launch(ctx, .gated_norm, params.seq * params.base.items * params.base.value_heads, 1, &gate);
        self.prefix_batch_calls += 1;
        self.prefix_shared_qk_calls += @intFromBool(shared_qk);
    }

    pub fn runBatch(self: *Module, ctx: *context.CudaContext, params: *BatchParams, fused: bool, cached_gates: bool) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        const channels: usize = (2 * params.key_heads + params.value_heads) * 128;
        if (fused) {
            try self.launch(ctx, if (cached_gates) .batch_conv_normalize_cached_gates else .batch_conv_normalize, channels / 128, params.items, &packet);
            try self.program.launch(ctx, if (cached_gates) .batch_recurrent_norm_cached_gates else .batch_recurrent_norm, 1, params.items * params.value_heads, 512, &packet);
            self.cached_batch_calls += @intFromBool(cached_gates);
            return;
        }
        try self.launch(ctx, .batch_causal_conv, channels / 128, params.items, &packet);
        var c: u32 = @intCast(channels);
        var norm = [_]?*anyopaque{ &params.conv, &c, &params.key_heads };
        try self.launch(ctx, .normalize_qk, params.items * 2 * params.key_heads, 1, &norm);
        try self.launch(ctx, .batch_recurrent, 4, params.items * params.value_heads, &packet);
        var gate = [_]?*anyopaque{ &params.core, &params.z, &params.norm_weight, &params.out, &params.value_heads, &params.eps };
        try self.launch(ctx, .gated_norm, params.items * params.value_heads, 1, &gate);
    }

    // Pointer order: mixed, conv weight, history, next history, conv, beta, a,
    // a_log, dt_bias, recurrent, core, z, norm weight, output.
    pub fn run(self: *Module, ctx: *context.CudaContext, pointers: [14]driver.CUdeviceptr, seq: usize, key_heads: usize, value_heads: usize, initialized: bool, eps: f32, cached_gates: bool, parallel_prefix: bool) !void {
        var p = pointers;
        var n: i32 = @intCast(seq);
        var kh: i32 = @intCast(key_heads);
        var vh: i32 = @intCast(value_heads);
        var channels: i32 = @intCast((2 * key_heads + value_heads) * 128);
        var valid: i32 = @intFromBool(initialized);
        var epsilon = eps;
        var conv = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &n, &channels, &valid };
        if (cached_gates) {
            var cached = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &n, &channels, &valid, &p[5], &p[6], &p[7], &p[8], &p[13], &vh };
            try self.launch(ctx, if (parallel_prefix and seq > 1) .prefix_conv_normalize_cached_gates else .conv_cached_gates, (@as(usize, @intCast(channels)) + 127) / 128, if (parallel_prefix and seq > 1) seq else 1, &cached);
        } else try self.launch(ctx, .causal_conv, (@as(usize, @intCast(channels)) + 127) / 128, 1, &conv);
        var norm = [_]?*anyopaque{ &p[4], &channels, &kh };
        if (parallel_prefix and cached_gates and seq > 1)
            self.parallel_prefix_calls += 1
        else
            try self.launch(ctx, .normalize_qk, seq * 2 * key_heads, 1, &norm);
        var recur = [_]?*anyopaque{ &p[4], &p[5], &p[6], &p[7], &p[8], &p[9], &p[10], &n, &kh, &vh, &valid };
        if (cached_gates) {
            var cached = [_]?*anyopaque{ &p[4], &p[5], &p[6], &p[7], &p[8], &p[9], &p[10], &n, &kh, &vh, &valid, &p[13] };
            try self.launch(ctx, .recurrent_cached_gates, 4, value_heads, &cached);
        } else try self.launch(ctx, .recurrent, 4, value_heads, &recur);
        var gate = [_]?*anyopaque{ &p[10], &p[11], &p[12], &p[13], &vh, &epsilon };
        try self.launch(ctx, .gated_norm, seq * value_heads, 1, &gate);
        self.cached_single_calls += @intFromBool(cached_gates);
    }
};
