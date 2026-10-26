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

pub const Module = struct {
    module: driver.CUmodule = null,
    bf16_prefill: ?@import("lighton_prefill.zig").Module = null,
    bf16_prefill_configured: bool = false,
    functions: [23]driver.CUfunction = @splat(null),

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
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[12], (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    /// Resolve the opt-in library once per compute owner, before any claim
    /// can mutate histories. An invalid requested library is a hard error.
    pub fn configureBf16Prefill(self: *Module, ctx: *context.CudaContext, path: ?[]const u8) !void {
        if (self.bf16_prefill_configured) return;
        if (path) |name| {
            if (ctx.info.compute_major != 12 or ctx.info.compute_minor != 1) return error.LightOnPrefillUnsupportedDevice;
            self.bf16_prefill = try @import("lighton_prefill.zig").Module.init(name);
        }
        self.bf16_prefill_configured = true;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.bf16_prefill) |*prefill| {
            prefill.deinit(ctx);
            self.bf16_prefill = null;
        }
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.* = .{};
        }
    }

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module != null) return;
        var module: driver.CUmodule = null;
        const image = @embedFile("lighton_recurrent.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var functions: [23]driver.CUfunction = @splat(null);
        inline for (.{ "lighton_conv", "lighton_normalize", "lighton_recurrent", "lighton_gated_norm", "lighton_batch_conv", "lighton_batch_recurrent", "lighton_batch_conv_normalize", "lighton_batch_recurrent_norm", "lighton_batch_conv_normalize_cached_gates", "lighton_batch_recurrent_norm_cached_gates", "lighton_conv_cached_gates", "lighton_recurrent_cached_gates", "lighton_split_projections", "lighton_prefix_conv_normalize_cached_gates", "lighton_prefix_batch_conv_normalize", "lighton_prefix_batch_recurrent", "lighton_prefix_batch_recurrent_shared_qk", "lighton_ragged_conv_normalize_cached_gates", "lighton_ragged_recurrent_shared_qk", "lighton_ragged_prefix_conv", "lighton_ragged_recurrent_prefix_only", "lighton_ragged_prefix_gated_norm", "lighton_decode_fused_p2" }, 0..) |name, i| {
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&functions[i], module, name));
        }
        self.* = .{ .module = module, .functions = functions };
    }

    fn launch(self: *Module, ctx: *context.CudaContext, index: usize, grid_x: usize, grid_y: usize, args: []?*anyopaque) !void {
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[index], @intCast(grid_x), @intCast(grid_y), 1, 128, 1, 1, 0, ctx.stream, args.ptr, null));
        ctx.noteKernelLaunch();
    }

    pub fn runFusedDecode(self: *Module, ctx: *context.CudaContext, params: *RaggedParams) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        try self.launchFusedDecode(ctx, params, &packet, params.base.items);
        self.cached_batch_calls += 1;
    }

    fn launchFusedDecode(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, packet: []?*anyopaque, pages: usize) !void {
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[22], 1, params.base.items * params.base.value_heads, 1, 256, 1, 1, 0, ctx.stream, packet.ptr, null));
        ctx.noteKernelLaunch();
        self.fused_decode_calls += 1;
        self.fused_decode_pages += pages;
    }

    pub fn runRagged(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, rows: u32) !void {
        return self.runRaggedWithDecodeFusion(ctx, params, rows, false);
    }

    pub fn runRaggedWithDecodeFusion(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, rows: u32, fused_decode: bool) !void {
        return self.runRaggedWithPrefill(ctx, params, rows, fused_decode, 0, 0);
    }

    pub fn runRaggedWithPrefill(self: *Module, ctx: *context.CudaContext, params: *RaggedParams, rows: u32, fused_decode: bool, scratch: driver.CUdeviceptr, scratch_bytes: usize) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        const channels: usize = (2 * params.base.key_heads + params.base.value_heads) * 128;
        var decode_pages: usize = 0;
        if (fused_decode) for (params.lengths[0..params.base.items]) |n| {
            decode_pages += @intFromBool(n == 1);
        };
        if (scratch_bytes != 0) {
            const prefill = if (self.bf16_prefill) |*value| value else return error.LightOnPrefillUnavailable;
            try prefill.run(params, scratch, scratch_bytes, ctx.info.selected_device, ctx.stream);
            try self.launch(ctx, 21, rows * params.base.value_heads, 1, &packet);
            if (decode_pages > 0) try self.launchFusedDecode(ctx, params, &packet, decode_pages);
        } else if (decode_pages > 0) {
            if (decode_pages < params.base.items) {
                try self.launch(ctx, 19, channels / 128, rows, &packet);
                try self.launch(ctx, 20, 4, params.base.items * params.base.value_heads, &packet);
                try self.launch(ctx, 21, rows * params.base.value_heads, 1, &packet);
            }
            try self.launchFusedDecode(ctx, params, &packet, decode_pages);
        } else {
            try self.launch(ctx, 17, channels / 128, rows, &packet);
            try self.launch(ctx, 18, 4, params.base.items * params.base.value_heads, &packet);
            var gate = [_]?*anyopaque{ &params.base.core, &params.base.z, &params.base.norm_weight, &params.base.out, &params.base.value_heads, &params.base.eps };
            try self.launch(ctx, 3, rows * params.base.value_heads, 1, &gate);
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
        try self.launch(ctx, 14, channels / 128, params.base.items * params.seq, &packet);
        try self.launch(ctx, if (shared_qk) 16 else 15, 4, params.base.items * params.base.value_heads, &packet);
        var gate = [_]?*anyopaque{ &params.base.core, &params.base.z, &params.base.norm_weight, &params.base.out, &params.base.value_heads, &params.base.eps };
        try self.launch(ctx, 3, params.seq * params.base.items * params.base.value_heads, 1, &gate);
        self.prefix_batch_calls += 1;
        self.prefix_shared_qk_calls += @intFromBool(shared_qk);
    }

    pub fn runBatch(self: *Module, ctx: *context.CudaContext, params: *BatchParams, fused: bool, cached_gates: bool) !void {
        var packet = [_]?*anyopaque{@ptrCast(params)};
        const channels: usize = (2 * params.key_heads + params.value_heads) * 128;
        if (fused) {
            try self.launch(ctx, if (cached_gates) 8 else 6, channels / 128, params.items, &packet);
            try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[if (cached_gates) 9 else 7], 1, params.items * params.value_heads, 1, 512, 1, 1, 0, ctx.stream, &packet, null));
            ctx.noteKernelLaunch();
            self.cached_batch_calls += @intFromBool(cached_gates);
            return;
        }
        try self.launch(ctx, 4, channels / 128, params.items, &packet);
        var c: u32 = @intCast(channels);
        var norm = [_]?*anyopaque{ &params.conv, &c, &params.key_heads };
        try self.launch(ctx, 1, params.items * 2 * params.key_heads, 1, &norm);
        try self.launch(ctx, 5, 4, params.items * params.value_heads, &packet);
        var gate = [_]?*anyopaque{ &params.core, &params.z, &params.norm_weight, &params.out, &params.value_heads, &params.eps };
        try self.launch(ctx, 3, params.items * params.value_heads, 1, &gate);
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
            try self.launch(ctx, if (parallel_prefix and seq > 1) 13 else 10, (@as(usize, @intCast(channels)) + 127) / 128, if (parallel_prefix and seq > 1) seq else 1, &cached);
        } else try self.launch(ctx, 0, (@as(usize, @intCast(channels)) + 127) / 128, 1, &conv);
        var norm = [_]?*anyopaque{ &p[4], &channels, &kh };
        if (parallel_prefix and cached_gates and seq > 1)
            self.parallel_prefix_calls += 1
        else
            try self.launch(ctx, 1, seq * 2 * key_heads, 1, &norm);
        var recur = [_]?*anyopaque{ &p[4], &p[5], &p[6], &p[7], &p[8], &p[9], &p[10], &n, &kh, &vh, &valid };
        if (cached_gates) {
            var cached = [_]?*anyopaque{ &p[4], &p[5], &p[6], &p[7], &p[8], &p[9], &p[10], &n, &kh, &vh, &valid, &p[13] };
            try self.launch(ctx, 11, 4, value_heads, &cached);
        } else try self.launch(ctx, 2, 4, value_heads, &recur);
        var gate = [_]?*anyopaque{ &p[10], &p[11], &p[12], &p[13], &vh, &epsilon };
        try self.launch(ctx, 3, seq * value_heads, 1, &gate);
        self.cached_single_calls += @intFromBool(cached_gates);
    }
};
