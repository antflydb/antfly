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
const buffer = @import("buffer.zig");

const Cleanup = @import("../../execution/cleanup_context.zig").Context;

pub const State = struct {
    ctx: ?*context.CudaContext = null,
    history: [2]buffer.DeviceBuffer = .{ .{}, .{} },
    recurrent: buffer.DeviceBuffer = .{},
    /// One physical owner for pooled history, recurrence and owned weight
    /// snapshots. Their descriptors are borrowed, non-overlapping views.
    packed_storage: buffer.DeviceBuffer = .{},
    weights: [4]buffer.DeviceBuffer = .{ .{}, .{}, .{}, .{} },
    owns_weights: [4]bool = @splat(true),
    // The compute/session owner already outlives ctx and every page state.
    // Its ordinary admitted cache can retain exact persistent allocations.
    release_context: ?*anyopaque = null,
    release_buffer: ?*const fn (*anyopaque, *buffer.DeviceBuffer) void = null,
    prepare_release_with_cleanup: ?*const fn (*anyopaque, Cleanup) anyerror!void = null,
    release_buffer_with_cleanup: ?*const fn (*anyopaque, *buffer.DeviceBuffer, bool, Cleanup) anyerror!void = null,
    weight_sources: [4]driver.CUdeviceptr = @splat(0),
    key_heads: usize = 0,
    value_heads: usize = 0,
    initialized: bool = false,
    failed: bool = false,
    admission: ?@import("../../runtime/tier/memory.zig").AdmissionLease = null,
    // ONLY a synchronous temporary eager owner may install this borrowed loan.
    // Persistent decode/cache/graph State never receives a RunContext budget.
    temporary_budget: ?*@import("../../execution/recurrent_state_admission.zig").Budget = null,

    pub fn reset(self: *State) void {
        self.initialized = false;
        self.failed = false;
    }

    fn releaseOwned(self: *State, ctx: *context.CudaContext, value: *buffer.DeviceBuffer) void {
        // A packed view must never become an independently cached alias.
        if (value.borrowed) {
            value.free(ctx);
            return;
        }
        if (!self.failed) if (self.release_buffer) |release| {
            release(self.release_context.?, value);
            return;
        };
        value.free(ctx);
    }

    pub fn validateCleanup(self: *const State, current: Cleanup) !void {
        // The separate generic additional-state producer still has its legacy
        // raw quota. Do not pretend a late Context supplies its missing cold
        // retirement shell, or refund it under a caller's numerical lanes.
        if (self.admission != null and !current.outside_execution) return error.UnpreparedRecurrentStateQuotaRetirement;
        if (self.temporary_budget) |budget| try budget.validateCleanup(current);
        if (current.boundary) |epoch| if (try epoch.isSealed()) return error.RetirementEpochSealed;
    }
    pub fn deinitWithCleanup(self: *State, current: Cleanup) !void {
        try self.validateCleanup(current);
        if (self.prepare_release_with_cleanup) |prepare| try prepare(self.release_context.?, current);
        if (self.ctx) |ctx| {
            for (&self.history) |*value| try self.releaseOwnedWithCleanup(ctx, value, current);
            try self.releaseOwnedWithCleanup(ctx, &self.recurrent, current);
            for (&self.weights, self.owns_weights) |*value, owned| if (owned) try self.releaseOwnedWithCleanup(ctx, value, current);
            try self.releaseOwnedWithCleanup(ctx, &self.packed_storage, current);
        }
        const temporary_budget = self.temporary_budget;
        if (temporary_budget) |budget| try budget.returnAfterBackingRetired(current);
        var admission = self.admission;
        self.* = .{}; // consume original handle before arbitrary outside refund
        if (admission) |*lease| lease.release(); // explicit outside-only above
    }
    fn releaseOwnedWithCleanup(self: *State, ctx: *context.CudaContext, value: *buffer.DeviceBuffer, current: Cleanup) !void {
        if (value.ptr == 0) return;
        if (value.borrowed) {
            _ = try value.freeChecked(ctx);
            return;
        }
        if (self.release_buffer_with_cleanup) |release| {
            try release(self.release_context.?, value, !self.failed and self.release_buffer != null, current);
        } else _ = try value.freeChecked(ctx);
    }

    pub fn deinit(self: *State) void {
        if (self.temporary_budget != null) @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (self.ctx) |ctx| {
            for (&self.history) |*value| self.releaseOwned(ctx, value);
            self.releaseOwned(ctx, &self.recurrent);
            for (&self.weights, self.owns_weights) |*value, owned| if (owned) self.releaseOwned(ctx, value);
            self.releaseOwned(ctx, &self.packed_storage);
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
const catalog = @import("../../tile/cuda_standalone.zig");
const Program = @import("embedded_module.zig").EmbeddedModule(@import("embedded_module.zig").profile(catalog.gated_delta).image, catalog.gated_delta_entries);

/// Geometry implemented by the existing independent-page packet kernels.
/// Generic recurrent shapes retain their established ordinary core path.
pub fn packetGeometrySupported(key_heads: usize, value_heads: usize, dim: usize, value_dim: usize, conv_kernel: usize) bool {
    return dim == 128 and value_dim == 128 and conv_kernel == 4 and key_heads == 16 and (value_heads == 16 or value_heads == 32);
}

pub const Module = struct {
    program: Program = .{},
    bf16_prefill: ?@import("lighton_prefill.zig").Module = null,
    bf16_prefill_configured: bool = false,

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
            prefill.deinitChecked(ctx) catch return;
            self.bf16_prefill = null;
        }
        self.program.deinitChecked(ctx) catch return;
        self.* = .{};
    }

    pub fn deinitWithCleanup(self: *Module, ctx: *context.CudaContext, current: @import("../../execution/cleanup_context.zig").Context) !void {
        if (self.bf16_prefill) |*prefill| {
            try prefill.deinitWithCleanup(ctx, current);
            self.bf16_prefill = null;
        }
        try self.program.deinitChecked(ctx);
        self.* = .{};
    }

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try self.program.init(ctx);
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
            try self.launch(ctx, .ragged_prefix_gated_norm, rows * params.base.value_heads, 1, &packet);
            if (decode_pages > 0) try self.launchFusedDecode(ctx, params, &packet, decode_pages);
        } else if (decode_pages > 0) {
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

test "typed recurrent state cleanup prepares actual current B once and releases only allocating root not borrowed packed views" {
    const Sink = @import("../../execution/retirement_sink.zig");
    var sink = try Sink.Ref.create();
    defer sink.release();
    var old_guard = try sink.enter();
    var old = try old_guard.retainEpoch();
    defer old.release();
    old_guard.finish();
    var current = try sink.enter();
    defer current.finish();
    var epoch = try current.retainEpoch();
    defer epoch.release();
    const Probe = struct {
        expected: *const Sink.Epoch,
        prepares: usize = 0,
        roots: usize = 0,
        fn prepare(raw: *anyopaque, cleanup: Cleanup) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const actual = cleanup.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (try actual.isSealed()) return error.RetirementEpochSealed;
            try std.testing.expect(actual.record == self.expected.record and actual.nonce == self.expected.nonce);
            self.prepares += 1;
        }
        fn release(raw: *anyopaque, value: *buffer.DeviceBuffer, recycle: bool, _: Cleanup) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), self.prepares);
            try std.testing.expect(recycle and !value.borrowed);
            try std.testing.expectEqual(@as(driver.CUdeviceptr, 0x1000), value.ptr);
            self.roots += 1;
            value.* = .{};
        }
        fn legacy(_: *anyopaque, _: *buffer.DeviceBuffer) void {
            @panic("typed release must not enter legacy callback");
        }
    };
    var probe = Probe{ .expected = &epoch };
    var ctx: context.CudaContext = undefined; // callbacks never dereference SDK
    var state = State{ .ctx = &ctx, .release_context = &probe, .release_buffer = Probe.legacy, .prepare_release_with_cleanup = Probe.prepare, .release_buffer_with_cleanup = Probe.release, .owns_weights = @splat(false), .history = .{ .{ .ptr = 0x1000, .len = 16, .borrowed = true }, .{ .ptr = 0x1010, .len = 16, .borrowed = true } }, .recurrent = .{ .ptr = 0x1020, .len = 32, .borrowed = true }, .packed_storage = .{ .ptr = 0x1000, .len = 64 } };
    try std.testing.expectError(error.RetirementEpochSealed, state.deinitWithCleanup(.{ .boundary = &old }));
    try std.testing.expectEqual(@as(usize, 0), probe.roots);
    try std.testing.expectEqual(@as(driver.CUdeviceptr, 0x1000), state.packed_storage.ptr);
    try state.deinitWithCleanup(.{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 1), probe.prepares);
    try std.testing.expectEqual(@as(usize, 1), probe.roots);
    try std.testing.expect(state.ctx == null);
}

test "typed recurrent cleanup refuses legacy additional state quota before mutating borrowed or SDK roots" {
    const Memory = @import("../../runtime/tier/memory.zig");
    var controller = Memory.AdmissionController{};
    const lease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false);
    var state = State{ .admission = lease, .packed_storage = .{ .ptr = 0x1000, .len = 64 }, .initialized = true };
    try std.testing.expectError(error.UnpreparedRecurrentStateQuotaRetirement, state.deinitWithCleanup(.{}));
    try std.testing.expect(state.initialized and state.packed_storage.ptr == 0x1000);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    // Explicit outside-only legacy quota owner, no SDK endpoint in this pure
    // fixture. The cold additional-state producer successor replaces this arm.
    try state.deinitWithCleanup(.{ .outside_execution = true });
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
}

test "CUDA captured recurrent packet geometry preserves actual supported selectors" {
    try std.testing.expect(packetGeometrySupported(16, 16, 128, 128, 4));
    try std.testing.expect(packetGeometrySupported(16, 32, 128, 128, 4));
    for ([_][5]usize{ .{ 1, 1, 128, 128, 4 }, .{ 8, 16, 128, 128, 4 }, .{ 16, 8, 128, 128, 4 }, .{ 16, 16, 64, 128, 4 }, .{ 16, 16, 128, 64, 4 }, .{ 16, 16, 128, 128, 3 } }) |shape|
        try std.testing.expect(!packetGeometrySupported(shape[0], shape[1], shape[2], shape[3], shape[4]));
}
