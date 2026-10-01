// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pinned efficient boundary attention, using only the existing CUDA driver.
//! Saved forward values and scratch are supplied by the resident tape; no
//! hidden activation cache, host tensor copies or runtime compilation library.
const std = @import("std");
const driver = @import("driver.zig");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
pub const Attrs = @import("ml").graph.node.BoundaryTrainingAttentionAttrs;
const forward_cubin = @embedFile("artifacts/gliner25_boundary_attention_forward.cubin");
const backward_cubin = @embedFile("artifacts/gliner25_boundary_attention_backward.cubin");
const forward_sm80 = @embedFile("artifacts/gliner25_boundary_attention_forward.sm80.cubin");
const backward_sm80 = @embedFile("artifacts/gliner25_boundary_attention_backward.sm80.cubin");
const forward_shared = 0;
const backward_shared = 0;

/// Hash actual embedded bytes once at trainer startup. Keeping the large
/// images out of comptime evaluation bounds compiler memory without weakening
/// checkpoint identity or trusting a separately generated hash constant.
pub fn artifactHash() [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ forward_cubin, backward_cubin, forward_sm80, backward_sm80 }) |bytes| hash.update(bytes);
    return hash.finalResult();
}

pub const Module = struct {
    forward_module: driver.CUmodule,
    backward_module: driver.CUmodule,
    forward: driver.CUfunction,
    backward: driver.CUfunction,
    bias: driver.CUfunction,
    delta: driver.CUfunction,
    zero: driver.CUfunction,

    pub fn init(ctx: *Context) !Module {
        if (ctx.info.compute_major != 8) return error.CudaKernelUnavailable;
        try ctx.makeCurrent();
        if (try ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
        const sm89 = ctx.info.compute_major == 8 and ctx.info.compute_minor == 9;
        var f: driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&f, if (sm89) forward_cubin.ptr else forward_sm80.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(f);
        var b: driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&b, if (sm89) backward_cubin.ptr else backward_sm80.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(b);
        const forward = try function(ctx, f, "boundary_forward");
        const backward = try function(ctx, b, "boundary_backward");
        // The driver validates the device's opt-in shared-memory capacity.
        return .{ .forward_module = f, .backward_module = b, .forward = forward, .backward = backward, .bias = try function(ctx, f, "boundary_bias"), .delta = try function(ctx, b, "boundary_delta32"), .zero = try function(ctx, f, "boundary_zero") };
    }

    pub fn deinit(self: *Module, ctx: *Context) void {
        ctx.makeCurrent() catch {};
        if (self.backward_module != null) _ = ctx.driver.fns.cuModuleUnload(self.backward_module);
        if (self.forward_module != null) _ = ctx.driver.fns.cuModuleUnload(self.forward_module);
        self.* = undefined;
    }

    fn function(ctx: *Context, module: driver.CUmodule, name: [:0]const u8) !driver.CUfunction {
        var result: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&result, module, name.ptr));
        return result;
    }

    fn region(buffer: Buffer, offset: usize, bytes: usize) !Buffer {
        if (buffer.ptr == 0 or offset > buffer.len or bytes > buffer.len - offset) return error.InvalidCudaState;
        return .{ .ptr = try std.math.add(driver.CUdeviceptr, buffer.ptr, offset), .len = bytes };
    }

    fn launch(ctx: *Context, f: driver.CUfunction, params: []?*anyopaque, grid: [3]u32, block: [3]u32, shared: u32) !void {
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(f, grid[0], grid[1], grid[2], block[0], block[1], block[2], shared, ctx.stream, params.ptr, null));
        ctx.noteKernelLaunch();
    }

    /// Backward takes saved packed output and a same-shaped cotangent. Only
    /// its attended prefix is differentiated; the saved LSE suffix is opaque.
    pub fn execute(self: *const Module, ctx: *Context, attrs: Attrs, backward: bool, output: Buffer, qkv: Buffer, mask: Buffer, saved: Buffer, upstream: Buffer, scratch: Buffer) !void {
        const layout = try attrs.layout();
        const out_bytes: usize = @intCast(layout.output_elements * 4);
        const saved_bytes: usize = @intCast((layout.output_elements + layout.lse_elements) * 4);
        _ = try region(qkv, 0, out_bytes * 3);
        _ = try region(mask, 0, @intCast(layout.rows * 4));
        _ = try region(output, 0, if (backward) out_bytes * 3 else saved_bytes);
        const forward_saved = if (backward) saved else output;
        _ = try region(forward_saved, 0, saved_bytes);
        if (backward) _ = try region(upstream, 0, saved_bytes);
        _ = try region(scratch, 0, try layout.scratchBytes(backward));
        const bias_bytes: usize = @intCast(layout.bias_elements * 4);
        var bias = (try region(scratch, 0, bias_bytes)).ptr;
        var mask_ptr = mask.ptr;
        var batch: i32 = @intCast(attrs.batch);
        var sequence: i32 = @intCast(attrs.seq_len);
        var heads: i32 = @intCast(attrs.num_heads);
        var columns: i32 = @intCast(layout.bias_columns);
        var window: i32 = @intCast(attrs.window);
        var out_ptr = output.ptr;
        var forward_ptr = forward_saved.ptr;
        var lse = (try region(forward_saved, out_bytes, @intCast(layout.lse_elements * 4))).ptr;
        var packed_ptr = qkv.ptr;
        try ctx.makeCurrent();
        if (try ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
        var bias_args = [_]?*anyopaque{ @ptrCast(&bias), @ptrCast(&mask_ptr), @ptrCast(&batch), @ptrCast(&sequence), @ptrCast(&columns), @ptrCast(&window) };
        try launch(ctx, self.bias, &bias_args, .{ @intCast(@divTrunc(layout.bias_elements + 255, 256)), 1, 1 }, .{ 256, 1, 1 }, 0);
        if (!backward) {
            var params = [_]?*anyopaque{ @ptrCast(&out_ptr), @ptrCast(&lse), @ptrCast(&packed_ptr), @ptrCast(&bias), @ptrCast(&batch), @ptrCast(&sequence), @ptrCast(&heads), @ptrCast(&columns) };
            try launch(ctx, self.forward, &params, .{ (attrs.seq_len + 127) / 128, attrs.num_heads, attrs.batch }, .{ 128, 1, 1 }, forward_shared);
        } else {
            var zero_ptr = out_ptr;
            var zero_count: i32 = @intCast(layout.output_elements * 3);
            var zero_args = [_]?*anyopaque{ @ptrCast(&zero_ptr), @ptrCast(&zero_count) };
            try launch(ctx, self.zero, &zero_args, .{ @intCast(@divTrunc(layout.output_elements * 3 + 255, 256)), 1, 1 }, .{ 256, 1, 1 }, 0);
            const delta_bytes: usize = @intCast(layout.delta_elements * 4);
            var delta = (try region(scratch, bias_bytes, delta_bytes)).ptr;
            const delta_padded = std.mem.alignForward(usize, delta_bytes, 16);
            var workspace = (try region(scratch, bias_bytes + delta_padded, layout.workspace_bytes)).ptr;
            var dy = upstream.ptr;
            var delta_args = [_]?*anyopaque{ @ptrCast(&delta), @ptrCast(&forward_ptr), @ptrCast(&dy), @ptrCast(&batch), @ptrCast(&sequence), @ptrCast(&heads) };
            try launch(ctx, self.delta, &delta_args, .{ @intCast(@divTrunc(layout.delta_elements + 3, 4)), 1, 1 }, .{ 32, 4, 1 }, 0);
            // num_splits_key=1 and kernel window_size=0 (mask is in bias), so
            // the pinned kernel initializes its own query accumulation tiles.
            var params = [_]?*anyopaque{ @ptrCast(&out_ptr), @ptrCast(&forward_ptr), @ptrCast(&dy), @ptrCast(&lse), @ptrCast(&delta), @ptrCast(&packed_ptr), @ptrCast(&bias), @ptrCast(&workspace), @ptrCast(&batch), @ptrCast(&sequence), @ptrCast(&heads), @ptrCast(&columns) };
            try launch(ctx, self.backward, &params, .{ (attrs.seq_len + 127) / 128, attrs.num_heads, attrs.batch }, .{ 128, 1, 1 }, backward_shared);
        }
    }
};
