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

//! Experimental ABI2 tiled prefill. The owning context kernel validates views,
//! immutable descriptor lifetime and status before handing a dispatch here.
//! Pipeline objects share its existing runtime; no additional queue or KV copy.
const std = @import("std");
const shared = @import("metal_generated_kernels.zig");
const mr = @import("metal_runtime.zig");
const views = @import("../runtime/kv/context_view.zig");

pub const Kernels = struct {
    hd256: *mr.RawMetalGeneratedPipeline,
    hd512: *mr.RawMetalGeneratedPipeline,

    fn create(name: [*:0]const u8) !*mr.RawMetalGeneratedPipeline {
        if (comptime !@import("build_options").enable_metal) return error.MetalNotEnabled;
        const source = @embedFile("context_flash.metal");
        var message: [4096]u8 = @splat(0);
        return mr.termite_metal_generated_pipeline_create(source.ptr, source.len, name, 1, &message, message.len) orelse {
            std.log.err("Context flash Metal kernel: {s}", .{std.mem.sliceTo(&message, 0)});
            return error.MetalKernelCompilationFailed;
        };
    }

    pub fn init() !Kernels {
        const hd256 = try create("context_flash_256");
        errdefer mr.termite_metal_generated_pipeline_destroy(hd256);
        return .{ .hd256 = hd256, .hd512 = try create("context_flash_512") };
    }

    pub fn deinit(self: *Kernels) void {
        mr.termite_metal_generated_pipeline_destroy(self.hd256);
        mr.termite_metal_generated_pipeline_destroy(self.hd512);
        self.* = undefined;
    }

    fn pipeline(self: *const Kernels, params: shared.Params, inputs: []const shared.Ref, output: shared.Ref) !*mr.RawMetalGeneratedPipeline {
        if (params.op != views.abi_version or (params.dim != 256 and params.dim != 512) or
            params.n == 0 or params.n > 8192 or params.groups == 0 or params.stride == 0 or
            params.groups % params.stride != 0 or inputs.len != 7 or
            !std.math.isFinite(params.scale) or params.scale <= 0 or
            !std.math.isFinite(params.epsilon) or params.epsilon < 0) return error.InvalidContextAttention;
        _ = std.math.add(u32, params.position, params.n) catch return error.InvalidContextAttention;
        const elements = @as(u64, params.n) * params.groups * params.dim;
        if (elements > std.math.maxInt(u32) or inputs[0].bytes < elements * 4 or output.bytes < elements * 4)
            return error.MetalBufferRange;
        for (inputs) |input| if (input.handle == output.handle) return error.AliasedContextStaging;
        return if (params.dim == 256) self.hd256 else self.hd512;
    }

    /// Synchronous completion. The caller reads its existing status buffer.
    pub fn dispatch(self: *const Kernels, core: *shared.Kernels, params: shared.Params, inputs: []const shared.Ref, output: shared.Ref) !shared.PairTiming {
        const selected = try self.pipeline(params, inputs, output);
        return core.runThreadgroupsPipeline(selected, params, inputs, output, .{ 1 + (params.n - 1) / 8, params.groups, 1 }, .{ if (params.dim == 256) 128 else 256, 1, 1 });
    }

    /// Encode into a caller-owned frame. Descriptors, status and every buffer
    /// must remain immutable/alive until its owner drains and validates status.
    pub fn dispatchOnActiveFrame(self: *const Kernels, owner: *shared.Kernels, params: shared.Params, inputs: []const shared.Ref, output: shared.Ref) !void {
        return self.dispatchOnActiveRuntime(owner, owner.runtime, params, inputs, output);
    }

    /// Encode into a decoder-owned frame on the same Metal device. The
    /// staging owner keeps dummy buffers alive; the request owner keeps
    /// descriptors/status immutable until its centralized frame completion.
    pub fn dispatchOnActiveRuntime(self: *const Kernels, staging: *shared.Kernels, owner_runtime: *mr.RawMetalDecodeRuntime, params: shared.Params, inputs: []const shared.Ref, output: shared.Ref) !void {
        const selected = try self.pipeline(params, inputs, output);
        return staging.runThreadgroupsPipelineOnActiveRuntime(owner_runtime, selected, params, inputs, output, .{ 1 + (params.n - 1) / 8, params.groups, 1 }, .{ if (params.dim == 256) 128 else 256, 1, 1 });
    }
};

/// Immutable pipelines may be borrowed under a model-owned execution lease.
pub const Execution = Kernels;

fn sample(initial: u32) f32 {
    var i = (initial ^ 61) ^ (initial >> 16);
    i *%= 9;
    i ^= i >> 4;
    i *%= 0x27d4eb2d;
    i ^= i >> 15;
    return (@as(f32, @floatFromInt(i % 2001)) - 1000) / 2000;
}

fn fixture(core: *shared.Kernels, kernels: *const Kernels, dim: u32, query_rows: u32, kv_heads: u32, geometry: u32, nonfinite: u32) !void {
    errdefer std.debug.print("context_flash_fixture: dim={d} query_rows={d} kv_heads={d} geometry={d} nonfinite={d}\n", .{ dim, query_rows, kv_heads, geometry, nonfinite });
    const alloc = std.testing.allocator;
    const p = shared.Params{ .op = 2, .n = query_rows, .dim = dim, .groups = 8, .position = if (geometry == 1) 544 else 160, .stride = kv_heads, .mode = if (geometry >= 2) 32 else 0, .index = kv_heads * 2, .epsilon = if (geometry == 1) 0.7 else 0, .scale = 1 / @sqrt(@as(f32, @floatFromInt(dim))), .theta = @bitCast(@as(u32, if (geometry == 3) 16 else 0)) };
    var heads: [2]views.Head = undefined;
    var spans: [4]views.Span = undefined;
    var capacity: u32 = 0;
    var bias_count: u32 = 0;
    for (0..kv_heads) |h| {
        heads[h] = .{ .kv_start = capacity, .capacity_rows = 0, .bias_start = bias_count, .bias_rows = 0, .span_start = @intCast(h * 2), .span_count = 2 };
        const rows: u32 = switch (geometry) {
            0 => blk: {
                const first: u32 = @intCast(37 - h * 6);
                const total = p.position + query_rows;
                spans[h * 2] = .{ .physical_start = 0, .row_count = first, .logical_start = 0, .logical_count = first };
                spans[h * 2 + 1] = .{ .physical_start = first + 3, .row_count = total - first, .logical_start = first, .logical_count = total - first };
                break :blk total + 3;
            },
            1 => blk: {
                const retained: u32 = @intCast(17 + h * 6);
                const exact = 32 + query_rows;
                spans[h * 2] = .{ .physical_start = 0, .row_count = retained, .logical_start = 0, .logical_count = 512, .kind = .compacted };
                spans[h * 2 + 1] = .{ .physical_start = retained + 3, .row_count = exact, .logical_start = 512, .logical_count = exact };
                heads[h].bias_rows = retained;
                bias_count += retained;
                break :blk retained + 3 + exact;
            },
            3 => blk: {
                const total = 32 + query_rows;
                spans[h * 2] = .{ .physical_start = total + 3, .row_count = 16, .logical_start = 0, .logical_count = 16 };
                spans[h * 2 + 1] = .{ .physical_start = 0, .row_count = total, .logical_start = 128, .logical_count = total };
                break :blk total + 3 + 16;
            },
            else => blk: {
                const first: u32 = @intCast(16 + h * 4);
                const total = 32 + query_rows;
                spans[h * 2] = .{ .physical_start = total + 5 - first, .row_count = first, .logical_start = 128, .logical_count = first };
                spans[h * 2 + 1] = .{ .physical_start = 0, .row_count = total - first, .logical_start = 128 + first, .logical_count = total - first };
                break :blk total + 5;
            },
        };
        heads[h].capacity_rows = rows;
        capacity += rows;
    }
    const view = views.View{ .version = views.abi_version, .epoch = 1, .logical_tokens = p.position + query_rows, .capacity_rows = capacity, .bias_rows = bias_count, .kv_heads = kv_heads, .head_dim = dim, .heads = heads[0..kv_heads], .spans = spans[0 .. kv_heads * 2] };
    _ = try view.validate();
    const ne = query_rows * 8 * dim;
    const ke = capacity * dim;
    const sizes = [_]usize{ ne * 4, ke * 2, ke * 2, @max(1, bias_count) * 4, kv_heads * 2 * @sizeOf(views.Span), 4, kv_heads * @sizeOf(views.Head), ne * 4 };
    var buffers: [8]shared.Buffer = undefined;
    var initialized: usize = 0;
    defer for (buffers[0..initialized]) |*buffer| buffer.deinit();
    for (sizes, 0..) |bytes, i| {
        buffers[i] = try shared.Buffer.init(core.runtime, bytes);
        initialized += 1;
        @memset(buffers[i].bytes[0..bytes], 0);
    }
    const q = buffers[0].floats();
    const k = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(buffers[1].bytes[0..sizes[1]])));
    const v = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(buffers[2].bytes[0..sizes[2]])));
    const bias = buffers[3].floats();
    for (q, 0..) |*value, i| value.* = sample(@as(u32, @intCast(i)) + 100);
    for (k, v, 0..) |*key, *value, i| {
        key.* = @floatCast(sample(@as(u32, @intCast(i)) + 200));
        value.* = @floatCast(sample(@as(u32, @intCast(i)) + 300));
    }
    for (bias[0..bias_count], 0..) |*value, i| value.* = @log(1.5 + sample(@as(u32, @intCast(i)) + 400));
    @memcpy(buffers[4].bytes[0..sizes[4]], std.mem.sliceAsBytes(view.spans));
    @memcpy(buffers[6].bytes[0..sizes[6]], std.mem.sliceAsBytes(view.heads));
    switch (nonfinite) {
        1 => q[0] = std.math.nan(f32),
        2 => k[0] = std.math.inf(f16),
        3 => v[0] = std.math.inf(f16),
        4 => bias[0] = std.math.nan(f32),
        else => {},
    }
    var inputs: [7]shared.Ref = undefined;
    for (&inputs, 0..) |*ref, i| ref.* = buffers[i].ref();
    _ = try kernels.dispatch(core, p, &inputs, buffers[7].ref());
    const status = std.mem.bytesToValue(u32, buffers[5].bytes[0..4]);
    const output = buffers[7].floats();
    if (nonfinite != 0) {
        try std.testing.expect(status != 0);
        for (output) |value| try std.testing.expect(std.math.isFinite(value));
        return;
    }
    try std.testing.expectEqual(@as(u32, 0), status);
    var error_squared: f64 = 0;
    var reference_squared: f64 = 0;
    // Independent scalar FP64 oracle, using the original f32 Q and stored f16
    // K/V. No candidate tiling, matrix accumulation or mask helper is reused.
    const expected = try alloc.alloc(f64, dim);
    defer alloc.free(expected);
    for (0..query_rows) |row| for (0..8) |head| {
        @memset(expected, 0);
        const h = heads[head / (8 / kv_heads)];
        var logits: [512]f64 = undefined;
        var physical: [512]usize = undefined;
        var count: usize = 0;
        var maximum = -std.math.inf(f64);
        for (view.spans[h.span_start..][0..h.span_count]) |span| for (0..span.row_count) |i| {
            const query_position = p.position + row;
            const key_position = span.logical_start + i;
            if (span.kind == .exact and (key_position > query_position or (p.mode != 0 and key_position >= @as(u32, @bitCast(p.theta)) and query_position - key_position >= p.mode))) continue;
            const offset = (h.kv_start + span.physical_start + i) * dim;
            var score: f64 = 0;
            for (0..dim) |d| score += @as(f64, q[(row * 8 + head) * dim + d]) * @as(f64, k[offset + d]);
            score *= p.scale;
            if (p.epsilon > 0) score = @as(f64, p.epsilon) * std.math.tanh(score / p.epsilon);
            if (span.kind == .compacted) score += bias[h.bias_start + span.bias_start + i];
            logits[count] = score;
            physical[count] = offset;
            maximum = @max(maximum, score);
            count += 1;
        };
        var denominator: f64 = 0;
        for (logits[0..count]) |*score| {
            score.* = @exp(score.* - maximum);
            denominator += score.*;
        }
        for (logits[0..count], physical[0..count]) |weight, offset| for (expected, 0..) |*value, d| {
            value.* += weight * @as(f64, v[offset + d]);
        };
        for (expected, 0..) |value, d| {
            const reference = value / denominator;
            const delta = @as(f64, output[(row * 8 + head) * dim + d]) - reference;
            if (@abs(delta) > 0.001) std.debug.print("context_flash_mismatch: row={d} head={d} d={d} actual={d} reference={d}\n", .{ row, head, d, output[(row * 8 + head) * dim + d], reference });
            try std.testing.expect(@abs(delta) <= 0.001);
            error_squared += delta * delta;
            reference_squared += reference * reference;
        }
    };
    try std.testing.expect(@sqrt(error_squared / @max(reference_squared, 1e-30)) <= 0.005);
}

test "context Metal flash ABI2 matches FP64 across widths tails GQA compacted wrapped and protected spans" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var core = try shared.Kernels.initSource(@embedFile("context_storage.metal"), "context_storage");
    defer core.deinit();
    var kernels = try Kernels.init();
    defer kernels.deinit();
    for ([_]u32{ 256, 512 }) |dim| {
        for ([_]u32{ 1, 2 }) |heads| for (0..4) |geometry| for ([_]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 64 }) |rows| {
            try fixture(&core, &kernels, dim, rows, heads, @intCast(geometry), 0);
        };
        for (1..5) |nonfinite| try fixture(&core, &kernels, dim, 8, 2, 1, @intCast(nonfinite));
    }
}
