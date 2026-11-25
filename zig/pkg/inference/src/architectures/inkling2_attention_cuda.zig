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

//! One resident Inkling attention sublayer. The decoder owns its normalized
//! inputs, per-visit convolution history, and the transactional KV append.
const std = @import("std");
const model = @import("inkling2_checkpoint.zig");
const buffer = @import("../ops/cuda/buffer.zig");
const Context = @import("../ops/cuda/context.zig").CudaContext;
const Blas = @import("../ops/cuda/cublaslt.zig").CublasLt;
const dense_matvec = @import("../ops/cuda/dense_bf16_matvec.zig");
const Dense = dense_matvec.Module;
const Arena = @import("../ops/cuda/pinned_input_arena.zig").Arena;
const cache = @import("../ops/cuda/hybrid_paged_kv.zig");
const norm = @import("../ops/cuda/head_norm_bf16.zig");
const conv = @import("../ops/cuda/causal_depthwise4_bf16.zig");
const relative = @import("../ops/cuda/paged_relative_attention.zig");
const DeviceBuffer = buffer.DeviceBuffer;

const Weight = enum { q, k, v, r, sink, gate, out, q_scale, r_scale, k_taps, v_taps, k_dynamic, v_dynamic, relative_basis };
const query_weights = [_]Weight{ .q, .r, .gate, .sink };
const projection_weights = query_weights ++ [_]Weight{ .k, .v };

fn projectionWidth(text: model.TextConfig, weight: Weight) u32 {
    return switch (weight) {
        .q, .gate => @as(u32, text.num_attention_heads) * text.head_dim,
        .r => @as(u32, text.num_attention_heads) * text.d_rel,
        .sink => text.num_attention_heads,
        .k, .v => @as(u32, text.num_key_value_heads) * text.head_dim,
        else => unreachable,
    };
}

fn projectionSize(text: model.TextConfig, weights: []const Weight) u32 {
    var width: u32 = 0;
    for (weights) |weight| width += projectionWidth(text, weight);
    return width;
}

fn view(parent: DeviceBuffer, offset: usize, bytes: usize) DeviceBuffer {
    return .{ .ptr = parent.ptr + offset, .len = bytes, .borrowed = true };
}

const WeightArray = [@typeInfo(Weight).@"enum".field_names.len]DeviceBuffer;

pub const Layer = struct {
    weights: WeightArray = @splat(.{}),
    local: bool,
    packed_projection: DeviceBuffer = .{},
    packed_input_weights: dense_matvec.Packed = .{},
    packed_output_weights: dense_matvec.Packed = .{},

    fn get(self: *const Layer, key: Weight) DeviceBuffer {
        return self.weights[@backingInt(key)];
    }

    pub fn load(allocator: std.mem.Allocator, ctx: *Context, source: *const model.Checkpoint, staging: *Arena, layer: usize) !Layer {
        if (layer >= model.layer_count) return error.InvalidInkling2Layer;
        const text = source.config.value.text_config;
        const local = std.mem.indexOfScalar(u8, &text.local_layer_ids, @intCast(layer)) != null;
        var result: Layer = .{ .local = local };
        errdefer result.deinit(ctx);
        const names = [_][]const u8{ "attn.wq_du.weight", "attn.wr_du.weight", "attn.gate_attn.weight", "attn.attn_sink_proj.weight", "attn.wk_dv.weight", "attn.wv_dv.weight" };
        const packed_bytes = @as(usize, text.hidden_size) * projectionSize(text, &projection_weights) * 2;
        result.packed_projection = try DeviceBuffer.allocWithPurpose(ctx, packed_bytes, .model_cache);
        errdefer ctx.synchronize() catch {};
        var offset: usize = 0;
        for (projection_weights, names) |key, name| {
            const bytes = @as(usize, text.hidden_size) * projectionWidth(text, key) * 2;
            const destination = view(result.packed_projection, offset, bytes);
            const tensor = try source.layerTensor(layer, name);
            try staging.copyWeight(ctx, destination, tensor);
            result.weights[@backingInt(key)] = destination;
            offset += bytes;
        }
        const entries = [_]struct { Weight, []const u8 }{
            .{ .out, "attn.wo_ud.weight" },
            .{ .q_scale, "attn.q_l2_clip.scale" },
            .{ .r_scale, "attn.rel_l2_clip.scale" },
            .{ .k_taps, if (local) "attn.k_sconv.weight" else "attn.k_sconv.static_w" },
            .{ .v_taps, if (local) "attn.v_sconv.weight" else "attn.v_sconv.static_w" },
        };
        for (entries) |entry| result.weights[@backingInt(entry[0])] = try staging.uploadWeight(ctx, try source.layerTensor(layer, entry[1]));
        if (!local) {
            result.weights[@backingInt(Weight.k_dynamic)] = try staging.uploadWeight(ctx, try source.layerTensor(layer, "attn.k_sconv.weight_proj"));
            result.weights[@backingInt(Weight.v_dynamic)] = try staging.uploadWeight(ctx, try source.layerTensor(layer, "attn.v_sconv.weight_proj"));
        }
        const basis = try source.layerTensor(layer, "attn.rel_logits_proj.proj");
        // Checkpoint stores [relative_dim, extent]. The retained BF16 linear
        // consumes weights in [output, input] layout.
        const transposed = try allocator.alloc(u8, basis.len);
        defer allocator.free(transposed);
        for (0..text.d_rel) |d| for (0..text.rel_extent) |relative_index| {
            const from = (d * text.rel_extent + relative_index) * 2;
            const to = (relative_index * text.d_rel + d) * 2;
            @memcpy(transposed[to..][0..2], basis[from..][0..2]);
        };
        result.weights[@backingInt(Weight.relative_basis)] = try staging.uploadWeight(ctx, transposed);
        try ctx.synchronize();
        return result;
    }

    pub fn preparePackedWeights(self: *Layer, ctx: *Context, dense: *Dense, text: model.TextConfig) !void {
        self.packed_input_weights = try dense.tryPack(ctx, self.packed_projection, text.hidden_size, projectionSize(text, &projection_weights), .bf16);
        self.packed_output_weights = try dense.tryPack(ctx, self.get(.out), projectionWidth(text, .q), text.hidden_size, .bf16);
    }

    pub fn deinit(self: *Layer, ctx: *Context) void {
        self.packed_input_weights.deinit(ctx);
        self.packed_output_weights.deinit(ctx);
        for (&self.weights) |*weight| weight.free(ctx);
        self.packed_projection.free(ctx);
    }
};

const Temp = enum { projection, q, q_norm, k, v, k_norm, v_norm, r, r_norm, sink, gate, dynamic, rel_logits, attention };
const TempArray = [@typeInfo(Temp).@"enum".field_names.len]DeviceBuffer;

pub const Workspace = struct {
    backing: DeviceBuffer,
    slices: TempArray,
    rows: u32,

    pub const Layout = struct {
        offsets: [@typeInfo(Temp).@"enum".field_names.len]usize,
        lengths: [@typeInfo(Temp).@"enum".field_names.len]usize,
        bytes: usize,
        rows: u32,
    };

    pub fn layout(text: model.TextConfig, rows: u32) !Layout {
        if (rows == 0 or rows > 256) return error.InvalidShape;
        const q = @as(usize, text.num_attention_heads) * text.head_dim;
        const kv = @as(usize, text.num_key_value_heads) * text.head_dim;
        const rel = @as(usize, text.num_attention_heads) * text.d_rel;
        const widths = [_]usize{
            projectionSize(text, &projection_weights),
            q,
            q,
            kv,
            kv,
            kv,
            kv,
            rel,
            rel,
            text.num_attention_heads,
            q,
            @as(usize, text.num_key_value_heads) * text.sconv_kernel_size,
            @as(usize, text.num_attention_heads) * text.rel_extent,
            q,
        };
        var result: Layout = .{ .offsets = undefined, .lengths = undefined, .bytes = 0, .rows = rows };
        for (widths, &result.offsets, &result.lengths) |width, *offset, *length| {
            offset.* = std.mem.alignForward(usize, result.bytes, 256);
            length.* = @as(usize, rows) * width * 2;
            result.bytes = offset.* + length.*;
        }
        return result;
    }

    pub fn init(ctx: *Context, text: model.TextConfig, rows: u32) !Workspace {
        return allocate(ctx, try layout(text, rows));
    }

    pub fn allocate(ctx: *Context, plan: Layout) !Workspace {
        const backing = try DeviceBuffer.allocWithPurpose(ctx, plan.bytes, .command_workspace);
        var slices: TempArray = undefined;
        for (&slices, plan.lengths, plan.offsets) |*slice, length, offset|
            slice.* = .{ .ptr = backing.ptr + offset, .len = length, .borrowed = true };
        return .{ .backing = backing, .slices = slices, .rows = plan.rows };
    }

    pub fn deinit(self: *Workspace, ctx: *Context) void {
        self.backing.free(ctx);
    }

    /// The multi-row path keeps the unconvolved K/V rows in their own
    /// projection planes through the end of the visit.
    pub fn rawKv(self: Workspace) cache.Planes {
        std.debug.assert(self.rows > 1);
        return .{ .key = self.get(.k), .value = self.get(.v) };
    }

    fn get(self: Workspace, temp: Temp) DeviceBuffer {
        return self.slices[@backingInt(temp)];
    }
};

pub const Input = struct { q: DeviceBuffer, k: DeviceBuffer, v: DeviceBuffer };
pub const Request = struct {
    input: Input,
    matvec: ?*Dense = null,
    use_packed: bool = true,
    output: DeviceBuffer,
    project_output: bool = true,
    workspace: Workspace,
    blas_space: DeviceBuffer,
    attention_space: DeviceBuffer,
    cache_owner: *cache.Cache,
    visit: cache.Visit,
    page_ids_device: DeviceBuffer,
    replay_position: DeviceBuffer = .{},
    old_history: cache.Planes,
    next_history: cache.Planes,
};

const WeightSlice = struct {
    raw: DeviceBuffer,
    encoded: dense_matvec.Packed,
    first_row: u32 = 0,
};

fn projectBf16(ctx: *Context, blas: *Blas, r: Request, output: DeviceBuffer, input: DeviceBuffer, weight: WeightSlice, k: u32, n: u32) !void {
    if (r.workspace.rows == 1) {
        if (r.matvec) |dense| {
            if (r.use_packed and try dense.tryRunPacked(ctx, output, input, weight.encoded, weight.first_row, n, .bf16)) return;
            if (try dense.tryRun(ctx, output, input, weight.raw, k, n, .bf16)) return;
        }
    }
    try blas.matmulBf16WeightBf16Out(ctx, output, input, weight.raw, r.blas_space, r.workspace.rows, k, n);
}

pub const Forward = struct {
    normalization: norm.Module = .{},
    convolution: conv.Module = .{},
    attention: relative.Module = .{},

    pub fn deinit(self: *Forward, ctx: *Context) void {
        self.attention.deinit(ctx);
        self.convolution.deinit(ctx);
        self.normalization.deinit(ctx);
    }

    pub fn projectOutputBatch(dense: *Dense, blas: *Blas, ctx: *Context, text: model.TextConfig, layer: *const Layer, rows: u32, input: DeviceBuffer, output: DeviceBuffer, scratch: DeviceBuffer) !void {
        if (try dense.tryRunPackedBatch(ctx, output, input, layer.packed_output_weights, rows)) return;
        const heads = projectionWidth(text, .q);
        const input_bytes = @as(usize, heads) * 2;
        const output_bytes = @as(usize, text.hidden_size) * 2;
        for (0..rows) |lane| {
            const source = view(input, lane * input_bytes, input_bytes);
            const destination = view(output, lane * output_bytes, output_bytes);
            if (try dense.tryRunPacked(ctx, destination, source, layer.packed_output_weights, 0, text.hidden_size, .bf16)) continue;
            if (try dense.tryRun(ctx, destination, source, layer.get(.out), heads, text.hidden_size, .bf16)) continue;
            try blas.matmulBf16WeightBf16Out(ctx, destination, source, layer.get(.out), scratch, 1, heads, text.hidden_size);
        }
    }

    pub fn run(self: *Forward, ctx: *Context, blas: *Blas, text: model.TextConfig, layer: *const Layer, r: Request) !void {
        if ((r.visit.group == .sliding) != layer.local) return error.InvalidInkling2Layer;
        const replay = if (r.replay_position.ptr != 0) try r.cache_owner.replayBinding(r.visit) else null;
        const position: u32 = if (replay) |binding| @intCast(binding.range.visible_end - 1) else @intCast(r.cache_owner.committedTokens());
        const rows = r.workspace.rows;
        const q_width = @as(u32, text.num_attention_heads) * text.head_dim;
        const kv_width = @as(u32, text.num_key_value_heads) * text.head_dim;
        const query_sources = r.input.q.ptr == r.input.k.ptr and r.input.q.ptr == r.input.v.ptr;
        const output_keys = [_]Temp{ .q, .r, .gate, .sink, .k, .v };
        var projected = r.workspace.slices;
        const fused = rows == 1 and r.matvec != null;
        var skipped: usize = 0;
        if (fused) {
            const packed_output = r.workspace.get(.projection);
            const segmented = !query_sources and r.use_packed and try r.matvec.?.tryRunSegmented(ctx, packed_output, .{
                r.input.q, r.input.k, r.input.v,
            }, layer.packed_input_weights, .{ projectionSize(text, &query_weights), kv_width, kv_width });
            const keys: []const Weight = if (query_sources or segmented) &projection_weights else &query_weights;
            skipped = keys.len;
            if (!segmented) {
                const output_width = projectionSize(text, keys);
                const weights: WeightSlice = .{
                    .raw = view(layer.packed_projection, 0, @as(usize, output_width) * text.hidden_size * 2),
                    .encoded = layer.packed_input_weights,
                };
                try projectBf16(ctx, blas, r, packed_output, r.input.q, weights, text.hidden_size, output_width);
            }
            var offset: usize = 0;
            for (keys, output_keys[0..keys.len]) |weight, key| {
                const bytes = @as(usize, projectionWidth(text, weight)) * 2;
                projected[@backingInt(key)] = view(packed_output, offset, bytes);
                offset += bytes;
            }
        }
        const inputs = [_]DeviceBuffer{ r.input.q, r.input.q, r.input.q, r.input.q, r.input.k, r.input.v };
        var first_row = projectionSize(text, projection_weights[0..skipped]);
        for (projection_weights[skipped..], output_keys[skipped..], inputs[skipped..]) |weight, key, input| {
            const width = projectionWidth(text, weight);
            try projectBf16(ctx, blas, r, projected[@backingInt(key)], input, .{
                .raw = layer.get(weight),
                .encoded = layer.packed_input_weights,
                .first_row = first_row,
            }, text.hidden_size, width);
            first_row += width;
        }
        var query_norm: [2]norm.Request = undefined;
        for ([_]struct { Temp, Temp, Weight, u16, f32 }{
            .{ .q, .q_norm, .q_scale, text.head_dim, text.qk_l2_clip_threshold },
            .{ .r, .r_norm, .r_scale, text.d_rel, text.rel_l2_clip_threshold },
        }, &query_norm) |op, *request| request.* = .{
            .input = projected[@backingInt(op[0])],
            .output = r.workspace.get(op[1]),
            .gamma = layer.get(op[2]),
            .rows = rows,
            .heads = text.num_attention_heads,
            .dim = op[3],
            .position = position,
            .log_floor = if (layer.local) 0 else text.log_scaling_n_floor,
            .log_alpha = text.log_scaling_alpha,
            .clip = op[4],
            .eps = text.rms_norm_eps,
        };
        try self.normalization.runPair(ctx, query_norm[0], query_norm[1]);
        for ([_]struct { Temp, Temp, Weight, Weight, DeviceBuffer, DeviceBuffer, DeviceBuffer, f32 }{
            .{ .k, .k_norm, .k_taps, .k_dynamic, r.input.k, r.old_history.key, r.next_history.key, text.qk_l2_clip_threshold },
            .{ .v, .v_norm, .v_taps, .v_dynamic, r.input.v, r.old_history.value, r.next_history.value, 0 },
        }) |op| {
            const dynamic = if (layer.local) DeviceBuffer{} else r.workspace.get(.dynamic);
            if (!layer.local) try blas.matmulBf16WeightBf16Out(ctx, dynamic, op[4], layer.get(op[3]), r.blas_space, rows, text.hidden_size, @as(u32, text.num_key_value_heads) * text.sconv_kernel_size);
            try self.convolution.runHeadNorm(ctx, .{
                .input = projected[@backingInt(op[0])],
                .weight = layer.get(op[2]),
                .history = op[5],
                .dynamic_taps = dynamic,
                .output = r.workspace.get(op[1]),
                .next_history = op[6],
                .rows = rows,
                .channels = kv_width,
                .head_size = text.head_dim,
                .valid_history = @min(3, position),
                .output_scale = if (layer.local) 1.0 / @sqrt(@as(f32, 5.0)) else 1.0 / 3.0,
            }, op[7], text.rms_norm_eps);
        }
        try blas.matmulBf16WeightBf16Out(ctx, r.workspace.get(.rel_logits), r.workspace.get(.r_norm), layer.get(.relative_basis), r.blas_space, @as(usize, rows) * text.num_attention_heads, text.d_rel, text.rel_extent);
        const staged: cache.Planes = .{ .key = r.workspace.get(.k_norm), .value = r.workspace.get(.v_norm) };
        const binding = if (replay) |binding| blk: {
            try self.attention.appendReplay(ctx, binding, r.replay_position, r.page_ids_device, staged, text.num_key_value_heads);
            break :blk binding;
        } else blk: {
            try r.cache_owner.queueWrite(r.visit, staged);
            break :blk try r.cache_owner.stagedBinding(r.visit);
        };
        try self.attention.run(ctx, .{
            .geometry = .{
                .rows = rows,
                .start = position,
                .q_heads = text.num_attention_heads,
                .kv_heads = text.num_key_value_heads,
                .window = if (layer.local) text.sliding_window_size else 0,
                .relative_extent = text.rel_extent,
                .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(text.head_dim))),
            },
            .kv = binding,
            .device_page_ids = r.page_ids_device,
            .replay_position = r.replay_position,
            .query = r.workspace.get(.q_norm),
            .relative = r.workspace.get(.rel_logits),
            .sinks = projected[@backingInt(Temp.sink)],
            .gate = projected[@backingInt(Temp.gate)],
            .scratch = r.attention_space,
            .output = if (r.project_output) r.workspace.get(.attention) else r.output,
        });
        if (r.project_output) try projectBf16(ctx, blas, r, r.output, r.workspace.get(.attention), .{
            .raw = layer.get(.out),
            .encoded = layer.packed_output_weights,
        }, q_width, text.hidden_size);
    }
};
