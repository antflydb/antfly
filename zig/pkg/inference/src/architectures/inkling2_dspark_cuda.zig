// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! DSpark block drafter for the released Inkling 2 Mini checkpoint.
const std = @import("std");
const MmapRegion = @import("../util/c_file.zig").MmapRegion;
const checkpoint = @import("inkling2_checkpoint.zig");
const safetensors = @import("../models/safetensors.zig");
const buffer = @import("../ops/cuda/buffer.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const ctx_mod = @import("../ops/cuda/context.zig");
const Context = ctx_mod.CudaContext;
const Blas = @import("../ops/cuda/cublaslt.zig").CublasLt;
const Arena = @import("../ops/cuda/pinned_input_arena.zig").Arena;
const embedded = @import("../ops/cuda/embedded_module.zig");
const catalog = @import("../tile/cuda_standalone.zig");
const launch_abi = @import("../ops/cuda/launch_abi.zig");
const residual = @import("../ops/cuda/residual_block_mix_bf16.zig");
const Program = embedded.EmbeddedModule(embedded.profile(catalog.block_decoder_bf16).image, catalog.block_decoder_bf16_entries);

pub const block_rows = 8;
pub const proposal_rows = block_rows - 1;
const width = 2304;
const q_heads = 24;
const kv_heads = 8;
const dim = 128;
const q_width = q_heads * dim;
const kv_width = kv_heads * dim;
const inner = 9216;
const rank = 256;
const rnn_input_width = 2 * (rank + width);
const num_layers = 3;
const argmax_tile = 4096;

const LayerWeight = enum { context_norm, input_norm, post_norm, q_norm, k_norm, context_k_norm, q, k, v, context_k, context_v, attention_out, gate, up, down };
const HeadWeight = enum { mask, norm, context_norm, rnn, rnn_bias, previous, latent_out, latent_bias };
const Temp = enum {
    context_mix,
    context_rnn,
    residual,
    normed,
    q,
    attention,
    update,
    gate,
    up,
    base_logits,
    target_logits,
    rnn_input,
    rnn_raw,
    rnn_latent,
    correction,
    tiles,
    token,
    blas,
};

fn Buffers(comptime E: type) type {
    return [@typeInfo(E).@"enum".field_names.len]DeviceBuffer;
}

const Tensor = struct { name: []const u8, shape: []const i64 };

const layer_tensors: std.EnumArray(LayerWeight, Tensor) = .init(.{
    .context_norm = .{ .name = "dflare_norm.weight", .shape = &.{width} },
    .input_norm = .{ .name = "input_layernorm.weight", .shape = &.{width} },
    .post_norm = .{ .name = "post_attention_layernorm.weight", .shape = &.{width} },
    .q_norm = .{ .name = "self_attn.q_norm.weight", .shape = &.{dim} },
    .k_norm = .{ .name = "self_attn.k_norm.weight", .shape = &.{dim} },
    .context_k_norm = .{ .name = "self_attn.k_norm_ctx.weight", .shape = &.{dim} },
    .q = .{ .name = "self_attn.q_proj.weight", .shape = &.{ q_width, width } },
    .k = .{ .name = "self_attn.k_proj.weight", .shape = &.{ kv_width, width } },
    .v = .{ .name = "self_attn.v_proj.weight", .shape = &.{ kv_width, width } },
    .context_k = .{ .name = "self_attn.k_proj_ctx.weight", .shape = &.{ kv_width, width } },
    .context_v = .{ .name = "self_attn.v_proj_ctx.weight", .shape = &.{ kv_width, width } },
    .attention_out = .{ .name = "self_attn.o_proj.weight", .shape = &.{ width, q_width } },
    .gate = .{ .name = "mlp.gate_proj.weight", .shape = &.{ inner, width } },
    .up = .{ .name = "mlp.up_proj.weight", .shape = &.{ inner, width } },
    .down = .{ .name = "mlp.down_proj.weight", .shape = &.{ width, inner } },
});

pub const SharedWeights = struct {
    embed: DeviceBuffer,
    embed_norm: DeviceBuffer,
    unembed: DeviceBuffer,
    vocab: u32,
    padded_vocab: u32,
    logit_cap: f32,
};

fn tensorBytes(reader: *const safetensors.MMapReader, tensor: Tensor) ![]const u8 {
    const info = reader.header.tensors.get(tensor.name) orelse return error.MissingInkling2DraftWeight;
    if (info.dtype != .bf16 or info.encoding != .dense or !std.mem.eql(i64, info.shape, tensor.shape))
        return error.InvalidInkling2DraftWeight;
    return reader.rawTensorBytes(tensor.name);
}

fn region(source: DeviceBuffer, offset: usize, len: usize) DeviceBuffer {
    std.debug.assert(offset <= source.len and len <= source.len - offset);
    return .{ .ptr = source.ptr + offset, .len = len, .borrowed = true };
}

fn rows(source: DeviceBuffer, first: usize, count: usize, columns: usize) DeviceBuffer {
    return region(source, first * columns * 2, count * columns * 2);
}

fn decodeBf16(bytes: []const u8, i: usize) f32 {
    return @bitCast(@as(u32, std.mem.readInt(u16, bytes[i * 2 ..][0..2], .little)) << 16);
}

fn alloc(ctx: *Context, len: usize) !DeviceBuffer {
    return DeviceBuffer.allocWithPurpose(ctx, len, .command_workspace);
}

pub const Draft = struct {
    ctx: *Context,
    shared: SharedWeights,
    eps: f32,
    theta: f32,
    window: u32,
    local: [num_layers]bool,
    taps: [3]u8,
    capacity: u32,
    stage: [4]DeviceBuffer = @splat(.{}),
    mix_weights: [4][4]f32 = undefined,
    layers: [num_layers]Buffers(LayerWeight) = @splat(@splat(.{})),
    kv: [num_layers][2]DeviceBuffer = @splat(@splat(.{})),
    head: Buffers(HeadWeight) = @splat(.{}),
    temps: Buffers(Temp) = @splat(.{}),
    folded: DeviceBuffer = .{},
    fold_bias: DeviceBuffer = .{},
    zero_state: DeviceBuffer = .{},
    result: buffer.HostBuffer = .{},
    program: Program = .{},
    mix: residual.Module = .{},
    position: u32 = 0,
    rnn_side: u32 = 0,

    pub fn load(
        allocator: std.mem.Allocator,
        ctx: *Context,
        blas: *Blas,
        dir: []const u8,
        config: checkpoint.DraftConfig,
        shared: SharedWeights,
        capacity: u32,
        feature_rows: u32,
    ) !Draft {
        if (config.hidden_size != width or config.num_hidden_layers != num_layers or config.num_attention_heads != q_heads or
            config.num_key_value_heads != kv_heads or config.head_dim != dim or config.intermediate_size != inner or
            config.block_size != block_rows or config.markov_rank != rank or !std.mem.eql(u8, config.markov_head_type, "rnn_hidden") or
            config.sliding_window_size == 0 or !std.math.isFinite(config.rms_norm_eps) or config.rms_norm_eps <= 0 or
            !std.math.isFinite(config.rope_theta) or config.rope_theta <= 0 or
            capacity == 0 or capacity > config.max_position_embeddings or feature_rows == 0 or
            config.target_layer_ids[3] != checkpoint.visit_count or shared.padded_vocab < shared.vocab or shared.vocab == 0 or
            !std.math.isFinite(shared.logit_cap) or shared.logit_cap <= 0)
            return error.UnsupportedInkling2DraftConfig;
        for (config.target_layer_ids[0..3], config.target_layer_ids[1..4]) |a, b| {
            if (a >= b) return error.UnsupportedInkling2DraftConfig;
        }
        var local: [num_layers]bool = @splat(false);
        for (config.local_layer_ids) |i| {
            if (i >= num_layers or local[i]) return error.UnsupportedInkling2DraftConfig;
            local[i] = true;
        }
        var self: Draft = .{
            .ctx = ctx,
            .shared = shared,
            .eps = config.rms_norm_eps,
            .theta = config.rope_theta,
            .window = config.sliding_window_size,
            .local = local,
            .taps = config.target_layer_ids[0..3].*,
            .capacity = capacity,
        };
        errdefer self.deinit();

        const path = try std.fs.path.join(allocator, &.{ dir, "mtp.safetensors" });
        defer allocator.free(path);
        var reader = try safetensors.MMapReader.openFileAbsoluteWithOptions(allocator, path, 1024 * 1024 * 1024, safetensors.max_header_size, .{});
        defer reader.deinit();
        try safetensors.validateReader(allocator, &reader);
        const mapping = if (reader.mmap_region) |region_| region_.data else null;
        var upload_read = MmapRegion.SequentialRead.begin(mapping, reader.file_bytes);
        defer upload_read.deinit();
        var arena: Arena = .{};
        defer arena.deinit(ctx);
        const mix_raw = try tensorBytes(&reader, .{ .name = "fuse_weight", .shape = &.{ 4, 4 } });
        for (&self.mix_weights, 0..) |*weights, i| {
            for (weights, 0..) |*alpha, j| {
                alpha.* = decodeBf16(mix_raw, i * 4 + j);
                if (!std.math.isFinite(alpha.*)) return error.InvalidInkling2DraftWeight;
            }
            const max = @max(@max(weights[0], weights[1]), @max(weights[2], weights[3]));
            var total: f32 = 0;
            for (weights) |*alpha| {
                alpha.* = @exp(alpha.* - max);
                total += alpha.*;
            }
            for (weights) |*alpha| alpha.* /= total;
        }
        for (&self.stage) |*plane| plane.* = try alloc(ctx, @as(usize, feature_rows) * width * 2);
        for (&self.layers, &self.kv, 0..) |*layer, *kv, i| {
            var name_buf: [96]u8 = undefined;
            for (std.enums.values(LayerWeight)) |key| {
                var tensor = layer_tensors.get(key);
                tensor.name = try std.fmt.bufPrint(&name_buf, "layers.{d}.{s}", .{ i, tensor.name });
                layer[@backingInt(key)] = try arena.uploadWeight(ctx, try tensorBytes(&reader, tensor));
            }
            for (kv) |*plane| plane.* = try DeviceBuffer.allocWithPurpose(ctx, (@as(usize, capacity) + block_rows) * kv_width * 2, .kv_state);
        }
        const head_tensors: std.EnumArray(HeadWeight, Tensor) = .init(.{
            .mask = .{ .name = "mask_embed", .shape = &.{width} },
            .norm = .{ .name = "norm.weight", .shape = &.{width} },
            .context_norm = .{ .name = "markov_head.dflare_norm.weight", .shape = &.{width} },
            .rnn = .{ .name = "markov_head.rnn_proj.weight", .shape = &.{ 3 * rank, rnn_input_width } },
            .rnn_bias = .{ .name = "markov_head.rnn_proj.bias", .shape = &.{3 * rank} },
            .previous = .{ .name = "markov_head.markov_w1.weight", .shape = &.{ shared.padded_vocab, rank } },
            .latent_out = .{ .name = "markov_head.hidden_out.weight", .shape = &.{ width, rank } },
            .latent_bias = .{ .name = "markov_head.hidden_out.bias", .shape = &.{width} },
        });
        for (std.enums.values(HeadWeight)) |key| {
            const bytes = try tensorBytes(&reader, head_tensors.get(key));
            if (key == .rnn_bias) {
                var bias: [3 * rank]f32 = undefined;
                for (&bias, 0..) |*value, i| value.* = decodeBf16(bytes, i);
                self.head[@backingInt(key)] = try arena.uploadWeight(ctx, std.mem.sliceAsBytes(&bias));
            } else self.head[@backingInt(key)] = try arena.uploadWeight(ctx, bytes);
        }
        const vocab: usize = shared.padded_vocab;
        const work_bytes: std.EnumArray(Temp, usize) = .init(.{
            .context_mix = @as(usize, feature_rows) * width * 2,
            .context_rnn = 2 * width * 2,
            .residual = block_rows * width * 2,
            .normed = block_rows * width * 2,
            .q = block_rows * q_width * 2,
            .attention = block_rows * q_width * 2,
            .update = block_rows * width * 2,
            .gate = block_rows * inner * 2,
            .up = block_rows * inner * 2,
            .base_logits = proposal_rows * vocab * 2,
            .target_logits = block_rows * vocab * 4,
            .rnn_input = rnn_input_width * 2,
            .rnn_raw = rank * 3 * 4,
            .rnn_latent = rank * 2,
            .correction = vocab * 2,
            .tiles = ((vocab - 1) / argmax_tile + 1) * 8 * block_rows,
            .token = 4 * block_rows,
            .blas = 8 * 1024 * 1024,
        });
        for (std.enums.values(Temp)) |key| self.temps[@backingInt(key)] = try alloc(ctx, work_bytes.get(key));
        self.result = try buffer.HostBuffer.alloc(ctx, 4 * block_rows);
        self.folded = try DeviceBuffer.allocWithPurpose(ctx, vocab * rank * 2, .model_cache);
        self.fold_bias = try DeviceBuffer.allocWithPurpose(ctx, vocab * 2, .model_cache);
        const zero: [rank * 2]u8 = @splat(0);
        self.zero_state = try arena.uploadWeight(ctx, &zero);
        try blas.matmulAttentionStridedTyped(ctx, self.folded, shared.unembed, self.weight(.latent_out), self.get(.blas), vocab, rank, width, width, rank, rank, 1, 0, 0, 0, false, false, false, 1, .bf16);
        try self.project(blas, self.fold_bias, self.weight(.latent_bias), shared.unembed, 1, width, vocab);
        try self.program.init(ctx);
        try ctx.synchronize();
        return self;
    }

    pub fn deinit(self: *Draft) void {
        const ctx = self.ctx;
        ctx.synchronize() catch {};
        self.program.deinit(ctx);
        self.mix.deinit(ctx);
        self.result.free(ctx);
        self.zero_state.free(ctx);
        self.fold_bias.free(ctx);
        self.folded.free(ctx);
        for (&self.temps) |*value| value.free(ctx);
        for (&self.head) |*value| value.free(ctx);
        for (&self.kv) |*layer| for (layer) |*value| value.free(ctx);
        for (&self.layers) |*layer| for (layer) |*value| value.free(ctx);
        for (&self.stage) |*value| value.free(ctx);
    }

    fn get(self: *const Draft, key: Temp) DeviceBuffer {
        return self.temps[@backingInt(key)];
    }

    fn weight(self: *const Draft, key: HeadWeight) DeviceBuffer {
        return self.head[@backingInt(key)];
    }

    fn layerWeight(self: *const Draft, i: usize, key: LayerWeight) DeviceBuffer {
        return self.layers[i][@backingInt(key)];
    }

    fn launch(self: *Draft, comptime entry: Program.Entry, grid_x: u32, grid_y: u32, threads: u32, arguments: anytype) !void {
        var abi = launch_abi.addresses(arguments);
        try self.program.launch(self.ctx, entry, grid_x, grid_y, threads, &abi);
    }

    fn project(self: *Draft, blas: *Blas, dst: DeviceBuffer, input: DeviceBuffer, w: DeviceBuffer, count: u32, in_dim: usize, out_dim: usize) !void {
        try blas.matmulBf16WeightBf16Out(self.ctx, dst, input, w, self.get(.blas), count, in_dim, out_dim);
    }

    fn mixFeatures(self: *Draft, out: DeviceBuffer, first: u32, count: u32, i: usize, gamma: DeviceBuffer) !void {
        var output = out.ptr;
        var inputs: [4]u64 = undefined;
        for (&inputs, self.stage) |*ptr, plane| ptr.* = rows(plane, first, count, width).ptr;
        var alpha = self.mix_weights[i];
        var gamma_ptr = gamma.ptr;
        var hidden: u32 = width;
        var eps = self.eps;
        try self.launch(.mix_rms, count, 1, 128, .{ &output, &inputs, &alpha, &gamma_ptr, &hidden, &eps });
    }

    fn normRope(self: *Draft, input: DeviceBuffer, gamma: DeviceBuffer, count: u32, heads: u32, start: u32) !void {
        var data = input.ptr;
        var scale = gamma.ptr;
        var n = heads;
        var position = start;
        var theta = self.theta;
        var eps = self.eps;
        try self.launch(.rope, count, heads, 64, .{ &data, &scale, &n, &position, &theta, &eps });
    }

    /// Target features remain in their staging planes until the target's
    /// stream fence. Only a committed prefix becomes draft context.
    pub fn stageContext(self: *Draft, blas: *Blas, start: u32, count: u32) !void {
        if (start != self.position or count == 0 or count > self.capacity - start or
            @as(usize, count) * width * 2 > self.stage[0].len) return error.InvalidInkling2DraftContext;
        const mix = self.get(.context_mix);
        for (0..num_layers) |i| {
            try self.mixFeatures(mix, 0, count, i, self.layerWeight(i, .context_norm));
            const k = rows(self.kv[i][0], start, count, kv_width);
            const v = rows(self.kv[i][1], start, count, kv_width);
            try self.project(blas, k, mix, self.layerWeight(i, .context_k), count, width, kv_width);
            try self.project(blas, v, mix, self.layerWeight(i, .context_v), count, width, kv_width);
            try self.normRope(k, self.layerWeight(i, .context_k_norm), count, kv_heads, start);
        }
        try self.mixFeatures(rows(self.get(.context_rnn), 1 - self.rnn_side, 1, width), count - 1, 1, num_layers, self.weight(.context_norm));
    }

    pub fn commitContext(self: *Draft, count: u32) void {
        self.position += count;
        self.rnn_side = 1 - self.rnn_side;
    }

    fn addNorm(self: *Draft, gamma: DeviceBuffer) !void {
        var ptrs = [_]u64{ self.get(.normed).ptr, self.get(.residual).ptr, self.get(.update).ptr, gamma.ptr };
        var columns: u32 = width;
        var eps = self.eps;
        try self.launch(.add_rms, block_rows, 1, 128, .{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &columns, &eps });
    }

    fn runBlock(self: *Draft, blas: *Blas, anchor: u32) !void {
        const residual_rows = self.get(.residual);
        const normed = self.get(.normed);
        try self.mix.norm(self.ctx, .{ .width = width }, rows(self.shared.embed, anchor, 1, width), .bf16, self.shared.embed_norm, self.eps, rows(residual_rows, 0, 1, width));
        for (1..block_rows) |i| try rows(residual_rows, i, 1, width).copyFromDevice(self.ctx, self.weight(.mask), width * 2);
        try self.mix.norm(self.ctx, .{ .width = width, .rows = block_rows }, residual_rows, .bf16, self.layerWeight(0, .input_norm), self.eps, normed);
        for (0..num_layers) |i| {
            const q = self.get(.q);
            const k = rows(self.kv[i][0], self.position, block_rows, kv_width);
            const v = rows(self.kv[i][1], self.position, block_rows, kv_width);
            try self.project(blas, q, normed, self.layerWeight(i, .q), block_rows, width, q_width);
            try self.project(blas, k, normed, self.layerWeight(i, .k), block_rows, width, kv_width);
            try self.project(blas, v, normed, self.layerWeight(i, .v), block_rows, width, kv_width);
            try self.normRope(q, self.layerWeight(i, .q_norm), block_rows, q_heads, self.position);
            try self.normRope(k, self.layerWeight(i, .k_norm), block_rows, kv_heads, self.position);

            const first = if (self.local[i]) self.position -| self.window else 0;
            var keys = self.position + block_rows - first;
            var ptrs = [_]u64{ self.get(.attention).ptr, q.ptr, rows(self.kv[i][0], first, keys, kv_width).ptr, rows(self.kv[i][1], first, keys, kv_width).ptr };
            var nq: u32 = q_heads;
            var nk: u32 = kv_heads;
            try self.launch(.attention, block_rows, q_heads, 128, .{ &ptrs[0], &ptrs[1], &ptrs[2], &ptrs[3], &nq, &nk, &keys });
            try self.project(blas, self.get(.update), self.get(.attention), self.layerWeight(i, .attention_out), block_rows, q_width, width);
            try self.addNorm(self.layerWeight(i, .post_norm));

            const gate = self.get(.gate);
            const up = self.get(.up);
            try self.project(blas, gate, normed, self.layerWeight(i, .gate), block_rows, width, inner);
            try self.project(blas, up, normed, self.layerWeight(i, .up), block_rows, width, inner);
            var gate_ptr = gate.ptr;
            var up_ptr = up.ptr;
            var count: u32 = block_rows * inner;
            try self.launch(.swiglu, (count + 255) / 256, 1, 256, .{ &gate_ptr, &up_ptr, &count });
            try self.project(blas, self.get(.update), gate, self.layerWeight(i, .down), block_rows, inner, width);
            try self.addNorm(if (i + 1 == num_layers) self.weight(.norm) else self.layerWeight(i + 1, .input_norm));
        }
    }

    pub fn propose(self: *Draft, blas: *Blas, anchor: u32, count: u32) ![proposal_rows]u32 {
        if (anchor >= self.shared.vocab or self.position == 0 or self.position >= self.capacity or count > proposal_rows or count == 0)
            return error.InvalidInkling2DraftContext;
        try self.runBlock(blas, anchor);
        const normed = self.get(.normed);
        const base = self.get(.base_logits);
        const vocab = self.shared.padded_vocab;
        try self.project(blas, base, rows(normed, 1, count, width), self.shared.unembed, count, width, vocab);
        const input = self.get(.rnn_input);
        const state = region(input, 0, rank * 2);
        const prev = region(input, rank * 2, rank * 2);
        const hidden = region(input, 2 * rank * 2, width * 2);
        const feature = region(input, (2 * rank + width) * 2, width * 2);
        try state.copyFromDevice(self.ctx, self.zero_state, rank * 2);
        try feature.copyFromDevice(self.ctx, rows(self.get(.context_rnn), self.rnn_side, 1, width), width * 2);
        var last = anchor;
        var proposed: [proposal_rows]u32 = @splat(0);
        for (0..count) |i| {
            try prev.copyFromDevice(self.ctx, rows(self.weight(.previous), last, 1, rank), rank * 2);
            try hidden.copyFromDevice(self.ctx, rows(normed, i + 1, 1, width), width * 2);
            const raw = self.get(.rnn_raw);
            try blas.matmulBf16WeightBiasF32Out(self.ctx, raw, input, self.weight(.rnn), self.weight(.rnn_bias), self.get(.blas), 1, rnn_input_width, 3 * rank);
            const latent = self.get(.rnn_latent);
            var ptrs = [_]u64{ state.ptr, latent.ptr, raw.ptr };
            var state_rank: u32 = rank;
            try self.launch(.recurrent, 1, 1, rank, .{ &ptrs[0], &ptrs[1], &ptrs[2], &state_rank });
            const correction = self.get(.correction);
            try blas.matmulBf16WeightBiasBf16Out(self.ctx, correction, latent, self.folded, self.fold_bias, self.get(.blas), 1, rank, vocab);
            last = try self.select(rows(base, i, 1, vocab), correction);
            proposed[i] = last;
        }
        return proposed;
    }

    /// Run the target vocabulary projection for all speculative input rows
    /// together, then read back only their greedy indices.
    pub fn selectTarget(self: *Draft, blas: *Blas, normalized: DeviceBuffer, count: u32) ![block_rows]u32 {
        if (count == 0 or count > block_rows) return error.InvalidInkling2DraftContext;
        const logits = self.get(.target_logits);
        const token = self.get(.token);
        var ptrs = [_]u64{ self.get(.tiles).ptr, logits.ptr, token.ptr };
        var vocab = self.shared.vocab;
        var stride = self.shared.padded_vocab;
        var cap = self.shared.logit_cap;
        var tiles = (vocab - 1) / argmax_tile + 1;
        try blas.matmulBf16WeightF32Out(self.ctx, logits, normalized, self.shared.unembed, self.get(.blas), count, width, stride);
        try self.launch(.target_argmax_tiles, tiles, count, 256, .{ &ptrs[0], &ptrs[1], &vocab, &stride, &cap });
        try self.launch(.argmax_merge, 1, count, 256, .{ &ptrs[2], &ptrs[0], &tiles });
        try token.copyIndexMetadataToHost(self.ctx, self.result.bytes()[0 .. count * 4]);
        try self.ctx.synchronize();
        var ids: [block_rows]u32 = @splat(0);
        for (ids[0..count], 0..) |*id, i| {
            id.* = std.mem.readInt(u32, self.result.constBytes()[i * 4 ..][0..4], .little);
            if (id.* >= self.shared.vocab) return error.NonFiniteInkling2DraftLogit;
        }
        return ids;
    }

    fn select(self: *Draft, base: DeviceBuffer, correction: DeviceBuffer) !u32 {
        const token = self.get(.token);
        var ptrs = [_]u64{ self.get(.tiles).ptr, base.ptr, correction.ptr, token.ptr };
        var vocab = self.shared.vocab;
        var cap = self.shared.logit_cap;
        var tiles = (vocab - 1) / argmax_tile + 1;
        try self.launch(.argmax_tiles, tiles, 1, 256, .{ &ptrs[0], &ptrs[1], &ptrs[2], &vocab, &cap });
        try self.launch(.argmax_merge, 1, 1, 256, .{ &ptrs[3], &ptrs[0], &tiles });
        try token.copyIndexMetadataToHost(self.ctx, self.result.bytes()[0..4]);
        try self.ctx.synchronize();
        const id = std.mem.readInt(u32, self.result.constBytes()[0..4], .little);
        if (id >= self.shared.vocab) return error.NonFiniteInkling2DraftLogit;
        return id;
    }
};
