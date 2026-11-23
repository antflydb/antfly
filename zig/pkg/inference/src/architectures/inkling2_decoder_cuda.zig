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

//! Resident fixed-loop text decoder with request-private CUDA state.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const MmapRegion = @import("../util/c_file.zig").MmapRegion;
const attention = @import("inkling2_attention_cuda.zig");
const moe = @import("inkling2_moe_cuda.zig");
const buffer = @import("../ops/cuda/buffer.zig");
const cuda_graph = @import("../ops/cuda/graph.zig");
const Context = @import("../ops/cuda/context.zig").CudaContext;
const Blas = @import("../ops/cuda/cublaslt.zig").CublasLt;
const dense_matvec = @import("../ops/cuda/dense_bf16_matvec.zig");
const Dense = dense_matvec.Module;
const Arena = @import("../ops/cuda/pinned_input_arena.zig").Arena;
const cache = @import("../ops/cuda/hybrid_paged_kv.zig");
const convolution = @import("../ops/cuda/causal_depthwise4_bf16.zig");
const relative = @import("../ops/cuda/paged_relative_attention.zig");
const residual = @import("../ops/cuda/residual_block_mix_bf16.zig");
const DeviceBuffer = buffer.DeviceBuffer;
const HostBuffer = buffer.HostBuffer;

const streams = 4;
const block_slots = 5;
const page_tokens = 16;
const Stream = enum { attn, k, v, mlp };

const ModelWeight = enum {
    embed,
    unembed,
    embed_norm,
    final_norm,
    output_query,
    cross_queries,
    cross_kv_queries,

    fn name(self: ModelWeight) []const u8 {
        return switch (self) {
            .embed => "model.llm.embed.weight",
            .unembed => "model.llm.unembed.weight",
            .embed_norm => "model.llm.embed_norm.weight",
            .final_norm => "model.llm.norm.weight",
            .output_query => "model.llm.attn_res.output_query",
            .cross_queries => "model.llm.middle_cycle_cross_pass_queries",
            .cross_kv_queries => "model.llm.middle_cycle_cross_pass_kv_queries",
        };
    }
};
const LayerWeight = enum { q_norm, k_norm, v_norm, mlp_norm, attn_taps, mlp_taps, attn_query, mlp_query };
const ControllerWeight = enum { down, gain_forget };

fn Weights(comptime Kind: type) type {
    return [@typeInfo(Kind).@"enum".field_names.len]DeviceBuffer;
}

fn sub(parent: DeviceBuffer, offset: usize, length: usize) DeviceBuffer {
    std.debug.assert(offset <= parent.len and length <= parent.len - offset);
    return .{ .ptr = parent.ptr + offset, .len = length, .borrowed = true };
}

fn row(parent: DeviceBuffer, index: usize, bytes: usize) DeviceBuffer {
    return sub(parent, index * bytes, bytes);
}

const Layer = struct {
    attn: attention.Layer,
    ffn: moe.Layer,
    weights: Weights(LayerWeight) = @splat(.{}),
    controllers: [streams]Weights(ControllerWeight) = @splat(@splat(.{})),
    attention_controller_down: DeviceBuffer = .{},

    fn load(allocator: std.mem.Allocator, ctx: *Context, source: *const checkpoint.Checkpoint, arena: *Arena, layer: usize) !Layer {
        var attn = try attention.Layer.load(allocator, ctx, source, arena, layer);
        const ffn = moe.Layer.load(ctx, source, arena, layer) catch |err| {
            attn.deinit(ctx);
            return err;
        };
        var result: Layer = .{ .attn = attn, .ffn = ffn };
        errdefer result.deinit(ctx);
        for ([_]struct { LayerWeight, []const u8 }{
            .{ .q_norm, if (attn.local) "attn_norm.weight" else "q_input_norm.weight" },
            .{ .k_norm, "k_input_norm.weight" },
            .{ .v_norm, "v_input_norm.weight" },
            .{ .mlp_norm, "mlp_norm.weight" },
            .{ .attn_taps, "attn_sconv.weight" },
            .{ .mlp_taps, "mlp_sconv.weight" },
        }) |entry| {
            if (attn.local and (entry[0] == .k_norm or entry[0] == .v_norm)) continue;
            result.weights[@backingInt(entry[0])] = try arena.uploadWeight(ctx, try source.layerTensor(layer, entry[1]));
        }
        var name: [128]u8 = undefined;
        if (layer != 0) {
            const key = try std.fmt.bufPrint(&name, "model.llm.attn_res.attn_queries.{d}", .{layer - 1});
            result.weights[@backingInt(LayerWeight.attn_query)] = try arena.uploadWeight(ctx, try source.reader.rawTensorBytes(key));
        }
        const key = try std.fmt.bufPrint(&name, "model.llm.attn_res.mlp_queries.{d}", .{layer});
        result.weights[@backingInt(LayerWeight.mlp_query)] = try arena.uploadWeight(ctx, try source.reader.rawTensorBytes(key));
        const text = source.config.value.text_config;
        const core = text.loop_config;
        if (layer >= core.core_start and layer < core.core_end) {
            const down_bytes = 4 * @as(usize, text.hidden_size) * core.gate_rank;
            result.attention_controller_down = try DeviceBuffer.allocWithPurpose(ctx, 3 * down_bytes, .model_cache);
            errdefer ctx.synchronize() catch {};
            inline for (std.enums.values(Stream)) |stream| {
                const controller = &result.controllers[@backingInt(stream)];
                const down_key = try std.fmt.bufPrint(&name, "controllers.{s}.down_weight", .{@tagName(stream)});
                const down_data = try source.layerTensor(layer, down_key);
                if (stream == .mlp) {
                    controller[@backingInt(ControllerWeight.down)] = try arena.uploadWeight(ctx, down_data);
                } else {
                    const destination = row(result.attention_controller_down, @backingInt(stream), down_bytes);
                    try arena.copyWeight(ctx, destination, down_data);
                    controller[@backingInt(ControllerWeight.down)] = destination;
                }
                const each_bytes = @as(usize, text.hidden_size) * core.gate_rank * 2;
                const gate_weights = try DeviceBuffer.allocWithPurpose(ctx, 2 * each_bytes, .model_cache);
                controller[@backingInt(ControllerWeight.gain_forget)] = gate_weights;
                errdefer ctx.synchronize() catch {};
                for ([_][]const u8{ "gain_weight", "forget_weight" }, 0..) |projection, index| {
                    const suffix = try std.fmt.bufPrint(&name, "controllers.{s}.{s}", .{ @tagName(stream), projection });
                    try arena.copyWeight(ctx, row(gate_weights, index, each_bytes), try source.layerTensor(layer, suffix));
                }
            }
        }
        try ctx.synchronize();
        return result;
    }

    fn deinit(self: *Layer, ctx: *Context) void {
        for (&self.controllers) |*controller| for (controller) |*allocation| allocation.free(ctx);
        self.attention_controller_down.free(ctx);
        for (&self.weights) |*allocation| allocation.free(ctx);
        self.ffn.deinit(ctx);
        self.attn.deinit(ctx);
    }

    fn weight(self: *const Layer, key: LayerWeight) DeviceBuffer {
        return self.weights[@backingInt(key)];
    }
};

pub const Decoder = struct {
    ctx: *Context,
    blas: Blas,
    text: checkpoint.TextConfig,
    schedule: [checkpoint.visit_count]checkpoint.Visit,
    expert_activation_precision: moe.ActivationPrecision = .block_scaled,
    layers: [checkpoint.layer_count]Layer = undefined,
    loaded_layers: usize = 0,
    weights: Weights(ModelWeight) = @splat(.{}),
    attn: attention.Forward = .{},
    matvec: Dense = .{},
    packed_head: dense_matvec.Packed = .{},
    head_shortlist: dense_matvec.Shortlist = .{},
    ffn: moe.Forward = .{},
    conv: convolution.Module = .{},
    mix: residual.Module = .{},

    pub const LoadOptions = struct {
        quantized_expert_activations: bool = false,
        pack_attention_weights: bool = false,
        retain_raw_head: bool = false,
        prepare_greedy: bool = false,
    };

    pub fn load(allocator: std.mem.Allocator, ctx: *Context, source: *const checkpoint.Checkpoint, options: LoadOptions) !Decoder {
        var model: Decoder = .{ .ctx = ctx, .blas = try Blas.openWithAllocator(allocator, ctx), .text = source.config.value.text_config, .schedule = source.schedule };
        // The packed BF16 expert path is qualified on GB10.
        if (!options.quantized_expert_activations and ctx.info.compute_major == 12 and ctx.info.compute_minor == 1)
            model.expert_activation_precision = .bf16;
        // The config tag belongs to the parsed checkpoint; all other text
        // configuration is stored by value.
        model.text.model_type = "inkling2_loop_text";
        model.blas.attention_plan_cache_enabled = true;
        errdefer model.deinit();
        var arena: Arena = .{};
        defer arena.deinit(ctx);
        // Resident loading scans almost the entire shard. Restore readahead
        // for the upload, then return the mapping to its default policy.
        const mapping = if (source.reader.mmap_region) |region| region.data else null;
        var upload_read = MmapRegion.SequentialRead.begin(mapping, source.reader.file_bytes);
        defer upload_read.deinit();
        inline for (std.enums.values(ModelWeight)) |key| {
            model.weights[@backingInt(key)] = try arena.uploadWeight(ctx, try source.reader.rawTensorBytes(key.name()));
        }
        for (&model.layers, 0..) |*layer, i| {
            layer.* = try Layer.load(allocator, ctx, source, &arena, i);
            model.loaded_layers += 1;
            try model.ffn.prepareResident(ctx, model.text, &layer.ffn);
        }
        model.packed_head = try model.matvec.tryPack(ctx, model.weight(.unembed), model.text.hidden_size, model.text.unpadded_vocab_size, .f32);
        if (options.prepare_greedy and model.text.final_logit_softcapping >= 0x1p-16 and model.text.final_logit_softcapping <= 0x1p16) {
            model.head_shortlist = try model.matvec.tryPrepareShortlist(ctx, model.weight(.unembed), model.packed_head);
        }
        if (model.packed_head.storage.ptr != 0 and !options.retain_raw_head) model.weights[@backingInt(ModelWeight.unembed)].free(ctx);
        if (options.pack_attention_weights) {
            for (&model.layers) |*layer| try layer.attn.preparePackedWeights(ctx, &model.matvec, model.text);
        }
        return model;
    }

    pub fn deinit(self: *Decoder) void {
        const ctx = self.ctx;
        self.mix.deinit(ctx);
        self.conv.deinit(ctx);
        self.ffn.deinit(ctx);
        self.attn.deinit(ctx);
        self.matvec.deinit(ctx);
        self.head_shortlist.deinit(ctx);
        self.packed_head.deinit(ctx);
        for (self.layers[0..self.loaded_layers]) |*layer| layer.deinit(ctx);
        for (&self.weights) |*allocation| allocation.free(ctx);
        self.blas.deinit();
    }

    fn weight(self: *const Decoder, key: ModelWeight) DeviceBuffer {
        return self.weights[@backingInt(key)];
    }
};

const Temp = enum { stack, prelude, bank, inputs, mix, message, pair, rank, gain_forget, raw, attn, mlp, normalized, logits, blas, relative, router, routed, pages, position };

/// A request is bound to one CUDA stream. Initialize it in place and keep it
/// at that address until deinit so the cache can retain its internal owners.
pub const Request = struct {
    model: *Decoder,
    kv: cache.Cache = undefined,
    workspace: attention.Workspace = undefined,
    pages: HostBuffer = .{},
    replay_position: HostBuffer = .{},
    replay_graphs: [2]cuda_graph.CapturedGraph = @splat(.{}),
    replay_enabled: bool = false,
    logits: HostBuffer = .{},
    shortlist_workspace: DeviceBuffer = .{},
    history: DeviceBuffer = .{},
    temps: Weights(Temp) = @splat(.{}),
    history_side: usize = 0,
    /// Opt in to a different BF16 dot-product reduction order.
    use_dense: bool = false,
    use_reference_head: bool = false,
    use_packed_weights: bool = true,
    use_separate_controllers: bool = false,

    pub fn init(self: *Request, allocator: std.mem.Allocator, model: *Decoder, max_tokens: u32) !void {
        const text = model.text;
        if (max_tokens == 0 or max_tokens > text.model_max_length) return error.InvalidInkling2Context;
        self.* = .{
            .model = model,
            .replay_enabled = model.ctx.info.compute_major == 12 and model.ctx.info.compute_minor <= 1 and
                max_tokens >= 5 and max_tokens <= text.sliding_window_size,
        };
        const ctx = model.ctx;
        const controller_rows: usize = if (supportsGroupedControllers(ctx)) 3 else 1;
        try self.kv.init(allocator, ctx, .{
            .layout = .{ .page_tokens = page_tokens, .window_tokens = text.sliding_window_size, .max_tokens = max_tokens, .max_append_tokens = 1 },
            .full_slots = checkpoint.full_slot_count,
            .sliding_slots = checkpoint.sliding_slot_count,
            .kv_heads = text.num_key_value_heads,
            .head_dim = text.head_dim,
        });
        errdefer self.kv.deinit() catch {};
        self.workspace = try attention.Workspace.init(ctx, text, 1);
        errdefer self.workspace.deinit(ctx);
        errdefer self.freeBuffers();
        const hidden = @as(usize, text.hidden_size) * 2;
        const kv = @as(usize, text.num_key_value_heads) * text.head_dim * 2;
        const ids = (std.math.divCeil(usize, max_tokens, page_tokens) catch unreachable) * 4;
        const nv = try moe.scratchBytes(.nvfp4, text, 1);
        const mx = try moe.scratchBytes(.mxfp8, text, 1);
        const relative_bytes = try relative.scratchBytes(.{
            .rows = 1,
            .start = max_tokens - 1,
            .q_heads = text.num_attention_heads,
            .kv_heads = text.num_key_value_heads,
            .relative_extent = text.rel_extent,
            .score_scale = 1,
        }, 0, max_tokens);
        self.history = try DeviceBuffer.allocWithPurpose(ctx, 2 * checkpoint.visit_count * 6 * (hidden + kv), .kv_state);
        self.pages = try HostBuffer.alloc(ctx, 2 * ids);
        if (self.replay_enabled) {
            self.replay_position = try HostBuffer.alloc(ctx, 4);
            self.temps[@backingInt(Temp.position)] = try DeviceBuffer.allocWithPurpose(ctx, 4, .command_workspace);
        }
        self.logits = try HostBuffer.alloc(ctx, @as(usize, text.unpadded_vocab_size) * 4);
        for ([_]struct { Temp, usize }{
            .{ .stack, block_slots * hidden },               .{ .prelude, hidden },                         .{ .bank, 2 * hidden },                   .{ .inputs, 3 * hidden },
            .{ .mix, 2 * hidden * controller_rows },         .{ .message, 2 * hidden * controller_rows },   .{ .pair, 2 * hidden * controller_rows }, .{ .rank, @as(usize, text.loop_config.gate_rank) * 2 * controller_rows },
            .{ .gain_forget, 4 * hidden * controller_rows }, .{ .raw, hidden },                             .{ .attn, hidden },                       .{ .mlp, hidden },
            .{ .normalized, hidden },                        .{ .logits, @as(usize, text.vocab_size) * 4 }, .{ .blas, 8 * 1024 * 1024 },              .{ .relative, relative_bytes },
            .{ .router, @max(nv.logits, mx.logits) },        .{ .routed, @max(nv.routed, mx.routed) },      .{ .pages, 2 * ids },
        }) |item| self.temps[@backingInt(item[0])] = try DeviceBuffer.allocWithPurpose(ctx, item[1], .command_workspace);
        if (model.head_shortlist.workspaceBytes() != 0) {
            self.shortlist_workspace = DeviceBuffer.allocWithPurpose(ctx, model.head_shortlist.workspaceBytes(), .command_workspace) catch |err| switch (err) {
                error.CudaMemoryLimitExceeded => .{},
                else => return err,
            };
        }
    }

    pub fn deinit(self: *Request) !void {
        const ctx = self.model.ctx;
        try ctx.synchronize();
        for (&self.replay_graphs) |*owner| try owner.deinitChecked(ctx);
        try self.kv.deinit();
        self.freeBuffers();
        self.workspace.deinit(self.model.ctx);
    }

    fn freeBuffers(self: *Request) void {
        const ctx = self.model.ctx;
        for (&self.temps) |*temp| temp.free(ctx);
        self.history.free(ctx);
        self.shortlist_workspace.free(ctx);
        self.logits.free(ctx);
        self.pages.free(ctx);
        self.replay_position.free(ctx);
    }

    pub fn position(self: *const Request) usize {
        return self.kv.committedTokens();
    }

    fn get(self: *const Request, temp: Temp) DeviceBuffer {
        return self.temps[@backingInt(temp)];
    }

    fn shape(self: *const Request) residual.Shape {
        return .{ .width = self.model.text.hidden_size };
    }

    fn block(self: *const Request, index: usize) DeviceBuffer {
        return row(self.get(.stack), index, @as(usize, self.model.text.hidden_size) * 2);
    }

    fn pageTable(self: *const Request, group: cache.Group) DeviceBuffer {
        const storage = self.get(.pages);
        return row(storage, @intFromBool(group == .full), storage.len / 2);
    }

    fn publishPages(self: *Request) !void {
        const capacity = self.pages.len / 2;
        for ([_]cache.Group{ .sliding, .full }, 0..) |group, index| {
            const host: []align(@alignOf(u32)) u8 = @alignCast(self.pages.bytes()[index * capacity ..][0..capacity]);
            const count = try self.kv.stagedPageIds(group, std.mem.bytesAsSlice(u32, host));
            try self.pageTable(group).copyFromHost(self.model.ctx, host[0 .. count * @sizeOf(u32)]);
        }
    }

    fn histories(self: *const Request, visit: usize, side: usize) [4]DeviceBuffer {
        const text = self.model.text;
        const kv = 6 * @as(usize, text.num_key_value_heads) * text.head_dim;
        const hidden = 6 * @as(usize, text.hidden_size);
        const base = (side * checkpoint.visit_count + visit) * 2 * (kv + hidden);
        return .{
            sub(self.history, base, kv),              sub(self.history, base + kv, kv),
            sub(self.history, base + 2 * kv, hidden), sub(self.history, base + 2 * kv + hidden, hidden),
        };
    }

    fn hiddenConv(self: *Request, weight: DeviceBuffer, old: DeviceBuffer, next: DeviceBuffer, output: DeviceBuffer) !void {
        try self.model.conv.run(self.model.ctx, .{
            .rows = 1,
            .channels = self.model.text.hidden_size,
            .head_size = self.model.text.hidden_size,
            .input = self.get(.raw),
            .weight = weight,
            .history = old,
            .next_history = next,
            .output = output,
            .valid_history = @intCast(@min(3, self.position())),
            .output_scale = 1.0 / @sqrt(@as(f32, 5.0)),
        });
    }

    fn supportsGroupedControllers(ctx: *const Context) bool {
        return ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1;
    }

    fn controllerQuery(self: *const Request, layer_id: usize, stream: Stream) DeviceBuffer {
        const text = self.model.text;
        const key: ModelWeight = if (stream == .k or stream == .v) .cross_kv_queries else .cross_queries;
        const lane: usize = @intFromBool(stream == .v or stream == .mlp);
        return row(self.model.weight(key), 2 * (layer_id - text.loop_config.core_start) + lane, @as(usize, text.hidden_size) * 4);
    }

    fn projectControllerDown(self: *Request, layer: *const Layer, stream: Stream, pair: DeviceBuffer, hidden: DeviceBuffer) !void {
        const m = self.model;
        const weights = layer.controllers[@backingInt(stream)];
        try m.blas.matmulBf16WeightBf16Out(m.ctx, hidden, pair, weights[@backingInt(ControllerWeight.down)], self.get(.blas), 1, 2 * @as(usize, m.text.hidden_size), m.text.loop_config.gate_rank);
    }

    fn projectController(self: *Request, layer: *const Layer, stream: Stream, pair: DeviceBuffer, gate: DeviceBuffer) !void {
        const m = self.model;
        const text = m.text;
        const ctx = m.ctx;
        const width = text.hidden_size;
        const rank = text.loop_config.gate_rank;
        const bytes = @as(usize, width) * 2;
        const hidden = self.get(.rank);
        const blas_space = self.get(.blas);
        const weights = layer.controllers[@backingInt(stream)];
        try self.projectControllerDown(layer, stream, pair, hidden);
        const gate_weight = weights[@backingInt(ControllerWeight.gain_forget)];
        if (self.use_dense) {
            if (!try m.matvec.tryRun(ctx, gate, hidden, gate_weight, rank, @as(u32, width) * 2, .f32))
                try m.blas.matmulBf16WeightF32Out(ctx, gate, hidden, gate_weight, blas_space, 1, rank, @as(usize, width) * 2);
        } else {
            const weight_bytes = @as(usize, rank) * bytes;
            try m.blas.matmulBf16WeightF32Out(ctx, row(gate, 0, bytes * 2), hidden, row(gate_weight, 0, weight_bytes), blas_space, 1, rank, width);
            try m.blas.matmulBf16WeightF32Out(ctx, row(gate, 1, bytes * 2), hidden, row(gate_weight, 1, weight_bytes), blas_space, 1, rank, width);
        }
    }

    fn controller(self: *Request, layer: *const Layer, layer_id: usize, stream: Stream, gamma: DeviceBuffer, output: DeviceBuffer) !void {
        const m = self.model;
        const bytes = @as(usize, m.text.hidden_size) * 2;
        const bank = self.get(.bank);
        const input = self.get(.mix);
        const msg = self.get(.message);
        const pair = self.get(.pair);
        const gate = self.get(.gain_forget);
        try m.mix.mixPair(m.ctx, self.shape(), row(bank, 0, bytes), row(bank, 1, bytes), self.controllerQuery(layer_id, stream), input, msg, pair);
        try self.projectController(layer, stream, pair, gate);
        try m.mix.blend(m.ctx, self.shape(), input, msg, row(gate, 0, bytes * 2), row(gate, 1, bytes * 2), gamma, m.text.rms_norm_eps, output);
    }

    fn groupControllers(self: *Request, layer: *const Layer, layer_id: usize, gammas: [3]DeviceBuffer) !void {
        const m = self.model;
        const bytes = @as(usize, m.text.hidden_size) * 2;
        const bank = self.get(.bank);
        const dest = self.get(.mix);
        const messages = self.get(.message);
        const pairs = self.get(.pair);
        const gates = self.get(.gain_forget);
        const attention_streams = [_]Stream{ .attn, .k, .v };
        var queries: [3]DeviceBuffer = undefined;
        for (attention_streams, &queries) |stream, *query| query.* = self.controllerQuery(layer_id, stream);
        const group: residual.RowGroup = .{ .width = m.text.hidden_size, .broadcast_destination = layer.attn.local };
        try m.mix.mixThreePairs(m.ctx, group, row(bank, 0, bytes), row(bank, 1, bytes), queries, dest, messages, pairs);
        if (self.use_dense and m.text.loop_config.gate_rank == 64) {
            const rank_rows = self.get(.rank);
            const rank: usize = 64;
            const columns = @as(usize, m.text.hidden_size) * 2;
            const batched = blk: {
                m.blas.matmulAttentionStridedTyped(
                    m.ctx,
                    rank_rows,
                    pairs,
                    layer.attention_controller_down,
                    self.get(.blas),
                    1,
                    rank,
                    columns,
                    columns,
                    columns,
                    rank,
                    3,
                    columns,
                    columns * rank,
                    rank,
                    false,
                    true,
                    false,
                    1.0,
                    .bf16,
                ) catch |err| switch (err) {
                    error.CublasLtUnsupported => break :blk false,
                    else => return err,
                };
                break :blk true;
            };
            var weights: [3]DeviceBuffer = undefined;
            for (attention_streams, 0..) |stream, i| {
                if (!batched) try self.projectControllerDown(layer, stream, row(pairs, i, 2 * bytes), row(rank_rows, i, rank * 2));
                weights[i] = layer.controllers[@backingInt(stream)][@backingInt(ControllerWeight.gain_forget)];
            }
            try m.matvec.runThreeF32x64(m.ctx, gates, rank_rows, weights, @as(u32, m.text.hidden_size) * 2);
        } else {
            for (attention_streams, 0..) |stream, i| try self.projectController(layer, stream, row(pairs, i, 2 * bytes), row(gates, i, 4 * bytes));
        }
        try m.mix.blendThree(m.ctx, group, dest, messages, gates, gammas, m.text.rms_norm_eps, self.get(.inputs));
    }

    fn runVisit(self: *Request, visit: checkpoint.Visit, ordinal: usize, stack_depth: usize, replay: DeviceBuffer) !usize {
        const m = self.model;
        const ctx = m.ctx;
        const text = m.text;
        const i = visit.weight_layer;
        const layer = &m.layers[i];
        const looped = visit.pass > 0;
        const partial = self.block(stack_depth);
        const width = @as(usize, text.hidden_size) * 2;
        const eps = text.rms_norm_eps;
        const grouped = looped and !self.use_separate_controllers and supportsGroupedControllers(ctx);
        var input: [3]DeviceBuffer = undefined;
        var gammas: [3]DeviceBuffer = undefined;
        for ([_]Stream{ .attn, .k, .v }, [_]LayerWeight{ .q_norm, .k_norm, .v_norm }, 0..) |stream, norm_key, channel| {
            if (layer.attn.local and channel > 0 and !looped) {
                input[channel] = input[0];
                continue;
            }
            const gamma = layer.weight(if (layer.attn.local) .q_norm else norm_key);
            input[channel] = row(self.get(.inputs), channel, width);
            if (i == 0) {
                try m.mix.norm(ctx, self.shape(), partial, .bf16, gamma, eps, input[channel]);
                continue;
            }
            if (looped) {
                gammas[channel] = gamma;
                if (!layer.attn.local or channel == 0) {
                    const query = row(layer.weight(.attn_query), channel, width * 2);
                    const mixed = if (grouped) row(self.get(.mix), channel, width * 2) else self.get(.mix);
                    try m.mix.mix(ctx, self.shape(), self.get(.stack), partial, .{}, @intCast(stack_depth + 1), query, mixed);
                }
                if (!grouped) try self.controller(layer, i, stream, gamma, input[channel]);
            } else {
                const query = row(layer.weight(.attn_query), channel, width * 2);
                try m.mix.mixAndNorm(ctx, self.shape(), self.get(.stack), partial, .{}, @intCast(stack_depth + 1), query, gamma, eps, input[channel]);
            }
        }
        if (grouped) try self.groupControllers(layer, i, gammas);
        const old = self.histories(ordinal, self.history_side);
        const next = self.histories(ordinal, 1 - self.history_side);
        try m.attn.run(ctx, &m.blas, text, &layer.attn, .{
            .matvec = if (self.use_dense) &m.matvec else null,
            .use_packed = self.use_packed_weights,
            .input = .{ .q = input[0], .k = input[1], .v = input[2] },
            .output = self.get(.raw),
            .workspace = self.workspace,
            .blas_space = self.get(.blas),
            .attention_space = self.get(.relative),
            .cache_owner = &self.kv,
            .visit = .{ .group = visit.group, .slot = visit.slot },
            .page_ids_device = self.pageTable(visit.group),
            .replay_position = replay,
            .old_history = .{ .key = old[0], .value = old[1] },
            .next_history = .{ .key = next[0], .value = next[1] },
        });
        const attn = self.get(.attn);
        try self.hiddenConv(layer.weight(.attn_taps), old[2], next[2], attn);
        const boundary = i % text.n_layers_per_attn_res_block == 0;
        const completed = stack_depth + @intFromBool(boundary);
        const previous = if (boundary) DeviceBuffer{} else partial;
        const mlp_input = self.get(.normalized);
        if (looped) {
            try m.mix.mix(ctx, self.shape(), self.get(.stack), attn, previous, @intCast(completed + 1), layer.weight(.mlp_query), self.get(.mix));
            try self.controller(layer, i, .mlp, layer.weight(.mlp_norm), mlp_input);
        } else {
            try m.mix.mixAndNorm(ctx, self.shape(), self.get(.stack), attn, previous, @intCast(completed + 1), layer.weight(.mlp_query), layer.weight(.mlp_norm), eps, mlp_input);
        }
        try m.ffn.run(ctx, &m.blas, text, &layer.ffn, 1, mlp_input, self.get(.raw), .{
            .logits = self.get(.router),
            .blas = self.get(.blas),
            .routed = self.get(.routed),
            .activation_precision = m.expert_activation_precision,
        });
        try self.hiddenConv(layer.weight(.mlp_taps), old[3], next[3], self.get(.mlp));
        try m.mix.sum(ctx, self.shape(), previous, attn, self.get(.mlp), self.block(completed));
        return completed;
    }

    /// Commit prompt tokens whose logits will not be consumed.
    pub fn append(self: *Request, token: u32) !void {
        try self.run(.{ .token = token }, .none);
    }

    /// Run a token through all visits. The logit buffer is owned by the request.
    pub fn step(self: *Request, token: u32) ![]const f32 {
        try self.run(.{ .token = token }, .logits);
        const bytes: []align(@alignOf(f32)) const u8 = @alignCast(self.logits.constBytes());
        return std.mem.bytesAsSlice(f32, bytes);
    }

    /// The shortlist certifies which rows may tie or beat an exact capped
    /// score. The returned index always comes from the original BF16 dot.
    pub fn stepGreedy(self: *Request, token: u32) !u32 {
        if (self.shortlist_workspace.ptr == 0 or self.use_reference_head or !self.use_packed_weights) {
            const logits = try self.step(token);
            var best: usize = 0;
            for (logits, 0..) |logit, index| {
                if (!std.math.isFinite(logit)) return error.NonFiniteInkling2Logit;
                if (logit > logits[best]) best = index;
            }
            return @intCast(best);
        }
        try self.run(.{ .token = token }, .greedy);
        const best = std.mem.readInt(u32, self.logits.constBytes()[0..4], .little);
        if (best >= self.model.text.unpadded_vocab_size) return error.NonFiniteInkling2Logit;
        return best;
    }

    /// Append a projected vision row in the decoder's embedding space.
    pub fn appendEmbedding(self: *Request, embedding: DeviceBuffer) !void {
        try self.run(.{ .embedding = embedding }, .none);
    }

    const Input = union(enum) { token: u32, embedding: DeviceBuffer };
    const Output = enum { none, logits, greedy };

    fn runVisits(self: *Request, replay: DeviceBuffer) !usize {
        const m = self.model;
        const text = m.text;
        const ctx = m.ctx;
        const hidden = @as(usize, text.hidden_size) * 2;
        var depth: usize = 0;
        var prelude_depth: usize = undefined;
        for (m.schedule, 0..) |visit, ordinal| {
            if (visit.weight_layer == text.loop_config.core_start) {
                if (visit.pass == 0) {
                    prelude_depth = depth;
                    try self.get(.prelude).copyFromDevice(ctx, self.block(depth), hidden);
                } else {
                    std.debug.assert(depth == prelude_depth + 2);
                    try self.get(.bank).copyFromDevice(ctx, sub(self.get(.stack), (prelude_depth + 1) * hidden, 2 * hidden), 2 * hidden);
                    depth = prelude_depth;
                    try self.block(depth).copyFromDevice(ctx, self.get(.prelude), hidden);
                }
            }
            depth = try self.runVisit(visit, ordinal, depth, replay);
        }
        return depth;
    }

    fn captureVisits(self: *Request) !cuda_graph.CapturedGraph {
        const ctx = self.model.ctx;
        try cuda_graph.beginCapture(ctx);
        const depth = self.runVisits(self.get(.position)) catch |err| {
            cuda_graph.discardRecording(ctx);
            return err;
        };
        if (depth != block_slots - 1) {
            cuda_graph.discardRecording(ctx);
            return error.InvalidInkling2Layer;
        }
        return cuda_graph.endCapture(ctx);
    }

    fn replayVisits(self: *Request) !bool {
        if (!self.replay_enabled or self.position() < 3) return false;
        const ctx = self.model.ctx;
        const owner = &self.replay_graphs[self.history_side];
        if (owner.exec == null) {
            owner.* = self.captureVisits() catch |err| {
                std.log.warn("Inkling CUDA graph capture unavailable: {s}", .{@errorName(err)});
                self.replay_enabled = false;
                return false;
            };
        }
        const stage = self.replay_position.bytes();
        std.mem.writeInt(u32, stage[0..4], @intCast(self.position()), .little);
        try self.get(.position).copyFromHost(ctx, stage);
        try owner.launch(ctx);
        try self.kv.markReplaySubmitted();
        return true;
    }

    fn projectLogits(self: *Request, logits: DeviceBuffer, normalized: DeviceBuffer) !void {
        const m = self.model;
        const text = m.text;
        const ctx = m.ctx;
        if (!self.use_reference_head) {
            if (self.use_packed_weights and try m.matvec.tryRunPacked(ctx, logits, normalized, m.packed_head, 0, text.unpadded_vocab_size, .f32)) return;
            if (try m.matvec.tryRun(ctx, logits, normalized, m.weight(.unembed), text.hidden_size, text.unpadded_vocab_size, .f32)) return;
        }
        try m.blas.matmulBf16WeightF32Out(ctx, logits, normalized, m.weight(.unembed), self.get(.blas), 1, text.hidden_size, text.vocab_size);
    }

    fn run(self: *Request, input: Input, output: Output) !void {
        const m = self.model;
        const text = m.text;
        const ctx = m.ctx;
        const hidden = @as(usize, text.hidden_size) * 2;
        switch (input) {
            .token => |token| if (token >= text.unpadded_vocab_size) return error.InvalidInkling2Token,
            .embedding => |embedding| try buffer.validateRegions(1, .{embedding}, .{hidden}, .{2}, 1),
        }
        try self.kv.prepareAppend(1);
        errdefer {
            // Some work may have been enqueued before the first KV write.
            ctx.synchronize() catch {};
            self.kv.abort() catch {};
        }
        try self.publishPages();
        switch (input) {
            .token => |token| try m.mix.norm(ctx, self.shape(), row(m.weight(.embed), token, hidden), .bf16, m.weight(.embed_norm), text.rms_norm_eps, self.block(0)),
            .embedding => |embedding| try self.block(0).copyFromDevice(ctx, embedding, hidden),
        }
        const depth = if (try self.replayVisits()) block_slots - 1 else try self.runVisits(.{});
        if (output != .none) {
            const normalized = self.get(.normalized);
            try m.mix.mixAndNorm(ctx, self.shape(), self.get(.stack), self.block(depth), .{}, @intCast(depth + 1), m.weight(.output_query), m.weight(.final_norm), text.rms_norm_eps, normalized);
            if (output == .greedy) {
                try m.matvec.runGreedy(ctx, normalized, m.packed_head, m.head_shortlist, self.shortlist_workspace, self.logits.bytes()[0..4], text.final_logit_softcapping);
            } else {
                const logits = self.get(.logits);
                try self.projectLogits(logits, normalized);
                try m.mix.softcap(ctx, logits, text.unpadded_vocab_size, text.final_logit_softcapping);
                try logits.copyToHost(ctx, self.logits.bytes());
            }
        }
        try self.kv.commit();
        self.history_side = 1 - self.history_side;
    }
};
