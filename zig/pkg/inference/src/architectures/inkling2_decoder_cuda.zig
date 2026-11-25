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

//! Resident fixed-loop text decoder with request-private CUDA state.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const MmapRegion = @import("../util/c_file.zig").MmapRegion;
const attention = @import("inkling2_attention_cuda.zig");
const moe = @import("inkling2_moe_cuda.zig");
const dspark = @import("inkling2_dspark_cuda.zig");
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
pub const prefill_chunk_rows = 16;
pub const decode_batch_rows = 4;

pub fn speculativeBlockContextSupported(text: checkpoint.TextConfig, tokens: usize) bool {
    // The multirow target verifier has diverged from row-one greedy decoding
    // in the long-context log-scaling regime.
    return tokens <= text.log_scaling_n_floor;
}
const Stream = checkpoint.ControllerStream;

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
const LayerWeight = checkpoint.ResidualWeight;
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
        for (std.enums.values(LayerWeight), &result.weights) |weight_key, *destination| {
            if (try source.residualWeight(layer, attn.local, weight_key)) |bytes|
                destination.* = try arena.uploadWeight(ctx, bytes);
        }
        const text = source.config.value.text_config;
        const core = text.loop_config;
        if (layer >= core.core_start and layer < core.core_end) {
            const down_bytes = 4 * @as(usize, text.hidden_size) * core.gate_rank;
            result.attention_controller_down = try DeviceBuffer.allocWithPurpose(ctx, 3 * down_bytes, .model_cache);
            errdefer ctx.synchronize() catch {};
            inline for (std.enums.values(Stream)) |stream| {
                const controller = &result.controllers[@backingInt(stream)];
                const weights = try source.controller(layer, stream);
                if (stream == .mlp) {
                    controller[@backingInt(ControllerWeight.down)] = try arena.uploadWeight(ctx, weights.down);
                } else {
                    const destination = row(result.attention_controller_down, @backingInt(stream), down_bytes);
                    try arena.copyWeight(ctx, destination, weights.down);
                    controller[@backingInt(ControllerWeight.down)] = destination;
                }
                const each_bytes = @as(usize, text.hidden_size) * core.gate_rank * 2;
                const gate_weights = try DeviceBuffer.allocWithPurpose(ctx, 2 * each_bytes, .model_cache);
                controller[@backingInt(ControllerWeight.gain_forget)] = gate_weights;
                errdefer ctx.synchronize() catch {};
                for ([_][]const u8{ weights.gain, weights.forget }, 0..) |gate, index|
                    try arena.copyWeight(ctx, row(gate_weights, index, each_bytes), gate);
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

    pub fn sharedDraftWeights(self: *const Decoder) dspark.SharedWeights {
        return .{
            .embed = self.weight(.embed),
            .embed_norm = self.weight(.embed_norm),
            .unembed = self.weight(.unembed),
            .vocab = self.text.unpadded_vocab_size,
            .padded_vocab = self.text.vocab_size,
            .logit_cap = self.text.final_logit_softcapping,
        };
    }

    fn weight(self: *const Decoder, key: ModelWeight) DeviceBuffer {
        return self.weights[@backingInt(key)];
    }
};

const Temp = enum { stack, prelude, bank, inputs, mix, message, pair, rank, gain_forget, raw, attn, mlp, normalized, logits, blas, relative, router, routed, pages, cohort, position };

/// A request is bound to one CUDA stream. Initialize it in place and keep it
/// at that address until deinit so the cache can retain its internal owners.
fn requestCacheConfig(text: checkpoint.TextConfig, max_tokens: u32, chunk_rows: u32) cache.Config {
    return .{
        .layout = .{ .page_tokens = page_tokens, .window_tokens = text.sliding_window_size, .max_tokens = max_tokens, .max_append_tokens = chunk_rows },
        .full_slots = checkpoint.full_slot_count,
        .sliding_slots = checkpoint.sliding_slot_count,
        .kv_heads = text.num_key_value_heads,
        .head_dim = text.head_dim,
    };
}

/// Cold layout used by both admission and allocation. Kernel dispatch only
/// receives the individual buffers described by this plan.
pub const RequestPlan = struct {
    model: *Decoder,
    max_tokens: u32,
    chunk_rows: u32,
    kv: cache.Config,
    attention_space: attention.Workspace.Layout,
    history_bytes: usize,
    page_table_bytes: usize,
    logits_bytes: usize,
    shortlist_bytes: usize,
    temp_bytes: std.EnumArray(Temp, usize),
    replay_enabled: bool,

    pub fn init(model: *Decoder, max_tokens: u32, chunk_rows: u32) !RequestPlan {
        const text = model.text;
        if (max_tokens == 0 or max_tokens > text.model_max_length or chunk_rows == 0 or
            chunk_rows > @min(prefill_chunk_rows, max_tokens)) return error.InvalidInkling2Context;
        const ctx = model.ctx;
        const replay_enabled = ctx.info.compute_major == 12 and ctx.info.compute_minor <= 1 and
            max_tokens >= 5 and max_tokens <= text.sliding_window_size;
        const controller_rows: usize = @max(chunk_rows, if (Request.supportsGroupedControllers(ctx)) @as(u32, 3) else 1);
        const hidden = @as(usize, text.hidden_size) * 2;
        const kv_row = @as(usize, text.num_key_value_heads) * text.head_dim * 2;
        const head_bytes = @as(usize, text.num_attention_heads) * text.head_dim * 2;
        const page_table_bytes = (try std.math.divCeil(usize, max_tokens, page_tokens)) * 8;
        const nv = try moe.scratchBytes(.nvfp4, text, chunk_rows);
        const mx = try moe.scratchBytes(.mxfp8, text, chunk_rows);
        const activation_bytes = hidden * chunk_rows;
        return .{
            .model = model,
            .max_tokens = max_tokens,
            .chunk_rows = chunk_rows,
            .kv = requestCacheConfig(text, max_tokens, chunk_rows),
            .attention_space = try attention.Workspace.layout(text, chunk_rows),
            .history_bytes = 2 * checkpoint.visit_count * 6 * (hidden + kv_row),
            .page_table_bytes = page_table_bytes,
            .logits_bytes = @as(usize, text.unpadded_vocab_size) * 4,
            .shortlist_bytes = model.head_shortlist.workspaceBytes(),
            .replay_enabled = replay_enabled,
            .temp_bytes = .initDefault(0, .{
                .stack = block_slots * activation_bytes,
                .prelude = activation_bytes,
                .bank = 2 * activation_bytes,
                .inputs = 3 * activation_bytes,
                .mix = 2 * hidden * controller_rows,
                .message = 2 * hidden * controller_rows,
                .pair = 2 * hidden * controller_rows,
                .rank = @as(usize, text.loop_config.gate_rank) * 2 * controller_rows,
                .gain_forget = 4 * hidden * controller_rows,
                .raw = activation_bytes,
                .cohort = if (chunk_rows >= 2) (head_bytes + hidden) * @min(chunk_rows, decode_batch_rows) else 0,
                .attn = activation_bytes,
                .mlp = activation_bytes,
                .normalized = activation_bytes,
                .logits = @as(usize, text.vocab_size) * 4,
                .blas = 8 * 1024 * 1024,
                .relative = try relativeScratchBytes(text, max_tokens, chunk_rows),
                .router = @max(nv.logits, mx.logits),
                .routed = @max(nv.routed, mx.routed),
                .pages = page_table_bytes,
                .position = if (replay_enabled) 4 else 0,
            }),
        };
    }

    pub fn memory(self: RequestPlan, speculative: bool, batched_verification: bool) !struct { host: usize, kv: usize, workspace: usize } {
        var workspace = self.attention_space.bytes + self.shortlist_bytes;
        for (self.temp_bytes.values) |bytes| workspace = try std.math.add(usize, workspace, bytes);
        const legacy_floor = 64 * 1024 * 1024 + 2 * self.page_table_bytes +
            self.temp_bytes.get(.relative) + self.temp_bytes.get(.router) + self.temp_bytes.get(.routed);
        if (speculative) {
            workspace = try std.math.add(usize, workspace, dspark.Draft.requestWorkspaceBytes(self.chunk_rows));
            if (batched_verification and speculativeBlockContextSupported(self.model.text, self.max_tokens))
                workspace = try std.math.add(usize, workspace, verificationHistoryBytes(self.model.text));
        }
        return .{
            .host = @max(self.logits_bytes + self.page_table_bytes + @as(usize, if (self.replay_enabled) 4 else 0), 16 * self.page_table_bytes + @as(usize, self.model.text.vocab_size) * 4),
            .kv = try self.kv.deviceBytes(.full) + try self.kv.deviceBytes(.sliding) + 2 * self.history_bytes,
            .workspace = @max(workspace, legacy_floor),
        };
    }
};

fn verificationHistoryBytes(text: checkpoint.TextConfig) usize {
    const widths = @as(usize, text.hidden_size) + @as(usize, text.num_key_value_heads) * text.head_dim;
    return 4 * widths * checkpoint.visit_count * dspark.block_rows;
}

fn relativeScratchBytes(text: checkpoint.TextConfig, max_tokens: u32, chunk_rows: u32) !usize {
    var relative_bytes: usize = 0;
    for (1..chunk_rows + 1) |count| {
        const rows: u32 = @intCast(count);
        relative_bytes = @max(relative_bytes, try relative.scratchBytes(.{
            .rows = rows,
            .start = max_tokens - rows,
            .q_heads = text.num_attention_heads,
            .kv_heads = text.num_key_value_heads,
            .relative_extent = text.rel_extent,
            .score_scale = 1,
        }, 0, max_tokens));
    }
    return relative_bytes;
}

pub const Request = struct {
    model: *Decoder,
    kv: cache.Cache = undefined,
    workspace: attention.Workspace = undefined,
    append_capacity: u32 = 1,
    max_tokens: u32 = 0,
    pages: HostBuffer = .{},
    replay_position: HostBuffer = .{},
    replay_graphs: [2]cuda_graph.CapturedGraph = @splat(.{}),
    replay_enabled: bool = false,
    single_row_prepared: bool = false,
    logits: HostBuffer = .{},
    shortlist_workspace: DeviceBuffer = .{},
    history: DeviceBuffer = .{},
    verification_history: DeviceBuffer = .{},
    record_history: bool = false,
    batched_verification: bool = false,
    draft: ?*dspark.Draft = null,
    temps: Weights(Temp) = @splat(.{}),
    history_side: usize = 0,
    /// Opt in to a different BF16 dot-product reduction order.
    use_dense: bool = false,
    use_reference_head: bool = false,
    use_packed_weights: bool = true,
    use_separate_controllers: bool = false,

    pub fn init(self: *Request, allocator: std.mem.Allocator, model: *Decoder, max_tokens: u32) !void {
        try self.initWithPrefill(allocator, model, max_tokens, 1);
    }

    pub fn initWithPrefill(self: *Request, allocator: std.mem.Allocator, model: *Decoder, max_tokens: u32, chunk_rows: u32) !void {
        try self.initWithPlan(allocator, try RequestPlan.init(model, max_tokens, chunk_rows));
    }

    pub fn initWithPlan(self: *Request, allocator: std.mem.Allocator, plan: RequestPlan) !void {
        const ctx = plan.model.ctx;
        self.* = .{
            .model = plan.model,
            .max_tokens = plan.max_tokens,
            .append_capacity = plan.chunk_rows,
            .replay_enabled = plan.replay_enabled,
        };
        try self.kv.init(allocator, ctx, plan.kv);
        errdefer self.kv.deinit() catch {};
        self.workspace = try attention.Workspace.allocate(ctx, plan.attention_space);
        self.workspace.rows = 1;
        errdefer self.workspace.deinit(ctx);
        errdefer self.freeBuffers();
        self.history = try DeviceBuffer.allocWithPurpose(ctx, plan.history_bytes, .kv_state);
        self.pages = try HostBuffer.alloc(ctx, plan.page_table_bytes);
        if (plan.replay_enabled) {
            self.replay_position = try HostBuffer.alloc(ctx, 4);
            self.temps[@backingInt(Temp.position)] = try DeviceBuffer.allocWithPurpose(ctx, plan.temp_bytes.get(.position), .command_workspace);
        }
        self.logits = try HostBuffer.alloc(ctx, plan.logits_bytes);
        for (plan.temp_bytes.values[0..@backingInt(Temp.position)], self.temps[0..@backingInt(Temp.position)]) |bytes, *target| {
            if (bytes != 0) target.* = try DeviceBuffer.allocWithPurpose(ctx, bytes, .command_workspace);
        }
        if (plan.shortlist_bytes != 0) {
            self.shortlist_workspace = DeviceBuffer.allocWithPurpose(ctx, plan.shortlist_bytes, .command_workspace) catch |err| switch (err) {
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
        self.verification_history.free(ctx);
        self.shortlist_workspace.free(ctx);
        self.logits.free(ctx);
        self.pages.free(ctx);
        self.replay_position.free(ctx);
    }

    /// Attach before the first append so every prompt row can seed the
    /// drafter. Request graphs borrow the drafter's feature staging planes.
    pub fn attachDraft(self: *Request, draft: *dspark.Draft) !void {
        return self.attachDraftWithVerification(draft, .batched);
    }

    pub const DraftVerification = enum { exact_greedy, batched };

    pub fn attachDraftWithVerification(self: *Request, draft: *dspark.Draft, verification: DraftVerification) !void {
        if (self.position() != 0 or self.draft != null or draft.ctx != self.model.ctx or
            draft.capacity != self.max_tokens or draft.position != 0) return error.InvalidInkling2DraftContext;
        const text = self.model.text;
        for (draft.stage) |plane| {
            if (plane.len < @as(usize, self.append_capacity) * text.hidden_size * 2) return error.InvalidInkling2DraftContext;
        }
        self.batched_verification = verification == .batched and speculativeBlockContextSupported(text, self.max_tokens);
        if (self.batched_verification)
            self.verification_history = try DeviceBuffer.allocWithPurpose(self.model.ctx, verificationHistoryBytes(text), .command_workspace);
        self.draft = draft;
    }

    pub fn position(self: *const Request) usize {
        return self.kv.committedTokens();
    }

    fn get(self: *const Request, temp: Temp) DeviceBuffer {
        return self.temps[@backingInt(temp)];
    }

    fn shape(self: *const Request) residual.Shape {
        return .{ .rows = self.workspace.rows, .width = self.model.text.hidden_size };
    }

    fn blockBytes(self: *const Request) usize {
        return @as(usize, self.model.text.hidden_size) * 2 * self.workspace.rows;
    }

    fn block(self: *const Request, index: usize) DeviceBuffer {
        return row(self.get(.stack), index, self.blockBytes());
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

    fn verificationHistory(self: *const Request, visit: usize) [4]DeviceBuffer {
        const text = self.model.text;
        const kv = dspark.block_rows * @as(usize, text.num_key_value_heads) * text.head_dim * 2;
        const hidden = dspark.block_rows * @as(usize, text.hidden_size) * 2;
        const base = visit * 2 * (kv + hidden);
        return .{
            sub(self.verification_history, base, kv),
            sub(self.verification_history, base + kv, kv),
            sub(self.verification_history, base + 2 * kv, hidden),
            sub(self.verification_history, base + 2 * kv + hidden, hidden),
        };
    }

    fn retainHistoryPrefix(self: *Request, count: u32) !void {
        const ctx = self.model.ctx;
        for (0..checkpoint.visit_count) |visit| {
            const old = self.histories(visit, self.history_side);
            const next = self.histories(visit, 1 - self.history_side);
            const projected = self.verificationHistory(visit);
            for (old, next, projected) |before, after, source| {
                const bytes = after.len / 3;
                const newest = @min(count, 3);
                const from_old = 3 - newest;
                if (from_old != 0)
                    try sub(after, 0, from_old * bytes).copyFromDevice(ctx, sub(before, newest * bytes, from_old * bytes), from_old * bytes);
                try sub(after, from_old * bytes, newest * bytes).copyFromDevice(ctx, sub(source, (count - newest) * bytes, newest * bytes), newest * bytes);
            }
        }
    }

    fn hiddenConv(self: *Request, weight: DeviceBuffer, old: DeviceBuffer, next: DeviceBuffer, input: DeviceBuffer, output: DeviceBuffer) !void {
        try self.model.conv.run(self.model.ctx, .{
            .rows = self.workspace.rows,
            .channels = self.model.text.hidden_size,
            .head_size = self.model.text.hidden_size,
            .input = input,
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
        try m.blas.matmulBf16WeightBf16Out(m.ctx, hidden, pair, weights[@backingInt(ControllerWeight.down)], self.get(.blas), self.workspace.rows, 2 * @as(usize, m.text.hidden_size), m.text.loop_config.gate_rank);
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
        if (self.use_dense and self.workspace.rows == 1) {
            if (!try m.matvec.tryRun(ctx, gate, hidden, gate_weight, rank, @as(u32, width) * 2, .f32))
                try m.blas.matmulBf16WeightF32Out(ctx, gate, hidden, gate_weight, blas_space, 1, rank, @as(usize, width) * 2);
        } else {
            const weight_bytes = @as(usize, rank) * bytes;
            const gate_bytes = self.blockBytes() * 2;
            try m.blas.matmulBf16WeightF32Out(ctx, row(gate, 0, gate_bytes), hidden, row(gate_weight, 0, weight_bytes), blas_space, self.workspace.rows, rank, width);
            try m.blas.matmulBf16WeightF32Out(ctx, row(gate, 1, gate_bytes), hidden, row(gate_weight, 1, weight_bytes), blas_space, self.workspace.rows, rank, width);
        }
    }

    fn controller(self: *Request, layer: *const Layer, layer_id: usize, stream: Stream, gamma: DeviceBuffer, output: DeviceBuffer) !void {
        const m = self.model;
        const bytes = self.blockBytes();
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

    fn prepareAttention(self: *Request, visit: checkpoint.Visit, ordinal: usize, stack_depth: usize, replay: DeviceBuffer, raw_heads: ?DeviceBuffer) !void {
        const m = self.model;
        const ctx = m.ctx;
        const text = m.text;
        const i = visit.weight_layer;
        const layer = &m.layers[i];
        const looped = visit.pass > 0;
        const partial = self.block(stack_depth);
        const bytes = self.blockBytes();
        const query_bytes = @as(usize, text.hidden_size) * 4;
        const eps = text.rms_norm_eps;
        const grouped = looped and self.workspace.rows == 1 and !self.use_separate_controllers and supportsGroupedControllers(ctx);
        var input: [3]DeviceBuffer = undefined;
        var gammas: [3]DeviceBuffer = undefined;
        for ([_]Stream{ .attn, .k, .v }, [_]LayerWeight{ .q_norm, .k_norm, .v_norm }, 0..) |stream, norm_key, channel| {
            if (layer.attn.local and channel > 0 and !looped) {
                input[channel] = input[0];
                continue;
            }
            const gamma = layer.weight(if (layer.attn.local) .q_norm else norm_key);
            input[channel] = row(self.get(.inputs), channel, bytes);
            if (i == 0) {
                try m.mix.norm(ctx, self.shape(), partial, .bf16, gamma, eps, input[channel]);
                continue;
            }
            if (looped) {
                gammas[channel] = gamma;
                if (!layer.attn.local or channel == 0) {
                    const query = row(layer.weight(.attn_query), channel, query_bytes);
                    const mixed = if (grouped) row(self.get(.mix), channel, bytes * 2) else self.get(.mix);
                    try m.mix.mix(ctx, self.shape(), self.get(.stack), partial, .{}, @intCast(stack_depth + 1), query, mixed);
                }
                if (!grouped) try self.controller(layer, i, stream, gamma, input[channel]);
            } else {
                const query = row(layer.weight(.attn_query), channel, query_bytes);
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
            .output = raw_heads orelse self.get(.raw),
            .project_output = raw_heads == null,
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
        if (self.record_history) {
            const stage = self.verificationHistory(ordinal);
            const raw_kv = self.workspace.rawKv();
            const kv_bytes = stage[0].len / dspark.block_rows * self.workspace.rows;
            try stage[0].copyFromDevice(ctx, raw_kv.key, kv_bytes);
            try stage[1].copyFromDevice(ctx, raw_kv.value, kv_bytes);
            try stage[2].copyFromDevice(ctx, self.get(.raw), self.blockBytes());
        }
    }

    fn prepareFfn(self: *Request, visit: checkpoint.Visit, ordinal: usize, stack_depth: usize, projected_attention: DeviceBuffer, mlp_input: DeviceBuffer) !usize {
        const m = self.model;
        const ctx = m.ctx;
        const text = m.text;
        const i = visit.weight_layer;
        const layer = &m.layers[i];
        const eps = text.rms_norm_eps;
        const old = self.histories(ordinal, self.history_side);
        const next = self.histories(ordinal, 1 - self.history_side);
        const attn = self.get(.attn);
        try self.hiddenConv(layer.weight(.attn_taps), old[2], next[2], projected_attention, attn);
        const boundary = i % text.n_layers_per_attn_res_block == 0;
        const completed = stack_depth + @intFromBool(boundary);
        const previous = if (boundary) DeviceBuffer{} else self.block(stack_depth);
        if (visit.pass > 0) {
            try m.mix.mix(ctx, self.shape(), self.get(.stack), attn, previous, @intCast(completed + 1), layer.weight(.mlp_query), self.get(.mix));
            try self.controller(layer, i, .mlp, layer.weight(.mlp_norm), mlp_input);
        } else {
            try m.mix.mixAndNorm(ctx, self.shape(), self.get(.stack), attn, previous, @intCast(completed + 1), layer.weight(.mlp_query), layer.weight(.mlp_norm), eps, mlp_input);
        }
        return completed;
    }

    fn finishVisit(self: *Request, visit: checkpoint.Visit, ordinal: usize, completed: usize, mlp_output: DeviceBuffer) !void {
        const m = self.model;
        const ctx = m.ctx;
        const text = m.text;
        const layer = &m.layers[visit.weight_layer];
        const old = self.histories(ordinal, self.history_side);
        const next = self.histories(ordinal, 1 - self.history_side);
        const previous = if (visit.weight_layer % text.n_layers_per_attn_res_block == 0) DeviceBuffer{} else self.block(completed);
        if (self.record_history) try self.verificationHistory(ordinal)[3].copyFromDevice(ctx, mlp_output, self.blockBytes());
        try self.hiddenConv(layer.weight(.mlp_taps), old[3], next[3], mlp_output, self.get(.mlp));
        try m.mix.sum(ctx, self.shape(), previous, self.get(.attn), self.get(.mlp), self.block(completed));
    }

    fn runVisit(self: *Request, visit: checkpoint.Visit, ordinal: usize, stack_depth: usize, replay: DeviceBuffer) !usize {
        try self.prepareAttention(visit, ordinal, stack_depth, replay, null);
        const completed = try self.prepareFfn(visit, ordinal, stack_depth, self.get(.raw), self.get(.normalized));
        const m = self.model;
        try m.ffn.run(m.ctx, &m.blas, m.text, &m.layers[visit.weight_layer].ffn, self.workspace.rows, self.get(.normalized), self.get(.raw), .{
            .logits = self.get(.router),
            .blas = self.get(.blas),
            .routed = self.get(.routed),
            .activation_precision = m.expert_activation_precision,
        });
        try self.finishVisit(visit, ordinal, completed, self.get(.raw));
        return completed;
    }

    /// Commit prompt tokens whose logits will not be consumed.
    pub fn append(self: *Request, token: u32) !void {
        try self.run(.{ .tokens = &.{token} }, 1, .none);
    }

    /// Run a token through all visits. The logit buffer is owned by the request.
    pub fn step(self: *Request, token: u32) ![]const f32 {
        try self.run(.{ .tokens = &.{token} }, 1, .logits);
        const bytes: []align(@alignOf(f32)) const u8 = @alignCast(self.logits.constBytes());
        return std.mem.bytesAsSlice(f32, bytes);
    }

    pub fn canBatchGreedy(self: *const Request) bool {
        return self.use_dense and self.get(.cohort).ptr != 0 and self.draft == null and self.shortlist_workspace.ptr != 0 and
            !self.use_reference_head and self.use_packed_weights;
    }

    /// Share the attention output projection and routed FFN. Attention scores,
    /// convolution histories and vocabulary heads stay per sequence.
    pub fn stepGreedyBatch(requests: []const *Request, tokens: []const u32, results: []u32) !void {
        std.debug.assert(requests.len >= 2 and requests.len <= decode_batch_rows and tokens.len == requests.len and results.len == requests.len);
        const leader = requests[0];
        std.debug.assert(requests.len <= leader.append_capacity);
        const m = leader.model;
        const ctx = m.ctx;
        const text = m.text;
        const hidden = @as(usize, text.hidden_size) * 2;
        for (requests, tokens, 0..) |request, token, index| {
            std.debug.assert(request.model == m and request.canBatchGreedy() and request.workspace.rows == 1);
            std.debug.assert(std.mem.indexOfScalar(*Request, requests[0..index], request) == null);
            try request.validateInput(.{ .tokens = &.{token} }, 1);
        }
        var staged: usize = 0;
        errdefer for (requests[0..staged]) |request| request.abortAppend();
        for (requests, tokens) |request, token| {
            try request.kv.prepareAppend(1);
            staged += 1;
            try request.stageInput(.{ .tokens = &.{token} });
        }
        const head_bytes = @as(usize, text.num_attention_heads) * text.head_dim * 2;
        const heads = sub(leader.get(.cohort), 0, requests.len * head_bytes);
        // Once the output projection consumes the heads, its input storage
        // becomes the FFN input. The projected rows are consumed before the
        // FFN writes its outputs back into the second region.
        const inputs = sub(heads, 0, requests.len * hidden);
        const outputs = sub(leader.get(.cohort), heads.len, requests.len * hidden);
        var cursor_storage: [decode_batch_rows]VisitCursor = @splat(.{});
        const cursors = cursor_storage[0..requests.len];
        for (m.schedule, 0..) |visit, ordinal| {
            const layer = &m.layers[visit.weight_layer];
            for (requests, cursors, 0..) |request, *cursor, lane| {
                try request.advanceVisit(visit, ordinal, cursor);
                try request.prepareAttention(visit, ordinal, cursor.depth, .{}, row(heads, lane, head_bytes));
            }
            try attention.Forward.projectOutputBatch(&m.matvec, &m.blas, ctx, text, &layer.attn, @intCast(requests.len), heads, outputs, leader.get(.blas));
            for (requests, cursors, 0..) |request, *cursor, lane| {
                cursor.depth = try request.prepareFfn(visit, ordinal, cursor.depth, row(outputs, lane, hidden), row(inputs, lane, hidden));
            }
            try m.ffn.run(ctx, &m.blas, text, &layer.ffn, @intCast(requests.len), inputs, outputs, .{
                .logits = leader.get(.router),
                .blas = leader.get(.blas),
                .routed = leader.get(.routed),
                .activation_precision = m.expert_activation_precision,
                .rowwise_router = true,
            });
            for (requests, cursors, 0..) |request, cursor, lane| {
                try request.finishVisit(visit, ordinal, cursor.depth, row(outputs, lane, hidden));
            }
        }
        for (requests, cursors) |request, cursor| {
            try request.normalizeOutput(cursor.depth);
            try request.submitGreedy();
        }
        for (requests, results) |request, *result| {
            try request.commitAppend(1);
            result.* = try request.greedyToken();
        }
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
        try self.run(.{ .tokens = &.{token} }, 1, .greedy);
        return self.greedyToken();
    }

    fn greedyToken(self: *const Request) !u32 {
        const best = std.mem.readInt(u32, self.logits.constBytes()[0..4], .little);
        if (best >= self.model.text.unpadded_vocab_size) return error.NonFiniteInkling2Logit;
        return best;
    }

    /// Append a projected vision row in the decoder's embedding space.
    pub fn appendEmbedding(self: *Request, embedding: DeviceBuffer) !void {
        try self.run(.{ .embedding = embedding }, 1, .none);
    }

    /// Commit prompt tokens in bounded chunks. A failed chunk leaves the
    /// preceding committed prefix available.
    pub fn appendTokens(self: *Request, tokens: []const u32) !void {
        if (tokens.len > self.max_tokens - self.position()) return error.InvalidInkling2Context;
        var offset: usize = 0;
        while (offset < tokens.len) {
            const rows: u32 = @intCast(@min(self.append_capacity, tokens.len - offset));
            try self.run(.{ .tokens = tokens[offset..][0..rows] }, rows, .none);
            offset += rows;
        }
    }

    /// Commit contiguous BF16 vision embeddings in decoder row order.
    pub fn appendEmbeddings(self: *Request, embeddings: DeviceBuffer, rows: u32) !void {
        return self.appendEmbeddingsWithControl(embeddings, rows, null);
    }

    pub fn appendEmbeddingsWithControl(self: *Request, embeddings: DeviceBuffer, rows: u32, control: ?@import("../execution_control.zig").InferenceExecutionControl) !void {
        if (rows == 0) return;
        if (rows > self.max_tokens - self.position()) return error.InvalidInkling2Context;
        const row_bytes = @as(usize, self.model.text.hidden_size) * 2;
        try buffer.validateRegions(1, .{embeddings}, .{@as(usize, rows) * row_bytes}, .{2}, 1);
        var offset: u32 = 0;
        while (offset < rows) {
            if (control) |active| try active.check();
            const count = @min(self.append_capacity, rows - offset);
            const chunk = sub(embeddings, @as(usize, offset) * row_bytes, @as(usize, count) * row_bytes);
            try self.run(.{ .embedding = chunk }, count, .none);
            offset += count;
        }
    }

    const Input = union(enum) { tokens: []const u32, embedding: DeviceBuffer };
    const Output = union(enum) { none, logits, greedy, verify: *Verification };

    pub const Verification = struct {
        ids: [dspark.block_rows]u32,
        count: u32,
    };

    /// The final returned token is the continuation after the last accepted
    /// input. It has not been appended to the cache.
    pub fn verifyDraft(self: *Request, anchor: u32, proposed: []const u32) !Verification {
        if (self.draft == null or proposed.len == 0 or proposed.len >= dspark.block_rows or
            proposed.len >= self.append_capacity) return error.InvalidInkling2DraftContext;
        var tokens: [dspark.block_rows]u32 = undefined;
        tokens[0] = anchor;
        @memcpy(tokens[1..][0..proposed.len], proposed);
        const input = tokens[0 .. proposed.len + 1];
        if (!self.batched_verification) return self.verifyDraftGreedy(input);
        var result: Verification = undefined;
        try self.run(.{ .tokens = input }, @intCast(input.len), .{ .verify = &result });
        return result;
    }

    fn verifyDraftGreedy(self: *Request, tokens: []const u32) !Verification {
        if (tokens.len > self.max_tokens - self.position()) return error.InvalidInkling2Context;
        try self.validateInput(.{ .tokens = tokens }, @intCast(tokens.len));
        var verification: Verification = .{ .ids = undefined, .count = 0 };
        for (tokens, 0..) |token, index| {
            const next = try self.stepGreedy(token);
            verification.ids[index] = next;
            verification.count += 1;
            if (index + 1 < tokens.len and next != tokens[index + 1]) break;
        }
        return verification;
    }

    const VisitCursor = struct {
        depth: usize = 0,
        prelude_depth: usize = undefined,
        tap: usize = 0,
    };

    fn advanceVisit(self: *Request, visit: checkpoint.Visit, ordinal: usize, cursor: *VisitCursor) !void {
        const m = self.model;
        const hidden = self.blockBytes();
        switch (visit.preludeAction(m.text.loop_config.core_start)) {
            .none => {},
            .save => {
                cursor.prelude_depth = cursor.depth;
                try self.get(.prelude).copyFromDevice(m.ctx, self.block(cursor.depth), hidden);
            },
            .restore => {
                std.debug.assert(cursor.depth == cursor.prelude_depth + 2);
                try self.get(.bank).copyFromDevice(m.ctx, sub(self.get(.stack), (cursor.prelude_depth + 1) * hidden, 2 * hidden), 2 * hidden);
                cursor.depth = cursor.prelude_depth;
                try self.block(cursor.depth).copyFromDevice(m.ctx, self.get(.prelude), hidden);
            },
        }
        if (self.draft) |draft| {
            if (cursor.tap < draft.taps.len and ordinal == draft.taps[cursor.tap]) {
                try draft.stage[cursor.tap].copyFromDevice(m.ctx, self.block(cursor.depth), hidden);
                cursor.tap += 1;
            }
        }
    }

    fn runVisits(self: *Request, replay: DeviceBuffer) !usize {
        var cursor: VisitCursor = .{};
        for (self.model.schedule, 0..) |visit, ordinal| {
            try self.advanceVisit(visit, ordinal, &cursor);
            cursor.depth = try self.runVisit(visit, ordinal, cursor.depth, replay);
        }
        return cursor.depth;
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
        // Batched prefill may reach decoding before the row-one BLAS plans exist.
        if (!self.replay_enabled or !self.single_row_prepared or self.workspace.rows != 1 or self.position() < 3) return false;
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

    fn validateInput(self: *Request, input: Input, rows: u32) !void {
        const text = self.model.text;
        const hidden = @as(usize, text.hidden_size) * 2;
        switch (input) {
            .tokens => |tokens| for (tokens) |token| {
                if (token >= text.unpadded_vocab_size) return error.InvalidInkling2Token;
            },
            .embedding => |embedding| try buffer.validateRegions(1, .{embedding}, .{hidden * rows}, .{2}, 1),
        }
    }

    fn stageInput(self: *Request, input: Input) !void {
        const m = self.model;
        const text = m.text;
        const ctx = m.ctx;
        const hidden = @as(usize, text.hidden_size) * 2;
        try self.publishPages();
        switch (input) {
            .tokens => |tokens| for (tokens, 0..) |token, index| {
                try m.mix.norm(ctx, .{ .width = text.hidden_size }, row(m.weight(.embed), token, hidden), .bf16, m.weight(.embed_norm), text.rms_norm_eps, row(self.block(0), index, hidden));
            },
            .embedding => |embedding| try self.block(0).copyFromDevice(ctx, embedding, self.blockBytes()),
        }
    }

    fn abortAppend(self: *Request) void {
        // Some work may be in flight even when no KV write was reached.
        self.model.ctx.synchronize() catch {};
        self.kv.abort() catch {};
    }

    fn run(self: *Request, input: Input, rows: u32, output: Output) !void {
        std.debug.assert(rows > 0 and rows <= self.append_capacity and (rows == 1 or output == .none or output == .verify));
        try self.validateInput(input, rows);
        self.workspace.rows = rows;
        defer self.workspace.rows = 1;
        self.record_history = output == .verify;
        defer self.record_history = false;
        try self.kv.prepareAppend(rows);
        errdefer self.abortAppend();
        try self.stageInput(input);
        const depth = if (try self.replayVisits()) block_slots - 1 else try self.runVisits(.{});
        try self.finishAppend(input, rows, output, depth);
        if (rows == 1) self.single_row_prepared = true;
    }

    fn finishAppend(self: *Request, input: Input, rows: u32, output: Output, depth: usize) !void {
        const m = self.model;
        const text = m.text;
        const ctx = m.ctx;
        const normalized = self.get(.normalized);
        if (output != .none or self.draft != null) try self.normalizeOutput(depth);
        var committed = rows;
        switch (output) {
            .none => {},
            .greedy => try self.submitGreedy(),
            .logits => {
                const logits = self.get(.logits);
                try self.projectLogits(logits, normalized);
                try m.mix.softcap(ctx, logits, text.unpadded_vocab_size, text.final_logit_softcapping);
                try logits.copyToHost(ctx, self.logits.bytes());
            },
            .verify => |verification| {
                const best = try self.draft.?.selectTarget(&m.blas, normalized, rows);
                committed = 1;
                while (committed < rows and best[committed - 1] == input.tokens[committed]) : (committed += 1) {}
                verification.* = .{ .ids = best, .count = committed };
                if (committed < rows) try self.retainHistoryPrefix(committed);
            },
        }
        if (self.draft) |draft| {
            try draft.stage[3].copyFromDevice(ctx, normalized, self.blockBytes());
            try draft.stageContext(&m.blas, @intCast(self.position()), committed);
        }
        try self.commitAppend(committed);
    }

    fn normalizeOutput(self: *Request, depth: usize) !void {
        const m = self.model;
        try m.mix.mixAndNorm(m.ctx, self.shape(), self.get(.stack), self.block(depth), .{}, @intCast(depth + 1), m.weight(.output_query), m.weight(.final_norm), m.text.rms_norm_eps, self.get(.normalized));
    }

    fn submitGreedy(self: *Request) !void {
        const m = self.model;
        try m.matvec.runGreedy(m.ctx, self.get(.normalized), m.packed_head, m.head_shortlist, self.shortlist_workspace, self.logits.bytes()[0..4], m.text.final_logit_softcapping);
    }

    fn commitAppend(self: *Request, committed: u32) !void {
        try self.kv.commitPrefix(committed);
        if (self.draft) |draft| draft.commitContext(committed);
        self.history_side = 1 - self.history_side;
    }
};
