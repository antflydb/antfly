// Copyright 2026 Antfly, Inc.
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
const ml = @import("ml");
const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const graph_weight_bridge = @import("graph_weight_bridge.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const gemma4 = @import("gemma4.zig");
const gemma4_mm = @import("../architectures/gemma4_multimodal.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const gemma_graph = @import("../architectures/gemma_graph.zig");
const gpt_model = @import("../models/gpt.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const hf_tokenizer = @import("termite_hf_tokenizer");
const real_autodiff = @import("real_autodiff_trainer.zig");
const gemma4_real = @import("gemma4_real_autodiff.zig");
const ops_mod = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;
const NodeId = ml.graph.NodeId;
const SafetensorsSource = weight_source_mod.SafetensorsSource;

const MediaKind = enum { image, audio };

const CachedProjectedMedia = struct {
    embeddings: []f32,
    tokens: usize,
    hidden_size: usize,
};

const ProjectedMediaCache = struct {
    items: std.StringHashMapUnmanaged(CachedProjectedMedia) = .empty,

    fn deinit(self: *ProjectedMediaCache, allocator: std.mem.Allocator) void {
        var it = self.items.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.embeddings);
        }
        self.items.deinit(allocator);
        self.* = undefined;
    }
};

fn copyTensorFloat32(dst: []f32, tensor: *const Tensor) !void {
    if (tensor.dtype != .f32) return error.AdapterShapeMismatch;
    if (tensor.data.len != dst.len * @sizeOf(f32)) return error.AdapterShapeMismatch;

    if (tensor.asFloat32IfAligned()) |values| {
        if (values.len != dst.len) return error.AdapterShapeMismatch;
        @memcpy(dst, values);
        return;
    }

    for (dst, 0..) |*value, idx| {
        const offset = idx * @sizeOf(f32);
        const bits = std.mem.readInt(u32, tensor.data[offset..][0..@sizeOf(f32)], .little);
        value.* = @bitCast(bits);
    }
}

pub const MultimodalCtx = struct {
    allocator: std.mem.Allocator,
    compute_backend: *const ComputeBackend,
    graph_config: gemma_graph.Config,
    graph_options: gemma_graph.BuildOptions = .{},
    tokenizer: *hf_tokenizer.HfTokenizer,
    gguf_projector_path: []const u8,
    gguf_projector_sha256: []const u8,
    projected_media_cache: ProjectedMediaCache = .{},
    projected_media_cache_hits: usize = 0,
    projected_media_cache_misses: usize = 0,
    built: ?gemma_graph.GemmaGraph = null,
    lm_logits: ?NodeId = null,
    current_embeddings: ?[]const f32 = null,

    pub fn init(
        allocator: std.mem.Allocator,
        compute_backend: *const ComputeBackend,
        graph_config: gemma_graph.Config,
        gguf_projector_path: []const u8,
        gguf_projector_sha256: []const u8,
        tokenizer: *hf_tokenizer.HfTokenizer,
    ) MultimodalCtx {
        return .{
            .allocator = allocator,
            .compute_backend = compute_backend,
            .graph_config = graph_config,
            .tokenizer = tokenizer,
            .gguf_projector_path = gguf_projector_path,
            .gguf_projector_sha256 = gguf_projector_sha256,
        };
    }

    pub fn initRecursive(
        allocator: std.mem.Allocator,
        compute_backend: *const ComputeBackend,
        graph_config: gemma_graph.Config,
        gguf_projector_path: []const u8,
        gguf_projector_sha256: []const u8,
        tokenizer: *hf_tokenizer.HfTokenizer,
        shared_block_size: usize,
    ) MultimodalCtx {
        var ctx = init(allocator, compute_backend, graph_config, gguf_projector_path, gguf_projector_sha256, tokenizer);
        ctx.graph_options = .{ .recursive_shared_block_size = @intCast(shared_block_size) };
        return ctx;
    }

    pub fn deinit(self: *MultimodalCtx) void {
        self.projected_media_cache.deinit(self.allocator);
        self.tokenizer.deinitSelf();
        self.* = undefined;
    }

    pub fn projectedMediaCacheEntries(self: *const MultimodalCtx) usize {
        return self.projected_media_cache.items.count();
    }

    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: ml.graph.NodeId,
        attention_mask: ml.graph.NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!ml.graph.NodeId {
        _ = attention_mask;
        const self: *MultimodalCtx = @ptrCast(@alignCast(ctx_opaque));
        const input_embeddings = try bld.parameter(
            "__input_embeddings",
            Shape.init(.f32, &.{ @as(i64, @intCast(batch)), @as(i64, @intCast(seq_len)), @as(i64, @intCast(self.graph_config.hidden_size)) }),
        );
        self.built = try gemma_graph.buildForwardGraphWithOptions(bld, self.graph_config, batch, seq_len, .{
            .input_ids = input_ids,
            .input_embeddings = input_embeddings,
            .rope_cos = ml.graph.null_node,
            .rope_sin = ml.graph.null_node,
        }, self.graph_options);
        return self.built.?.output_node;
    }

    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        forward_output: ml.graph.NodeId,
        targets: ml.graph.NodeId,
    ) anyerror!ml.graph.NodeId {
        const self: *MultimodalCtx = @ptrCast(@alignCast(ctx_opaque));
        const logits = try self.buildLogits(bld, forward_output);
        return bld.crossEntropyLoss(logits, targets);
    }

    pub fn buildLogits(
        self: *MultimodalCtx,
        bld: *Builder,
        forward_output: ml.graph.NodeId,
    ) !ml.graph.NodeId {
        const out_shape = bld.graph.node(forward_output).output_shape;
        const total_rows: u32 = @intCast(out_shape.dim(0) * out_shape.dim(1));
        const hidden_size: u32 = @intCast(out_shape.dim(2));

        const hidden_flat = try bld.reshape(forward_output, Shape.init(.f32, &.{ @as(i64, @intCast(total_rows)), @as(i64, @intCast(hidden_size)) }));
        const lm_head_w = if (self.graph_config.weight_tying) blk: {
            var name_buf: [256]u8 = undefined;
            const name = try prefixedModelName(&name_buf, self.graph_config, "model.embed_tokens.weight");
            break :blk try bld.parameter(name, Shape.init(.f32, &.{ @as(i64, @intCast(self.graph_config.vocab_size)), @as(i64, @intCast(hidden_size)) }));
        } else try bld.parameter("lm_head.weight", Shape.init(.f32, &.{ @as(i64, @intCast(self.graph_config.vocab_size)), @as(i64, @intCast(hidden_size)) }));
        const logits = try bld.linearNoBias(hidden_flat, lm_head_w, total_rows, hidden_size, self.graph_config.vocab_size);
        self.lm_logits = logits;
        return logits;
    }

    pub fn remapGraphNodes(ctx_opaque: *anyopaque, id_map: []const NodeId) anyerror!void {
        const self: *MultimodalCtx = @ptrCast(@alignCast(ctx_opaque));
        if (self.built) |*built| {
            built.input_ids_node = id_map[built.input_ids_node];
            if (built.rope_cos_node != ml.graph.null_node) built.rope_cos_node = id_map[built.rope_cos_node];
            if (built.rope_sin_node != ml.graph.null_node) built.rope_sin_node = id_map[built.rope_sin_node];
            built.output_node = id_map[built.output_node];
        }
        if (self.lm_logits) |node_id| self.lm_logits = id_map[node_id];
    }
};

pub const OwnedTrainerInput = struct {
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
    input_embeddings: []f32,
    trainer_input: real_autodiff.TrainerInput,

    pub fn deinit(self: *OwnedTrainerInput, allocator: std.mem.Allocator) void {
        allocator.free(self.input_ids);
        allocator.free(self.attention_mask);
        allocator.free(self.targets);
        allocator.free(self.input_embeddings);
        self.* = undefined;
    }
};

pub fn loadTokenizerForModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !*hf_tokenizer.HfTokenizer {
    const tokenizer_path = try std.fs.path.join(allocator, &.{ model_dir, gemma4.tokenizer_file_name });
    defer allocator.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFile(allocator, tokenizer_path);
    defer allocator.free(tokenizer_bytes);
    return hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
}

pub fn makeTrainerInputForExample(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !OwnedTrainerInput {
    return makeTrainerInputForExampleWeighted(allocator, ctx, example, seq_len, null, null);
}

pub fn makeTrainerInputForExampleScaled(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    token_scale_override: ?f32,
) !OwnedTrainerInput {
    return makeTrainerInputForExampleWeighted(allocator, ctx, example, seq_len, token_scale_override, null);
}

pub fn makeTrainerInputForTokenLogprobGrads(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    logprob_grads: []const f32,
) !OwnedTrainerInput {
    const rows_f: f32 = @floatFromInt(seq_len);
    const token_scales = try allocator.alloc(f32, logprob_grads.len);
    defer allocator.free(token_scales);
    for (logprob_grads, 0..) |grad, idx| token_scales[idx] = -grad * rows_f;
    return makeTrainerInputForExampleWeighted(allocator, ctx, example, seq_len, null, token_scales);
}

fn makeTrainerInputForExampleWeighted(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    token_scale_override: ?f32,
    token_scales: ?[]const f32,
) !OwnedTrainerInput {
    const rows: usize = @intCast(seq_len);
    const vocab_size: usize = @intCast(ctx.graph_config.vocab_size);

    const input_ids = try allocator.alloc(i64, rows);
    errdefer allocator.free(input_ids);
    @memset(input_ids, 0);
    const attention_mask = try allocator.alloc(f32, rows);
    errdefer allocator.free(attention_mask);
    @memset(attention_mask, 0.0);

    const active_len = @min(example.num_input_tokens, example.input_ids.len);
    const usable = @min(active_len, rows);
    for (0..usable) |i| {
        input_ids[i] = example.input_ids[i];
        attention_mask[i] = 1.0;
    }

    const targets = try allocator.alloc(f32, rows * vocab_size);
    errdefer allocator.free(targets);
    @memset(targets, 0.0);
    const use_teacher_targets = token_scales == null and token_scale_override == null;
    const filled_teacher_targets = use_teacher_targets and try gemma4_real.fillTeacherTopKTargets(targets, rows, vocab_size, example);
    if (!filled_teacher_targets) {
        var valid_tokens: usize = 0;
        for (0..@min(example.labels.len, rows)) |i| {
            if (example.labels[i] != -100) valid_tokens += 1;
        }
        if (token_scales) |scales| {
            if (scales.len != valid_tokens) return error.GradientShapeMismatch;
        }
        const default_row_scale: f32 = token_scale_override orelse if (valid_tokens == 0)
            0.0
        else
            @as(f32, @floatFromInt(rows)) / @as(f32, @floatFromInt(valid_tokens));
        var supervised_idx: usize = 0;
        for (0..@min(example.labels.len, rows)) |i| {
            const label = example.labels[i];
            if (label < 0) continue;
            const idx: usize = @intCast(label);
            if (idx >= vocab_size) return error.LabelOutOfRange;
            const row_scale = if (token_scales) |scales| scales[supervised_idx] else default_row_scale;
            targets[i * vocab_size + idx] = row_scale;
            supervised_idx += 1;
        }
    }

    const embeddings = try buildExampleEmbeddings(allocator, ctx, example, usable, rows);
    errdefer allocator.free(embeddings);
    ctx.current_embeddings = embeddings;

    return .{
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .input_embeddings = embeddings,
        .trainer_input = .{
            .ctx = @ptrCast(ctx),
            .build_forward = &MultimodalCtx.buildForward,
            .build_loss = &MultimodalCtx.buildLoss,
            .input_ids = input_ids,
            .attention_mask = attention_mask,
            .targets = targets,
            .targets_shape = Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(vocab_size)) }),
            .batch = 1,
            .seq_len = seq_len,
            .bind_arch_inputs = &bindArchInputs,
            .remap_graph_nodes = &MultimodalCtx.remapGraphNodes,
        },
    };
}

pub fn logitsForExample(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) ![]f32 {
    var owned = try makeTrainerInputForExample(allocator, ctx, example, seq_len);
    defer owned.deinit(allocator);
    try trainer.ensureGraphBuilt(owned.trainer_input);

    var gs = &trainer.graph_state.?;
    const logits_node = ctx.lm_logits orelse return error.MissingTrainerLogitsNode;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| trainer.compute_backend.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }

    const input_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.input_ids_node,
        .name = "__input_ids",
        .shape = gs.graph.node(gs.input_ids_node).output_shape,
    };
    const mask_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.attention_mask_node,
        .name = "__attention_mask",
        .shape = gs.graph.node(gs.attention_mask_node).output_shape,
    };
    const targets_placeholder = graph_input_binder.PlaceholderInfo{
        .node_id = gs.targets_node,
        .name = "__targets",
        .shape = gs.graph.node(gs.targets_node).output_shape,
    };

    const input_ct = try graph_input_binder.bindI64(trainer.compute_backend, allocator, input_placeholder, owned.input_ids);
    try rt.put(allocator, gs.input_ids_node, input_ct);
    const mask_ct = try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, owned.attention_mask);
    try rt.put(allocator, gs.attention_mask_node, mask_ct);
    const targets_ct = try graph_input_binder.bindF32(trainer.compute_backend, allocator, targets_placeholder, owned.targets);
    try rt.put(allocator, gs.targets_node, targets_ct);
    try bindArchInputs(ctx, trainer.compute_backend, allocator, &gs.graph, &rt, 1, seq_len, owned.attention_mask);

    for (trainer.lora_params.items) |slot| {
        const dims = try allocator.alloc(i32, slot.dims.len);
        defer allocator.free(dims);
        @memcpy(dims, slot.dims);
        const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, dims);
        try rt.put(allocator, slot.node_id, ct);
    }

    const saved_outputs = try allocator.dupe(NodeId, gs.graph.outputs.items);
    defer {
        gs.graph.outputs.clearRetainingCapacity();
        for (saved_outputs) |node_id| gs.graph.outputs.append(allocator, node_id) catch {};
        allocator.free(saved_outputs);
    }
    gs.graph.outputs.clearRetainingCapacity();
    try gs.graph.markOutput(logits_node);

    var rt_inputs = std.ArrayList(interpreter.RuntimeInput).empty;
    defer rt_inputs.deinit(allocator);
    {
        var it = rt.iterator();
        while (it.next()) |entry| {
            try rt_inputs.append(allocator, .{
                .node_id = entry.key_ptr.*,
                .value = entry.value_ptr.*,
            });
        }
    }

    var exec_result = try interpreter.execute(allocator, &gs.graph, trainer.compute_backend, .{
        .runtime_inputs = rt_inputs.items,
    });
    defer exec_result.deinit(trainer.compute_backend);
    return trainer.compute_backend.toFloat32(exec_result.outputs[0], allocator);
}

pub fn initializeTrainerFromAdapterDir(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *MultimodalCtx,
    adapter_model_dir: []const u8,
    bootstrap_example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !void {
    var bootstrap = try makeTrainerInputForExample(allocator, ctx, bootstrap_example, seq_len);
    defer bootstrap.deinit(allocator);
    try trainer.ensureGraphBuilt(bootstrap.trainer_input);

    var inspection = try gemma4.inspectCheckpoint(allocator, adapter_model_dir);
    defer gemma4.freeInspectionSummary(allocator, &inspection);
    const checkpoint_path = inspection.adapter_checkpoint_path orelse return error.MissingAdapterCheckpoint;

    var source = try SafetensorsSource.initAbsolute(allocator, checkpoint_path);
    defer source.weightSource().deinit();
    const ws = source.weightSource();

    for (trainer.lora_params.items) |*slot| {
        const tensor_name = try mapTrainerSlotNameToGemmaAdapterTensor(allocator, slot.name);
        defer allocator.free(tensor_name);

        var loaded = try ws.getTensor(tensor_name);
        defer loaded.deinit();

        if (loaded.tensor.shape.len != slot.dims.len) return error.AdapterShapeMismatch;
        for (slot.dims, 0..) |want_dim, idx| {
            if (loaded.tensor.shape[idx] != @as(i64, want_dim)) return error.AdapterShapeMismatch;
        }

        try copyTensorFloat32(slot.weights, &loaded.tensor);
        @memset(slot.grad_accum, 0.0);
    }
}

fn mapTrainerSlotNameToGemmaAdapterTensor(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    if (try mapUseSiteTrainerSlotNameToLoopTensor(allocator, name)) |mapped| return mapped;
    if (std.mem.endsWith(u8, name, ".lora_A") or std.mem.endsWith(u8, name, ".lora_B")) {
        return std.fmt.allocPrint(allocator, "{s}.weight", .{name});
    }
    return try allocator.dupe(u8, name);
}

fn mapUseSiteTrainerSlotNameToLoopTensor(allocator: std.mem.Allocator, name: []const u8) !?[]u8 {
    const suffix_a = ".lora_A";
    const suffix_b = ".lora_B";
    const kind: u8, const without_suffix = blk: {
        if (std.mem.endsWith(u8, name, suffix_a)) break :blk .{ 'A', name[0 .. name.len - suffix_a.len] };
        if (std.mem.endsWith(u8, name, suffix_b)) break :blk .{ 'B', name[0 .. name.len - suffix_b.len] };
        return null;
    };
    const marker = ".use_";
    const marker_pos = std.mem.lastIndexOf(u8, without_suffix, marker) orelse return null;
    const digits = without_suffix[marker_pos + marker.len ..];
    if (digits.len == 0) return null;
    _ = std.fmt.parseUnsigned(usize, digits, 10) catch return null;
    return try std.fmt.allocPrint(
        allocator,
        "{s}.loop_{s}.lora_{c}.weight",
        .{ without_suffix[0..marker_pos], digits, kind },
    );
}

pub fn bindArchInputs(
    ctx_opaque: *anyopaque,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    graph: *const ml.graph.Graph,
    rt_map: *std.AutoHashMapUnmanaged(ml.graph.NodeId, CT),
    batch: u32,
    seq_len: u32,
    attention_mask: []const f32,
) !void {
    _ = attention_mask;
    const ctx: *MultimodalCtx = @ptrCast(@alignCast(ctx_opaque));
    const embeddings = ctx.current_embeddings orelse return error.MissingInputEmbeddings;
    const node_id = graph_weight_bridge.findParameterByName(graph, "__input_embeddings") orelse return error.MissingInputEmbeddings;
    const shape = [_]i32{ @intCast(batch), @intCast(seq_len), @intCast(ctx.graph_config.hidden_size) };
    const ct = try cb.fromFloat32Shape(embeddings, &shape);
    try rt_map.put(allocator, node_id, ct);
}

pub fn trainPreparedExamples(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *MultimodalCtx,
    examples: []const gemma4.PreparedExampleInput,
    max_examples: usize,
    seq_len: u32,
) !@import("gemma4_real_autodiff.zig").CausalLmMetrics {
    return runPreparedExamples(allocator, trainer, ctx, examples, max_examples, seq_len, .train);
}

pub fn evaluatePreparedExamples(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *MultimodalCtx,
    examples: []const gemma4.PreparedExampleInput,
    max_examples: usize,
    seq_len: u32,
) !@import("gemma4_real_autodiff.zig").CausalLmMetrics {
    return runPreparedExamples(allocator, trainer, ctx, examples, max_examples, seq_len, .eval);
}

pub fn materializeTeacherTopKTargets(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    gguf_projector_path: []const u8,
    gguf_projector_sha256: []const u8,
    prepared: *gemma4.PreparedInputsSummary,
    backend_kind: gemma4_real.BackendKind,
    options: gemma4_real.TeacherTopKOptions,
) !gemma4_real.TeacherTopKSummary {
    if (options.top_k == 0) return error.InvalidTeacherTopK;
    if (options.temperature <= 0 or std.math.isNan(options.temperature)) return error.InvalidTeacherTemperature;

    const graph_config = try gemma4_real.loadGraphConfig(allocator, base_model_dir);
    const vocab_size: usize = @intCast(graph_config.vocab_size);
    if (options.top_k > vocab_size) return error.InvalidTeacherTopK;
    const seq_len = prepared.max_seq_len;
    if (seq_len == 0) return error.InvalidPreparedInputLength;

    var backend = try gemma4_real.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();
    const tokenizer = try loadTokenizerForModelDir(allocator, base_model_dir);
    var ctx = MultimodalCtx.init(
        allocator,
        backend.backendPtr(),
        graph_config,
        gguf_projector_path,
        gguf_projector_sha256,
        tokenizer,
    );
    defer ctx.deinit();

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const ids_shape = Shape.init(.f32, &.{ 1, @as(i64, @intCast(seq_len)) });
    const input_ids_node = try bld.parameter("__teacher_input_ids", ids_shape);
    const attention_mask_node = try bld.parameter("__teacher_attention_mask", ids_shape);
    const hidden = try MultimodalCtx.buildForward(@ptrCast(&ctx), &bld, input_ids_node, attention_mask_node, 1, @intCast(seq_len));
    const logits_node = try ctx.buildLogits(&bld, hidden);
    try graph.markOutput(logits_node);
    const embeddings_node = graph_weight_bridge.findParameterByName(&graph, "__input_embeddings") orelse return error.MissingInputEmbeddings;

    const limit = if (options.max_examples > 0 and options.max_examples < prepared.examples.len) options.max_examples else prepared.examples.len;
    var summary = gemma4_real.TeacherTopKSummary{
        .top_k = options.top_k,
        .temperature = options.temperature,
    };
    for (prepared.examples[0..limit]) |*example| {
        if (example.input_ids.len == 0) continue;
        const logits = try forwardTeacherLogitsForExample(
            allocator,
            backend.backendPtr(),
            &graph,
            input_ids_node,
            attention_mask_node,
            embeddings_node,
            &ctx,
            example,
            seq_len,
            vocab_size,
        );
        defer allocator.free(logits);

        const token_ids = try allocator.alloc(i32, seq_len * options.top_k);
        errdefer allocator.free(token_ids);
        const probs = try allocator.alloc(f32, seq_len * options.top_k);
        errdefer allocator.free(probs);
        try gemma4_real.fillTopKFromLogits(token_ids, probs, logits, seq_len, vocab_size, options.top_k, options.temperature);

        gemma4_real.replaceTeacherTargets(allocator, example, token_ids, probs, options.top_k, options.temperature);
        summary.examples_written += 1;
        summary.supervised_tokens_seen += example.num_supervised_tokens;
    }
    summary.examples_seen = limit;
    return summary;
}

fn forwardTeacherLogitsForExample(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph: *const ml.graph.Graph,
    input_ids_node: ml.graph.NodeId,
    attention_mask_node: ml.graph.NodeId,
    embeddings_node: ml.graph.NodeId,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: usize,
    vocab_size: usize,
) ![]f32 {
    const input_ids = try allocator.alloc(f32, seq_len);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(f32, seq_len);
    defer allocator.free(attention_mask);
    @memset(input_ids, 0.0);
    @memset(attention_mask, 0.0);
    const active_len = @min(example.num_input_tokens, example.input_ids.len);
    const usable = @min(active_len, seq_len);
    for (0..usable) |idx| {
        input_ids[idx] = @floatFromInt(example.input_ids[idx]);
        attention_mask[idx] = 1.0;
    }
    const embeddings = try buildExampleEmbeddings(allocator, ctx, example, usable, seq_len);
    defer allocator.free(embeddings);

    const dims = [_]i32{ 1, @intCast(seq_len) };
    const input_ct = try cb.fromFloat32Shape(input_ids, &dims);
    defer cb.free(input_ct);
    const mask_ct = try cb.fromFloat32Shape(attention_mask, &dims);
    defer cb.free(mask_ct);
    const embedding_dims = [_]i32{ 1, @intCast(seq_len), @intCast(ctx.graph_config.hidden_size) };
    const embeddings_ct = try cb.fromFloat32Shape(embeddings, &embedding_dims);
    defer cb.free(embeddings_ct);
    const rt_inputs = [_]interpreter.RuntimeInput{
        .{ .node_id = input_ids_node, .value = input_ct },
        .{ .node_id = attention_mask_node, .value = mask_ct },
        .{ .node_id = embeddings_node, .value = embeddings_ct },
    };
    var exec_result = try interpreter.execute(allocator, graph, cb, .{ .runtime_inputs = &rt_inputs });
    defer exec_result.deinit(cb);
    if (exec_result.outputs.len != 1) return error.InvalidTeacherLogits;
    const logits = try cb.toFloat32(exec_result.outputs[0], allocator);
    errdefer allocator.free(logits);
    if (logits.len != seq_len * vocab_size) return error.InvalidTeacherLogits;
    return logits;
}

fn runPreparedExamples(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *MultimodalCtx,
    examples: []const gemma4.PreparedExampleInput,
    max_examples: usize,
    seq_len: u32,
    mode: enum { train, eval },
) !@import("gemma4_real_autodiff.zig").CausalLmMetrics {
    var metrics: @import("gemma4_real_autodiff.zig").CausalLmMetrics = .{};
    const limit = if (max_examples > 0 and max_examples < examples.len) max_examples else examples.len;
    var total_weighted_loss: f64 = 0;
    var total_weighted_grad_norm: f64 = 0;
    var total_weighted_teacher_temperature: f64 = 0;

    for (examples[0..limit]) |*example| {
        if (example.num_supervised_tokens == 0) continue;
        var input = try makeTrainerInputForExample(allocator, ctx, example, seq_len);
        defer input.deinit(allocator);
        const step = switch (mode) {
            .train => try trainer.step(input.trainer_input),
            .eval => try trainer.evaluate(input.trainer_input),
        };
        const weight: f64 = @floatFromInt(example.num_supervised_tokens);
        metrics.examples_seen += 1;
        metrics.supervised_tokens_seen += example.num_supervised_tokens;
        if (exampleHasTeacherTargets(example)) {
            metrics.teacher_examples_seen += 1;
            metrics.teacher_supervised_tokens_seen += example.num_supervised_tokens;
            total_weighted_teacher_temperature += @as(f64, example.teacher_temperature) * weight;
        }
        total_weighted_loss += @as(f64, step.loss) * weight;
        total_weighted_grad_norm += @as(f64, step.grad_norm) * weight;
        if (step.optimizer_stepped) metrics.optimizer_steps += 1;
    }

    if (metrics.supervised_tokens_seen > 0) {
        const denom: f64 = @floatFromInt(metrics.supervised_tokens_seen);
        metrics.average_loss = total_weighted_loss / denom;
        metrics.mean_grad_norm = total_weighted_grad_norm / denom;
    }
    if (metrics.teacher_supervised_tokens_seen > 0) {
        metrics.mean_teacher_temperature = total_weighted_teacher_temperature / @as(f64, @floatFromInt(metrics.teacher_supervised_tokens_seen));
    }
    return metrics;
}

fn exampleHasTeacherTargets(example: *const gemma4.PreparedExampleInput) bool {
    return example.teacher_top_k > 0 and
        example.teacher_top_k_token_ids.len > 0 and
        example.teacher_top_k_probs.len > 0;
}

fn buildExampleEmbeddings(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    example: *const gemma4.PreparedExampleInput,
    active_len: usize,
    rows: usize,
) ![]f32 {
    if (active_len > example.input_ids.len) return error.InvalidTensorShape;
    const active_input_ids = example.input_ids[0..active_len];
    if (example.image_paths.len == 0 and example.audio_paths.len == 0) {
        return buildTextOnlyEmbeddings(allocator, ctx, active_input_ids, rows);
    }
    const image_bytes = try loadMediaBytes(allocator, example.image_paths);
    defer freeMediaBytes(allocator, image_bytes);
    const audio_bytes = try loadMediaBytes(allocator, example.audio_paths);
    defer freeMediaBytes(allocator, audio_bytes);

    var projected_images = if (image_bytes.len > 0)
        try projectImagesWithCache(allocator, ctx, image_bytes)
    else
        null;
    defer if (projected_images) |*images| images.deinit();
    var projected_audio = if (audio_bytes.len > 0)
        try projectAudioWithCache(allocator, ctx, audio_bytes)
    else
        null;
    defer if (projected_audio) |*audio| audio.deinit();

    if (projected_images) |*images| {
        if (!std.mem.eql(usize, images.tokens_per_image, example.image_token_counts)) return error.ImagePlaceholderCountMismatch;
    }
    if (projected_audio) |*audio| {
        if (!std.mem.eql(usize, audio.tokens_per_audio, example.audio_token_counts)) return error.AudioPlaceholderCountMismatch;
    }

    var prepared = try gemma4_mm.prepareExpandedPromptEmbeddings(
        ctx.compute_backend,
        allocator,
        ctx.tokenizer.tokenizer(),
        ctx.graph_config,
        active_input_ids,
        if (projected_images) |*images| images else null,
        if (projected_audio) |*audio| audio else null,
    );
    defer prepared.deinit(ctx.compute_backend);
    const input_embeddings = prepared.input_embeddings orelse return error.InvalidPreparedPrompt;
    const embeddings = try ctx.compute_backend.toFloat32(input_embeddings, allocator);
    errdefer allocator.free(embeddings);
    return padEmbeddingRows(allocator, embeddings, rows, ctx.graph_config.hidden_size);
}

fn buildTextOnlyEmbeddings(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    input_ids: []const i32,
    rows: usize,
) ![]f32 {
    const usable = @min(input_ids.len, rows);
    const ids64 = try allocator.alloc(i64, usable);
    defer allocator.free(ids64);
    for (input_ids[0..usable], 0..) |id, idx| ids64[idx] = id;
    var name_buf: [256]u8 = undefined;
    const embed_name = try prefixedModelName(&name_buf, ctx.graph_config, "model.embed_tokens.weight");
    const embed_w = try ctx.compute_backend.getWeight(embed_name);
    defer ctx.compute_backend.free(embed_w);
    const embedded = try ctx.compute_backend.embeddingLookup(embed_w, ids64, usable, ctx.graph_config.hidden_size);
    defer ctx.compute_backend.free(embedded);
    const out = try ctx.compute_backend.toFloat32(embedded, allocator);
    errdefer allocator.free(out);
    const scale = ctx.graph_config.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) {
        for (out) |*value| value.* *= scale;
    }
    return padEmbeddingRows(allocator, out, rows, ctx.graph_config.hidden_size);
}

fn padEmbeddingRows(
    allocator: std.mem.Allocator,
    embeddings: []f32,
    rows: usize,
    hidden_size: usize,
) ![]f32 {
    const expected = rows * hidden_size;
    if (embeddings.len > expected or embeddings.len % hidden_size != 0) return error.InvalidTensorShape;
    if (embeddings.len == expected) return embeddings;

    const padded = try allocator.alloc(f32, expected);
    @memset(padded, 0.0);
    @memcpy(padded[0..embeddings.len], embeddings);
    allocator.free(embeddings);
    return padded;
}

fn prefixedModelName(buf: *[256]u8, config: gemma_graph.Config, name: []const u8) ![]const u8 {
    if (config.weight_prefix.len == 0 or !std.mem.startsWith(u8, name, "model.")) return name;
    return std.fmt.bufPrint(buf, "{s}.{s}", .{ config.weight_prefix, name["model.".len..] }) catch error.NameTooLong;
}

fn projectImagesWithCache(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    image_bytes: []const []const u8,
) !gemma4_projector.ProjectedImages {
    var all_embeddings = std.ArrayListUnmanaged(f32).empty;
    errdefer all_embeddings.deinit(allocator);
    var tokens_per_image = try allocator.alloc(usize, image_bytes.len);
    errdefer allocator.free(tokens_per_image);
    var hidden_size: usize = 0;

    for (image_bytes, 0..) |bytes, idx| {
        const cached = try cachedProjectedMedia(allocator, ctx, .image, bytes);
        if (hidden_size == 0) hidden_size = cached.hidden_size else if (hidden_size != cached.hidden_size) return error.InvalidTensorShape;
        try all_embeddings.appendSlice(allocator, cached.embeddings);
        tokens_per_image[idx] = cached.tokens;
    }

    return .{
        .allocator = allocator,
        .embeddings = try all_embeddings.toOwnedSlice(allocator),
        .tokens_per_image = tokens_per_image,
        .hidden_size = hidden_size,
    };
}

fn projectAudioWithCache(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    audio_bytes: []const []const u8,
) !gemma4_projector.ProjectedAudio {
    var all_embeddings = std.ArrayListUnmanaged(f32).empty;
    errdefer all_embeddings.deinit(allocator);
    var tokens_per_audio = try allocator.alloc(usize, audio_bytes.len);
    errdefer allocator.free(tokens_per_audio);
    var hidden_size: usize = 0;

    for (audio_bytes, 0..) |bytes, idx| {
        const cached = try cachedProjectedMedia(allocator, ctx, .audio, bytes);
        if (hidden_size == 0) hidden_size = cached.hidden_size else if (hidden_size != cached.hidden_size) return error.InvalidTensorShape;
        try all_embeddings.appendSlice(allocator, cached.embeddings);
        tokens_per_audio[idx] = cached.tokens;
    }

    return .{
        .allocator = allocator,
        .embeddings = try all_embeddings.toOwnedSlice(allocator),
        .tokens_per_audio = tokens_per_audio,
        .hidden_size = hidden_size,
    };
}

fn cachedProjectedMedia(
    allocator: std.mem.Allocator,
    ctx: *MultimodalCtx,
    kind: MediaKind,
    bytes: []const u8,
) !*const CachedProjectedMedia {
    const media_sha256 = try gemma4.sha256HexAlloc(allocator, bytes);
    defer allocator.free(media_sha256);
    const kind_name = switch (kind) {
        .image => "image",
        .audio => "audio",
    };
    const lookup_key = try std.fmt.allocPrint(allocator, "{s}:{s}:{s}", .{ ctx.gguf_projector_sha256, kind_name, media_sha256 });
    defer allocator.free(lookup_key);
    if (ctx.projected_media_cache.items.getPtr(lookup_key)) |cached| {
        ctx.projected_media_cache_hits += 1;
        return cached;
    }
    ctx.projected_media_cache_misses += 1;

    const owned_key = try allocator.dupe(u8, lookup_key);
    errdefer allocator.free(owned_key);
    const projected = switch (kind) {
        .image => blk: {
            var images = try gemma4_projector.encodeProjectedImages(ctx.compute_backend, allocator, ctx.gguf_projector_path, &.{bytes});
            defer images.deinit();
            if (images.tokens_per_image.len != 1) return error.InvalidPreparedPrompt;
            break :blk CachedProjectedMedia{
                .embeddings = try allocator.dupe(f32, images.embeddings),
                .tokens = images.tokens_per_image[0],
                .hidden_size = images.hidden_size,
            };
        },
        .audio => blk: {
            var audio = try gemma4_projector.encodeProjectedAudio(ctx.compute_backend, allocator, ctx.gguf_projector_path, &.{bytes});
            defer audio.deinit();
            if (audio.tokens_per_audio.len != 1) return error.InvalidPreparedPrompt;
            break :blk CachedProjectedMedia{
                .embeddings = try allocator.dupe(f32, audio.embeddings),
                .tokens = audio.tokens_per_audio[0],
                .hidden_size = audio.hidden_size,
            };
        },
    };
    errdefer allocator.free(projected.embeddings);
    const entry = try ctx.projected_media_cache.items.getOrPut(allocator, owned_key);
    if (entry.found_existing) {
        allocator.free(owned_key);
        allocator.free(projected.embeddings);
    } else {
        entry.key_ptr.* = owned_key;
        entry.value_ptr.* = projected;
    }
    return entry.value_ptr;
}

fn loadMediaBytes(allocator: std.mem.Allocator, paths: []const []const u8) ![]const []const u8 {
    if (paths.len == 0) return &.{};
    const out = try allocator.alloc([]const u8, paths.len);
    errdefer allocator.free(out);
    var loaded: usize = 0;
    errdefer {
        for (out[0..loaded]) |item| allocator.free(item);
    }
    for (paths, 0..) |path, idx| {
        out[idx] = try c_file.readFile(allocator, path);
        loaded += 1;
    }
    return out;
}

fn freeMediaBytes(allocator: std.mem.Allocator, items: []const []const u8) void {
    if (items.len == 0) return;
    for (items) |item| allocator.free(item);
    allocator.free(items);
}
