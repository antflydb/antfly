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
const c_file = @import("../util/c_file.zig");
const gemma_graph = @import("../architectures/gemma_graph.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const gemma4 = @import("gemma4.zig");
const compat = @import("../io/compat.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const session_factory = @import("../architectures/session_factory.zig");
const Session = @import("../backends/session.zig").Session;
const ops_mod = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;

const Builder = ml.graph.Builder;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

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

pub const CausalLmMetrics = struct {
    examples_seen: usize = 0,
    supervised_tokens_seen: usize = 0,
    teacher_examples_seen: usize = 0,
    teacher_supervised_tokens_seen: usize = 0,
    mean_teacher_temperature: f64 = 0,
    average_loss: f64 = 0,
    mean_grad_norm: f64 = 0,
    optimizer_steps: usize = 0,
};

pub const TeacherTopKOptions = struct {
    top_k: usize = 8,
    temperature: f32 = 1.0,
    max_examples: usize = 0,
};

pub const TeacherTopKSummary = struct {
    examples_seen: usize = 0,
    examples_written: usize = 0,
    supervised_tokens_seen: usize = 0,
    top_k: usize = 0,
    temperature: f32 = 1.0,
};

pub const BackendKind = enum { native, cuda };

pub const LossTargetEncoding = enum {
    dense_distribution,
    sparse_token,
};

pub const LoadedBackend = struct {
    kind: BackendKind,
    compute_backend: ComputeBackend,
    session: Session,

    pub fn backendPtr(self: *LoadedBackend) *const ComputeBackend {
        return &self.compute_backend;
    }

    pub fn deinit(self: *LoadedBackend) void {
        self.compute_backend.deinit();
        self.session.close();
        self.* = undefined;
    }
};

pub const GemmaAutodiffCtx = struct {
    graph_config: gemma_graph.Config,
    graph_options: gemma_graph.BuildOptions = .{},
    loss_target_encoding: LossTargetEncoding = .dense_distribution,
    built: ?gemma_graph.GemmaGraph = null,
    lm_logits: ?NodeId = null,

    pub fn init(graph_config: gemma_graph.Config) GemmaAutodiffCtx {
        return .{ .graph_config = graph_config };
    }

    /// Preference objectives supervise exactly one token per causal row.
    /// Keep those labels sparse instead of uploading a sequence-by-vocabulary
    /// one-hot tensor on every DPO/GRPO microstep.
    pub fn initPreference(graph_config: gemma_graph.Config) GemmaAutodiffCtx {
        return .{
            .graph_config = graph_config,
            .loss_target_encoding = .sparse_token,
        };
    }

    pub fn initRecursive(graph_config: gemma_graph.Config, shared_block_size: usize) GemmaAutodiffCtx {
        return .{
            .graph_config = graph_config,
            .graph_options = .{ .recursive_shared_block_size = @intCast(shared_block_size) },
        };
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
        const self: *GemmaAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        self.built = try gemma_graph.buildForwardGraphWithOptions(bld, self.graph_config, batch, seq_len, .{
            .input_ids = input_ids,
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
        const self: *GemmaAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const logits = try self.buildLogits(bld, forward_output);
        return switch (self.loss_target_encoding) {
            .dense_distribution => bld.crossEntropyLoss(logits, targets),
            .sparse_token => self.buildSparseTokenLoss(bld, logits, targets),
        };
    }

    fn buildSparseTokenLoss(
        self: *GemmaAutodiffCtx,
        bld: *Builder,
        logits: NodeId,
        targets: NodeId,
    ) !NodeId {
        _ = self;
        const logits_shape = bld.graph.node(logits).output_shape;
        const targets_shape = bld.graph.node(targets).output_shape;
        if (logits_shape.rank() != 2 or targets_shape.rank() != 2) return error.ShapeMismatch;
        const rows = logits_shape.dim(0);
        const vocab_size = logits_shape.dim(1);
        if (rows <= 0 or vocab_size <= 0 or targets_shape.dim(0) != rows or targets_shape.dim(1) != 2) {
            return error.ShapeMismatch;
        }

        // targets[:, 0] is the vocabulary token ID and targets[:, 1] is
        // the (possibly signed) cross-entropy coefficient. Transpose first
        // so the ordinary axis-0 gather and its CUDA scatter-add VJP can be
        // used without a host fallback.
        const log_probs = try bld.logSoftmax(logits);
        const transposed = try bld.transpose(log_probs, &.{ 1, 0 });
        const token_ids_matrix = try bld.sliceLastDim(targets, 0, 1);
        const token_ids = try bld.reshape(token_ids_matrix, Shape.init(.f32, &.{rows}));
        const candidates = try bld.gather(
            transposed,
            token_ids,
            Shape.init(.f32, &.{ rows, rows }),
        );

        // candidates[token_slot, causal_row] contains the log-probability
        // of token_ids[token_slot] at causal_row. A second axis-0 gather of
        // its diagonal selects one exact token per row. Keeping this second
        // index below 2^24 preserves exact f32 integer representation used
        // by the CUDA graph ABI.
        const row_count: usize = @intCast(rows);
        const candidate_count = std.math.mul(usize, row_count, row_count) catch
            return error.SparseTargetSequenceTooLong;
        if (candidate_count == 0 or candidate_count - 1 > (1 << 24)) {
            return error.SparseTargetSequenceTooLong;
        }
        const candidate_flat = try bld.reshape(candidates, Shape.init(.f32, &.{ @as(i64, @intCast(candidate_count)), 1 }));
        const diagonal_data = try bld.graph.allocator.alloc(f32, row_count);
        defer bld.graph.allocator.free(diagonal_data);
        for (diagonal_data, 0..) |*index, row| index.* = @floatFromInt(row * row_count + row);
        const diagonal_indices = try bld.tensorConst(diagonal_data, Shape.init(.f32, &.{rows}));
        const selected = try bld.gather(candidate_flat, diagonal_indices, Shape.init(.f32, &.{ rows, 1 }));
        const scales = try bld.sliceLastDim(targets, 1, 2);
        const weighted = try bld.mul(scales, selected);
        return bld.neg(try bld.reduceMean(weighted, &.{ 0, 1 }));
    }

    fn targetsShape(self: *const GemmaAutodiffCtx, rows: usize) Shape {
        return switch (self.loss_target_encoding) {
            .dense_distribution => Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(self.graph_config.vocab_size)) }),
            .sparse_token => Shape.init(.f32, &.{ @as(i64, @intCast(rows)), 2 }),
        };
    }

    pub fn buildLogits(
        self: *GemmaAutodiffCtx,
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
        const self: *GemmaAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        if (self.built) |*built| {
            built.input_ids_node = id_map[built.input_ids_node];
            if (built.rope_cos_node != ml.graph.null_node) built.rope_cos_node = id_map[built.rope_cos_node];
            if (built.rope_sin_node != ml.graph.null_node) built.rope_sin_node = id_map[built.rope_sin_node];
            built.output_node = id_map[built.output_node];
        }
        if (self.lm_logits) |node_id| self.lm_logits = id_map[node_id];
    }
};

fn prefixedModelName(buf: *[256]u8, config: gemma_graph.Config, name: []const u8) ![]const u8 {
    if (config.weight_prefix.len == 0 or !std.mem.startsWith(u8, name, "model.")) return name;
    return std.fmt.bufPrint(buf, "{s}.{s}", .{ config.weight_prefix, name["model.".len..] }) catch error.NameTooLong;
}

pub const OwnedTrainerInput = struct {
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
    trainer_input: real_autodiff.TrainerInput,

    pub fn deinit(self: *OwnedTrainerInput, allocator: std.mem.Allocator) void {
        allocator.free(self.input_ids);
        allocator.free(self.attention_mask);
        allocator.free(self.targets);
        self.* = undefined;
    }
};

pub fn loadGraphConfig(allocator: std.mem.Allocator, model_dir: []const u8) !gemma_graph.Config {
    const config = try session_factory.loadGptConfigMetadataFromModelDir(allocator, model_dir);
    try gemma_graph.validateConfig(config);
    return config;
}

pub fn loadBackendForModelDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend_kind: BackendKind,
) !LoadedBackend {
    const session = switch (backend_kind) {
        .native => try session_factory.createNativeSession(allocator, model_dir),
        .cuda => try session_factory.createGemma4CudaTrainingSession(allocator, model_dir),
    };
    errdefer session.close();
    const compute_backend = try session_factory.getComputeBackend(session, allocator);
    errdefer compute_backend.deinit();
    return .{
        .kind = backend_kind,
        .compute_backend = compute_backend,
        .session = session,
    };
}

/// Frozen base-policy scorer that reuses an already loaded compute backend.
/// Its graph contains no LoRA nodes, so it is a correct reference policy even
/// while a separate trainer updates adapters against the same resident base
/// weights. This avoids a second CUDA checkpoint and avoids CPU reference
/// forwards when DPO/GRPO use the policy checkpoint as their reference.
pub const FrozenBaseScorer = struct {
    allocator: std.mem.Allocator,
    compute_backend: *const ComputeBackend,
    graph: Graph,
    input_ids_node: NodeId,
    attention_mask_node: NodeId,
    hidden_shape: Shape,
    graph_config: gemma_graph.Config,
    seq_len: usize,
    vocab_size: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        compute_backend: *const ComputeBackend,
        graph_config: gemma_graph.Config,
        seq_len: usize,
    ) !FrozenBaseScorer {
        if (seq_len == 0) return error.InvalidPreparedInputLength;
        var graph = Graph.init(allocator);
        errdefer graph.deinit();
        var bld = Builder.init(&graph);
        const input_shape = Shape.init(.f32, &.{ 1, @as(i64, @intCast(seq_len)) });
        const input_ids_node = try bld.parameter("__frozen_reference_input_ids", input_shape);
        const attention_mask_node = try bld.parameter("__frozen_reference_attention_mask", input_shape);
        var ctx = GemmaAutodiffCtx.init(graph_config);
        const hidden = try GemmaAutodiffCtx.buildForward(
            @ptrCast(&ctx),
            &bld,
            input_ids_node,
            attention_mask_node,
            1,
            @intCast(seq_len),
        );
        try graph.markOutput(hidden);
        return .{
            .allocator = allocator,
            .compute_backend = compute_backend,
            .graph = graph,
            .input_ids_node = input_ids_node,
            .attention_mask_node = attention_mask_node,
            .hidden_shape = graph.node(hidden).output_shape,
            .graph_config = graph_config,
            .seq_len = seq_len,
            .vocab_size = @intCast(graph_config.vocab_size),
        };
    }

    pub fn deinit(self: *FrozenBaseScorer) void {
        self.graph.deinit();
        self.* = undefined;
    }

    fn selectedLogprobsForInputIds(
        self: *FrozenBaseScorer,
        raw_input_ids: []const i32,
        row_indices: []const usize,
        token_ids: []const i32,
    ) ![]f32 {
        if (raw_input_ids.len == 0) return error.EmptyPrompt;
        if (raw_input_ids.len > self.seq_len) return error.SequenceTooLong;

        const input_ids = try self.allocator.alloc(f32, self.seq_len);
        defer self.allocator.free(input_ids);
        @memset(input_ids, 0.0);
        const attention_mask = try self.allocator.alloc(f32, self.seq_len);
        defer self.allocator.free(attention_mask);
        @memset(attention_mask, 0.0);
        for (raw_input_ids, 0..) |token_id, idx| {
            if (token_id < 0) return error.LabelOutOfRange;
            input_ids[idx] = @floatFromInt(token_id);
            attention_mask[idx] = 1.0;
        }

        const dims = [_]i32{ 1, @intCast(self.seq_len) };
        const input_ct = try self.compute_backend.fromFloat32Shape(input_ids, &dims);
        defer self.compute_backend.free(input_ct);
        const mask_ct = try self.compute_backend.fromFloat32Shape(attention_mask, &dims);
        defer self.compute_backend.free(mask_ct);
        const runtime_inputs = [_]interpreter.RuntimeInput{
            .{ .node_id = self.input_ids_node, .value = input_ct },
            .{ .node_id = self.attention_mask_node, .value = mask_ct },
        };
        var result = try interpreter.execute(
            self.allocator,
            &self.graph,
            self.compute_backend,
            .{ .runtime_inputs = &runtime_inputs },
        );
        defer result.deinit(self.compute_backend);
        if (result.outputs.len != 1) return error.InvalidReferenceLogits;
        return readSelectedTokenLogprobs(
            self.allocator,
            self.compute_backend,
            self.graph_config,
            result.outputs[0],
            self.hidden_shape,
            row_indices,
            token_ids,
        );
    }

    pub fn sequenceLogprobForExample(
        self: *FrozenBaseScorer,
        example: *const gemma4.PreparedExampleInput,
    ) !f32 {
        const usable = @min(example.input_ids.len, self.seq_len);
        if (usable == 0) return error.EmptyPrompt;
        var row_indices = std.ArrayList(usize).empty;
        defer row_indices.deinit(self.allocator);
        var token_ids = std.ArrayList(i32).empty;
        defer token_ids.deinit(self.allocator);
        const rows = @min(example.labels.len, self.seq_len);
        for (0..rows) |row_idx| {
            const label = example.labels[row_idx];
            if (label < 0) continue;
            if (row_idx == 0) return error.InvalidCausalLabelPosition;
            const token_idx: usize = @intCast(label);
            if (token_idx >= self.vocab_size) return error.LabelOutOfRange;
            try row_indices.append(self.allocator, row_idx - 1);
            try token_ids.append(self.allocator, label);
        }
        if (token_ids.items.len == 0) return 0.0;
        const logps = try self.selectedLogprobsForInputIds(
            example.input_ids[0..usable],
            row_indices.items,
            token_ids.items,
        );
        defer self.allocator.free(logps);
        var sum_logp: f32 = 0.0;
        for (logps) |logp| sum_logp += logp;
        return sum_logp;
    }

    pub fn tokenLogprobs(
        self: *FrozenBaseScorer,
        prompt: []const i32,
        completion: []const i32,
        out_per_token_logp: []f32,
    ) !void {
        if (prompt.len == 0) return error.EmptyPrompt;
        if (completion.len == 0) return error.EmptyCompletion;
        if (completion.len != out_per_token_logp.len) return error.LogpLenMismatch;
        const total_len = std.math.add(usize, prompt.len, completion.len) catch return error.SequenceTooLong;
        if (total_len > self.seq_len) return error.SequenceTooLong;

        const input_ids = try self.allocator.alloc(i32, total_len);
        defer self.allocator.free(input_ids);
        @memcpy(input_ids[0..prompt.len], prompt);
        @memcpy(input_ids[prompt.len..], completion);
        const row_indices = try self.allocator.alloc(usize, completion.len);
        defer self.allocator.free(row_indices);
        for (row_indices, 0..) |*row, completion_idx| row.* = prompt.len + completion_idx - 1;
        const logps = try self.selectedLogprobsForInputIds(input_ids, row_indices, completion);
        defer self.allocator.free(logps);
        if (logps.len != out_per_token_logp.len) return error.LogpLenMismatch;
        @memcpy(out_per_token_logp, logps);
    }
};

pub fn makeTrainerInputForExample(
    allocator: std.mem.Allocator,
    ctx: *GemmaAutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !OwnedTrainerInput {
    const examples = [_]gemma4.PreparedExampleInput{example.*};
    return makeTrainerInputForExamplesWeighted(allocator, ctx, &examples, seq_len, null, null);
}

pub fn makeTrainerInputForExampleScaled(
    allocator: std.mem.Allocator,
    ctx: *GemmaAutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    token_scale_override: ?f32,
) !OwnedTrainerInput {
    const examples = [_]gemma4.PreparedExampleInput{example.*};
    return makeTrainerInputForExamplesWeighted(allocator, ctx, &examples, seq_len, token_scale_override, null);
}

pub fn makeTrainerInputForTokenLogprobGrads(
    allocator: std.mem.Allocator,
    ctx: *GemmaAutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    logprob_grads: []const f32,
) !OwnedTrainerInput {
    const examples = [_]gemma4.PreparedExampleInput{example.*};
    return makeTrainerInputForTokenLogprobGradsBatch(allocator, ctx, &examples, seq_len, logprob_grads);
}

/// Build one preference backward input containing several independent padded
/// sequences. `logprob_grads` is flattened in example order and contains one
/// derivative for every supervised completion token. The sparse loss reduces
/// over all batch rows, so scale each coefficient by batch * sequence length
/// to preserve the caller's exact derivative with respect to token log-prob.
pub fn makeTrainerInputForTokenLogprobGradsBatch(
    allocator: std.mem.Allocator,
    ctx: *GemmaAutodiffCtx,
    examples: []const gemma4.PreparedExampleInput,
    seq_len: u32,
    logprob_grads: []const f32,
) !OwnedTrainerInput {
    if (examples.len == 0) return error.EmptyTrainingBatch;
    const rows = std.math.mul(usize, examples.len, @as(usize, @intCast(seq_len))) catch
        return error.InvalidTensorShape;
    const rows_f: f32 = @floatFromInt(rows);
    const token_scales = try allocator.alloc(f32, logprob_grads.len);
    defer allocator.free(token_scales);
    for (logprob_grads, 0..) |grad, idx| token_scales[idx] = -grad * rows_f;
    return makeTrainerInputForExamplesWeighted(allocator, ctx, examples, seq_len, null, token_scales);
}

fn makeTrainerInputForExamplesWeighted(
    allocator: std.mem.Allocator,
    ctx: *GemmaAutodiffCtx,
    examples: []const gemma4.PreparedExampleInput,
    seq_len: u32,
    token_scale_override: ?f32,
    token_scales: ?[]const f32,
) !OwnedTrainerInput {
    if (examples.len == 0) return error.EmptyTrainingBatch;
    const batch = std.math.cast(u32, examples.len) orelse return error.InvalidTensorShape;
    const seq_len_usize: usize = @intCast(seq_len);
    const vocab_size: usize = @intCast(ctx.graph_config.vocab_size);
    const rows = std.math.mul(usize, examples.len, seq_len_usize) catch return error.InvalidTensorShape;

    const input_ids = try allocator.alloc(i64, rows);
    errdefer allocator.free(input_ids);
    @memset(input_ids, 0);
    const attention_mask = try allocator.alloc(f32, rows);
    errdefer allocator.free(attention_mask);
    @memset(attention_mask, 0.0);

    for (examples, 0..) |example, batch_idx| {
        const row_base = batch_idx * seq_len_usize;
        const usable = @min(example.input_ids.len, seq_len_usize);
        for (0..usable) |i| {
            input_ids[row_base + i] = example.input_ids[i];
            attention_mask[row_base + i] = 1.0;
        }
    }

    const target_count = switch (ctx.loss_target_encoding) {
        .dense_distribution => std.math.mul(usize, rows, vocab_size) catch return error.InvalidTensorShape,
        .sparse_token => std.math.mul(usize, rows, 2) catch return error.InvalidTensorShape,
    };
    const targets = try allocator.alloc(f32, target_count);
    errdefer allocator.free(targets);
    @memset(targets, 0.0);

    const use_teacher_targets = token_scales == null and token_scale_override == null;
    if (ctx.loss_target_encoding == .sparse_token and use_teacher_targets) {
        for (examples) |*example| {
            if (exampleHasTeacherTargets(example)) return error.SparseTargetsDoNotSupportTeacherDistributions;
        }
    }
    // Batched soft teacher distributions are not currently exposed. All
    // preference callers provide explicit per-token derivatives.
    const filled_teacher_targets = examples.len == 1 and
        ctx.loss_target_encoding == .dense_distribution and
        use_teacher_targets and try fillTeacherTopKTargets(targets, rows, vocab_size, &examples[0]);
    if (!filled_teacher_targets) {
        var valid_tokens: usize = 0;
        for (examples) |example| {
            for (0..@min(example.labels.len, seq_len_usize)) |i| {
                if (example.labels[i] != -100) valid_tokens += 1;
            }
        }
        if (token_scales) |scales| {
            if (scales.len != valid_tokens) return error.GradientShapeMismatch;
        }
        const default_row_scale: f32 = token_scale_override orelse if (valid_tokens == 0)
            0.0
        else
            @as(f32, @floatFromInt(rows)) / @as(f32, @floatFromInt(valid_tokens));

        var supervised_idx: usize = 0;
        for (examples, 0..) |example, batch_idx| {
            const row_base = batch_idx * seq_len_usize;
            for (0..@min(example.labels.len, seq_len_usize)) |i| {
                const label = example.labels[i];
                if (label < 0) continue;
                if (i == 0) return error.InvalidCausalLabelPosition;
                const idx: usize = @intCast(label);
                if (idx >= vocab_size) return error.LabelOutOfRange;
                const row_scale = if (token_scales) |scales| scales[supervised_idx] else default_row_scale;
                const causal_row = row_base + i - 1;
                switch (ctx.loss_target_encoding) {
                    .dense_distribution => targets[causal_row * vocab_size + idx] = row_scale,
                    .sparse_token => {
                        targets[causal_row * 2] = @floatFromInt(idx);
                        targets[causal_row * 2 + 1] = row_scale;
                    },
                }
                supervised_idx += 1;
            }
        }
    }

    return .{
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .trainer_input = .{
            .ctx = @ptrCast(ctx),
            .build_forward = &GemmaAutodiffCtx.buildForward,
            .build_loss = &GemmaAutodiffCtx.buildLoss,
            .input_ids = input_ids,
            .attention_mask = attention_mask,
            .targets = targets,
            .targets_shape = ctx.targetsShape(rows),
            .batch = batch,
            .seq_len = seq_len,
            .bind_arch_inputs = null,
            .remap_graph_nodes = &GemmaAutodiffCtx.remapGraphNodes,
        },
    };
}

pub fn makeTrainerInputForLogprobCoeff(
    allocator: std.mem.Allocator,
    ctx: *GemmaAutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    logprob_coeff: f32,
) !OwnedTrainerInput {
    const rows_f: f32 = @floatFromInt(seq_len);
    return makeTrainerInputForExampleScaled(allocator, ctx, example, seq_len, -logprob_coeff * rows_f);
}

pub fn tokenLogprobsForPromptCompletion(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    prompt: []const i32,
    completion: []const i32,
    seq_len: u32,
    out_logps: []f32,
) !void {
    if (completion.len != out_logps.len) return error.LogpLenMismatch;
    if (prompt.len == 0) return error.EmptyPrompt;
    if (completion.len == 0) return error.EmptyCompletion;
    const total_len = prompt.len + completion.len;
    if (total_len > seq_len) return error.SequenceTooLong;

    const joined = try concatPromptCompletion(allocator, prompt, completion);
    defer allocator.free(joined);
    const row_indices = try allocator.alloc(usize, completion.len);
    defer allocator.free(row_indices);
    for (row_indices, 0..) |*row, comp_idx| row.* = prompt.len + comp_idx - 1;
    const logps = try executePreferenceReadbackForInputIds(
        allocator,
        trainer,
        ctx,
        joined,
        seq_len,
        .{ .token_logprobs = .{ .row_indices = row_indices, .token_ids = completion } },
    );
    defer allocator.free(logps);
    if (logps.len != out_logps.len) return error.LogpLenMismatch;
    @memcpy(out_logps, logps);
}

pub fn sampleCompletionRanked(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    prompt: []const i32,
    seq_len: u32,
    max_completion_tokens: usize,
    rank: usize,
    eos_token_id: ?i32,
    out_tokens: *std.ArrayList(i32),
    out_logps: *std.ArrayList(f32),
) !void {
    if (prompt.len == 0) return error.EmptyPrompt;
    var seq = std.ArrayList(i32).empty;
    defer seq.deinit(allocator);
    try seq.appendSlice(allocator, prompt);

    var step: usize = 0;
    while (step < max_completion_tokens and seq.items.len < seq_len) : (step += 1) {
        const logits = try executePreferenceReadbackForInputIds(
            allocator,
            trainer,
            ctx,
            seq.items,
            seq_len,
            .{ .logits_row = seq.items.len - 1 },
        );
        defer allocator.free(logits);
        const token_id = try selectRankedToken(allocator, logits, rank);
        const token_logp = logProbAtToken(logits, token_id);
        try out_tokens.append(allocator, @intCast(token_id));
        try out_logps.append(allocator, token_logp);
        try seq.append(allocator, @intCast(token_id));
        if (eos_token_id) |eos_id| if (token_id == @as(usize, @intCast(eos_id))) break;
    }
    if (out_tokens.items.len == 0) return error.EmptyCompletion;
}

/// Draw a completion from the policy softmax and record the exact behavior
/// log-probability used by GRPO's importance ratio.
pub fn sampleCompletion(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    prompt: []const i32,
    seq_len: u32,
    max_completion_tokens: usize,
    seed: u64,
    eos_token_id: ?i32,
    out_tokens: *std.ArrayList(i32),
    out_logps: *std.ArrayList(f32),
) !void {
    if (prompt.len == 0) return error.EmptyPrompt;
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var seq = std.ArrayList(i32).empty;
    defer seq.deinit(allocator);
    try seq.appendSlice(allocator, prompt);

    var step: usize = 0;
    while (step < max_completion_tokens and seq.items.len < seq_len) : (step += 1) {
        const logits = try executePreferenceReadbackForInputIds(
            allocator,
            trainer,
            ctx,
            seq.items,
            seq_len,
            .{ .logits_row = seq.items.len - 1 },
        );
        defer allocator.free(logits);
        const token_id = try sampleToken(logits, random);
        try out_tokens.append(allocator, @intCast(token_id));
        try out_logps.append(allocator, logProbAtToken(logits, token_id));
        try seq.append(allocator, @intCast(token_id));
        if (eos_token_id) |eos_id| if (token_id == @as(usize, @intCast(eos_id))) break;
    }
    if (out_tokens.items.len == 0) return error.EmptyCompletion;
}

pub fn fillTeacherTopKTargets(
    targets: []f32,
    rows: usize,
    vocab_size: usize,
    example: *const gemma4.PreparedExampleInput,
) !bool {
    const top_k = example.teacher_top_k;
    if (top_k == 0) return false;
    if (example.teacher_top_k_token_ids.len != example.teacher_top_k_probs.len) return error.InvalidTeacherDistillationTargets;
    if (example.teacher_top_k_token_ids.len % top_k != 0) return error.InvalidTeacherDistillationTargets;
    const teacher_rows = example.teacher_top_k_token_ids.len / top_k;
    if (teacher_rows == 0) return false;
    if (teacher_rows < @min(example.labels.len, rows)) return error.InvalidTeacherDistillationTargets;

    var active_rows: usize = 0;
    for (0..@min(example.labels.len, rows)) |row| {
        if (example.labels[row] != -100) active_rows += 1;
    }
    if (active_rows == 0) return false;
    const temperature = example.teacher_temperature;
    if (temperature <= 0 or std.math.isNan(temperature)) return error.InvalidTeacherTemperature;
    const distillation_scale = temperature * temperature;
    const row_scale = (@as(f32, @floatFromInt(rows)) / @as(f32, @floatFromInt(active_rows))) * distillation_scale;

    for (0..@min(example.labels.len, rows)) |row| {
        if (example.labels[row] == -100) continue;
        if (row == 0) return error.InvalidCausalLabelPosition;
        const base = row * top_k;
        var prob_sum: f32 = 0.0;
        for (0..top_k) |ki| {
            const prob = example.teacher_top_k_probs[base + ki];
            if (prob < 0 or std.math.isNan(prob)) return error.InvalidTeacherDistillationTargets;
            prob_sum += prob;
        }
        if (prob_sum <= 0) return error.InvalidTeacherDistillationTargets;
        for (0..top_k) |ki| {
            const token_id = example.teacher_top_k_token_ids[base + ki];
            if (token_id < 0) continue;
            const idx: usize = @intCast(token_id);
            if (idx >= vocab_size) return error.LabelOutOfRange;
            targets[(row - 1) * vocab_size + idx] += row_scale * (example.teacher_top_k_probs[base + ki] / prob_sum);
        }
    }
    return true;
}

fn exampleHasTeacherTargets(example: *const gemma4.PreparedExampleInput) bool {
    return example.teacher_top_k > 0 and
        example.teacher_top_k_token_ids.len > 0 and
        example.teacher_top_k_probs.len > 0;
}

pub fn materializeTeacherTopKTargets(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    prepared: *gemma4.PreparedInputsSummary,
    backend_kind: BackendKind,
    options: TeacherTopKOptions,
) !TeacherTopKSummary {
    if (prepared.examples_with_images > 0 or prepared.examples_with_audio > 0) return error.MultimodalTeacherMaterializationNotYetSupported;
    if (options.top_k == 0) return error.InvalidTeacherTopK;
    if (options.temperature <= 0 or std.math.isNan(options.temperature)) return error.InvalidTeacherTemperature;

    const graph_config = try loadGraphConfig(allocator, base_model_dir);
    const vocab_size: usize = @intCast(graph_config.vocab_size);
    if (options.top_k > vocab_size) return error.InvalidTeacherTopK;
    const seq_len = prepared.max_seq_len;
    if (seq_len == 0) return error.InvalidPreparedInputLength;

    var backend = try loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const ids_shape = Shape.init(.f32, &.{ 1, @as(i64, @intCast(seq_len)) });
    const input_ids_node = try bld.parameter("__teacher_input_ids", ids_shape);
    const attention_mask_node = try bld.parameter("__teacher_attention_mask", ids_shape);
    var ctx = GemmaAutodiffCtx.init(graph_config);
    const hidden = try GemmaAutodiffCtx.buildForward(@ptrCast(&ctx), &bld, input_ids_node, attention_mask_node, 1, @intCast(seq_len));
    const logits_node = try ctx.buildLogits(&bld, hidden);
    try graph.markOutput(logits_node);

    const limit = if (options.max_examples > 0 and options.max_examples < prepared.examples.len) options.max_examples else prepared.examples.len;
    var summary = TeacherTopKSummary{
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
            example,
            seq_len,
            vocab_size,
        );
        defer allocator.free(logits);

        const token_ids = try allocator.alloc(i32, seq_len * options.top_k);
        errdefer allocator.free(token_ids);
        const probs = try allocator.alloc(f32, seq_len * options.top_k);
        errdefer allocator.free(probs);
        try fillTopKFromLogits(token_ids, probs, logits, seq_len, vocab_size, options.top_k, options.temperature);

        replaceTeacherTargets(allocator, example, token_ids, probs, options.top_k, options.temperature);
        summary.examples_written += 1;
        summary.supervised_tokens_seen += example.num_supervised_tokens;
    }
    summary.examples_seen = limit;
    return summary;
}

fn forwardTeacherLogitsForExample(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph: *const Graph,
    input_ids_node: NodeId,
    attention_mask_node: NodeId,
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
    const usable = @min(example.input_ids.len, seq_len);
    for (0..usable) |idx| {
        input_ids[idx] = @floatFromInt(example.input_ids[idx]);
        attention_mask[idx] = 1.0;
    }

    const dims = [_]i32{ 1, @intCast(seq_len) };
    const input_ct = try cb.fromFloat32Shape(input_ids, &dims);
    defer cb.free(input_ct);
    const mask_ct = try cb.fromFloat32Shape(attention_mask, &dims);
    defer cb.free(mask_ct);
    const rt_inputs = [_]interpreter.RuntimeInput{
        .{ .node_id = input_ids_node, .value = input_ct },
        .{ .node_id = attention_mask_node, .value = mask_ct },
    };
    var exec_result = try interpreter.execute(allocator, graph, cb, .{ .runtime_inputs = &rt_inputs });
    defer exec_result.deinit(cb);
    if (exec_result.outputs.len != 1) return error.InvalidTeacherLogits;
    const logits = try cb.toFloat32(exec_result.outputs[0], allocator);
    errdefer allocator.free(logits);
    if (logits.len != seq_len * vocab_size) return error.InvalidTeacherLogits;
    return logits;
}

pub fn fillTopKFromLogits(
    token_ids: []i32,
    probs: []f32,
    logits: []const f32,
    rows: usize,
    vocab_size: usize,
    top_k: usize,
    temperature: f32,
) !void {
    if (token_ids.len != rows * top_k or probs.len != rows * top_k) return error.InvalidTeacherTopK;
    var row: usize = 0;
    while (row < rows) : (row += 1) {
        const out_base = row * top_k;
        for (0..top_k) |slot| {
            token_ids[out_base + slot] = -1;
            probs[out_base + slot] = -std.math.inf(f32);
        }
        const row_logits = logits[row * vocab_size ..][0..vocab_size];
        for (row_logits, 0..) |logit, token_idx| {
            if (std.math.isNan(logit)) continue;
            const score = logit / temperature;
            var insert_at: ?usize = null;
            for (0..top_k) |slot| {
                if (score > probs[out_base + slot]) {
                    insert_at = slot;
                    break;
                }
            }
            if (insert_at) |slot| {
                var move_idx = top_k - 1;
                while (move_idx > slot) : (move_idx -= 1) {
                    probs[out_base + move_idx] = probs[out_base + move_idx - 1];
                    token_ids[out_base + move_idx] = token_ids[out_base + move_idx - 1];
                }
                probs[out_base + slot] = score;
                token_ids[out_base + slot] = @intCast(token_idx);
            }
        }

        const max_score = probs[out_base];
        if (token_ids[out_base] < 0 or max_score == -std.math.inf(f32)) return error.InvalidTeacherLogits;
        var sum_exp: f32 = 0.0;
        for (0..top_k) |slot| {
            const value = @exp(probs[out_base + slot] - max_score);
            probs[out_base + slot] = value;
            sum_exp += value;
        }
        if (sum_exp <= 0 or std.math.isNan(sum_exp)) return error.InvalidTeacherLogits;
        for (0..top_k) |slot| probs[out_base + slot] /= sum_exp;
    }
}

pub fn replaceTeacherTargets(
    allocator: std.mem.Allocator,
    example: *gemma4.PreparedExampleInput,
    token_ids: []i32,
    probs: []f32,
    top_k: usize,
    temperature: f32,
) void {
    if (example.teacher_top_k_token_ids.len > 0) allocator.free(example.teacher_top_k_token_ids);
    if (example.teacher_top_k_probs.len > 0) allocator.free(example.teacher_top_k_probs);
    example.teacher_top_k_token_ids = token_ids;
    example.teacher_top_k_probs = probs;
    example.teacher_top_k = top_k;
    example.teacher_temperature = temperature;
}

pub fn findFirstSupervisedExample(examples: []const gemma4.PreparedExampleInput) ?*const gemma4.PreparedExampleInput {
    for (examples) |*example| {
        if (example.num_supervised_tokens > 0) return example;
    }
    return null;
}

pub fn initializeTrainerFromAdapterDir(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    adapter_model_dir: []const u8,
    bootstrap_example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !void {
    return initializeTrainerFromAdapterDirBatched(
        allocator,
        trainer,
        ctx,
        adapter_model_dir,
        bootstrap_example,
        seq_len,
        1,
    );
}

/// Initialize the trainer directly at its hot preference batch shape. This
/// avoids constructing and compiling a throwaway batch-one graph before a
/// batched DPO/GRPO update.
pub fn initializeTrainerFromAdapterDirBatched(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    adapter_model_dir: []const u8,
    bootstrap_example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
    batch_size: usize,
) !void {
    if (batch_size == 0) return error.EmptyTrainingBatch;
    const examples = try allocator.alloc(gemma4.PreparedExampleInput, batch_size);
    defer allocator.free(examples);
    for (examples) |*example| example.* = bootstrap_example.*;
    var bootstrap = try makeTrainerInputForExamplesWeighted(allocator, ctx, examples, seq_len, null, null);
    defer bootstrap.deinit(allocator);
    try trainer.ensureGraphBuilt(bootstrap.trainer_input);

    try loadTrainerAdapterWeights(allocator, trainer, adapter_model_dir);
}

fn loadTrainerAdapterWeights(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    adapter_model_dir: []const u8,
) !void {
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

pub fn trainPreparedExamples(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    examples: []const gemma4.PreparedExampleInput,
    max_examples: usize,
    seq_len: u32,
) !CausalLmMetrics {
    return runPreparedExamples(allocator, trainer, ctx, examples, max_examples, seq_len, .train);
}

pub fn evaluatePreparedExamples(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    examples: []const gemma4.PreparedExampleInput,
    max_examples: usize,
    seq_len: u32,
) !CausalLmMetrics {
    return runPreparedExamples(allocator, trainer, ctx, examples, max_examples, seq_len, .eval);
}

fn runPreparedExamples(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    examples: []const gemma4.PreparedExampleInput,
    max_examples: usize,
    seq_len: u32,
    mode: enum { train, eval },
) !CausalLmMetrics {
    var metrics = CausalLmMetrics{};
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

pub fn saveTrainerAsGemmaBundle(
    allocator: std.mem.Allocator,
    trainer: *const real_autodiff.RealAutodiffTrainer,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    out_dir: []const u8,
) !void {
    var adapter_inspect = try gemma4.inspectCheckpoint(allocator, adapter_model_dir);
    defer gemma4.freeInspectionSummary(allocator, &adapter_inspect);

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, gemma4.adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, gemma4.adapter_config_file_name });
    defer allocator.free(adapter_config_path);

    var tensors = std.ArrayList(WriteTensorF32).empty;
    defer tensors.deinit(allocator);
    var owned_names = std.ArrayList([]const u8).empty;
    defer {
        for (owned_names.items) |name| allocator.free(name);
        owned_names.deinit(allocator);
    }
    var owned_shapes = std.ArrayList([]const usize).empty;
    defer {
        for (owned_shapes.items) |shape| allocator.free(shape);
        owned_shapes.deinit(allocator);
    }

    for (trainer.lora_params.items) |slot| {
        const mapped_name = try mapTrainerSlotNameToGemmaAdapterTensor(allocator, slot.name);
        errdefer allocator.free(mapped_name);
        const dims = try dimsToUsize(allocator, slot.dims);
        errdefer allocator.free(dims);
        try owned_names.append(allocator, mapped_name);
        try owned_shapes.append(allocator, dims);
        try tensors.append(allocator, .{
            .name = mapped_name,
            .shape = dims,
            .data = slot.weights,
        });
    }

    try writeHeaderAndTensorsF32(allocator, adapter_checkpoint_path, tensors.items);
    const base_name = adapter_inspect.base_model_name_or_path orelse base_model_dir;
    const rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse gemma4.default_lora_target_modules[0..];
    try writeAdapterConfigJson(allocator, adapter_config_path, base_name, rank, alpha, target_modules, .{
        .enabled = adapter_inspect.recursive_lora_enabled,
        .source_num_layers = adapter_inspect.recursive_source_num_layers orelse 0,
        .shared_block_size = adapter_inspect.recursive_shared_block_size orelse 0,
        .loop_count = adapter_inspect.recursive_loop_count orelse 0,
        .init_strategy = adapter_inspect.recursive_init_strategy orelse "average_residual_svd",
    });

    try copySupportingArtifactIfPresent(allocator, adapter_inspect.tokenizer_config_path, out_dir, gemma4.tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_inspect.tokenizer_path, out_dir, gemma4.tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_inspect.special_tokens_map_path, out_dir, gemma4.special_tokens_map_file_name);
}

/// Bind LoRA state for policy scoring without downloading device-resident
/// optimizer weights. CUDA updates live in `slot.device.weight`; re-uploading
/// the stale host snapshot here would make every later rollout appear to use
/// the initial policy. Device tensors are therefore borrowed by the runtime
/// map, while host-only tensors and deterministic eval dropout masks are owned
/// by the map and released after execution.
fn bindScoringLoraInputs(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    rt: *std.AutoHashMapUnmanaged(NodeId, CT),
    borrowed_rt: *std.AutoHashMapUnmanaged(NodeId, void),
) !void {
    const gs = &trainer.graph_state.?;
    if (trainer.config.lora.dropout > 0.0) {
        for (gs.lora_adapter.adapters.items) |info| {
            if (info.dropout_mask_id == ml.graph.null_node) continue;
            const mask_shape = gs.graph.node(info.dropout_mask_id).output_shape;
            var dims_buffer: [ml.graph.shape.max_rank]i32 = undefined;
            const rank: usize = mask_shape.rank();
            var mask_len: usize = 1;
            for (0..rank) |axis| {
                const dim = mask_shape.dim(@intCast(axis));
                if (dim <= 0 or dim > std.math.maxInt(i32)) return error.InvalidTensorShape;
                dims_buffer[axis] = @intCast(dim);
                mask_len = std.math.mul(usize, mask_len, @as(usize, @intCast(dim))) catch return error.InvalidTensorShape;
            }
            const mask = try allocator.alloc(f32, mask_len);
            defer allocator.free(mask);
            @memset(mask, 1.0);
            const ct = try trainer.compute_backend.fromFloat32Shape(mask, dims_buffer[0..rank]);
            rt.put(allocator, info.dropout_mask_id, ct) catch |err| {
                trainer.compute_backend.free(ct);
                return err;
            };
        }
    }

    for (trainer.lora_params.items) |slot| {
        if (slot.device) |device| {
            try rt.put(allocator, slot.node_id, device.weight);
            borrowed_rt.put(allocator, slot.node_id, {}) catch |err| {
                _ = rt.remove(slot.node_id);
                return err;
            };
        } else {
            const ct = try trainer.compute_backend.fromFloat32Shape(slot.weights, slot.dims);
            rt.put(allocator, slot.node_id, ct) catch |err| {
                trainer.compute_backend.free(ct);
                return err;
            };
        }
    }
}

pub fn sequenceLogprobForExample(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    example: *const gemma4.PreparedExampleInput,
    seq_len: u32,
) !f32 {
    const rows: usize = @intCast(seq_len);
    const usable = @min(example.input_ids.len, rows);
    if (usable == 0) return error.EmptyPrompt;
    const input_ids = try allocator.alloc(i32, usable);
    defer allocator.free(input_ids);
    for (example.input_ids[0..usable], input_ids) |token_id, *dst| {
        dst.* = std.math.cast(i32, token_id) orelse return error.LabelOutOfRange;
    }

    var row_indices = std.ArrayList(usize).empty;
    defer row_indices.deinit(allocator);
    var token_ids = std.ArrayList(i32).empty;
    defer token_ids.deinit(allocator);
    const label_rows = @min(example.labels.len, rows);
    for (0..label_rows) |row_idx| {
        const label = example.labels[row_idx];
        if (label < 0) continue;
        if (row_idx == 0) return error.InvalidCausalLabelPosition;
        if (@as(usize, @intCast(label)) >= @as(usize, @intCast(ctx.graph_config.vocab_size)))
            return error.LabelOutOfRange;
        try row_indices.append(allocator, row_idx - 1);
        try token_ids.append(allocator, label);
    }
    if (token_ids.items.len == 0) return 0.0;
    const logps = try executePreferenceReadbackForInputIds(
        allocator,
        trainer,
        ctx,
        input_ids,
        seq_len,
        .{ .token_logprobs = .{ .row_indices = row_indices.items, .token_ids = token_ids.items } },
    );
    defer allocator.free(logps);
    var sum_logp: f32 = 0.0;
    for (logps) |logp| sum_logp += logp;
    return sum_logp;
}

const PreferenceReadback = union(enum) {
    logits_row: usize,
    token_logprobs: struct {
        row_indices: []const usize,
        token_ids: []const i32,
    },
};

fn projectHiddenRowsToLogits(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_config: gemma_graph.Config,
    hidden: CT,
    hidden_shape: Shape,
    row_start: usize,
    row_count: usize,
) !struct { tensor: CT, shape: Shape } {
    _ = allocator;
    const rank: usize = hidden_shape.rank();
    if (rank < 2 or row_count == 0) return error.InvalidPreferenceReadback;
    const row_axis = rank - 2;
    const hidden_axis = rank - 1;
    const row_extent_i64 = hidden_shape.dims[row_axis];
    const hidden_size_i64 = hidden_shape.dims[hidden_axis];
    if (row_extent_i64 <= 0 or hidden_size_i64 <= 0) return error.InvalidPreferenceReadback;
    const row_extent: usize = @intCast(row_extent_i64);
    if (row_start >= row_extent or row_count > row_extent - row_start) return error.InvalidPreferenceReadback;
    var leading_count: usize = 1;
    for (hidden_shape.dims[0..row_axis]) |dim| {
        if (dim <= 0) return error.InvalidPreferenceReadback;
        leading_count = std.math.mul(usize, leading_count, @intCast(dim)) catch return error.InvalidPreferenceReadback;
    }
    if (leading_count != 1) return error.InvalidPreferenceReadback;

    var starts = [_]i64{0} ** ml.graph.shape.max_rank;
    var limits = [_]i64{0} ** ml.graph.shape.max_rank;
    var strides = [_]i64{1} ** ml.graph.shape.max_rank;
    for (hidden_shape.dims[0..rank], 0..) |dim, idx| limits[idx] = dim;
    starts[row_axis] = @intCast(row_start);
    limits[row_axis] = @intCast(row_start + row_count);
    const selected_hidden = try cb.primSlice(
        hidden,
        starts[0..rank],
        limits[0..rank],
        strides[0..rank],
        hidden_shape.dims[0..rank],
    );
    defer cb.free(selected_hidden);

    const hidden_size: usize = @intCast(hidden_size_i64);
    const selected_2d_shape = [_]i64{ @intCast(row_count), hidden_size_i64 };
    const selected_2d = try cb.primReshape(selected_hidden, &selected_2d_shape);
    defer cb.free(selected_2d);
    var name_buf: [256]u8 = undefined;
    const head_name = if (graph_config.weight_tying)
        try prefixedModelName(&name_buf, graph_config, "model.embed_tokens.weight")
    else
        "lm_head.weight";
    const head = try cb.getWeight(head_name);
    defer cb.free(head);
    const logits = try cb.linearNoBias(
        selected_2d,
        head,
        row_count,
        hidden_size,
        @intCast(graph_config.vocab_size),
    );
    const logits_shape = Shape.init(.f32, &.{ @as(i64, @intCast(row_count)), @as(i64, @intCast(graph_config.vocab_size)) });
    return .{ .tensor = logits, .shape = logits_shape };
}

fn readSelectedTokenLogprobs(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_config: gemma_graph.Config,
    hidden: CT,
    hidden_shape: Shape,
    row_indices: []const usize,
    token_ids: []const i32,
) ![]f32 {
    if (row_indices.len == 0 or row_indices.len != token_ids.len) return error.InvalidPreferenceReadback;
    var row_min = row_indices[0];
    var row_max = row_indices[0];
    for (row_indices) |row| {
        row_min = @min(row_min, row);
        row_max = @max(row_max, row);
    }
    const row_span = row_max - row_min + 1;
    const projected = try projectHiddenRowsToLogits(
        allocator,
        cb,
        graph_config,
        hidden,
        hidden_shape,
        row_min,
        row_span,
    );
    defer cb.free(projected.tensor);
    const vocab_size: usize = @intCast(graph_config.vocab_size);

    const relative_rows = try allocator.alloc(u32, row_indices.len);
    defer allocator.free(relative_rows);
    const selected_tokens = try allocator.alloc(u32, token_ids.len);
    defer allocator.free(selected_tokens);
    for (row_indices, token_ids, relative_rows, selected_tokens) |row, token_id, *relative_row, *selected_token| {
        if (token_id < 0 or @as(usize, @intCast(token_id)) >= vocab_size) return error.LabelOutOfRange;
        relative_row.* = std.math.cast(u32, row - row_min) orelse return error.InvalidPreferenceReadback;
        selected_token.* = @intCast(token_id);
    }
    if (try cb.selectedTokenLogprobs(
        projected.tensor,
        relative_rows,
        selected_tokens,
        row_span,
        vocab_size,
    )) |selected| {
        defer cb.free(selected);
        return cb.toFloat32(selected, allocator);
    }

    const log_probs = try cb.primLogSoftmax(projected.tensor, @intCast(vocab_size));
    defer cb.free(log_probs);

    const token_indices = try allocator.alloc(f32, token_ids.len);
    defer allocator.free(token_indices);
    for (token_ids, token_indices) |token_id, *dst| {
        if (token_id < 0 or @as(usize, @intCast(token_id)) >= vocab_size) return error.LabelOutOfRange;
        dst.* = @floatFromInt(token_id);
    }
    const token_index_shape = [_]i32{@intCast(token_indices.len)};
    const token_indices_ct = try cb.fromFloat32Shape(token_indices, &token_index_shape);
    defer cb.free(token_indices_ct);
    const candidate_matrix = try cb.primGather(
        log_probs,
        token_indices_ct,
        @intCast(projected.shape.rank() - 1),
        projected.shape.dims[0..projected.shape.rank()],
    );
    defer cb.free(candidate_matrix);

    const matrix_count = std.math.mul(usize, row_span, token_ids.len) catch return error.InvalidPreferenceReadback;
    if (matrix_count == 0 or matrix_count - 1 > (1 << 24)) return error.PreferenceReadbackIndexOutOfRange;
    const flat_shape = [_]i64{@intCast(matrix_count)};
    const candidate_flat = try cb.primReshape(candidate_matrix, &flat_shape);
    defer cb.free(candidate_flat);
    const diagonal_indices = try allocator.alloc(f32, token_ids.len);
    defer allocator.free(diagonal_indices);
    for (row_indices, 0..) |row, idx| {
        diagonal_indices[idx] = @floatFromInt((row - row_min) * token_ids.len + idx);
    }
    const diagonal_shape = [_]i32{@intCast(diagonal_indices.len)};
    const diagonal_indices_ct = try cb.fromFloat32Shape(diagonal_indices, &diagonal_shape);
    defer cb.free(diagonal_indices_ct);
    const selected = try cb.primGather(candidate_flat, diagonal_indices_ct, 0, &flat_shape);
    defer cb.free(selected);
    return cb.toFloat32(selected, allocator);
}

fn executePreferenceReadbackForInputIds(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *GemmaAutodiffCtx,
    raw_input_ids: []const i32,
    seq_len: u32,
    readback: PreferenceReadback,
) ![]f32 {
    const rows: usize = @intCast(seq_len);
    if (raw_input_ids.len == 0) return error.EmptyPrompt;
    if (raw_input_ids.len > rows) return error.SequenceTooLong;

    const input_ids = try allocator.alloc(i64, rows);
    defer allocator.free(input_ids);
    @memset(input_ids, 0);
    const attention_mask = try allocator.alloc(f32, rows);
    defer allocator.free(attention_mask);
    @memset(attention_mask, 0.0);
    for (raw_input_ids, 0..) |token_id, idx| {
        input_ids[idx] = token_id;
        attention_mask[idx] = 1.0;
    }

    const trainer_input = real_autodiff.TrainerInput{
        .ctx = @ptrCast(ctx),
        .build_forward = &GemmaAutodiffCtx.buildForward,
        .build_loss = &GemmaAutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        // Scoring executes only the post-LoRA hidden-state output. Targets
        // are a graph-build shape contract, not a 128 MiB host allocation.
        .targets = &.{},
        .targets_shape = ctx.targetsShape(rows),
        .batch = 1,
        .seq_len = seq_len,
        .bind_arch_inputs = null,
        .remap_graph_nodes = &GemmaAutodiffCtx.remapGraphNodes,
    };

    try trainer.ensureGraphBuilt(trainer_input);
    var gs = &trainer.graph_state.?;
    const hidden_node = gs.forward_output_node;
    const hidden_shape = gs.graph.node(hidden_node).output_shape;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    var borrowed_rt = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| {
            if (!borrowed_rt.contains(entry.key_ptr.*)) trainer.compute_backend.free(entry.value_ptr.*);
        }
        borrowed_rt.deinit(allocator);
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
    const input_ct = try graph_input_binder.bindI64(trainer.compute_backend, allocator, input_placeholder, input_ids);
    try rt.put(allocator, gs.input_ids_node, input_ct);
    const mask_ct = try graph_input_binder.bindF32(trainer.compute_backend, allocator, mask_placeholder, attention_mask);
    try rt.put(allocator, gs.attention_mask_node, mask_ct);
    try bindScoringLoraInputs(allocator, trainer, &rt, &borrowed_rt);

    // Execute a read-only graph view with a private output list. Rewriting the
    // trainer graph's shared outputs made concurrent/nested scoring racy and
    // required best-effort restoration on error. Node/constant/parameter
    // storage is immutable during execution, so only the output list needs an
    // independent allocation.
    var scoring_graph = gs.graph;
    scoring_graph.outputs = .empty;
    defer scoring_graph.outputs.deinit(allocator);
    try scoring_graph.markOutput(hidden_node);

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

    var exec_result = try interpreter.execute(allocator, &scoring_graph, trainer.compute_backend, .{
        .runtime_inputs = rt_inputs.items,
    });
    defer exec_result.deinit(trainer.compute_backend);
    if (exec_result.outputs.len != 1) return error.InvalidPreferenceReadback;
    return switch (readback) {
        .logits_row => |row| blk: {
            const projected = try projectHiddenRowsToLogits(
                allocator,
                trainer.compute_backend,
                ctx.graph_config,
                exec_result.outputs[0],
                hidden_shape,
                row,
                1,
            );
            defer trainer.compute_backend.free(projected.tensor);
            break :blk trainer.compute_backend.toFloat32(projected.tensor, allocator);
        },
        .token_logprobs => |selection| readSelectedTokenLogprobs(
            allocator,
            trainer.compute_backend,
            ctx.graph_config,
            exec_result.outputs[0],
            hidden_shape,
            selection.row_indices,
            selection.token_ids,
        ),
    };
}

fn concatPromptCompletion(allocator: std.mem.Allocator, prompt: []const i32, completion: []const i32) ![]i32 {
    const out = try allocator.alloc(i32, prompt.len + completion.len);
    @memcpy(out[0..prompt.len], prompt);
    @memcpy(out[prompt.len..], completion);
    return out;
}

fn selectRankedToken(allocator: std.mem.Allocator, logits: []const f32, rank: usize) !usize {
    const Entry = struct {
        idx: usize,
        value: f32,
    };
    var entries = try allocator.alloc(Entry, logits.len);
    defer allocator.free(entries);
    for (logits, 0..) |value, idx| {
        entries[idx] = .{ .idx = idx, .value = value };
    }
    std.sort.heap(Entry, entries, {}, struct {
        fn lessThan(_: void, lhs: Entry, rhs: Entry) bool {
            return lhs.value > rhs.value;
        }
    }.lessThan);
    return entries[@min(rank, entries.len - 1)].idx;
}

fn sampleToken(logits: []const f32, random: std.Random) !usize {
    if (logits.len == 0) return error.InvalidLogits;
    var max_logit = logits[0];
    for (logits) |value| {
        if (!std.math.isFinite(value)) return error.InvalidLogits;
        max_logit = @max(max_logit, value);
    }
    var total: f64 = 0.0;
    for (logits) |value| total += @exp(@as(f64, value - max_logit));
    if (!std.math.isFinite(total) or total <= 0.0) return error.InvalidLogits;
    const threshold = random.float(f64) * total;
    var cumulative: f64 = 0.0;
    for (logits, 0..) |value, idx| {
        cumulative += @exp(@as(f64, value - max_logit));
        if (cumulative >= threshold) return idx;
    }
    return logits.len - 1;
}

fn logProbAtToken(logits: []const f32, token_id: usize) f32 {
    var max_logit = logits[0];
    for (logits[1..]) |value| {
        if (value > max_logit) max_logit = value;
    }
    var sum_exp: f64 = 0.0;
    for (logits) |value| {
        sum_exp += @exp(@as(f64, value - max_logit));
    }
    const log_z = @as(f64, max_logit) + @log(sum_exp);
    return @as(f32, @floatCast(@as(f64, logits[token_id]) - log_z));
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn mapTrainerSlotNameToGemmaAdapterTensor(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    if (try mapUseSiteTrainerSlotNameToLoopTensor(allocator, name)) |mapped| return mapped;
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        return std.fmt.allocPrint(allocator, "{s}.weight", .{name});
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
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

fn dimsToUsize(allocator: std.mem.Allocator, dims: []const i32) ![]usize {
    const out = try allocator.alloc(usize, dims.len);
    for (dims, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
    _ = allocator;
    var header_buf: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;
    try writer.writeByte('{');
    var offset: u64 = 0;
    for (tensors, 0..) |tensor, idx| {
        if (idx != 0) try writer.writeByte(',');
        const byte_len = tensor.data.len * @sizeOf(f32);
        try writer.print("\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{tensor.name});
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        offset += byte_len;
    }
    try writer.writeByte('}');

    var file = try compat.cwd().createFile(compat.io(), path, .{ .truncate = true });
    defer file.close(compat.io());
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(compat.io(), &len_buf);
    try file.writeStreamingAll(compat.io(), header_buf.written());
    for (tensors) |tensor| {
        for (tensor.data) |item| {
            const bits: u32 = @bitCast(item);
            var bits_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &bits_buf, bits, .little);
            try file.writeStreamingAll(compat.io(), &bits_buf);
        }
    }
}

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    target_modules: []const []const u8,
    recursive_config: @import("recursive_lora.zig").Config,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    if (recursive_config.enabled) {
        try std.json.Stringify.value(.{
            .base_model_name_or_path = base_model_name_or_path,
            .peft_type = "LORA",
            .task_type = "CAUSAL_LM",
            .r = rank,
            .lora_alpha = alpha,
            .target_modules = target_modules,
            .recursive_lora = .{
                .enabled = true,
                .source_num_layers = recursive_config.source_num_layers,
                .shared_block_size = recursive_config.shared_block_size,
                .loop_count = recursive_config.loop_count,
                .init_strategy = recursive_config.init_strategy,
            },
        }, .{ .whitespace = .indent_2 }, &buffer.writer);
    } else {
        try std.json.Stringify.value(.{
            .base_model_name_or_path = base_model_name_or_path,
            .peft_type = "LORA",
            .task_type = "CAUSAL_LM",
            .r = rank,
            .lora_alpha = alpha,
            .target_modules = target_modules,
        }, .{ .whitespace = .indent_2 }, &buffer.writer);
    }
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn copySupportingArtifactIfPresent(
    allocator: std.mem.Allocator,
    maybe_src_path: ?[]const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src_path = maybe_src_path orelse return;
    const contents = try c_file.readFile(allocator, src_path);
    defer allocator.free(contents);
    const dst_path = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst_path, .data = contents });
}

test "makeTrainerInputForExample builds masked one-hot targets" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.init(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 32,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var prompt_input_ids = [_]i32{ 1, 2 };
    var response_input_ids = [_]i32{ 3, 4 };
    var input_ids = [_]i32{ 1, 2, 3, 4 };
    var labels = [_]i32{ -100, -100, 3, 4 };
    const ex = gemma4.PreparedExampleInput{
        .mode = .instruction,
        .prompt_input_ids = prompt_input_ids[0..],
        .response_input_ids = response_input_ids[0..],
        .num_prompt_tokens = 2,
        .num_response_tokens = 2,
        .input_ids = input_ids[0..],
        .labels = labels[0..],
        .num_input_tokens = 4,
        .num_supervised_tokens = 2,
    };

    var owned = try makeTrainerInputForExample(allocator, &ctx, &ex, 4);
    defer owned.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), owned.input_ids[0]);
    try std.testing.expectEqual(@as(f32, 0.0), owned.targets[0]);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), owned.targets[1 * 32 + 3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), owned.targets[2 * 32 + 4], 1e-6);
}

test "makeTrainerInputForTokenLogprobGrads builds per-token weighted targets" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.init(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 32,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var prompt_input_ids = [_]i32{ 1, 2 };
    var response_input_ids = [_]i32{ 3, 4 };
    var input_ids = [_]i32{ 1, 2, 3, 4 };
    var labels = [_]i32{ -100, -100, 3, 4 };
    const ex = gemma4.PreparedExampleInput{
        .mode = .instruction,
        .prompt_input_ids = prompt_input_ids[0..],
        .response_input_ids = response_input_ids[0..],
        .num_prompt_tokens = 2,
        .num_response_tokens = 2,
        .input_ids = input_ids[0..],
        .labels = labels[0..],
        .num_input_tokens = 4,
        .num_supervised_tokens = 2,
    };

    var owned = try makeTrainerInputForTokenLogprobGrads(allocator, &ctx, &ex, 4, &.{ 0.25, -0.5 });
    defer owned.deinit(allocator);

    try std.testing.expectApproxEqAbs(@as(f32, -1.0), owned.targets[1 * 32 + 3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), owned.targets[2 * 32 + 4], 1e-6);
}

test "preference trainer input keeps token targets sparse and causally shifted" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.initPreference(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 262144,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var prompt_input_ids = [_]i32{ 1, 2 };
    var response_input_ids = [_]i32{ 262143, 4 };
    var input_ids = [_]i32{ 1, 2, 262143, 4 };
    var labels = [_]i32{ -100, -100, 262143, 4 };
    const ex = gemma4.PreparedExampleInput{
        .mode = .instruction,
        .prompt_input_ids = prompt_input_ids[0..],
        .response_input_ids = response_input_ids[0..],
        .num_prompt_tokens = 2,
        .num_response_tokens = 2,
        .input_ids = input_ids[0..],
        .labels = labels[0..],
        .num_input_tokens = 4,
        .num_supervised_tokens = 2,
    };

    var owned = try makeTrainerInputForTokenLogprobGrads(allocator, &ctx, &ex, 4, &.{ 0.25, -0.5 });
    defer owned.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 8), owned.targets.len);
    try std.testing.expectEqual(@as(i64, 4), owned.trainer_input.targets_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), owned.trainer_input.targets_shape.dim(1));
    try std.testing.expectEqualSlices(f32, &.{
        0,      0,
        262143, -1,
        4,      2,
        0,      0,
    }, owned.targets);
}

test "batched preference trainer input preserves per-token logprob derivatives" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.initPreference(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 32,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var first_input_ids = [_]i32{ 1, 2, 3, 4 };
    var first_labels = [_]i32{ -100, -100, 3, 4 };
    var second_input_ids = [_]i32{ 5, 6, 7 };
    var second_labels = [_]i32{ -100, -100, 7 };
    const first_prompt_ids = first_input_ids[0..2];
    const first_response_ids = first_input_ids[2..];
    const second_prompt_ids = second_input_ids[0..2];
    const second_response_ids = second_input_ids[2..];
    const examples = [_]gemma4.PreparedExampleInput{
        .{
            .mode = .instruction,
            .prompt_input_ids = first_prompt_ids,
            .response_input_ids = first_response_ids,
            .num_prompt_tokens = first_prompt_ids.len,
            .num_response_tokens = first_response_ids.len,
            .input_ids = &first_input_ids,
            .labels = &first_labels,
            .num_input_tokens = first_input_ids.len,
            .num_supervised_tokens = 2,
        },
        .{
            .mode = .instruction,
            .prompt_input_ids = second_prompt_ids,
            .response_input_ids = second_response_ids,
            .num_prompt_tokens = second_prompt_ids.len,
            .num_response_tokens = second_response_ids.len,
            .input_ids = &second_input_ids,
            .labels = &second_labels,
            .num_input_tokens = second_input_ids.len,
            .num_supervised_tokens = 1,
        },
    };

    var owned = try makeTrainerInputForTokenLogprobGradsBatch(
        allocator,
        &ctx,
        &examples,
        4,
        &.{ 0.25, -0.5, 1.0 },
    );
    defer owned.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 2), owned.trainer_input.batch);
    try std.testing.expectEqual(@as(i64, 8), owned.trainer_input.targets_shape.dim(0));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5, 6, 7, 0 }, owned.input_ids);
    try std.testing.expectEqualSlices(f32, &.{
        0, 0,
        3, -2,
        4, 4,
        0, 0,
        0, 0,
        7, -8,
        0, 0,
        0, 0,
    }, owned.targets);
}

test "sparse preference loss has a selected-token VJP" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.initPreference(.{
        .family = .gemma,
        .hidden_size = 4,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .num_key_value_heads = 1,
        .attention_head_dim = 4,
        .intermediate_size = 8,
        .vocab_size = 8,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const logits = try bld.parameter("logits", Shape.init(.f32, &.{ 4, 8 }));
    const targets = try bld.parameter("targets", Shape.init(.f32, &.{ 4, 2 }));
    const loss = try ctx.buildSparseTokenLoss(&bld, logits, targets);
    try std.testing.expect(graph.node(loss).output_shape.numElements().? == 1);
    try graph.markOutput(loss);

    var gradient = try ml.graph.autodiff.gradient(allocator, &graph, loss, &.{logits});
    defer gradient.deinit();
    try std.testing.expectEqual(@as(usize, 1), gradient.param_grads.len);
    try std.testing.expectEqualSlices(i64, &.{ 4, 8 }, gradient.graph.node(gradient.param_grads[0]).output_shape.dims[0..2]);
}

test "sparse preference loss log-softmax gradient matches finite differences" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.initPreference(.{
        .family = .gemma,
        .hidden_size = 4,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .num_key_value_heads = 1,
        .attention_head_dim = 4,
        .intermediate_size = 8,
        .vocab_size = 5,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    const logits = try bld.parameter("logits", Shape.init(.f32, &.{ 3, 5 }));
    const targets = try bld.parameter("targets", Shape.init(.f32, &.{ 3, 2 }));
    const loss = try ctx.buildSparseTokenLoss(&bld, logits, targets);
    try graph.markOutput(loss);

    const logits_values = [_]f32{
        0.2,  -0.4, 0.7,  1.1,  -0.1,
        -0.3, 0.6,  0.1,  -0.8, 0.9,
        1.2,  0.0,  -0.5, 0.4,  0.3,
    };
    // One exact token ID and one signed log-probability coefficient per row.
    const target_values = [_]f32{ 3, 0.7, 1, -0.4, 4, 1.1 };
    const max_relative_error = try ml.graph.grad_check.checkGradients(
        allocator,
        &graph,
        loss,
        &.{logits},
        &.{ &logits_values, &target_values },
        1e-3,
    );
    try std.testing.expect(max_relative_error < 5e-3);
}

test "makeTrainerInputForExample builds sparse teacher soft targets" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.init(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 32,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var prompt_input_ids = [_]i32{ 1, 2 };
    var response_input_ids = [_]i32{ 3, 4 };
    var input_ids = [_]i32{ 1, 2, 3, 4 };
    var labels = [_]i32{ -100, -100, 3, 4 };
    var teacher_ids = [_]i32{
        0, 0,
        0, 0,
        7, 8,
        9, 10,
    };
    var teacher_probs = [_]f32{
        0.0,  0.0,
        0.0,  0.0,
        0.75, 0.25,
        0.2,  0.8,
    };
    const ex = gemma4.PreparedExampleInput{
        .mode = .instruction,
        .prompt_input_ids = prompt_input_ids[0..],
        .response_input_ids = response_input_ids[0..],
        .num_prompt_tokens = 2,
        .num_response_tokens = 2,
        .input_ids = input_ids[0..],
        .labels = labels[0..],
        .num_input_tokens = 4,
        .num_supervised_tokens = 2,
        .teacher_top_k_token_ids = teacher_ids[0..],
        .teacher_top_k_probs = teacher_probs[0..],
        .teacher_top_k = 2,
        .teacher_temperature = 1.0,
    };

    var owned = try makeTrainerInputForExample(allocator, &ctx, &ex, 4);
    defer owned.deinit(allocator);

    try std.testing.expectEqual(@as(f32, 0.0), owned.targets[0 * 32 + 0]);
    try std.testing.expectEqual(@as(f32, 0.0), owned.targets[1 * 32 + 0]);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), owned.targets[1 * 32 + 7], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), owned.targets[1 * 32 + 8], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.4), owned.targets[2 * 32 + 9], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.6), owned.targets[2 * 32 + 10], 1e-6);
    try std.testing.expectEqual(@as(f32, 0.0), owned.targets[2 * 32 + 3]);
    try std.testing.expectEqual(@as(f32, 0.0), owned.targets[3 * 32 + 4]);
}

test "makeTrainerInputForTokenLogprobGrads keeps prompt rows zero and aligns completion weights" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.init(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 32,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var prompt_input_ids = [_]i32{ 1, 2, 3 };
    var response_input_ids = [_]i32{ 5, 7, 9 };
    var input_ids = [_]i32{ 1, 2, 3, 5, 7, 9 };
    var labels = [_]i32{ -100, -100, -100, 5, 7, 9 };
    const ex = gemma4.PreparedExampleInput{
        .mode = .instruction,
        .prompt_input_ids = prompt_input_ids[0..],
        .response_input_ids = response_input_ids[0..],
        .num_prompt_tokens = 3,
        .num_response_tokens = 3,
        .input_ids = input_ids[0..],
        .labels = labels[0..],
        .num_input_tokens = 6,
        .num_supervised_tokens = 3,
    };

    var owned = try makeTrainerInputForTokenLogprobGrads(allocator, &ctx, &ex, 6, &.{ 0.25, -0.5, 1.5 });
    defer owned.deinit(allocator);

    for (0..2) |row_idx| {
        const row = owned.targets[row_idx * 32 ..][0..32];
        for (row) |value| try std.testing.expectEqual(@as(f32, 0.0), value);
    }
    try std.testing.expectApproxEqAbs(@as(f32, -1.5), owned.targets[2 * 32 + 5], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), owned.targets[3 * 32 + 7], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -9.0), owned.targets[4 * 32 + 9], 1e-6);
}

test "makeTrainerInputForExample scales teacher soft targets by temperature squared" {
    const allocator = std.testing.allocator;
    var ctx = GemmaAutodiffCtx.init(.{
        .family = .gemma,
        .hidden_size = 16,
        .num_hidden_layers = 2,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 4,
        .intermediate_size = 32,
        .vocab_size = 32,
        .position_encoding = .rope,
        .norm_type = .rms_norm,
        .activation = .gelu_new,
        .norm_eps = 1e-6,
        .norm_weight_offset = 1.0,
    });
    var prompt_input_ids = [_]i32{1};
    var response_input_ids = [_]i32{2};
    var input_ids = [_]i32{ 1, 2 };
    var labels = [_]i32{ -100, 2 };
    var teacher_ids = [_]i32{
        0, 0,
        5, 6,
    };
    var teacher_probs = [_]f32{
        0.0,  0.0,
        0.25, 0.75,
    };
    const ex = gemma4.PreparedExampleInput{
        .mode = .instruction,
        .prompt_input_ids = prompt_input_ids[0..],
        .response_input_ids = response_input_ids[0..],
        .num_prompt_tokens = 1,
        .num_response_tokens = 1,
        .input_ids = input_ids[0..],
        .labels = labels[0..],
        .num_input_tokens = 2,
        .num_supervised_tokens = 1,
        .teacher_top_k_token_ids = teacher_ids[0..],
        .teacher_top_k_probs = teacher_probs[0..],
        .teacher_top_k = 2,
        .teacher_temperature = 2.0,
    };

    var owned = try makeTrainerInputForExample(allocator, &ctx, &ex, 2);
    defer owned.deinit(allocator);

    try std.testing.expectApproxEqAbs(@as(f32, 2.0), owned.targets[0 * 32 + 5], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), owned.targets[0 * 32 + 6], 1e-6);
}
