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

// Unified entry point for reranker training. Dispatches between the
// cached-surrogate path (`reranker_lora.zig`) and the real-forward path
// (`reranker_real_forward.zig`) behind a single `RerankerTrainingMode` enum
// and a tagged-union input. Both underlying modules remain intact; this file
// is purely a discoverability layer so callers can swap modes without
// rewiring call sites.
//
// The two modes have fundamentally different loop semantics — cached is
// epoch-level, real-forward is step-level — so the return type is a tagged
// union rather than a common struct. Callers switch on the mode they passed.

const std = @import("std");
const reranker_lora = @import("reranker_lora.zig");
const reranker_real_forward = @import("reranker_real_forward.zig");
const reranker_data = @import("reranker_data.zig");
const reranker_head = @import("reranker_head.zig");
const bert_types = @import("../models/bert.zig");
const bert_graph = @import("../architectures/bert_graph.zig");
const ops = @import("../ops/ops.zig");
const coord_mod = @import("training_memory_coordinator.zig");
const lora_adapter_set = @import("lora_adapter_set.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const graph_input_binder = @import("graph_input_binder.zig");
const ml = @import("ml");

pub const LoRAAdapterSet = lora_adapter_set.LoRAAdapterSet;
pub const TrainingMemoryCoordinator = coord_mod.TrainingMemoryCoordinator;

pub const RerankerTrainingMode = enum {
    /// Surrogate-gradient training from pre-computed pooled features.
    /// Fast and memory-light: no real forward pass happens inside the loop,
    /// so NEFTune, real LoRA-gradient flow through the encoder, and
    /// Hypura-style memory coordination are all inapplicable. The original
    /// path shipped in `reranker_lora.zig` and is still the default for
    /// existing cached datasets. Runs a full multi-epoch training session
    /// per call and returns an epoch-level summary.
    cached_surrogate,
    /// Real BERT/DeBERTa forward pass with LoRA applied, surrogate backward
    /// on the last encoder layer. Exercises NEFTune at the genuine embedding
    /// output, accepts an optional Hypura `TrainingMemoryCoordinator`, and
    /// is the only mode that can meaningfully report activation memory or
    /// spill gradients between micro-batches. Runs ONE batch step per call;
    /// the caller drives the outer training loop.
    real_forward,
    /// Real BERT forward pass built as an `ml.graph` computation graph, with
    /// LoRA injected via `ml.graph.lora.injectLoRA` and gradients computed
    /// by the autodiff system through every encoder layer — i.e., actual
    /// full LoRA training equivalent to HF PEFT / Unsloth semantics. Uses
    /// `real_autodiff_trainer.zig` as the harness. Runs ONE batch step per
    /// call. Backend-agnostic via the ComputeBackend polymorphism.
    real_autodiff,
};

// ---------------------------------------------------------------------------
// Input variants
// ---------------------------------------------------------------------------

pub const CachedSurrogateInput = struct {
    model_dir: []const u8,
    adapter_model_dir: []const u8,
    head_input: []const u8,
    train_examples: []const reranker_data.Example,
    eval_examples: []const reranker_data.Example,
    out_dir: []const u8,
    backend: reranker_head.BackendChoice,
    options: reranker_lora.SurrogateTrainOptions,
};

pub const RealForwardInput = struct {
    compute_backend: *const ops.ComputeBackend,
    bert_config: bert_types.Config,
    adapter_set: *LoRAAdapterSet,
    head_weight: []f32,
    head_bias: *f32,
    grad_head_weight: []f32,
    grad_head_bias: *f32,
    examples: []const reranker_real_forward.RealForwardExample,
    config: reranker_real_forward.RealForwardTrainConfig,
    step: u64,
    coord: ?*TrainingMemoryCoordinator = null,
};

/// Input for the real-autodiff path. The caller owns a `RealAutodiffTrainer`
/// (built once at training setup with LoRA config + compute backend) and
/// passes it plus a `TrainerInput` containing the current batch. The step
/// dispatches through `trainer.step` which handles graph build (first step
/// only), LoRA injection, autodiff, and optimizer update.
///
/// For BERT-family rerankers, a convenience builder `bertTrainerInput` below
/// constructs the `TrainerInput` with callbacks that call
/// `bert_graph.buildForwardGraph` + a BCE head.
pub const RealAutodiffInput = struct {
    trainer: *real_autodiff.RealAutodiffTrainer,
    trainer_input: real_autodiff.TrainerInput,
};

/// Re-export so reranker_train's callers can name the graph config without
/// pulling the architectures path directly.
pub const BertGraphConfig = bert_graph.Config;

pub const RerankerTrainInput = union(RerankerTrainingMode) {
    cached_surrogate: CachedSurrogateInput,
    real_forward: RealForwardInput,
    real_autodiff: RealAutodiffInput,
};

// ---------------------------------------------------------------------------
// Result variants
// ---------------------------------------------------------------------------

pub const RerankerTrainResult = union(RerankerTrainingMode) {
    cached_surrogate: reranker_lora.SurrogateTrainEvalSummary,
    real_forward: reranker_real_forward.StepResult,
    real_autodiff: real_autodiff.StepResult,

    pub fn loss(self: RerankerTrainResult) f64 {
        return switch (self) {
            .cached_surrogate => |s| s.after_eval.average_loss,
            .real_forward => |r| @floatCast(r.loss),
            .real_autodiff => |r| @floatCast(r.loss),
        };
    }
};

// ---------------------------------------------------------------------------
// Single entry point
// ---------------------------------------------------------------------------

/// Dispatch training to the underlying mode-specific implementation.
///
/// NOTE: the two modes have different loop semantics:
///   * `cached_surrogate` runs a full multi-epoch session and returns an
///     epoch-level summary. Call it once per training run.
///   * `real_forward` runs ONE batch step and returns a step result. Call
///     it inside an outer loop the caller owns.
pub fn train(
    allocator: std.mem.Allocator,
    input: RerankerTrainInput,
) !RerankerTrainResult {
    switch (input) {
        .cached_surrogate => |inp| {
            const summary = try reranker_lora.trainEvalSurrogate(
                allocator,
                inp.model_dir,
                inp.adapter_model_dir,
                inp.head_input,
                inp.train_examples,
                inp.eval_examples,
                inp.out_dir,
                inp.backend,
                inp.options,
            );
            return .{ .cached_surrogate = summary };
        },
        .real_forward => |inp| {
            const result = try reranker_real_forward.trainStep(
                allocator,
                inp.compute_backend,
                inp.bert_config,
                inp.adapter_set,
                inp.head_weight,
                inp.head_bias,
                inp.grad_head_weight,
                inp.grad_head_bias,
                inp.examples,
                inp.config,
                inp.step,
                inp.coord,
            );
            return .{ .real_forward = result };
        },
        .real_autodiff => |inp| {
            const result = try inp.trainer.step(inp.trainer_input);
            return .{ .real_autodiff = result };
        },
    }
}

// ---------------------------------------------------------------------------
// Convenience constructors — thin wrappers so callers do not have to spell
// the union variant payload inline at call sites.
// ---------------------------------------------------------------------------

pub fn cachedInput(inp: CachedSurrogateInput) RerankerTrainInput {
    return .{ .cached_surrogate = inp };
}

pub fn realForwardInput(inp: RealForwardInput) RerankerTrainInput {
    return .{ .real_forward = inp };
}

pub fn realAutodiffInput(inp: RealAutodiffInput) RerankerTrainInput {
    return .{ .real_autodiff = inp };
}

// ---------------------------------------------------------------------------
// BERT-graph convenience: builds a TrainerInput whose callbacks construct
// the BERT forward graph from `bert_graph.buildForwardGraph` and attach a
// simple pooled classifier head (CLS pooling + linear + sigmoid BCE).
//
// The caller must keep `ctx` alive for the duration of `step`. The context
// stores the graph config + pre-allocated shapes so the callbacks can
// reference them without heap allocation inside the build_forward closure.
// ---------------------------------------------------------------------------

pub const BertAutodiffCtx = struct {
    graph_config: bert_graph.Config,
    built: ?bert_graph.BertGraph = null,

    fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *ml.graph.Builder,
        input_ids: ml.graph.NodeId,
        attention_mask: ml.graph.NodeId,
        batch: u32,
        seq_len: u32,
    ) anyerror!ml.graph.NodeId {
        const self: *BertAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const total: i64 = @intCast(@as(i64, @intCast(batch)) * @as(i64, @intCast(seq_len)));
        const bh: i64 = @intCast(@as(u32, batch) * self.graph_config.num_attention_heads);

        // The harness provides `input_ids` and `attention_mask`. We still
        // need the derived BERT-specific inputs as new placeholder nodes
        // the caller must bind at execution time:
        //   - position_ids: [total] i64 — derived from seq_len per batch.
        //   - token_type_ids: [total] i64 — default 0.
        //   - attn_bias: [bh, seq, seq] f32 — built from attention_mask.
        //
        // For the MVP these are fresh `bld.parameter` placeholders; a future
        // refactor will derive attn_bias from `attention_mask` inside the
        // graph via primitive ops (sub + mul + broadcast + reshape).
        _ = attention_mask;
        const position_ids = try bld.parameter(
            "__bert_position_ids",
            ml.graph.Shape.init(.i64, &.{total}),
        );
        const token_type_ids: ?ml.graph.NodeId = if (self.graph_config.use_token_type)
            try bld.parameter("__bert_token_type_ids", ml.graph.Shape.init(.i64, &.{total}))
        else
            null;
        const attn_bias = try bld.parameter(
            "__bert_attn_bias",
            ml.graph.Shape.init(.f32, &.{ bh, @intCast(seq_len), @intCast(seq_len) }),
        );

        self.built = try bert_graph.buildForwardGraph(
            bld,
            self.graph_config,
            batch,
            seq_len,
            .{
                .input_ids = input_ids,
                .position_ids = position_ids,
                .token_type_ids = token_type_ids,
                .attn_bias = attn_bias,
            },
        );
        return self.built.?.output_node;
    }

    fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *ml.graph.Builder,
        forward_output: ml.graph.NodeId,
        targets: ml.graph.NodeId,
    ) anyerror!ml.graph.NodeId {
        _ = ctx_opaque;
        return bld.mseLoss(forward_output, targets);
    }

    /// Bind BERT-specific placeholders (position_ids, token_type_ids,
    /// attn_bias) into the runtime map using graph_input_binder helpers.
    fn bindArchInputs(
        ctx_opaque: *anyopaque,
        cb: *const ops.ComputeBackend,
        allocator: std.mem.Allocator,
        graph: *const ml.graph.Graph,
        rt_map: *std.AutoHashMapUnmanaged(ml.graph.NodeId, ops.CT),
        batch: u32,
        seq_len: u32,
        attention_mask: []const f32,
    ) anyerror!void {
        const self: *BertAutodiffCtx = @ptrCast(@alignCast(ctx_opaque));
        const total = batch * seq_len;
        const num_heads = self.graph_config.num_attention_heads;

        // Position IDs: [0, 1, ..., seq_len-1] repeated per batch item.
        const pos_ids = try graph_input_binder.BertPlaceholderPrep.buildPositionIds(allocator, batch, seq_len);
        defer allocator.free(pos_ids);
        if (graph_input_binder.findParameterByName(graph, "__bert_position_ids")) |node_id| {
            const pos_f32 = try allocator.alloc(f32, total);
            defer allocator.free(pos_f32);
            for (pos_ids, 0..) |id, i| pos_f32[i] = @floatFromInt(id);
            const dims = [_]i32{@intCast(total)};
            const ct = try cb.fromFloat32Shape(pos_f32, &dims);
            try rt_map.put(allocator, node_id, ct);
        }

        // Token type IDs: all zeros.
        if (graph_input_binder.findParameterByName(graph, "__bert_token_type_ids")) |node_id| {
            const tt_f32 = try allocator.alloc(f32, total);
            defer allocator.free(tt_f32);
            @memset(tt_f32, 0);
            const dims = [_]i32{@intCast(total)};
            const ct = try cb.fromFloat32Shape(tt_f32, &dims);
            try rt_map.put(allocator, node_id, ct);
        }

        // Attention bias: derived from attention_mask via buildAttnBias.
        // Produces [batch * num_heads, seq_len, seq_len] with -1e9 at
        // padded positions and 0.0 at valid positions.
        if (graph_input_binder.findParameterByName(graph, "__bert_attn_bias")) |node_id| {
            const bias = try graph_input_binder.BertPlaceholderPrep.buildAttnBias(
                allocator,
                attention_mask,
                batch,
                seq_len,
                num_heads,
            );
            defer allocator.free(bias);
            const dims = [_]i32{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len) };
            const ct = try cb.fromFloat32Shape(bias, &dims);
            try rt_map.put(allocator, node_id, ct);
        }
    }
};

pub fn bertTrainerInput(
    ctx: *BertAutodiffCtx,
    input_ids: []const i64,
    attention_mask: []const f32,
    targets: []const f32,
    targets_shape: ml.graph.Shape,
    batch: u32,
    seq_len: u32,
) real_autodiff.TrainerInput {
    return .{
        .ctx = @ptrCast(ctx),
        .build_forward = &BertAutodiffCtx.buildForward,
        .build_loss = &BertAutodiffCtx.buildLoss,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .targets = targets,
        .targets_shape = targets_shape,
        .batch = batch,
        .seq_len = seq_len,
        .bind_arch_inputs = &BertAutodiffCtx.bindArchInputs,
    };
}

test "RerankerTrainingMode variants are distinct" {
    try std.testing.expect(RerankerTrainingMode.cached_surrogate != RerankerTrainingMode.real_forward);
    const input = RerankerTrainInput{
        .real_forward = undefined,
    };
    try std.testing.expectEqual(RerankerTrainingMode.real_forward, @as(RerankerTrainingMode, input));
}
