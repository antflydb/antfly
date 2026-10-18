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

//! RealAutodiffTrainer callbacks for paired EmbeddingGemma 2 retrieval.

const std = @import("std");
const ml = @import("ml");
const graph_mod = @import("embedding_gemma2_graph.zig");
const training = @import("embedding_gemma2_training.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");

const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;

/// Batches are laid out as [queries..., documents...]. The document suffix is
/// the complete candidate pool and may contain additional hard negatives.
/// Both sides pass through the same trainable encoder and receive gradients.
pub const Context = struct {
    config: graph_mod.Config,
    plan: training.TrainingPlan = .{},
    temperature: f32 = 0.05,
    query_count: u32,
    active_document_count: u32 = 0,
    built: ?graph_mod.Built = null,

    pub fn validate(self: Context) !void {
        try self.config.validate();
        if (!std.math.isFinite(self.temperature) or self.temperature <= 0)
            return error.InvalidEmbeddingGemma2Loss;
        if (self.plan.max_sequence > self.config.max_sequence)
            return error.InvalidEmbeddingGemma2TrainingPlan;
    }

    pub fn buildForward(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        input_ids: NodeId,
        attention_mask: NodeId,
        batch: u32,
        sequence: u32,
    ) anyerror!NodeId {
        const self: *Context = @ptrCast(@alignCast(ctx_opaque));
        try self.validate();
        if (self.query_count == 0 or self.query_count >= batch or sequence > self.plan.max_sequence)
            return error.InvalidEmbeddingGemma2ContrastiveBatch;
        self.active_document_count = batch - self.query_count;
        const requested_dim = self.plan.mrl_dimensions[self.plan.mrl_dimensions.len - 1];
        self.built = try graph_mod.buildForward(bld, self.config, batch, sequence, requested_dim, .{
            .input_ids = input_ids,
            .attention_mask = attention_mask,
        });
        return self.built.?.embedding;
    }

    pub fn buildLoss(
        ctx_opaque: *anyopaque,
        bld: *Builder,
        embeddings: NodeId,
        targets: NodeId,
    ) anyerror!NodeId {
        const self: *Context = @ptrCast(@alignCast(ctx_opaque));
        const query_count = self.query_count;
        const document_count = self.active_document_count;
        if (query_count == 0 or document_count == 0) return error.InvalidEmbeddingGemma2ContrastiveBatch;
        const dim = self.plan.mrl_dimensions[self.plan.mrl_dimensions.len - 1];
        const transposed = try bld.transpose(embeddings, &.{ 1, 0 });
        const queries_t = try bld.sliceLastDim(transposed, 0, @intCast(query_count));
        const documents_t = try bld.sliceLastDim(transposed, @intCast(query_count), @intCast(query_count + document_count));
        const queries = try bld.transpose(queries_t, &.{ 1, 0 });
        const documents = try bld.transpose(documents_t, &.{ 1, 0 });
        const positive_targets = try bld.sliceLastDim(targets, 0, @intCast(document_count));
        const allowed = try bld.sliceLastDim(targets, @intCast(document_count), @intCast(document_count * 2));

        // Each MRL prefix is truncated and normalized independently. Targets
        // are an identity matrix uploaded by makeIdentityTargets.
        var total: ?NodeId = null;
        var prior: u32 = 0;
        for (self.plan.mrl_dimensions) |mrl_dim| {
            if (mrl_dim == 0 or mrl_dim > dim or mrl_dim <= prior) return error.InvalidEmbeddingGemma2TrainingPlan;
            prior = mrl_dim;
            const q = try truncateAndNormalize(bld, queries, query_count, dim, mrl_dim);
            const d = try truncateAndNormalize(bld, documents, document_count, dim, mrl_dim);
            const loss = try graph_mod.buildAsymmetricMaskedInfoNce(bld, q, d, positive_targets, allowed, query_count, document_count, mrl_dim, self.temperature);
            total = if (total) |sum| try bld.add(sum, loss) else loss;
        }
        return bld.mul(total.?, try bld.scalarConst(.f32, 1.0 / @as(f32, @floatFromInt(self.plan.mrl_dimensions.len))));
    }

    pub fn targetsShape(query_count: u32, document_count: u32) !Shape {
        if (query_count == 0 or document_count == 0) return error.InvalidEmbeddingGemma2ContrastiveBatch;
        return Shape.init(.f32, &.{ @as(i64, @intCast(query_count)), @as(i64, @intCast(document_count * 2)) });
    }

    pub fn trainerConfig(self: *const Context) real_autodiff.TrainerConfig {
        return .{
            .lora = .{
                .rank = self.plan.rank,
                .alpha = self.plan.alpha,
                .dropout = self.plan.dropout,
                .target_patterns = &training.default_target_suffixes,
                .strict_target_patterns = true,
            },
            .hidden_size_hint = self.config.hidden_size,
            .num_layers_hint = self.config.layer_count,
            .graph_cache_capacity = 1,
        };
    }
};

pub fn makeTargets(allocator: std.mem.Allocator, query_count: u32, document_count: u32, positive_document_indices: []const u32, allowed_candidates: ?[]const u8) ![]f32 {
    if (query_count == 0 or document_count == 0 or positive_document_indices.len != query_count or
        (allowed_candidates != null and allowed_candidates.?.len != @as(usize, query_count) * document_count))
        return error.InvalidEmbeddingGemma2ContrastiveBatch;
    const columns = std.math.mul(usize, document_count, 2) catch return error.InvalidEmbeddingGemma2ContrastiveBatch;
    const targets = try allocator.alloc(f32, std.math.mul(usize, query_count, columns) catch return error.InvalidEmbeddingGemma2ContrastiveBatch);
    @memset(targets, 0);
    for (positive_document_indices, 0..) |document, query| {
        if (document >= document_count or (allowed_candidates != null and allowed_candidates.?[query * document_count + document] == 0)) {
            allocator.free(targets);
            return error.InvalidEmbeddingGemma2ContrastiveBatch;
        }
        targets[query * columns + document] = 1;
        for (0..document_count) |candidate| {
            const allowed = if (allowed_candidates) |mask| mask[query * document_count + candidate] else 1;
            if (allowed > 1) {
                allocator.free(targets);
                return error.InvalidEmbeddingGemma2ContrastiveBatch;
            }
            targets[query * columns + document_count + candidate] = @floatFromInt(allowed);
        }
    }
    return targets;
}

fn truncateAndNormalize(bld: *Builder, full: NodeId, rows: u32, full_dim: u32, dim: u32) !NodeId {
    return graph_mod.truncateL2(bld, full, rows, full_dim, dim);
}

test "EmbeddingGemma2 paired trainer context builds true contrastive gradients" {
    const allocator = std.testing.allocator;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var bld = Builder.init(&graph);
    var context = Context{
        .config = .{
            .vocab_size = 11,
            .hidden_size = 4,
            .layer_count = 2,
            .intermediate_size = 8,
            .embedding_dim = 6,
            .query_heads = 2,
            .local_kv_heads = 1,
            .global_kv_heads = 1,
            .local_head_dim = 2,
            .global_head_dim = 2,
            .local_radius = 1,
            .global_every = 2,
            .max_sequence = 4,
        },
        .query_count = 2,
        .plan = .{ .rank = 2, .alpha = 4, .max_sequence = 4, .mrl_dimensions = &.{ 3, 6 } },
    };
    const ids = try bld.parameter("ids", Shape.init(.f32, &.{ 4, 2 }));
    const mask = try bld.parameter("mask", Shape.init(.f32, &.{ 4, 2 }));
    const embeddings = try Context.buildForward(@ptrCast(&context), &bld, ids, mask, 4, 2);
    const targets = try bld.parameter("targets", try Context.targetsShape(2, 2));
    const loss = try Context.buildLoss(@ptrCast(&context), &bld, embeddings, targets);
    const seed = try bld.parameter("seed", graph.node(loss).output_shape);
    var projection: ?NodeId = null;
    for (graph.parameters.items) |parameter_id| {
        if (std.mem.eql(u8, graph.parameterName(graph.node(parameter_id)), "language_model.embedding_projection.weight")) {
            projection = parameter_id;
            break;
        }
    }
    var gradients = try ml.graph.autodiff.gradientWithSeeds(allocator, &graph, &.{.{ .output = loss, .cotangent = seed }}, &.{projection orelse return error.MissingProjectionParameter}, .{ .require_all_gradients = true });
    defer gradients.deinit();
    try std.testing.expect(gradients.param_grads[0] != ml.graph.null_node);
}

test "EmbeddingGemma2 targets support hard negatives and masked alternate positives" {
    const targets = try makeTargets(std.testing.allocator, 2, 4, &.{ 1, 3 }, &.{ 1, 1, 0, 1, 1, 0, 1, 1 });
    defer std.testing.allocator.free(targets);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 0, 1, 1 }, targets);
}
