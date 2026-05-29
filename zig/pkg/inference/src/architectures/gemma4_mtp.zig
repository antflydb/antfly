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
const platform = @import("antfly_platform");

const activations = @import("../backends/activations.zig");
const gpt_arch = @import("gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");

pub const DraftResult = struct {
    token: usize,
    projected_activation: []f32,
};

pub const DraftRequest = struct {
    allocator: std.mem.Allocator,
    target_cb: *const ops.ComputeBackend,
    draft_cb: *const ops.ComputeBackend,
    target_config: gpt_mod.Config,
    draft_config: gpt_mod.Config,
    token_id: i64,
    activation: []const f32,
    decode_context: *const gpt_arch.DecodeContext,
};

fn targetKvDonorLayer(target_config: gpt_mod.Config, wants_sliding: bool) ?u32 {
    if (target_config.num_kv_shared_layers == 0 or target_config.num_hidden_layers <= target_config.num_kv_shared_layers) return null;
    const non_shared_layers = target_config.num_hidden_layers - target_config.num_kv_shared_layers;
    var donor: ?u32 = null;
    for (0..non_shared_layers) |layer| {
        if (target_config.layerUsesSlidingAttention(layer) == wants_sliding) {
            donor = @intCast(layer);
        }
    }
    return donor;
}

fn maskedEmbeddingArgmax(
    request: DraftRequest,
    draft_cfg: gpt_mod.Config,
    assistant_hidden: ops.CT,
    logits: []const f32,
) !?usize {
    if (!draft_cfg.mtp_use_ordered_embeddings or draft_cfg.mtp_num_centroids == 0 or draft_cfg.mtp_centroid_intermediate_top_k == 0) return null;
    const allocator = request.allocator;
    const vocab_size: usize = @intCast(draft_cfg.vocab_size);
    const num_centroids: usize = @intCast(draft_cfg.mtp_num_centroids);
    if (vocab_size == 0 or num_centroids == 0 or vocab_size % num_centroids != 0) return null;

    const centroid_w = request.draft_cb.getWeight("masked_embedding.centroids.weight") catch return null;
    defer request.draft_cb.free(centroid_w);
    const centroid_logits_ct = try request.draft_cb.linearNoBias(
        assistant_hidden,
        centroid_w,
        1,
        @intCast(draft_cfg.hidden_size),
        @intCast(num_centroids),
    );
    defer request.draft_cb.free(centroid_logits_ct);
    const centroid_logits = try request.draft_cb.toFloat32(centroid_logits_ct, allocator);
    defer allocator.free(centroid_logits);
    if (centroid_logits.len != num_centroids) return error.InvalidTensorShape;

    const top_k = @min(@as(usize, @intCast(draft_cfg.mtp_centroid_intermediate_top_k)), num_centroids);
    var top_centroids = try allocator.alloc(usize, top_k);
    defer allocator.free(top_centroids);
    var top_scores = try allocator.alloc(f32, top_k);
    defer allocator.free(top_scores);
    @memset(top_centroids, 0);
    @memset(top_scores, -std.math.inf(f32));
    for (centroid_logits, 0..) |score, centroid| {
        var insert_at: usize = top_k;
        for (top_scores, 0..) |existing, idx| {
            if (score > existing) {
                insert_at = idx;
                break;
            }
        }
        if (insert_at == top_k) continue;
        var move_idx = top_k - 1;
        while (move_idx > insert_at) : (move_idx -= 1) {
            top_scores[move_idx] = top_scores[move_idx - 1];
            top_centroids[move_idx] = top_centroids[move_idx - 1];
        }
        top_scores[insert_at] = score;
        top_centroids[insert_at] = centroid;
    }

    const ordering_w = request.draft_cb.getWeight("masked_embedding.token_ordering") catch return null;
    defer request.draft_cb.free(ordering_w);
    const ordering_host = try request.draft_cb.toFloat32(ordering_w, allocator);
    defer allocator.free(ordering_host);
    if (ordering_host.len != vocab_size) return error.InvalidTensorShape;

    const cluster_size = vocab_size / num_centroids;
    var best_token: usize = 0;
    var best_score = -std.math.inf(f32);
    const use_inverse_ordering = getenvBool("ANTFLY_INFERENCE_GEMMA4_MTP_INVERSE_TOKEN_ORDERING");
    for (top_centroids) |centroid| {
        const start = centroid * cluster_size;
        const end = start + cluster_size;
        if (use_inverse_ordering) {
            for (ordering_host, 0..) |ordered_pos_float, token| {
                if (ordered_pos_float < 0) continue;
                const ordered_pos: usize = @intFromFloat(ordered_pos_float);
                if (ordered_pos < start or ordered_pos >= end) continue;
                const score = logits[token];
                if (score > best_score) {
                    best_score = score;
                    best_token = token;
                }
            }
        } else {
            for (ordering_host[start..end]) |token_float| {
                if (token_float < 0) continue;
                const token: usize = @intFromFloat(token_float);
                if (token >= vocab_size) continue;
                const score = logits[token];
                if (score > best_score) {
                    best_score = score;
                    best_token = token;
                }
            }
        }
    }
    return best_token;
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
}

/// Run one Gemma 4 MTP assistant draft step.
///
/// Gemma 4 assistant checkpoints are query-only draft heads. They consume the
/// previous token's target embedding plus a target/backbone activation, run the
/// compact assistant stack against target KV, and project back to target hidden
/// width for the next chained draft step. Draft logits come from the assistant
/// hidden state and its tied assistant embeddings.
pub fn draftToken(request: DraftRequest) !DraftResult {
    const allocator = request.allocator;
    var draft_cfg = request.draft_config;
    const backbone_hidden: usize = @intCast(draft_cfg.mtp_backbone_hidden_size);
    const draft_hidden: usize = @intCast(draft_cfg.hidden_size);
    if (!draft_cfg.gemma4_mtp_assistant or backbone_hidden == 0) return error.InvalidDraftModelForGeneration;
    if (request.activation.len != backbone_hidden) return error.InvalidTensorShape;
    if (request.target_config.hidden_size != backbone_hidden) return error.IncompatibleDraftModel;
    draft_cfg.mtp_kv_sliding_donor_layer = targetKvDonorLayer(request.target_config, true) orelse return error.IncompatibleDraftModel;
    draft_cfg.mtp_kv_full_donor_layer = targetKvDonorLayer(request.target_config, false) orelse return error.IncompatibleDraftModel;

    const target_embed_w = try gpt_arch.getEmbeddingWeight(request.target_cb, request.target_config);
    defer request.target_cb.free(target_embed_w);
    const token_arr = [_]i64{request.token_id};
    const target_embedded = try request.target_cb.embeddingLookup(target_embed_w, &token_arr, 1, backbone_hidden);
    const target_embedding = try gpt_arch.maybeScaleTokenEmbeddings(
        request.target_cb,
        allocator,
        request.target_config,
        target_embedded,
        1,
        backbone_hidden,
    );
    defer request.target_cb.free(target_embedding);
    const target_embedding_host = try request.target_cb.toFloat32(target_embedding, allocator);
    defer allocator.free(target_embedding_host);
    if (target_embedding_host.len != backbone_hidden) return error.InvalidTensorShape;

    const concat_host = try allocator.alloc(f32, backbone_hidden * 2);
    defer allocator.free(concat_host);
    @memcpy(concat_host[0..backbone_hidden], target_embedding_host);
    @memcpy(concat_host[backbone_hidden..][0..backbone_hidden], request.activation);
    const concat_shape = [_]i32{ 1, @intCast(backbone_hidden * 2) };
    const concat_ct = try request.draft_cb.fromFloat32Shape(concat_host, &concat_shape);
    defer request.draft_cb.free(concat_ct);

    const pre_w = try request.draft_cb.getWeight("pre_projection.weight");
    defer request.draft_cb.free(pre_w);
    const assistant_input = try request.draft_cb.linearNoBias(concat_ct, pre_w, 1, backbone_hidden * 2, draft_hidden);

    const assistant_hidden = try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
        request.draft_cb,
        allocator,
        draft_cfg,
        assistant_input,
        .{},
        1,
        request.decode_context.total_sequence_len,
        request.decode_context,
        null,
    );
    defer request.draft_cb.free(assistant_hidden);

    const draft_lm_w = try gpt_arch.getEmbeddingWeight(request.draft_cb, draft_cfg);
    defer request.draft_cb.free(draft_lm_w);
    const logits_ct = try request.draft_cb.linearNoBias(
        assistant_hidden,
        draft_lm_w,
        1,
        draft_hidden,
        draft_cfg.vocab_size,
    );
    defer request.draft_cb.free(logits_ct);
    const logits = try request.draft_cb.toFloat32(logits_ct, allocator);
    defer allocator.free(logits);

    const post_w = try request.draft_cb.getWeight("post_projection.weight");
    defer request.draft_cb.free(post_w);
    const projected = try request.draft_cb.linearNoBias(assistant_hidden, post_w, 1, draft_hidden, backbone_hidden);
    defer request.draft_cb.free(projected);
    const projected_host = try request.draft_cb.toFloat32(projected, allocator);
    errdefer allocator.free(projected_host);
    if (projected_host.len != backbone_hidden) return error.InvalidTensorShape;

    const token = (try maskedEmbeddingArgmax(request, draft_cfg, assistant_hidden, logits)) orelse
        activations.argmax(logits[0..draft_cfg.vocab_size]);
    return .{
        .token = token,
        .projected_activation = projected_host,
    };
}
