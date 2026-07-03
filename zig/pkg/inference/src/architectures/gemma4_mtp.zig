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

pub const DraftLogitSource = enum {
    linear_argmax,
    masked_embedding,
    backend_argmax,
    host_argmax,

    pub fn name(self: DraftLogitSource) []const u8 {
        return switch (self) {
            .linear_argmax => "linear_argmax",
            .masked_embedding => "masked_embedding",
            .backend_argmax => "backend_argmax",
            .host_argmax => "host_argmax",
        };
    }
};

pub const ConcatOrder = enum {
    embedding_activation,
    activation_embedding,

    pub fn name(self: ConcatOrder) []const u8 {
        return switch (self) {
            .embedding_activation => "embedding_activation",
            .activation_embedding => "activation_embedding",
        };
    }
};

pub const KvDonorMode = enum {
    shared_type,
    tail_base,
    assistant_index,
    non_shared_tail_base,
    first_shared_base,

    pub fn name(self: KvDonorMode) []const u8 {
        return switch (self) {
            .shared_type => "shared_type",
            .tail_base => "tail_base",
            .assistant_index => "assistant_index",
            .non_shared_tail_base => "non_shared_tail_base",
            .first_shared_base => "first_shared_base",
        };
    }
};

pub const DraftResult = struct {
    token: usize,
    projected_activation: []f32,
    logits: ?[]f32 = null,
    logit_source: DraftLogitSource = .host_argmax,
};

pub const DraftDeviceResult = struct {
    token: usize,
    projected_activation: ops.CT,
    logits: ?[]f32 = null,
    logit_source: DraftLogitSource = .host_argmax,
};

pub fn parseConcatOrder(raw: ?[]const u8) ConcatOrder {
    const value = raw orelse return .embedding_activation;
    if (std.mem.eql(u8, value, "activation_embedding")) return .activation_embedding;
    if (std.mem.eql(u8, value, "embedding_activation")) return .embedding_activation;
    return .embedding_activation;
}

pub fn parseKvDonorMode(raw: ?[]const u8) KvDonorMode {
    const value = raw orelse return .shared_type;
    if (std.mem.eql(u8, value, "tail_base")) return .tail_base;
    if (std.mem.eql(u8, value, "assistant_index")) return .assistant_index;
    if (std.mem.eql(u8, value, "non_shared_tail_base")) return .non_shared_tail_base;
    if (std.mem.eql(u8, value, "first_shared_base")) return .first_shared_base;
    if (std.mem.eql(u8, value, "shared_type")) return .shared_type;
    return .shared_type;
}

pub fn concatOrderFromEnv() ConcatOrder {
    const raw = platform.env.getenv("ANTFLY_GEMMA4_MTP_CONCAT_ORDER");
    return parseConcatOrder(raw);
}

pub fn kvDonorModeFromEnv() KvDonorMode {
    const raw = platform.env.getenv("ANTFLY_GEMMA4_MTP_KV_DONOR_MODE");
    return parseKvDonorMode(raw);
}

fn fillConcatInput(dest: []f32, target_embedding: []const f32, activation: []const f32, order: ConcatOrder) !void {
    if (target_embedding.len != activation.len or dest.len != target_embedding.len * 2) return error.InvalidTensorShape;
    const backbone_hidden = target_embedding.len;
    switch (order) {
        .embedding_activation => {
            @memcpy(dest[0..backbone_hidden], target_embedding);
            @memcpy(dest[backbone_hidden..][0..backbone_hidden], activation);
        },
        .activation_embedding => {
            @memcpy(dest[0..backbone_hidden], activation);
            @memcpy(dest[backbone_hidden..][0..backbone_hidden], target_embedding);
        },
    }
}

pub fn mtpKvDonorBaseLayer(target_config: gpt_mod.Config, draft_config: gpt_mod.Config, mode: KvDonorMode) !?u32 {
    const target_layers = target_config.num_hidden_layers;
    const draft_layers = draft_config.num_hidden_layers;
    if (target_layers < draft_layers) return error.IncompatibleDraftModel;
    return switch (mode) {
        .shared_type => blk: {
            if (!(target_config.num_kv_shared_layers == 0 and draft_config.mtp_num_centroids == 0)) break :blk null;
            break :blk target_layers - draft_layers;
        },
        .tail_base => target_layers - draft_layers,
        .assistant_index => 0,
        .non_shared_tail_base => blk: {
            const non_shared_layers = if (target_config.num_kv_shared_layers > 0)
                target_layers - target_config.num_kv_shared_layers
            else
                target_layers;
            if (non_shared_layers < draft_layers) return error.IncompatibleDraftModel;
            break :blk non_shared_layers - draft_layers;
        },
        .first_shared_base => blk: {
            if (target_config.num_kv_shared_layers == 0) break :blk target_layers - draft_layers;
            const first_shared = target_layers - target_config.num_kv_shared_layers;
            if (first_shared + draft_layers > target_layers) return error.IncompatibleDraftModel;
            break :blk first_shared;
        },
    };
}

pub const DraftRequest = struct {
    allocator: std.mem.Allocator,
    target_cb: *const ops.ComputeBackend,
    draft_cb: *const ops.ComputeBackend,
    target_config: gpt_mod.Config,
    draft_config: gpt_mod.Config,
    token_id: i64,
    activation: []const f32,
    decode_context: *const gpt_arch.DecodeContext,
    debug_top_k: usize = 0,
};

pub const DraftDeviceRequest = struct {
    allocator: std.mem.Allocator,
    target_cb: *const ops.ComputeBackend,
    draft_cb: *const ops.ComputeBackend,
    target_config: gpt_mod.Config,
    draft_config: gpt_mod.Config,
    token_id: i64,
    activation: ops.CT,
    target_embedding_on_draft: ?ops.CT = null,
    decode_context: *const gpt_arch.DecodeContext,
    debug_top_k: usize = 0,
};

pub fn deviceResidentDraftAllowed(debug_top_k: usize) bool {
    return debug_top_k == 0 and
        !mtpStageTraceEnabled() and
        !getenvBool("ANTFLY_GEMMA4_MTP_DISABLE_DEVICE_RESIDENT");
}

fn targetKvDonorLayer(target_config: gpt_mod.Config, wants_sliding: bool) ?u32 {
    if (target_config.num_hidden_layers == 0 or target_config.num_hidden_layers <= target_config.num_kv_shared_layers) return null;
    const non_shared_layers = if (target_config.num_kv_shared_layers > 0)
        target_config.num_hidden_layers - target_config.num_kv_shared_layers
    else
        target_config.num_hidden_layers;
    var donor: ?u32 = null;
    for (0..non_shared_layers) |layer| {
        if (target_config.layerUsesSlidingAttention(layer) == wants_sliding) {
            donor = @intCast(layer);
        }
    }
    return donor;
}

fn prepareDraftConfigForMtp(
    target_config: gpt_mod.Config,
    draft_config: *gpt_mod.Config,
    kv_donor_mode: KvDonorMode,
) !void {
    if (try mtpKvDonorBaseLayer(target_config, draft_config.*, kv_donor_mode)) |base_layer| {
        draft_config.mtp_kv_donor_base_layer = base_layer;
    } else {
        draft_config.mtp_kv_sliding_donor_layer = targetKvDonorLayer(target_config, true) orelse return error.IncompatibleDraftModel;
        draft_config.mtp_kv_full_donor_layer = targetKvDonorLayer(target_config, false) orelse return error.IncompatibleDraftModel;
    }
}

fn maskedEmbeddingArgmaxForBackend(
    allocator: std.mem.Allocator,
    draft_cb: *const ops.ComputeBackend,
    draft_cfg: gpt_mod.Config,
    assistant_hidden: ops.CT,
    logits_ct: ops.CT,
) !?usize {
    if (!draft_cfg.mtp_use_ordered_embeddings or draft_cfg.mtp_num_centroids == 0 or draft_cfg.mtp_centroid_intermediate_top_k == 0) return null;
    const vocab_size: usize = @intCast(draft_cfg.vocab_size);
    const num_centroids: usize = @intCast(draft_cfg.mtp_num_centroids);
    if (vocab_size == 0 or num_centroids == 0 or vocab_size % num_centroids != 0) return null;

    const centroid_w = draft_cb.getWeight("masked_embedding.centroids.weight") catch return null;
    defer draft_cb.free(centroid_w);
    const centroid_logits_ct = try draft_cb.linearNoBias(
        assistant_hidden,
        centroid_w,
        1,
        @intCast(draft_cfg.hidden_size),
        @intCast(num_centroids),
    );
    defer draft_cb.free(centroid_logits_ct);
    const ordering_w = draft_cb.getWeight("masked_embedding.token_ordering") catch return null;
    defer draft_cb.free(ordering_w);
    const top_k = @min(@as(usize, @intCast(draft_cfg.mtp_centroid_intermediate_top_k)), num_centroids);
    const use_inverse_ordering = getenvBool("ANTFLY_INFERENCE_GEMMA4_MTP_INVERSE_TOKEN_ORDERING");
    if (!getenvBool("ANTFLY_CUDA_DISABLE_GEMMA4_MTP_DEVICE")) {
        if (try draft_cb.gemma4MtpMaskedArgmax(&.{
            .logits = logits_ct,
            .centroid_logits = centroid_logits_ct,
            .token_ordering = ordering_w,
            .vocab_size = vocab_size,
            .num_centroids = num_centroids,
            .top_k = top_k,
            .use_inverse_ordering = use_inverse_ordering,
        })) |token| {
            return @intCast(token);
        }
    }

    const logits = try draft_cb.toFloat32(logits_ct, allocator);
    defer allocator.free(logits);
    if (logits.len < vocab_size) return error.InvalidTensorShape;
    const centroid_logits = try draft_cb.toFloat32(centroid_logits_ct, allocator);
    defer allocator.free(centroid_logits);
    if (centroid_logits.len != num_centroids) return error.InvalidTensorShape;

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

    const ordering_host = try draft_cb.toFloat32(ordering_w, allocator);
    defer allocator.free(ordering_host);
    if (ordering_host.len != vocab_size) return error.InvalidTensorShape;

    const cluster_size = vocab_size / num_centroids;
    var best_token: usize = 0;
    var best_score = -std.math.inf(f32);
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

fn maskedEmbeddingArgmax(
    request: DraftRequest,
    draft_cfg: gpt_mod.Config,
    assistant_hidden: ops.CT,
    logits_ct: ops.CT,
) !?usize {
    return maskedEmbeddingArgmaxForBackend(request.allocator, request.draft_cb, draft_cfg, assistant_hidden, logits_ct);
}

fn hasMaskedEmbeddingWeights(cb: *const ops.ComputeBackend, draft_cfg: gpt_mod.Config) bool {
    if (getenvBool("ANTFLY_GEMMA4_MTP_DISABLE_MASKED_EMBEDDING")) return false;
    if (!draft_cfg.mtp_use_ordered_embeddings or draft_cfg.mtp_num_centroids == 0 or draft_cfg.mtp_centroid_intermediate_top_k == 0) return false;
    const centroid_w = cb.getWeight("masked_embedding.centroids.weight") catch return false;
    cb.free(centroid_w);
    const ordering_w = cb.getWeight("masked_embedding.token_ordering") catch return false;
    cb.free(ordering_w);
    return true;
}

fn mtpPhaseNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
}

fn getenvUsize(comptime name: [*:0]const u8) ?usize {
    return platform.env.getenvUsize(name);
}

fn mtpStageTraceEnabled() bool {
    return getenvBool("ANTFLY_GEMMA4_MTP_STAGE_TRACE");
}

fn mtpAssistantReplayEnabled() bool {
    return getenvBool("ANTFLY_GEMMA4_MTP_ASSISTANT_REPLAY");
}

fn mtpStageTraceValueCount() usize {
    return @min(getenvUsize("ANTFLY_GEMMA4_MTP_STAGE_TRACE_VALUES") orelse 6, 32);
}

const StageStats = struct {
    len: usize,
    finite: usize,
    mean_abs: f64,
    rms: f64,
    l2: f64,
    max_abs: f32,
    min: f32,
    max: f32,
};

fn stageStatsForSlice(values: []const f32) StageStats {
    var finite: usize = 0;
    var sum_abs: f64 = 0.0;
    var sum_sq: f64 = 0.0;
    var max_abs: f32 = 0.0;
    var min_value: f32 = 0.0;
    var max_value: f32 = 0.0;
    for (values) |value| {
        if (!std.math.isFinite(value)) continue;
        if (finite == 0) {
            min_value = value;
            max_value = value;
        } else {
            min_value = @min(min_value, value);
            max_value = @max(max_value, value);
        }
        const abs_value = @abs(value);
        max_abs = @max(max_abs, abs_value);
        sum_abs += @as(f64, abs_value);
        const wide = @as(f64, value);
        sum_sq += wide * wide;
        finite += 1;
    }
    if (finite == 0) {
        return .{
            .len = values.len,
            .finite = 0,
            .mean_abs = 0.0,
            .rms = 0.0,
            .l2 = 0.0,
            .max_abs = 0.0,
            .min = 0.0,
            .max = 0.0,
        };
    }
    const denom = @as(f64, @floatFromInt(finite));
    return .{
        .len = values.len,
        .finite = finite,
        .mean_abs = sum_abs / denom,
        .rms = @sqrt(sum_sq / denom),
        .l2 = @sqrt(sum_sq),
        .max_abs = max_abs,
        .min = min_value,
        .max = max_value,
    };
}

fn printStageStats(stage: []const u8, values: []const f32) void {
    const stats = stageStatsForSlice(values);
    std.debug.print(
        "gemma4_mtp_stage: stage={s} len={d} finite={d} mean_abs={d:.6} rms={d:.6} l2={d:.6} max_abs={d:.6} min={d:.6} max={d:.6} first=",
        .{ stage, stats.len, stats.finite, stats.mean_abs, stats.rms, stats.l2, stats.max_abs, stats.min, stats.max },
    );
    const first_count = @min(values.len, mtpStageTraceValueCount());
    for (values[0..first_count], 0..) |value, idx| {
        if (idx > 0) std.debug.print(",", .{});
        std.debug.print("{d:.6}", .{value});
    }
    if (first_count == 0) std.debug.print("none", .{});
    std.debug.print("\n", .{});
}

fn traceHostStage(stage: []const u8, values: []const f32) void {
    if (!mtpStageTraceEnabled()) return;
    printStageStats(stage, values);
}

fn traceBackendStage(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, stage: []const u8, tensor: ops.CT) !void {
    if (!mtpStageTraceEnabled()) return;
    const values = try cb.toFloat32(tensor, allocator);
    defer allocator.free(values);
    printStageStats(stage, values);
}

fn getMtpWeight(cb: *const ops.ComputeBackend, name: []const u8) !ops.CT {
    return cb.getWeight(name) catch |err| {
        if (err != error.WeightNotFound) return err;
        var buf: [128]u8 = undefined;
        const prefixed = std.fmt.bufPrint(&buf, "mtp.{s}", .{name}) catch return error.NameTooLong;
        return cb.getWeight(prefixed) catch |prefixed_err| {
            if (prefixed_err != error.WeightNotFound) return prefixed_err;
            const nextn_prefixed = std.fmt.bufPrint(&buf, "nextn.{s}", .{name}) catch return error.NameTooLong;
            return cb.getWeight(nextn_prefixed);
        };
    };
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
    const kv_donor_mode = kvDonorModeFromEnv();
    const concat_order = concatOrderFromEnv();
    const needs_stage_trace = mtpStageTraceEnabled();
    try prepareDraftConfigForMtp(request.target_config, &draft_cfg, kv_donor_mode);
    if (needs_stage_trace) {
        std.debug.print(
            "gemma4_mtp_stage: event=draft_begin token={d} total_seq={d} query_seq={d} kv_seq={d} concat_order={s} kv_donor_mode={s} debug_top_k={d}\n",
            .{
                request.token_id,
                request.decode_context.total_sequence_len,
                request.decode_context.query_sequence_len,
                request.decode_context.kv_sequence_len,
                concat_order.name(),
                kv_donor_mode.name(),
                request.debug_top_k,
            },
        );
    }

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
    traceHostStage("target_embedding", target_embedding_host);
    traceHostStage("target_activation", request.activation);

    const concat_host = try allocator.alloc(f32, backbone_hidden * 2);
    defer allocator.free(concat_host);
    try fillConcatInput(concat_host, target_embedding_host, request.activation, concat_order);
    traceHostStage("concat_input", concat_host);
    const concat_shape = [_]i32{ 1, @intCast(backbone_hidden * 2) };
    const concat_ct = try request.draft_cb.fromFloat32Shape(concat_host, &concat_shape);
    defer request.draft_cb.free(concat_ct);

    const pre_w = try getMtpWeight(request.draft_cb, "pre_projection.weight");
    defer request.draft_cb.free(pre_w);
    const assistant_input = try request.draft_cb.linearNoBias(concat_ct, pre_w, 1, backbone_hidden * 2, draft_hidden);
    try traceBackendStage(request.draft_cb, allocator, "assistant_input", assistant_input);

    const assistant_hidden = if (mtpAssistantReplayEnabled()) blk: {
        const hidden_result = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0OverridesCudaReplay(
            request.draft_cb,
            allocator,
            draft_cfg,
            assistant_input,
            .{},
            1,
            request.decode_context.total_sequence_len,
            request.decode_context,
            null,
            "gpt.mtp_assistant_final_hidden",
        );
        if (hidden_result.total_rows != 1) {
            request.draft_cb.free(hidden_result.hidden);
            return error.InvalidTensorShape;
        }
        break :blk hidden_result.hidden;
    } else try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
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
    try traceBackendStage(request.draft_cb, allocator, "assistant_hidden", assistant_hidden);

    const post_w = try getMtpWeight(request.draft_cb, "post_projection.weight");
    defer request.draft_cb.free(post_w);
    const projected = try request.draft_cb.linearNoBias(assistant_hidden, post_w, 1, draft_hidden, backbone_hidden);
    defer request.draft_cb.free(projected);
    const projected_host = try request.draft_cb.toFloat32(projected, allocator);
    errdefer allocator.free(projected_host);
    if (projected_host.len != backbone_hidden) return error.InvalidTensorShape;
    traceHostStage("projected_activation", projected_host);

    const draft_lm_w = try gpt_arch.getEmbeddingWeight(request.draft_cb, draft_cfg);
    defer request.draft_cb.free(draft_lm_w);
    const needs_debug_logits = request.debug_top_k > 0 or needs_stage_trace;
    const has_masked_embedding = hasMaskedEmbeddingWeights(request.draft_cb, draft_cfg);
    var logits_host: ?[]f32 = null;
    errdefer if (logits_host) |logits| allocator.free(logits);
    var logit_source: DraftLogitSource = .host_argmax;
    const token = blk: {
        if (!has_masked_embedding and !needs_debug_logits) {
            if (try request.draft_cb.linearNoBiasArgmaxLastRow(
                assistant_hidden,
                draft_lm_w,
                1,
                draft_hidden,
                draft_cfg.vocab_size,
            )) |token_id| {
                logit_source = .linear_argmax;
                break :blk @as(usize, @intCast(token_id));
            }
        }
        const logits_ct = try request.draft_cb.linearNoBias(
            assistant_hidden,
            draft_lm_w,
            1,
            draft_hidden,
            draft_cfg.vocab_size,
        );
        defer request.draft_cb.free(logits_ct);
        if (needs_debug_logits) {
            logits_host = try request.draft_cb.toFloat32(logits_ct, allocator);
            if (logits_host.?.len < draft_cfg.vocab_size) return error.InvalidTensorShape;
            gpt_arch.applyFinalLogitSoftcapInPlace(draft_cfg, logits_host.?[0..draft_cfg.vocab_size]);
            traceHostStage("draft_logits", logits_host.?[0..draft_cfg.vocab_size]);
        }
        if (try maskedEmbeddingArgmax(request, draft_cfg, assistant_hidden, logits_ct)) |token_id| {
            logit_source = .masked_embedding;
            break :blk token_id;
        }
        if (logits_host) |logits| {
            logit_source = .host_argmax;
            break :blk activations.argmax(logits[0..draft_cfg.vocab_size]);
        }
        if (try request.draft_cb.argmaxLastRow(logits_ct, 1, @intCast(draft_cfg.vocab_size))) |token_id| {
            logit_source = .backend_argmax;
            break :blk @as(usize, @intCast(token_id));
        }
        const logits = try request.draft_cb.toFloat32(logits_ct, allocator);
        defer allocator.free(logits);
        gpt_arch.applyFinalLogitSoftcapInPlace(draft_cfg, logits[0..draft_cfg.vocab_size]);
        logit_source = .host_argmax;
        break :blk activations.argmax(logits[0..draft_cfg.vocab_size]);
    };
    return .{
        .token = token,
        .projected_activation = projected_host,
        .logits = logits_host,
        .logit_source = logit_source,
    };
}

pub fn draftTokenDevice(request: DraftDeviceRequest) !DraftDeviceResult {
    if (!deviceResidentDraftAllowed(request.debug_top_k)) return error.UnsupportedBackend;
    const allocator = request.allocator;
    var draft_cfg = request.draft_config;
    const backbone_hidden: usize = @intCast(draft_cfg.mtp_backbone_hidden_size);
    const draft_hidden: usize = @intCast(draft_cfg.hidden_size);
    if (!draft_cfg.gemma4_mtp_assistant or backbone_hidden == 0) return error.InvalidDraftModelForGeneration;
    if (request.target_config.hidden_size != backbone_hidden) return error.IncompatibleDraftModel;

    const kv_donor_mode = kvDonorModeFromEnv();
    const concat_order = concatOrderFromEnv();
    try prepareDraftConfigForMtp(request.target_config, &draft_cfg, kv_donor_mode);

    const draft_target_embedding = if (request.target_embedding_on_draft) |cached|
        cached
    else blk: {
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
        break :blk (try request.draft_cb.copyTensorFromBackend(request.target_cb, target_embedding)) orelse return error.UnsupportedBackend;
    };
    defer if (request.target_embedding_on_draft == null) request.draft_cb.free(draft_target_embedding);

    const concat_ct = switch (concat_order) {
        .embedding_activation => try request.draft_cb.concat(draft_target_embedding, request.activation, 1, backbone_hidden, backbone_hidden),
        .activation_embedding => try request.draft_cb.concat(request.activation, draft_target_embedding, 1, backbone_hidden, backbone_hidden),
    };
    defer request.draft_cb.free(concat_ct);

    const phase_trace = getenvBool("ANTFLY_GEMMA4_MTP_DRAFT_PHASE_TRACE");
    var phase_t0: u64 = if (phase_trace) mtpPhaseNowNs() else 0;
    const pre_w = try getMtpWeight(request.draft_cb, "pre_projection.weight");
    defer request.draft_cb.free(pre_w);
    const assistant_input = try request.draft_cb.linearNoBias(concat_ct, pre_w, 1, backbone_hidden * 2, draft_hidden);
    if (phase_trace) { const now = mtpPhaseNowNs(); std.debug.print("mtp_phase: pre_proj_us={d}\n", .{(now - phase_t0) / 1000}); phase_t0 = now; }
    var draft_frame_active = false;
    if (request.draft_cb.kind() == .metal and !request.draft_cb.decoderRuntimeHasActiveFrame() and !getenvBool("ANTFLY_GEMMA4_MTP_DISABLE_DRAFT_FRAME")) {
        draft_frame_active = request.draft_cb.decoderRuntimeBeginFrame() catch false;
    }
    errdefer if (draft_frame_active) {
        request.draft_cb.decoderRuntimeCancelFrame() catch {};
    };

    const assistant_hidden = if (mtpAssistantReplayEnabled()) blk: {
        const hidden_result = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0OverridesCudaReplay(
            request.draft_cb,
            allocator,
            draft_cfg,
            assistant_input,
            .{},
            1,
            request.decode_context.total_sequence_len,
            request.decode_context,
            null,
            "gpt.mtp_assistant_final_hidden",
        );
        if (hidden_result.total_rows != 1) {
            request.draft_cb.free(hidden_result.hidden);
            return error.InvalidTensorShape;
        }
        break :blk hidden_result.hidden;
    } else try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
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
    if (phase_trace) { const now = mtpPhaseNowNs(); std.debug.print("mtp_phase: assistant_forward_us={d}\n", .{(now - phase_t0) / 1000}); phase_t0 = now; }

    const post_w = try getMtpWeight(request.draft_cb, "post_projection.weight");
    defer request.draft_cb.free(post_w);
    const projected = try request.draft_cb.linearNoBias(assistant_hidden, post_w, 1, draft_hidden, backbone_hidden);
    errdefer request.draft_cb.free(projected);

    if (phase_trace) { const now = mtpPhaseNowNs(); std.debug.print("mtp_phase: post_proj_us={d}\n", .{(now - phase_t0) / 1000}); phase_t0 = now; }
    if (draft_frame_active) {
        draft_frame_active = false;
        try request.draft_cb.decoderRuntimeSubmitAndWaitFrame();
    }
    const draft_lm_w = try gpt_arch.getEmbeddingWeight(request.draft_cb, draft_cfg);
    defer request.draft_cb.free(draft_lm_w);
    const has_masked_embedding = hasMaskedEmbeddingWeights(request.draft_cb, draft_cfg);
    var logit_source: DraftLogitSource = .host_argmax;
    const token = blk: {
        if (!has_masked_embedding) {
            if (try request.draft_cb.linearNoBiasArgmaxLastRow(
                assistant_hidden,
                draft_lm_w,
                1,
                draft_hidden,
                draft_cfg.vocab_size,
            )) |token_id| {
                logit_source = .linear_argmax;
                break :blk @as(usize, @intCast(token_id));
            }
        }

        const logits_ct = try request.draft_cb.linearNoBias(
            assistant_hidden,
            draft_lm_w,
            1,
            draft_hidden,
            draft_cfg.vocab_size,
        );
        defer request.draft_cb.free(logits_ct);

        if (!getenvBool("ANTFLY_GEMMA4_MTP_DISABLE_MASKED_EMBEDDING")) {
            if (try maskedEmbeddingArgmaxForBackend(allocator, request.draft_cb, draft_cfg, assistant_hidden, logits_ct)) |token_id| {
                logit_source = .masked_embedding;
                break :blk token_id;
            }
        }
        if (try request.draft_cb.argmaxLastRow(logits_ct, 1, @intCast(draft_cfg.vocab_size))) |token_id| {
            logit_source = .backend_argmax;
            break :blk @as(usize, @intCast(token_id));
        }

        const logits = try request.draft_cb.toFloat32(logits_ct, allocator);
        defer allocator.free(logits);
        if (logits.len < draft_cfg.vocab_size) return error.InvalidTensorShape;
        gpt_arch.applyFinalLogitSoftcapInPlace(draft_cfg, logits[0..draft_cfg.vocab_size]);
        logit_source = .host_argmax;
        break :blk activations.argmax(logits[0..draft_cfg.vocab_size]);
    };

    if (phase_trace) { std.debug.print("mtp_phase: lm_argmax_us={d} source={s}\n", .{ (mtpPhaseNowNs() - phase_t0) / 1000, @tagName(logit_source) }); }
    return .{
        .token = token,
        .projected_activation = projected,
        .logits = null,
        .logit_source = logit_source,
    };
}

test "gemma4 mtp stage stats summarize finite values" {
    const values = [_]f32{ -3.0, 4.0, 0.0 };
    const stats = stageStatsForSlice(values[0..]);
    try std.testing.expectEqual(@as(usize, 3), stats.len);
    try std.testing.expectEqual(@as(usize, 3), stats.finite);
    try std.testing.expectApproxEqAbs(@as(f64, 7.0 / 3.0), stats.mean_abs, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), stats.l2, 0.000001);
    try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 25.0 / 3.0)), stats.rms, 0.000001);
    try std.testing.expectEqual(@as(f32, 4.0), stats.max_abs);
    try std.testing.expectEqual(@as(f32, -3.0), stats.min);
    try std.testing.expectEqual(@as(f32, 4.0), stats.max);
}

test "gemma4 mtp parses concat and kv donor modes" {
    try std.testing.expectEqual(ConcatOrder.embedding_activation, parseConcatOrder(null));
    try std.testing.expectEqual(ConcatOrder.embedding_activation, parseConcatOrder("embedding_activation"));
    try std.testing.expectEqual(ConcatOrder.activation_embedding, parseConcatOrder("activation_embedding"));
    try std.testing.expectEqual(ConcatOrder.embedding_activation, parseConcatOrder("unknown"));

    try std.testing.expectEqual(KvDonorMode.shared_type, parseKvDonorMode(null));
    try std.testing.expectEqual(KvDonorMode.shared_type, parseKvDonorMode("shared_type"));
    try std.testing.expectEqual(KvDonorMode.tail_base, parseKvDonorMode("tail_base"));
    try std.testing.expectEqual(KvDonorMode.assistant_index, parseKvDonorMode("assistant_index"));
    try std.testing.expectEqual(KvDonorMode.non_shared_tail_base, parseKvDonorMode("non_shared_tail_base"));
    try std.testing.expectEqual(KvDonorMode.first_shared_base, parseKvDonorMode("first_shared_base"));
    try std.testing.expectEqual(KvDonorMode.shared_type, parseKvDonorMode("unknown"));
}

test "gemma4 mtp fill concat input supports both orders" {
    const embedding = [_]f32{ 1.0, 2.0 };
    const activation = [_]f32{ 3.0, 4.0 };
    var dest = [_]f32{0.0} ** 4;

    try fillConcatInput(dest[0..], embedding[0..], activation[0..], .embedding_activation);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0, 4.0 }, dest[0..]);

    try fillConcatInput(dest[0..], embedding[0..], activation[0..], .activation_embedding);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0, 1.0, 2.0 }, dest[0..]);
}

test "gemma4 mtp tail-base donor mode maps assistant to target tail" {
    const target = gpt_mod.Config{
        .family = .gemma,
        .num_hidden_layers = 35,
        .num_kv_shared_layers = 20,
    };
    const draft = gpt_mod.Config{
        .family = .gemma,
        .gemma4_mtp_assistant = true,
        .num_hidden_layers = 4,
        .mtp_num_centroids = 0,
    };

    try std.testing.expectEqual(@as(?u32, null), try mtpKvDonorBaseLayer(target, draft, .shared_type));
    try std.testing.expectEqual(@as(?u32, 31), try mtpKvDonorBaseLayer(target, draft, .tail_base));
    try std.testing.expectEqual(@as(?u32, 0), try mtpKvDonorBaseLayer(target, draft, .assistant_index));
    try std.testing.expectEqual(@as(?u32, 11), try mtpKvDonorBaseLayer(target, draft, .non_shared_tail_base));
    try std.testing.expectEqual(@as(?u32, 15), try mtpKvDonorBaseLayer(target, draft, .first_shared_base));
}
