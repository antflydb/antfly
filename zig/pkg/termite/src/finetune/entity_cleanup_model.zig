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
const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const cleanup_data = @import("entity_cleanup_data.zig");
const cleanup_pipeline = @import("../pipelines/entity_cleanup.zig");

pub const artifact_family_version = "entity_cleanup_head/v1alpha1";
pub const cache_family_version = "entity_cleanup_cache/v1alpha1";
pub const head_file_name = "entity_cleanup_head.json";

pub const FeatureConfig = struct {
    feature_dim: usize = 128,
    context_window: usize = 24,
};

pub const CachedMentionSummary = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    detect_score: f32,
    features: []f32,
    keep: bool,
    preferred_surface: bool,
    group_id: ?[]const u8 = null,
};

pub const CachedSummary = struct {
    artifact_family_version: []const u8,
    input_path: []const u8,
    split: ?[]const u8 = null,
    feature_dim: usize,
    context_window: usize,
    stats: cleanup_data.Stats,
    mentions: []CachedMentionSummary,
};

const CachedSummaryFile = struct {
    summary: CachedSummary,
};

pub const CleanupHead = struct {
    allocator: std.mem.Allocator,
    feature_dim: usize,
    embedding_dim: usize,
    context_window: usize,
    validity_weight: []f32,
    validity_bias: f32,
    representative_weight: []f32,
    representative_bias: f32,
    embedding_proj: []f32,
    min_validity_score: f32 = 0.5,
    dedup_similarity_threshold: f32 = 0.9,

    pub fn init(allocator: std.mem.Allocator, feature_dim: usize, embedding_dim: usize, context_window: usize) !CleanupHead {
        const validity_weight = try allocator.alloc(f32, feature_dim);
        errdefer allocator.free(validity_weight);
        const representative_weight = try allocator.alloc(f32, feature_dim);
        errdefer allocator.free(representative_weight);
        const embedding_proj = try allocator.alloc(f32, feature_dim * embedding_dim);
        errdefer allocator.free(embedding_proj);

        @memset(validity_weight, 0);
        @memset(representative_weight, 0);
        @memset(embedding_proj, 0);
        for (0..embedding_dim) |row| {
            if (row < feature_dim) embedding_proj[row * feature_dim + row] = 1.0;
        }

        return .{
            .allocator = allocator,
            .feature_dim = feature_dim,
            .embedding_dim = embedding_dim,
            .context_window = context_window,
            .validity_weight = validity_weight,
            .validity_bias = 0,
            .representative_weight = representative_weight,
            .representative_bias = 0,
            .embedding_proj = embedding_proj,
        };
    }

    pub fn deinit(self: *CleanupHead) void {
        self.allocator.free(self.validity_weight);
        self.allocator.free(self.representative_weight);
        self.allocator.free(self.embedding_proj);
        self.* = undefined;
    }
};

pub const TrainOptions = struct {
    epochs: usize = 3,
    learning_rate: f32 = 0.05,
    embedding_learning_rate: f32 = 0.01,
    max_mentions: usize = 0,
    embedding_dim: usize = 32,
};

pub const EvalSummary = struct {
    mentions_seen: usize = 0,
    validity_accuracy: f64 = 0,
    preferred_accuracy: f64 = 0,
    pair_accuracy: f64 = 0,
    cluster_precision: f64 = 0,
    cluster_recall: f64 = 0,
    cluster_f1: f64 = 0,
    gold_clusters: usize = 0,
    predicted_clusters: usize = 0,
    overmerged_clusters: usize = 0,
    oversplit_clusters: usize = 0,
};

pub const TrainEvalSummary = struct {
    artifact_family_version: []const u8,
    train_mentions: usize,
    eval_mentions: usize,
    feature_dim: usize,
    embedding_dim: usize,
    context_window: usize,
    epochs: usize,
    learning_rate: f32,
    embedding_learning_rate: f32,
    eval: EvalSummary,
};

pub fn prepareCachedSummary(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    split: ?[]const u8,
    examples: []const cleanup_data.Example,
    feature_cfg: FeatureConfig,
) !CachedSummary {
    var mentions = std.ArrayListUnmanaged(CachedMentionSummary).empty;
    errdefer {
        for (mentions.items) |*mention| freeCachedMentionSummary(allocator, mention);
        mentions.deinit(allocator);
    }

    for (examples) |example| {
        for (example.mentions) |mention| {
            if (mention.end > example.text.len or mention.start >= mention.end) return error.InvalidCleanupMentionSpan;
            const entity = cleanup_pipeline.Entity{
                .text = example.text[mention.start..mention.end],
                .label = mention.label,
                .start = mention.start,
                .end = mention.end,
                .score = 1.0,
            };
            const features = try cleanup_pipeline.buildFeatureVector(allocator, example.text, entity, .{
                .feature_dim = feature_cfg.feature_dim,
                .context_window = feature_cfg.context_window,
            });
            try mentions.append(allocator, .{
                .text = try allocator.dupe(u8, entity.text),
                .label = try allocator.dupe(u8, entity.label),
                .start = entity.start,
                .end = entity.end,
                .detect_score = entity.score,
                .features = features,
                .keep = mention.keep,
                .preferred_surface = mention.preferred_surface,
                .group_id = if (mention.group_id) |gid| try allocator.dupe(u8, gid) else null,
            });
        }
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, cache_family_version),
        .input_path = try allocator.dupe(u8, input_path),
        .split = if (split) |value| try allocator.dupe(u8, value) else null,
        .feature_dim = feature_cfg.feature_dim,
        .context_window = feature_cfg.context_window,
        .stats = cleanup_data.computeStats(examples),
        .mentions = try mentions.toOwnedSlice(allocator),
    };
}

pub fn saveCachedSummary(allocator: std.mem.Allocator, path: []const u8, summary: CachedSummary) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .summary = summary }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

pub fn loadCachedSummary(allocator: std.mem.Allocator, path: []const u8) !CachedSummary {
    const raw = try c_file.readFileMax(allocator, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(CachedSummaryFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return try cloneCachedSummary(allocator, &parsed.summary);
}

pub fn freeCachedSummary(allocator: std.mem.Allocator, summary: *CachedSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.input_path);
    if (summary.split) |split| allocator.free(split);
    for (summary.mentions) |*mention| freeCachedMentionSummary(allocator, mention);
    allocator.free(summary.mentions);
    summary.* = undefined;
}

pub fn saveHead(allocator: std.mem.Allocator, head: *const CleanupHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const path = try std.fs.path.join(allocator, &.{ out_dir, head_file_name });
    defer allocator.free(path);

    var file = try compat.cwd().createFile(compat.io(), path, .{ .truncate = true });
    defer file.close(compat.io());
    var buf: [8192]u8 = undefined;
    var writer = file.writerStreaming(compat.io(), &buf);
    try std.json.Stringify.value(.{
        .artifact_family_version = artifact_family_version,
        .feature_dim = head.feature_dim,
        .embedding_dim = head.embedding_dim,
        .context_window = head.context_window,
        .min_validity_score = head.min_validity_score,
        .dedup_similarity_threshold = head.dedup_similarity_threshold,
        .validity_weight = head.validity_weight,
        .validity_bias = head.validity_bias,
        .representative_weight = head.representative_weight,
        .representative_bias = head.representative_bias,
        .embedding_proj = head.embedding_proj,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

pub fn loadHeadIfPresent(allocator: std.mem.Allocator, model_dir: []const u8) !?CleanupHead {
    const path = try std.fs.path.join(allocator, &.{ model_dir, head_file_name });
    defer allocator.free(path);
    _ = compat.cwd().statFile(compat.io(), path, .{}) catch return null;
    return try loadHead(allocator, path);
}

pub fn loadHead(allocator: std.mem.Allocator, path: []const u8) !CleanupHead {
    const raw = try c_file.readFileMax(allocator, path, 64 * 1024 * 1024);
    defer allocator.free(raw);

    const Parsed = struct {
        feature_dim: usize,
        embedding_dim: usize,
        context_window: usize,
        min_validity_score: ?f32 = null,
        dedup_similarity_threshold: ?f32 = null,
        validity_weight: []const f32,
        validity_bias: f32,
        representative_weight: []const f32,
        representative_bias: f32,
        embedding_proj: []const f32,
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(Parsed, arena.allocator(), raw, .{ .ignore_unknown_fields = true });

    const expected_embedding_proj_len = try std.math.mul(usize, parsed.feature_dim, parsed.embedding_dim);
    if (parsed.validity_weight.len != parsed.feature_dim) return error.InvalidCleanupHeadArtifact;
    if (parsed.representative_weight.len != parsed.feature_dim) return error.InvalidCleanupHeadArtifact;
    if (parsed.embedding_proj.len != expected_embedding_proj_len) return error.InvalidCleanupHeadArtifact;

    var head = try CleanupHead.init(allocator, parsed.feature_dim, parsed.embedding_dim, parsed.context_window);
    @memcpy(head.validity_weight, parsed.validity_weight);
    head.validity_bias = parsed.validity_bias;
    @memcpy(head.representative_weight, parsed.representative_weight);
    head.representative_bias = parsed.representative_bias;
    @memcpy(head.embedding_proj, parsed.embedding_proj);
    head.min_validity_score = parsed.min_validity_score orelse 0.5;
    head.dedup_similarity_threshold = parsed.dedup_similarity_threshold orelse 0.9;
    return head;
}

pub fn scoreEntities(
    allocator: std.mem.Allocator,
    head: *const CleanupHead,
    source_text: []const u8,
    entities: []const cleanup_pipeline.Entity,
) ![]cleanup_pipeline.CleanupMention {
    const out = try allocator.alloc(cleanup_pipeline.CleanupMention, entities.len);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |*mention| mention.deinit(allocator);
        allocator.free(out);
    }

    for (entities, 0..) |entity, idx| {
        const features = try cleanup_pipeline.buildFeatureVector(allocator, source_text, entity, .{
            .feature_dim = head.feature_dim,
            .context_window = head.context_window,
        });
        defer allocator.free(features);

        out[idx] = .{
            .entity = .{
                .text = try allocator.dupe(u8, entity.text),
                .label = try allocator.dupe(u8, entity.label),
                .start = entity.start,
                .end = entity.end,
                .score = entity.score,
            },
            .validity_score = sigmoid(dot(head.validity_weight, features) + head.validity_bias),
            .representative_score = sigmoid(dot(head.representative_weight, features) + head.representative_bias),
            .embedding = try projectEmbedding(allocator, head, features),
        };
        built += 1;
    }

    return out;
}

pub fn trainEvalCached(
    allocator: std.mem.Allocator,
    train_summary: *const CachedSummary,
    eval_summary: *const CachedSummary,
    out_dir: []const u8,
    options: TrainOptions,
) !TrainEvalSummary {
    if (train_summary.split != null and eval_summary.split != null and
        std.mem.eql(u8, train_summary.split.?, eval_summary.split.?))
    {
        return error.InvalidCleanupSplit;
    }

    var head = try CleanupHead.init(
        allocator,
        train_summary.feature_dim,
        options.embedding_dim,
        train_summary.context_window,
    );
    defer head.deinit();

    const train_mentions = effectiveMentions(train_summary.mentions, options.max_mentions);
    const eval_mentions = effectiveMentions(eval_summary.mentions, options.max_mentions);

    for (0..options.epochs) |_| {
        trainValidityHead(&head, train_mentions, options.learning_rate);
        trainRepresentativeHead(&head, train_mentions, options.learning_rate);
        trainEmbeddingProjection(&head, train_mentions, options.embedding_learning_rate);
    }

    try saveHead(allocator, &head, out_dir);

    return .{
        .artifact_family_version = artifact_family_version,
        .train_mentions = train_mentions.len,
        .eval_mentions = eval_mentions.len,
        .feature_dim = head.feature_dim,
        .embedding_dim = head.embedding_dim,
        .context_window = head.context_window,
        .epochs = options.epochs,
        .learning_rate = options.learning_rate,
        .embedding_learning_rate = options.embedding_learning_rate,
        .eval = evaluateHead(&head, eval_mentions),
    };
}

fn effectiveMentions(mentions: []const CachedMentionSummary, max_mentions: usize) []const CachedMentionSummary {
    if (max_mentions == 0) return mentions;
    return mentions[0..@min(mentions.len, max_mentions)];
}

fn trainValidityHead(head: *CleanupHead, mentions: []const CachedMentionSummary, learning_rate: f32) void {
    for (mentions) |mention| {
        const target: f32 = if (mention.keep) 1.0 else 0.0;
        const pred = sigmoid(dot(head.validity_weight, mention.features) + head.validity_bias);
        const err = pred - target;
        for (head.validity_weight, mention.features) |*weight, feature| {
            weight.* -= learning_rate * err * feature;
        }
        head.validity_bias -= learning_rate * err;
    }
}

fn trainRepresentativeHead(head: *CleanupHead, mentions: []const CachedMentionSummary, learning_rate: f32) void {
    for (mentions) |mention| {
        if (!mention.keep) continue;
        const target: f32 = if (mention.preferred_surface) 1.0 else 0.0;
        const pred = sigmoid(dot(head.representative_weight, mention.features) + head.representative_bias);
        const err = pred - target;
        for (head.representative_weight, mention.features) |*weight, feature| {
            weight.* -= learning_rate * err * feature;
        }
        head.representative_bias -= learning_rate * err;
    }
}

fn trainEmbeddingProjection(head: *CleanupHead, mentions: []const CachedMentionSummary, learning_rate: f32) void {
    const kept = countKeptGroupedMentions(mentions);
    if (kept < 2) return;

    var i: usize = 0;
    while (i < mentions.len) : (i += 1) {
        const a = mentions[i];
        if (!a.keep or a.group_id == null) continue;

        var j: usize = i + 1;
        while (j < mentions.len) : (j += 1) {
            const b = mentions[j];
            if (!b.keep or b.group_id == null) continue;
            const target: f32 = if (std.mem.eql(u8, a.group_id.?, b.group_id.?)) 1.0 else 0.0;
            trainEmbeddingPair(head, a.features, b.features, target, learning_rate);
        }
    }
}

fn trainEmbeddingPair(head: *CleanupHead, a_features: []const f32, b_features: []const f32, target: f32, learning_rate: f32) void {
    const emb_a = std.heap.page_allocator.alloc(f32, head.embedding_dim) catch return;
    defer std.heap.page_allocator.free(emb_a);
    const emb_b = std.heap.page_allocator.alloc(f32, head.embedding_dim) catch return;
    defer std.heap.page_allocator.free(emb_b);

    projectEmbeddingInto(head, a_features, emb_a);
    projectEmbeddingInto(head, b_features, emb_b);

    const raw_score = dot(emb_a, emb_b);
    const pred = sigmoid(raw_score);
    const err = pred - target;

    for (0..head.embedding_dim) |row| {
        const row_offset = row * head.feature_dim;
        const ej = emb_b[row];
        const ei = emb_a[row];
        for (0..head.feature_dim) |col| {
            const grad = err * (ej * a_features[col] + ei * b_features[col]);
            head.embedding_proj[row_offset + col] -= learning_rate * grad;
        }
    }
}

fn evaluateHead(head: *const CleanupHead, mentions: []const CachedMentionSummary) EvalSummary {
    var out = EvalSummary{ .mentions_seen = mentions.len };
    if (mentions.len == 0) return out;

    var valid_correct: usize = 0;
    var preferred_total: usize = 0;
    var preferred_correct: usize = 0;
    var pair_total: usize = 0;
    var pair_correct: usize = 0;

    for (mentions) |mention| {
        const pred_keep = sigmoid(dot(head.validity_weight, mention.features) + head.validity_bias) >= 0.5;
        if (pred_keep == mention.keep) valid_correct += 1;

        if (mention.keep) {
            preferred_total += 1;
            const pred_pref = sigmoid(dot(head.representative_weight, mention.features) + head.representative_bias) >= 0.5;
            if (pred_pref == mention.preferred_surface) preferred_correct += 1;
        }
    }

    var i: usize = 0;
    while (i < mentions.len) : (i += 1) {
        const a = mentions[i];
        if (!a.keep or a.group_id == null) continue;
        const emb_a = projectEmbedding(std.heap.page_allocator, head, a.features) catch break;
        defer std.heap.page_allocator.free(emb_a);

        var j: usize = i + 1;
        while (j < mentions.len) : (j += 1) {
            const b = mentions[j];
            if (!b.keep or b.group_id == null) continue;
            const emb_b = projectEmbedding(std.heap.page_allocator, head, b.features) catch break;
            defer std.heap.page_allocator.free(emb_b);
            const pred_same = dot(emb_a, emb_b) >= head.dedup_similarity_threshold;
            const target_same = std.mem.eql(u8, a.group_id.?, b.group_id.?);
            pair_total += 1;
            if (pred_same == target_same) pair_correct += 1;
        }
    }

    out.validity_accuracy = @as(f64, @floatFromInt(valid_correct)) / @as(f64, @floatFromInt(mentions.len));
    out.preferred_accuracy = if (preferred_total > 0)
        @as(f64, @floatFromInt(preferred_correct)) / @as(f64, @floatFromInt(preferred_total))
    else
        0;
    out.pair_accuracy = if (pair_total > 0)
        @as(f64, @floatFromInt(pair_correct)) / @as(f64, @floatFromInt(pair_total))
    else
        0;
    populateClusterMetrics(head, mentions, &out);
    return out;
}

fn populateClusterMetrics(head: *const CleanupHead, mentions: []const CachedMentionSummary, out: *EvalSummary) void {
    const allocator = std.heap.page_allocator;
    const eligible = countKeptGroupedMentions(mentions);
    if (eligible == 0) return;

    const parents = allocator.alloc(usize, mentions.len) catch return;
    defer allocator.free(parents);
    for (parents, 0..) |*parent, idx| parent.* = idx;

    var i: usize = 0;
    while (i < mentions.len) : (i += 1) {
        const a = mentions[i];
        if (!a.keep or a.group_id == null) continue;
        const emb_a = projectEmbedding(allocator, head, a.features) catch return;
        defer allocator.free(emb_a);

        var j: usize = i + 1;
        while (j < mentions.len) : (j += 1) {
            const b = mentions[j];
            if (!b.keep or b.group_id == null) continue;
            if (!std.mem.eql(u8, a.label, b.label)) continue;
            const emb_b = projectEmbedding(allocator, head, b.features) catch return;
            defer allocator.free(emb_b);
            if (dot(emb_a, emb_b) >= head.dedup_similarity_threshold) {
                unionCluster(parents, i, j);
            }
        }
    }

    const predicted_roots = allocator.alloc(usize, eligible) catch return;
    defer allocator.free(predicted_roots);
    const gold_group_indices = allocator.alloc(usize, eligible) catch return;
    defer allocator.free(gold_group_indices);

    var predicted_count: usize = 0;
    var gold_count: usize = 0;
    for (mentions, 0..) |mention, idx| {
        if (!mention.keep or mention.group_id == null) continue;
        const root = findClusterRoot(parents, idx);
        if (!containsUsize(predicted_roots[0..predicted_count], root)) {
            predicted_roots[predicted_count] = root;
            predicted_count += 1;
        }
        const gold_idx = findOrAppendGoldGroup(mentions, gold_group_indices[0..gold_count], mention.group_id.?);
        if (gold_idx == gold_count) {
            gold_group_indices[gold_count] = idx;
            gold_count += 1;
        }
    }

    var pure_predicted: usize = 0;
    var overmerged: usize = 0;
    for (predicted_roots[0..predicted_count]) |root| {
        var seen_gold = std.StringHashMapUnmanaged(void).empty;
        defer seen_gold.deinit(allocator);
        for (mentions, 0..) |mention, idx| {
            if (!mention.keep or mention.group_id == null) continue;
            if (findClusterRoot(parents, idx) != root) continue;
            seen_gold.put(allocator, mention.group_id.?, {}) catch return;
        }
        const seen_count = seen_gold.count();
        if (seen_count == 1) pure_predicted += 1;
        if (seen_count > 1) overmerged += 1;
    }

    var complete_gold: usize = 0;
    var oversplit: usize = 0;
    for (gold_group_indices[0..gold_count]) |representative_idx| {
        const group_id = mentions[representative_idx].group_id.?;
        var seen_roots = std.AutoHashMapUnmanaged(usize, void).empty;
        defer seen_roots.deinit(allocator);
        var first_root: ?usize = null;
        for (mentions, 0..) |mention, idx| {
            if (!mention.keep or mention.group_id == null) continue;
            if (!std.mem.eql(u8, mention.group_id.?, group_id)) continue;
            const root = findClusterRoot(parents, idx);
            if (first_root == null) first_root = root;
            seen_roots.put(allocator, root, {}) catch return;
        }
        const seen_count = seen_roots.count();
        if (seen_count == 1 and predictedClusterIsPureForGroup(parents, mentions, first_root.?, group_id)) {
            complete_gold += 1;
        }
        if (seen_count > 1) oversplit += 1;
    }

    out.gold_clusters = gold_count;
    out.predicted_clusters = predicted_count;
    out.overmerged_clusters = overmerged;
    out.oversplit_clusters = oversplit;
    out.cluster_precision = if (predicted_count > 0)
        @as(f64, @floatFromInt(pure_predicted)) / @as(f64, @floatFromInt(predicted_count))
    else
        0;
    out.cluster_recall = if (gold_count > 0)
        @as(f64, @floatFromInt(complete_gold)) / @as(f64, @floatFromInt(gold_count))
    else
        0;
    out.cluster_f1 = if (out.cluster_precision + out.cluster_recall > 0)
        2.0 * out.cluster_precision * out.cluster_recall / (out.cluster_precision + out.cluster_recall)
    else
        0;
}

fn findClusterRoot(parents: []usize, idx: usize) usize {
    var root = idx;
    while (parents[root] != root) root = parents[root];
    var cursor = idx;
    while (parents[cursor] != cursor) {
        const next = parents[cursor];
        parents[cursor] = root;
        cursor = next;
    }
    return root;
}

fn unionCluster(parents: []usize, lhs: usize, rhs: usize) void {
    const lhs_root = findClusterRoot(parents, lhs);
    const rhs_root = findClusterRoot(parents, rhs);
    if (lhs_root != rhs_root) parents[rhs_root] = lhs_root;
}

fn containsUsize(values: []const usize, needle: usize) bool {
    for (values) |value| {
        if (value == needle) return true;
    }
    return false;
}

fn findOrAppendGoldGroup(mentions: []const CachedMentionSummary, representatives: []const usize, group_id: []const u8) usize {
    for (representatives, 0..) |idx, pos| {
        if (std.mem.eql(u8, mentions[idx].group_id.?, group_id)) return pos;
    }
    return representatives.len;
}

fn predictedClusterIsPureForGroup(parents: []usize, mentions: []const CachedMentionSummary, root: usize, group_id: []const u8) bool {
    for (mentions, 0..) |mention, idx| {
        if (!mention.keep or mention.group_id == null) continue;
        if (findClusterRoot(parents, idx) != root) continue;
        if (!std.mem.eql(u8, mention.group_id.?, group_id)) return false;
    }
    return true;
}

fn countKeptGroupedMentions(mentions: []const CachedMentionSummary) usize {
    var count: usize = 0;
    for (mentions) |mention| {
        if (mention.keep and mention.group_id != null) count += 1;
    }
    return count;
}

fn projectEmbedding(allocator: std.mem.Allocator, head: *const CleanupHead, features: []const f32) ![]f32 {
    const out = try allocator.alloc(f32, head.embedding_dim);
    projectEmbeddingInto(head, features, out);
    normalizeVector(out);
    return out;
}

fn projectEmbeddingInto(head: *const CleanupHead, features: []const f32, out: []f32) void {
    @memset(out, 0);
    for (0..head.embedding_dim) |row| {
        const row_slice = head.embedding_proj[row * head.feature_dim .. (row + 1) * head.feature_dim];
        out[row] = dot(row_slice, features);
    }
}

fn normalizeVector(values: []f32) void {
    var norm: f32 = 0;
    for (values) |value| norm += value * value;
    if (norm <= 0) return;
    const inv = 1.0 / @sqrt(norm);
    for (values) |*value| value.* *= inv;
}

fn dot(a: []const f32, b: []const f32) f32 {
    var sum: f32 = 0;
    for (a, b) |av, bv| sum += av * bv;
    return sum;
}

fn sigmoid(x: f32) f32 {
    if (x >= 0) {
        const z = @exp(-x);
        return 1.0 / (1.0 + z);
    }
    const z = @exp(x);
    return z / (1.0 + z);
}

fn freeCachedMentionSummary(allocator: std.mem.Allocator, mention: *CachedMentionSummary) void {
    allocator.free(mention.text);
    allocator.free(mention.label);
    allocator.free(mention.features);
    if (mention.group_id) |group_id| allocator.free(group_id);
    mention.* = undefined;
}

fn cloneCachedSummary(allocator: std.mem.Allocator, summary: *const CachedSummary) !CachedSummary {
    const mentions = try allocator.alloc(CachedMentionSummary, summary.mentions.len);
    var built: usize = 0;
    errdefer {
        for (mentions[0..built]) |*mention| freeCachedMentionSummary(allocator, mention);
        allocator.free(mentions);
    }

    for (summary.mentions, 0..) |mention, idx| {
        mentions[idx] = .{
            .text = try allocator.dupe(u8, mention.text),
            .label = try allocator.dupe(u8, mention.label),
            .start = mention.start,
            .end = mention.end,
            .detect_score = mention.detect_score,
            .features = try allocator.dupe(f32, mention.features),
            .keep = mention.keep,
            .preferred_surface = mention.preferred_surface,
            .group_id = if (mention.group_id) |gid| try allocator.dupe(u8, gid) else null,
        };
        built += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, summary.artifact_family_version),
        .input_path = try allocator.dupe(u8, summary.input_path),
        .split = if (summary.split) |value| try allocator.dupe(u8, value) else null,
        .feature_dim = summary.feature_dim,
        .context_window = summary.context_window,
        .stats = summary.stats,
        .mentions = mentions,
    };
}

test "entity cleanup head learns simple cleanup task" {
    const allocator = std.testing.allocator;

    var train = CachedSummary{
        .artifact_family_version = try allocator.dupe(u8, cache_family_version),
        .input_path = try allocator.dupe(u8, "train"),
        .feature_dim = 8,
        .context_window = 8,
        .stats = .{},
        .mentions = try allocator.alloc(CachedMentionSummary, 3),
    };
    defer freeCachedSummary(allocator, &train);
    train.mentions[0] = .{
        .text = try allocator.dupe(u8, "T1mKaye"),
        .label = try allocator.dupe(u8, "person"),
        .start = 0,
        .end = 8,
        .detect_score = 1,
        .features = try allocator.dupe(f32, &[_]f32{ 1, 0, 0, 0, 0, 0, 0, 0 }),
        .keep = false,
        .preferred_surface = false,
    };
    train.mentions[1] = .{
        .text = try allocator.dupe(u8, "Tim Kaye"),
        .label = try allocator.dupe(u8, "person"),
        .start = 10,
        .end = 18,
        .detect_score = 1,
        .features = try allocator.dupe(f32, &[_]f32{ 0, 1, 0, 0, 0, 0, 0, 0 }),
        .keep = true,
        .preferred_surface = true,
        .group_id = try allocator.dupe(u8, "g1"),
    };
    train.mentions[2] = .{
        .text = try allocator.dupe(u8, "TIM KAYE"),
        .label = try allocator.dupe(u8, "person"),
        .start = 20,
        .end = 28,
        .detect_score = 1,
        .features = try allocator.dupe(f32, &[_]f32{ 0, 0.9, 0.1, 0, 0, 0, 0, 0 }),
        .keep = true,
        .preferred_surface = false,
        .group_id = try allocator.dupe(u8, "g1"),
    };

    var eval = try cloneCachedSummary(allocator, &train);
    defer freeCachedSummary(allocator, &eval);

    const out_dir = "/tmp/entity-cleanup-head-test";
    const summary = try trainEvalCached(allocator, &train, &eval, out_dir, .{
        .epochs = 20,
        .learning_rate = 0.1,
        .embedding_learning_rate = 0.05,
        .embedding_dim = 4,
    });
    try std.testing.expect(summary.eval.validity_accuracy > 0.6);
    try std.testing.expect(summary.eval.preferred_accuracy > 0.5);
    try std.testing.expect(summary.eval.pair_accuracy > 0.5);
    try std.testing.expectEqual(@as(usize, 1), summary.eval.gold_clusters);
    try std.testing.expect(summary.eval.cluster_f1 > 0.5);
}

test "entity cleanup trainer rejects identical explicit train eval split" {
    const allocator = std.testing.allocator;

    var train = CachedSummary{
        .artifact_family_version = try allocator.dupe(u8, cache_family_version),
        .input_path = try allocator.dupe(u8, "train"),
        .split = try allocator.dupe(u8, "train"),
        .feature_dim = 2,
        .context_window = 0,
        .stats = .{},
        .mentions = try allocator.alloc(CachedMentionSummary, 0),
    };
    defer freeCachedSummary(allocator, &train);

    var eval = try cloneCachedSummary(allocator, &train);
    defer freeCachedSummary(allocator, &eval);

    try std.testing.expectError(error.InvalidCleanupSplit, trainEvalCached(allocator, &train, &eval, "/tmp/entity-cleanup-invalid-split", .{}));
}

test "entity cleanup head rejects malformed artifact lengths" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "entity_cleanup_head.json",
        .data =
        \\{
        \\  "feature_dim": 4,
        \\  "embedding_dim": 2,
        \\  "context_window": 8,
        \\  "validity_weight": [1, 2, 3],
        \\  "validity_bias": 0.0,
        \\  "representative_weight": [1, 2, 3, 4],
        \\  "representative_bias": 0.0,
        \\  "embedding_proj": [0, 0, 0, 0, 0, 0, 0, 0]
        \\}
        ,
    });

    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "entity_cleanup_head.json" });
    defer allocator.free(path);

    try std.testing.expectError(error.InvalidCleanupHeadArtifact, loadHead(allocator, path));
}
