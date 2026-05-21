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
const build_options = @import("build_options");
const hf_tokenizer = @import("termite_hf_tokenizer");
const tokenizer_mod = @import("termite_tokenizer");

const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const backends = @import("../backends/backends.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const manifest_mod = @import("../models/manifest.zig");
const tensor_access = @import("../models/tensor_access.zig");
const weight_source = @import("../models/weight_source.zig");
const session_factory = @import("../architectures/session_factory.zig");
const bert_arch = @import("../architectures/bert.zig");
const deberta_arch = @import("../architectures/deberta.zig");
const graph_bridge = @import("graph_bridge.zig");
const reranker = @import("reranker.zig");
const reranker_data = @import("reranker_data.zig");
const text_encoder_boundary = @import("text_encoder_boundary.zig");
const optimizers = @import("ml").graph.optimizers;

pub const head_checkpoint_file_name = "legacy_reranker_head.safetensors";
pub const head_config_file_name = "legacy_reranker_head_config.json";
pub const merged_head_checkpoint_file_name = "model.safetensors";
pub const pooled_cache_file_name = "reranker_pooled_cache.json";
pub const pooled_cache_family_version = "reranker_pooled_cache/v1alpha1";
pub const top_layer_cache_file_name = "reranker_top_layer_cache.json";
pub const top_layer_cache_family_version = "reranker_top_layer_cache/v1alpha1";

pub const BackendChoice = reranker.BackendChoice;
pub const parseBackendChoice = reranker.parseBackendChoice;
pub const EvalSummary = reranker.EvalSummary;

pub const RerankerHead = struct {
    allocator: std.mem.Allocator,
    hidden_size: usize,
    weight: []f32,
    bias: f32,

    pub fn deinit(self: *RerankerHead) void {
        self.allocator.free(self.weight);
        self.* = undefined;
    }
};

pub const TrainEpochOptions = struct {
    learning_rate: f32 = 0.001,
    max_examples: usize = 256,
    use_schedule_free: bool = false,
};

pub const TrainEpochSummary = struct {
    examples_seen: usize = 0,
    average_loss: f64 = 0,
};

pub const TrainEvalSummary = struct {
    train: TrainEpochSummary,
    eval: EvalSummary,
    output_dir: []const u8,
};

pub const CachedPooledExample = struct {
    pooled: []f32,
    score: f32,

    pub fn deinit(self: *CachedPooledExample, allocator: std.mem.Allocator) void {
        allocator.free(self.pooled);
        self.* = undefined;
    }
};

pub const CachedPooledExampleSummary = struct {
    query: []const u8,
    document: []const u8,
    pooled: []f32,
    score: f32,
};

pub const CachedPooledSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8 = null,
    requested_backend: []const u8,
    max_examples: usize,
    hidden_size: usize,
    dataset_stats: reranker_data.DatasetStats,
    pairwise_training_pairs: usize,
    examples: []CachedPooledExampleSummary,
};

const CachedPooledSummaryFile = struct {
    summary: CachedPooledSummary,
};

pub const CachedTopLayerExampleSummary = struct {
    query: []const u8,
    document: []const u8,
    hidden_in: []f32,
    attention_mask: []i64,
    token_type_ids: ?[]i64 = null,
    score: f32,
    seq_len: usize,
};

pub const CachedTopLayerSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8 = null,
    requested_backend: []const u8,
    top_layer_count: usize,
    hidden_size: usize,
    dataset_stats: reranker_data.DatasetStats,
    pairwise_training_pairs: usize,
    examples: []CachedTopLayerExampleSummary,
};

const CachedTopLayerSummaryFile = struct {
    summary: CachedTopLayerSummary,
};

const ExamplesAndCached = struct {
    examples: []reranker_data.Example,
    cached: []CachedPooledExample,
};

pub const TopLayerBoundary = struct {
    hidden_in: []f32,
    attention_mask: []i64,
    token_type_ids: ?[]i64 = null,
    seq_len: usize,
};

pub fn resolveModelHiddenSize(allocator: std.mem.Allocator, model_dir: []const u8) !usize {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    if (manifest.hidden_size == 0) return error.MissingHiddenSize;
    return manifest.hidden_size;
}

pub fn initHeadFromModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !RerankerHead {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const hidden_size: usize = manifest.hidden_size;
    if (hidden_size == 0) return error.MissingHiddenSize;

    const weight = try allocator.alloc(f32, hidden_size);
    errdefer allocator.free(weight);
    @memset(weight, 0.0);
    var bias: f32 = 0.0;

    var access = try tensor_access.openFromManifest(allocator, manifest);
    defer access.deinit();

    var out_proj_weight = loadOptionalTensorAsF32(allocator, access, "classifier.out_proj.weight") catch null;
    if (out_proj_weight) |*tensor| {
        defer tensor.deinit();
        if (tensor.shape.len == 2 and tensor.shape[1] == hidden_size and tensor.shape[0] >= 1) {
            @memcpy(weight, tensor.asFloat32()[0..hidden_size]);
            var bias_tensor = try loadTensorAsF32(allocator, access, "classifier.out_proj.bias");
            defer bias_tensor.deinit();
            if (bias_tensor.shape.len >= 1 and bias_tensor.asFloat32().len >= 1) bias = bias_tensor.asFloat32()[0];
        }
    } else {
        var linear_weight = loadOptionalTensorAsF32(allocator, access, "classifier.weight") catch null;
        if (linear_weight) |*tensor| {
            defer tensor.deinit();
            if (tensor.shape.len == 2 and tensor.shape[1] == hidden_size and tensor.shape[0] >= 1) {
                @memcpy(weight, tensor.asFloat32()[0..hidden_size]);
                var bias_tensor = try loadTensorAsF32(allocator, access, "classifier.bias");
                defer bias_tensor.deinit();
                if (bias_tensor.shape.len >= 1 and bias_tensor.asFloat32().len >= 1) bias = bias_tensor.asFloat32()[0];
            }
        }
    }

    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .weight = weight,
        .bias = bias,
    };
}

pub fn saveHead(allocator: std.mem.Allocator, head: *const RerankerHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, head_config_file_name });
    defer allocator.free(config_path);

    const bias = [_]f32{head.bias};
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "legacy_reranker.classifier.weight", .shape = &.{ 1, head.hidden_size }, .data = head.weight },
        .{ .name = "legacy_reranker.classifier.bias", .shape = &.{1}, .data = &bias },
    });

    var file = try compat.cwd().createFile(compat.io(), config_path, .{ .truncate = true });
    defer file.close(compat.io());
    var buf: [256]u8 = undefined;
    var writer = file.writerStreaming(compat.io(), &buf);
    try std.json.Stringify.value(.{
        .task = "reranker_regression",
        .hidden_size = head.hidden_size,
        .checkpoint = head_checkpoint_file_name,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

pub fn materializeMergedHead(allocator: std.mem.Allocator, head: *const RerankerHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, merged_head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);

    const bias = [_]f32{head.bias};
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "classifier.out_proj.weight", .shape = &.{ 1, head.hidden_size }, .data = head.weight },
        .{ .name = "classifier.out_proj.bias", .shape = &.{1}, .data = &bias },
    });
}

pub fn loadHeadIfPresent(allocator: std.mem.Allocator, model_dir: []const u8, hidden_size: usize) !?RerankerHead {
    const checkpoint_path = try std.fs.path.join(allocator, &.{ model_dir, head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    _ = compat.cwd().statFile(compat.io(), checkpoint_path, .{}) catch return null;

    var access = try openTensorAccessForFile(allocator, checkpoint_path);
    defer access.deinit();
    var weight_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.weight");
    defer weight_tensor.deinit();
    var bias_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.bias");
    defer bias_tensor.deinit();
    if (weight_tensor.shape.len != 2 or weight_tensor.shape[0] < 1 or weight_tensor.shape[1] != hidden_size) return error.ShapeMismatch;

    const weight = try allocator.dupe(f32, weight_tensor.asFloat32()[0..hidden_size]);
    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .weight = weight,
        .bias = if (bias_tensor.asFloat32().len > 0) bias_tensor.asFloat32()[0] else 0.0,
    };
}

pub fn loadHeadFromInput(allocator: std.mem.Allocator, input_path: []const u8, hidden_size: usize) !?RerankerHead {
    const stat = compat.cwd().statFile(compat.io(), input_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    return switch (stat.kind) {
        .directory => loadHeadIfPresent(allocator, input_path, hidden_size),
        .file => try loadHeadFromCheckpointPath(allocator, input_path, hidden_size),
        else => error.InvalidHeadInput,
    };
}

pub fn evaluateHead(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    head: *const RerankerHead,
    examples: []const reranker_data.Example,
    backend: BackendChoice,
    max_examples: usize,
) !EvalSummary {
    const cached = try precomputePooledExamples(allocator, model_dir, examples, backend, max_examples);
    defer freeCachedPooledExamples(allocator, cached);
    return try evaluateHeadCached(allocator, head, examples[0..cached.len], cached);
}

pub fn trainHeadEpoch(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    head: *RerankerHead,
    examples: []const reranker_data.Example,
    backend: BackendChoice,
    options: TrainEpochOptions,
) !TrainEpochSummary {
    const cached = try precomputePooledExamples(allocator, model_dir, examples, backend, options.max_examples);
    defer freeCachedPooledExamples(allocator, cached);
    return try trainHeadEpochCached(allocator, model_dir, head, cached, backend, options);
}

pub fn precomputePooledExamples(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    examples: []const reranker_data.Example,
    backend: BackendChoice,
    max_examples: usize,
) ![]CachedPooledExample {
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    const effective = examples[0..@min(examples.len, max_examples)];
    const cached = try allocator.alloc(CachedPooledExample, effective.len);
    errdefer allocator.free(cached);
    var built: usize = 0;
    errdefer {
        for (cached[0..built]) |*entry| entry.deinit(allocator);
    }
    for (effective, 0..) |example, idx| {
        cached[idx] = .{
            .pooled = try encodePairPooled(allocator, &encoder, example.query, example.document),
            .score = example.score,
        };
        built += 1;
    }
    return cached;
}

pub fn freeCachedPooledExamples(allocator: std.mem.Allocator, cached: []CachedPooledExample) void {
    for (cached) |*entry| entry.deinit(allocator);
    allocator.free(cached);
}

pub fn evaluateHeadCached(
    allocator: std.mem.Allocator,
    head: *const RerankerHead,
    examples: []const reranker_data.Example,
    cached: []const CachedPooledExample,
) !EvalSummary {
    if (examples.len != cached.len) return error.ShapeMismatch;
    const predicted = try allocator.alloc(f64, cached.len);
    defer allocator.free(predicted);
    for (cached, 0..) |entry, idx| {
        predicted[idx] = scoreHead(head, entry.pooled);
    }
    return try reranker.computeEvalSummary(allocator, examples, predicted);
}

pub fn trainHeadEpochCached(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    head: *RerankerHead,
    cached: []const CachedPooledExample,
    backend: BackendChoice,
    options: TrainEpochOptions,
) !TrainEpochSummary {
    if (cached.len == 0) return .{};
    const opt: optimizers.Optimizer = if (options.use_schedule_free)
        .{ .schedule_free_adamw = .{} }
    else
        .{ .adamw = .{} };
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    var graph = try graph_bridge.LinearClassifierGraph.init(allocator, 1, head.hidden_size, 1);
    defer graph.deinit();

    var linear_head = graph_bridge.LinearHead{
        .allocator = allocator,
        .weight = try allocator.alloc(f32, head.hidden_size),
        .bias = try allocator.alloc(f32, 1),
        .num_labels = 1,
        .input_dim = head.hidden_size,
    };
    defer linear_head.deinit();
    @memcpy(linear_head.weight, head.weight);
    linear_head.bias[0] = head.bias;

    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();

    var total_loss: f64 = 0;
    var seen: usize = 0;
    for (cached) |example| {
        const target = [_]f32{example.score};
        const batch = graph_bridge.RegressionBatch{
            .features = example.pooled,
            .targets = &target,
            .rows = 1,
            .input_dim = head.hidden_size,
            .output_dim = 1,
        };
        const summary = try graph_bridge.trainLinearRegressorOneStep(
            allocator,
            &encoder.compute_backend,
            &graph,
            &linear_head,
            batch,
            opt,
            &optimizer_state,
            options.learning_rate,
        );
        total_loss += summary.loss_after;
        seen += 1;
    }

    @memcpy(head.weight, linear_head.weight);
    head.bias = linear_head.bias[0];

    return .{
        .examples_seen = seen,
        .average_loss = total_loss / @as(f64, @floatFromInt(@max(seen, 1))),
    };
}

pub fn prepareCachedPooledSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8,
    examples: []const reranker_data.Example,
    backend: BackendChoice,
    max_examples: usize,
) !CachedPooledSummary {
    const effective = examples[0..@min(examples.len, max_examples)];
    const cached = try precomputePooledExamples(allocator, model_dir, effective, backend, max_examples);
    defer freeCachedPooledExamples(allocator, cached);

    const summary_examples = try allocator.alloc(CachedPooledExampleSummary, cached.len);
    var built: usize = 0;
    errdefer {
        for (summary_examples[0..built]) |*entry| freeCachedPooledExampleSummary(allocator, entry);
        allocator.free(summary_examples);
    }
    for (cached, effective, 0..) |entry, example, idx| {
        summary_examples[idx] = .{
            .query = try allocator.dupe(u8, example.query),
            .document = try allocator.dupe(u8, example.document),
            .pooled = try allocator.dupe(f32, entry.pooled),
            .score = example.score,
        };
        built += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, pooled_cache_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .input_path = try allocator.dupe(u8, input_path),
        .split = if (split) |value| try allocator.dupe(u8, value) else null,
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .max_examples = max_examples,
        .hidden_size = if (cached.len > 0) cached[0].pooled.len else 0,
        .dataset_stats = reranker_data.computeStats(effective),
        .pairwise_training_pairs = reranker_data.countPairwiseTrainingPairs(effective),
        .examples = summary_examples,
    };
}

pub fn loadCachedPooledSummary(allocator: std.mem.Allocator, path: []const u8) !CachedPooledSummary {
    const raw = try c_file.readFileMax(allocator, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(CachedPooledSummaryFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return try cloneCachedPooledSummary(allocator, &parsed.summary);
}

pub fn saveCachedPooledSummary(allocator: std.mem.Allocator, path: []const u8, summary: CachedPooledSummary) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .summary = summary }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

pub fn prepareCachedTopLayerSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8,
    examples: []const reranker_data.Example,
    backend: BackendChoice,
    max_examples: usize,
    top_layer_count: usize,
) !CachedTopLayerSummary {
    if (top_layer_count == 0) return error.InvalidTopLayerCount;
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    const effective = examples[0..@min(examples.len, max_examples)];
    const summary_examples = try allocator.alloc(CachedTopLayerExampleSummary, effective.len);
    var built: usize = 0;
    errdefer {
        for (summary_examples[0..built]) |*entry| freeCachedTopLayerExampleSummary(allocator, entry);
        allocator.free(summary_examples);
    }

    for (effective, 0..) |example, idx| {
        var boundary = try text_encoder_boundary.encodePairTopLayerBoundary(allocator, model_dir, backend, example.query, example.document, top_layer_count);
        errdefer boundary.deinit(allocator);
        summary_examples[idx] = .{
            .query = try allocator.dupe(u8, example.query),
            .document = try allocator.dupe(u8, example.document),
            .hidden_in = boundary.hidden_in,
            .attention_mask = boundary.attention_mask,
            .token_type_ids = boundary.token_type_ids,
            .score = example.score,
            .seq_len = boundary.seq_len,
        };
        built += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, top_layer_cache_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .input_path = try allocator.dupe(u8, input_path),
        .split = if (split) |value| try allocator.dupe(u8, value) else null,
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = top_layer_count,
        .hidden_size = switch (encoder.arch_config) {
            .bert => |cfg| cfg.hidden_size,
            .deberta => |cfg| cfg.hidden_size,
        },
        .dataset_stats = reranker_data.computeStats(effective),
        .pairwise_training_pairs = reranker_data.countPairwiseTrainingPairs(effective),
        .examples = summary_examples,
    };
}

pub fn loadCachedTopLayerSummary(allocator: std.mem.Allocator, path: []const u8) !CachedTopLayerSummary {
    const raw = try c_file.readFileMax(allocator, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(CachedTopLayerSummaryFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return try cloneCachedTopLayerSummary(allocator, &parsed.summary);
}

pub fn saveCachedTopLayerSummary(allocator: std.mem.Allocator, path: []const u8, summary: CachedTopLayerSummary) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .summary = summary }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

pub fn replayTopLayersFromBoundary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: BackendChoice,
    boundary: *const CachedTopLayerExampleSummary,
    top_layer_count: usize,
) ![]f32 {
    return text_encoder_boundary.replayTopLayersFromBoundary(
        allocator,
        model_dir,
        backend,
        boundary.hidden_in,
        boundary.attention_mask,
        boundary.seq_len,
        top_layer_count,
    );
}

pub fn replayTopLayersWithRuntime(
    allocator: std.mem.Allocator,
    encoder: *EncoderRuntime,
    boundary: *const CachedTopLayerExampleSummary,
    top_layer_count: usize,
) ![]f32 {
    return replayTopLayersWithEncoder(allocator, encoder, boundary, top_layer_count);
}

pub fn replayBoundaryToLayerInputWithRuntime(
    allocator: std.mem.Allocator,
    encoder: *EncoderRuntime,
    boundary: *const CachedTopLayerExampleSummary,
    top_layer_count: usize,
    target_layer_idx: usize,
) ![]f32 {
    const batch: usize = 1;
    return switch (encoder.arch_config) {
        .bert => |cfg| blk: {
            const start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
            if (target_layer_idx < start or target_layer_idx >= cfg.num_hidden_layers) return error.InvalidTopLayerCount;
            if (target_layer_idx == start) break :blk try allocator.dupe(f32, boundary.hidden_in);
            break :blk try bert_arch.forwardFromHiddenRange(
                &encoder.compute_backend,
                allocator,
                cfg,
                boundary.hidden_in,
                boundary.attention_mask,
                batch,
                boundary.seq_len,
                start,
                target_layer_idx,
            );
        },
        .deberta => |cfg| blk: {
            const start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
            if (target_layer_idx < start or target_layer_idx >= cfg.num_hidden_layers) return error.InvalidTopLayerCount;
            if (target_layer_idx == start) break :blk try allocator.dupe(f32, boundary.hidden_in);
            break :blk try deberta_arch.forwardFromHiddenRange(
                &encoder.compute_backend,
                allocator,
                cfg,
                boundary.hidden_in,
                boundary.attention_mask,
                batch,
                boundary.seq_len,
                start,
                target_layer_idx,
            );
        },
    };
}

pub fn evaluateHeadTopLayerCachedSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    head: *const RerankerHead,
    summary: *const CachedTopLayerSummary,
    backend: BackendChoice,
) !EvalSummary {
    if (summary.hidden_size != head.hidden_size) return error.ShapeMismatch;
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    const examples = try buildExamplesFromTopLayerSummary(allocator, summary);
    defer allocator.free(examples);
    const predicted = try allocator.alloc(f64, summary.examples.len);
    defer allocator.free(predicted);

    for (summary.examples, 0..) |*entry, idx| {
        const hidden = try replayTopLayersWithEncoder(allocator, &encoder, entry, summary.top_layer_count);
        defer allocator.free(hidden);
        predicted[idx] = scoreHead(head, extractClsEmbedding(hidden, summary.hidden_size));
    }
    return try reranker.computeEvalSummary(allocator, examples, predicted);
}

pub fn trainHeadEpochTopLayerCachedSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    head: *RerankerHead,
    summary: *const CachedTopLayerSummary,
    backend: BackendChoice,
    options: TrainEpochOptions,
) !TrainEpochSummary {
    if (summary.hidden_size != head.hidden_size) return error.ShapeMismatch;
    if (summary.examples.len == 0) return .{};
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    var graph = try graph_bridge.LinearClassifierGraph.init(allocator, 1, head.hidden_size, 1);
    defer graph.deinit();

    var linear_head = graph_bridge.LinearHead{
        .allocator = allocator,
        .weight = try allocator.alloc(f32, head.hidden_size),
        .bias = try allocator.alloc(f32, 1),
        .num_labels = 1,
        .input_dim = head.hidden_size,
    };
    defer linear_head.deinit();
    @memcpy(linear_head.weight, head.weight);
    linear_head.bias[0] = head.bias;

    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();

    var total_loss: f64 = 0;
    var seen: usize = 0;
    const limit = @min(summary.examples.len, options.max_examples);
    for (summary.examples[0..limit]) |*entry| {
        const hidden = try replayTopLayersWithEncoder(allocator, &encoder, entry, summary.top_layer_count);
        defer allocator.free(hidden);
        const target = [_]f32{entry.score};
        const batch = graph_bridge.RegressionBatch{
            .features = extractClsEmbedding(hidden, summary.hidden_size),
            .targets = &target,
            .rows = 1,
            .input_dim = head.hidden_size,
            .output_dim = 1,
        };
        const train_summary = try graph_bridge.trainLinearRegressorOneStep(
            allocator,
            &encoder.compute_backend,
            &graph,
            &linear_head,
            batch,
            .{ .adam = .{} },
            &optimizer_state,
            options.learning_rate,
        );
        total_loss += train_summary.loss_after;
        seen += 1;
    }

    @memcpy(head.weight, linear_head.weight);
    head.bias = linear_head.bias[0];

    return .{
        .examples_seen = seen,
        .average_loss = total_loss / @as(f64, @floatFromInt(@max(seen, 1))),
    };
}

pub fn trainEvalHeadTopLayerCachedSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    train_summary: *const CachedTopLayerSummary,
    eval_summary: *const CachedTopLayerSummary,
    out_dir: []const u8,
    backend: BackendChoice,
    epochs: usize,
    options: TrainEpochOptions,
) !TrainEvalSummary {
    if (train_summary.hidden_size == 0) return error.EmptyTopLayerCache;
    if (eval_summary.hidden_size != train_summary.hidden_size) return error.ShapeMismatch;
    if (eval_summary.top_layer_count != train_summary.top_layer_count) return error.ShapeMismatch;

    var head = try loadHeadIfPresent(allocator, out_dir, train_summary.hidden_size) orelse try initHeadFromModelDir(allocator, model_dir);
    defer head.deinit();
    if (head.hidden_size != train_summary.hidden_size) return error.ShapeMismatch;

    var train_epoch = TrainEpochSummary{};
    for (0..epochs) |_| {
        train_epoch = try trainHeadEpochTopLayerCachedSummary(allocator, model_dir, &head, train_summary, backend, options);
    }
    try saveHead(allocator, &head, out_dir);
    try materializeMergedHead(allocator, &head, out_dir);
    const eval_result = try evaluateHeadTopLayerCachedSummary(allocator, model_dir, &head, eval_summary, backend);
    return .{
        .train = train_epoch,
        .eval = eval_result,
        .output_dir = try allocator.dupe(u8, out_dir),
    };
}

pub fn trainEvalHeadCachedSummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    train_summary: *const CachedPooledSummary,
    eval_summary: *const CachedPooledSummary,
    out_dir: []const u8,
    backend: BackendChoice,
    epochs: usize,
    options: TrainEpochOptions,
) !TrainEvalSummary {
    if (train_summary.hidden_size == 0) return error.EmptyPooledCache;
    if (eval_summary.hidden_size != train_summary.hidden_size) return error.ShapeMismatch;
    const train_pair = try buildExamplesAndCachedFromSummary(allocator, train_summary);
    const train_examples = train_pair.examples;
    const train_cached = train_pair.cached;
    defer {
        freeCachedPooledExamples(allocator, train_cached);
        allocator.free(train_examples);
    }
    const eval_pair = try buildExamplesAndCachedFromSummary(allocator, eval_summary);
    const eval_examples = eval_pair.examples;
    const eval_cached = eval_pair.cached;
    defer {
        freeCachedPooledExamples(allocator, eval_cached);
        allocator.free(eval_examples);
    }

    var head = try loadHeadIfPresent(allocator, out_dir, train_summary.hidden_size) orelse try initHeadFromModelDir(allocator, model_dir);
    defer head.deinit();
    if (head.hidden_size != train_summary.hidden_size) return error.ShapeMismatch;

    var train_epoch = TrainEpochSummary{};
    for (0..epochs) |_| {
        train_epoch = try trainHeadEpochCached(allocator, model_dir, &head, train_cached, backend, options);
    }
    try saveHead(allocator, &head, out_dir);
    try materializeMergedHead(allocator, &head, out_dir);
    const eval_result = try evaluateHeadCached(allocator, &head, eval_examples, eval_cached);
    return .{
        .train = train_epoch,
        .eval = eval_result,
        .output_dir = try allocator.dupe(u8, out_dir),
    };
}

pub fn trainEvalHead(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    train_examples: []const reranker_data.Example,
    eval_examples: []const reranker_data.Example,
    out_dir: []const u8,
    backend: BackendChoice,
    epochs: usize,
    options: TrainEpochOptions,
) !TrainEvalSummary {
    var head = try loadHeadIfPresent(allocator, out_dir, (try initHeadFromModelDir(allocator, model_dir)).hidden_size) orelse try initHeadFromModelDir(allocator, model_dir);
    defer head.deinit();

    var train_summary = TrainEpochSummary{};
    for (0..epochs) |_| {
        train_summary = try trainHeadEpoch(allocator, model_dir, &head, train_examples, backend, options);
    }
    try saveHead(allocator, &head, out_dir);
    const eval_summary = try evaluateHead(allocator, model_dir, &head, eval_examples, backend, options.max_examples);
    return .{
        .train = train_summary,
        .eval = eval_summary,
        .output_dir = try allocator.dupe(u8, out_dir),
    };
}

pub fn materializeHeadFromDir(allocator: std.mem.Allocator, model_dir: []const u8, head_dir: []const u8, out_dir: []const u8) !void {
    const hidden_size = try resolveModelHiddenSize(allocator, model_dir);
    var head = (try loadHeadFromInput(allocator, head_dir, hidden_size)) orelse return error.MissingRerankerHead;
    defer head.deinit();
    try materializeMergedHead(allocator, &head, out_dir);
}

pub const EncoderRuntime = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    hf_tok: *hf_tokenizer.HfTokenizer,
    max_length: usize,
    compute_backend: ComputeBackend,
    arch_config: session_factory.GenericEncoderArchConfig,

    pub fn deinit(self: *EncoderRuntime) void {
        self.session.close();
        self.hf_tok.deinitSelf();
    }
};

pub fn openEncoder(allocator: std.mem.Allocator, model_dir: []const u8, backend: BackendChoice) !EncoderRuntime {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    const tok_bytes = try c_file.readFileFromDir(allocator, model_dir, "tokenizer.json");
    defer allocator.free(tok_bytes);
    const hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);

    const session = switch (backend) {
        .native => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
        .mlx => try session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, .generic),
        .cuda => try session_factory.createCudaSessionWithTaskOverride(allocator, model_dir, .generic),
        .auto => blk: {
            if (comptime build_options.enable_cuda) {
                if (backends.gpu_inventory.cudaRuntimeAvailable()) {
                    if (session_factory.createCudaSessionWithTaskOverride(allocator, model_dir, .generic) catch |err| switch (err) {
                        error.UnsupportedCudaArchitecture => null,
                        else => return err,
                    }) |cuda_session| break :blk cuda_session;
                }
            }
            if (build_options.enable_mlx) {
                break :blk session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, .generic) catch try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic);
            }
            break :blk try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic);
        },
    };
    const compute_backend = try session_factory.getComputeBackend(session, allocator);
    return .{
        .allocator = allocator,
        .session = session,
        .hf_tok = hf_tok,
        .max_length = manifest.max_position_embeddings,
        .compute_backend = compute_backend,
        .arch_config = try session_factory.getGenericEncoderArchConfig(session),
    };
}

fn loadHeadFromCheckpointPath(allocator: std.mem.Allocator, checkpoint_path: []const u8, hidden_size: usize) !RerankerHead {
    var access = try openTensorAccessForFile(allocator, checkpoint_path);
    defer access.deinit();
    var weight_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.weight");
    defer weight_tensor.deinit();
    var bias_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.bias");
    defer bias_tensor.deinit();
    if (weight_tensor.shape.len != 2 or weight_tensor.shape[0] < 1 or weight_tensor.shape[1] != hidden_size) return error.ShapeMismatch;

    const weight = try allocator.dupe(f32, weight_tensor.asFloat32()[0..hidden_size]);
    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .weight = weight,
        .bias = if (bias_tensor.asFloat32().len > 0) bias_tensor.asFloat32()[0] else 0.0,
    };
}

pub fn encodePairPooled(allocator: std.mem.Allocator, encoder: *EncoderRuntime, query: []const u8, document: []const u8) ![]f32 {
    const encoded_inputs = try encodePairInputs(allocator, encoder, query, document);
    defer freeEncodedPairInputs(allocator, &encoded_inputs);
    return try encodePairPooledFromInputs(allocator, encoder, &encoded_inputs);
}

fn encodePairTopLayerBoundary(
    allocator: std.mem.Allocator,
    encoder: *EncoderRuntime,
    query: []const u8,
    document: []const u8,
    top_layer_count: usize,
) !TopLayerBoundary {
    const encoded_inputs = try encodePairInputs(allocator, encoder, query, document);
    errdefer freeEncodedPairInputs(allocator, &encoded_inputs);
    const batch: usize = 1;
    const start = switch (encoder.arch_config) {
        .bert => |cfg| cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers),
        .deberta => |cfg| cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers),
    };
    const hidden_in = switch (encoder.arch_config) {
        .bert => |cfg| try bert_arch.forwardUntilLayer(
            &encoder.compute_backend,
            allocator,
            cfg,
            encoded_inputs.ids_i64,
            encoded_inputs.mask_i64,
            encoded_inputs.type_i64,
            batch,
            encoded_inputs.seq_len,
            start,
        ),
        .deberta => |cfg| try deberta_arch.forwardUntilLayer(
            &encoder.compute_backend,
            allocator,
            cfg,
            encoded_inputs.ids_i64,
            encoded_inputs.mask_i64,
            batch,
            encoded_inputs.seq_len,
            start,
        ),
    };
    const attention_mask = try allocator.dupe(i64, encoded_inputs.mask_i64);
    return .{
        .hidden_in = hidden_in,
        .attention_mask = attention_mask,
        .token_type_ids = if (encoded_inputs.type_i64) |value| try allocator.dupe(i64, value) else null,
        .seq_len = encoded_inputs.seq_len,
    };
}

fn replayTopLayersWithEncoder(
    allocator: std.mem.Allocator,
    encoder: *EncoderRuntime,
    boundary: *const CachedTopLayerExampleSummary,
    top_layer_count: usize,
) ![]f32 {
    const batch: usize = 1;
    return switch (encoder.arch_config) {
        .bert => |cfg| blk: {
            const start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
            break :blk try bert_arch.forwardFromHidden(
                &encoder.compute_backend,
                allocator,
                cfg,
                boundary.hidden_in,
                boundary.attention_mask,
                batch,
                boundary.seq_len,
                start,
            );
        },
        .deberta => |cfg| blk: {
            const start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
            break :blk try deberta_arch.forwardFromHidden(
                &encoder.compute_backend,
                allocator,
                cfg,
                boundary.hidden_in,
                boundary.attention_mask,
                batch,
                boundary.seq_len,
                start,
            );
        },
    };
}

fn buildExamplesFromTopLayerSummary(
    allocator: std.mem.Allocator,
    summary: *const CachedTopLayerSummary,
) ![]reranker_data.Example {
    const examples = try allocator.alloc(reranker_data.Example, summary.examples.len);
    for (summary.examples, 0..) |entry, idx| {
        examples[idx] = .{
            .query = entry.query,
            .document = entry.document,
            .score = entry.score,
        };
    }
    return examples;
}

fn extractClsEmbedding(hidden: []const f32, hidden_size: usize) []const f32 {
    return hidden[0..hidden_size];
}

const EncodedPairInputs = struct {
    ids_i64: []i64,
    mask_i64: []i64,
    type_i64: ?[]i64,
    seq_len: usize,
};

fn encodePairInputs(allocator: std.mem.Allocator, encoder: *EncoderRuntime, query: []const u8, document: []const u8) !EncodedPairInputs {
    const tok = encoder.hf_tok.tokenizer();
    const special = tok.specialTokens();
    var encoded = try tok.encodeForPair(allocator, query, document, encoder.max_length);
    defer encoded.deinit();

    const max_len = encoded.ids.len;
    const ids_i64 = try allocator.alloc(i64, max_len);
    errdefer allocator.free(ids_i64);
    const mask_i64 = try allocator.alloc(i64, max_len);
    errdefer allocator.free(mask_i64);
    const type_i64 = try allocator.alloc(i64, max_len);
    errdefer allocator.free(type_i64);
    for (0..max_len) |i| {
        ids_i64[i] = encoded.ids[i];
        mask_i64[i] = encoded.attention_mask[i];
        type_i64[i] = 0;
    }
    buildCrossEncoderTokenTypes(type_i64, encoded.ids, encoded.attention_mask, special.sep_id);

    return .{
        .ids_i64 = ids_i64,
        .mask_i64 = mask_i64,
        .type_i64 = type_i64,
        .seq_len = max_len,
    };
}

fn encodePairPooledFromInputs(allocator: std.mem.Allocator, encoder: *EncoderRuntime, encoded_inputs: *const EncodedPairInputs) ![]f32 {
    const max_len = encoded_inputs.seq_len;

    const input_info = encoder.session.inputInfo();
    var needs_attention_mask = false;
    var needs_token_type = false;
    for (input_info) |info| {
        if (std.mem.eql(u8, info.name, "attention_mask")) needs_attention_mask = true;
        if (std.mem.eql(u8, info.name, "token_type_ids")) needs_token_type = true;
    }

    const shape = [_]i64{ 1, @intCast(max_len) };
    var input_ids = try Tensor.initInt64(allocator, "input_ids", &shape, encoded_inputs.ids_i64);
    defer input_ids.deinit();
    var attention_mask = try Tensor.initInt64(allocator, "attention_mask", &shape, encoded_inputs.mask_i64);
    defer attention_mask.deinit();
    var token_type_ids: ?Tensor = null;
    defer if (token_type_ids) |*tensor| tensor.deinit();
    const inputs = if (needs_token_type) blk: {
        token_type_ids = try Tensor.initInt64(allocator, "token_type_ids", &shape, encoded_inputs.type_i64.?);
        if (needs_attention_mask) break :blk &[_]Tensor{ input_ids, attention_mask, token_type_ids.? };
        break :blk &[_]Tensor{ input_ids, token_type_ids.? };
    } else if (needs_attention_mask) &[_]Tensor{ input_ids, attention_mask } else &[_]Tensor{input_ids};

    const outputs = try encoder.session.run(inputs, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }
    if (outputs.len == 0) return error.MissingOutputs;
    const hidden = &outputs[0];
    if (!std.mem.eql(u8, hidden.name, "last_hidden_state")) return error.UnexpectedOutputTensor;
    if (hidden.shape.len != 3) return error.InvalidOutputShape;
    const hidden_size: usize = @intCast(hidden.shape[2]);
    return try allocator.dupe(f32, hidden.asFloat32()[0..hidden_size]);
}

fn freeEncodedPairInputs(allocator: std.mem.Allocator, encoded_inputs: *const EncodedPairInputs) void {
    allocator.free(encoded_inputs.ids_i64);
    allocator.free(encoded_inputs.mask_i64);
    if (encoded_inputs.type_i64) |value| allocator.free(value);
}

fn buildCrossEncoderTokenTypes(dst: []i64, ids: []const i32, attention_mask: []const i32, sep_id: i32) void {
    var in_segment_b = false;
    var sep_count: usize = 0;
    for (ids, attention_mask, 0..) |id, mask, idx| {
        if (mask == 0) {
            dst[idx] = 0;
        } else if (in_segment_b) {
            dst[idx] = 1;
        } else {
            dst[idx] = 0;
            if (id == sep_id) {
                sep_count += 1;
                if (sep_count == 1) in_segment_b = true;
            }
        }
    }
}

pub fn scoreHead(head: *const RerankerHead, pooled: []const f32) f64 {
    var sum: f64 = head.bias;
    for (pooled, head.weight) |value, weight| sum += @as(f64, value) * @as(f64, weight);
    return sum;
}

fn loadOptionalTensorAsF32(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, name: []const u8) !?Tensor {
    return loadTensorAsF32(allocator, access, name) catch |err| switch (err) {
        error.TensorNotFound => null,
        else => err,
    };
}

fn loadTensorAsF32(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, name: []const u8) !Tensor {
    var record = try access.getRecord(allocator, name);
    defer record.deinit();
    var tensor = (try record.materializeDense(allocator)) orelse return error.UnsupportedTensorEncoding;
    if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
        const converted = try weight_source.convertToF32(allocator, &tensor);
        tensor.deinit();
        return converted;
    }
    if (tensor.dtype != .f32) {
        tensor.deinit();
        return error.UnsupportedTensorType;
    }
    return tensor;
}

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    if (std.mem.endsWith(u8, path, ".index.json")) {
        const access = try tensor_access.ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
    var header_buf: std.Io.Writer.Allocating = .init(allocator);
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

    const io = compat.io();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);

    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(io, &len_buf);
    try file.writeStreamingAll(io, header_buf.written());
    for (tensors) |tensor| {
        for (tensor.data) |item| {
            const bits: u32 = @bitCast(item);
            var bits_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &bits_buf, bits, .little);
            try file.writeStreamingAll(io, &bits_buf);
        }
    }
}

test "reranker head save and load round trip" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_reranker_head_roundtrip_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    var head = RerankerHead{
        .allocator = allocator,
        .hidden_size = 4,
        .weight = try allocator.dupe(f32, &.{ 0.1, 0.2, 0.3, 0.4 }),
        .bias = 0.5,
    };
    defer head.deinit();
    try saveHead(allocator, &head, root);

    var loaded = (try loadHeadIfPresent(allocator, root, 4)).?;
    defer loaded.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), loaded.bias, 1e-6);
    try std.testing.expectEqualSlices(f32, head.weight, loaded.weight);
}

fn cloneCachedPooledSummary(allocator: std.mem.Allocator, source: *const CachedPooledSummary) !CachedPooledSummary {
    const examples = try allocator.alloc(CachedPooledExampleSummary, source.examples.len);
    var built: usize = 0;
    errdefer {
        for (examples[0..built]) |*entry| freeCachedPooledExampleSummary(allocator, entry);
        allocator.free(examples);
    }
    for (source.examples, 0..) |entry, idx| {
        examples[idx] = .{
            .query = try allocator.dupe(u8, entry.query),
            .document = try allocator.dupe(u8, entry.document),
            .pooled = try allocator.dupe(f32, entry.pooled),
            .score = entry.score,
        };
        built += 1;
    }
    return .{
        .artifact_family_version = try allocator.dupe(u8, source.artifact_family_version),
        .model_dir = try allocator.dupe(u8, source.model_dir),
        .input_path = try allocator.dupe(u8, source.input_path),
        .split = if (source.split) |value| try allocator.dupe(u8, value) else null,
        .requested_backend = try allocator.dupe(u8, source.requested_backend),
        .max_examples = source.max_examples,
        .hidden_size = source.hidden_size,
        .dataset_stats = source.dataset_stats,
        .pairwise_training_pairs = source.pairwise_training_pairs,
        .examples = examples,
    };
}

fn buildExamplesAndCachedFromSummary(
    allocator: std.mem.Allocator,
    summary: *const CachedPooledSummary,
) !ExamplesAndCached {
    const examples = try allocator.alloc(reranker_data.Example, summary.examples.len);
    errdefer allocator.free(examples);
    const cached = try allocator.alloc(CachedPooledExample, summary.examples.len);
    var built: usize = 0;
    errdefer {
        for (cached[0..built]) |*entry| entry.deinit(allocator);
        allocator.free(cached);
    }
    for (summary.examples, 0..) |entry, idx| {
        examples[idx] = .{
            .query = entry.query,
            .document = entry.document,
            .score = entry.score,
        };
        cached[idx] = .{
            .pooled = try allocator.dupe(f32, entry.pooled),
            .score = entry.score,
        };
        built += 1;
    }
    return .{
        .examples = examples,
        .cached = cached,
    };
}

fn freeCachedPooledExampleSummary(allocator: std.mem.Allocator, entry: *CachedPooledExampleSummary) void {
    allocator.free(entry.query);
    allocator.free(entry.document);
    allocator.free(entry.pooled);
    entry.* = undefined;
}

pub fn freeCachedPooledSummary(allocator: std.mem.Allocator, summary: *CachedPooledSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.input_path);
    if (summary.split) |value| allocator.free(value);
    allocator.free(summary.requested_backend);
    for (summary.examples) |*entry| freeCachedPooledExampleSummary(allocator, entry);
    allocator.free(summary.examples);
    summary.* = undefined;
}

fn cloneCachedTopLayerSummary(allocator: std.mem.Allocator, source: *const CachedTopLayerSummary) !CachedTopLayerSummary {
    const examples = try allocator.alloc(CachedTopLayerExampleSummary, source.examples.len);
    var built: usize = 0;
    errdefer {
        for (examples[0..built]) |*entry| freeCachedTopLayerExampleSummary(allocator, entry);
        allocator.free(examples);
    }
    for (source.examples, 0..) |entry, idx| {
        examples[idx] = .{
            .query = try allocator.dupe(u8, entry.query),
            .document = try allocator.dupe(u8, entry.document),
            .hidden_in = try allocator.dupe(f32, entry.hidden_in),
            .attention_mask = try allocator.dupe(i64, entry.attention_mask),
            .token_type_ids = if (entry.token_type_ids) |value| try allocator.dupe(i64, value) else null,
            .score = entry.score,
            .seq_len = entry.seq_len,
        };
        built += 1;
    }
    return .{
        .artifact_family_version = try allocator.dupe(u8, source.artifact_family_version),
        .model_dir = try allocator.dupe(u8, source.model_dir),
        .input_path = try allocator.dupe(u8, source.input_path),
        .split = if (source.split) |value| try allocator.dupe(u8, value) else null,
        .requested_backend = try allocator.dupe(u8, source.requested_backend),
        .top_layer_count = source.top_layer_count,
        .hidden_size = source.hidden_size,
        .dataset_stats = source.dataset_stats,
        .pairwise_training_pairs = source.pairwise_training_pairs,
        .examples = examples,
    };
}

fn freeCachedTopLayerExampleSummary(allocator: std.mem.Allocator, entry: *CachedTopLayerExampleSummary) void {
    allocator.free(entry.query);
    allocator.free(entry.document);
    allocator.free(entry.hidden_in);
    allocator.free(entry.attention_mask);
    if (entry.token_type_ids) |value| allocator.free(value);
    entry.* = undefined;
}

pub fn freeCachedTopLayerSummary(allocator: std.mem.Allocator, summary: *CachedTopLayerSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.input_path);
    if (summary.split) |value| allocator.free(value);
    allocator.free(summary.requested_backend);
    for (summary.examples) |*entry| freeCachedTopLayerExampleSummary(allocator, entry);
    allocator.free(summary.examples);
    summary.* = undefined;
}

fn freeTopLayerBoundary(allocator: std.mem.Allocator, boundary: *const TopLayerBoundary) void {
    allocator.free(boundary.hidden_in);
    allocator.free(boundary.attention_mask);
    if (boundary.token_type_ids) |value| allocator.free(value);
}
