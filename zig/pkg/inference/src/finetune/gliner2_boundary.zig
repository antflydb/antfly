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

const backends = @import("../backends/backends.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gliner_head = @import("../architectures/gliner_head.zig");
const c_file = @import("../util/c_file.zig");
const gliner2_data = @import("gliner2_data.zig");
const text_encoder_boundary = @import("text_encoder_boundary.zig");
const graph_bridge = @import("graph_bridge.zig");
const ml = @import("ml");
const optimizers = ml.graph.optimizers;

pub const boundary_cache_file_name = "gliner2_top_layer_boundary_cache.json";
pub const boundary_cache_family_version = "gliner2_top_layer_boundary_cache/v1alpha1";
pub const boundary_head_file_name = "gliner2_top_layer_boundary_head.json";
pub const boundary_head_family_version = "gliner2_top_layer_boundary_head/v1alpha1";
pub const boundary_task_head_file_name = "gliner2_top_layer_boundary_task_head.json";
pub const boundary_task_head_family_version = "gliner2_top_layer_boundary_task_head/v1alpha1";

pub const CachedBoundaryExampleSummary = struct {
    text: []const u8,
    hidden_in: []f32,
    input_ids: []i32,
    attention_mask: []i64,
    words_mask: []i32,
    first_token_positions: []i32,
    span_indices: []i32,
    span_mask: []f32,
    span_labels: []f32,
    e_token_positions: []i32,
    e_token_end_positions: []i32,
    entity_type_kind: []i32,
    seq_len: usize,
    max_words_per_sample: usize,
    max_spans: usize,
    num_entity_types: usize,
};

pub const CachedBoundarySummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8 = null,
    requested_backend: []const u8,
    top_layer_count: usize,
    hidden_size: usize,
    max_length: usize,
    max_span_width: usize,
    entity_types: []const []const u8,
    dataset_stats: gliner2_data.DatasetStats,
    examples: []CachedBoundaryExampleSummary,
};

const CachedBoundarySummaryFile = struct {
    summary: CachedBoundarySummary,
};

const SavedBoundaryHeadFile = struct {
    head: BoundaryHead,
};

const SavedBoundaryTaskHeadFile = struct {
    head: BoundaryTaskHead,
};

pub const EvalSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    requested_backend: []const u8,
    top_layer_count: usize,
    hidden_size: usize,
    examples_seen: usize,
    active_span_labels: usize,
    positive_labels: usize,
    average_loss: f64,
    average_positive_probability: f64,
    average_negative_probability: f64,
};

pub const BoundaryHead = struct {
    artifact_family_version: []const u8,
    requested_backend: []const u8,
    top_layer_count: usize,
    weight: f32,
    bias: f32,
};

pub const TrainEvalSummary = struct {
    artifact_family_version: []const u8,
    requested_backend: []const u8,
    top_layer_count: usize,
    learning_rate: f32,
    epochs: usize,
    saved_head_file: []const u8,
    train_before: EvalSummary,
    train_after: EvalSummary,
    eval_before: EvalSummary,
    eval_after: EvalSummary,
};

pub const BoundaryTaskHead = struct {
    artifact_family_version: []const u8,
    requested_backend: []const u8,
    top_layer_count: usize,
    num_entity_types: usize,
    raw_weights: []f32,
    label_bias: []f32,
    global_bias: f32,
};

pub const TaskHeadTrainEvalSummary = struct {
    artifact_family_version: []const u8,
    requested_backend: []const u8,
    top_layer_count: usize,
    num_entity_types: usize,
    learning_rate: f32,
    epochs: usize,
    saved_head_file: []const u8,
    train_before: EvalSummary,
    train_after: EvalSummary,
    eval_before: EvalSummary,
    eval_after: EvalSummary,
};

const CalibrationBatch = struct {
    logits: []f32,
    targets: []f32,
    label_indices: []u32,

    fn deinit(self: *CalibrationBatch, allocator: std.mem.Allocator) void {
        allocator.free(self.logits);
        allocator.free(self.targets);
        allocator.free(self.label_indices);
        self.* = undefined;
    }
};

pub fn prepareCachedBoundarySummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    input_path: []const u8,
    split: ?[]const u8,
    examples: []const gliner2_data.Example,
    entity_types: []const []const u8,
    backend: text_encoder_boundary.BackendChoice,
    max_examples: usize,
    max_length: usize,
    max_span_width: usize,
    top_layer_count: usize,
) !CachedBoundarySummary {
    if (top_layer_count == 0) return error.InvalidTopLayerCount;

    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, model_dir);
    defer tokenizer.deinit(allocator);

    const effective = examples[0..@min(examples.len, max_examples)];
    var workspace = try gliner2_data.ReusableBatch.init(allocator, 1, max_length, max_span_width, entity_types.len);
    defer workspace.deinit();

    const hidden_size = try resolveHiddenSize(allocator, model_dir);
    const summary_examples = try allocator.alloc(CachedBoundaryExampleSummary, effective.len);
    var built: usize = 0;
    errdefer {
        for (summary_examples[0..built]) |*entry| freeCachedBoundaryExampleSummary(allocator, entry);
        allocator.free(summary_examples);
    }

    for (effective, 0..) |ex, idx| {
        var batch = try gliner2_data.buildSimpleBatchInto(&workspace, &tokenizer, &.{ex}, entity_types, max_span_width);
        defer batch.deinit();

        const seq_len = actualSeqLen(batch.attention_mask[0..batch.max_length]);
        const ids_i64 = try allocator.alloc(i64, seq_len);
        defer allocator.free(ids_i64);
        const mask_i64 = try allocator.alloc(i64, seq_len);
        defer allocator.free(mask_i64);
        for (0..seq_len) |i| {
            ids_i64[i] = batch.input_ids[i];
            mask_i64[i] = batch.attention_mask[i];
        }

        var boundary = try text_encoder_boundary.captureTopLayerBoundaryFromEncodedInputs(
            allocator,
            model_dir,
            backend,
            ids_i64,
            mask_i64,
            null,
            seq_len,
            top_layer_count,
        );
        defer boundary.deinit(allocator);

        summary_examples[idx] = .{
            .text = try allocator.dupe(u8, ex.text),
            .hidden_in = try allocator.dupe(f32, boundary.hidden_in),
            .input_ids = try allocator.dupe(i32, batch.input_ids[0..batch.max_length]),
            .attention_mask = try allocator.dupe(i64, boundary.attention_mask),
            .words_mask = try allocator.dupe(i32, batch.words_mask[0..batch.max_length]),
            .first_token_positions = try allocator.dupe(i32, batch.first_token_positions[0..batch.max_words_per_sample]),
            .span_indices = try allocator.dupe(i32, batch.span_indices[0 .. batch.max_spans * 2]),
            .span_mask = try allocator.dupe(f32, batch.span_mask[0..batch.max_spans]),
            .span_labels = try allocator.dupe(f32, batch.span_labels[0 .. batch.max_spans * batch.num_entity_types]),
            .e_token_positions = try allocator.dupe(i32, batch.e_token_positions[0..batch.num_entity_types]),
            .e_token_end_positions = try allocator.dupe(i32, batch.e_token_end_positions[0..batch.num_entity_types]),
            .entity_type_kind = try allocator.dupe(i32, batch.entity_type_kind[0..batch.num_entity_types]),
            .seq_len = seq_len,
            .max_words_per_sample = batch.max_words_per_sample,
            .max_spans = batch.max_spans,
            .num_entity_types = batch.num_entity_types,
        };
        built += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_cache_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .input_path = try allocator.dupe(u8, input_path),
        .split = if (split) |value| try allocator.dupe(u8, value) else null,
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = top_layer_count,
        .hidden_size = hidden_size,
        .max_length = max_length,
        .max_span_width = max_span_width,
        .entity_types = try dupeStringSlice(allocator, entity_types),
        .dataset_stats = try gliner2_data.computeStats(allocator, effective),
        .examples = summary_examples,
    };
}

pub fn saveCachedBoundarySummary(allocator: std.mem.Allocator, path: []const u8, summary: CachedBoundarySummary) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .summary = summary }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

pub fn loadCachedBoundarySummary(allocator: std.mem.Allocator, path: []const u8) !CachedBoundarySummary {
    const raw = try c_file.readFileMax(allocator, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(CachedBoundarySummaryFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return try cloneCachedBoundarySummary(allocator, &parsed.summary);
}

pub fn evaluateCachedBoundarySummary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
) !EvalSummary {
    return evaluateCachedBoundarySummaryWithHead(allocator, model_dir, summary, backend, null);
}

pub fn evaluateCachedBoundarySummaryWithHead(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
    head: ?*const BoundaryHead,
) !EvalSummary {
    var batch = try collectCalibrationBatch(allocator, model_dir, summary, backend);
    defer batch.deinit(allocator);

    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_cache_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = summary.top_layer_count,
        .hidden_size = summary.hidden_size,
        .examples_seen = summary.examples.len,
        .active_span_labels = batch.logits.len,
        .positive_labels = countPositiveLabels(batch.targets),
        .average_loss = computeAverageLoss(batch.logits, batch.targets, head),
        .average_positive_probability = computeAveragePositiveProbability(batch.logits, batch.targets, head),
        .average_negative_probability = computeAverageNegativeProbability(batch.logits, batch.targets, head),
    };
}

pub fn evaluateCachedBoundarySummaryWithTaskHead(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
    head: ?*const BoundaryTaskHead,
) !EvalSummary {
    var batch = try collectCalibrationBatch(allocator, model_dir, summary, backend);
    defer batch.deinit(allocator);
    return try buildEvalSummaryFromTaskBatch(allocator, model_dir, summary, backend, &batch, head);
}

pub const BoundaryTrainOptions = struct {
    learning_rate: f32 = 0.001,
    epochs: usize = 1,
    use_schedule_free: bool = false,
};

pub fn trainEvalBoundaryHead(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    train_summary: *const CachedBoundarySummary,
    eval_summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
    out_dir: []const u8,
    options: BoundaryTrainOptions,
) !TrainEvalSummary {
    const learning_rate = options.learning_rate;
    const epochs = options.epochs;
    const opt: optimizers.Optimizer = if (options.use_schedule_free)
        .{ .schedule_free_adamw = .{} }
    else
        .{ .adamw = .{} };
    var train_batch = try collectCalibrationBatch(allocator, model_dir, train_summary, backend);
    defer train_batch.deinit(allocator);
    var eval_batch = try collectCalibrationBatch(allocator, model_dir, eval_summary, backend);
    defer eval_batch.deinit(allocator);

    var head = try initBoundaryHead(allocator, backend, train_summary.top_layer_count);
    defer freeBoundaryHead(allocator, &head);

    var train_before = try buildEvalSummaryFromBatch(allocator, model_dir, train_summary, backend, &train_batch, &head);
    errdefer freeEvalSummary(allocator, &train_before);
    var eval_before = try buildEvalSummaryFromBatch(allocator, model_dir, eval_summary, backend, &eval_batch, &head);
    errdefer freeEvalSummary(allocator, &eval_before);

    var graph = try graph_bridge.LinearClassifierGraph.init(allocator, 1, 1, 1);
    defer graph.deinit();

    var linear_head = graph_bridge.LinearHead{
        .allocator = allocator,
        .weight = try allocator.alloc(f32, 1),
        .bias = try allocator.alloc(f32, 1),
        .num_labels = 1,
        .input_dim = 1,
    };
    defer linear_head.deinit();
    linear_head.weight[0] = head.weight;
    linear_head.bias[0] = head.bias;

    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();

    var session = switch (backend) {
        .native => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
        .mlx => try session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, .generic),
        .auto => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
    };
    defer session.close();
    var cb = try session_factory.getComputeBackend(session, allocator);
    defer cb.deinit();

    for (0..epochs) |_| {
        for (0..train_batch.logits.len) |row_idx| {
            const batch = graph_bridge.RegressionBatch{
                .features = train_batch.logits[row_idx .. row_idx + 1],
                .targets = train_batch.targets[row_idx .. row_idx + 1],
                .rows = 1,
                .input_dim = 1,
                .output_dim = 1,
            };
            _ = try graph_bridge.trainLinearRegressorOneStep(
                allocator,
                &cb,
                &graph,
                &linear_head,
                batch,
                opt,
                &optimizer_state,
                learning_rate,
            );
        }
    }

    head.weight = linear_head.weight[0];
    head.bias = linear_head.bias[0];

    try std.Io.Dir.cwd().createDirPath(compat.io(), out_dir);
    try saveBoundaryHead(allocator, out_dir, &head);

    var train_after = try buildEvalSummaryFromBatch(allocator, model_dir, train_summary, backend, &train_batch, &head);
    errdefer freeEvalSummary(allocator, &train_after);
    var eval_after = try buildEvalSummaryFromBatch(allocator, model_dir, eval_summary, backend, &eval_batch, &head);
    errdefer freeEvalSummary(allocator, &eval_after);

    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_head_family_version),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = train_summary.top_layer_count,
        .learning_rate = learning_rate,
        .epochs = epochs,
        .saved_head_file = try allocator.dupe(u8, boundary_head_file_name),
        .train_before = train_before,
        .train_after = train_after,
        .eval_before = eval_before,
        .eval_after = eval_after,
    };
}

pub const BoundaryTaskHeadTrainOptions = struct {
    learning_rate: f32 = 0.001,
    epochs: usize = 1,
    use_schedule_free: bool = false,
};

pub fn trainEvalBoundaryTaskHead(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    train_summary: *const CachedBoundarySummary,
    eval_summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
    out_dir: []const u8,
    options: BoundaryTaskHeadTrainOptions,
) !TaskHeadTrainEvalSummary {
    const learning_rate = options.learning_rate;
    const epochs = options.epochs;
    const opt: optimizers.Optimizer = if (options.use_schedule_free)
        .{ .schedule_free_adamw = .{} }
    else
        .{ .adamw = .{} };
    var train_batch = try collectCalibrationBatch(allocator, model_dir, train_summary, backend);
    defer train_batch.deinit(allocator);
    var eval_batch = try collectCalibrationBatch(allocator, model_dir, eval_summary, backend);
    defer eval_batch.deinit(allocator);

    var head = try initBoundaryTaskHead(allocator, backend, train_summary.top_layer_count, train_summary.entity_types.len);
    defer freeBoundaryTaskHead(allocator, &head);

    var train_before = try buildEvalSummaryFromTaskBatch(allocator, model_dir, train_summary, backend, &train_batch, &head);
    errdefer freeEvalSummary(allocator, &train_before);
    var eval_before = try buildEvalSummaryFromTaskBatch(allocator, model_dir, eval_summary, backend, &eval_batch, &head);
    errdefer freeEvalSummary(allocator, &eval_before);

    const input_dim = train_summary.entity_types.len * 2;
    var graph = try graph_bridge.LinearClassifierGraph.init(allocator, 1, input_dim, 1);
    defer graph.deinit();
    var linear_head = graph_bridge.LinearHead{
        .allocator = allocator,
        .weight = try allocator.alloc(f32, input_dim),
        .bias = try allocator.alloc(f32, 1),
        .num_labels = 1,
        .input_dim = input_dim,
    };
    defer linear_head.deinit();
    @memset(linear_head.weight, 0);
    @memset(linear_head.bias, 0);
    for (0..head.num_entity_types) |label_idx| {
        linear_head.weight[label_idx] = head.raw_weights[label_idx];
        linear_head.weight[head.num_entity_types + label_idx] = head.label_bias[label_idx];
    }
    linear_head.bias[0] = head.global_bias;

    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();

    var session = switch (backend) {
        .native => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
        .mlx => try session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, .generic),
        .auto => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
    };
    defer session.close();
    var cb = try session_factory.getComputeBackend(session, allocator);
    defer cb.deinit();

    const features = try allocator.alloc(f32, input_dim);
    defer allocator.free(features);
    for (0..epochs) |_| {
        for (0..train_batch.logits.len) |row_idx| {
            const label_idx = train_batch.label_indices[row_idx];
            if (label_idx >= head.num_entity_types) continue;
            @memset(features, 0);
            features[label_idx] = train_batch.logits[row_idx];
            features[head.num_entity_types + label_idx] = 1.0;
            const batch = graph_bridge.RegressionBatch{
                .features = features,
                .targets = train_batch.targets[row_idx .. row_idx + 1],
                .rows = 1,
                .input_dim = input_dim,
                .output_dim = 1,
            };
            _ = try graph_bridge.trainLinearRegressorOneStep(
                allocator,
                &cb,
                &graph,
                &linear_head,
                batch,
                opt,
                &optimizer_state,
                learning_rate,
            );
        }
    }

    for (0..head.num_entity_types) |label_idx| {
        head.raw_weights[label_idx] = linear_head.weight[label_idx];
        head.label_bias[label_idx] = linear_head.weight[head.num_entity_types + label_idx];
    }
    head.global_bias = linear_head.bias[0];

    try std.Io.Dir.cwd().createDirPath(compat.io(), out_dir);
    try saveBoundaryTaskHead(allocator, out_dir, &head);

    var train_after = try buildEvalSummaryFromTaskBatch(allocator, model_dir, train_summary, backend, &train_batch, &head);
    errdefer freeEvalSummary(allocator, &train_after);
    var eval_after = try buildEvalSummaryFromTaskBatch(allocator, model_dir, eval_summary, backend, &eval_batch, &head);
    errdefer freeEvalSummary(allocator, &eval_after);

    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_task_head_family_version),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = train_summary.top_layer_count,
        .num_entity_types = head.num_entity_types,
        .learning_rate = learning_rate,
        .epochs = epochs,
        .saved_head_file = try allocator.dupe(u8, boundary_task_head_file_name),
        .train_before = train_before,
        .train_after = train_after,
        .eval_before = eval_before,
        .eval_after = eval_after,
    };
}

pub fn saveBoundaryHead(allocator: std.mem.Allocator, out_dir: []const u8, head: *const BoundaryHead) !void {
    const path = try std.fs.path.join(allocator, &.{ out_dir, boundary_head_file_name });
    defer allocator.free(path);
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .head = head.* }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

pub fn saveBoundaryTaskHead(allocator: std.mem.Allocator, out_dir: []const u8, head: *const BoundaryTaskHead) !void {
    const path = try std.fs.path.join(allocator, &.{ out_dir, boundary_task_head_file_name });
    defer allocator.free(path);
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .head = head.* }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

pub fn resolveBoundaryHeadPath(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    if (isRegularFilePath(input)) return try allocator.dupe(u8, input);
    const path = try std.fs.path.join(allocator, &.{ input, boundary_head_file_name });
    errdefer allocator.free(path);
    if (!isRegularFilePath(path)) return error.MissingBoundaryHead;
    return path;
}

pub fn resolveBoundaryTaskHeadPath(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    if (isRegularFilePath(input)) return try allocator.dupe(u8, input);
    const path = try std.fs.path.join(allocator, &.{ input, boundary_task_head_file_name });
    errdefer allocator.free(path);
    if (!isRegularFilePath(path)) return error.MissingBoundaryHead;
    return path;
}

pub fn loadBoundaryHead(allocator: std.mem.Allocator, path: []const u8) !BoundaryHead {
    const raw = try c_file.readFileMax(allocator, path, 4 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(SavedBoundaryHeadFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return .{
        .artifact_family_version = try allocator.dupe(u8, parsed.head.artifact_family_version),
        .requested_backend = try allocator.dupe(u8, parsed.head.requested_backend),
        .top_layer_count = parsed.head.top_layer_count,
        .weight = parsed.head.weight,
        .bias = parsed.head.bias,
    };
}

pub fn loadBoundaryTaskHead(allocator: std.mem.Allocator, path: []const u8) !BoundaryTaskHead {
    const raw = try c_file.readFileMax(allocator, path, 8 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(SavedBoundaryTaskHeadFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return .{
        .artifact_family_version = try allocator.dupe(u8, parsed.head.artifact_family_version),
        .requested_backend = try allocator.dupe(u8, parsed.head.requested_backend),
        .top_layer_count = parsed.head.top_layer_count,
        .num_entity_types = parsed.head.num_entity_types,
        .raw_weights = try allocator.dupe(f32, parsed.head.raw_weights),
        .label_bias = try allocator.dupe(f32, parsed.head.label_bias),
        .global_bias = parsed.head.global_bias,
    };
}

pub fn freeBoundaryHead(allocator: std.mem.Allocator, head: *BoundaryHead) void {
    allocator.free(head.artifact_family_version);
    allocator.free(head.requested_backend);
    head.* = undefined;
}

pub fn freeBoundaryTaskHead(allocator: std.mem.Allocator, head: *BoundaryTaskHead) void {
    allocator.free(head.artifact_family_version);
    allocator.free(head.requested_backend);
    allocator.free(head.raw_weights);
    allocator.free(head.label_bias);
    head.* = undefined;
}

pub fn freeEvalSummary(allocator: std.mem.Allocator, summary: *EvalSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.requested_backend);
    summary.* = undefined;
}

pub fn freeTrainEvalSummary(allocator: std.mem.Allocator, summary: *TrainEvalSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.requested_backend);
    allocator.free(summary.saved_head_file);
    freeEvalSummary(allocator, &summary.train_before);
    freeEvalSummary(allocator, &summary.train_after);
    freeEvalSummary(allocator, &summary.eval_before);
    freeEvalSummary(allocator, &summary.eval_after);
    summary.* = undefined;
}

pub fn freeTaskHeadTrainEvalSummary(allocator: std.mem.Allocator, summary: *TaskHeadTrainEvalSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.requested_backend);
    allocator.free(summary.saved_head_file);
    freeEvalSummary(allocator, &summary.train_before);
    freeEvalSummary(allocator, &summary.train_after);
    freeEvalSummary(allocator, &summary.eval_before);
    freeEvalSummary(allocator, &summary.eval_after);
    summary.* = undefined;
}

fn collectCalibrationBatch(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
) !CalibrationBatch {
    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, model_dir);
    defer tokenizer.deinit(allocator);

    var session = switch (backend) {
        .native => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
        .mlx => try session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, .generic),
        .auto => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, .generic),
    };
    defer session.close();
    var cb = try session_factory.getComputeBackend(session, allocator);
    defer cb.deinit();

    var logits = std.ArrayListUnmanaged(f32).empty;
    defer logits.deinit(allocator);
    var targets = std.ArrayListUnmanaged(f32).empty;
    defer targets.deinit(allocator);
    var label_indices = std.ArrayListUnmanaged(u32).empty;
    defer label_indices.deinit(allocator);

    for (summary.examples) |entry| {
        const hidden = try text_encoder_boundary.replayTopLayersFromBoundary(
            allocator,
            model_dir,
            backend,
            entry.hidden_in,
            entry.attention_mask,
            entry.seq_len,
            summary.top_layer_count,
        );
        defer allocator.free(hidden);

        const input_ids_i64 = try allocator.alloc(i64, entry.seq_len);
        defer allocator.free(input_ids_i64);
        const words_mask_i64 = try allocator.alloc(i64, entry.seq_len);
        defer allocator.free(words_mask_i64);
        for (0..entry.seq_len) |i| {
            input_ids_i64[i] = entry.input_ids[i];
            words_mask_i64[i] = entry.words_mask[i];
        }

        const span_idx_i64 = try allocator.alloc(i64, entry.max_spans * 2);
        defer allocator.free(span_idx_i64);
        for (0..entry.max_spans * 2) |i| span_idx_i64[i] = entry.span_indices[i];

        const forward = try gliner_head.forward(
            &cb,
            allocator,
            hidden,
            input_ids_i64,
            words_mask_i64,
            span_idx_i64,
            1,
            entry.seq_len,
            @intCast(summary.hidden_size),
            tokenizer.ent_id,
        );
        defer allocator.free(forward.logits);

        const labels_per_span = @min(forward.num_labels, entry.num_entity_types);
        const spans = @min(forward.num_words * forward.max_width, entry.max_spans);
        for (0..spans) |span_idx| {
            if (entry.span_mask[span_idx] <= 0.0) continue;
            for (0..labels_per_span) |label_idx| {
                const flat_idx = span_idx * labels_per_span + label_idx;
                if (flat_idx >= forward.logits.len) continue;
                const target = entry.span_labels[span_idx * entry.num_entity_types + label_idx];
                try logits.append(allocator, forward.logits[flat_idx]);
                try targets.append(allocator, target);
                try label_indices.append(allocator, @intCast(label_idx));
            }
        }
    }

    return .{
        .logits = try logits.toOwnedSlice(allocator),
        .targets = try targets.toOwnedSlice(allocator),
        .label_indices = try label_indices.toOwnedSlice(allocator),
    };
}

pub fn freeCachedBoundarySummary(allocator: std.mem.Allocator, summary: *CachedBoundarySummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.input_path);
    if (summary.split) |value| allocator.free(value);
    allocator.free(summary.requested_backend);
    for (summary.entity_types) |label| allocator.free(label);
    allocator.free(summary.entity_types);
    for (summary.examples) |*entry| freeCachedBoundaryExampleSummary(allocator, entry);
    allocator.free(summary.examples);
    summary.* = undefined;
}

fn cloneCachedBoundarySummary(allocator: std.mem.Allocator, source: *const CachedBoundarySummary) !CachedBoundarySummary {
    const examples = try allocator.alloc(CachedBoundaryExampleSummary, source.examples.len);
    var built: usize = 0;
    errdefer {
        for (examples[0..built]) |*entry| freeCachedBoundaryExampleSummary(allocator, entry);
        allocator.free(examples);
    }
    for (source.examples, 0..) |entry, idx| {
        examples[idx] = .{
            .text = try allocator.dupe(u8, entry.text),
            .hidden_in = try allocator.dupe(f32, entry.hidden_in),
            .input_ids = try allocator.dupe(i32, entry.input_ids),
            .attention_mask = try allocator.dupe(i64, entry.attention_mask),
            .words_mask = try allocator.dupe(i32, entry.words_mask),
            .first_token_positions = try allocator.dupe(i32, entry.first_token_positions),
            .span_indices = try allocator.dupe(i32, entry.span_indices),
            .span_mask = try allocator.dupe(f32, entry.span_mask),
            .span_labels = try allocator.dupe(f32, entry.span_labels),
            .e_token_positions = try allocator.dupe(i32, entry.e_token_positions),
            .e_token_end_positions = try allocator.dupe(i32, entry.e_token_end_positions),
            .entity_type_kind = try allocator.dupe(i32, entry.entity_type_kind),
            .seq_len = entry.seq_len,
            .max_words_per_sample = entry.max_words_per_sample,
            .max_spans = entry.max_spans,
            .num_entity_types = entry.num_entity_types,
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
        .max_length = source.max_length,
        .max_span_width = source.max_span_width,
        .entity_types = try dupeStringSlice(allocator, source.entity_types),
        .dataset_stats = source.dataset_stats,
        .examples = examples,
    };
}

fn freeCachedBoundaryExampleSummary(allocator: std.mem.Allocator, entry: *CachedBoundaryExampleSummary) void {
    allocator.free(entry.text);
    allocator.free(entry.hidden_in);
    allocator.free(entry.input_ids);
    allocator.free(entry.attention_mask);
    allocator.free(entry.words_mask);
    allocator.free(entry.first_token_positions);
    allocator.free(entry.span_indices);
    allocator.free(entry.span_mask);
    allocator.free(entry.span_labels);
    allocator.free(entry.e_token_positions);
    allocator.free(entry.e_token_end_positions);
    allocator.free(entry.entity_type_kind);
    entry.* = undefined;
}

fn resolveHiddenSize(allocator: std.mem.Allocator, model_dir: []const u8) !usize {
    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "encoder_config", "config.json" });
    defer allocator.free(config_path);
    const raw = try compat.cwd().readFileAlloc(compat.io(), config_path, allocator, .limited(8 * 1024 * 1024));
    defer allocator.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    const hidden_val = obj.get("hidden_size") orelse return error.MissingHiddenSize;
    return switch (hidden_val) {
        .integer => @intCast(hidden_val.integer),
        else => error.InvalidModelConfig,
    };
}

fn actualSeqLen(attention_mask: []const i32) usize {
    var len: usize = 0;
    for (attention_mask) |value| {
        if (value == 0) break;
        len += 1;
    }
    return len;
}

fn initBoundaryHead(
    allocator: std.mem.Allocator,
    backend: text_encoder_boundary.BackendChoice,
    top_layer_count: usize,
) !BoundaryHead {
    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_head_family_version),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = top_layer_count,
        .weight = 1.0,
        .bias = 0.0,
    };
}

fn initBoundaryTaskHead(
    allocator: std.mem.Allocator,
    backend: text_encoder_boundary.BackendChoice,
    top_layer_count: usize,
    num_entity_types: usize,
) !BoundaryTaskHead {
    const raw_weights = try allocator.alloc(f32, num_entity_types);
    errdefer allocator.free(raw_weights);
    const label_bias = try allocator.alloc(f32, num_entity_types);
    errdefer allocator.free(label_bias);
    @memset(raw_weights, 1.0);
    @memset(label_bias, 0.0);
    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_task_head_family_version),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = top_layer_count,
        .num_entity_types = num_entity_types,
        .raw_weights = raw_weights,
        .label_bias = label_bias,
        .global_bias = 0.0,
    };
}

fn buildEvalSummaryFromBatch(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
    batch: *const CalibrationBatch,
    head: ?*const BoundaryHead,
) !EvalSummary {
    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_cache_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = summary.top_layer_count,
        .hidden_size = summary.hidden_size,
        .examples_seen = summary.examples.len,
        .active_span_labels = batch.logits.len,
        .positive_labels = countPositiveLabels(batch.targets),
        .average_loss = computeAverageLoss(batch.logits, batch.targets, head),
        .average_positive_probability = computeAveragePositiveProbability(batch.logits, batch.targets, head),
        .average_negative_probability = computeAverageNegativeProbability(batch.logits, batch.targets, head),
    };
}

fn buildEvalSummaryFromTaskBatch(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    summary: *const CachedBoundarySummary,
    backend: text_encoder_boundary.BackendChoice,
    batch: *const CalibrationBatch,
    head: ?*const BoundaryTaskHead,
) !EvalSummary {
    return .{
        .artifact_family_version = try allocator.dupe(u8, boundary_cache_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .top_layer_count = summary.top_layer_count,
        .hidden_size = summary.hidden_size,
        .examples_seen = summary.examples.len,
        .active_span_labels = batch.logits.len,
        .positive_labels = countPositiveLabels(batch.targets),
        .average_loss = computeAverageTaskLoss(batch.logits, batch.targets, batch.label_indices, head),
        .average_positive_probability = computeAverageTaskPositiveProbability(batch.logits, batch.targets, batch.label_indices, head),
        .average_negative_probability = computeAverageTaskNegativeProbability(batch.logits, batch.targets, batch.label_indices, head),
    };
}

fn calibratedLogit(raw_logit: f32, head: ?*const BoundaryHead) f32 {
    if (head) |value| return raw_logit * value.weight + value.bias;
    return raw_logit;
}

fn calibratedTaskLogit(raw_logit: f32, label_idx: usize, head: ?*const BoundaryTaskHead) f32 {
    if (head) |value| {
        if (label_idx < value.num_entity_types) {
            return raw_logit * value.raw_weights[label_idx] + value.label_bias[label_idx] + value.global_bias;
        }
    }
    return raw_logit;
}

fn countPositiveLabels(targets: []const f32) usize {
    var total: usize = 0;
    for (targets) |target| {
        if (target > 0.5) total += 1;
    }
    return total;
}

fn computeAverageLoss(logits: []const f32, targets: []const f32, head: ?*const BoundaryHead) f64 {
    if (logits.len == 0) return 0;
    var total: f64 = 0;
    for (logits, targets) |raw_logit, target| {
        const prob = sigmoid(calibratedLogit(raw_logit, head));
        total += binaryCrossEntropy(prob, target);
    }
    return total / @as(f64, @floatFromInt(logits.len));
}

fn computeAveragePositiveProbability(logits: []const f32, targets: []const f32, head: ?*const BoundaryHead) f64 {
    var total: f64 = 0;
    var count: usize = 0;
    for (logits, targets) |raw_logit, target| {
        if (target <= 0.5) continue;
        total += sigmoid(calibratedLogit(raw_logit, head));
        count += 1;
    }
    return if (count == 0) 0 else total / @as(f64, @floatFromInt(count));
}

fn computeAverageNegativeProbability(logits: []const f32, targets: []const f32, head: ?*const BoundaryHead) f64 {
    var total: f64 = 0;
    var count: usize = 0;
    for (logits, targets) |raw_logit, target| {
        if (target > 0.5) continue;
        total += sigmoid(calibratedLogit(raw_logit, head));
        count += 1;
    }
    return if (count == 0) 0 else total / @as(f64, @floatFromInt(count));
}

fn computeAverageTaskLoss(logits: []const f32, targets: []const f32, label_indices: []const u32, head: ?*const BoundaryTaskHead) f64 {
    if (logits.len == 0) return 0;
    var total: f64 = 0;
    for (logits, targets, label_indices) |raw_logit, target, label_idx| {
        const prob = sigmoid(calibratedTaskLogit(raw_logit, label_idx, head));
        total += binaryCrossEntropy(prob, target);
    }
    return total / @as(f64, @floatFromInt(logits.len));
}

fn computeAverageTaskPositiveProbability(logits: []const f32, targets: []const f32, label_indices: []const u32, head: ?*const BoundaryTaskHead) f64 {
    var total: f64 = 0;
    var count: usize = 0;
    for (logits, targets, label_indices) |raw_logit, target, label_idx| {
        if (target <= 0.5) continue;
        total += sigmoid(calibratedTaskLogit(raw_logit, label_idx, head));
        count += 1;
    }
    return if (count == 0) 0 else total / @as(f64, @floatFromInt(count));
}

fn computeAverageTaskNegativeProbability(logits: []const f32, targets: []const f32, label_indices: []const u32, head: ?*const BoundaryTaskHead) f64 {
    var total: f64 = 0;
    var count: usize = 0;
    for (logits, targets, label_indices) |raw_logit, target, label_idx| {
        if (target > 0.5) continue;
        total += sigmoid(calibratedTaskLogit(raw_logit, label_idx, head));
        count += 1;
    }
    return if (count == 0) 0 else total / @as(f64, @floatFromInt(count));
}

fn sigmoid(x: f32) f64 {
    const xd = @as(f64, x);
    return 1.0 / (1.0 + @exp(-xd));
}

fn binaryCrossEntropy(prob: f64, target: f32) f64 {
    const p = std.math.clamp(prob, 1e-6, 1.0 - 1e-6);
    const y = @as(f64, target);
    return -(y * @log(p) + (1.0 - y) * @log(1.0 - p));
}

fn dupeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    const out = try allocator.alloc([]const u8, values.len);
    errdefer allocator.free(out);
    for (values, 0..) |value, idx| out[idx] = try allocator.dupe(u8, value);
    return out;
}

fn isRegularFilePath(path: []const u8) bool {
    const cwd = compat.cwd();
    cwd.access(compat.io(), path, .{}) catch return false;
    return true;
}
