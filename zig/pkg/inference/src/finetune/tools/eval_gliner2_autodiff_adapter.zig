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
const ml = @import("ml");
const inference = @import("inference_internal");

const deberta_graph = inference.architectures.deberta_graph;
const native_compute_mod = inference.native_compute.native;
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const metal_compute = if (build_options.enable_metal) inference.native_compute.metal else struct {};
const gpu_hosted_store = inference.native_compute.gpu_hosted_store;
const metal_runtime = inference.metal_runtime;
const real_autodiff = inference.finetune.real_autodiff_trainer;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const gliner2_bundle = inference.finetune.gliner2;
const gliner2_data = inference.finetune.gliner2_data;
const weight_source_mod = inference.models.weight_source;
const safetensors = inference.models.safetensors;
const compat = inference.io.compat;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = inference.backends.Tensor;
const MetalWeightStore = if (build_options.enable_metal) gpu_hosted_store.WeightStore else void;

const mlx_mod = inference.backends.mlx;
const mlx = if (build_options.enable_mlx) mlx_mod else struct {};
const mlx_compute = if (build_options.enable_mlx) inference.native_compute.mlx else struct {};
const mlx_c = if (build_options.enable_mlx) mlx_mod.c else struct {};
const MlxWeightStore = if (build_options.enable_mlx) mlx_compute.WeightStore else void;
const MlxMap = if (build_options.enable_mlx) mlx_c.mlx_map_string_to_array else void;

pub const EvalBackend = enum {
    auto,
    metal,
    mlx,
    native,
};

const Manifest = struct {
    num_classes: usize,
    hidden_size: usize,
    lora_rank: usize,
    lora_alpha: f64,
    lora_targets: []const []const u8,
    entity_labels: []const []const u8,
    objective: gliner2_autodiff.GlinerObjective = .token,

    fn deinit(self: *Manifest, allocator: std.mem.Allocator) void {
        for (self.lora_targets) |item| allocator.free(item);
        allocator.free(self.lora_targets);
        for (self.entity_labels) |item| allocator.free(item);
        allocator.free(self.entity_labels);
        self.* = undefined;
    }
};

pub const EvalOptions = struct {
    model_dir: []const u8,
    adapter_dir: []const u8,
    text: []const u8,
    entity_types_csv: ?[]const u8 = null,
    seq_len: usize = 64,
    max_span_width: usize = 4,
    backend: EvalBackend = .native,
    compiled_required: bool = false,
    objective_override: ?gliner2_autodiff.GlinerObjective = null,
    expect_text: ?[]const u8 = null,
    expect_label: ?[]const u8 = null,
    min_score: ?f32 = null,
    prediction_threshold: ?f32 = null,
    label_thresholds: []const LabelThreshold = &.{},
    label_score_biases: []const LabelScoreBias = &.{},
    nms_overlap_threshold: ?f64 = null,
    max_predictions: ?usize = null,
    top_k_per_label: ?usize = null,
    best_span_per_label_start: bool = false,
    best_label_per_span_start: bool = false,
    require_entitylike_span: bool = false,
};

pub const LabelThreshold = struct {
    label: []const u8,
    threshold: f32,
};

pub const LabelScoreBias = struct {
    label: []const u8,
    bias: f32,
};

pub const TopEntity = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    score: f32,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var opts = parseArgs(init.minimal.args, allocator) catch |err| {
        if (err == error.HelpRequested) return;
        return err;
    };
    defer opts.deinit(allocator);

    var summary = try evalSavedAdapter(allocator, opts);
    defer summary.deinit(allocator);

    if (opts.value.min_score) |min_score| {
        if (!std.math.isFinite(min_score) or min_score < 0) return error.InvalidScoreThreshold;
    }
    if (opts.value.expect_text != null or opts.value.expect_label != null) {
        if (!hasExpectedPrediction(summary.predictions, opts.value.expect_text, opts.value.expect_label, opts.value.min_score)) {
            return error.SemanticGoldenPredictionMissing;
        }
    } else if (opts.value.min_score) |min_score| {
        if (summary.top.score < min_score) return error.SemanticGoldenScoreBelowThreshold;
    }

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary.jsonView(), .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

const OwnedOptions = struct {
    value: EvalOptions,
    owned_entity_types_csv: ?[]const u8 = null,
    owned_label_thresholds: []const LabelThreshold = &.{},
    owned_label_score_biases: []const LabelScoreBias = &.{},

    fn deinit(self: *OwnedOptions, allocator: std.mem.Allocator) void {
        if (self.owned_entity_types_csv) |value| allocator.free(value);
        for (self.owned_label_thresholds) |entry| allocator.free(entry.label);
        allocator.free(self.owned_label_thresholds);
        for (self.owned_label_score_biases) |entry| allocator.free(entry.label);
        allocator.free(self.owned_label_score_biases);
        self.* = undefined;
    }
};

fn parseArgs(args_in: std.process.Args, allocator: std.mem.Allocator) !OwnedOptions {
    var args = try std.process.Args.Iterator.initAllocator(args_in, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    if (std.mem.eql(u8, model_dir, "--help") or std.mem.eql(u8, model_dir, "-h")) {
        printUsage();
        return error.HelpRequested;
    }
    const adapter_dir = args.next() orelse return usageError();
    const text = args.next() orelse return usageError();
    var opts = EvalOptions{
        .model_dir = model_dir,
        .adapter_dir = adapter_dir,
        .text = text,
    };
    var owned_entity_types_csv: ?[]const u8 = null;
    var label_thresholds = std.ArrayListUnmanaged(LabelThreshold).empty;
    var label_score_biases = std.ArrayListUnmanaged(LabelScoreBias).empty;
    errdefer {
        for (label_thresholds.items) |entry| allocator.free(entry.label);
        label_thresholds.deinit(allocator);
        for (label_score_biases.items) |entry| allocator.free(entry.label);
        label_score_biases.deinit(allocator);
    }

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--entity-types")) {
            opts.entity_types_csv = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            opts.seq_len = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--max-span-width")) {
            opts.max_span_width = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = parseBackend(args.next() orelse return usageError()) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--compiled-required")) {
            opts.compiled_required = true;
        } else if (std.mem.eql(u8, arg, "--objective")) {
            opts.objective_override = try parseObjective(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--expect-text")) {
            opts.expect_text = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--expect-label")) {
            opts.expect_label = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--min-score")) {
            opts.min_score = try std.fmt.parseFloat(f32, args.next() orelse return usageError());
            if (opts.prediction_threshold == null) opts.prediction_threshold = opts.min_score;
        } else if (std.mem.eql(u8, arg, "--min-prediction-score")) {
            opts.prediction_threshold = try std.fmt.parseFloat(f32, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--label-thresholds")) {
            try parseLabelThresholdCsv(allocator, args.next() orelse return usageError(), &label_thresholds);
        } else if (std.mem.eql(u8, arg, "--label-score-biases")) {
            try parseLabelScoreBiasCsv(allocator, args.next() orelse return usageError(), &label_score_biases);
        } else if (std.mem.eql(u8, arg, "--nms-overlap")) {
            opts.nms_overlap_threshold = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--no-nms")) {
            opts.nms_overlap_threshold = null;
        } else if (std.mem.eql(u8, arg, "--max-predictions")) {
            opts.max_predictions = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--top-k-per-label")) {
            opts.top_k_per_label = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--best-span-per-label-start")) {
            opts.best_span_per_label_start = true;
        } else if (std.mem.eql(u8, arg, "--best-label-per-span-start")) {
            opts.best_label_per_span_start = true;
        } else if (std.mem.eql(u8, arg, "--require-entitylike-span")) {
            opts.require_entitylike_span = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return error.HelpRequested;
        } else if (opts.entity_types_csv == null) {
            owned_entity_types_csv = try allocator.dupe(u8, arg);
            opts.entity_types_csv = owned_entity_types_csv.?;
        } else {
            return usageError();
        }
    }

    opts.label_thresholds = try label_thresholds.toOwnedSlice(allocator);
    opts.label_score_biases = try label_score_biases.toOwnedSlice(allocator);
    return .{
        .value = opts,
        .owned_entity_types_csv = owned_entity_types_csv,
        .owned_label_thresholds = opts.label_thresholds,
        .owned_label_score_biases = opts.label_score_biases,
    };
}

fn hasExpectedPrediction(
    predictions: []const TopEntity,
    expect_text: ?[]const u8,
    expect_label: ?[]const u8,
    min_score: ?f32,
) bool {
    for (predictions) |prediction| {
        if (expect_text) |expected| {
            if (!std.mem.eql(u8, prediction.text, expected)) continue;
        }
        if (expect_label) |expected| {
            if (!std.mem.eql(u8, prediction.label, expected)) continue;
        }
        if (min_score) |threshold| {
            if (prediction.score < threshold) continue;
        }
        return true;
    }
    return false;
}

fn selectTopEntities(
    allocator: std.mem.Allocator,
    predictions: []const TopEntity,
    opts: EvalOptions,
) ![]TopEntity {
    var candidates = std.ArrayListUnmanaged(usize).empty;
    defer candidates.deinit(allocator);
    const fallback_threshold = opts.prediction_threshold orelse 0.0;
    for (predictions, 0..) |prediction, idx| {
        if (opts.require_entitylike_span and !isEntityLikeSpanText(prediction.text)) continue;
        if (prediction.score >= thresholdForLabel(opts.label_thresholds, prediction.label, fallback_threshold)) {
            try candidates.append(allocator, idx);
        }
    }
    std.mem.sort(usize, candidates.items, predictions, topEntityBetterThan);
    if (opts.nms_overlap_threshold == null and !opts.best_span_per_label_start and !opts.best_label_per_span_start and opts.top_k_per_label == null and opts.max_predictions == null) {
        return copyTopEntitiesByIndex(allocator, predictions, candidates.items);
    }

    var selected = std.ArrayListUnmanaged(usize).empty;
    defer selected.deinit(allocator);
    var per_label_counts = std.StringHashMapUnmanaged(usize){};
    defer per_label_counts.deinit(allocator);
    for (candidates.items) |candidate_idx| {
        const candidate = predictions[candidate_idx];
        if (opts.max_predictions) |max_predictions| {
            if (selected.items.len >= max_predictions) break;
        }
        if (opts.top_k_per_label) |top_k| {
            if ((per_label_counts.get(candidate.label) orelse 0) >= top_k) continue;
        }
        var suppressed = false;
        for (selected.items) |selected_idx| {
            const existing = predictions[selected_idx];
            if (opts.best_label_per_span_start and candidate.start == existing.start) {
                suppressed = true;
                break;
            }
            if (!std.mem.eql(u8, candidate.label, existing.label)) continue;
            if (opts.best_span_per_label_start and candidate.start == existing.start) {
                suppressed = true;
                break;
            }
            if (opts.nms_overlap_threshold) |overlap_threshold| {
                if (spanOverlapRatio(candidate.start, candidate.end, existing.start, existing.end) > overlap_threshold) {
                    suppressed = true;
                    break;
                }
            }
        }
        if (!suppressed) {
            try selected.append(allocator, candidate_idx);
            if (opts.top_k_per_label != null) {
                const prior = per_label_counts.get(candidate.label) orelse 0;
                try per_label_counts.put(allocator, candidate.label, prior + 1);
            }
        }
    }
    return copyTopEntitiesByIndex(allocator, predictions, selected.items);
}

pub fn isEntityLikeSpanText(text: []const u8) bool {
    const trimmed = std.mem.trim(u8, text, " \t\r\n\"'`.,;:!?()[]{}");
    if (trimmed.len == 0) return false;

    var first_token: ?[]const u8 = null;
    var last_token: []const u8 = trimmed;
    var token_count: usize = 0;
    var tokens = std.mem.tokenizeAny(u8, trimmed, " \t\r\n");
    while (tokens.next()) |raw_token| {
        const token = std.mem.trim(u8, raw_token, "\"'`.,;:!?()[]{}");
        if (token.len == 0) continue;
        if (first_token == null) first_token = token;
        last_token = token;
        token_count += 1;
    }
    if (token_count == 0) return false;

    const first = first_token.?;
    if (!tokenHasEntityBoundary(first)) return false;
    if (!tokenHasEntityBoundary(last_token)) return false;
    if (isLowercaseBoundaryStopword(first) or isLowercaseBoundaryStopword(last_token)) return false;
    return true;
}

fn tokenHasEntityBoundary(token: []const u8) bool {
    for (token) |ch| {
        if (std.ascii.isAlphabetic(ch)) return std.ascii.isUpper(ch);
        if (std.ascii.isDigit(ch)) return true;
    }
    return false;
}

fn isLowercaseBoundaryStopword(token: []const u8) bool {
    if (token.len == 0) return true;
    for (token) |ch| {
        if (std.ascii.isAlphabetic(ch) and std.ascii.isUpper(ch)) return false;
        if (std.ascii.isDigit(ch)) return false;
    }
    const words = [_][]const u8{
        "a", "an", "and", "at", "by", "for", "from", "in", "into", "of", "on", "or", "the", "to", "with",
    };
    for (words) |word| {
        if (std.ascii.eqlIgnoreCase(token, word)) return true;
    }
    return false;
}

fn thresholdForLabel(label_thresholds: []const LabelThreshold, label: []const u8, fallback: f32) f32 {
    for (label_thresholds) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return entry.threshold;
    }
    return fallback;
}

fn minDecodeThreshold(opts: EvalOptions) ?f32 {
    var threshold = opts.prediction_threshold;
    for (opts.label_thresholds) |entry| {
        threshold = if (threshold) |existing| @min(existing, entry.threshold) else entry.threshold;
    }
    for (opts.label_score_biases) |entry| {
        if (entry.bias <= 0.0) continue;
        threshold = if (threshold) |existing| @min(existing, scoreWithBias(existing, -entry.bias)) else null;
    }
    return threshold;
}

fn copyTopEntitiesByIndex(
    allocator: std.mem.Allocator,
    predictions: []const TopEntity,
    indices: []const usize,
) ![]TopEntity {
    var out = try allocator.alloc(TopEntity, indices.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |prediction| {
            allocator.free(prediction.text);
            allocator.free(prediction.label);
        }
        allocator.free(out);
    }
    for (indices, 0..) |idx, out_idx| {
        const prediction = predictions[idx];
        out[out_idx] = .{
            .text = try allocator.dupe(u8, prediction.text),
            .label = try allocator.dupe(u8, prediction.label),
            .start = prediction.start,
            .end = prediction.end,
            .score = prediction.score,
        };
        initialized += 1;
    }
    return out;
}

fn topEntityBetterThan(predictions: []const TopEntity, lhs_idx: usize, rhs_idx: usize) bool {
    const lhs = predictions[lhs_idx];
    const rhs = predictions[rhs_idx];
    if (lhs.score != rhs.score) return lhs.score > rhs.score;
    const lhs_len = lhs.end - lhs.start;
    const rhs_len = rhs.end - rhs.start;
    if (lhs_len != rhs_len) return lhs_len < rhs_len;
    if (lhs.start != rhs.start) return lhs.start < rhs.start;
    return lhs.end < rhs.end;
}

fn spanOverlapRatio(a_start: usize, a_end: usize, b_start: usize, b_end: usize) f64 {
    if (a_end <= a_start or b_end <= b_start) return 0.0;
    const start = @max(a_start, b_start);
    const end = @min(a_end, b_end);
    if (end <= start) return 0.0;
    const intersection = end - start;
    const union_len = @max(a_end, b_end) - @min(a_start, b_start);
    if (union_len == 0) return 0.0;
    return @as(f64, @floatFromInt(intersection)) / @as(f64, @floatFromInt(union_len));
}

pub const EvalSummary = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    adapter_dir: []const u8,
    text: []const u8,
    entity_types: []const []const u8,
    seq_len: usize,
    num_classes: usize,
    objective: gliner2_autodiff.GlinerObjective,
    backend: EvalBackend,
    lora_rank: usize,
    lora_alpha: f64,
    loaded_base_weight_count: usize,
    top: TopEntity,
    predictions: []TopEntity,

    pub fn deinit(self: *EvalSummary, allocator: std.mem.Allocator) void {
        allocator.free(self.model_dir);
        allocator.free(self.adapter_dir);
        allocator.free(self.text);
        for (self.entity_types) |item| allocator.free(item);
        allocator.free(self.entity_types);
        allocator.free(self.top.text);
        allocator.free(self.top.label);
        for (self.predictions) |prediction| {
            allocator.free(prediction.text);
            allocator.free(prediction.label);
        }
        allocator.free(self.predictions);
        self.* = undefined;
    }

    fn jsonView(self: *const EvalSummary) struct {
        model_dir: []const u8,
        adapter_dir: []const u8,
        text: []const u8,
        entity_types: []const []const u8,
        seq_len: usize,
        num_classes: usize,
        objective: []const u8,
        backend: []const u8,
        lora_rank: usize,
        lora_alpha: f64,
        loaded_base_weight_count: usize,
        top_entity: TopEntity,
        predictions: []const TopEntity,
    } {
        return .{
            .model_dir = self.model_dir,
            .adapter_dir = self.adapter_dir,
            .text = self.text,
            .entity_types = self.entity_types,
            .seq_len = self.seq_len,
            .num_classes = self.num_classes,
            .objective = objectiveName(self.objective),
            .backend = backendLabel(self.backend),
            .lora_rank = self.lora_rank,
            .lora_alpha = self.lora_alpha,
            .loaded_base_weight_count = self.loaded_base_weight_count,
            .top_entity = self.top,
            .predictions = self.predictions,
        };
    }
};

pub fn evalSavedAdapterText(allocator: std.mem.Allocator, opts: EvalOptions) !EvalSummary {
    return evalSavedAdapter(allocator, .{ .value = opts });
}

fn evalSavedAdapter(allocator: std.mem.Allocator, owned_opts: OwnedOptions) !EvalSummary {
    const opts = owned_opts.value;
    if (opts.seq_len == 0 or opts.seq_len > 4096) return error.InvalidSeqLen;
    if (opts.max_span_width == 0) return error.InvalidMaxSpanWidth;

    var manifest = try loadManifest(allocator, opts.adapter_dir);
    defer manifest.deinit(allocator);
    const graph_config = try loadDebertaGraphConfig(allocator, opts.model_dir);
    if (manifest.hidden_size != graph_config.hidden_size) return error.HiddenSizeMismatch;
    const objective = opts.objective_override orelse manifest.objective;

    const entity_types = if (opts.entity_types_csv) |csv|
        try parseCsv(allocator, csv)
    else
        try dupeStringSlice(allocator, manifest.entity_labels);
    errdefer freeStringSlice(allocator, entity_types);
    if (entity_types.len == 0 or entity_types.len + 1 > manifest.num_classes) return error.InvalidEntityTypes;
    if (objective == .span_start and entity_types.len + 1 != manifest.num_classes) return error.InvalidEntityTypes;
    try validateLabelThresholds(opts.label_thresholds, entity_types);
    try validateLabelScoreBiases(opts.label_score_biases, entity_types);

    const task_head_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.task_head_checkpoint_file_name });
    defer allocator.free(task_head_path);
    var task_head = try gliner2_bundle.loadClassifierTaskHead(allocator, task_head_path);
    defer task_head.deinit();
    if (task_head.num_classes != manifest.num_classes or task_head.hidden_size != manifest.hidden_size) return error.TaskHeadShapeMismatch;

    const safetensors_path = try std.fs.path.join(allocator, &.{ opts.model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);

    var loaded_base_weight_count: usize = 0;
    var native_weight_store: WeightStore = undefined;
    var native_owned_names = std.ArrayListUnmanaged([]const u8).empty;
    var native_backend: NativeCompute = undefined;
    var metal_weight_store: MetalWeightStore = undefined;
    var metal_backend: if (build_options.enable_metal) metal_compute.MetalCompute else void = undefined;
    var mlx_weight_store: MlxWeightStore = undefined;
    var mlx_backend: if (build_options.enable_mlx) *mlx_compute.MlxCompute else void = undefined;

    const force_native = envFlag("TERMITE_GLINER2_FORCE_NATIVE");
    const metal_available = if (comptime build_options.enable_metal)
        (!force_native and metal_runtime.metalDeviceAvailable())
    else
        false;
    const mlx_available = if (comptime build_options.enable_mlx)
        (!force_native and (mlx.metalDeviceAvailable() or mlx.allowCpuStreamWithoutMetal()))
    else
        false;
    const selected_backend = try selectBackend(opts.backend, force_native, metal_available, mlx_available);

    var cb = if (selected_backend == .metal) blk: {
        if (comptime build_options.enable_metal) {
            metal_weight_store = .{
                .allocator = allocator,
                .resident_weights = if (comptime build_options.enable_mlx) mlx_c.mlx_map_string_to_array_new() else {},
                .stream = if (comptime build_options.enable_mlx) mlx.openDefaultStream().stream else {},
                .prefix = "",
                .lazy_weights = .{},
            };
            errdefer deinitGpuHostedWeightStore(allocator, &metal_weight_store);
            loaded_base_weight_count = try loadSafetensorsIntoGpuHostedStore(allocator, &metal_weight_store, safetensors_path);
            try addTaskHeadWeightsToGpuHostedStore(allocator, &metal_weight_store, &task_head);
            metal_compute.initPrefetchQueue(&metal_weight_store, allocator);
            metal_backend = try metal_compute.MetalCompute.init(allocator, &metal_weight_store, null);
            break :blk metal_backend.computeBackend();
        } else unreachable;
    } else if (selected_backend == .mlx) blk: {
        if (comptime build_options.enable_mlx) {
            const resident_weights = try loadSafetensorsIntoMlxStore(allocator, safetensors_path, &task_head);
            mlx_weight_store = .{
                .allocator = allocator,
                .resident_weights = resident_weights.map,
                .stream = mlx.openDefaultStream().stream,
                .prefix = "",
                .lazy_weights = .{},
            };
            errdefer deinitMlxWeightStore(allocator, &mlx_weight_store);
            loaded_base_weight_count = resident_weights.loaded_count;
            mlx_backend = try allocator.create(mlx_compute.MlxCompute);
            errdefer allocator.destroy(mlx_backend);
            mlx_backend.* = try mlx_compute.MlxCompute.init(allocator, &mlx_weight_store, null);
            break :blk mlx_backend.computeBackend();
        } else unreachable;
    } else blk: {
        native_weight_store = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        };
        errdefer deinitNativeWeightStore(allocator, &native_weight_store, &native_owned_names);
        const source_ptr = try SafetensorsSource.initAbsolute(allocator, safetensors_path);
        var ws = source_ptr.weightSource();
        defer ws.deinit();
        loaded_base_weight_count = try loadBaseWeightsFromSource(allocator, &ws, &native_weight_store, &native_owned_names);
        try addTaskHeadWeights(allocator, &native_weight_store, &native_owned_names, &task_head);
        native_backend = NativeCompute.init(allocator, &native_weight_store, null);
        break :blk native_backend.computeBackend();
    };
    defer switch (selected_backend) {
        .native => deinitNativeWeightStore(allocator, &native_weight_store, &native_owned_names),
        .metal => if (comptime build_options.enable_metal) deinitGpuHostedWeightStore(allocator, &metal_weight_store),
        .mlx => if (comptime build_options.enable_mlx) deinitMlxWeightStore(allocator, &mlx_weight_store),
        .auto => {},
    };
    defer switch (selected_backend) {
        .metal => if (comptime build_options.enable_metal) metal_backend.deinit(),
        .mlx => if (comptime build_options.enable_mlx) {
            mlx_backend.deinit();
            allocator.destroy(mlx_backend);
        },
        else => {},
    };

    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = @intCast(manifest.num_classes),
        .objective = objective,
    });

    const regular_trainable_params = [_][]const u8{ "classifier.weight", "classifier.bias" };
    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = .{
                .rank = @intCast(manifest.lora_rank),
                .alpha = @floatCast(manifest.lora_alpha),
                .target_patterns = manifest.lora_targets,
            },
            .lr_schedule = .{ .constant = 1e-3 },
            .max_grad_norm = 1.0,
            .grad_accum_steps = 1,
            .lora_a_init_std = 0.02,
            .hidden_size_hint = graph_config.hidden_size,
            .num_layers_hint = graph_config.num_hidden_layers,
            .seed = 42,
            .regular_trainable_params = &regular_trainable_params,
            .execution_engine = switch (selected_backend) {
                .metal => .compiled_metal,
                .mlx => .compiled_mlx,
                else => .interpreter,
            },
            .compiled_required = opts.compiled_required,
        },
    );
    defer trainer.deinit();

    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, opts.model_dir);
    defer tokenizer.deinit(allocator);

    try ensureGraphBuilt(allocator, &tokenizer, entity_types, opts.text, manifest.num_classes, opts.seq_len, opts.max_span_width, objective, &trainer, &gliner_ctx);

    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);
    try loadPeftAdaptersIntoTrainer(allocator, adapter_checkpoint_path, &trainer);
    try loadTaskHeadIntoTrainer(&task_head, &trainer);

    const predictions = try decodeTextEntities(
        allocator,
        &tokenizer,
        entity_types,
        opts.text,
        manifest.num_classes,
        objective,
        opts.seq_len,
        opts.max_span_width,
        &trainer,
        &gliner_ctx,
        minDecodeThreshold(opts),
        opts.label_score_biases,
    );
    errdefer freeTopEntities(allocator, predictions);
    if (predictions.len == 0) return error.NoEntityPredictions;
    const selected_predictions = try selectTopEntities(allocator, predictions, opts);
    errdefer freeTopEntities(allocator, selected_predictions);
    freeTopEntities(allocator, predictions);
    if (selected_predictions.len == 0) return error.NoEntityPredictions;

    return .{
        .allocator = allocator,
        .model_dir = try allocator.dupe(u8, opts.model_dir),
        .adapter_dir = try allocator.dupe(u8, opts.adapter_dir),
        .text = try allocator.dupe(u8, opts.text),
        .entity_types = entity_types,
        .seq_len = opts.seq_len,
        .num_classes = manifest.num_classes,
        .objective = objective,
        .backend = selected_backend,
        .lora_rank = manifest.lora_rank,
        .lora_alpha = manifest.lora_alpha,
        .loaded_base_weight_count = loaded_base_weight_count,
        .top = .{
            .text = try allocator.dupe(u8, selected_predictions[0].text),
            .label = try allocator.dupe(u8, selected_predictions[0].label),
            .start = selected_predictions[0].start,
            .end = selected_predictions[0].end,
            .score = selected_predictions[0].score,
        },
        .predictions = selected_predictions,
    };
}

fn loadManifest(allocator: std.mem.Allocator, adapter_dir: []const u8) !Manifest {
    const manifest_path = try std.fs.path.join(allocator, &.{ adapter_dir, "training_manifest.json" });
    defer allocator.free(manifest_path);
    const data = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(data);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTrainingManifest;
    const obj = parsed.value.object;
    return .{
        .num_classes = jsonUsize(obj.get("num_classes")) orelse return error.InvalidTrainingManifest,
        .hidden_size = jsonUsize(obj.get("hidden_size")) orelse return error.InvalidTrainingManifest,
        .lora_rank = jsonUsize(obj.get("lora_rank")) orelse return error.InvalidTrainingManifest,
        .lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidTrainingManifest,
        .lora_targets = try parseCsv(allocator, jsonString(obj.get("lora_targets")) orelse return error.InvalidTrainingManifest),
        .entity_labels = try parseStringArray(allocator, obj.get("entity_labels") orelse return error.InvalidTrainingManifest),
        .objective = try parseObjective(jsonString(obj.get("objective")) orelse "token"),
    };
}

fn loadBaseWeightsFromSource(
    allocator: std.mem.Allocator,
    ws: anytype,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
) !usize {
    const hf_names = try ws.listNames(allocator);
    defer allocator.free(hf_names);

    var loaded_count: usize = 0;
    for (hf_names) |hf_name| {
        var lw = try ws.getTensor(hf_name);
        const stripped = stripEncoderPrefix(hf_name);
        const owned_name = try allocator.dupe(u8, stripped);
        try owned_names.append(allocator, owned_name);
        lw.tensor.name = owned_name;
        try weight_store.resident_weights.put(allocator, owned_name, lw);
        loaded_count += 1;
    }
    return loaded_count;
}

fn addTaskHeadWeights(
    allocator: std.mem.Allocator,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
    head: *const gliner2_bundle.ClassifierTaskHead,
) !void {
    {
        const name = try allocator.dupe(u8, "classifier.weight");
        try owned_names.append(allocator, name);
        const shape = [_]i64{ @intCast(head.num_classes), @intCast(head.hidden_size) };
        const tensor = try Tensor.initFloat32(allocator, name, &shape, head.weight);
        try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
    }
    {
        const name = try allocator.dupe(u8, "classifier.bias");
        try owned_names.append(allocator, name);
        const shape = [_]i64{@intCast(head.num_classes)};
        const tensor = try Tensor.initFloat32(allocator, name, &shape, head.bias);
        try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
    }
}

fn deinitNativeWeightStore(
    allocator: std.mem.Allocator,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
) void {
    var it = weight_store.resident_weights.iterator();
    while (it.next()) |entry| entry.value_ptr.deinit();
    weight_store.resident_weights.deinit(allocator);
    for (owned_names.items) |name| allocator.free(name);
    owned_names.deinit(allocator);
}

fn loadSafetensorsIntoGpuHostedStore(
    allocator: std.mem.Allocator,
    weight_store: *MetalWeightStore,
    safetensors_path: []const u8,
) !usize {
    if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
    var source = try SafetensorsSource.initAbsolute(allocator, safetensors_path);
    errdefer source.weightSource().deinit();
    const ws = source.weightSource();
    const names = try ws.listNames(allocator);
    defer allocator.free(names);

    var loaded_count: usize = 0;
    for (names) |name| {
        var loaded = ws.getTensor(name) catch continue;
        defer loaded.deinit();
        var owned_loaded = try cloneLoadedWeight(allocator, loaded);
        errdefer owned_loaded.deinit();
        const stripped = stripEncoderPrefix(name);
        const owned_name = try allocator.dupe(u8, stripped);
        errdefer allocator.free(owned_name);
        try weight_store.lazy_weights.put(allocator, owned_name, .{
            .tensor_ref = undefined,
            .host_loaded = owned_loaded,
            .active_tier = .host,
            .loaded_bytes = owned_loaded.tensor.data.len,
        });
        loaded_count += 1;
    }
    source.weightSource().deinit();
    return loaded_count;
}

fn cloneLoadedWeight(allocator: std.mem.Allocator, loaded: LoadedWeight) !LoadedWeight {
    if (loaded.quantized or loaded.quantized_storage != null) return error.UnsupportedQuantizedEvalWeight;
    const owned_data = try allocator.dupe(u8, loaded.tensor.data);
    errdefer allocator.free(owned_data);
    const owned_shape = try allocator.dupe(i64, loaded.tensor.shape);
    errdefer allocator.free(owned_shape);
    return .{
        .tensor = .{
            .data = owned_data,
            .dtype = loaded.tensor.dtype,
            .shape = owned_shape,
            .name = "",
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        },
        .quantized = false,
    };
}

fn addTaskHeadWeightsToGpuHostedStore(
    allocator: std.mem.Allocator,
    weight_store: *MetalWeightStore,
    head: *const gliner2_bundle.ClassifierTaskHead,
) !void {
    if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
    {
        const shape = [_]i64{ @intCast(head.num_classes), @intCast(head.hidden_size) };
        const tensor = try Tensor.initFloat32(allocator, "classifier.weight", &shape, head.weight);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "classifier.weight"), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
    {
        const shape = [_]i64{@intCast(head.num_classes)};
        const tensor = try Tensor.initFloat32(allocator, "classifier.bias", &shape, head.bias);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "classifier.bias"), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
}

fn deinitGpuHostedWeightStore(allocator: std.mem.Allocator, weight_store: *MetalWeightStore) void {
    if (comptime !build_options.enable_metal) return;
    metal_compute.deinitPrefetchQueue(weight_store);
    var it = weight_store.lazy_weights.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        if (entry.value_ptr.host_loaded) |*loaded| loaded.deinit();
        if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
    }
    weight_store.lazy_weights.deinit(allocator);
    if (comptime build_options.enable_mlx) {
        _ = mlx_c.mlx_map_string_to_array_free(weight_store.resident_weights);
    }
}

const LoadedMlxWeights = if (build_options.enable_mlx) struct {
    map: MlxMap,
    loaded_count: usize,
} else struct {};

fn loadSafetensorsIntoMlxStore(
    allocator: std.mem.Allocator,
    safetensors_path: []const u8,
    head: *const gliner2_bundle.ClassifierTaskHead,
) !LoadedMlxWeights {
    if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
    const raw_weights = try mlx.loadSafetensors(safetensors_path, allocator, mlx.openDefaultStream().stream);
    errdefer _ = mlx_c.mlx_map_string_to_array_free(raw_weights);
    const stripped_weights = mlx_c.mlx_map_string_to_array_new();
    errdefer _ = mlx_c.mlx_map_string_to_array_free(stripped_weights);
    const it = mlx_c.mlx_map_string_to_array_iterator_new(raw_weights);
    defer _ = mlx_c.mlx_map_string_to_array_iterator_free(it);
    var loaded_count: usize = 0;
    while (true) {
        var key: [*c]const u8 = null;
        var val = mlx_c.mlx_array_new();
        if (mlx_c.mlx_map_string_to_array_iterator_next(&key, &val, it) != 0) {
            _ = mlx_c.mlx_array_free(val);
            break;
        }
        if (key == null) {
            _ = mlx_c.mlx_array_free(val);
            break;
        }
        const name = std.mem.span(key);
        const stripped_z = try allocator.dupeSentinel(u8, stripEncoderPrefix(name), 0);
        defer allocator.free(stripped_z);
        _ = mlx_c.mlx_map_string_to_array_insert(stripped_weights, stripped_z.ptr, val);
        _ = mlx_c.mlx_array_free(val);
        loaded_count += 1;
    }
    _ = mlx_c.mlx_map_string_to_array_free(raw_weights);
    try addTaskHeadWeightsToMlxStore(allocator, stripped_weights, head);
    return .{ .map = stripped_weights, .loaded_count = loaded_count };
}

fn addTaskHeadWeightsToMlxStore(
    allocator: std.mem.Allocator,
    weights: MlxMap,
    head: *const gliner2_bundle.ClassifierTaskHead,
) !void {
    if (comptime !build_options.enable_mlx) return error.MlxBackendUnavailable;
    const weight_shape = [_]i32{ @intCast(head.num_classes), @intCast(head.hidden_size) };
    const weight_arr = mlx.arrayFromFloat32(head.weight, &weight_shape);
    try mlx.insertWeight(weights, allocator, "classifier.weight", weight_arr);
    const bias_shape = [_]i32{@intCast(head.num_classes)};
    const bias_arr = mlx.arrayFromFloat32(head.bias, &bias_shape);
    try mlx.insertWeight(weights, allocator, "classifier.bias", bias_arr);
}

fn deinitMlxWeightStore(allocator: std.mem.Allocator, weight_store: *MlxWeightStore) void {
    if (comptime !build_options.enable_mlx) return;
    _ = mlx_c.mlx_map_string_to_array_free(weight_store.resident_weights);
    var it = weight_store.lazy_weights.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        if (entry.value_ptr.host_loaded) |*loaded| loaded.deinit();
        if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
    }
    weight_store.lazy_weights.deinit(allocator);
}

const DebertaJsonConfig = struct {
    vocab_size: u32 = 128100,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 3072,
    max_position_embeddings: u32 = 512,
    position_buckets: u32 = 256,
};

fn loadDebertaGraphConfig(allocator: std.mem.Allocator, model_dir: []const u8) !deberta_graph.Config {
    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "encoder_config", "config.json" });
    defer allocator.free(config_path);
    const bytes = compat.cwd().readFileAlloc(compat.io(), config_path, allocator, .limited(8 * 1024 * 1024)) catch |err| switch (err) {
        error.FileNotFound => blk: {
            const fallback_path = try std.fs.path.join(allocator, &.{ model_dir, "config.json" });
            defer allocator.free(fallback_path);
            break :blk try compat.cwd().readFileAlloc(compat.io(), fallback_path, allocator, .limited(8 * 1024 * 1024));
        },
        else => return err,
    };
    defer allocator.free(bytes);
    const config = try parseDebertaConfig(allocator, bytes);
    return .{
        .vocab_size = config.vocab_size,
        .hidden_size = config.hidden_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .intermediate_size = config.intermediate_size,
        .max_position_embeddings = config.max_position_embeddings,
        .position_buckets = config.position_buckets,
    };
}

fn parseDebertaConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !DebertaJsonConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var config = DebertaJsonConfig{};
    if (obj.get("vocab_size")) |v| config.vocab_size = jsonU32(v) orelse config.vocab_size;
    if (obj.get("hidden_size")) |v| config.hidden_size = jsonU32(v) orelse config.hidden_size;
    if (obj.get("num_hidden_layers")) |v| config.num_hidden_layers = jsonU32(v) orelse config.num_hidden_layers;
    if (obj.get("num_attention_heads")) |v| config.num_attention_heads = jsonU32(v) orelse config.num_attention_heads;
    if (obj.get("intermediate_size")) |v| config.intermediate_size = jsonU32(v) orelse config.intermediate_size;
    if (obj.get("max_position_embeddings")) |v| config.max_position_embeddings = jsonU32(v) orelse config.max_position_embeddings;
    if (obj.get("position_buckets")) |v| config.position_buckets = jsonU32(v) orelse config.position_buckets;
    return config;
}

fn ensureGraphBuilt(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    seq_len: usize,
    max_span_width: usize,
    objective: gliner2_autodiff.GlinerObjective,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !void {
    switch (objective) {
        .token => {
            const input_ids = try allocator.alloc(i64, seq_len);
            defer allocator.free(input_ids);
            const attention_mask = try allocator.alloc(f32, seq_len);
            defer allocator.free(attention_mask);
            const targets = try allocator.alloc(f32, seq_len * num_classes);
            defer allocator.free(targets);
            try fillInferenceBuffers(allocator, tokenizer, entity_types, text, num_classes, input_ids, attention_mask, targets);
            const trainer_input = gliner2_autodiff.makeTrainerInput(
                gliner_ctx,
                input_ids,
                attention_mask,
                targets,
                gliner2_autodiff.tokenTargetsShape(1, @intCast(seq_len), @intCast(num_classes)),
                1,
                @intCast(seq_len),
            );
            try trainer.ensureGraphBuilt(trainer_input);
        },
        .span_start => {
            const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
            var encoded = try gliner2_data.buildSimpleBatch(allocator, tokenizer, &examples, entity_types, seq_len, max_span_width, 1);
            defer encoded.deinit();
            const input_ids = try allocator.alloc(i64, seq_len);
            defer allocator.free(input_ids);
            const attention_mask = try allocator.alloc(f32, seq_len);
            defer allocator.free(attention_mask);
            try copyEncodedInputs(&encoded, input_ids, attention_mask);
            const target_width = gliner2_autodiff.spanStartTargetWidth(encoded.num_entity_types);
            const target_len = encoded.batch_size * encoded.max_spans * target_width;
            const targets = try allocator.alloc(f32, target_len);
            defer allocator.free(targets);
            _ = try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatch(&encoded, targets);
            const trainer_input = gliner2_autodiff.makeTrainerInput(
                gliner_ctx,
                input_ids,
                attention_mask,
                targets,
                gliner2_autodiff.spanStartTargetsShape(1, @intCast(encoded.max_spans), @intCast(encoded.num_entity_types)),
                1,
                @intCast(seq_len),
            );
            try trainer.ensureGraphBuilt(trainer_input);
        },
    }
}

fn decodeTextEntities(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    objective: gliner2_autodiff.GlinerObjective,
    seq_len: usize,
    max_span_width: usize,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
    prediction_threshold: ?f32,
    label_score_biases: []const LabelScoreBias,
) ![]TopEntity {
    return switch (objective) {
        .token => decodeTextEntitiesFromTokenBridge(
            allocator,
            tokenizer,
            entity_types,
            text,
            num_classes,
            seq_len,
            max_span_width,
            trainer,
            gliner_ctx,
            prediction_threshold,
            label_score_biases,
        ),
        .span_start => decodeTextEntitiesFromSpanStart(
            allocator,
            tokenizer,
            entity_types,
            text,
            seq_len,
            max_span_width,
            trainer,
            gliner_ctx,
            prediction_threshold,
            label_score_biases,
        ),
    };
}

fn decodeTextEntitiesFromTokenBridge(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    seq_len: usize,
    max_span_width: usize,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
    prediction_threshold: ?f32,
    label_score_biases: []const LabelScoreBias,
) ![]TopEntity {
    const input_ids = try allocator.alloc(i64, seq_len);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(f32, seq_len);
    defer allocator.free(attention_mask);
    const targets = try allocator.alloc(f32, seq_len * num_classes);
    defer allocator.free(targets);
    try fillInferenceBuffers(allocator, tokenizer, entity_types, text, num_classes, input_ids, attention_mask, targets);

    const logits = try gliner2_autodiff.tokenLogitsForBatch(
        allocator,
        trainer,
        gliner_ctx,
        input_ids,
        attention_mask,
        1,
        @intCast(seq_len),
    );
    defer allocator.free(logits);
    if (logits.len != seq_len * num_classes) return error.LogitShapeMismatch;
    for (logits) |value| if (!std.math.isFinite(value)) return error.NonFiniteLogit;

    const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
    var decoded_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        tokenizer,
        &examples,
        entity_types,
        seq_len,
        max_span_width,
        1,
    );
    defer decoded_batch.deinit();
    const span_scores = try gliner2_data.tokenLogitsToSpanScoresAlloc(allocator, &decoded_batch, logits, num_classes);
    defer allocator.free(span_scores);
    applyLabelScoreBiases(span_scores, decoded_batch.num_entity_types, entity_types, label_score_biases);

    var max_span_score: f32 = 0.0;
    for (span_scores) |score| {
        if (!std.math.isFinite(score)) return error.NonFiniteSpanScore;
        max_span_score = @max(max_span_score, score);
    }
    if (max_span_score <= 0.0) return error.NoPositiveSpanScores;

    const threshold = prediction_threshold orelse max_span_score - 1e-6;
    const predictions = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &decoded_batch,
        &examples,
        entity_types,
        span_scores,
        threshold,
    );
    defer allocator.free(predictions);
    if (predictions.len == 0) return error.NoEntityPredictions;
    return copyPredictions(allocator, predictions);
}

fn decodeTextEntitiesFromSpanStart(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    seq_len: usize,
    max_span_width: usize,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
    prediction_threshold: ?f32,
    label_score_biases: []const LabelScoreBias,
) ![]TopEntity {
    const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
    var decoded_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        tokenizer,
        &examples,
        entity_types,
        seq_len,
        max_span_width,
        1,
    );
    defer decoded_batch.deinit();

    const input_ids = try allocator.alloc(i64, seq_len);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(f32, seq_len);
    defer allocator.free(attention_mask);
    try copyEncodedInputs(&decoded_batch, input_ids, attention_mask);

    const target_width = gliner2_autodiff.spanStartTargetWidth(decoded_batch.num_entity_types);
    const target_len = decoded_batch.batch_size * decoded_batch.max_spans * target_width;
    const targets = try allocator.alloc(f32, target_len);
    defer allocator.free(targets);
    _ = try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatch(&decoded_batch, targets);

    const logits = try gliner2_autodiff.spanStartLogitsForBatch(
        allocator,
        trainer,
        gliner_ctx,
        input_ids,
        attention_mask,
        targets,
        gliner2_autodiff.spanStartTargetsShape(1, @intCast(decoded_batch.max_spans), @intCast(decoded_batch.num_entity_types)),
        1,
        @intCast(seq_len),
    );
    defer allocator.free(logits);

    const expected_scores = decoded_batch.batch_size * decoded_batch.max_spans * decoded_batch.num_entity_types;
    if (logits.len != expected_scores) return error.LogitShapeMismatch;
    const span_scores = try allocator.alloc(f32, expected_scores);
    defer allocator.free(span_scores);
    var max_span_score: f32 = 0.0;
    for (0..decoded_batch.batch_size * decoded_batch.max_spans) |span_idx| {
        const valid = decoded_batch.span_mask[span_idx] > 0.0;
        for (0..decoded_batch.num_entity_types) |entity_idx| {
            const idx = span_idx * decoded_batch.num_entity_types + entity_idx;
            const logit = logits[idx];
            if (!std.math.isFinite(logit)) return error.NonFiniteLogit;
            const score = if (valid) scoreWithBias(sigmoid(logit), labelScoreBias(entity_types, entity_idx, label_score_biases)) else 0.0;
            if (!std.math.isFinite(score)) return error.NonFiniteSpanScore;
            span_scores[idx] = score;
            if (valid) max_span_score = @max(max_span_score, score);
        }
    }
    if (max_span_score <= 0.0) return error.NoPositiveSpanScores;

    const threshold = prediction_threshold orelse max_span_score - 1e-6;
    const predictions = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &decoded_batch,
        &examples,
        entity_types,
        span_scores,
        threshold,
    );
    defer allocator.free(predictions);
    if (predictions.len == 0) return error.NoEntityPredictions;
    return copyPredictions(allocator, predictions);
}

fn applyLabelScoreBiases(
    span_scores: []f32,
    num_entity_types: usize,
    entity_types: []const []const u8,
    label_score_biases: []const LabelScoreBias,
) void {
    if (label_score_biases.len == 0) return;
    for (span_scores, 0..) |*score, idx| {
        const entity_idx = idx % num_entity_types;
        score.* = scoreWithBias(score.*, labelScoreBias(entity_types, entity_idx, label_score_biases));
    }
}

fn labelScoreBias(entity_types: []const []const u8, entity_idx: usize, label_score_biases: []const LabelScoreBias) f32 {
    if (entity_idx >= entity_types.len) return 0.0;
    const label = entity_types[entity_idx];
    for (label_score_biases) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return entry.bias;
    }
    return 0.0;
}

fn scoreWithBias(score: f32, bias: f32) f32 {
    if (bias == 0.0 or score <= 0.0 or score >= 1.0) return score;
    const logit = @log(score / (1.0 - score)) + bias;
    return sigmoid(logit);
}

fn copyPredictions(allocator: std.mem.Allocator, predictions: []const gliner2_data.EntityPrediction) ![]TopEntity {
    var out = try allocator.alloc(TopEntity, predictions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |prediction| {
            allocator.free(prediction.text);
            allocator.free(prediction.label);
        }
        allocator.free(out);
    }
    for (predictions, 0..) |prediction, idx| {
        out[idx] = .{
            .text = try allocator.dupe(u8, prediction.text),
            .label = try allocator.dupe(u8, prediction.label),
            .start = prediction.start,
            .end = prediction.end,
            .score = prediction.score,
        };
        initialized += 1;
    }
    return out;
}

fn freeTopEntities(allocator: std.mem.Allocator, predictions: []TopEntity) void {
    for (predictions) |prediction| {
        allocator.free(prediction.text);
        allocator.free(prediction.label);
    }
    allocator.free(predictions);
}

fn fillInferenceBuffers(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
) !void {
    const seq_len = input_ids.len;
    if (attention_mask.len != seq_len or targets.len != seq_len * num_classes) return error.InvalidInputShape;
    var tok_ids_buf: [4096]i32 = undefined;
    var tok_mask_buf: [4096]i32 = undefined;
    var words_mask_buf: [4096]i32 = undefined;
    var first_pos_buf: [4096]i32 = undefined;
    var e_tok_pos_buf: [128]i32 = undefined;
    var e_tok_end_buf: [128]i32 = undefined;
    if (seq_len > tok_ids_buf.len or entity_types.len > e_tok_pos_buf.len) return error.InvalidInputShape;

    const tok_ids = tok_ids_buf[0..seq_len];
    const tok_mask = tok_mask_buf[0..seq_len];
    const words_mask = words_mask_buf[0..seq_len];
    const first_pos = first_pos_buf[0..seq_len];
    const e_pos = e_tok_pos_buf[0..entity_types.len];
    const e_end = e_tok_end_buf[0..entity_types.len];
    _ = tokenizer.encodeInto(allocator, text, entity_types, tok_ids, tok_mask, words_mask, first_pos, e_pos, e_end);

    @memset(targets, 0.0);
    for (0..seq_len) |idx| {
        input_ids[idx] = tok_ids[idx];
        attention_mask[idx] = @floatFromInt(tok_mask[idx]);
        const row = idx * num_classes;
        if (tok_mask[idx] != 0) targets[row] = 1.0;
    }
}

fn copyEncodedInputs(
    batch: *const gliner2_data.EncodedBatch,
    input_ids: []i64,
    attention_mask: []f32,
) !void {
    const expected = batch.batch_size * batch.max_length;
    if (batch.batch_size != 1 or input_ids.len != expected or attention_mask.len != expected) return error.InvalidInputShape;
    if (batch.input_ids.len != expected or batch.attention_mask.len != expected) return error.InvalidInputShape;
    for (0..expected) |idx| {
        input_ids[idx] = batch.input_ids[idx];
        attention_mask[idx] = @floatFromInt(batch.attention_mask[idx]);
    }
}

fn sigmoid(x: f32) f32 {
    if (x >= 0.0) {
        const z = @exp(-x);
        return 1.0 / (1.0 + z);
    }
    const z = @exp(x);
    return z / (1.0 + z);
}

fn loadPeftAdaptersIntoTrainer(
    allocator: std.mem.Allocator,
    adapter_checkpoint_path: []const u8,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer reader.deinit();
    for (trainer.lora_params.items) |*slot| {
        const peft_name = try autodiffSlotNameToPeftName(allocator, slot.name);
        defer allocator.free(peft_name);
        var tensor = try reader.readTensor(peft_name);
        defer tensor.deinit();
        if (tensor.elementCount() != slot.weights.len) return error.AdapterTensorShapeMismatch;
        try copyTensorF32Into(slot.weights, &tensor);
        @memset(slot.grad_accum, 0.0);
    }
}

fn loadTaskHeadIntoTrainer(
    head: *const gliner2_bundle.ClassifierTaskHead,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, "classifier.weight")) {
            if (slot.weights.len != head.weight.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.weight);
            @memset(slot.grad_accum, 0.0);
        } else if (std.mem.eql(u8, slot.name, "classifier.bias")) {
            if (slot.weights.len != head.bias.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.bias);
            @memset(slot.grad_accum, 0.0);
        }
    }
}

fn copyTensorF32Into(dst: []f32, tensor: *const Tensor) !void {
    if (tensor.dtype != .f32) return error.AdapterTensorDTypeMismatch;
    if (tensor.data.len != dst.len * @sizeOf(f32)) return error.AdapterTensorShapeMismatch;
    for (dst, 0..) |*value, idx| {
        const raw = tensor.data[idx * @sizeOf(f32) ..][0..@sizeOf(f32)];
        value.* = @bitCast(std.mem.readInt(u32, raw, .little));
    }
}

fn autodiffSlotNameToPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        const base = name[0 .. name.len - ".lora_A".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_A");
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        const base = name[0 .. name.len - ".lora_B".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_B");
    }
    return error.InvalidAutodiffAdapterName;
}

fn autodiffBaseToPeftName(allocator: std.mem.Allocator, base_no_weight: []const u8, adapter_name: []const u8) ![]const u8 {
    if (std.mem.startsWith(u8, base_no_weight, "encoder.layer.")) {
        return std.fmt.allocPrint(allocator, "encoder.{s}.{s}.weight", .{ base_no_weight, adapter_name });
    }
    return std.fmt.allocPrint(allocator, "{s}.{s}.weight", .{ base_no_weight, adapter_name });
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    return tensor_name;
}

fn stripEncoderPrefix(name: []const u8) []const u8 {
    const prefix = "encoder.";
    if (std.mem.startsWith(u8, name, prefix)) return name[prefix.len..];
    return name;
}

fn parseCsv(allocator: std.mem.Allocator, value: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, item));
    }
    if (out.items.len == 0) return error.EmptyCsv;
    return try out.toOwnedSlice(allocator);
}

fn parseLabelThresholdCsv(allocator: std.mem.Allocator, value: []const u8, out: *std.ArrayListUnmanaged(LabelThreshold)) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        const eq_idx = std.mem.indexOfScalar(u8, item, '=') orelse return error.InvalidLabelThreshold;
        const label = std.mem.trim(u8, item[0..eq_idx], " \t\r\n");
        const threshold_text = std.mem.trim(u8, item[eq_idx + 1 ..], " \t\r\n");
        if (label.len == 0 or threshold_text.len == 0) return error.InvalidLabelThreshold;
        if (containsLabelThreshold(out.items, label)) return error.DuplicateLabelThreshold;
        const threshold = try std.fmt.parseFloat(f32, threshold_text);
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidScoreThreshold;
        try out.append(allocator, .{
            .label = try allocator.dupe(u8, label),
            .threshold = threshold,
        });
    }
}

fn parseLabelScoreBiasCsv(allocator: std.mem.Allocator, value: []const u8, out: *std.ArrayListUnmanaged(LabelScoreBias)) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        const eq_idx = std.mem.indexOfScalar(u8, item, '=') orelse return error.InvalidLabelScoreBias;
        const label = std.mem.trim(u8, item[0..eq_idx], " \t\r\n");
        const bias_text = std.mem.trim(u8, item[eq_idx + 1 ..], " \t\r\n");
        if (label.len == 0 or bias_text.len == 0) return error.InvalidLabelScoreBias;
        if (containsLabelScoreBias(out.items, label)) return error.DuplicateLabelScoreBias;
        const bias = try std.fmt.parseFloat(f32, bias_text);
        if (!std.math.isFinite(bias)) return error.InvalidLabelScoreBias;
        try out.append(allocator, .{
            .label = try allocator.dupe(u8, label),
            .bias = bias,
        });
    }
}

fn containsLabelThreshold(values: []const LabelThreshold, label: []const u8) bool {
    for (values) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return true;
    }
    return false;
}

fn containsLabelScoreBias(values: []const LabelScoreBias, label: []const u8) bool {
    for (values) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return true;
    }
    return false;
}

fn validateLabelThresholds(label_thresholds: []const LabelThreshold, entity_types: []const []const u8) !void {
    for (label_thresholds) |entry| {
        if (indexOfLabel(entity_types, entry.label) == null) return error.UnknownLabelThreshold;
    }
}

fn validateLabelScoreBiases(label_score_biases: []const LabelScoreBias, entity_types: []const []const u8) !void {
    for (label_score_biases) |entry| {
        if (indexOfLabel(entity_types, entry.label) == null) return error.UnknownLabelScoreBias;
    }
}

fn indexOfLabel(labels: []const []const u8, label: []const u8) ?usize {
    for (labels, 0..) |candidate, idx| {
        if (std.mem.eql(u8, candidate, label)) return idx;
    }
    return null;
}

fn parseStringArray(allocator: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidTrainingManifest;
    var out = try allocator.alloc([]const u8, value.array.items.len);
    errdefer {
        for (out) |item| allocator.free(item);
        allocator.free(out);
    }
    for (value.array.items, 0..) |entry, idx| {
        if (entry != .string) return error.InvalidTrainingManifest;
        out[idx] = try allocator.dupe(u8, entry.string);
    }
    return out;
}

fn dupeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) ![][]const u8 {
    var out = try allocator.alloc([]const u8, values.len);
    errdefer {
        for (out) |item| allocator.free(item);
        allocator.free(out);
    }
    for (values, 0..) |value, idx| out[idx] = try allocator.dupe(u8, value);
    return out;
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn parseObjective(value: []const u8) !gliner2_autodiff.GlinerObjective {
    if (std.mem.eql(u8, value, "token")) return .token;
    if (std.mem.eql(u8, value, "span-start") or std.mem.eql(u8, value, "span_start")) return .span_start;
    return error.InvalidObjective;
}

fn objectiveName(objective: gliner2_autodiff.GlinerObjective) []const u8 {
    return switch (objective) {
        .token => "token",
        .span_start => "span-start",
    };
}

fn parseBackend(value: []const u8) ?EvalBackend {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "mlx")) return .mlx;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    return null;
}

fn selectBackend(
    requested: EvalBackend,
    force_native: bool,
    metal_available: bool,
    mlx_available: bool,
) !EvalBackend {
    if (force_native) return .native;
    return switch (requested) {
        .auto => if (metal_available) .metal else if (mlx_available) .mlx else .native,
        .metal => if (metal_available) .metal else error.MetalBackendUnavailable,
        .mlx => if (mlx_available) .mlx else error.MlxBackendUnavailable,
        .native => .native,
    };
}

fn backendLabel(backend: EvalBackend) []const u8 {
    return switch (backend) {
        .auto => "auto",
        .metal => "Metal",
        .mlx => "MLX (Apple Silicon)",
        .native => "native CPU/BLAS",
    };
}

fn envFlag(name: [:0]const u8) bool {
    const value = std.c.getenv(name) orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const v = value orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn jsonF64(value: ?std.json.Value) ?f64 {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => null,
    };
}

fn jsonUsize(value: ?std.json.Value) ?usize {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| if (i >= 0) @intCast(i) else null,
        else => null,
    };
}

fn jsonU32(value: std.json.Value) ?u32 {
    return switch (value) {
        .integer => |i| if (i >= 0) @intCast(i) else null,
        else => null,
    };
}

fn usageError() error{InvalidArguments} {
    printUsage();
    return error.InvalidArguments;
}

fn printUsage() void {
    std.debug.print(
        \\usage: eval-gliner2-autodiff-adapter <model_dir> <adapter_dir> <text> [entity_types_csv] [options]
        \\example: eval-gliner2-autodiff-adapter /tmp/gliner2 /tmp/gliner2-run "Alice joined Acme in Paris" person,organization,location --expect-label organization --min-score 0.05
        \\
        \\options:
        \\  --entity-types CSV
        \\  --seq-len N
        \\  --max-span-width N
        \\  --backend auto|metal|mlx|native
        \\  --compiled-required
        \\  --objective token|span-start
        \\  --expect-text TEXT
        \\  --expect-label LABEL
        \\  --min-score FLOAT
        \\  --min-prediction-score FLOAT
        \\  --label-thresholds label=FLOAT[,label=FLOAT...]
        \\  --label-score-biases label=FLOAT[,label=FLOAT...]
        \\  --nms-overlap FLOAT
        \\  --no-nms
        \\  --max-predictions N
        \\  --top-k-per-label N
        \\  --best-span-per-label-start
        \\  --best-label-per-span-start
        \\  --require-entitylike-span
        \\
    , .{});
}

test "isEntityLikeSpanText rejects function-word boundaries" {
    try std.testing.expect(isEntityLikeSpanText("Microsoft"));
    try std.testing.expect(isEntityLikeSpanText("New York"));
    try std.testing.expect(isEntityLikeSpanText("Bank of America"));
    try std.testing.expect(!isEntityLikeSpanText("in"));
    try std.testing.expect(!isEntityLikeSpanText("in London"));
    try std.testing.expect(!isEntityLikeSpanText("Microsoft opened"));
}
