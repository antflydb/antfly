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
const run_validation = inference.finetune.gliner2_run_validation;
const weight_source_mod = inference.models.weight_source;
const safetensors = inference.models.safetensors;
const compat = inference.io.compat;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = inference.backends.Tensor;
const ComputeBackend = inference.ops.ComputeBackend;
const MetalWeightStore = if (build_options.enable_metal) gpu_hosted_store.WeightStore else void;

pub const EvalBackend = enum {
    auto,
    metal,
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
    /// Trained candidate-span width; 0 when the manifest predates the field.
    max_span_width: usize = 0,

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
    /// Structured callers should prefer this over `entity_types_csv`; it
    /// preserves labels containing commas and avoids a serialization hop.
    entity_types: ?[]const []const u8 = null,
    entity_types_csv: ?[]const u8 = null,
    seq_len: usize = 64,
    /// Candidate-span width. Defaults to the adapter manifest's trained
    /// max_span_width (trainer default 8 for manifests predating the field);
    /// set explicitly to override.
    max_span_width: ?usize = null,
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

pub const NativeClassificationPrediction = struct {
    label: []const u8,
    score: f32,
};

pub const NativeFieldPrediction = struct {
    field: []const u8,
    value: []const u8,
    instance: usize,
    start: ?usize = null,
    end: ?usize = null,
    score: f32,
};

pub const NativeTaskPrediction = struct {
    kind: gliner2_data.UpstreamTaskKind,
    name: []const u8,
    /// Raw argmax from `count_pred` for span tasks. Classification tasks use
    /// the number of selected labels.
    predicted_count: usize,
    /// Number of non-empty instances emitted by the upstream public decoder.
    emitted_count: usize,
    classifications: []NativeClassificationPrediction,
    fields: []NativeFieldPrediction,

    fn deinit(self: *NativeTaskPrediction, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.classifications) |prediction| allocator.free(prediction.label);
        allocator.free(self.classifications);
        for (self.fields) |prediction| {
            allocator.free(prediction.field);
            allocator.free(prediction.value);
        }
        allocator.free(self.fields);
        self.* = undefined;
    }
};

pub const NativeRecordPrediction = struct {
    tasks: []NativeTaskPrediction,
    entity_predictions: []TopEntity,

    pub fn deinit(self: *NativeRecordPrediction, allocator: std.mem.Allocator) void {
        for (self.tasks) |*task| task.deinit(allocator);
        allocator.free(self.tasks);
        freeTopEntities(allocator, self.entity_predictions);
        self.* = undefined;
    }
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
        } else if (std.mem.eql(u8, arg, "--upstream-entity-decode")) {
            opts.prediction_threshold = 0.5;
            opts.nms_overlap_threshold = 0.0;
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

/// Long-lived native evaluator for full-task adapters. Base weights, tokenizer,
/// and adapter mmap are initialized once; the small inference graph is cached
/// by per-record entity-schema width.
pub const NativeEvalSessionOptions = struct {
    model_dir: []const u8,
    adapter_dir: []const u8,
    seq_len: usize = 64,
    /// Candidate-span width. Defaults to the adapter manifest's trained
    /// max_span_width (trainer default 8 for manifests predating the field);
    /// set explicitly to override.
    max_span_width: ?usize = null,
    objective_override: ?gliner2_autodiff.GlinerObjective = null,
    prediction_threshold: ?f32 = null,
    label_score_biases: []const LabelScoreBias = &.{},
};

const NativeEvalGraph = struct {
    entity_type_count: usize,
    gliner_ctx: gliner2_autodiff.GlinerAutodiffCtx,
    trainer: real_autodiff.RealAutodiffTrainer,
};

const NativeFullTaskGraph = struct {
    schema_width: usize,
    task_count: usize,
    gliner_ctx: gliner2_autodiff.GlinerAutodiffCtx,
    trainer: real_autodiff.RealAutodiffTrainer,
};

pub const NativeEvalSession = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    seq_len: usize,
    max_span_width: usize,
    objective: gliner2_autodiff.GlinerObjective,
    use_upstream_word_splitting: bool,
    prediction_threshold: ?f32,
    label_score_biases: []const LabelScoreBias,
    manifest: Manifest,
    graph_config: deberta_graph.Config,
    weight_store: WeightStore,
    owned_weight_names: std.ArrayListUnmanaged([]const u8),
    safetensors_source: *SafetensorsSource,
    native_backend: NativeCompute,
    compute_backend: ComputeBackend,
    tokenizer: gliner2_data.Tokenizer,
    adapter_reader: safetensors.MMapReader,
    graphs: std.ArrayListUnmanaged(*NativeEvalGraph) = .empty,
    full_task_graphs: std.ArrayListUnmanaged(*NativeFullTaskGraph) = .empty,

    pub fn init(allocator: std.mem.Allocator, opts: NativeEvalSessionOptions) !*NativeEvalSession {
        if (opts.seq_len == 0 or opts.seq_len > 4096) return error.InvalidSeqLen;

        const self = try allocator.create(NativeEvalSession);
        errdefer allocator.destroy(self);

        var manifest = try loadManifest(allocator, opts.adapter_dir);
        errdefer manifest.deinit(allocator);
        const max_span_width = try resolveMaxSpanWidth(opts.max_span_width, &manifest);
        const objective = opts.objective_override orelse manifest.objective;
        if (objective == .token) return error.NativeEvalSessionRequiresSpanObjective;

        const graph_config = try loadDebertaGraphConfig(allocator, opts.model_dir);
        if (manifest.hidden_size != graph_config.hidden_size) return error.HiddenSizeMismatch;

        const safetensors_path = try std.fs.path.join(allocator, &.{ opts.model_dir, "model.safetensors" });
        defer allocator.free(safetensors_path);
        var weight_store = WeightStore{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        };
        var owned_weight_names = std.ArrayListUnmanaged([]const u8).empty;
        errdefer deinitNativeWeightStore(allocator, &weight_store, &owned_weight_names);
        const source = try SafetensorsSource.initAbsolute(allocator, safetensors_path);
        errdefer source.weightSource().deinit();
        var ws = source.weightSource();
        _ = try loadBaseWeightsFromSource(allocator, &ws, &weight_store, &owned_weight_names);

        const task_head_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.task_head_checkpoint_file_name });
        defer allocator.free(task_head_path);
        var task_head = try gliner2_bundle.loadClassifierTaskHead(allocator, task_head_path);
        defer task_head.deinit();
        if (task_head.num_classes != manifest.num_classes or task_head.hidden_size != manifest.hidden_size) return error.TaskHeadShapeMismatch;
        var zero_task_head = try initZeroTaskHead(allocator, 1, manifest.hidden_size);
        defer zero_task_head.deinit();
        try addTaskHeadWeights(allocator, &weight_store, &owned_weight_names, &zero_task_head);
        try addParityTopLevelWeights(allocator, &weight_store, &owned_weight_names, manifest.hidden_size);

        var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, opts.model_dir);
        errdefer tokenizer.deinit(allocator);
        const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.adapter_checkpoint_file_name });
        defer allocator.free(adapter_checkpoint_path);
        var adapter_reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
        errdefer adapter_reader.deinit();

        self.* = .{
            .allocator = allocator,
            .model_dir = opts.model_dir,
            .seq_len = opts.seq_len,
            .max_span_width = max_span_width,
            .objective = objective,
            .use_upstream_word_splitting = objective == .gliner2_total_loss,
            .prediction_threshold = opts.prediction_threshold,
            .label_score_biases = opts.label_score_biases,
            .manifest = manifest,
            .graph_config = graph_config,
            .weight_store = weight_store,
            .owned_weight_names = owned_weight_names,
            .safetensors_source = source,
            .native_backend = undefined,
            .compute_backend = undefined,
            .tokenizer = tokenizer,
            .adapter_reader = adapter_reader,
        };
        self.native_backend = NativeCompute.init(allocator, &self.weight_store, null);
        self.compute_backend = self.native_backend.computeBackend();
        return self;
    }

    pub fn deinit(self: *NativeEvalSession) void {
        for (self.graphs.items) |graph| {
            graph.trainer.deinit();
            self.allocator.destroy(graph);
        }
        self.graphs.deinit(self.allocator);
        for (self.full_task_graphs.items) |graph| {
            graph.trainer.deinit();
            self.allocator.destroy(graph);
        }
        self.full_task_graphs.deinit(self.allocator);
        self.adapter_reader.deinit();
        self.tokenizer.deinit(self.allocator);
        deinitNativeWeightStore(self.allocator, &self.weight_store, &self.owned_weight_names);
        self.safetensors_source.weightSource().deinit();
        self.manifest.deinit(self.allocator);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn evalText(
        self: *NativeEvalSession,
        text: []const u8,
        entity_types: []const []const u8,
    ) ![]TopEntity {
        if (entity_types.len == 0 or entity_types.len > gliner2_autodiff.max_span_start_entity_types) return error.InvalidEntityTypes;
        const graph = try self.graphFor(entity_types, text);
        return decodeTextEntities(
            self.allocator,
            &self.tokenizer,
            entity_types,
            text,
            entity_types.len + 1,
            .span_start,
            self.use_upstream_word_splitting,
            self.seq_len,
            self.max_span_width,
            &graph.trainer,
            &graph.gliner_ctx,
            self.prediction_threshold,
            self.label_score_biases,
        );
    }

    /// Decode every task in one upstream-format record with one encoder
    /// execution. The record's full schema metadata is retained while gold
    /// fields and labels are removed from the inference batch.
    pub fn evalRecord(
        self: *NativeEvalSession,
        record: gliner2_data.UpstreamRecord,
    ) !NativeRecordPrediction {
        if (self.objective != .gliner2_total_loss) return error.NativeFullTaskEvalRequiresTotalLoss;
        try validateFullTaskRecord(record);

        var schema_tasks = try schemaOnlyTasksAlloc(self.allocator, record.tasks);
        defer schema_tasks.deinit(self.allocator);
        const schema_record = gliner2_data.UpstreamRecord{
            .text = record.text,
            .tasks = schema_tasks.tasks,
            .prefix_tokens = record.prefix_tokens,
        };
        const labels = try gliner2_data.buildUpstreamTaskLabelVocab(self.allocator, &.{schema_record}, null);
        defer freeStringSlice(self.allocator, labels);
        if (labels.len == 0 or labels.len > gliner2_autodiff.max_span_start_entity_types) return error.TooManySchemaSlots;

        var encoded = try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
            self.allocator,
            &self.tokenizer,
            &.{schema_record},
            labels.len,
            self.seq_len,
            self.max_span_width,
            1,
        );
        defer encoded.deinit();
        const input_ids = try self.allocator.alloc(i64, self.seq_len);
        defer self.allocator.free(input_ids);
        const attention_mask = try self.allocator.alloc(f32, self.seq_len);
        defer self.allocator.free(attention_mask);
        try copyEncodedInputs(&encoded, input_ids, attention_mask);

        const target_width = gliner2_autodiff.gliner2TotalLossTargetWidthEx(labels.len, max_full_task_instances);
        const targets = try self.allocator.alloc(f32, encoded.max_spans * target_width);
        defer self.allocator.free(targets);
        try fillFullTaskEvalTargets(self.allocator, &encoded, schema_record, labels, max_full_task_instances, targets);
        const targets_shape = gliner2_autodiff.gliner2TotalLossTargetsShapeEx(
            1,
            @intCast(encoded.max_spans),
            @intCast(labels.len),
            max_full_task_instances,
        );
        const graph = try self.fullTaskGraphFor(labels.len, record.tasks.len, input_ids, attention_mask, targets, targets_shape);
        var logits = try gliner2_autodiff.gliner2EvalLogitsForBatch(
            self.allocator,
            &graph.trainer,
            &graph.gliner_ctx,
            input_ids,
            attention_mask,
            targets,
            targets_shape,
            1,
            @intCast(self.seq_len),
        );
        defer logits.deinit(self.allocator);
        return decodeFullTaskRecord(
            self.allocator,
            record,
            &encoded,
            labels,
            logits,
            self.prediction_threshold orelse 0.5,
            self.label_score_biases,
        );
    }

    fn graphFor(
        self: *NativeEvalSession,
        entity_types: []const []const u8,
        text: []const u8,
    ) !*NativeEvalGraph {
        for (self.graphs.items) |graph| {
            if (graph.entity_type_count == entity_types.len) return graph;
        }

        const graph = try self.allocator.create(NativeEvalGraph);
        errdefer self.allocator.destroy(graph);
        try replaceTaskHeadWithZeros(self.allocator, &self.weight_store, entity_types.len + 1, self.manifest.hidden_size);
        graph.* = .{
            .entity_type_count = entity_types.len,
            .gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
                .graph_config = self.graph_config,
                .num_classes = @intCast(entity_types.len + 1),
                .objective = .span_start,
            }),
            .trainer = try real_autodiff.RealAutodiffTrainer.init(
                self.allocator,
                &self.compute_backend,
                nativeTrainerConfig(self.manifest, self.graph_config, true),
            ),
        };
        errdefer graph.trainer.deinit();
        try ensureGraphBuilt(
            self.allocator,
            &self.tokenizer,
            entity_types,
            text,
            entity_types.len + 1,
            self.seq_len,
            self.max_span_width,
            self.use_upstream_word_splitting,
            .span_start,
            &graph.trainer,
            &graph.gliner_ctx,
        );
        try loadPeftAdaptersFromReader(self.allocator, &self.adapter_reader, &graph.trainer);
        try self.graphs.append(self.allocator, graph);
        return graph;
    }

    fn fullTaskGraphFor(
        self: *NativeEvalSession,
        schema_width: usize,
        task_count: usize,
        input_ids: []const i64,
        attention_mask: []const f32,
        targets: []const f32,
        targets_shape: ml.graph.Shape,
    ) !*NativeFullTaskGraph {
        for (self.full_task_graphs.items) |graph| {
            if (graph.schema_width == schema_width and graph.task_count == task_count) return graph;
        }
        const graph = try self.allocator.create(NativeFullTaskGraph);
        errdefer self.allocator.destroy(graph);
        graph.* = .{
            .schema_width = schema_width,
            .task_count = task_count,
            .gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
                .graph_config = self.graph_config,
                .num_classes = @intCast(schema_width + 1),
                .objective = .gliner2_total_loss,
                .span_start_loss_reduction = .sum,
                .structure_max_instances = max_full_task_instances,
                .max_schema_tasks = @intCast(task_count),
            }),
            .trainer = try real_autodiff.RealAutodiffTrainer.init(
                self.allocator,
                &self.compute_backend,
                nativeTrainerConfig(self.manifest, self.graph_config, false),
            ),
        };
        errdefer graph.trainer.deinit();
        const trainer_input = gliner2_autodiff.makeTrainerInput(
            &graph.gliner_ctx,
            input_ids,
            attention_mask,
            targets,
            targets_shape,
            1,
            @intCast(self.seq_len),
        );
        try graph.trainer.ensureGraphBuilt(trainer_input);
        try loadPeftAdaptersFromReader(self.allocator, &self.adapter_reader, &graph.trainer);
        try self.full_task_graphs.append(self.allocator, graph);
        return graph;
    }
};

const max_full_task_instances: u32 = 19;

const SchemaOnlyTasks = struct {
    tasks: []gliner2_data.UpstreamTask,
    derived_schema_fields: []?[][]const u8,

    fn deinit(self: *SchemaOnlyTasks, allocator: std.mem.Allocator) void {
        for (self.derived_schema_fields) |fields| if (fields) |items| allocator.free(items);
        allocator.free(self.derived_schema_fields);
        allocator.free(self.tasks);
        self.* = undefined;
    }
};

fn schemaOnlyTasksAlloc(
    allocator: std.mem.Allocator,
    tasks: []const gliner2_data.UpstreamTask,
) !SchemaOnlyTasks {
    const out = try allocator.alloc(gliner2_data.UpstreamTask, tasks.len);
    errdefer allocator.free(out);
    const derived = try allocator.alloc(?[][]const u8, tasks.len);
    errdefer allocator.free(derived);
    @memset(derived, null);
    errdefer for (derived) |fields| if (fields) |items| allocator.free(items);

    for (tasks, 0..) |task, idx| {
        out[idx] = task;
        if (task.kind != .classifications and task.schema_fields.len == 0) {
            var names = std.ArrayListUnmanaged([]const u8).empty;
            defer names.deinit(allocator);
            for (task.fields) |field| {
                if (indexOfLabel(names.items, field.name) == null) try names.append(allocator, field.name);
            }
            if (names.items.len == 0) return error.EmptyTaskSchema;
            derived[idx] = try names.toOwnedSlice(allocator);
            out[idx].schema_fields = derived[idx].?;
        }
        out[idx].fields = &.{};
        out[idx].true_labels = &.{};
        out[idx].count = if (task.kind == .classifications) 0 else 1;
    }
    return .{ .tasks = out, .derived_schema_fields = derived };
}

fn validateFullTaskRecord(record: gliner2_data.UpstreamRecord) !void {
    if (record.tasks.len == 0) return error.NoGliner2Tasks;
    for (record.tasks) |task| switch (task.kind) {
        .classifications => {
            if (task.labels.len == 0) return error.InvalidClassificationSchema;
        },
        .entities, .json_structures => {
            if (task.schema_fields.len == 0 and task.fields.len == 0) return error.EmptyTaskSchema;
        },
        .relations => {
            if (task.schema_fields.len != 2 or
                !std.mem.eql(u8, task.schema_fields[0], "head") or
                !std.mem.eql(u8, task.schema_fields[1], "tail"))
            {
                return error.UnsupportedRelationFields;
            }
            if (task.count == 0 or task.fields.len != task.count * 2) return error.UnsupportedRelationFields;
            for (0..task.count) |instance| {
                var head_count: usize = 0;
                var tail_count: usize = 0;
                for (task.fields) |field| {
                    if (field.instance >= task.count) return error.UnsupportedRelationFields;
                    if (field.instance != instance) continue;
                    if (std.mem.eql(u8, field.name, "head")) {
                        head_count += @intFromBool(std.mem.trim(u8, field.value, " \t\r\n").len > 0);
                    } else if (std.mem.eql(u8, field.name, "tail")) {
                        tail_count += @intFromBool(std.mem.trim(u8, field.value, " \t\r\n").len > 0);
                    } else return error.UnsupportedRelationFields;
                }
                if (head_count != 1 or tail_count != 1) return error.UnsupportedRelationFields;
            }
        },
    };
}

fn fillFullTaskEvalTargets(
    allocator: std.mem.Allocator,
    encoded: *const gliner2_data.EncodedBatch,
    record: gliner2_data.UpstreamRecord,
    labels: []const []const u8,
    max_instances: u32,
    out: []f32,
) !void {
    const E = labels.len;
    const width = gliner2_autodiff.gliner2TotalLossTargetWidthEx(E, max_instances);
    if (encoded.batch_size != 1 or out.len != encoded.max_spans * width) return error.InvalidGlinerSpanTargetShape;
    @memset(out, 0.0);
    const schema_idx_offset = 2 * E;
    const row_idx_offset = 3 * E;
    const count_idx_offset = 4 * E;
    const start_idx_offset = 5 * E;
    const parent_idx_offset = gliner2_autodiff.gliner2TotalLossParentIndexOffset(E);
    const task_groups_offset = gliner2_autodiff.gliner2TotalLossTaskGroupsOffset(E, max_instances);
    const active_fields_offset = gliner2_autodiff.gliner2TotalLossActiveFieldsOffset(E, max_instances);

    const active = try allocator.alloc(bool, E);
    defer allocator.free(active);
    @memset(active, false);
    const task_groups = try allocator.alloc(f32, E);
    defer allocator.free(task_groups);
    @memset(task_groups, 0.0);
    for (record.tasks, 0..) |task, task_idx| {
        if (task.kind == .classifications) continue;
        for (task.schema_fields) |field| {
            const key = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, field);
            defer allocator.free(key);
            const label_idx = indexOfLabel(labels, key) orelse return error.SchemaFieldMissingFromLocalSlots;
            active[label_idx] = true;
            task_groups[label_idx] = @floatFromInt(task_idx + 1);
        }
    }

    for (0..encoded.max_spans) |span_idx| {
        const row = out[span_idx * width ..][0..width];
        @memcpy(row[task_groups_offset .. task_groups_offset + E], task_groups);
        for (0..E) |label_idx| {
            const schema_pos = encoded.e_token_positions[label_idx];
            if (schema_pos < 0) return error.InvalidSchemaTokenPosition;
            row[schema_idx_offset + label_idx] = @floatFromInt(schema_pos);
            row[row_idx_offset + label_idx] = @floatFromInt(span_idx);
            row[count_idx_offset + label_idx] = @floatFromInt(label_idx);
            if (active[label_idx]) {
                row[E + label_idx] = 1.0;
                // The count transformer reads schema activity from its own
                // block; leaving it zero masks out every field at eval time.
                row[active_fields_offset + label_idx] = 1.0;
            }
        }
        if (encoded.span_mask[span_idx] <= 0.0) continue;
        const start_word = encoded.span_indices[span_idx * 2];
        const end_word = encoded.span_indices[span_idx * 2 + 1];
        if (start_word < 0 or end_word < start_word) return error.InvalidSpanWordIndex;
        const start_token = encoded.first_token_positions[@intCast(start_word)];
        const end_token = encoded.first_token_positions[@intCast(end_word)];
        if (start_token < 0 or end_token < 0) return error.InvalidTokenPosition;
        row[start_idx_offset] = @floatFromInt(start_token);
        row[start_idx_offset + 1] = @floatFromInt(end_token);
    }

    for (record.tasks, 0..) |_, task_idx| {
        if (task_idx >= encoded.max_spans or task_idx >= encoded.max_schemas) return error.TooManySchemaTasks;
        const count_idx = task_idx;
        if (count_idx >= encoded.schema_special_counts.len or encoded.schema_special_counts[count_idx] <= 0) return error.InvalidSchemaTokenPosition;
        const pos_idx = task_idx * encoded.max_schema_specials;
        const parent_pos = encoded.schema_special_positions[pos_idx];
        if (parent_pos < 0) return error.InvalidSchemaTokenPosition;
        out[task_idx * width + parent_idx_offset] = @floatFromInt(parent_pos);
    }
}

const TextSpanCandidate = struct {
    value: []const u8,
    start: usize,
    end: usize,
    score: f32,
};

fn textSpanCandidatesAlloc(
    allocator: std.mem.Allocator,
    record: gliner2_data.UpstreamRecord,
    encoded: *const gliner2_data.EncodedBatch,
    logits: []const f32,
    schema_width: usize,
    instance: usize,
    field_idx: usize,
    threshold: f32,
    score_bias: f32,
) ![]TextSpanCandidate {
    const boundaries = try gliner2_data.upstreamWordBoundariesAlloc(allocator, record.text);
    defer allocator.free(boundaries);
    const prefix_words = record.prefix_tokens.len;
    var out = std.ArrayListUnmanaged(TextSpanCandidate).empty;
    errdefer out.deinit(allocator);
    for (0..encoded.max_spans) |span_idx| {
        if (encoded.span_mask[span_idx] <= 0.0) continue;
        const word_start_raw = encoded.span_indices[span_idx * 2];
        const word_end_raw = encoded.span_indices[span_idx * 2 + 1];
        if (word_start_raw < 0 or word_end_raw < word_start_raw) continue;
        const word_start: usize = @intCast(word_start_raw);
        const word_end: usize = @intCast(word_end_raw);
        if (word_start < prefix_words or word_end < prefix_words) continue;
        const text_start = word_start - prefix_words;
        const text_end = word_end - prefix_words;
        if (text_start >= boundaries.len or text_end >= boundaries.len) continue;
        const logit_idx = span_idx * (@as(usize, max_full_task_instances) * schema_width) + instance * schema_width + field_idx;
        if (logit_idx >= logits.len or !std.math.isFinite(logits[logit_idx])) return error.LogitShapeMismatch;
        const score = scoreWithBias(sigmoid(logits[logit_idx]), score_bias);
        if (score < threshold) continue;
        const start = boundaries[text_start][0];
        const end = boundaries[text_end][1];
        if (start >= end or end > record.text.len) continue;
        try out.append(allocator, .{ .value = record.text[start..end], .start = start, .end = end, .score = score });
    }
    return out.toOwnedSlice(allocator);
}

fn spanCandidateBetterThan(_: void, lhs: TextSpanCandidate, rhs: TextSpanCandidate) bool {
    if (lhs.score != rhs.score) return lhs.score > rhs.score;
    if (lhs.start != rhs.start) return lhs.start < rhs.start;
    return lhs.end < rhs.end;
}

fn nonOverlappingCandidatesAlloc(allocator: std.mem.Allocator, candidates: []const TextSpanCandidate) ![]TextSpanCandidate {
    const sorted = try allocator.dupe(TextSpanCandidate, candidates);
    defer allocator.free(sorted);
    std.mem.sort(TextSpanCandidate, sorted, {}, spanCandidateBetterThan);
    var selected = std.ArrayListUnmanaged(TextSpanCandidate).empty;
    errdefer selected.deinit(allocator);
    for (sorted) |candidate| {
        var overlaps = false;
        for (selected.items) |existing| {
            if (!(candidate.end <= existing.start or candidate.start >= existing.end)) {
                overlaps = true;
                break;
            }
        }
        if (!overlaps) try selected.append(allocator, candidate);
    }
    return selected.toOwnedSlice(allocator);
}

const ChoiceFieldSchema = struct {
    field: []const u8,
    choices: [][]const u8,
};

fn freeChoiceFields(allocator: std.mem.Allocator, fields: []ChoiceFieldSchema) void {
    for (fields) |field| allocator.free(field.choices);
    allocator.free(fields);
}

fn choiceFieldsForTaskAlloc(
    allocator: std.mem.Allocator,
    prefix_tokens: []const []const u8,
    task_name: []const u8,
) ![]ChoiceFieldSchema {
    const parent = try std.fmt.allocPrint(allocator, "{s}:", .{task_name});
    defer allocator.free(parent);
    var out = std.ArrayListUnmanaged(ChoiceFieldSchema).empty;
    errdefer {
        for (out.items) |field| allocator.free(field.choices);
        out.deinit(allocator);
    }
    var i: usize = 0;
    while (i + 2 < prefix_tokens.len) {
        if (!std.mem.eql(u8, prefix_tokens[i], "(") or !std.mem.eql(u8, prefix_tokens[i + 1], parent)) {
            i += 1;
            continue;
        }
        var cursor = i + 2;
        while (true) {
            if (cursor >= prefix_tokens.len) return error.InvalidChoicePrefixSchema;
            if (std.mem.eql(u8, prefix_tokens[cursor], ")")) {
                cursor += 1;
                break;
            }
            const field_name = prefix_tokens[cursor];
            cursor += 1;
            if (cursor >= prefix_tokens.len or !std.mem.eql(u8, prefix_tokens[cursor], "(")) return error.InvalidChoicePrefixSchema;
            cursor += 1;
            var choices = std.ArrayListUnmanaged([]const u8).empty;
            defer choices.deinit(allocator);
            while (true) {
                if (cursor >= prefix_tokens.len or std.mem.eql(u8, prefix_tokens[cursor], ")") or std.mem.eql(u8, prefix_tokens[cursor], "|")) return error.InvalidChoicePrefixSchema;
                try choices.append(allocator, prefix_tokens[cursor]);
                cursor += 1;
                if (cursor >= prefix_tokens.len) return error.InvalidChoicePrefixSchema;
                if (std.mem.eql(u8, prefix_tokens[cursor], ")")) {
                    cursor += 1;
                    break;
                }
                if (!std.mem.eql(u8, prefix_tokens[cursor], "|")) return error.InvalidChoicePrefixSchema;
                cursor += 1;
            }
            if (choices.items.len == 0) return error.InvalidChoicePrefixSchema;
            var existing_idx: ?usize = null;
            for (out.items, 0..) |existing, idx| {
                if (std.mem.eql(u8, existing.field, field_name)) {
                    existing_idx = idx;
                    break;
                }
            }
            if (existing_idx) |idx| {
                const existing = out.items[idx].choices;
                if (existing.len != choices.items.len) return error.InconsistentChoiceSchema;
                for (existing, choices.items) |lhs, rhs| if (!std.mem.eql(u8, lhs, rhs)) return error.InconsistentChoiceSchema;
            } else {
                try out.append(allocator, .{ .field = field_name, .choices = try choices.toOwnedSlice(allocator) });
            }
            if (cursor >= prefix_tokens.len) return error.InvalidChoicePrefixSchema;
            if (std.mem.eql(u8, prefix_tokens[cursor], ",")) {
                cursor += 1;
                continue;
            }
            if (!std.mem.eql(u8, prefix_tokens[cursor], ")")) return error.InvalidChoicePrefixSchema;
        }
        i = cursor;
    }
    return out.toOwnedSlice(allocator);
}

fn choiceField(fields: []const ChoiceFieldSchema, name: []const u8) ?ChoiceFieldSchema {
    for (fields) |field| if (std.mem.eql(u8, field.field, name)) return field;
    return null;
}

fn asciiContainsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    for (0..haystack.len - needle.len + 1) |start| {
        if (std.ascii.eqlIgnoreCase(haystack[start .. start + needle.len], needle)) return true;
    }
    return false;
}

fn firstChoiceWordIndex(prefix_tokens: []const []const u8, choice: []const u8) ?usize {
    for (prefix_tokens, 0..) |token, idx| {
        if (std.ascii.eqlIgnoreCase(token, choice) or asciiContainsIgnoreCase(token, choice)) return idx;
    }
    return null;
}

fn appendOwnedFieldPrediction(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(NativeFieldPrediction),
    field: []const u8,
    candidate: TextSpanCandidate,
    instance: usize,
) !void {
    const owned_field = try allocator.dupe(u8, field);
    errdefer allocator.free(owned_field);
    const owned_value = try allocator.dupe(u8, candidate.value);
    errdefer allocator.free(owned_value);
    try out.append(allocator, .{
        .field = owned_field,
        .value = owned_value,
        .instance = instance,
        .start = candidate.start,
        .end = candidate.end,
        .score = candidate.score,
    });
}

fn appendOwnedChoicePrediction(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(NativeFieldPrediction),
    field: []const u8,
    value: []const u8,
    instance: usize,
    score: f32,
) !void {
    const owned_field = try allocator.dupe(u8, field);
    errdefer allocator.free(owned_field);
    const owned_value = try allocator.dupe(u8, value);
    errdefer allocator.free(owned_value);
    try out.append(allocator, .{ .field = owned_field, .value = owned_value, .instance = instance, .score = score });
}

fn decodeClassificationTaskAlloc(
    allocator: std.mem.Allocator,
    task: gliner2_data.UpstreamTask,
    task_idx: usize,
    labels: []const []const u8,
    logits: []const f32,
) ![]NativeClassificationPrediction {
    const E = labels.len;
    var task_logits = try allocator.alloc(f32, task.labels.len);
    defer allocator.free(task_logits);
    for (task.labels, 0..) |label, idx| {
        const key = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, label);
        defer allocator.free(key);
        const label_idx = indexOfLabel(labels, key) orelse return error.SchemaFieldMissingFromLocalSlots;
        const logit_idx = task_idx * E + label_idx;
        if (logit_idx >= logits.len or !std.math.isFinite(logits[logit_idx])) return error.LogitShapeMismatch;
        task_logits[idx] = logits[logit_idx];
    }
    var selected = std.ArrayListUnmanaged(usize).empty;
    defer selected.deinit(allocator);
    if (task.multi_label) {
        for (task_logits, 0..) |logit, idx| if (sigmoid(logit) >= 0.5) try selected.append(allocator, idx);
        if (selected.items.len == 0) try selected.append(allocator, maxIndex(task_logits));
    } else {
        try selected.append(allocator, maxIndex(task_logits));
    }
    const out = try allocator.alloc(NativeClassificationPrediction, selected.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |prediction| allocator.free(prediction.label);
        allocator.free(out);
    }
    for (selected.items, 0..) |label_idx, out_idx| {
        const score = if (task.multi_label) sigmoid(task_logits[label_idx]) else softmaxProbability(task_logits, label_idx);
        out[out_idx] = .{ .label = try allocator.dupe(u8, task.labels[label_idx]), .score = score };
        initialized += 1;
    }
    return out;
}

fn maxIndex(values: []const f32) usize {
    std.debug.assert(values.len > 0);
    var best: usize = 0;
    for (values[1..], 1..) |value, idx| {
        if (value > values[best]) best = idx;
    }
    return best;
}

fn softmaxProbability(values: []const f32, selected: usize) f32 {
    var max_value = values[0];
    for (values[1..]) |value| max_value = @max(max_value, value);
    var total: f32 = 0.0;
    for (values) |value| total += @exp(value - max_value);
    return @exp(values[selected] - max_value) / total;
}

fn decodeFullTaskRecord(
    allocator: std.mem.Allocator,
    record: gliner2_data.UpstreamRecord,
    encoded: *const gliner2_data.EncodedBatch,
    labels: []const []const u8,
    logits: gliner2_autodiff.Gliner2EvalLogits,
    threshold: f32,
    label_score_biases: []const LabelScoreBias,
) !NativeRecordPrediction {
    if (!std.math.isFinite(threshold) or threshold < 0.0 or threshold > 1.0) return error.InvalidScoreThreshold;
    const E = labels.len;
    if (logits.span.len != encoded.max_spans * @as(usize, max_full_task_instances) * E or
        logits.classification.len != record.tasks.len * E or
        logits.count.len != record.tasks.len * 20)
    {
        return error.LogitShapeMismatch;
    }
    const tasks = try allocator.alloc(NativeTaskPrediction, record.tasks.len);
    var initialized: usize = 0;
    errdefer {
        for (tasks[0..initialized]) |*task| task.deinit(allocator);
        allocator.free(tasks);
    }
    var entities = std.ArrayListUnmanaged(TopEntity).empty;
    errdefer {
        for (entities.items) |prediction| {
            allocator.free(prediction.text);
            allocator.free(prediction.label);
        }
        entities.deinit(allocator);
    }

    for (record.tasks, 0..) |task, task_idx| {
        tasks[task_idx] = .{
            .kind = task.kind,
            .name = try allocator.dupe(u8, task.name),
            .predicted_count = 0,
            .emitted_count = 0,
            .classifications = try allocator.alloc(NativeClassificationPrediction, 0),
            .fields = try allocator.alloc(NativeFieldPrediction, 0),
        };
        initialized += 1;
        if (task.kind == .classifications) {
            allocator.free(tasks[task_idx].classifications);
            tasks[task_idx].classifications = try decodeClassificationTaskAlloc(
                allocator,
                task,
                task_idx,
                labels,
                logits.classification,
            );
            tasks[task_idx].predicted_count = tasks[task_idx].classifications.len;
            tasks[task_idx].emitted_count = tasks[task_idx].classifications.len;
            continue;
        }

        const count_row = logits.count[task_idx * 20 ..][0..20];
        for (count_row) |value| if (!std.math.isFinite(value)) return error.NonFiniteLogit;
        const predicted_count = maxIndex(count_row);
        tasks[task_idx].predicted_count = predicted_count;
        if (predicted_count == 0) continue;

        var fields = std.ArrayListUnmanaged(NativeFieldPrediction).empty;
        errdefer {
            for (fields.items) |prediction| {
                allocator.free(prediction.field);
                allocator.free(prediction.value);
            }
            fields.deinit(allocator);
        }
        const choices = if (task.kind == .json_structures)
            try choiceFieldsForTaskAlloc(allocator, record.prefix_tokens, task.name)
        else
            try allocator.alloc(ChoiceFieldSchema, 0);
        defer freeChoiceFields(allocator, choices);

        switch (task.kind) {
            .classifications => unreachable,
            .entities => {
                for (task.schema_fields, 0..) |field_name, field_order| {
                    const field_idx = try fullTaskFieldIndex(allocator, labels, task, field_name);
                    const candidates = try textSpanCandidatesAlloc(
                        allocator,
                        record,
                        encoded,
                        logits.span,
                        E,
                        0,
                        field_idx,
                        threshold,
                        labelScoreBias(task.schema_fields, field_order, label_score_biases),
                    );
                    defer allocator.free(candidates);
                    const selected = try nonOverlappingCandidatesAlloc(allocator, candidates);
                    defer allocator.free(selected);
                    for (selected) |candidate| {
                        try appendOwnedFieldPrediction(allocator, &fields, field_name, candidate, 0);
                        const owned_text = try allocator.dupe(u8, candidate.value);
                        errdefer allocator.free(owned_text);
                        const owned_label = try allocator.dupe(u8, field_name);
                        errdefer allocator.free(owned_label);
                        try entities.append(allocator, .{
                            .text = owned_text,
                            .label = owned_label,
                            .start = candidate.start,
                            .end = candidate.end,
                            .score = candidate.score,
                        });
                    }
                }
                tasks[task_idx].emitted_count = 1;
            },
            .json_structures => {
                for (0..predicted_count) |raw_instance| {
                    const emitted_instance = tasks[task_idx].emitted_count;
                    const fields_before = fields.items.len;
                    for (task.schema_fields) |field_name| {
                        const field_idx = try fullTaskFieldIndex(allocator, labels, task, field_name);
                        if (choiceField(choices, field_name)) |choice_schema| {
                            for (choice_schema.choices) |choice| {
                                const word_idx = firstChoiceWordIndex(record.prefix_tokens, choice) orelse return error.InvalidChoicePrefixSchema;
                                const span_idx = word_idx * (encoded.max_spans / encoded.max_words_per_sample);
                                if (span_idx >= encoded.max_spans or encoded.span_mask[span_idx] <= 0.0) return error.InvalidChoicePrefixSchema;
                                const logit_idx = span_idx * (@as(usize, max_full_task_instances) * E) +
                                    raw_instance * E + field_idx;
                                if (logit_idx >= logits.span.len or !std.math.isFinite(logits.span[logit_idx])) return error.LogitShapeMismatch;
                                const score = sigmoid(logits.span[logit_idx]);
                                if (score >= threshold) try appendOwnedChoicePrediction(allocator, &fields, field_name, choice, emitted_instance, score);
                            }
                        } else {
                            const candidates = try textSpanCandidatesAlloc(allocator, record, encoded, logits.span, E, raw_instance, field_idx, threshold, 0.0);
                            defer allocator.free(candidates);
                            const selected = try nonOverlappingCandidatesAlloc(allocator, candidates);
                            defer allocator.free(selected);
                            for (selected) |candidate| try appendOwnedFieldPrediction(allocator, &fields, field_name, candidate, emitted_instance);
                        }
                    }
                    if (fields.items.len > fields_before) tasks[task_idx].emitted_count += 1;
                }
            },
            .relations => {
                for (0..predicted_count) |raw_instance| {
                    var pair: [2]?TextSpanCandidate = .{ null, null };
                    for (task.schema_fields, 0..) |field_name, field_order| {
                        const field_idx = try fullTaskFieldIndex(allocator, labels, task, field_name);
                        const candidates = try textSpanCandidatesAlloc(allocator, record, encoded, logits.span, E, raw_instance, field_idx, threshold, 0.0);
                        defer allocator.free(candidates);
                        if (candidates.len > 0) pair[field_order] = candidates[0];
                    }
                    if (pair[0] == null or pair[1] == null) continue;
                    const emitted_instance = tasks[task_idx].emitted_count;
                    try appendOwnedFieldPrediction(allocator, &fields, "head", pair[0].?, emitted_instance);
                    try appendOwnedFieldPrediction(allocator, &fields, "tail", pair[1].?, emitted_instance);
                    tasks[task_idx].emitted_count += 1;
                }
            },
        }
        allocator.free(tasks[task_idx].fields);
        tasks[task_idx].fields = try fields.toOwnedSlice(allocator);
    }
    return .{ .tasks = tasks, .entity_predictions = try entities.toOwnedSlice(allocator) };
}

fn fullTaskFieldIndex(
    allocator: std.mem.Allocator,
    labels: []const []const u8,
    task: gliner2_data.UpstreamTask,
    field: []const u8,
) !usize {
    const key = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, field);
    defer allocator.free(key);
    return indexOfLabel(labels, key) orelse error.SchemaFieldMissingFromLocalSlots;
}

fn nativeTrainerConfig(
    manifest: Manifest,
    graph_config: deberta_graph.Config,
    include_legacy_task_head: bool,
) real_autodiff.TrainerConfig {
    return .{
        .lora = .{
            .rank = @intCast(manifest.lora_rank),
            .alpha = @floatCast(manifest.lora_alpha),
            .target_patterns = manifest.lora_targets,
            .strict_target_patterns = true,
        },
        .lr_schedule = .{ .constant = 1e-3 },
        .max_grad_norm = 1.0,
        .grad_accum_steps = 1,
        .lora_a_init_std = 0.02,
        .hidden_size_hint = graph_config.hidden_size,
        .num_layers_hint = graph_config.num_hidden_layers,
        .seed = 42,
        .regular_trainable_params = if (include_legacy_task_head) &native_eval_regular_params else &.{},
        .execution_engine = .interpreter,
    };
}

const native_eval_regular_params = [_][]const u8{ "task_classifier.weight", "task_classifier.bias" };

fn replaceTaskHeadWithZeros(
    allocator: std.mem.Allocator,
    weight_store: *WeightStore,
    num_classes: usize,
    hidden_size: usize,
) !void {
    const weight_entry = weight_store.resident_weights.getPtr("task_classifier.weight") orelse return error.MissingTaskHeadWeight;
    const bias_entry = weight_store.resident_weights.getPtr("task_classifier.bias") orelse return error.MissingTaskHeadBias;
    const weight = try allocator.alloc(f32, num_classes * hidden_size);
    defer allocator.free(weight);
    @memset(weight, 0.0);
    const bias = try allocator.alloc(f32, num_classes);
    defer allocator.free(bias);
    @memset(bias, 0.0);

    const weight_shape = [_]i64{ @intCast(num_classes), @intCast(hidden_size) };
    var next_weight = try Tensor.initFloat32(allocator, weight_entry.tensor.name, &weight_shape, weight);
    errdefer next_weight.deinit();
    const bias_shape = [_]i64{@intCast(num_classes)};
    var next_bias = try Tensor.initFloat32(allocator, bias_entry.tensor.name, &bias_shape, bias);
    errdefer next_bias.deinit();

    weight_entry.deinit();
    weight_entry.* = .{ .tensor = next_weight };
    bias_entry.deinit();
    bias_entry.* = .{ .tensor = next_bias };
}

pub fn freeEntityPredictions(allocator: std.mem.Allocator, predictions: []TopEntity) void {
    freeTopEntities(allocator, predictions);
}

pub fn evalSavedAdapterText(allocator: std.mem.Allocator, opts: EvalOptions) !EvalSummary {
    return evalSavedAdapter(allocator, .{ .value = opts });
}

fn evalSavedAdapter(allocator: std.mem.Allocator, owned_opts: OwnedOptions) !EvalSummary {
    const opts = owned_opts.value;
    if (opts.seq_len == 0 or opts.seq_len > 4096) return error.InvalidSeqLen;

    var manifest = try loadManifest(allocator, opts.adapter_dir);
    defer manifest.deinit(allocator);
    const max_span_width = try resolveMaxSpanWidth(opts.max_span_width, &manifest);
    const graph_config = try loadDebertaGraphConfig(allocator, opts.model_dir);
    if (manifest.hidden_size != graph_config.hidden_size) return error.HiddenSizeMismatch;
    const objective = opts.objective_override orelse manifest.objective;
    // Full-task training uses a wider loss-only target layout. Entity
    // decoding needs only the shared schema-conditioned structure head, so
    // build its inference graph through the span-start layout.
    const inference_objective: gliner2_autodiff.GlinerObjective = if (objective == .gliner2_total_loss) .span_start else objective;

    if (opts.entity_types != null and opts.entity_types_csv != null) return error.DuplicateEntityTypeSource;
    const entity_types = if (opts.entity_types) |structured|
        try dupeStringSlice(allocator, structured)
    else if (opts.entity_types_csv) |csv|
        try parseCsv(allocator, csv)
    else if (objective == .gliner2_total_loss)
        return error.EntityTypesRequiredForTotalLoss
    else
        try dupeUserEntityLabels(allocator, manifest.entity_labels);
    errdefer freeStringSlice(allocator, entity_types);
    if (entity_types.len == 0 or entity_types.len > gliner2_autodiff.max_span_start_entity_types) return error.InvalidEntityTypes;
    if (objective == .token and entity_types.len + 1 > manifest.num_classes) return error.InvalidEntityTypes;
    try validateLabelThresholds(opts.label_thresholds, entity_types);
    try validateLabelScoreBiases(opts.label_score_biases, entity_types);
    // Total-loss manifests contain every contextual JSON/relation/
    // classification field in their training graph width. Entity decoding is
    // schema-conditioned and has no class-specific weights, so inference can
    // use the requested entity schema directly instead of padding it back to
    // the dataset-global training vocabulary.
    const inference_num_classes = if (objective == .span_start or objective == .gliner2_total_loss)
        entity_types.len + 1
    else
        manifest.num_classes;

    const task_head_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.task_head_checkpoint_file_name });
    defer allocator.free(task_head_path);
    var task_head = try gliner2_bundle.loadClassifierTaskHead(allocator, task_head_path);
    defer task_head.deinit();
    if (task_head.num_classes != manifest.num_classes or task_head.hidden_size != manifest.hidden_size) return error.TaskHeadShapeMismatch;
    var schema_task_head: gliner2_bundle.ClassifierTaskHead = undefined;
    var owns_schema_task_head = false;
    defer if (owns_schema_task_head) schema_task_head.deinit();
    const inference_task_head: *const gliner2_bundle.ClassifierTaskHead = if (objective == .span_start or objective == .gliner2_total_loss) blk: {
        // Span/schema inference carries this deprecated classifier only as a
        // zero-valued compatibility branch. Its values never affect logits,
        // and its width is local to the requested schema rather than to the
        // training dataset's slot width.
        schema_task_head = try initZeroTaskHead(allocator, inference_num_classes, manifest.hidden_size);
        owns_schema_task_head = true;
        break :blk &schema_task_head;
    } else &task_head;

    const safetensors_path = try std.fs.path.join(allocator, &.{ opts.model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);

    var loaded_base_weight_count: usize = 0;
    var native_weight_store: WeightStore = undefined;
    var native_owned_names = std.ArrayListUnmanaged([]const u8).empty;
    var native_safetensors_source: ?*SafetensorsSource = null;
    var native_backend: NativeCompute = undefined;
    var metal_weight_store: MetalWeightStore = undefined;
    var metal_backend: if (build_options.enable_metal) metal_compute.MetalCompute else void = undefined;

    const force_native = envFlag("TERMITE_GLINER2_FORCE_NATIVE");
    const metal_available = if (comptime build_options.enable_metal)
        (!force_native and metal_runtime.metalDeviceAvailable())
    else
        false;
    const selected_backend = try selectBackend(opts.backend, force_native, metal_available);

    var cb = if (selected_backend == .metal) blk: {
        if (comptime build_options.enable_metal) {
            metal_weight_store = .{
                .allocator = allocator,
                .prefix = "",
                .lazy_weights = .{},
            };
            errdefer deinitGpuHostedWeightStore(allocator, &metal_weight_store);
            loaded_base_weight_count = try loadSafetensorsIntoGpuHostedStore(allocator, &metal_weight_store, safetensors_path);
            try addTaskHeadWeightsToGpuHostedStore(allocator, &metal_weight_store, inference_task_head);
            try addParityTopLevelWeightsToGpuHostedStore(allocator, &metal_weight_store, manifest.hidden_size);
            metal_compute.initPrefetchQueue(&metal_weight_store, allocator);
            metal_backend = try metal_compute.MetalCompute.init(allocator, &metal_weight_store, null);
            break :blk metal_backend.computeBackend();
        } else unreachable;
    } else blk: {
        native_weight_store = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        };
        errdefer {
            deinitNativeWeightStore(allocator, &native_weight_store, &native_owned_names);
            if (native_safetensors_source) |src| src.weightSource().deinit();
        }
        const source_ptr = try SafetensorsSource.initAbsolute(allocator, safetensors_path);
        native_safetensors_source = source_ptr;
        var ws = source_ptr.weightSource();
        loaded_base_weight_count = try loadBaseWeightsFromSource(allocator, &ws, &native_weight_store, &native_owned_names);
        try addTaskHeadWeights(allocator, &native_weight_store, &native_owned_names, inference_task_head);
        try addParityTopLevelWeights(allocator, &native_weight_store, &native_owned_names, manifest.hidden_size);
        native_backend = NativeCompute.init(allocator, &native_weight_store, null);
        break :blk native_backend.computeBackend();
    };
    defer switch (selected_backend) {
        .native => {
            deinitNativeWeightStore(allocator, &native_weight_store, &native_owned_names);
            if (native_safetensors_source) |src| src.weightSource().deinit();
        },
        .metal => if (comptime build_options.enable_metal) deinitGpuHostedWeightStore(allocator, &metal_weight_store),
        .auto => {},
    };
    defer switch (selected_backend) {
        .metal => if (comptime build_options.enable_metal) metal_backend.deinit(),
        else => {},
    };

    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = @intCast(inference_num_classes),
        .objective = inference_objective,
    });

    const regular_trainable_params = [_][]const u8{ "task_classifier.weight", "task_classifier.bias" };
    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = .{
                .rank = @intCast(manifest.lora_rank),
                .alpha = @floatCast(manifest.lora_alpha),
                .target_patterns = manifest.lora_targets,
                .strict_target_patterns = true,
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
                .metal => .compiled_device,
                else => .interpreter,
            },
            .compiled_required = opts.compiled_required,
        },
    );
    defer trainer.deinit();

    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, opts.model_dir);
    defer tokenizer.deinit(allocator);

    try ensureGraphBuilt(
        allocator,
        &tokenizer,
        entity_types,
        opts.text,
        inference_num_classes,
        opts.seq_len,
        max_span_width,
        objective == .gliner2_total_loss,
        inference_objective,
        &trainer,
        &gliner_ctx,
    );

    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);
    try loadPeftAdaptersIntoTrainer(allocator, adapter_checkpoint_path, &trainer);
    try loadTaskHeadIntoTrainer(inference_task_head, &trainer);

    const predictions = try decodeTextEntities(
        allocator,
        &tokenizer,
        entity_types,
        opts.text,
        inference_num_classes,
        inference_objective,
        objective == .gliner2_total_loss,
        opts.seq_len,
        max_span_width,
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
        .num_classes = inference_num_classes,
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
    const schema_version = jsonString(obj.get("schema_version")) orelse return error.InvalidTrainingManifest;
    const artifact_family = jsonString(obj.get("artifact_family_version")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, schema_version, run_validation.expected_manifest_schema_version) or
        !std.mem.eql(u8, artifact_family, run_validation.expected_artifact_family_version))
    {
        return error.UnsupportedTrainingManifest;
    }
    const lora_targets = if (obj.get("resolved_lora_targets")) |targets|
        try parseGraphTargetArray(allocator, targets)
    else blk: {
        const requested = try parseCsv(allocator, jsonString(obj.get("lora_targets")) orelse return error.InvalidTrainingManifest);
        defer freeStringSlice(allocator, requested);
        break :blk try gliner2_bundle.expandLoRATargetModules(allocator, requested);
    };
    errdefer freeStringSlice(allocator, lora_targets);
    return .{
        .num_classes = jsonUsize(obj.get("num_classes")) orelse return error.InvalidTrainingManifest,
        .hidden_size = jsonUsize(obj.get("hidden_size")) orelse return error.InvalidTrainingManifest,
        .lora_rank = jsonUsize(obj.get("lora_rank")) orelse return error.InvalidTrainingManifest,
        .lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidTrainingManifest,
        .lora_targets = lora_targets,
        .entity_labels = try parseStringArray(allocator, obj.get("entity_labels") orelse return error.InvalidTrainingManifest),
        .objective = try parseObjective(jsonString(obj.get("objective")) orelse "token"),
        .max_span_width = jsonUsize(obj.get("max_span_width")) orelse 0,
    };
}

/// Trainer/upstream default used when neither the caller nor the adapter's
/// manifest pins a candidate-span width (manifests recorded before the
/// max_span_width field existed).
const default_eval_max_span_width: usize = 8;

/// Evaluating with a different span width than the adapter was trained with
/// changes the candidate-span grid, so default to the manifest's trained
/// value and treat an explicit caller width as an override.
fn resolveMaxSpanWidth(requested: ?usize, manifest: *const Manifest) !usize {
    const resolved = requested orelse
        (if (manifest.max_span_width > 0) manifest.max_span_width else default_eval_max_span_width);
    if (resolved == 0) return error.InvalidMaxSpanWidth;
    return resolved;
}

/// Resolve the effective eval span width for an adapter directory without
/// keeping the manifest open: the manifest's trained width unless
/// `requested` overrides it.
pub fn resolveAdapterMaxSpanWidth(allocator: std.mem.Allocator, adapter_dir: []const u8, requested: ?usize) !usize {
    var manifest = try loadManifest(allocator, adapter_dir);
    defer manifest.deinit(allocator);
    return resolveMaxSpanWidth(requested, &manifest);
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
        const name = try allocator.dupe(u8, "task_classifier.weight");
        try owned_names.append(allocator, name);
        const shape = [_]i64{ @intCast(head.num_classes), @intCast(head.hidden_size) };
        const tensor = try Tensor.initFloat32(allocator, name, &shape, head.weight);
        try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
    }
    {
        const name = try allocator.dupe(u8, "task_classifier.bias");
        try owned_names.append(allocator, name);
        const shape = [_]i64{@intCast(head.num_classes)};
        const tensor = try Tensor.initFloat32(allocator, name, &shape, head.bias);
        try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
    }
}

fn fillRectIdentity(data: []f32, out_dim: usize, in_dim: usize) void {
    @memset(data, 0.0);
    for (0..@min(out_dim, in_dim)) |i| data[i * in_dim + i] = 1.0;
}

const ParityWeightSpec = struct {
    name: []const u8,
    out_dim: u32,
    in_dim: u32,
};

fn parityWeightSpecs(hidden_size: u32) [25]ParityWeightSpec {
    const H = hidden_size;
    const FF = H * 4;
    const MID = H * 2;
    const COUNT: u32 = 128;
    const COUNT_FF: u32 = 256;
    return .{
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .out_dim = FF, .in_dim = H },
        .{ .name = "span_rep.span_rep_layer.project_start.3.weight", .out_dim = H, .in_dim = FF },
        .{ .name = "span_rep.span_rep_layer.project_end.0.weight", .out_dim = FF, .in_dim = H },
        .{ .name = "span_rep.span_rep_layer.project_end.3.weight", .out_dim = H, .in_dim = FF },
        .{ .name = "span_rep.span_rep_layer.out_project.0.weight", .out_dim = FF, .in_dim = H * 2 },
        .{ .name = "span_rep.span_rep_layer.out_project.3.weight", .out_dim = H, .in_dim = FF },
        .{ .name = "classifier.0.weight", .out_dim = MID, .in_dim = H },
        .{ .name = "classifier.2.weight", .out_dim = 1, .in_dim = MID },
        .{ .name = "count_pred.0.weight", .out_dim = MID, .in_dim = H },
        .{ .name = "count_pred.2.weight", .out_dim = 20, .in_dim = MID },
        .{ .name = "count_embed.pos_embedding.weight", .out_dim = 20, .in_dim = H },
        .{ .name = "count_embed.gru.weight_ih_l0", .out_dim = H * 3, .in_dim = H },
        .{ .name = "count_embed.gru.weight_hh_l0", .out_dim = H * 3, .in_dim = H },
        .{ .name = "count_embed.transformer.in_projector.weight", .out_dim = COUNT, .in_dim = H },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.in_proj_weight", .out_dim = COUNT * 3, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.out_proj.weight", .out_dim = COUNT, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear1.weight", .out_dim = COUNT_FF, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear2.weight", .out_dim = COUNT, .in_dim = COUNT_FF },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.in_proj_weight", .out_dim = COUNT * 3, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.out_proj.weight", .out_dim = COUNT, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear1.weight", .out_dim = COUNT_FF, .in_dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear2.weight", .out_dim = COUNT, .in_dim = COUNT_FF },
        .{ .name = "count_embed.transformer.out_projector.0.weight", .out_dim = H, .in_dim = H + COUNT },
        .{ .name = "count_embed.transformer.out_projector.2.weight", .out_dim = H, .in_dim = H },
        .{ .name = "count_embed.transformer.out_projector.4.weight", .out_dim = H, .in_dim = H },
    };
}

const ParityVectorFill = enum { zero, one };

const ParityVectorSpec = struct {
    name: []const u8,
    dim: u32,
    fill: ParityVectorFill = .zero,
};

fn fillVector(data: []f32, fill: ParityVectorFill) void {
    @memset(data, switch (fill) {
        .zero => 0.0,
        .one => 1.0,
    });
}

fn parityVectorSpecs(hidden_size: u32) [22]ParityVectorSpec {
    const H = hidden_size;
    const COUNT: u32 = 128;
    const COUNT_FF: u32 = 256;
    return .{
        .{ .name = "count_embed.gru.bias_ih_l0", .dim = H * 3 },
        .{ .name = "count_embed.gru.bias_hh_l0", .dim = H * 3 },
        .{ .name = "count_embed.transformer.in_projector.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.in_proj_bias", .dim = COUNT * 3 },
        .{ .name = "count_embed.transformer.transformer.layers.0.self_attn.out_proj.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm1.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm1.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear1.bias", .dim = COUNT_FF },
        .{ .name = "count_embed.transformer.transformer.layers.0.linear2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm2.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.0.norm2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.in_proj_bias", .dim = COUNT * 3 },
        .{ .name = "count_embed.transformer.transformer.layers.1.self_attn.out_proj.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm1.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm1.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear1.bias", .dim = COUNT_FF },
        .{ .name = "count_embed.transformer.transformer.layers.1.linear2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm2.weight", .dim = COUNT, .fill = .one },
        .{ .name = "count_embed.transformer.transformer.layers.1.norm2.bias", .dim = COUNT },
        .{ .name = "count_embed.transformer.out_projector.0.bias", .dim = H },
        .{ .name = "count_embed.transformer.out_projector.2.bias", .dim = H },
        .{ .name = "count_embed.transformer.out_projector.4.bias", .dim = H },
    };
}

fn addParityTopLevelWeights(
    allocator: std.mem.Allocator,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
    hidden_size: usize,
) !void {
    const specs = parityWeightSpecs(@intCast(hidden_size));
    for (specs) |spec| {
        if (weight_store.lazy_weights.contains(spec.name)) continue;
        const out_dim: usize = @intCast(spec.out_dim);
        const in_dim: usize = @intCast(spec.in_dim);
        const data = try allocator.alloc(f32, out_dim * in_dim);
        defer allocator.free(data);
        fillRectIdentity(data, out_dim, in_dim);
        const name = try allocator.dupe(u8, spec.name);
        const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
        const tensor = try Tensor.initFloat32(allocator, name, &shape, data);
        try putMissingOwnedResidentWeight(allocator, weight_store, owned_names, name, LoadedWeight{ .tensor = tensor });
    }
    const vector_specs = parityVectorSpecs(@intCast(hidden_size));
    for (vector_specs) |spec| {
        const dim: usize = @intCast(spec.dim);
        const data = try allocator.alloc(f32, dim);
        defer allocator.free(data);
        fillVector(data, spec.fill);
        const name = try allocator.dupe(u8, spec.name);
        const shape = [_]i64{@intCast(dim)};
        const tensor = try Tensor.initFloat32(allocator, name, &shape, data);
        try putMissingOwnedResidentWeight(allocator, weight_store, owned_names, name, LoadedWeight{ .tensor = tensor });
    }
}

fn putMissingOwnedResidentWeight(
    allocator: std.mem.Allocator,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
    name: []const u8,
    weight: LoadedWeight,
) !void {
    if (weight_store.resident_weights.contains(name)) {
        var discard = weight;
        discard.deinit();
        allocator.free(name);
        return;
    }
    try owned_names.append(allocator, name);
    try weight_store.resident_weights.put(allocator, name, weight);
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
        const tensor = try Tensor.initFloat32(allocator, "task_classifier.weight", &shape, head.weight);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "task_classifier.weight"), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
    {
        const shape = [_]i64{@intCast(head.num_classes)};
        const tensor = try Tensor.initFloat32(allocator, "task_classifier.bias", &shape, head.bias);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, "task_classifier.bias"), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
}

fn addParityTopLevelWeightsToGpuHostedStore(
    allocator: std.mem.Allocator,
    weight_store: *MetalWeightStore,
    hidden_size: usize,
) !void {
    if (comptime !build_options.enable_metal) return error.MetalBackendUnavailable;
    const specs = parityWeightSpecs(@intCast(hidden_size));
    for (specs) |spec| {
        if (weight_store.lazy_weights.contains(spec.name)) continue;
        const out_dim: usize = @intCast(spec.out_dim);
        const in_dim: usize = @intCast(spec.in_dim);
        const data = try allocator.alloc(f32, out_dim * in_dim);
        defer allocator.free(data);
        fillRectIdentity(data, out_dim, in_dim);
        const shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };
        const tensor = try Tensor.initFloat32(allocator, spec.name, &shape, data);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, spec.name), .{
            .tensor_ref = undefined,
            .host_loaded = .{ .tensor = tensor },
            .active_tier = .host,
            .loaded_bytes = tensor.data.len,
        });
    }
    const vector_specs = parityVectorSpecs(@intCast(hidden_size));
    for (vector_specs) |spec| {
        if (weight_store.lazy_weights.contains(spec.name)) continue;
        const dim: usize = @intCast(spec.dim);
        const data = try allocator.alloc(f32, dim);
        defer allocator.free(data);
        fillVector(data, spec.fill);
        const shape = [_]i64{@intCast(dim)};
        const tensor = try Tensor.initFloat32(allocator, spec.name, &shape, data);
        try weight_store.lazy_weights.put(allocator, try allocator.dupe(u8, spec.name), .{
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
    use_upstream_word_splitting: bool,
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
        .span_start, .gliner2_total_loss => {
            var encoded = try buildEntitySpanBatch(
                allocator,
                tokenizer,
                entity_types,
                text,
                seq_len,
                max_span_width,
                use_upstream_word_splitting,
            );
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
    use_upstream_word_splitting: bool,
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
        .span_start, .gliner2_total_loss => decodeTextEntitiesFromSpanStart(
            allocator,
            tokenizer,
            entity_types,
            text,
            seq_len,
            max_span_width,
            use_upstream_word_splitting,
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
    use_upstream_word_splitting: bool,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
    prediction_threshold: ?f32,
    label_score_biases: []const LabelScoreBias,
) ![]TopEntity {
    const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
    var decoded_batch = try buildEntitySpanBatch(
        allocator,
        tokenizer,
        entity_types,
        text,
        seq_len,
        max_span_width,
        use_upstream_word_splitting,
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

fn buildEntitySpanBatch(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    seq_len: usize,
    max_span_width: usize,
    use_upstream_word_splitting: bool,
) !gliner2_data.EncodedBatch {
    if (!use_upstream_word_splitting) {
        const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
        return gliner2_data.buildSimpleBatch(allocator, tokenizer, &examples, entity_types, seq_len, max_span_width, 1);
    }
    const task = gliner2_data.UpstreamTask{
        .kind = .entities,
        .name = "entities",
        .schema_fields = entity_types,
        .count = 1,
    };
    const record = gliner2_data.UpstreamRecord{ .text = text, .tasks = &.{task} };
    return gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
        allocator,
        tokenizer,
        &.{record},
        entity_types.len,
        seq_len,
        max_span_width,
        1,
    );
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
    try loadPeftAdaptersFromReader(allocator, &reader, trainer);
}

fn loadPeftAdaptersFromReader(
    allocator: std.mem.Allocator,
    reader: *safetensors.MMapReader,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    for (trainer.lora_params.items) |*slot| {
        const peft_name = try gliner2_bundle.autodiffParamNameToPeftName(allocator, slot.name);
        defer allocator.free(peft_name);
        var tensor = reader.readTensor(peft_name) catch blk: {
            const legacy_name = try autodiffSlotNameToLegacyPeftName(allocator, slot.name);
            defer allocator.free(legacy_name);
            break :blk try reader.readTensor(legacy_name);
        };
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
        if (std.mem.eql(u8, slot.name, "task_classifier.weight")) {
            if (slot.weights.len != head.weight.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.weight);
            @memset(slot.grad_accum, 0.0);
        } else if (std.mem.eql(u8, slot.name, "task_classifier.bias")) {
            if (slot.weights.len != head.bias.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.bias);
            @memset(slot.grad_accum, 0.0);
        }
    }
}

fn initZeroTaskHead(
    allocator: std.mem.Allocator,
    num_classes: usize,
    hidden_size: usize,
) !gliner2_bundle.ClassifierTaskHead {
    const weight = try allocator.alloc(f32, num_classes * hidden_size);
    errdefer allocator.free(weight);
    const bias = try allocator.alloc(f32, num_classes);
    @memset(weight, 0.0);
    @memset(bias, 0.0);
    return .{
        .allocator = allocator,
        .weight = weight,
        .bias = bias,
        .num_classes = num_classes,
        .hidden_size = hidden_size,
    };
}

fn copyTensorF32Into(dst: []f32, tensor: *const Tensor) !void {
    if (tensor.dtype != .f32) return error.AdapterTensorDTypeMismatch;
    if (tensor.data.len != dst.len * @sizeOf(f32)) return error.AdapterTensorShapeMismatch;
    for (dst, 0..) |*value, idx| {
        const raw = tensor.data[idx * @sizeOf(f32) ..][0..@sizeOf(f32)];
        value.* = @bitCast(std.mem.readInt(u32, raw, .little));
    }
}

fn autodiffSlotNameToLegacyPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
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
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| allocator.free(item);
        allocator.free(out);
    }
    for (value.array.items, 0..) |entry, idx| {
        if (entry != .string) return error.InvalidTrainingManifest;
        out[idx] = try allocator.dupe(u8, entry.string);
        initialized += 1;
    }
    return out;
}

fn parseGraphTargetArray(allocator: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidTrainingManifest;
    var out = try allocator.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| allocator.free(item);
        allocator.free(out);
    }
    for (value.array.items, 0..) |entry, idx| {
        if (entry != .string) return error.InvalidTrainingManifest;
        out[idx] = try allocator.dupe(u8, stripEncoderPrefix(entry.string));
        initialized += 1;
    }
    return out;
}

test "resolved PEFT encoder targets map back to graph namespace" {
    const allocator = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, "[\"encoder.encoder.layer.0.attention.self.query_proj\",\"classifier.0\"]", .{});
    defer parsed.deinit();
    const targets = try parseGraphTargetArray(allocator, parsed.value);
    defer freeStringSlice(allocator, targets);
    try std.testing.expectEqualStrings("encoder.layer.0.attention.self.query_proj", targets[0]);
    try std.testing.expectEqualStrings("classifier.0", targets[1]);
}

fn dupeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) ![][]const u8 {
    var out = try allocator.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| allocator.free(item);
        allocator.free(out);
    }
    for (values, 0..) |value, idx| {
        out[idx] = try allocator.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn dupeUserEntityLabels(allocator: std.mem.Allocator, manifest_labels: []const []const u8) ![][]const u8 {
    var count: usize = 0;
    for (manifest_labels) |label| {
        if (!std.mem.startsWith(u8, label, "@gliner2:")) count += 1;
    }
    var out = try allocator.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| allocator.free(item);
        allocator.free(out);
    }
    for (manifest_labels) |label| {
        if (std.mem.startsWith(u8, label, "@gliner2:")) continue;
        out[initialized] = try allocator.dupe(u8, label);
        initialized += 1;
    }
    return out;
}

test "full-task manifests expose only user entity labels to entity decoding" {
    const allocator = std.testing.allocator;
    const labels = [_][]const u8{
        "person",
        "@gliner2:c:9:sentiment:8:positive",
        "organization",
        "@gliner2:j:7:product:5:price",
    };
    const entity_labels = try dupeUserEntityLabels(allocator, &labels);
    defer freeStringSlice(allocator, entity_labels);
    try std.testing.expectEqual(@as(usize, 2), entity_labels.len);
    try std.testing.expectEqualStrings("person", entity_labels[0]);
    try std.testing.expectEqualStrings("organization", entity_labels[1]);
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn parseObjective(value: []const u8) !gliner2_autodiff.GlinerObjective {
    if (std.mem.eql(u8, value, "token")) return .token;
    if (std.mem.eql(u8, value, "span-start") or std.mem.eql(u8, value, "span_start")) return .span_start;
    if (std.mem.eql(u8, value, "gliner2-total-loss") or std.mem.eql(u8, value, "gliner2_total_loss")) return .gliner2_total_loss;
    return error.InvalidObjective;
}

fn objectiveName(objective: gliner2_autodiff.GlinerObjective) []const u8 {
    return switch (objective) {
        .token => "token",
        .span_start => "span-start",
        .gliner2_total_loss => "gliner2-total-loss",
    };
}

fn parseBackend(value: []const u8) ?EvalBackend {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    return null;
}

fn selectBackend(
    requested: EvalBackend,
    force_native: bool,
    metal_available: bool,
) !EvalBackend {
    if (force_native) return .native;
    return switch (requested) {
        .auto => if (metal_available) .metal else .native,
        .metal => if (metal_available) .metal else error.MetalBackendUnavailable,
        .native => .native,
    };
}

fn backendLabel(backend: EvalBackend) []const u8 {
    return switch (backend) {
        .auto => "auto",
        .metal => "Metal",
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
        \\  --entity-types CSV        Required for gliner2-total-loss artifacts
        \\  --seq-len N
        \\  --max-span-width N        Default: adapter manifest's trained width
        \\  --backend auto|metal|native
        \\  --compiled-required
        \\  --objective token|span-start|gliner2-total-loss
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
        \\  --upstream-entity-decode
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

test "full-task classification decoder matches upstream single and multi label rules" {
    const allocator = std.testing.allocator;
    const positive_key = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "sentiment", "positive");
    defer allocator.free(positive_key);
    const negative_key = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "sentiment", "negative");
    defer allocator.free(negative_key);
    const labels = [_][]const u8{ positive_key, negative_key };

    const single_task = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "sentiment",
        .labels = &.{ "positive", "negative" },
    };
    const single = try decodeClassificationTaskAlloc(allocator, single_task, 0, &labels, &.{ 1.0, 3.0 });
    defer {
        for (single) |prediction| allocator.free(prediction.label);
        allocator.free(single);
    }
    try std.testing.expectEqual(@as(usize, 1), single.len);
    try std.testing.expectEqualStrings("negative", single[0].label);
    try std.testing.expect(single[0].score > 0.8);

    var multi_task = single_task;
    multi_task.multi_label = true;
    const multi = try decodeClassificationTaskAlloc(allocator, multi_task, 0, &labels, &.{ 1.0, 0.0 });
    defer {
        for (multi) |prediction| allocator.free(prediction.label);
        allocator.free(multi);
    }
    try std.testing.expectEqual(@as(usize, 2), multi.len);
    try std.testing.expectEqualStrings("positive", multi[0].label);
    try std.testing.expectEqualStrings("negative", multi[1].label);

    const fallback = try decodeClassificationTaskAlloc(allocator, multi_task, 0, &labels, &.{ -2.0, -1.0 });
    defer {
        for (fallback) |prediction| allocator.free(prediction.label);
        allocator.free(fallback);
    }
    try std.testing.expectEqual(@as(usize, 1), fallback.len);
    try std.testing.expectEqualStrings("negative", fallback[0].label);
}

test "schema-only full-task evaluation preserves conditioning metadata" {
    const allocator = std.testing.allocator;
    const fields = [_]gliner2_data.UpstreamField{.{ .name = "person", .value = "Alice" }};
    const tasks = [_]gliner2_data.UpstreamTask{
        .{
            .kind = .entities,
            .name = "entities",
            .label_descriptions = &.{.{ .label = "person", .description = "a human" }},
            .fields = &fields,
        },
        .{
            .kind = .classifications,
            .name = "sentiment",
            .labels = &.{ "positive", "negative" },
            .true_labels = &.{"positive"},
            .multi_label = true,
            .prompt = "Choose a sentiment.",
            .label_descriptions = &.{.{ .label = "positive", .description = "favorable" }},
            .examples = &.{.{ .input = "Great.", .output = "positive" }},
        },
    };
    var schema_only = try schemaOnlyTasksAlloc(allocator, &tasks);
    defer schema_only.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), schema_only.tasks[0].fields.len);
    try std.testing.expectEqual(@as(usize, 1), schema_only.tasks[0].schema_fields.len);
    try std.testing.expectEqualStrings("person", schema_only.tasks[0].schema_fields[0]);
    try std.testing.expectEqualStrings("a human", schema_only.tasks[0].label_descriptions[0].description);
    try std.testing.expectEqual(@as(usize, 0), schema_only.tasks[1].true_labels.len);
    try std.testing.expect(schema_only.tasks[1].multi_label);
    try std.testing.expectEqualStrings("Choose a sentiment.", schema_only.tasks[1].prompt.?);
    try std.testing.expectEqualStrings("Great.", schema_only.tasks[1].examples[0].input);
}

test "full-task relation evaluation fails closed outside ordered head tail" {
    const unsupported = [_]gliner2_data.UpstreamTask{.{
        .kind = .relations,
        .name = "works_for",
        .schema_fields = &.{ "head", "tail", "place" },
        .fields = &.{
            .{ .name = "head", .value = "Alice" },
            .{ .name = "tail", .value = "Acme" },
            .{ .name = "place", .value = "Seattle" },
        },
        .count = 1,
    }};
    try std.testing.expectError(
        error.UnsupportedRelationFields,
        validateFullTaskRecord(.{ .text = "Alice joined Acme in Seattle.", .tasks = &unsupported }),
    );
}
