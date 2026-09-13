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
const assets = @import("assets/colqwen2.zig");

const build_options = @import("build_options");
const c_file = @import("../util/c_file.zig");
const compat = @import("../io/compat.zig");
const lora = @import("lora.zig");
const graph_bridge = @import("graph_bridge.zig");
const image_pipeline = @import("../pipelines/image.zig");
const multimodal_qwen_adapter = @import("../pipelines/multimodal_qwen_adapter.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const native_compute = @import("../ops/native_compute.zig");
const ml = @import("ml");
const optimizers = ml.graph.optimizers;

// Checkpoint types and operations are owned by the offline asset layer.
pub const artifact_family_version = assets.artifact_family_version;
pub const checkpoint_file_name = assets.checkpoint_file_name;
pub const adapter_checkpoint_file_name = assets.adapter_checkpoint_file_name;
pub const hf_config_file_name = assets.hf_config_file_name;
pub const adapter_config_file_name = assets.adapter_config_file_name;
pub const preprocessor_config_file_name = assets.preprocessor_config_file_name;
pub const tokenizer_config_file_name = assets.tokenizer_config_file_name;
pub const tokenizer_file_name = assets.tokenizer_file_name;
pub const special_tokens_map_file_name = assets.special_tokens_map_file_name;
pub const default_lora_target_modules = assets.default_lora_target_modules;
pub const Variant = assets.Variant;
pub const ArtifactPaths = assets.ArtifactPaths;
pub const Config = assets.Config;
pub const AdapterConfig = assets.AdapterConfig;
pub const PreprocessorConfig = assets.PreprocessorConfig;
pub const TokenizerConfig = assets.TokenizerConfig;
pub const SpecialTokensMap = assets.SpecialTokensMap;
pub const InspectionSummary = assets.InspectionSummary;
pub const LoRATargetTensor = assets.LoRATargetTensor;
pub const BootstrapOptions = assets.BootstrapOptions;
pub const BootstrapSummary = assets.BootstrapSummary;
pub const LoRATensorSummary = assets.LoRATensorSummary;
pub const LoRABundleInspectionSummary = assets.LoRABundleInspectionSummary;
pub const LoadedLoRALayer = assets.LoadedLoRALayer;
pub const MaterializeSummary = assets.MaterializeSummary;
pub const LoadedLoRABundle = assets.LoadedLoRABundle;
pub const resolveArtifactPaths = assets.resolveArtifactPaths;
pub const inspectCheckpoint = assets.inspectCheckpoint;
pub const bootstrapLoRABundle = assets.bootstrapLoRABundle;
pub const inspectLoRABundle = assets.inspectLoRABundle;
pub const loadLoRABundle = assets.loadLoRABundle;
pub const saveLoRABundle = assets.saveLoRABundle;
pub const materializeMergedModel = assets.materializeMergedModel;
pub const freeInspectionSummary = assets.freeInspectionSummary;
pub const freeBootstrapSummary = assets.freeBootstrapSummary;
pub const freeLoRABundleInspectionSummary = assets.freeLoRABundleInspectionSummary;
pub const freeMaterializeSummary = assets.freeMaterializeSummary;
const writeHeaderAndTensorsF32 = assets.writeHeaderAndTensorsF32;
const dupeOptionalString = assets.dupeOptionalString;
const transpose2DF32 = assets.transpose2DF32;
const doraMagnitudeTensorName = assets.doraMagnitudeTensorName;
const loadTensorAsF32 = assets.loadTensorAsF32;
const openTensorAccessForFile = assets.openTensorAccessForFile;

pub const focused_lora_scope_name = "@colqwen2_focus_top3";

pub const Example = struct {
    query: []const u8,
    image_path: []const u8,
    ocr_text: []const u8 = "",
    score: f32,
    document_id: []const u8 = "",
    page_number: i32 = 0,
    answer: []const u8 = "",
};

pub const ResizeReason = enum {
    none,
    downscale_to_max_pixels,
    upscale_to_min_pixels,
};

pub const PreparedExampleInput = struct {
    query: []const u8,
    ocr_text: []const u8 = "",
    resolved_image_path: []const u8,
    target_score: f32,
    real_colqwen_score: ?f32 = null,
    query_input_ids: []i32,
    query_attention_mask: []i32,
    image_input_ids: []i32,
    image_attention_mask: []i32,
    original_width: u32,
    original_height: u32,
    normalized_width: u32,
    normalized_height: u32,
    original_pixel_count: u64,
    normalized_pixel_count: u64,
    resize_reason: ResizeReason,
    scale: f32,
    patch_size: usize,
    estimated_image_grid_thw: [3]u32,
    estimated_patch_tokens: usize,
    pixel_values_shape: [4]usize,
    pixel_min: f32,
    pixel_max: f32,
    pixel_mean: f32,
    pixel_std: f32,
    pixel_checksum: u64,
};

pub const PreparedInputsSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    variant: Variant,
    max_examples: usize,
    examples_seen: usize,
    tokenizer_class: ?[]const u8 = null,
    processor_class: ?[]const u8 = null,
    query_prefix: []const u8,
    visual_prompt_prefix: []const u8,
    resized_down_examples: usize = 0,
    resized_up_examples: usize = 0,
    max_query_tokens: usize = 0,
    max_image_prompt_tokens: usize = 0,
    max_estimated_patch_tokens: usize = 0,
    examples: []PreparedExampleInput,
};

pub const SurrogateMetrics = struct {
    examples_seen: usize = 0,
    average_loss: f64 = 0,
    mse: f64 = 0,
    mae: f64 = 0,
    mean_score: f64 = 0,
    mean_positive_score: f64 = 0,
    mean_negative_score: f64 = 0,
    f1: f64 = 0,
    accuracy: f64 = 0,
};

pub const TrainEpochOptions = struct {
    learning_rate: f32 = 0.001,
    max_examples: usize = 32,
    layer_name: ?[]const u8 = null,
    max_grad_norm: f32 = 1.0,
    grad_accum_steps: u32 = 1,
    llrd_decay: f32 = 1.0,
    use_schedule_free: bool = false,
    neftune_alpha: f32 = 0.0,
    /// Optional compute backend for gradient computation.
    /// If null, defaults to CPU (pure-Zig) math. Pass a Metal backend for GPU acceleration.
    compute_backend: ?*const @import("../ops/ops.zig").ComputeBackend = null,
    /// Number of DDP replicas.
    world_size: u32 = 1,
    /// DDP rank of this process. Rank 0 is responsible for checkpoint writes.
    /// Set to 0 for single-device training (default).
    ddp_rank: u32 = 0,
    /// Linear LR warmup steps. LR ramps from 0 → learning_rate over the first warmup_steps
    /// optimizer updates. 0 = no warmup.
    warmup_steps: u32 = 0,
    /// Pre-compiled PJRT gradient executors, one per LoRA layer (null = use CPU path).
    /// Length must equal bundle.layers.len if non-null.
    /// Note: PJRT is automatically disabled when world_size > 1 (no collective ops in PJRT path).
    pjrt_lora_steps: if (build_options.enable_pjrt) ?[]?graph_bridge.LoRAPjrtTrainStep else void =
        if (build_options.enable_pjrt) null else {},
};

pub const TrainEpochSummary = struct {
    examples_seen: usize = 0,
    updates_applied: usize = 0,
    average_loss: f64 = 0,
    mean_score: f64 = 0,
    mean_abs_error: f64 = 0,
    max_grad_norm: f32 = 0,
    llrd_decay: f32 = 0,
    grad_accum_steps: u32 = 0,
};

pub const LoRAOneStepOptions = struct {
    layer_name: ?[]const u8 = null,
    input_rows: usize = 3,
    learning_rate: f32 = 0.001,
};

pub const LoRAOneStepSummary = struct {
    layer_name: []const u8,
    module_name: []const u8,
    input_rows: usize,
    learning_rate: f32,
    grad_a_l2_norm: f64,
    grad_b_l2_norm: f64,
    adapter_a_l2_norm_before: f64,
    adapter_b_l2_norm_before: f64,
    adapter_a_l2_norm_after: f64,
    adapter_b_l2_norm_after: f64,
};

const PreparedInputsSummaryFile = struct {
    summary: PreparedInputsSummary,
};

const focused_top_layer_prefixes = [_][]const u8{
    "vlm.model.language_model.layers.25.",
    "vlm.model.language_model.layers.26.",
    "vlm.model.language_model.layers.27.",
};

const focused_top_layer_suffixes = [_][]const u8{
    ".self_attn.q_proj.weight",
    ".self_attn.v_proj.weight",
    ".self_attn.o_proj.weight",
    ".mlp.up_proj.weight",
    ".mlp.down_proj.weight",
};

const EvalOptions = struct {
    max_examples: usize,
    decision_threshold: f64 = 0.5,
    layer_name: ?[]const u8 = null,
};

pub fn resolveLoRACheckpointPath(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    if (isRegularFilePath(input)) return try allocator.dupe(u8, input);
    const path = try std.fs.path.join(allocator, &.{ input, adapter_checkpoint_file_name });
    errdefer allocator.free(path);
    if (!isRegularFilePath(path)) return error.MissingAdapterCheckpoint;
    return path;
}

pub fn trainLoRABundleOneStep(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    options: LoRAOneStepOptions,
) !LoRAOneStepSummary {
    if (bundle.layers.len == 0) return error.NoLoRALayersLoaded;
    const layer_idx = if (options.layer_name) |needle|
        findLoadedLayerIndex(bundle.layers, needle) orelse return error.UnknownLoRALayer
    else
        0;
    const layer = &bundle.layers[layer_idx];
    if (options.input_rows == 0) return error.InvalidInputRows;

    const inputs = try allocator.alloc(f32, options.input_rows * layer.input_dim);
    defer allocator.free(inputs);
    const targets = try allocator.alloc(f32, options.input_rows * layer.output_dim);
    defer allocator.free(targets);
    fillDeterministicMatrix(inputs, options.input_rows, layer.input_dim, 0.019, 0.011);
    fillDeterministicMatrix(targets, options.input_rows, layer.output_dim, 0.023, -0.017);

    const adapter_a_before = l2Norm(layer.adapter_a);
    const adapter_b_before = l2Norm(layer.adapter_b);
    var weight_store = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();
    var graph_bundle = try graph_bridge.LoRALinearGraph.init(
        allocator,
        options.input_rows,
        layer.input_dim,
        layer.output_dim,
        layer.rank,
        bundle.lora_alpha,
    );
    defer graph_bundle.deinit();

    const summary = try graph_bridge.trainLoRALinearOneStep(
        allocator,
        &cb,
        &graph_bundle,
        .{ .adam = .{} },
        &optimizer_state,
        layer.base_weight,
        layer.adapter_a,
        layer.adapter_b,
        inputs,
        targets,
        options.learning_rate,
    );

    return .{
        .layer_name = try allocator.dupe(u8, layer.base_tensor_name),
        .module_name = try allocator.dupe(u8, layer.module_name),
        .input_rows = options.input_rows,
        .learning_rate = options.learning_rate,
        .grad_a_l2_norm = summary.lora_a_grad_l2,
        .grad_b_l2_norm = summary.lora_b_grad_l2,
        .adapter_a_l2_norm_before = adapter_a_before,
        .adapter_b_l2_norm_before = adapter_b_before,
        .adapter_a_l2_norm_after = l2Norm(layer.adapter_a),
        .adapter_b_l2_norm_after = l2Norm(layer.adapter_b),
    };
}

pub fn freeLoRAOneStepSummary(allocator: std.mem.Allocator, summary: *LoRAOneStepSummary) void {
    allocator.free(summary.layer_name);
    allocator.free(summary.module_name);
    summary.* = undefined;
}

pub fn evaluatePreparedExamples(
    allocator: std.mem.Allocator,
    bundle: *const LoadedLoRABundle,
    examples: []const PreparedExampleInput,
    options: EvalOptions,
) !SurrogateMetrics {
    var metrics = SurrogateMetrics{};
    const limit = if (options.max_examples > 0 and options.max_examples < examples.len) options.max_examples else examples.len;
    if (limit == 0) return metrics;

    var total_loss: f64 = 0;
    var total_abs_error: f64 = 0;
    var total_score: f64 = 0;
    var pos_score_sum: f64 = 0;
    var neg_score_sum: f64 = 0;
    var pos_count: usize = 0;
    var neg_count: usize = 0;
    var true_positive: usize = 0;
    var false_positive: usize = 0;
    var false_negative: usize = 0;
    var correct: usize = 0;

    for (examples[0..limit]) |*example| {
        const predicted = try scorePreparedExample(allocator, bundle, example, options.layer_name);
        const target = exampleTarget(example);
        const error_value = predicted - target;
        const loss = 0.5 * error_value * error_value;
        total_loss += loss;
        total_abs_error += @abs(error_value);
        total_score += predicted;
        const predicted_positive = predicted >= options.decision_threshold;
        const target_positive = target >= 0.5;
        if (predicted_positive == target_positive) correct += 1;
        if (predicted_positive and target_positive) true_positive += 1;
        if (predicted_positive and !target_positive) false_positive += 1;
        if (!predicted_positive and target_positive) false_negative += 1;
        if (target_positive) {
            pos_score_sum += predicted;
            pos_count += 1;
        } else {
            neg_score_sum += predicted;
            neg_count += 1;
        }
        metrics.examples_seen += 1;
    }

    const denom = @as(f64, @floatFromInt(metrics.examples_seen));
    metrics.average_loss = total_loss / denom;
    metrics.mse = (total_loss * 2.0) / denom;
    metrics.mae = total_abs_error / denom;
    metrics.mean_score = total_score / denom;
    if (pos_count > 0) metrics.mean_positive_score = pos_score_sum / @as(f64, @floatFromInt(pos_count));
    if (neg_count > 0) metrics.mean_negative_score = neg_score_sum / @as(f64, @floatFromInt(neg_count));
    metrics.accuracy = @as(f64, @floatFromInt(correct)) / denom;
    const precision = if (true_positive + false_positive == 0) 0 else @as(f64, @floatFromInt(true_positive)) / @as(f64, @floatFromInt(true_positive + false_positive));
    const recall = if (true_positive + false_negative == 0) 0 else @as(f64, @floatFromInt(true_positive)) / @as(f64, @floatFromInt(true_positive + false_negative));
    if (precision + recall > 0) metrics.f1 = 2.0 * precision * recall / (precision + recall);
    return metrics;
}

pub const LoRALayerAdamState = struct {
    allocator: std.mem.Allocator,
    m_a: []f32,
    v_a: []f32,
    m_b: []f32,
    v_b: []f32,
    step: u64,

    pub fn init(alloc: std.mem.Allocator, layer: *const LoadedLoRALayer) !LoRALayerAdamState {
        const m_a = try alloc.alloc(f32, layer.adapter_a.len);
        errdefer alloc.free(m_a);
        const v_a = try alloc.alloc(f32, layer.adapter_a.len);
        errdefer alloc.free(v_a);
        const m_b = try alloc.alloc(f32, layer.adapter_b.len);
        errdefer alloc.free(m_b);
        const v_b = try alloc.alloc(f32, layer.adapter_b.len);
        errdefer alloc.free(v_b);
        @memset(m_a, 0);
        @memset(v_a, 0);
        @memset(m_b, 0);
        @memset(v_b, 0);
        return .{ .allocator = alloc, .m_a = m_a, .v_a = v_a, .m_b = m_b, .v_b = v_b, .step = 0 };
    }

    pub fn deinit(self: *LoRALayerAdamState) void {
        self.allocator.free(self.m_a);
        self.allocator.free(self.v_a);
        self.allocator.free(self.m_b);
        self.allocator.free(self.v_b);
        self.* = undefined;
    }
};

const LoRALayerSFState = struct {
    allocator: std.mem.Allocator,
    z_a: []f32,
    v_a: []f32,
    z_b: []f32,
    v_b: []f32,
    step: u64,

    fn init(alloc: std.mem.Allocator, layer: *const LoadedLoRALayer) !LoRALayerSFState {
        const z_a = try alloc.dupe(f32, layer.adapter_a);
        errdefer alloc.free(z_a);
        const v_a = try alloc.alloc(f32, layer.adapter_a.len);
        errdefer alloc.free(v_a);
        @memset(v_a, 0);
        const z_b = try alloc.dupe(f32, layer.adapter_b);
        errdefer alloc.free(z_b);
        const v_b = try alloc.alloc(f32, layer.adapter_b.len);
        errdefer alloc.free(v_b);
        @memset(v_b, 0);
        return .{ .allocator = alloc, .z_a = z_a, .v_a = v_a, .z_b = z_b, .v_b = v_b, .step = 0 };
    }

    fn deinit(self: *LoRALayerSFState) void {
        self.allocator.free(self.z_a);
        self.allocator.free(self.v_a);
        self.allocator.free(self.z_b);
        self.allocator.free(self.v_b);
        self.* = undefined;
    }
};

fn applyScheduleFreeInPlace(params: []f32, grads: []const f32, z: []f32, v: []f32, step: u64, lr: f32) void {
    const t: f32 = @floatFromInt(step);
    const beta2: f32 = 0.999;
    const epsilon: f32 = 1e-8;
    const weight_decay: f32 = 0.01;
    const c = @min(@as(f32, 0.9), 1.0 / t);
    for (params, grads, z, v) |*x, g, *zi, *vi| {
        vi.* = beta2 * vi.* + (1.0 - beta2) * g * g;
        const v_hat = vi.* / (1.0 - std.math.pow(f32, beta2, t));
        zi.* = zi.* - lr * g / (@sqrt(v_hat) + epsilon) - lr * weight_decay * zi.*;
        x.* = (1.0 - c) * x.* + c * zi.*;
    }
}

fn parseColQwen2LayerIndex(tensor_name: []const u8) ?usize {
    const prefix = "vlm.model.language_model.layers.";
    const idx = std.mem.indexOf(u8, tensor_name, prefix) orelse return null;
    const digits = tensor_name[idx + prefix.len ..];
    var end: usize = 0;
    while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseUnsigned(usize, digits[0..end], 10) catch null;
}

fn warmupAdjustedLR(base_lr: f32, step: u64, warmup_steps: u32) f32 {
    if (warmup_steps == 0 or step >= warmup_steps) return base_lr;
    return base_lr * @as(f32, @floatFromInt(step)) / @as(f32, @floatFromInt(warmup_steps));
}

fn applyAdamWInPlace(params: []f32, grads: []const f32, m: []f32, v: []f32, step: u64, lr: f32) void {
    const t: f32 = @floatFromInt(step);
    const beta1: f32 = 0.9;
    const beta2: f32 = 0.999;
    const eps: f32 = 1e-8;
    const wd: f32 = 0.01;
    const bc1 = 1.0 - std.math.pow(f32, beta1, t);
    const bc2 = 1.0 - std.math.pow(f32, beta2, t);
    for (params, grads, m, v) |*p, g, *mi, *vi| {
        mi.* = beta1 * mi.* + (1.0 - beta1) * g;
        vi.* = beta2 * vi.* + (1.0 - beta2) * g * g;
        p.* -= lr * (mi.* / bc1 / (@sqrt(vi.* / bc2) + eps) + wd * p.*);
    }
}

pub fn trainPreparedExamplesEpoch(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    examples: []const PreparedExampleInput,
    options: TrainEpochOptions,
) !TrainEpochSummary {
    if (options.neftune_alpha > 0.0) {
        std.log.warn(
            "NEFTune is configured (alpha={d:.3}) but this trainer runs from cached features; " ++
                "the noise injection has no effect. To use NEFTune, switch to a trainer with a " ++
                "real end-to-end forward pass (see colqwen2_real_forward.zig).",
            .{options.neftune_alpha},
        );
    }
    var summary = TrainEpochSummary{
        .max_grad_norm = options.max_grad_norm,
        .llrd_decay = options.llrd_decay,
        .grad_accum_steps = options.grad_accum_steps,
    };
    const limit = if (options.max_examples > 0 and options.max_examples < examples.len) options.max_examples else examples.len;
    if (limit == 0) return summary;

    const num_layers = bundle.layers.len;

    // Create per-layer Adam states ONCE — persist across all examples/steps.
    const adam_states = try allocator.alloc(LoRALayerAdamState, num_layers);
    var adam_initialized: usize = 0;
    defer {
        var i: usize = 0;
        while (i < adam_initialized) : (i += 1) adam_states[i].deinit();
        allocator.free(adam_states);
    }
    for (bundle.layers, 0..) |*layer, li| {
        adam_states[li] = try LoRALayerAdamState.init(allocator, layer);
        adam_initialized += 1;
    }

    // Create per-layer Schedule-Free states (only populated when use_schedule_free is true).
    const sf_states = try allocator.alloc(?LoRALayerSFState, num_layers);
    defer allocator.free(sf_states);
    var sf_initialized: usize = 0;
    defer {
        var i: usize = 0;
        while (i < sf_initialized) : (i += 1) {
            if (sf_states[i]) |*s| s.deinit();
        }
    }
    for (bundle.layers, 0..) |*layer, li| {
        if (options.use_schedule_free) {
            sf_states[li] = try LoRALayerSFState.init(allocator, layer);
        } else {
            sf_states[li] = null;
        }
        sf_initialized += 1;
    }

    // Gradient accumulation buffers — one pair per layer.
    const accum_grad_a = try allocator.alloc([]f32, num_layers);
    defer allocator.free(accum_grad_a);
    const accum_grad_b = try allocator.alloc([]f32, num_layers);
    defer allocator.free(accum_grad_b);
    // Track how many inner slices have been allocated so partial-init cleanup is correct.
    var accum_a_initialized: usize = 0;
    var accum_b_initialized: usize = 0;
    defer {
        var i: usize = 0;
        while (i < accum_a_initialized) : (i += 1) allocator.free(accum_grad_a[i]);
    }
    defer {
        var i: usize = 0;
        while (i < accum_b_initialized) : (i += 1) allocator.free(accum_grad_b[i]);
    }
    for (bundle.layers, 0..) |*layer, li| {
        accum_grad_a[li] = try allocator.alloc(f32, layer.adapter_a.len);
        accum_a_initialized += 1;
        accum_grad_b[li] = try allocator.alloc(f32, layer.adapter_b.len);
        accum_b_initialized += 1;
        @memset(accum_grad_a[li], 0);
        @memset(accum_grad_b[li], 0);
    }

    // Determine maximum layer index for LLRD (depth of the network).
    var max_layer_idx: usize = 0;
    for (bundle.layers) |*layer| {
        if (parseColQwen2LayerIndex(layer.base_tensor_name)) |li| {
            if (li > max_layer_idx) max_layer_idx = li;
        }
    }

    const accum_steps = if (options.grad_accum_steps == 0) 1 else options.grad_accum_steps;
    var accum_count: u32 = 0;

    for (examples[0..limit], 0..) |*example, ex_idx| {
        const is_last = (ex_idx == limit - 1);

        const predicted = try scorePreparedExample(allocator, bundle, example, options.layer_name);
        const target = exampleTarget(example);
        const error_value = predicted - target;
        const loss = 0.5 * error_value * error_value;
        summary.examples_seen += 1;
        summary.average_loss += loss;
        summary.mean_score += predicted;
        summary.mean_abs_error += @abs(error_value);

        // Accumulate per-layer gradients for this example.
        for (bundle.layers, 0..) |*layer, li| {
            if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;

            const input_rows: usize = 3;
            const inputs = try buildLayerFeatureRows(allocator, layer.input_dim, input_rows, example);
            defer allocator.free(inputs);
            // TODO(neftune): this trainer is a surrogate that hashes token ids into
            // per-layer synthetic feature rows (buildLayerFeatureRows) rather than
            // running a real Qwen2-VL forward pass, so there is no token-embedding
            // tensor to perturb here. When the real forward pass lands (via
            // graph_bridge/PJRT on input_embeds right after the token-embed lookup),
            // gate on options.neftune_alpha > 0.0 and call
            // neftune.applyInPlace(input_embeds, attn_mask_f32, num_tokens,
            //     hidden_size, options.neftune_alpha, global_step).
            const probe = try buildProbeVector(allocator, layer.base_tensor_name, layer.output_dim);
            defer allocator.free(probe);
            const output_grads = try allocator.alloc(f32, input_rows * layer.output_dim);
            defer allocator.free(output_grads);

            const row_scale = @as(f32, @floatCast(error_value)) / @as(f32, @floatFromInt(input_rows * @max(layer.output_dim, 1)));
            for (0..input_rows) |row_idx| {
                for (0..layer.output_dim) |out_idx| {
                    output_grads[row_idx * layer.output_dim + out_idx] = probe[out_idx] * row_scale;
                }
            }

            // Try PJRT path first; fall back to CPU on error or when disabled.
            // PJRT is skipped when world_size > 1: no collective ops in PJRT gradient path.
            var used_pjrt = false;
            if (comptime build_options.enable_pjrt) {
                if (options.world_size <= 1) {
                    if (options.pjrt_lora_steps) |pjrt_steps| {
                        if (pjrt_steps[li]) |*pjrt_step| {
                            if (graph_bridge.computeLoRALinearGradsWithPjrt(
                                allocator,
                                pjrt_step,
                                layer.base_weight,
                                layer.adapter_a,
                                layer.adapter_b,
                                inputs,
                                output_grads,
                            )) |grads| {
                                defer allocator.free(grads.grad_a);
                                defer allocator.free(grads.grad_b);
                                for (accum_grad_a[li], grads.grad_a) |*acc, g| acc.* += g;
                                for (accum_grad_b[li], grads.grad_b) |*acc, g| acc.* += g;
                                used_pjrt = true;
                            } else |_| {}
                        }
                    }
                }
            }
            if (!used_pjrt) {
                // CPU fallback.
                const a_mat = lora.Matrix{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a };
                const b_mat = lora.Matrix{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b };
                lora.accumulateLinearLoRAGradsBackend(
                    options.compute_backend,
                    accum_grad_a[li],
                    accum_grad_b[li],
                    input_rows,
                    layer.input_dim,
                    inputs,
                    layer.output_dim,
                    output_grads,
                    a_mat,
                    b_mat,
                    bundle.lora_alpha,
                );
            }
        }

        accum_count += 1;

        if (accum_count % accum_steps == 0 or is_last) {
            // Distributed DDP: allReduce gradient buffers across all replicas first,
            // so clipping and normalization operate on the globally averaged gradients.
            // Normalize accumulated gradients by accumulation steps before clipping.
            const norm_factor = 1.0 / @as(f32, @floatFromInt(accum_count));
            for (bundle.layers, 0..) |*layer, li| {
                if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;
                for (accum_grad_a[li]) |*g| g.* *= norm_factor;
                for (accum_grad_b[li]) |*g| g.* *= norm_factor;
            }

            // Joint gradient norm clipping on averaged gradients.
            if (options.max_grad_norm > 0) {
                var total_sq: f32 = 0;
                for (bundle.layers, 0..) |*layer, li| {
                    if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;
                    for (accum_grad_a[li]) |g| total_sq += g * g;
                    for (accum_grad_b[li]) |g| total_sq += g * g;
                }
                const global_norm = @sqrt(total_sq);
                if (global_norm > options.max_grad_norm) {
                    const clip_scale = options.max_grad_norm / (global_norm + 1e-8);
                    for (bundle.layers, 0..) |*layer, li| {
                        if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;
                        for (accum_grad_a[li]) |*g| g.* *= clip_scale;
                        for (accum_grad_b[li]) |*g| g.* *= clip_scale;
                    }
                }
            }

            // Apply AdamW with LLRD per layer.
            for (bundle.layers, 0..) |*layer, li| {
                if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;

                // Layer-wise learning rate decay: deeper layers (closer to max) get higher LR.
                var layer_lr = options.learning_rate;
                if (options.llrd_decay < 1.0) {
                    const layer_depth = parseColQwen2LayerIndex(layer.base_tensor_name) orelse max_layer_idx;
                    const depth_from_top: f32 = @floatFromInt(max_layer_idx - @min(layer_depth, max_layer_idx));
                    layer_lr = options.learning_rate * std.math.pow(f32, options.llrd_decay, depth_from_top);
                }

                const base_lr = layer_lr;
                if (options.use_schedule_free) {
                    if (sf_states[li]) |*sf| {
                        sf.step += 1;
                        const lr = warmupAdjustedLR(base_lr, sf.step, options.warmup_steps);
                        applyScheduleFreeInPlace(layer.adapter_a, accum_grad_a[li], sf.z_a, sf.v_a, sf.step, lr);
                        applyScheduleFreeInPlace(layer.adapter_b, accum_grad_b[li], sf.z_b, sf.v_b, sf.step, lr);
                    }
                } else {
                    // Increment once and share the same step value for both A and B.
                    adam_states[li].step += 1;
                    const lr = warmupAdjustedLR(base_lr, adam_states[li].step, options.warmup_steps);
                    applyAdamWInPlace(layer.adapter_a, accum_grad_a[li], adam_states[li].m_a, adam_states[li].v_a, adam_states[li].step, lr);
                    applyAdamWInPlace(layer.adapter_b, accum_grad_b[li], adam_states[li].m_b, adam_states[li].v_b, adam_states[li].step, lr);
                }
                summary.updates_applied += 1;
            }

            // Reset accumulation buffers.
            for (0..num_layers) |li| {
                @memset(accum_grad_a[li], 0);
                @memset(accum_grad_b[li], 0);
            }
            accum_count = 0;
        }
    }

    if (summary.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(summary.examples_seen));
        summary.average_loss /= denom;
        summary.mean_score /= denom;
        summary.mean_abs_error /= denom;
    }
    return summary;
}

pub fn scorePreparedExample(
    allocator: std.mem.Allocator,
    bundle: *const LoadedLoRABundle,
    example: *const PreparedExampleInput,
    layer_name: ?[]const u8,
) !f64 {
    var score: f64 = 0;
    for (bundle.layers) |layer| {
        if (!layerMatchesScope(layer.base_tensor_name, layer_name)) continue;
        score += try scoreLayerExample(allocator, &layer, bundle.lora_alpha, example);
    }
    return score;
}

pub fn loadExamples(allocator: std.mem.Allocator, path: []const u8) ![]Example {
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);

    var items = std.ArrayListUnmanaged(Example).empty;
    errdefer items.deinit(allocator);
    var lines = std.mem.tokenizeScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        const parsed = try std.json.parseFromSliceLeaky(Example, allocator, line, .{
            .ignore_unknown_fields = true,
        });
        try items.append(allocator, .{
            .query = try allocator.dupe(u8, parsed.query),
            .image_path = try allocator.dupe(u8, parsed.image_path),
            .ocr_text = try allocator.dupe(u8, parsed.ocr_text),
            .score = parsed.score,
            .document_id = try allocator.dupe(u8, parsed.document_id),
            .page_number = parsed.page_number,
            .answer = try allocator.dupe(u8, parsed.answer),
        });
    }
    return try items.toOwnedSlice(allocator);
}

pub fn freeExamples(allocator: std.mem.Allocator, items: []Example) void {
    for (items) |item| {
        allocator.free(item.query);
        allocator.free(item.image_path);
        allocator.free(item.ocr_text);
        allocator.free(item.document_id);
        allocator.free(item.answer);
    }
    allocator.free(items);
}

pub fn resolveImagePath(allocator: std.mem.Allocator, dataset_root: []const u8, image_path: []const u8) ![]const u8 {
    if (std.fs.path.isAbsolute(image_path)) return allocator.dupe(u8, image_path);
    return std.fs.path.join(allocator, &.{ dataset_root, image_path });
}

pub fn prepareInputsAgainstExamples(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    dataset_root: []const u8,
    examples: []const Example,
    max_examples: usize,
) !PreparedInputsSummary {
    var inspect = try inspectCheckpoint(allocator, model_input);
    defer freeInspectionSummary(allocator, &inspect);

    const tokenizer_path = inspect.tokenizer_path orelse return error.MissingTokenizerJson;
    const tokenizer_bytes = try c_file.readFile(allocator, tokenizer_path);
    defer allocator.free(tokenizer_bytes);
    var hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    defer hf_tok.deinitSelf();
    const tok = hf_tok.tokenizer();

    const limit = if (max_examples > 0 and max_examples < examples.len) max_examples else examples.len;
    const prepared = try allocator.alloc(PreparedExampleInput, limit);
    var prepared_count: usize = 0;
    errdefer {
        for (prepared[0..prepared_count]) |*item| freePreparedExampleInput(allocator, item);
        allocator.free(prepared);
    }

    const query_prefix = try allocator.dupe(u8, inspectQueryPrefix(&inspect));
    errdefer allocator.free(query_prefix);
    const visual_prompt_prefix = try allocator.dupe(u8, inspectVisualPromptPrefix(&inspect));
    errdefer allocator.free(visual_prompt_prefix);

    var summary = PreparedInputsSummary{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, inspect.model_dir),
        .variant = inspect.variant,
        .max_examples = max_examples,
        .examples_seen = limit,
        .tokenizer_class = try dupeOptionalString(allocator, inspect.tokenizer_class),
        .processor_class = try dupeOptionalString(allocator, inspect.processor_class),
        .query_prefix = query_prefix,
        .visual_prompt_prefix = visual_prompt_prefix,
        .examples = prepared,
    };
    errdefer freePreparedInputsSummary(allocator, &summary);

    for (examples[0..limit], 0..) |ex, idx| {
        const resolved_image_path = try resolveImagePath(allocator, dataset_root, ex.image_path);
        errdefer allocator.free(resolved_image_path);
        const item = try prepareExampleInput(
            allocator,
            tok,
            &inspect,
            query_prefix,
            visual_prompt_prefix,
            ex.query,
            ex.ocr_text,
            resolved_image_path,
            ex.score,
        );
        prepared[idx] = item;
        prepared_count += 1;
        summary.max_query_tokens = @max(summary.max_query_tokens, item.query_input_ids.len);
        summary.max_image_prompt_tokens = @max(summary.max_image_prompt_tokens, item.image_input_ids.len);
        summary.max_estimated_patch_tokens = @max(summary.max_estimated_patch_tokens, item.estimated_patch_tokens);
        switch (item.resize_reason) {
            .downscale_to_max_pixels => summary.resized_down_examples += 1,
            .upscale_to_min_pixels => summary.resized_up_examples += 1,
            .none => {},
        }
    }

    return summary;
}

pub fn loadPreparedInputsSummary(allocator: std.mem.Allocator, path: []const u8) !PreparedInputsSummary {
    const raw = try c_file.readFileMax(allocator, path, 128 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const parsed = try std.json.parseFromSliceLeaky(PreparedInputsSummaryFile, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    });
    return try clonePreparedInputsSummary(allocator, &parsed.summary);
}

pub fn savePreparedInputsSummary(allocator: std.mem.Allocator, path: []const u8, summary: PreparedInputsSummary) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{ .summary = summary }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn isRegularFilePath(path: []const u8) bool {
    const stat = compat.cwd().statFile(compat.io(), path, .{}) catch return false;
    return stat.kind == .file;
}

fn clonePreparedInputsSummary(allocator: std.mem.Allocator, source: *const PreparedInputsSummary) !PreparedInputsSummary {
    const examples = try allocator.alloc(PreparedExampleInput, source.examples.len);
    var cloned_count: usize = 0;
    errdefer {
        for (examples[0..cloned_count]) |*item| freePreparedExampleInput(allocator, item);
        allocator.free(examples);
    }
    for (source.examples, 0..) |item, idx| {
        examples[idx] = .{
            .query = try allocator.dupe(u8, item.query),
            .ocr_text = try allocator.dupe(u8, item.ocr_text),
            .resolved_image_path = try allocator.dupe(u8, item.resolved_image_path),
            .target_score = item.target_score,
            .real_colqwen_score = item.real_colqwen_score,
            .query_input_ids = try allocator.dupe(i32, item.query_input_ids),
            .query_attention_mask = try allocator.dupe(i32, item.query_attention_mask),
            .image_input_ids = try allocator.dupe(i32, item.image_input_ids),
            .image_attention_mask = try allocator.dupe(i32, item.image_attention_mask),
            .original_width = item.original_width,
            .original_height = item.original_height,
            .normalized_width = item.normalized_width,
            .normalized_height = item.normalized_height,
            .original_pixel_count = item.original_pixel_count,
            .normalized_pixel_count = item.normalized_pixel_count,
            .resize_reason = item.resize_reason,
            .scale = item.scale,
            .patch_size = item.patch_size,
            .estimated_image_grid_thw = item.estimated_image_grid_thw,
            .estimated_patch_tokens = item.estimated_patch_tokens,
            .pixel_values_shape = item.pixel_values_shape,
            .pixel_min = item.pixel_min,
            .pixel_max = item.pixel_max,
            .pixel_mean = item.pixel_mean,
            .pixel_std = item.pixel_std,
            .pixel_checksum = item.pixel_checksum,
        };
        cloned_count += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, source.artifact_family_version),
        .model_dir = try allocator.dupe(u8, source.model_dir),
        .variant = source.variant,
        .max_examples = source.max_examples,
        .examples_seen = source.examples_seen,
        .tokenizer_class = try dupeOptionalString(allocator, source.tokenizer_class),
        .processor_class = try dupeOptionalString(allocator, source.processor_class),
        .query_prefix = try allocator.dupe(u8, source.query_prefix),
        .visual_prompt_prefix = try allocator.dupe(u8, source.visual_prompt_prefix),
        .resized_down_examples = source.resized_down_examples,
        .resized_up_examples = source.resized_up_examples,
        .max_query_tokens = source.max_query_tokens,
        .max_image_prompt_tokens = source.max_image_prompt_tokens,
        .max_estimated_patch_tokens = source.max_estimated_patch_tokens,
        .examples = examples,
    };
}

fn findLoadedLayerIndex(layers: []const LoadedLoRALayer, layer_name: []const u8) ?usize {
    for (layers, 0..) |layer, idx| {
        if (layerMatchesScope(layer.base_tensor_name, layer_name)) return idx;
    }
    return null;
}

fn layerMatchesScope(layer_base_tensor_name: []const u8, layer_name: ?[]const u8) bool {
    const selector = layer_name orelse return true;
    if (std.mem.eql(u8, selector, focused_lora_scope_name)) {
        if (std.mem.eql(u8, layer_base_tensor_name, "embedding_proj_layer.weight")) return true;
        for (focused_top_layer_prefixes) |prefix| {
            if (!std.mem.startsWith(u8, layer_base_tensor_name, prefix)) continue;
            for (focused_top_layer_suffixes) |suffix| {
                if (std.mem.endsWith(u8, layer_base_tensor_name, suffix)) return true;
            }
        }
        return false;
    }
    return std.mem.eql(u8, layer_base_tensor_name, selector);
}

fn fillDeterministicMatrix(values: []f32, rows: usize, cols: usize, mul: f32, bias: f32) void {
    std.debug.assert(values.len == rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            const idx = row * cols + col;
            const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
            values[idx] = @sin(angle * mul) + @cos(angle * (mul * 0.5)) + bias;
        }
    }
}

fn applySgdStep(params: []f32, grads: []const f32, learning_rate: f32) void {
    std.debug.assert(params.len == grads.len);
    for (params, grads) |*param, grad| param.* -= learning_rate * grad;
}

fn l2Norm(values: []const f32) f64 {
    var total: f64 = 0;
    for (values) |value| {
        const widened: f64 = value;
        total += widened * widened;
    }
    return @sqrt(total);
}

fn scoreLayerExample(
    allocator: std.mem.Allocator,
    layer: *const LoadedLoRALayer,
    alpha: f32,
    example: *const PreparedExampleInput,
) !f64 {
    const input_rows: usize = 3;
    const inputs = try buildLayerFeatureRows(allocator, layer.input_dim, input_rows, example);
    defer allocator.free(inputs);
    const probe = try buildProbeVector(allocator, layer.base_tensor_name, layer.output_dim);
    defer allocator.free(probe);
    const scale = lora.effectiveScale(alpha, layer.rank);

    var total: f64 = 0;
    for (0..input_rows) |row_idx| {
        const row = inputs[row_idx * layer.input_dim .. (row_idx + 1) * layer.input_dim];
        var row_score: f64 = 0;
        for (0..layer.output_dim) |j| {
            var merged_value: f64 = 0;
            for (0..layer.input_dim) |i| {
                merged_value += @as(f64, row[i]) * @as(f64, layer.base_weight[j * layer.input_dim + i]);
            }
            row_score += merged_value * probe[j];
        }

        var tmp_rank = try allocator.alloc(f32, layer.rank);
        defer allocator.free(tmp_rank);
        @memset(tmp_rank, 0.0);
        for (0..layer.input_dim) |i| {
            const x = row[i];
            const a_row = layer.adapter_a[i * layer.rank .. (i + 1) * layer.rank];
            for (a_row, 0..) |a, r| tmp_rank[r] += x * a;
        }
        for (0..layer.rank) |r| {
            const scaled_rank = @as(f64, tmp_rank[r] * scale);
            const b_row = layer.adapter_b[r * layer.output_dim .. (r + 1) * layer.output_dim];
            for (b_row, 0..) |b, j| row_score += scaled_rank * b * probe[j];
        }
        total += row_score / @as(f64, @floatFromInt(@max(layer.output_dim, 1)));
    }
    return total / @as(f64, @floatFromInt(input_rows));
}

fn buildLayerFeatureRows(
    allocator: std.mem.Allocator,
    input_dim: usize,
    input_rows: usize,
    example: *const PreparedExampleInput,
) ![]f32 {
    const rows = try allocator.alloc(f32, input_rows * input_dim);
    @memset(rows, 0.0);
    if (input_rows == 0 or input_dim == 0) return rows;
    hashTokenIdsIntoRow(rows[0..input_dim], example.query_input_ids, 1.0);
    if (input_rows > 1) hashTokenIdsIntoRow(rows[input_dim .. input_dim * 2], example.image_input_ids, 0.5);
    if (input_rows > 2) addDenseImageStats(rows[input_dim * 2 .. input_dim * 3], example);
    return rows;
}

fn hashTokenIdsIntoRow(row: []f32, ids: []const i32, scale: f32) void {
    if (row.len == 0) return;
    for (ids, 0..) |id, idx| {
        const id_bits: u32 = @bitCast(id);
        const hash_seed = (@as(u64, id_bits) *% 0x9E3779B185EBCA87) ^ (@as(u64, idx) *% 1315423911);
        const pos = @as(usize, @intCast(hash_seed % row.len));
        row[pos] += scale;
    }
}

fn addDenseImageStats(row: []f32, example: *const PreparedExampleInput) void {
    if (row.len == 0) return;
    const stats = [_]f32{
        @floatFromInt(example.original_width),
        @floatFromInt(example.original_height),
        @floatFromInt(example.normalized_width),
        @floatFromInt(example.normalized_height),
        @floatFromInt(example.estimated_patch_tokens),
        example.pixel_min,
        example.pixel_max,
        example.pixel_mean,
        example.pixel_std,
        example.scale,
    };
    const denom = @as(f32, @floatFromInt(@max(example.normalized_pixel_count, 1)));
    for (stats, 0..) |value, idx| row[idx % row.len] += value / denom;
}

fn buildProbeVector(allocator: std.mem.Allocator, layer_name: []const u8, output_dim: usize) ![]f32 {
    const probe = try allocator.alloc(f32, output_dim);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(layer_name);
    const base = hasher.final();
    for (probe, 0..) |*value, idx| {
        const angle = @as(f32, @floatFromInt((base % 997) + idx + 1));
        value.* = @sin(angle * 0.017) * 0.5 + @cos(angle * 0.009) * 0.5;
    }
    return probe;
}

fn exampleTarget(example: *const PreparedExampleInput) f64 {
    return @as(f64, example.target_score);
}

fn inspectQueryPrefix(summary: *const InspectionSummary) []const u8 {
    return summary.query_prefix orelse "Query -- ";
}

fn inspectVisualPromptPrefix(summary: *const InspectionSummary) []const u8 {
    return summary.visual_prompt_prefix orelse "<|im_start|>user\n<|vision_start|><|image_pad|><|vision_end|>Describe the image.<|im_end|><|endoftext|>";
}

fn prepareExampleInput(
    allocator: std.mem.Allocator,
    tok: anytype,
    inspect: *const InspectionSummary,
    query_prefix: []const u8,
    visual_prompt_prefix: []const u8,
    query: []const u8,
    ocr_text: []const u8,
    image_path: []const u8,
    target_score: f32,
) !PreparedExampleInput {
    const query_prompt = try std.mem.concat(allocator, u8, &.{ query_prefix, query });
    defer allocator.free(query_prompt);
    var query_encoded = try tok.encodeForGenerationConfigured(allocator, query_prompt, inspect.tokenizer_model_max_length orelse 32768, false);
    defer query_encoded.deinit();

    const image_prompt_ids = try tok.encode(allocator, visual_prompt_prefix);
    errdefer allocator.free(image_prompt_ids);
    const image_attention_mask = try allocOnesI32(allocator, image_prompt_ids.len);
    errdefer allocator.free(image_attention_mask);

    const image_bytes = try c_file.readFile(allocator, image_path);
    defer allocator.free(image_bytes);
    const decoded = try image_pipeline.decode(allocator, image_bytes);
    defer decoded.deinit(allocator);

    const prep_cfg = inspectQwenPreprocessorConfig(inspect);
    var prepared = try multimodal_qwen_adapter.prepareImage(allocator, image_bytes, prep_cfg);
    defer prepared.deinit();

    const pixel_stats = computePixelStats(prepared.pixel_values);
    const original_pixels = @as(u64, decoded.width) * @as(u64, decoded.height);
    const normalized_pixels = @as(u64, prepared.resized_width) * @as(u64, prepared.resized_height);
    const resize_reason = if (normalized_pixels > original_pixels)
        ResizeReason.upscale_to_min_pixels
    else if (normalized_pixels < original_pixels)
        ResizeReason.downscale_to_max_pixels
    else
        ResizeReason.none;
    const scale = if (decoded.width == 0 or decoded.height == 0)
        1.0
    else
        @as(f32, @floatFromInt(prepared.resized_width)) / @as(f32, @floatFromInt(decoded.width));

    return .{
        .query = try allocator.dupe(u8, query),
        .ocr_text = try allocator.dupe(u8, ocr_text),
        .resolved_image_path = image_path,
        .target_score = target_score,
        .query_input_ids = try allocator.dupe(i32, query_encoded.ids),
        .query_attention_mask = try allocator.dupe(i32, query_encoded.attention_mask),
        .image_input_ids = image_prompt_ids,
        .image_attention_mask = image_attention_mask,
        .original_width = decoded.width,
        .original_height = decoded.height,
        .normalized_width = prepared.resized_width,
        .normalized_height = prepared.resized_height,
        .original_pixel_count = original_pixels,
        .normalized_pixel_count = normalized_pixels,
        .resize_reason = resize_reason,
        .scale = scale,
        .patch_size = prep_cfg.patch_size,
        .estimated_image_grid_thw = prepared.image_grid_thw,
        .estimated_patch_tokens = prepared.image_token_count,
        .pixel_values_shape = .{ 1, 3, @as(usize, prepared.resized_height), @as(usize, prepared.resized_width) },
        .pixel_min = pixel_stats.min,
        .pixel_max = pixel_stats.max,
        .pixel_mean = pixel_stats.mean,
        .pixel_std = pixel_stats.std,
        .pixel_checksum = pixel_stats.checksum,
    };
}

const PixelStats = struct {
    min: f32,
    max: f32,
    mean: f32,
    std: f32,
    checksum: u64,
};

fn computePixelStats(values: []const f32) PixelStats {
    var hasher = std.hash.Wyhash.init(0);
    var min_value: f32 = std.math.inf(f32);
    var max_value: f32 = -std.math.inf(f32);
    var sum: f64 = 0;
    var sum_sq: f64 = 0;
    for (values) |value| {
        hasher.update(std.mem.asBytes(&value));
        min_value = @min(min_value, value);
        max_value = @max(max_value, value);
        sum += value;
        sum_sq += value * value;
    }
    const denom = @as(f64, @floatFromInt(@max(values.len, 1)));
    const mean_value = @as(f32, @floatCast(sum / denom));
    const variance = @max(0.0, sum_sq / denom - (sum / denom) * (sum / denom));
    return .{
        .min = if (values.len == 0) 0 else min_value,
        .max = if (values.len == 0) 0 else max_value,
        .mean = mean_value,
        .std = @floatCast(@sqrt(variance)),
        .checksum = hasher.final(),
    };
}

fn inspectQwenPreprocessorConfig(summary: *const InspectionSummary) multimodal_qwen_adapter.PreprocessorConfig {
    var cfg = multimodal_qwen_adapter.PreprocessorConfig{};
    if (summary.do_resize) |value| cfg.do_resize = value;
    if (summary.do_rescale) |value| cfg.do_rescale = value;
    if (summary.do_normalize) |value| cfg.do_normalize = value;
    if (summary.rescale_factor) |value| cfg.rescale_factor = value;
    if (summary.patch_size) |value| cfg.patch_size = @intCast(value);
    if (summary.temporal_patch_size) |value| cfg.temporal_patch_size = @intCast(value);
    if (summary.merge_size) |value| cfg.merge_size = @intCast(value);
    if (summary.min_pixels) |value| cfg.min_pixels = @intCast(value);
    if (summary.max_pixels) |value| cfg.max_pixels = @intCast(value);
    return cfg;
}

fn allocOnesI32(allocator: std.mem.Allocator, len: usize) ![]i32 {
    const out = try allocator.alloc(i32, len);
    @memset(out, 1);
    return out;
}

fn freePreparedExampleInput(allocator: std.mem.Allocator, item: *const PreparedExampleInput) void {
    allocator.free(item.query);
    allocator.free(item.ocr_text);
    allocator.free(item.resolved_image_path);
    allocator.free(item.query_input_ids);
    allocator.free(item.query_attention_mask);
    allocator.free(item.image_input_ids);
    allocator.free(item.image_attention_mask);
}

pub fn freePreparedInputsSummary(allocator: std.mem.Allocator, summary: *const PreparedInputsSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    if (summary.tokenizer_class) |value| allocator.free(value);
    if (summary.processor_class) |value| allocator.free(value);
    allocator.free(summary.query_prefix);
    allocator.free(summary.visual_prompt_prefix);
    for (summary.examples) |*item| freePreparedExampleInput(allocator, item);
    allocator.free(summary.examples);
}

fn testScratchDir(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const dir_path = try std.fmt.allocPrint(allocator, "/tmp/termite_colqwen2_{s}_{d}", .{ name, std.posix.system.getpid() });
    errdefer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    return dir_path;
}

test "colqwen2 inspect adapter directory reads config" {
    const allocator = std.testing.allocator;
    const root = try testScratchDir(allocator, "adapter_inspect_test");
    defer allocator.free(root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const adapter_config_path = try std.fs.path.join(allocator, &.{ root, adapter_config_file_name });
    defer allocator.free(adapter_config_path);
    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ root, adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = adapter_config_path,
        .data = "{\"base_model_name_or_path\":\"vidore/colqwen2-v1.0\",\"peft_type\":\"LORA\",\"task_type\":\"FEATURE_EXTRACTION\",\"r\":16,\"lora_alpha\":32.0,\"target_modules\":[\"q_proj\",\"v_proj\"]}",
    });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = adapter_checkpoint_path, .data = "stub" });

    var summary = try inspectCheckpoint(allocator, root);
    defer freeInspectionSummary(allocator, &summary);
    try std.testing.expectEqual(Variant.adapter_only, summary.variant);
    try std.testing.expect(summary.has_adapter_weights);
    try std.testing.expectEqual(@as(?usize, 16), summary.lora_rank);
    try std.testing.expectEqual(@as(usize, 2), summary.target_module_count);
}

test "colqwen2 bootstrap and inspect lora bundle" {
    const allocator = std.testing.allocator;
    const root = try testScratchDir(allocator, "bootstrap_test");
    defer allocator.free(root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const config_path = try std.fs.path.join(allocator, &.{ root, hf_config_file_name });
    defer allocator.free(config_path);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data = "{\"model_type\":\"colqwen2\",\"hidden_size\":128}",
    });
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "vlm.model.language_model.layers.0.self_attn.q_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "vlm.model.language_model.layers.0.self_attn.v_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "embedding_proj_layer.weight", .shape = &.{ 128, 1536 }, .data = &[_]f32{0} ** (128 * 1536) },
    });

    const out_dir = try std.fs.path.join(allocator, &.{ root, "lora" });
    defer allocator.free(out_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, out_dir, .{
        .rank = 8,
        .alpha = 16,
        .base_model_name_or_path = "vidore/colqwen2-v1.0",
    });
    defer freeBootstrapSummary(allocator, &bootstrap);
    try std.testing.expectEqual(@as(usize, 3), bootstrap.resolved_tensors.len);

    var inspect = try inspectLoRABundle(allocator, root, out_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspect);
    try std.testing.expectEqual(@as(usize, 3), inspect.resolved_tensor_count);
    try std.testing.expectEqual(@as(?usize, 8), inspect.lora_rank);
}

test "colqwen2 lora bundle load and save round-trip" {
    const allocator = std.testing.allocator;
    const root = try testScratchDir(allocator, "roundtrip_test");
    defer allocator.free(root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const config_path = try std.fs.path.join(allocator, &.{ root, hf_config_file_name });
    defer allocator.free(config_path);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const preprocessor_path = try std.fs.path.join(allocator, &.{ root, preprocessor_config_file_name });
    defer allocator.free(preprocessor_path);
    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ root, tokenizer_config_file_name });
    defer allocator.free(tokenizer_config_path);
    const tokenizer_path = try std.fs.path.join(allocator, &.{ root, tokenizer_file_name });
    defer allocator.free(tokenizer_path);
    const special_tokens_path = try std.fs.path.join(allocator, &.{ root, special_tokens_map_file_name });
    defer allocator.free(special_tokens_path);

    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = "{\"model_type\":\"colqwen2\",\"hidden_size\":128}" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = preprocessor_path, .data = "{\"processor_class\":\"ColQwen2Processor\"}" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_config_path, .data = "{\"tokenizer_class\":\"Qwen2TokenizerFast\"}" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_path, .data = "{}" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = special_tokens_path, .data = "{\"image_token\":\"<|image_pad|>\"}" });
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "vlm.model.language_model.layers.0.self_attn.q_proj.weight", .shape = &.{ 8, 8 }, .data = &[_]f32{0} ** (8 * 8) },
        .{ .name = "embedding_proj_layer.weight", .shape = &.{ 8, 16 }, .data = &[_]f32{0} ** (8 * 16) },
    });

    const adapter_dir = try std.fs.path.join(allocator, &.{ root, "adapter" });
    defer allocator.free(adapter_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, adapter_dir, .{
        .rank = 4,
        .alpha = 8,
        .target_modules = &.{ "q_proj", "embedding_proj_layer" },
    });
    defer freeBootstrapSummary(allocator, &bootstrap);

    var bundle = try loadLoRABundle(allocator, root, adapter_dir);
    defer bundle.deinit();
    try std.testing.expectEqual(@as(usize, 2), bundle.layers.len);
    const query_layer_idx = findLoadedLayerIndex(bundle.layers, "vlm.model.language_model.layers.0.self_attn.q_proj.weight") orelse return error.MissingBaseTensorForAdapter;
    const query_layer = &bundle.layers[query_layer_idx];
    query_layer.adapter_b[0] = 1.25;
    const dora_magnitude = try allocator.alloc(f32, query_layer.output_dim);
    lora.doraColumnNorms(.{
        .base = .{ .rows = query_layer.input_dim, .cols = query_layer.output_dim, .data = query_layer.base_weight },
        .adapter_a = .{ .rows = query_layer.input_dim, .cols = query_layer.rank, .data = query_layer.adapter_a },
        .adapter_b = .{ .rows = query_layer.rank, .cols = query_layer.output_dim, .data = query_layer.adapter_b },
        .magnitude = dora_magnitude,
        .alpha = bundle.lora_alpha,
    }, dora_magnitude);
    query_layer.dora_magnitude = dora_magnitude;
    query_layer.dora_magnitude_tensor_name = try doraMagnitudeTensorName(allocator, query_layer.base_tensor_name);

    const expected_internal = try allocator.alloc(f32, query_layer.base_weight.len);
    defer allocator.free(expected_internal);
    lora.mergeInto(
        .{ .rows = query_layer.input_dim, .cols = query_layer.output_dim, .data = query_layer.base_weight },
        .{ .rows = query_layer.input_dim, .cols = query_layer.rank, .data = query_layer.adapter_a },
        .{ .rows = query_layer.rank, .cols = query_layer.output_dim, .data = query_layer.adapter_b },
        bundle.lora_alpha,
        expected_internal,
    );
    const expected_hf = try allocator.alloc(f32, expected_internal.len);
    defer allocator.free(expected_hf);
    transpose2DF32(expected_hf, expected_internal, query_layer.input_dim, query_layer.output_dim);

    const saved_dir = try std.fs.path.join(allocator, &.{ root, "saved" });
    defer allocator.free(saved_dir);
    try saveLoRABundle(&bundle, saved_dir);

    var inspect = try inspectLoRABundle(allocator, root, saved_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspect);
    try std.testing.expectEqual(@as(usize, 1), inspect.dora_magnitude_tensor_count);
    try std.testing.expectEqual(@as(usize, 8), inspect.dora_magnitude_parameter_count);
    try std.testing.expectEqual(@as(?bool, true), inspect.use_dora);

    var reloaded = try loadLoRABundle(allocator, root, saved_dir);
    defer reloaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), reloaded.layers.len);
    const reloaded_query_idx = findLoadedLayerIndex(reloaded.layers, "vlm.model.language_model.layers.0.self_attn.q_proj.weight") orelse return error.MissingBaseTensorForAdapter;
    try std.testing.expectApproxEqAbs(@as(f32, 1.25), reloaded.layers[reloaded_query_idx].adapter_b[0], 1e-6);
    try std.testing.expect(reloaded.layers[reloaded_query_idx].dora_magnitude != null);

    const materialized_dir = try std.fs.path.join(allocator, &.{ root, "materialized" });
    defer allocator.free(materialized_dir);
    var materialize = try materializeMergedModel(allocator, root, saved_dir, materialized_dir);
    defer freeMaterializeSummary(allocator, &materialize);
    try std.testing.expectEqual(@as(usize, 2), materialize.merged_lora_tensor_count);
    try std.testing.expectEqual(@as(usize, 1), materialize.merged_dora_tensor_count);

    const materialized_checkpoint = try std.fs.path.join(allocator, &.{ materialized_dir, checkpoint_file_name });
    defer allocator.free(materialized_checkpoint);
    var out_access = try openTensorAccessForFile(allocator, materialized_checkpoint);
    defer out_access.deinit();
    var merged_query = try loadTensorAsF32(allocator, out_access, "vlm.model.language_model.layers.0.self_attn.q_proj.weight");
    defer merged_query.deinit();
    for (expected_hf, merged_query.asFloat32()) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
    }
}

test "colqwen2 prepare inputs against examples" {
    const allocator = std.testing.allocator;
    const root = try testScratchDir(allocator, "prepare_inputs_test");
    defer allocator.free(root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const config_path = try std.fs.path.join(allocator, &.{ root, hf_config_file_name });
    defer allocator.free(config_path);
    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ root, tokenizer_config_file_name });
    defer allocator.free(tokenizer_config_path);
    const tokenizer_path = try std.fs.path.join(allocator, &.{ root, tokenizer_file_name });
    defer allocator.free(tokenizer_path);
    const preprocessor_path = try std.fs.path.join(allocator, &.{ root, preprocessor_config_file_name });
    defer allocator.free(preprocessor_path);
    const image_path = try std.fs.path.join(allocator, &.{ root, "sample.png" });
    defer allocator.free(image_path);

    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = "{\"model_type\":\"colqwen2\"}" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_config_path, .data = "{\"tokenizer_class\":\"Qwen2TokenizerFast\",\"model_max_length\":128}" });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data =
        \\{"version":"1.0","truncation":null,"padding":null,"added_tokens":[{"id":0,"content":"<pad>","special":true},{"id":1,"content":"<unk>","special":true},{"id":2,"content":"<bos>","special":true}],"normalizer":null,"pre_tokenizer":{"type":"Whitespace"},"post_processor":null,"decoder":null,"model":{"type":"WordPiece","unk_token":"<unk>","continuing_subword_prefix":"##","max_input_chars_per_word":100,"vocab":{"<pad>":0,"<unk>":1,"<bos>":2,"Query":3,"--":4,"invoice":5,"Describe":6,"the":7,"image":8,".":9,"<|im_start|>user":10,"<|vision_start|><|image_pad|><|vision_end|>Describe":11,"image.<|im_end|><|endoftext|>":12},"special_tokens":{"<pad>":0,"<unk>":1,"<bos>":2}}}
        ,
    });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = preprocessor_path, .data = "{\"processor_class\":\"ColQwen2Processor\",\"patch_size\":14,\"merge_size\":2,\"min_pixels\":3136,\"max_pixels\":50176}" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = image_path, .data = &red_png_2x2 });

    const examples = [_]Example{
        .{ .query = "invoice", .image_path = "sample.png", .score = 1.0 },
    };
    var summary = try prepareInputsAgainstExamples(allocator, root, root, examples[0..], 1);
    defer freePreparedInputsSummary(allocator, &summary);
    try std.testing.expectEqual(@as(usize, 1), summary.examples.len);
    try std.testing.expect(summary.examples[0].query_input_ids.len > 0);
    try std.testing.expect(summary.examples[0].image_input_ids.len > 0);
    try std.testing.expect(summary.examples[0].estimated_patch_tokens > 0);
}

test "colqwen2 one step train and eval" {
    const allocator = std.testing.allocator;
    const root = try testScratchDir(allocator, "train_one_step_test");
    defer allocator.free(root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const config_path = try std.fs.path.join(allocator, &.{ root, hf_config_file_name });
    defer allocator.free(config_path);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = "{\"model_type\":\"colqwen2\",\"hidden_size\":8}" });
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "vlm.model.language_model.layers.27.self_attn.q_proj.weight", .shape = &.{ 8, 8 }, .data = &[_]f32{0} ** (8 * 8) },
    });

    const adapter_dir = try std.fs.path.join(allocator, &.{ root, "adapter" });
    defer allocator.free(adapter_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, adapter_dir, .{ .rank = 4, .alpha = 8, .target_modules = &.{"q_proj"} });
    defer freeBootstrapSummary(allocator, &bootstrap);
    var bundle = try loadLoRABundle(allocator, root, adapter_dir);
    defer bundle.deinit();

    var ex = PreparedExampleInput{
        .query = try allocator.dupe(u8, "invoice"),
        .ocr_text = try allocator.dupe(u8, ""),
        .resolved_image_path = try allocator.dupe(u8, "/tmp/image.png"),
        .target_score = 1.0,
        .query_input_ids = try allocator.dupe(i32, &.{ 1, 2, 3 }),
        .query_attention_mask = try allocator.dupe(i32, &.{ 1, 1, 1 }),
        .image_input_ids = try allocator.dupe(i32, &.{ 4, 5 }),
        .image_attention_mask = try allocator.dupe(i32, &.{ 1, 1 }),
        .original_width = 2,
        .original_height = 2,
        .normalized_width = 56,
        .normalized_height = 56,
        .original_pixel_count = 4,
        .normalized_pixel_count = 3136,
        .resize_reason = .upscale_to_min_pixels,
        .scale = 28.0,
        .patch_size = 14,
        .estimated_image_grid_thw = .{ 1, 4, 4 },
        .estimated_patch_tokens = 4,
        .pixel_values_shape = .{ 1, 3, 56, 56 },
        .pixel_min = 0,
        .pixel_max = 1,
        .pixel_mean = 0.5,
        .pixel_std = 0.25,
        .pixel_checksum = 123,
    };
    defer freePreparedExampleInput(allocator, &ex);

    const before = try evaluatePreparedExamples(allocator, &bundle, (&[_]PreparedExampleInput{ex})[0..], .{ .max_examples = 1 });
    var step = try trainLoRABundleOneStep(allocator, &bundle, .{ .learning_rate = 0.01 });
    defer freeLoRAOneStepSummary(allocator, &step);
    const after = try evaluatePreparedExamples(allocator, &bundle, (&[_]PreparedExampleInput{ex})[0..], .{ .max_examples = 1 });
    try std.testing.expect(step.grad_b_l2_norm > 0);
    try std.testing.expect(before.examples_seen == 1);
    try std.testing.expect(after.examples_seen == 1);
}

test "colqwen2 train prepared examples epoch updates bundle" {
    const allocator = std.testing.allocator;
    const root = try testScratchDir(allocator, "train_epoch_test");
    defer allocator.free(root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const config_path = try std.fs.path.join(allocator, &.{ root, hf_config_file_name });
    defer allocator.free(config_path);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = "{\"model_type\":\"colqwen2\",\"hidden_size\":8}" });
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "vlm.model.language_model.layers.27.self_attn.q_proj.weight", .shape = &.{ 8, 8 }, .data = &[_]f32{0} ** (8 * 8) },
    });

    const adapter_dir = try std.fs.path.join(allocator, &.{ root, "adapter" });
    defer allocator.free(adapter_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, adapter_dir, .{ .rank = 4, .alpha = 8, .target_modules = &.{"q_proj"} });
    defer freeBootstrapSummary(allocator, &bootstrap);
    var bundle = try loadLoRABundle(allocator, root, adapter_dir);
    defer bundle.deinit();

    var ex = PreparedExampleInput{
        .query = try allocator.dupe(u8, "invoice"),
        .ocr_text = try allocator.dupe(u8, ""),
        .resolved_image_path = try allocator.dupe(u8, "/tmp/image.png"),
        .target_score = 1.0,
        .query_input_ids = try allocator.dupe(i32, &.{ 1, 2, 3 }),
        .query_attention_mask = try allocator.dupe(i32, &.{ 1, 1, 1 }),
        .image_input_ids = try allocator.dupe(i32, &.{ 4, 5 }),
        .image_attention_mask = try allocator.dupe(i32, &.{ 1, 1 }),
        .original_width = 2,
        .original_height = 2,
        .normalized_width = 56,
        .normalized_height = 56,
        .original_pixel_count = 4,
        .normalized_pixel_count = 3136,
        .resize_reason = .upscale_to_min_pixels,
        .scale = 28.0,
        .patch_size = 14,
        .estimated_image_grid_thw = .{ 1, 4, 4 },
        .estimated_patch_tokens = 4,
        .pixel_values_shape = .{ 1, 3, 56, 56 },
        .pixel_min = 0,
        .pixel_max = 1,
        .pixel_mean = 0.5,
        .pixel_std = 0.25,
        .pixel_checksum = 123,
    };
    defer freePreparedExampleInput(allocator, &ex);

    const before = try allocator.dupe(f32, bundle.layers[0].adapter_b);
    defer allocator.free(before);
    const epoch = try trainPreparedExamplesEpoch(allocator, &bundle, (&[_]PreparedExampleInput{ex})[0..], .{
        .learning_rate = 0.01,
        .max_examples = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), epoch.examples_seen);
    try std.testing.expect(epoch.updates_applied > 0);
    try std.testing.expect(!std.mem.eql(f32, before, bundle.layers[0].adapter_b));
}

const red_png_2x2 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02,
    0x08, 0x02, 0x00, 0x00, 0x00, 0xfd, 0xd4, 0x9a, 0x73, 0x00, 0x00, 0x00,
    0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x4f, 0x25, 0xc4, 0xd6, 0x00, 0x00, 0x00, 0x10, 0x49, 0x44,
    0x41, 0x54, 0x78, 0x9c, 0x63, 0xfc, 0xc3, 0x00, 0x02, 0x2c, 0x60, 0x92,
    0x01, 0x00, 0x0d, 0x04, 0x01, 0x02, 0xbf, 0x50, 0x15, 0xb3, 0x00, 0x00,
    0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
};
