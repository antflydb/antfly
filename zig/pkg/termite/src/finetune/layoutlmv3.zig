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
const tensor_mod = @import("../backends/tensor.zig");
const Tensor = tensor_mod.Tensor;
const DType = tensor_mod.DType;
const c_file = @import("../util/c_file.zig");
const manifest_mod = @import("../models/manifest.zig");
const safetensors = @import("../models/safetensors.zig");
const tensor_access = @import("../models/tensor_access.zig");
const weight_source = @import("../models/weight_source.zig");
const compat = @import("../io/compat.zig");
const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const document_data = @import("document_data.zig");
const lora = @import("lora.zig");
const neftune = @import("neftune.zig");
const graph_bridge = @import("graph_bridge.zig");
const blas_mod = @import("../ops/blas_compute.zig");
const ml = @import("ml");
const optimizers = ml.graph.optimizers;

pub const artifact_family_version = "layoutlmv3_document/v3alpha1";
pub const checkpoint_file_name = "model.safetensors";
pub const adapter_checkpoint_file_name = "adapter_model.safetensors";
pub const config_file_name = "config.json";
pub const preprocessor_config_file_name = "preprocessor_config.json";
pub const tokenizer_config_file_name = "tokenizer_config.json";
pub const tokenizer_file_name = "tokenizer.json";
pub const special_tokens_map_file_name = "special_tokens_map.json";
pub const adapter_config_file_name = "adapter_config.json";
pub const sequence_head_checkpoint_file_name = "layoutdoc_sequence_head.safetensors";
pub const token_head_checkpoint_file_name = "layoutdoc_token_head.safetensors";
pub const sequence_head_config_file_name = "sequence_head_config.json";
pub const token_head_config_file_name = "token_head_config.json";

pub const default_lora_target_modules = [_][]const u8{
    "query",
    "key",
    "value",
    "dense",
    "intermediate.dense",
    "output.dense",
    "classifier",
};

pub const AdapterConfig = struct {
    base_model_name_or_path: ?[]const u8 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    r: ?usize = null,
    lora_alpha: ?f64 = null,
    target_modules: ?[]const []const u8 = null,
    use_dora: ?bool = null,
};

pub const LoRATargetTensor = struct {
    tensor_name: []const u8,
    module_name: []const u8,
    input_dim: usize,
    output_dim: usize,
};

pub const BootstrapOptions = struct {
    rank: usize = 16,
    alpha: f32 = 32.0,
    base_model_name_or_path: ?[]const u8 = null,
    target_modules: ?[]const []const u8 = null,
};

pub const BootstrapSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    output_dir: []const u8,
    checkpoint_path: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: []const u8,
    base_model_name_or_path: []const u8,
    lora_rank: usize,
    lora_alpha: f32,
    target_modules: []const []const u8,
    resolved_tensors: []LoRATargetTensor,
};

pub const LoRATensorSummary = struct {
    base_tensor_name: []const u8,
    adapter_a_tensor_name: []const u8,
    adapter_b_tensor_name: []const u8,
    dora_magnitude_tensor_name: ?[]const u8 = null,
    module_name: []const u8,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    adapter_parameter_count: usize,
    dora_magnitude_parameter_count: usize = 0,
};

pub const LoRABundleInspectionSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    base_checkpoint_path: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    lora_rank: ?usize = null,
    lora_alpha: ?f64 = null,
    use_dora: ?bool = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    resolved_tensor_count: usize = 0,
    trainable_parameter_count: usize = 0,
    dora_magnitude_tensor_count: usize = 0,
    dora_magnitude_parameter_count: usize = 0,
    tensors: []LoRATensorSummary,
};

pub const LoadedLoRALayer = struct {
    base_tensor_name: []const u8,
    adapter_a_tensor_name: []const u8,
    adapter_b_tensor_name: []const u8,
    dora_magnitude_tensor_name: ?[]const u8 = null,
    module_name: []const u8,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    base_weight: []f32,
    adapter_a: []f32,
    adapter_b: []f32,
    dora_magnitude: ?[]f32 = null,
};

pub const LoadedLoRABundle = struct {
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    base_checkpoint_path: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    lora_rank: usize,
    lora_alpha: f32,
    target_modules: []const []const u8,
    layers: []LoadedLoRALayer,

    pub fn deinit(self: *LoadedLoRABundle) void {
        self.allocator.free(self.base_model_dir);
        self.allocator.free(self.adapter_model_dir);
        self.allocator.free(self.base_checkpoint_path);
        self.allocator.free(self.adapter_checkpoint_path);
        if (self.adapter_config_path) |value| self.allocator.free(value);
        if (self.base_model_name_or_path) |value| self.allocator.free(value);
        for (self.target_modules) |item| self.allocator.free(item);
        self.allocator.free(self.target_modules);
        for (self.layers) |layer| {
            self.allocator.free(layer.base_tensor_name);
            self.allocator.free(layer.adapter_a_tensor_name);
            self.allocator.free(layer.adapter_b_tensor_name);
            if (layer.dora_magnitude_tensor_name) |name| self.allocator.free(name);
            self.allocator.free(layer.module_name);
            self.allocator.free(layer.base_weight);
            self.allocator.free(layer.adapter_a);
            self.allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |magnitude| self.allocator.free(magnitude);
        }
        self.allocator.free(self.layers);
        self.* = undefined;
    }
};

pub const MaterializeSummary = struct {
    artifact_family_version: []const u8,
    task: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    output_checkpoint_path: []const u8,
    output_config_path: []const u8,
    labels: []const []const u8,
    merged_lora_tensor_count: usize,
    merged_dora_tensor_count: usize,
    copied_base_tensor_count: usize,
    attached_head_tensor_count: usize,
};

pub const LoRAOneStepOptions = struct {
    layer_name: ?[]const u8 = null,
    input_rows: usize = 4,
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

pub const SequenceMetrics = struct {
    examples_seen: usize = 0,
    average_loss: f64 = 0,
    accuracy: f64 = 0,
};

pub const SequenceEpochSummary = struct {
    epoch: usize,
    train_examples_seen: usize,
    train_average_loss: f64,
    train_accuracy: f64,
    eval_accuracy: f64,
    improved_best: bool,
};

pub const SequenceTrainEvalOptions = struct {
    max_train_examples: usize = 128,
    max_val_examples: usize = 64,
    epochs: usize = 4,
    learning_rate: f32 = 0.001,
    target_margin: f32 = 0.25,
    layer_name: ?[]const u8 = null,
    save_output_on_completion: bool = true,
    max_grad_norm: f32 = 1.0,
    llrd_decay: f32 = 1.0,
    grad_accum_steps: u32 = 1,
    use_schedule_free: bool = false,
    /// DDP rank of this process. Rank 0 is responsible for checkpoint writes.
    ddp_rank: u32 = 0,
    /// Optional compute backend for gradient computation.
    /// If null, defaults to native CPU math.
    compute_backend: ?*const ComputeBackend = null,
    /// NEFTune embedding-noise scale (Jain et al., NeurIPS 2023).
    /// 0.0 disables. Typical values: 5.0 - 15.0. Applied only during training,
    /// on the text-token hidden vector before the LoRA layers run.
    neftune_alpha: f32 = 0.0,
};

pub const SequenceTrainEvalSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    learning_rate: f32,
    epochs: usize,
    max_train_examples: usize,
    max_val_examples: usize,
    target_margin: f32,
    max_grad_norm: f32,
    llrd_decay: f32,
    grad_accum_steps: u32,
    use_schedule_free: bool,
    layer_name: ?[]const u8 = null,
    before_train: SequenceMetrics,
    before_val: SequenceMetrics,
    after_train: SequenceMetrics,
    after_val: SequenceMetrics,
    best_epoch: usize = 0,
    epoch_history: []SequenceEpochSummary,
};

pub const TokenMetrics = struct {
    examples_seen: usize = 0,
    tokens_seen: usize = 0,
    average_loss: f64 = 0,
    accuracy: f64 = 0,
    exact_match_accuracy: f64 = 0,
};

pub const TokenEpochSummary = struct {
    epoch: usize,
    train_examples_seen: usize,
    train_tokens_seen: usize,
    train_average_loss: f64,
    train_accuracy: f64,
    train_exact_match_accuracy: f64,
    eval_accuracy: f64,
    eval_exact_match_accuracy: f64,
    improved_best: bool,
};

pub const TokenTrainEvalOptions = struct {
    max_train_examples: usize = 128,
    max_val_examples: usize = 64,
    epochs: usize = 4,
    learning_rate: f32 = 0.001,
    target_margin: f32 = 0.25,
    teacher_target_blend: f32 = 0.5,
    prefer_latest_on_val_tie: bool = false,
    layer_name: ?[]const u8 = null,
    initial_task_head_input: ?[]const u8 = null,
    save_output_on_completion: bool = true,
    max_grad_norm: f32 = 1.0,
    llrd_decay: f32 = 1.0,
    grad_accum_steps: u32 = 1,
    use_schedule_free: bool = false,
    /// Optional compute backend for gradient computation.
    /// If null, defaults to native CPU math.
    compute_backend: ?*const ComputeBackend = null,
    /// DDP rank of this process. Rank 0 is responsible for checkpoint writes.
    ddp_rank: u32 = 0,
    /// Pre-compiled PJRT gradient executors, one per LoRA layer.
    /// Length must equal bundle.layers.len if non-null.
    pjrt_lora_steps: if (build_options.enable_pjrt) ?[]?graph_bridge.LoRAPjrtTrainStep else void =
        if (build_options.enable_pjrt) null else {},
    /// NEFTune embedding-noise scale (Jain et al., NeurIPS 2023).
    /// 0.0 disables. Typical values: 5.0 - 15.0. Applied only during training,
    /// on each token's text-token hidden vector before the LoRA layers run.
    neftune_alpha: f32 = 0.0,
};

pub const TokenTrainEvalSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    learning_rate: f32,
    epochs: usize,
    max_train_examples: usize,
    max_val_examples: usize,
    target_margin: f32,
    max_grad_norm: f32,
    llrd_decay: f32,
    grad_accum_steps: u32,
    use_schedule_free: bool,
    teacher_target_blend: f32,
    prefer_latest_on_val_tie: bool,
    layer_name: ?[]const u8 = null,
    before_train: TokenMetrics,
    before_val: TokenMetrics,
    after_train: TokenMetrics,
    after_val: TokenMetrics,
    best_epoch: usize,
    epoch_history: []TokenEpochSummary,
};

const SequenceTaskHead = struct {
    hidden_size: usize,
    num_labels: usize,
    label_vocab: [][]const u8,
    dense_weight: []f32,
    dense_bias: []f32,
    out_proj_weight: []f32,
    out_proj_bias: []f32,

    fn deinit(self: *SequenceTaskHead, allocator: std.mem.Allocator) void {
        for (self.label_vocab) |item| allocator.free(item);
        allocator.free(self.label_vocab);
        allocator.free(self.dense_weight);
        allocator.free(self.dense_bias);
        allocator.free(self.out_proj_weight);
        allocator.free(self.out_proj_bias);
        self.* = undefined;
    }
};

const TokenTaskHead = struct {
    hidden_size: usize,
    num_labels: usize,
    label_vocab: [][]const u8,
    classifier_weight: []f32,
    classifier_bias: []f32,

    fn deinit(self: *TokenTaskHead, allocator: std.mem.Allocator) void {
        for (self.label_vocab) |item| allocator.free(item);
        allocator.free(self.label_vocab);
        allocator.free(self.classifier_weight);
        allocator.free(self.classifier_bias);
        self.* = undefined;
    }
};

pub fn bootstrapLoRABundle(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    out_dir: []const u8,
    options: BootstrapOptions,
) !BootstrapSummary {
    if (options.rank == 0) return error.InvalidLoRARank;

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const checkpoint_path = manifest.safetensors_path orelse return error.MissingMergedCheckpoint;

    const requested_target_modules = options.target_modules orelse default_lora_target_modules[0..];
    const resolved_tensors = try inferLoRATargetTensors(allocator, checkpoint_path, requested_target_modules);
    errdefer freeLoRATargetTensors(allocator, resolved_tensors);
    if (resolved_tensors.len == 0) return error.NoLoRATargetTensorsResolved;

    try compat.cwd().createDirPath(compat.io(), out_dir);

    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    errdefer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    errdefer allocator.free(adapter_config_path);

    const base_model_name_or_path = if (options.base_model_name_or_path) |value|
        try allocator.dupe(u8, value)
    else
        try allocator.dupe(u8, model_dir);
    errdefer allocator.free(base_model_name_or_path);

    try writeBootstrapAdapterCheckpoint(allocator, adapter_checkpoint_path, resolved_tensors, options.rank);
    try writeAdapterConfigJson(allocator, adapter_config_path, base_model_name_or_path, options.rank, options.alpha, requested_target_modules, false);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, preprocessor_config_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, special_tokens_map_file_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .checkpoint_path = try allocator.dupe(u8, checkpoint_path),
        .adapter_checkpoint_path = adapter_checkpoint_path,
        .adapter_config_path = adapter_config_path,
        .base_model_name_or_path = base_model_name_or_path,
        .lora_rank = options.rank,
        .lora_alpha = options.alpha,
        .target_modules = try dupeStringSlice(allocator, requested_target_modules),
        .resolved_tensors = resolved_tensors,
    };
}

pub fn inspectLoRABundle(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
) !LoRABundleInspectionSummary {
    var base_manifest = try manifest_mod.loadFromDir(allocator, base_model_dir);
    defer base_manifest.deinit();
    var adapter_manifest = try manifest_mod.loadFromDir(allocator, adapter_model_dir);
    defer adapter_manifest.deinit();

    const base_checkpoint_path = base_manifest.safetensors_path orelse return error.MissingMergedCheckpoint;
    const adapter_checkpoint_path = try requiredPathInDir(allocator, adapter_model_dir, adapter_checkpoint_file_name);
    defer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try optionalPathInDir(allocator, adapter_model_dir, adapter_config_file_name);
    defer if (adapter_config_path) |path| allocator.free(path);

    var base_reader = try safetensors.MMapReader.openFileAbsolute(allocator, base_checkpoint_path);
    defer base_reader.deinit();
    var adapter_reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer adapter_reader.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const adapter_config = if (adapter_config_path) |path| try loadOptionalJson(AdapterConfig, arena_alloc, path) else null;

    var tensors: std.ArrayListUnmanaged(LoRATensorSummary) = .empty;
    errdefer {
        for (tensors.items) |*item| freeLoRATensorSummary(allocator, item);
        tensors.deinit(allocator);
    }

    var it = adapter_reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const adapter_a_name = entry.key_ptr.*;
        const parsed = parseLoRAAdapterTensorName(adapter_a_name) orelse continue;
        if (parsed.kind != .a) continue;

        const adapter_a_info = entry.value_ptr.*;
        const base_tensor_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(base_tensor_name);
        const adapter_b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(adapter_b_name);
        const adapter_b_info = adapter_reader.header.tensors.get(adapter_b_name) orelse return error.MissingAdapterPair;
        const base_info = base_reader.header.tensors.get(base_tensor_name) orelse return error.MissingBaseTensorForAdapter;

        if (adapter_a_info.shape.len != 2 or adapter_b_info.shape.len != 2 or base_info.shape.len != 2) return error.InvalidAdapterTensorShape;

        const rank: usize = @intCast(adapter_a_info.shape[0]);
        const input_dim: usize = @intCast(base_info.shape[1]);
        const output_dim: usize = @intCast(base_info.shape[0]);
        if (adapter_a_info.shape[1] != base_info.shape[1]) return error.AdapterInputDimMismatch;
        if (adapter_b_info.shape[0] != base_info.shape[0]) return error.AdapterOutputDimMismatch;
        if (adapter_a_info.shape[0] != adapter_b_info.shape[1]) return error.AdapterRankMismatch;

        const maybe_dora_name = try doraMagnitudeTensorName(allocator, base_tensor_name);
        defer allocator.free(maybe_dora_name);
        var dora_name_for_summary: ?[]const u8 = null;
        var dora_parameter_count: usize = 0;
        if (adapter_reader.header.tensors.get(maybe_dora_name)) |dora_info| {
            if (dora_info.shape.len != 1) return error.InvalidAdapterTensorShape;
            if (dora_info.shape[0] != base_info.shape[0]) return error.AdapterOutputDimMismatch;
            dora_name_for_summary = try allocator.dupe(u8, maybe_dora_name);
            dora_parameter_count = @intCast(dora_info.shape[0]);
        }

        try tensors.append(allocator, .{
            .base_tensor_name = try allocator.dupe(u8, base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, adapter_a_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, adapter_b_name),
            .dora_magnitude_tensor_name = dora_name_for_summary,
            .module_name = try allocator.dupe(u8, parsed.module_name),
            .input_dim = input_dim,
            .output_dim = output_dim,
            .rank = rank,
            .adapter_parameter_count = rank * input_dim + output_dim * rank + dora_parameter_count,
            .dora_magnitude_parameter_count = dora_parameter_count,
        });
    }

    std.mem.sort(LoRATensorSummary, tensors.items, {}, struct {
        fn lessThan(_: void, lhs: LoRATensorSummary, rhs: LoRATensorSummary) bool {
            return std.mem.lessThan(u8, lhs.base_tensor_name, rhs.base_tensor_name);
        }
    }.lessThan);

    var trainable_parameter_count: usize = 0;
    var dora_magnitude_tensor_count: usize = 0;
    var dora_magnitude_parameter_count: usize = 0;
    for (tensors.items) |item| {
        trainable_parameter_count += item.adapter_parameter_count;
        if (item.dora_magnitude_tensor_name != null) dora_magnitude_tensor_count += 1;
        dora_magnitude_parameter_count += item.dora_magnitude_parameter_count;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, base_checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, adapter_checkpoint_path),
        .adapter_config_path = if (adapter_config_path) |path| try allocator.dupe(u8, path) else null,
        .base_model_name_or_path = if (adapter_config) |cfg| try dupeOptionalString(allocator, cfg.base_model_name_or_path) else null,
        .lora_rank = if (adapter_config) |cfg| cfg.r else null,
        .lora_alpha = if (adapter_config) |cfg| cfg.lora_alpha else null,
        .use_dora = if (adapter_config) |cfg| cfg.use_dora else null,
        .target_module_count = if (adapter_config) |cfg| if (cfg.target_modules) |items| items.len else 0 else 0,
        .target_modules = if (adapter_config) |cfg| try dupeOptionalStringSlice(allocator, cfg.target_modules) else null,
        .resolved_tensor_count = tensors.items.len,
        .trainable_parameter_count = trainable_parameter_count,
        .dora_magnitude_tensor_count = dora_magnitude_tensor_count,
        .dora_magnitude_parameter_count = dora_magnitude_parameter_count,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
}

pub fn loadLoRABundle(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
) !LoadedLoRABundle {
    var inspected = try inspectLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspected);

    if (inspected.lora_rank == null or inspected.lora_alpha == null) return error.MissingAdapterConfig;
    const layers = try allocator.alloc(LoadedLoRALayer, inspected.tensors.len);
    var loaded_count: usize = 0;
    errdefer {
        for (layers[0..loaded_count]) |layer| {
            allocator.free(layer.base_tensor_name);
            allocator.free(layer.adapter_a_tensor_name);
            allocator.free(layer.adapter_b_tensor_name);
            if (layer.dora_magnitude_tensor_name) |name| allocator.free(name);
            allocator.free(layer.module_name);
            allocator.free(layer.base_weight);
            allocator.free(layer.adapter_a);
            allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |magnitude| allocator.free(magnitude);
        }
        allocator.free(layers);
    }

    var base_access = try openTensorAccessForModelDir(allocator, base_model_dir);
    defer base_access.deinit();
    var adapter_access = try openTensorAccessForFile(allocator, inspected.adapter_checkpoint_path);
    defer adapter_access.deinit();

    for (inspected.tensors, 0..) |tensor_summary, idx| {
        var adapter_a_tensor = try loadTensorAsF32(allocator, adapter_access, tensor_summary.adapter_a_tensor_name);
        defer adapter_a_tensor.deinit();
        var adapter_b_tensor = try loadTensorAsF32(allocator, adapter_access, tensor_summary.adapter_b_tensor_name);
        defer adapter_b_tensor.deinit();
        var base_weight_tensor = try loadTensorAsF32(allocator, base_access, tensor_summary.base_tensor_name);
        defer base_weight_tensor.deinit();
        if (adapter_a_tensor.shape.len != 2 or adapter_b_tensor.shape.len != 2 or base_weight_tensor.shape.len != 2) {
            return error.InvalidAdapterTensorShape;
        }

        const adapter_a = try allocator.alloc(f32, tensor_summary.input_dim * tensor_summary.rank);
        errdefer allocator.free(adapter_a);
        transpose2DF32(adapter_a, adapter_a_tensor.asFloat32(), tensor_summary.rank, tensor_summary.input_dim);

        const adapter_b = try allocator.alloc(f32, tensor_summary.rank * tensor_summary.output_dim);
        errdefer allocator.free(adapter_b);
        transpose2DF32(adapter_b, adapter_b_tensor.asFloat32(), tensor_summary.output_dim, tensor_summary.rank);

        const base_weight = try allocator.alloc(f32, tensor_summary.input_dim * tensor_summary.output_dim);
        errdefer allocator.free(base_weight);
        transpose2DF32(base_weight, base_weight_tensor.asFloat32(), tensor_summary.output_dim, tensor_summary.input_dim);

        var dora_magnitude_name: ?[]const u8 = null;
        var dora_magnitude: ?[]f32 = null;
        if (tensor_summary.dora_magnitude_tensor_name) |name| {
            var magnitude_tensor = try loadTensorAsF32(allocator, adapter_access, name);
            defer magnitude_tensor.deinit();
            if (magnitude_tensor.shape.len != 1 or magnitude_tensor.shape[0] != @as(i64, @intCast(tensor_summary.output_dim))) return error.InvalidAdapterTensorShape;
            const magnitude = try allocator.dupe(f32, magnitude_tensor.asFloat32());
            errdefer allocator.free(magnitude);
            dora_magnitude_name = try allocator.dupe(u8, name);
            errdefer if (dora_magnitude_name) |owned| allocator.free(owned);
            dora_magnitude = magnitude;
        }

        layers[idx] = .{
            .base_tensor_name = try allocator.dupe(u8, tensor_summary.base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, tensor_summary.adapter_a_tensor_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, tensor_summary.adapter_b_tensor_name),
            .dora_magnitude_tensor_name = dora_magnitude_name,
            .module_name = try allocator.dupe(u8, tensor_summary.module_name),
            .input_dim = tensor_summary.input_dim,
            .output_dim = tensor_summary.output_dim,
            .rank = tensor_summary.rank,
            .base_weight = base_weight,
            .adapter_a = adapter_a,
            .adapter_b = adapter_b,
            .dora_magnitude = dora_magnitude,
        };
        loaded_count += 1;
    }

    return .{
        .allocator = allocator,
        .base_model_dir = try allocator.dupe(u8, inspected.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, inspected.adapter_model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, inspected.base_checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, inspected.adapter_checkpoint_path),
        .adapter_config_path = try dupeOptionalString(allocator, inspected.adapter_config_path),
        .base_model_name_or_path = try dupeOptionalString(allocator, inspected.base_model_name_or_path),
        .lora_rank = inspected.lora_rank.?,
        .lora_alpha = @floatCast(inspected.lora_alpha.?),
        .target_modules = if (inspected.target_modules) |items|
            try dupeStringSlice(allocator, items)
        else
            try dupeStringSlice(allocator, default_lora_target_modules[0..]),
        .layers = layers,
    };
}

pub fn saveLoRABundle(bundle: *const LoadedLoRABundle, out_dir: []const u8) !void {
    const allocator = bundle.allocator;
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    defer allocator.free(config_path);

    var tensors = try allocator.alloc(WriteTensorF32, bundle.layers.len * 3);
    defer allocator.free(tensors);
    var owned_shapes: std.ArrayListUnmanaged([]const usize) = .empty;
    defer {
        for (owned_shapes.items) |item| allocator.free(item);
        owned_shapes.deinit(allocator);
    }
    var owned_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned_names.items) |item| allocator.free(item);
        owned_names.deinit(allocator);
    }
    var owned_data: std.ArrayListUnmanaged([]const f32) = .empty;
    defer {
        for (owned_data.items) |item| allocator.free(item);
        owned_data.deinit(allocator);
    }

    var tensor_idx: usize = 0;
    for (bundle.layers) |layer| {
        const adapter_a_hf = try allocator.alloc(f32, layer.rank * layer.input_dim);
        const adapter_b_hf = try allocator.alloc(f32, layer.output_dim * layer.rank);
        try owned_data.append(allocator, adapter_a_hf);
        try owned_data.append(allocator, adapter_b_hf);
        transpose2DF32(adapter_a_hf, layer.adapter_a, layer.input_dim, layer.rank);
        transpose2DF32(adapter_b_hf, layer.adapter_b, layer.rank, layer.output_dim);
        const a_shape = try allocator.dupe(usize, &.{ layer.rank, layer.input_dim });
        const b_shape = try allocator.dupe(usize, &.{ layer.output_dim, layer.rank });
        try owned_shapes.append(allocator, a_shape);
        try owned_shapes.append(allocator, b_shape);
        tensors[tensor_idx] = .{ .name = layer.adapter_a_tensor_name, .shape = a_shape, .data = adapter_a_hf };
        tensor_idx += 1;
        tensors[tensor_idx] = .{ .name = layer.adapter_b_tensor_name, .shape = b_shape, .data = adapter_b_hf };
        tensor_idx += 1;

        if (layer.dora_magnitude) |magnitude| {
            const magnitude_data = try allocator.dupe(f32, magnitude);
            try owned_data.append(allocator, magnitude_data);
            const magnitude_name = if (layer.dora_magnitude_tensor_name) |name|
                try allocator.dupe(u8, name)
            else
                try doraMagnitudeTensorName(allocator, layer.base_tensor_name);
            try owned_names.append(allocator, magnitude_name);
            const magnitude_shape = try allocator.dupe(usize, &.{layer.output_dim});
            try owned_shapes.append(allocator, magnitude_shape);
            tensors[tensor_idx] = .{ .name = magnitude_name, .shape = magnitude_shape, .data = magnitude_data };
            tensor_idx += 1;
        }
    }

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, tensors[0..tensor_idx]);
    try writeAdapterConfigJson(
        allocator,
        config_path,
        bundle.base_model_name_or_path orelse bundle.base_model_dir,
        bundle.lora_rank,
        bundle.lora_alpha,
        bundle.target_modules,
        bundleHasDoRA(bundle),
    );
}

pub fn materializeMergedModel(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    task: []const u8,
    out_dir: []const u8,
) !MaterializeSummary {
    if (!std.mem.eql(u8, task, "sequence") and !std.mem.eql(u8, task, "token")) return error.InvalidTask;

    var bundle = try loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();

    var base_access = try openTensorAccessForModelDir(allocator, base_model_dir);
    defer base_access.deinit();
    const base_names = try base_access.listNames(allocator);
    defer allocator.free(base_names);

    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, config_file_name });
    errdefer allocator.free(config_path);
    try compat.cwd().createDirPath(compat.io(), out_dir);

    const head_checkpoint_name = if (std.mem.eql(u8, task, "sequence"))
        sequence_head_checkpoint_file_name
    else
        token_head_checkpoint_file_name;
    const head_config_name = if (std.mem.eql(u8, task, "sequence"))
        sequence_head_config_file_name
    else
        token_head_config_file_name;
    const head_checkpoint_path = try requiredPathInDir(allocator, adapter_model_dir, head_checkpoint_name);
    defer allocator.free(head_checkpoint_path);
    const head_config_path = try requiredPathInDir(allocator, adapter_model_dir, head_config_name);
    defer allocator.free(head_config_path);
    const labels = try loadLabelsFromHeadConfig(allocator, head_config_path);
    errdefer {
        for (labels) |item| allocator.free(item);
        allocator.free(labels);
    }

    var head_access = try openTensorAccessForFile(allocator, head_checkpoint_path);
    defer head_access.deinit();
    const head_names = try head_access.listNames(allocator);
    defer allocator.free(head_names);

    var merged = std.StringArrayHashMapUnmanaged(Tensor){};
    defer {
        var it = merged.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            var tensor = entry.value_ptr.*;
            tensor.deinit();
        }
        merged.deinit(allocator);
    }

    var merged_dora_tensor_count: usize = 0;
    for (bundle.layers) |layer| {
        const merged_weight = try allocator.alloc(f32, layer.base_weight.len);
        const matrix = @import("lora.zig").Matrix{ .rows = layer.input_dim, .cols = layer.output_dim, .data = layer.base_weight };
        const adapter_a = @import("lora.zig").Matrix{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a };
        const adapter_b = @import("lora.zig").Matrix{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b };
        if (layer.dora_magnitude) |magnitude| {
            @import("lora.zig").doraMergeInto(.{
                .base = matrix,
                .adapter_a = adapter_a,
                .adapter_b = adapter_b,
                .magnitude = magnitude,
                .alpha = bundle.lora_alpha,
            }, merged_weight);
            merged_dora_tensor_count += 1;
        } else {
            @import("lora.zig").mergeInto(
                matrix,
                adapter_a,
                adapter_b,
                bundle.lora_alpha,
                merged_weight,
            );
        }

        const out_rows = layer.output_dim;
        const out_cols = layer.input_dim;
        const hf_weight = try allocator.alloc(f32, merged_weight.len);
        defer allocator.free(hf_weight);
        transpose2DF32(hf_weight, merged_weight, out_cols, out_rows);
        allocator.free(merged_weight);
        const shape = [_]i64{ @as(i64, @intCast(out_rows)), @as(i64, @intCast(out_cols)) };
        var merged_tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, &shape, hf_weight);
        errdefer merged_tensor.deinit();
        try merged.put(allocator, try allocator.dupe(u8, layer.base_tensor_name), merged_tensor);
    }

    for (head_names) |name| {
        var tensor = try loadTensorAsF32(allocator, head_access, name);
        errdefer tensor.deinit();
        try merged.put(allocator, try allocator.dupe(u8, name), tensor);
    }

    const file_data = try buildMergedSafetensorsFile(allocator, base_access, base_names, &merged);
    defer allocator.free(file_data);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = checkpoint_path, .data = file_data });

    try writeUpdatedConfig(allocator, base_model_dir, config_path, task, labels);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, preprocessor_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, "vocab.json");
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, "merges.txt");
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, "README.md");
    try copySupportingArtifactIfPresent(allocator, adapter_model_dir, out_dir, head_config_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .task = try allocator.dupe(u8, task),
        .base_model_dir = try allocator.dupe(u8, base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .output_checkpoint_path = checkpoint_path,
        .output_config_path = config_path,
        .labels = labels,
        .merged_lora_tensor_count = bundle.layers.len,
        .merged_dora_tensor_count = merged_dora_tensor_count,
        .copied_base_tensor_count = base_names.len - bundle.layers.len,
        .attached_head_tensor_count = head_names.len,
    };
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

    const input_len = options.input_rows * layer.input_dim;
    const output_len = options.input_rows * layer.output_dim;
    const inputs = try allocator.alloc(f32, input_len);
    defer allocator.free(inputs);
    const targets = try allocator.alloc(f32, output_len);
    defer allocator.free(targets);
    fillDeterministicMatrix(inputs, options.input_rows, layer.input_dim, 0.019, 0.011);
    fillDeterministicMatrix(targets, options.input_rows, layer.output_dim, 0.023, -0.017);

    const a_before = l2Norm(layer.adapter_a);
    const b_before = l2Norm(layer.adapter_b);
    var graph_weight_store = blas_mod.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var graph_compute = blas_mod.BlasCompute.init(allocator, &graph_weight_store, null);
    const graph_cb = graph_compute.computeBackend();
    var graph_optimizer_state = optimizers.OptimizerState.init(allocator);
    defer graph_optimizer_state.deinit();
    var graph_bundle = try graph_bridge.LoRALinearGraph.init(allocator, options.input_rows, layer.input_dim, layer.output_dim, layer.rank, bundle.lora_alpha);
    defer graph_bundle.deinit();
    const graph_summary = try graph_bridge.trainLoRALinearOneStep(
        allocator,
        &graph_cb,
        &graph_bundle,
        .{ .adam = .{} },
        &graph_optimizer_state,
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
        .grad_a_l2_norm = graph_summary.lora_a_grad_l2,
        .grad_b_l2_norm = graph_summary.lora_b_grad_l2,
        .adapter_a_l2_norm_before = a_before,
        .adapter_b_l2_norm_before = b_before,
        .adapter_a_l2_norm_after = l2Norm(layer.adapter_a),
        .adapter_b_l2_norm_after = l2Norm(layer.adapter_b),
    };
}

pub fn trainEvalSequenceLoRABundle(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    train_examples: []const document_data.SequenceExample,
    val_examples: []const document_data.SequenceExample,
    label_vocab: []const []const u8,
    out_dir: []const u8,
    options: SequenceTrainEvalOptions,
) !SequenceTrainEvalSummary {
    if (label_vocab.len == 0) return error.EmptyLabelVocabulary;
    if (label_vocab.len < 2) return error.InsufficientLabelVocabulary;
    const hidden_size = try resolveBundleHiddenSize(allocator, bundle);
    var head = try initSequenceTaskHead(allocator, hidden_size, label_vocab);
    defer head.deinit(allocator);

    const before_train = try evaluateSequenceExamples(allocator, bundle, &head, train_examples, options.max_train_examples, options.layer_name);
    const before_val = try evaluateSequenceExamples(allocator, bundle, &head, val_examples, options.max_val_examples, options.layer_name);
    var best_accuracy = before_val.accuracy;
    var best_epoch: usize = 0;
    var best_bundle = try cloneLoRABundle(allocator, bundle);
    defer best_bundle.deinit();
    var best_head = try cloneSequenceTaskHead(allocator, &head);
    defer best_head.deinit(allocator);
    var epoch_history: std.ArrayListUnmanaged(SequenceEpochSummary) = .empty;
    errdefer epoch_history.deinit(allocator);

    // Monotonic step counter used to make NEFTune noise deterministic per example.
    var neftune_step: u64 = 0;
    for (0..options.epochs) |epoch_idx| {
        const train_metrics = try trainSequenceEpoch(
            allocator,
            bundle,
            &head,
            train_examples,
            options.max_train_examples,
            options.learning_rate,
            options.target_margin,
            options.layer_name,
            options.llrd_decay,
            bundle.layers.len,
            options.use_schedule_free,
            options.compute_backend,
            options.neftune_alpha,
            &neftune_step,
        );
        const train_eval = try evaluateSequenceExamples(allocator, bundle, &head, train_examples, options.max_train_examples, options.layer_name);
        const val_eval = try evaluateSequenceExamples(allocator, bundle, &head, val_examples, options.max_val_examples, options.layer_name);
        const improved_best = val_eval.accuracy > best_accuracy;
        if (improved_best) {
            best_accuracy = val_eval.accuracy;
            best_epoch = epoch_idx + 1;
            best_bundle.deinit();
            best_bundle = try cloneLoRABundle(allocator, bundle);
            best_head.deinit(allocator);
            best_head = try cloneSequenceTaskHead(allocator, &head);
        }
        std.log.info("layoutlmv3 sequence train: epoch={d}/{d} loss={d:.4} train_acc={d:.3} val_acc={d:.3} best={}", .{ epoch_idx + 1, options.epochs, train_metrics.average_loss, train_eval.accuracy, val_eval.accuracy, improved_best });
        try epoch_history.append(allocator, .{
            .epoch = epoch_idx + 1,
            .train_examples_seen = train_metrics.examples_seen,
            .train_average_loss = train_metrics.average_loss,
            .train_accuracy = train_eval.accuracy,
            .eval_accuracy = val_eval.accuracy,
            .improved_best = improved_best,
        });
    }

    bundle.deinit();
    bundle.* = try cloneLoRABundle(allocator, &best_bundle);
    head.deinit(allocator);
    head = try cloneSequenceTaskHead(allocator, &best_head);
    if (options.save_output_on_completion and options.ddp_rank == 0) {
        try saveLoRABundle(bundle, out_dir);
        try saveSequenceTaskHead(allocator, &head, out_dir);
        std.log.info("layoutlmv3 sequence checkpoint: best_epoch={d} acc={d:.3} saved={s}", .{ best_epoch, best_accuracy, out_dir });
    }
    const after_train = try evaluateSequenceExamples(allocator, bundle, &head, train_examples, options.max_train_examples, options.layer_name);
    const after_val = try evaluateSequenceExamples(allocator, bundle, &head, val_examples, options.max_val_examples, options.layer_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, bundle.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, out_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .learning_rate = options.learning_rate,
        .epochs = options.epochs,
        .max_train_examples = options.max_train_examples,
        .max_val_examples = options.max_val_examples,
        .target_margin = options.target_margin,
        .max_grad_norm = options.max_grad_norm,
        .llrd_decay = options.llrd_decay,
        .grad_accum_steps = options.grad_accum_steps,
        .use_schedule_free = options.use_schedule_free,
        .layer_name = try dupeOptionalString(allocator, options.layer_name),
        .before_train = before_train,
        .before_val = before_val,
        .after_train = after_train,
        .after_val = after_val,
        .best_epoch = best_epoch,
        .epoch_history = try epoch_history.toOwnedSlice(allocator),
    };
}

pub fn trainEvalTokenLoRABundle(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    train_examples: []const document_data.TokenTaskExample,
    val_examples: []const document_data.TokenTaskExample,
    label_vocab: []const []const u8,
    out_dir: []const u8,
    options: TokenTrainEvalOptions,
) !TokenTrainEvalSummary {
    if (label_vocab.len == 0) return error.EmptyLabelVocabulary;
    if (label_vocab.len < 2) return error.InsufficientLabelVocabulary;
    const hidden_size = try resolveBundleHiddenSize(allocator, bundle);
    var head = if (try loadTokenTaskHeadIfPresent(allocator, options.initial_task_head_input, hidden_size, label_vocab)) |loaded|
        loaded
    else
        try initTokenTaskHead(allocator, hidden_size, label_vocab);
    defer head.deinit(allocator);

    const before_train = try evaluateTokenExamples(allocator, bundle, &head, train_examples, options.max_train_examples, options.layer_name);
    const before_val = try evaluateTokenExamples(allocator, bundle, &head, val_examples, options.max_val_examples, options.layer_name);
    var best_exact_match = before_val.exact_match_accuracy;
    var best_accuracy = before_val.accuracy;
    var best_train_exact_match = before_train.exact_match_accuracy;
    var best_train_accuracy = before_train.accuracy;
    var best_epoch: usize = 0;
    var best_bundle = try cloneLoRABundle(allocator, bundle);
    defer best_bundle.deinit();
    var best_head = try cloneTokenTaskHead(allocator, &head);
    defer best_head.deinit(allocator);
    var epoch_history: std.ArrayListUnmanaged(TokenEpochSummary) = .empty;
    errdefer epoch_history.deinit(allocator);

    // Monotonic step counter used to make NEFTune noise deterministic per token.
    var neftune_step: u64 = 0;
    for (0..options.epochs) |epoch_idx| {
        const train_metrics = try trainTokenEpoch(
            allocator,
            bundle,
            &head,
            train_examples,
            options.max_train_examples,
            options.learning_rate,
            options.target_margin,
            options.teacher_target_blend,
            options.layer_name,
            options.llrd_decay,
            bundle.layers.len,
            options.max_grad_norm,
            options.grad_accum_steps,
            options.use_schedule_free,
            options.compute_backend,
            options.pjrt_lora_steps,
            options.neftune_alpha,
            &neftune_step,
        );
        const train_eval = try evaluateTokenExamples(allocator, bundle, &head, train_examples, options.max_train_examples, options.layer_name);
        const val_eval = try evaluateTokenExamples(allocator, bundle, &head, val_examples, options.max_val_examples, options.layer_name);
        const improved_best = val_eval.exact_match_accuracy > best_exact_match or
            (val_eval.exact_match_accuracy == best_exact_match and val_eval.accuracy > best_accuracy) or
            (options.prefer_latest_on_val_tie and
                val_eval.exact_match_accuracy == best_exact_match and
                val_eval.accuracy == best_accuracy and
                (train_eval.exact_match_accuracy > best_train_exact_match or
                    (train_eval.exact_match_accuracy == best_train_exact_match and train_eval.accuracy > best_train_accuracy)));
        if (improved_best) {
            best_exact_match = val_eval.exact_match_accuracy;
            best_accuracy = val_eval.accuracy;
            best_train_exact_match = train_eval.exact_match_accuracy;
            best_train_accuracy = train_eval.accuracy;
            best_epoch = epoch_idx + 1;
            best_bundle.deinit();
            best_bundle = try cloneLoRABundle(allocator, bundle);
            best_head.deinit(allocator);
            best_head = try cloneTokenTaskHead(allocator, &head);
        }
        std.log.info("layoutlmv3 token train: epoch={d}/{d} loss={d:.4} train_exact={d:.3} val_exact={d:.3} best={}", .{ epoch_idx + 1, options.epochs, train_metrics.average_loss, train_eval.exact_match_accuracy, val_eval.exact_match_accuracy, improved_best });
        try epoch_history.append(allocator, .{
            .epoch = epoch_idx + 1,
            .train_examples_seen = train_metrics.examples_seen,
            .train_tokens_seen = train_metrics.tokens_seen,
            .train_average_loss = train_metrics.average_loss,
            .train_accuracy = train_eval.accuracy,
            .train_exact_match_accuracy = train_eval.exact_match_accuracy,
            .eval_accuracy = val_eval.accuracy,
            .eval_exact_match_accuracy = val_eval.exact_match_accuracy,
            .improved_best = improved_best,
        });
    }

    bundle.deinit();
    bundle.* = try cloneLoRABundle(allocator, &best_bundle);
    head.deinit(allocator);
    head = try cloneTokenTaskHead(allocator, &best_head);
    if (options.save_output_on_completion and options.ddp_rank == 0) {
        try saveLoRABundle(bundle, out_dir);
        try saveTokenTaskHead(allocator, &head, out_dir);
        std.log.info("layoutlmv3 token checkpoint: best_epoch={d} exact_match={d:.3} saved={s}", .{ best_epoch, best_exact_match, out_dir });
    }
    const after_train = try evaluateTokenExamples(allocator, bundle, &head, train_examples, options.max_train_examples, options.layer_name);
    const after_val = try evaluateTokenExamples(allocator, bundle, &head, val_examples, options.max_val_examples, options.layer_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, bundle.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, out_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .learning_rate = options.learning_rate,
        .epochs = options.epochs,
        .max_train_examples = options.max_train_examples,
        .max_val_examples = options.max_val_examples,
        .target_margin = options.target_margin,
        .max_grad_norm = options.max_grad_norm,
        .llrd_decay = options.llrd_decay,
        .grad_accum_steps = options.grad_accum_steps,
        .use_schedule_free = options.use_schedule_free,
        .teacher_target_blend = options.teacher_target_blend,
        .prefer_latest_on_val_tie = options.prefer_latest_on_val_tie,
        .layer_name = try dupeOptionalString(allocator, options.layer_name),
        .before_train = before_train,
        .before_val = before_val,
        .after_train = after_train,
        .after_val = after_val,
        .best_epoch = best_epoch,
        .epoch_history = try epoch_history.toOwnedSlice(allocator),
    };
}

pub fn freeBootstrapSummary(allocator: std.mem.Allocator, summary: *BootstrapSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.checkpoint_path);
    allocator.free(summary.adapter_checkpoint_path);
    allocator.free(summary.adapter_config_path);
    allocator.free(summary.base_model_name_or_path);
    for (summary.target_modules) |item| allocator.free(item);
    allocator.free(summary.target_modules);
    freeLoRATargetTensors(allocator, summary.resolved_tensors);
    summary.* = undefined;
}

pub fn freeMaterializeSummary(allocator: std.mem.Allocator, summary: *MaterializeSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.task);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.output_checkpoint_path);
    allocator.free(summary.output_config_path);
    for (summary.labels) |item| allocator.free(item);
    allocator.free(summary.labels);
    summary.* = undefined;
}

pub fn freeLoRAOneStepSummary(allocator: std.mem.Allocator, summary: *LoRAOneStepSummary) void {
    allocator.free(summary.layer_name);
    allocator.free(summary.module_name);
    summary.* = undefined;
}

pub fn freeSequenceTrainEvalSummary(allocator: std.mem.Allocator, summary: *SequenceTrainEvalSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    if (summary.layer_name) |value| allocator.free(value);
    allocator.free(summary.epoch_history);
    summary.* = undefined;
}

pub fn freeTokenTrainEvalSummary(allocator: std.mem.Allocator, summary: *TokenTrainEvalSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    if (summary.layer_name) |value| allocator.free(value);
    allocator.free(summary.epoch_history);
    summary.* = undefined;
}

pub fn freeLoRABundleInspectionSummary(allocator: std.mem.Allocator, summary: *LoRABundleInspectionSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.base_checkpoint_path);
    allocator.free(summary.adapter_checkpoint_path);
    if (summary.adapter_config_path) |path| allocator.free(path);
    if (summary.base_model_name_or_path) |path| allocator.free(path);
    if (summary.target_modules) |items| {
        for (items) |item| allocator.free(item);
        allocator.free(items);
    }
    for (summary.tensors) |*item| freeLoRATensorSummary(allocator, item);
    allocator.free(summary.tensors);
    summary.* = undefined;
}

fn freeLoRATargetTensors(allocator: std.mem.Allocator, tensors: []LoRATargetTensor) void {
    for (tensors) |item| {
        allocator.free(item.tensor_name);
        allocator.free(item.module_name);
    }
    allocator.free(tensors);
}

fn freeLoRATensorSummary(allocator: std.mem.Allocator, item: *LoRATensorSummary) void {
    allocator.free(item.base_tensor_name);
    allocator.free(item.adapter_a_tensor_name);
    allocator.free(item.adapter_b_tensor_name);
    if (item.dora_magnitude_tensor_name) |name| allocator.free(name);
    allocator.free(item.module_name);
    item.* = undefined;
}

fn inferLoRATargetTensors(
    allocator: std.mem.Allocator,
    checkpoint_path: []const u8,
    requested_target_modules: []const []const u8,
) ![]LoRATargetTensor {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, checkpoint_path);
    defer reader.deinit();

    var targets: std.ArrayListUnmanaged(LoRATargetTensor) = .empty;
    errdefer {
        for (targets.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        targets.deinit(allocator);
    }

    var it = reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const tensor_name = entry.key_ptr.*;
        const module_name = moduleNameForTensor(tensor_name) orelse continue;
        if (!stringSliceContains(requested_target_modules, module_name)) continue;
        const info = entry.value_ptr.*;
        if (info.shape.len != 2) continue;
        try targets.append(allocator, .{
            .tensor_name = try allocator.dupe(u8, tensor_name),
            .module_name = try allocator.dupe(u8, module_name),
            .output_dim = @intCast(info.shape[0]),
            .input_dim = @intCast(info.shape[1]),
        });
    }

    std.mem.sort(LoRATargetTensor, targets.items, {}, struct {
        fn lessThan(_: void, lhs: LoRATargetTensor, rhs: LoRATargetTensor) bool {
            return std.mem.lessThan(u8, lhs.tensor_name, rhs.tensor_name);
        }
    }.lessThan);
    return targets.toOwnedSlice(allocator);
}

fn moduleNameForTensor(tensor_name: []const u8) ?[]const u8 {
    const normalized = if (std.mem.endsWith(u8, tensor_name, ".weight"))
        tensor_name[0 .. tensor_name.len - ".weight".len]
    else
        tensor_name;

    const ordered_modules = [_][]const u8{
        "intermediate.dense",
        "output.dense",
        "query",
        "value",
        "classifier",
        "dense",
        "key",
    };
    inline for (ordered_modules) |module_name| {
        if (std.mem.eql(u8, normalized, module_name)) return module_name;
        const suffix = "." ++ module_name;
        if (std.mem.endsWith(u8, normalized, suffix)) return module_name;
    }
    return null;
}

const LoRAAdapterTensorKind = enum { a, b };

const ParsedLoRAAdapterTensorName = struct {
    base_tensor_base_name: []const u8,
    module_name: []const u8,
    kind: LoRAAdapterTensorKind,
};

fn parseLoRAAdapterTensorName(tensor_name: []const u8) ?ParsedLoRAAdapterTensorName {
    if (std.mem.endsWith(u8, tensor_name, ".lora_A.weight")) {
        const base = tensor_name[0 .. tensor_name.len - ".lora_A.weight".len];
        const module = moduleNameForTensor(base) orelse return null;
        return .{ .base_tensor_base_name = base, .module_name = module, .kind = .a };
    }
    if (std.mem.endsWith(u8, tensor_name, ".lora_B.weight")) {
        const base = tensor_name[0 .. tensor_name.len - ".lora_B.weight".len];
        const module = moduleNameForTensor(base) orelse return null;
        return .{ .base_tensor_base_name = base, .module_name = module, .kind = .b };
    }
    return null;
}

fn writeBootstrapAdapterCheckpoint(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    resolved_tensors: []const LoRATargetTensor,
    rank: usize,
) !void {
    var tensors = try allocator.alloc(WriteTensorF32, resolved_tensors.len * 2);
    defer allocator.free(tensors);
    var owned_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned_names.items) |item| allocator.free(item);
        owned_names.deinit(allocator);
    }
    var owned_shapes: std.ArrayListUnmanaged([]const usize) = .empty;
    defer {
        for (owned_shapes.items) |item| allocator.free(item);
        owned_shapes.deinit(allocator);
    }
    var owned_data: std.ArrayListUnmanaged([]const f32) = .empty;
    defer {
        for (owned_data.items) |item| allocator.free(item);
        owned_data.deinit(allocator);
    }
    var tensor_idx: usize = 0;

    for (resolved_tensors) |target| {
        const a_data = try buildDeterministicLoraA(allocator, rank, target.input_dim);
        const b_data = try buildZeroF32(allocator, target.output_dim * rank);
        try owned_data.append(allocator, a_data);
        try owned_data.append(allocator, b_data);

        const a_name = try std.fmt.allocPrint(allocator, "{s}.lora_A.weight", .{tensorBaseName(target.tensor_name)});
        const b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{tensorBaseName(target.tensor_name)});
        try owned_names.append(allocator, a_name);
        try owned_names.append(allocator, b_name);
        const a_shape = try allocator.dupe(usize, &.{ rank, target.input_dim });
        const b_shape = try allocator.dupe(usize, &.{ target.output_dim, rank });
        try owned_shapes.append(allocator, a_shape);
        try owned_shapes.append(allocator, b_shape);

        tensors[tensor_idx] = .{ .name = a_name, .shape = a_shape, .data = a_data };
        tensor_idx += 1;
        tensors[tensor_idx] = .{ .name = b_name, .shape = b_shape, .data = b_data };
        tensor_idx += 1;
    }

    try writeHeaderAndTensorsF32(allocator, output_path, tensors[0..tensor_idx]);
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn writeHeaderAndTensorsF32(
    allocator: std.mem.Allocator,
    path: []const u8,
    tensors: []const WriteTensorF32,
) !void {
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

fn buildDeterministicLoraA(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]f32 {
    const data = try allocator.alloc(f32, rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            const idx = row * cols + col;
            const angle = @as(f32, @floatFromInt((row + 1) * (col + 3)));
            data[idx] = @sin(angle * 0.013) * 0.01;
        }
    }
    return data;
}

fn buildZeroF32(allocator: std.mem.Allocator, len: usize) ![]f32 {
    const data = try allocator.alloc(f32, len);
    @memset(data, 0.0);
    return data;
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) {
        return tensor_name[0 .. tensor_name.len - ".weight".len];
    }
    return tensor_name;
}

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    target_modules: []const []const u8,
    use_dora: bool,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .base_model_name_or_path = base_model_name_or_path,
        .peft_type = "LORA",
        .task_type = "FEATURE_EXTRACTION",
        .r = rank,
        .lora_alpha = alpha,
        .target_modules = target_modules,
        .use_dora = use_dora,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn doraMagnitudeTensorName(allocator: std.mem.Allocator, base_tensor_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.lora_magnitude_vector.weight", .{base_tensor_name});
}

fn bundleHasDoRA(bundle: *const LoadedLoRABundle) bool {
    for (bundle.layers) |layer| {
        if (layer.dora_magnitude != null) return true;
    }
    return false;
}

fn copySupportingArtifactIfPresent(
    allocator: std.mem.Allocator,
    source_dir: []const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src = try optionalPathInDir(allocator, source_dir, file_name);
    defer if (src) |path| allocator.free(path);
    const src_path = src orelse return;
    const bytes = try c_file.readFile(allocator, src_path);
    defer allocator.free(bytes);
    const dst = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst, .data = bytes });
}

fn loadLabelsFromHeadConfig(allocator: std.mem.Allocator, path: []const u8) ![][]const u8 {
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const labels_value = parsed.value.object.get("labels") orelse return error.MissingLabelVocabulary;
    if (labels_value != .array) return error.InvalidLabelVocabulary;
    const out = try allocator.alloc([]const u8, labels_value.array.items.len);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |item| allocator.free(item);
        allocator.free(out);
    }
    for (labels_value.array.items, 0..) |item, idx| {
        if (item != .string) return error.InvalidLabelVocabulary;
        out[idx] = try allocator.dupe(u8, item.string);
        built += 1;
    }
    return out;
}

fn writeUpdatedConfig(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    output_path: []const u8,
    task: []const u8,
    labels: []const []const u8,
) !void {
    const src_path = try requiredPathInDir(allocator, base_model_dir, config_file_name);
    defer allocator.free(src_path);
    const bytes = try c_file.readFile(allocator, src_path);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidConfigJson;
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    const writer = &buffer.writer;
    try writer.writeByte('{');
    var first = true;

    const replacement_arch = if (std.mem.eql(u8, task, "sequence"))
        "LayoutLMv3ForSequenceClassification"
    else
        "LayoutLMv3ForTokenClassification";

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "num_labels") or
            std.mem.eql(u8, key, "id2label") or
            std.mem.eql(u8, key, "label2id") or
            std.mem.eql(u8, key, "architectures"))
        {
            continue;
        }
        try writeJsonObjectFieldPrefix(writer, &first, key);
        try std.json.Stringify.value(value, .{}, writer);
    }

    try writeJsonObjectFieldPrefix(writer, &first, "num_labels");
    try writer.print("{d}", .{labels.len});

    try writeJsonObjectFieldPrefix(writer, &first, "id2label");
    try writeId2LabelJson(writer, labels);

    try writeJsonObjectFieldPrefix(writer, &first, "label2id");
    try writeLabel2IdJson(writer, labels);

    try writeJsonObjectFieldPrefix(writer, &first, "architectures");
    try writer.writeAll("[");
    try std.json.Stringify.value(replacement_arch, .{}, writer);
    try writer.writeAll("]");

    try writer.writeByte('}');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = output_path, .data = buffer.written() });
}

fn writeJsonObjectFieldPrefix(writer: *std.Io.Writer, first: *bool, key: []const u8) !void {
    if (!first.*) try writer.writeByte(',');
    first.* = false;
    try std.json.Stringify.value(key, .{}, writer);
    try writer.writeByte(':');
}

fn writeId2LabelJson(writer: *std.Io.Writer, labels: []const []const u8) !void {
    try writer.writeByte('{');
    for (labels, 0..) |label, idx| {
        if (idx != 0) try writer.writeByte(',');
        const key_buf = try std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{idx});
        defer std.heap.page_allocator.free(key_buf);
        try std.json.Stringify.value(key_buf, .{}, writer);
        try writer.writeByte(':');
        try std.json.Stringify.value(label, .{}, writer);
    }
    try writer.writeByte('}');
}

fn writeLabel2IdJson(writer: *std.Io.Writer, labels: []const []const u8) !void {
    try writer.writeByte('{');
    for (labels, 0..) |label, idx| {
        if (idx != 0) try writer.writeByte(',');
        try std.json.Stringify.value(label, .{}, writer);
        try writer.writeByte(':');
        try writer.print("{d}", .{idx});
    }
    try writer.writeByte('}');
}

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    if (std.mem.endsWith(u8, path, ".index.json")) {
        const access = try tensor_access.ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
}

fn openTensorAccessForModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !tensor_access.TensorAccess {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    return try tensor_access.openFromManifest(allocator, manifest);
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

fn buildMergedSafetensorsFile(
    allocator: std.mem.Allocator,
    base_access: tensor_access.TensorAccess,
    base_names: [][]const u8,
    merged: *const std.StringArrayHashMapUnmanaged(Tensor),
) ![]u8 {
    var ordered_names = try allocator.alloc([]const u8, base_names.len + merged.count());
    defer allocator.free(ordered_names);
    var count: usize = 0;
    for (base_names) |name| {
        ordered_names[count] = name;
        count += 1;
    }
    var it_merged = merged.iterator();
    while (it_merged.next()) |entry| {
        if (!stringSliceContains(base_names, entry.key_ptr.*)) {
            ordered_names[count] = entry.key_ptr.*;
            count += 1;
        }
    }
    std.mem.sort([]const u8, ordered_names[0..count], {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    try header_buf.writer.writeByte('{');

    var data_parts = std.ArrayListUnmanaged([]const u8).empty;
    defer data_parts.deinit(allocator);
    var owned_records = std.ArrayListUnmanaged(Tensor).empty;
    defer {
        for (owned_records.items) |*tensor| tensor.deinit();
        owned_records.deinit(allocator);
    }

    var offset: u64 = 0;
    for (ordered_names[0..count], 0..) |name, idx| {
        var tensor: Tensor = undefined;
        if (merged.get(name)) |existing| {
            tensor = existing;
        } else {
            tensor = try loadTensorAsF32(allocator, base_access, name);
            try owned_records.append(allocator, tensor);
        }

        const shape = tensor.shape;
        const byte_len = tensor.data.len;
        if (idx != 0) try header_buf.writer.writeByte(',');
        try header_buf.writer.print("\"{s}\":{{\"dtype\":\"{s}\",\"shape\":[", .{ name, dtypeName(tensor.dtype) });
        for (shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try header_buf.writer.writeByte(',');
            try header_buf.writer.print("{}", .{dim});
        }
        try header_buf.writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        try data_parts.append(allocator, tensor.data);
        offset += byte_len;
    }
    try header_buf.writer.writeByte('}');

    var file = std.ArrayListUnmanaged(u8).empty;
    errdefer file.deinit(allocator);
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.appendSlice(allocator, &len_buf);
    try file.appendSlice(allocator, header_buf.written());
    for (data_parts.items) |part| try file.appendSlice(allocator, part);
    return try file.toOwnedSlice(allocator);
}

fn dtypeName(dtype: DType) []const u8 {
    return switch (dtype) {
        .f32 => "F32",
        .f16 => "F16",
        .bf16 => "BF16",
        .f64 => "F64",
        .i8 => "I8",
        .i16 => "I16",
        .i32 => "I32",
        .i64 => "I64",
        .u8 => "U8",
        .bool_ => "BOOL",
    };
}

fn requiredPathInDir(allocator: std.mem.Allocator, dir_path: []const u8, basename: []const u8) ![]u8 {
    return (try optionalPathInDir(allocator, dir_path, basename)) orelse error.FileNotFound;
}

fn optionalPathInDir(allocator: std.mem.Allocator, dir_path: []const u8, basename: []const u8) !?[]u8 {
    const path = try std.fs.path.join(allocator, &.{ dir_path, basename });
    errdefer allocator.free(path);
    compat.cwd().access(compat.io(), path, .{}) catch {
        allocator.free(path);
        return null;
    };
    return path;
}

fn loadOptionalJson(comptime T: type, allocator: std.mem.Allocator, path: []const u8) !?T {
    const bytes = c_file.readFile(allocator, path) catch return null;
    defer allocator.free(bytes);
    return try std.json.parseFromSliceLeaky(T, allocator, bytes, .{ .ignore_unknown_fields = true });
}

fn dupeStringSlice(allocator: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const out = try allocator.alloc([]const u8, value.len);
    errdefer allocator.free(out);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |item| allocator.free(item);
    }
    for (value, 0..) |item, idx| {
        out[idx] = try allocator.dupe(u8, item);
        built += 1;
    }
    return out;
}

fn dupeOptionalStringSlice(allocator: std.mem.Allocator, value: ?[]const []const u8) !?[][]const u8 {
    const items = value orelse return null;
    return try dupeStringSlice(allocator, items);
}

fn dupeOptionalString(allocator: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    const item = value orelse return null;
    return try allocator.dupe(u8, item);
}

fn cloneLoRABundle(allocator: std.mem.Allocator, source: *const LoadedLoRABundle) !LoadedLoRABundle {
    const layers = try allocator.alloc(LoadedLoRALayer, source.layers.len);
    var loaded_count: usize = 0;
    errdefer {
        for (layers[0..loaded_count]) |layer| {
            allocator.free(layer.base_tensor_name);
            allocator.free(layer.adapter_a_tensor_name);
            allocator.free(layer.adapter_b_tensor_name);
            if (layer.dora_magnitude_tensor_name) |name| allocator.free(name);
            allocator.free(layer.module_name);
            allocator.free(layer.base_weight);
            allocator.free(layer.adapter_a);
            allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |magnitude| allocator.free(magnitude);
        }
        allocator.free(layers);
    }
    for (source.layers, 0..) |layer, idx| {
        layers[idx] = .{
            .base_tensor_name = try allocator.dupe(u8, layer.base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, layer.adapter_a_tensor_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, layer.adapter_b_tensor_name),
            .dora_magnitude_tensor_name = try dupeOptionalString(allocator, layer.dora_magnitude_tensor_name),
            .module_name = try allocator.dupe(u8, layer.module_name),
            .input_dim = layer.input_dim,
            .output_dim = layer.output_dim,
            .rank = layer.rank,
            .base_weight = try allocator.dupe(f32, layer.base_weight),
            .adapter_a = try allocator.dupe(f32, layer.adapter_a),
            .adapter_b = try allocator.dupe(f32, layer.adapter_b),
            .dora_magnitude = if (layer.dora_magnitude) |magnitude| try allocator.dupe(f32, magnitude) else null,
        };
        loaded_count += 1;
    }
    return .{
        .allocator = allocator,
        .base_model_dir = try allocator.dupe(u8, source.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, source.adapter_model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, source.base_checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, source.adapter_checkpoint_path),
        .adapter_config_path = try dupeOptionalString(allocator, source.adapter_config_path),
        .base_model_name_or_path = try dupeOptionalString(allocator, source.base_model_name_or_path),
        .lora_rank = source.lora_rank,
        .lora_alpha = source.lora_alpha,
        .target_modules = try dupeStringSlice(allocator, source.target_modules),
        .layers = layers,
    };
}

fn stringSliceContains(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn findLoadedLayerIndex(layers: []const LoadedLoRALayer, layer_name: []const u8) ?usize {
    for (layers, 0..) |layer, idx| {
        if (layerMatchesScope(layer.base_tensor_name, layer_name)) return idx;
    }
    return null;
}

const LayoutLMv3ScopePreset = enum {
    token_top1,
    token_top3,
    sequence_top3,
};

fn layoutlmv3ScopePreset(selector: []const u8) ?LayoutLMv3ScopePreset {
    if (std.mem.eql(u8, selector, "@layoutlmv3_token_top1")) return .token_top1;
    if (std.mem.eql(u8, selector, "@layoutlmv3_token_top3")) return .token_top3;
    if (std.mem.eql(u8, selector, "@layoutlmv3_sequence_top3")) return .sequence_top3;
    return null;
}

fn parseLayoutLMv3EncoderLayerIndex(layer_base_tensor_name: []const u8) ?usize {
    const prefix = "encoder.layer.";
    const start = std.mem.indexOf(u8, layer_base_tensor_name, prefix) orelse return null;
    const digits = layer_base_tensor_name[start + prefix.len ..];
    var end: usize = 0;
    while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseUnsigned(usize, digits[0..end], 10) catch null;
}

fn layerLearningRate(base_tensor_name: []const u8, base_lr: f32, llrd_decay: f32, num_layers: usize) f32 {
    if (llrd_decay >= 1.0) return base_lr;
    const layer_idx = parseLayoutLMv3EncoderLayerIndex(base_tensor_name) orelse return base_lr;
    // layer 0 = shallowest = lowest LR
    const depth = if (num_layers > 0) num_layers - 1 - @min(layer_idx, num_layers - 1) else 0;
    return base_lr * std.math.pow(f32, llrd_decay, @as(f32, @floatFromInt(depth)));
}

fn layerContainsAnyModule(layer_base_tensor_name: []const u8, modules: []const []const u8) bool {
    for (modules) |module_name| {
        if (std.mem.indexOf(u8, layer_base_tensor_name, module_name) != null) return true;
    }
    return false;
}

fn matchesLayoutLMv3Preset(layer_base_tensor_name: []const u8, preset: LayoutLMv3ScopePreset) bool {
    const layer_idx = parseLayoutLMv3EncoderLayerIndex(layer_base_tensor_name) orelse return false;
    return switch (preset) {
        .token_top1 => layer_idx >= 11 and layerContainsAnyModule(layer_base_tensor_name, &.{
            "attention.self.query",
            "attention.self.value",
            "attention.output.dense",
            "output.dense",
        }),
        .token_top3 => layer_idx >= 9 and layerContainsAnyModule(layer_base_tensor_name, &.{
            "attention.self.query",
            "attention.self.value",
            "attention.output.dense",
            "intermediate.dense",
            "output.dense",
        }),
        .sequence_top3 => layer_idx >= 9 and layerContainsAnyModule(layer_base_tensor_name, &.{
            "attention.self.query",
            "attention.self.key",
            "attention.self.value",
            "attention.output.dense",
            "intermediate.dense",
            "output.dense",
        }),
    };
}

fn layerMatchesScope(layer_base_tensor_name: []const u8, layer_name: ?[]const u8) bool {
    const selector = layer_name orelse return true;
    if (layoutlmv3ScopePreset(selector)) |preset| {
        return matchesLayoutLMv3Preset(layer_base_tensor_name, preset);
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
    for (params, grads) |*param, grad| {
        param.* -= learning_rate * grad;
    }
}

const LoRAAdamState = struct {
    allocator: std.mem.Allocator,
    m: []f32,
    v: []f32,
    step: u64,

    fn init(allocator: std.mem.Allocator, size: usize) !LoRAAdamState {
        const m = try allocator.alloc(f32, size);
        errdefer allocator.free(m);
        const v = try allocator.alloc(f32, size);
        errdefer allocator.free(v);
        @memset(m, 0);
        @memset(v, 0);
        return .{ .allocator = allocator, .m = m, .v = v, .step = 0 };
    }

    fn deinit(self: *LoRAAdamState) void {
        self.allocator.free(self.m);
        self.allocator.free(self.v);
        self.* = undefined;
    }
};

const LoRAScheduleFreeState = struct {
    allocator: std.mem.Allocator,
    z: []f32, // base iterate
    v: []f32, // second moment (EMA of g^2)
    step: u64,

    fn init(allocator: std.mem.Allocator, initial_weights: []const f32) !LoRAScheduleFreeState {
        const z = try allocator.dupe(f32, initial_weights);
        errdefer allocator.free(z);
        const v = try allocator.alloc(f32, initial_weights.len);
        errdefer allocator.free(v);
        @memset(v, 0);
        return .{ .allocator = allocator, .z = z, .v = v, .step = 0 };
    }

    fn deinit(self: *LoRAScheduleFreeState) void {
        self.allocator.free(self.z);
        self.allocator.free(self.v);
        self.* = undefined;
    }
};

fn applyScheduleFreeStep(params: []f32, grads: []const f32, state: *LoRAScheduleFreeState, lr: f32) void {
    state.step += 1;
    const t: f32 = @floatFromInt(state.step);
    const beta1: f32 = 0.9; // c parameter for Polyak averaging
    const beta2: f32 = 0.999;
    const epsilon: f32 = 1e-8;
    const weight_decay: f32 = 0.01;
    const c = @min(@as(f32, 0.9), 1.0 / t);
    for (params, grads, state.z, state.v) |*x, g, *z, *v| {
        v.* = beta2 * v.* + (1.0 - beta2) * g * g;
        const v_hat = v.* / (1.0 - std.math.pow(f32, beta2, t));
        z.* = z.* - lr * g / (@sqrt(v_hat) + epsilon) - lr * weight_decay * z.*;
        x.* = (1.0 - c) * x.* + c * z.*;
    }
    _ = beta1; // beta1 is the Polyak mixing coefficient, expressed as c above
}

fn applyAdamWStep(
    params: []f32,
    grads: []const f32,
    state: *LoRAAdamState,
    lr: f32,
) void {
    std.debug.assert(params.len == grads.len);
    state.step += 1;
    const t: f32 = @floatFromInt(state.step);
    const beta1: f32 = 0.9;
    const beta2: f32 = 0.999;
    const epsilon: f32 = 1e-8;
    const weight_decay: f32 = 0.01;
    const bc1 = 1.0 - std.math.pow(f32, beta1, t);
    const bc2 = 1.0 - std.math.pow(f32, beta2, t);
    for (params, grads, state.m, state.v) |*param, grad, *m, *v| {
        m.* = beta1 * m.* + (1.0 - beta1) * grad;
        v.* = beta2 * v.* + (1.0 - beta2) * grad * grad;
        const m_hat = m.* / bc1;
        const v_hat = v.* / bc2;
        param.* -= lr * (m_hat / (@sqrt(v_hat) + epsilon) + weight_decay * param.*);
    }
}

fn l2Norm(values: []const f32) f64 {
    var total: f64 = 0;
    for (values) |value| {
        const widened: f64 = value;
        total += widened * widened;
    }
    return @sqrt(total);
}

fn transpose2DF32(out: []f32, input: []const f32, rows: usize, cols: usize) void {
    std.debug.assert(out.len == rows * cols);
    std.debug.assert(input.len == rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            out[col * rows + row] = input[row * cols + col];
        }
    }
}

fn computeSequenceFeatures(out: *[8]f32, ex: document_data.SequenceExample) void {
    out[0] = 1.0;
    out[1] = @as(f32, @floatFromInt(ex.num_tokens)) / 512.0;
    out[2] = @as(f32, @floatFromInt(ex.image_width)) / 2000.0;
    out[3] = @as(f32, @floatFromInt(ex.image_height)) / 2000.0;
    out[4] = ex.mean_darkness;
    out[5] = ex.std_darkness;
    out[6] = ex.top_darkness - ex.bottom_darkness;
    out[7] = ex.left_darkness - ex.right_darkness;
}

fn buildLayerInputVector(out: []f32, features: []const f32) void {
    for (out, 0..) |*value, idx| {
        if (idx < features.len) {
            value.* = features[idx];
            continue;
        }
        const a = features[idx % features.len];
        const b = features[(idx * 3 + 1) % features.len];
        const c = features[(idx * 5 + 2) % features.len];
        const angle = @as(f32, @floatFromInt(idx + 1));
        value.* = 0.6 * a + 0.25 * b + 0.1 * c + 0.05 * @sin(angle * 0.17);
    }
}

fn resolveBundleHiddenSize(allocator: std.mem.Allocator, bundle: *const LoadedLoRABundle) !usize {
    const config_path = try requiredPathInDir(allocator, bundle.base_model_dir, config_file_name);
    defer allocator.free(config_path);
    const bytes = try c_file.readFile(allocator, config_path);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value == .object) {
        if (parsed.value.object.get("hidden_size")) |hidden_size_value| {
            if (hidden_size_value == .integer) return @intCast(hidden_size_value.integer);
        }
    }
    for (bundle.layers) |layer| {
        if (layer.input_dim > 0) return layer.input_dim;
    }
    return error.MissingHiddenSize;
}

fn initSequenceTaskHead(allocator: std.mem.Allocator, hidden_size: usize, label_vocab: []const []const u8) !SequenceTaskHead {
    const dense_weight = try allocator.alloc(f32, hidden_size * hidden_size);
    const dense_bias = try allocator.alloc(f32, hidden_size);
    const out_proj_weight = try allocator.alloc(f32, hidden_size * label_vocab.len);
    const out_proj_bias = try allocator.alloc(f32, label_vocab.len);
    fillDeterministicMatrix(dense_weight, hidden_size, hidden_size, 0.0031, 0.0);
    @memset(dense_bias, 0.0);
    fillDeterministicMatrix(out_proj_weight, hidden_size, label_vocab.len, 0.0053, 0.0);
    @memset(out_proj_bias, 0.0);
    return .{
        .hidden_size = hidden_size,
        .num_labels = label_vocab.len,
        .label_vocab = try dupeStringSlice(allocator, label_vocab),
        .dense_weight = dense_weight,
        .dense_bias = dense_bias,
        .out_proj_weight = out_proj_weight,
        .out_proj_bias = out_proj_bias,
    };
}

fn cloneSequenceTaskHead(allocator: std.mem.Allocator, source: *const SequenceTaskHead) !SequenceTaskHead {
    return .{
        .hidden_size = source.hidden_size,
        .num_labels = source.num_labels,
        .label_vocab = try dupeStringSlice(allocator, source.label_vocab),
        .dense_weight = try allocator.dupe(f32, source.dense_weight),
        .dense_bias = try allocator.dupe(f32, source.dense_bias),
        .out_proj_weight = try allocator.dupe(f32, source.out_proj_weight),
        .out_proj_bias = try allocator.dupe(f32, source.out_proj_bias),
    };
}

fn sequenceTaskHeadToMlpHead(allocator: std.mem.Allocator, head: *const SequenceTaskHead) !graph_bridge.MlpHead {
    const dense_weight = try allocator.alloc(f32, head.hidden_size * head.hidden_size);
    errdefer allocator.free(dense_weight);
    transpose2DF32(dense_weight, head.dense_weight, head.hidden_size, head.hidden_size);
    const dense_bias = try allocator.dupe(f32, head.dense_bias);
    errdefer allocator.free(dense_bias);
    const out_weight = try allocator.alloc(f32, head.num_labels * head.hidden_size);
    errdefer allocator.free(out_weight);
    transpose2DF32(out_weight, head.out_proj_weight, head.hidden_size, head.num_labels);
    const out_bias = try allocator.dupe(f32, head.out_proj_bias);
    errdefer allocator.free(out_bias);
    return .{
        .allocator = allocator,
        .dense_weight = dense_weight,
        .dense_bias = dense_bias,
        .out_weight = out_weight,
        .out_bias = out_bias,
        .input_dim = head.hidden_size,
        .hidden_dim = head.hidden_size,
        .num_labels = head.num_labels,
    };
}

fn copyMlpHeadIntoSequenceTaskHead(head: *SequenceTaskHead, mlp_head: *const graph_bridge.MlpHead) void {
    std.debug.assert(head.hidden_size == mlp_head.input_dim);
    std.debug.assert(head.hidden_size == mlp_head.hidden_dim);
    std.debug.assert(head.num_labels == mlp_head.num_labels);
    transpose2DF32(head.dense_weight, mlp_head.dense_weight, mlp_head.hidden_dim, mlp_head.input_dim);
    @memcpy(head.dense_bias, mlp_head.dense_bias);
    transpose2DF32(head.out_proj_weight, mlp_head.out_weight, mlp_head.num_labels, mlp_head.hidden_dim);
    @memcpy(head.out_proj_bias, mlp_head.out_bias);
}

fn trainSequenceHeadGraphOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    optimizer_state: *optimizers.OptimizerState,
    head: *SequenceTaskHead,
    hidden: []const f32,
    label: usize,
    learning_rate: f32,
) !graph_bridge.MlpTrainSummary {
    if (hidden.len != head.hidden_size) return error.ShapeMismatch;
    var graph_bundle = try graph_bridge.MlpClassifierGraph.init(allocator, 1, head.hidden_size, head.hidden_size, head.num_labels);
    defer graph_bundle.deinit();
    var mlp_head = try sequenceTaskHeadToMlpHead(allocator, head);
    defer mlp_head.deinit();
    const labels = [_]usize{label};
    const summary = try graph_bridge.trainMlpClassifierOneStep(
        allocator,
        cb,
        &graph_bundle,
        &mlp_head,
        .{
            .features = hidden,
            .labels = labels[0..],
            .rows = 1,
            .input_dim = head.hidden_size,
            .num_labels = head.num_labels,
        },
        .{ .adamw = .{} },
        optimizer_state,
        learning_rate,
    );
    copyMlpHeadIntoSequenceTaskHead(head, &mlp_head);
    return summary;
}

fn initTokenTaskHead(allocator: std.mem.Allocator, hidden_size: usize, label_vocab: []const []const u8) !TokenTaskHead {
    const classifier_weight = try allocator.alloc(f32, hidden_size * label_vocab.len);
    const classifier_bias = try allocator.alloc(f32, label_vocab.len);
    @memset(classifier_weight, 0.0);
    @memset(classifier_bias, 0.0);
    return .{
        .hidden_size = hidden_size,
        .num_labels = label_vocab.len,
        .label_vocab = try dupeStringSlice(allocator, label_vocab),
        .classifier_weight = classifier_weight,
        .classifier_bias = classifier_bias,
    };
}

fn cloneTokenTaskHead(allocator: std.mem.Allocator, source: *const TokenTaskHead) !TokenTaskHead {
    return .{
        .hidden_size = source.hidden_size,
        .num_labels = source.num_labels,
        .label_vocab = try dupeStringSlice(allocator, source.label_vocab),
        .classifier_weight = try allocator.dupe(f32, source.classifier_weight),
        .classifier_bias = try allocator.dupe(f32, source.classifier_bias),
    };
}

fn tokenTaskHeadToLinearHead(allocator: std.mem.Allocator, head: *const TokenTaskHead) !graph_bridge.LinearHead {
    const weight = try allocator.alloc(f32, head.num_labels * head.hidden_size);
    errdefer allocator.free(weight);
    transpose2DF32(weight, head.classifier_weight, head.hidden_size, head.num_labels);
    const bias = try allocator.dupe(f32, head.classifier_bias);
    errdefer allocator.free(bias);
    return .{
        .allocator = allocator,
        .weight = weight,
        .bias = bias,
        .num_labels = head.num_labels,
        .input_dim = head.hidden_size,
    };
}

fn copyLinearHeadIntoTokenTaskHead(head: *TokenTaskHead, linear_head: *const graph_bridge.LinearHead) void {
    std.debug.assert(head.num_labels == linear_head.num_labels);
    std.debug.assert(head.hidden_size == linear_head.input_dim);
    transpose2DF32(head.classifier_weight, linear_head.weight, linear_head.num_labels, linear_head.input_dim);
    @memcpy(head.classifier_bias, linear_head.bias);
}

fn trainTokenHeadGraphOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    optimizer_state: *optimizers.OptimizerState,
    head: *TokenTaskHead,
    features: []const f32,
    labels: []const usize,
    learning_rate: f32,
) !graph_bridge.LinearTrainSummary {
    if (labels.len == 0) return error.EmptyTokenBatch;
    if (features.len != labels.len * head.hidden_size) return error.ShapeMismatch;

    var graph_bundle = try graph_bridge.LinearClassifierGraph.init(allocator, labels.len, head.hidden_size, head.num_labels);
    defer graph_bundle.deinit();
    var linear_head = try tokenTaskHeadToLinearHead(allocator, head);
    defer linear_head.deinit();

    const summary = try graph_bridge.trainLinearClassifierOneStep(
        allocator,
        cb,
        &graph_bundle,
        &linear_head,
        .{
            .features = features,
            .labels = labels,
            .rows = labels.len,
            .input_dim = head.hidden_size,
            .num_labels = head.num_labels,
        },
        .{ .adamw = .{} },
        optimizer_state,
        learning_rate,
    );
    copyLinearHeadIntoTokenTaskHead(head, &linear_head);
    return summary;
}

fn saveSequenceTaskHead(allocator: std.mem.Allocator, head: *const SequenceTaskHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, sequence_head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, sequence_head_config_file_name });
    defer allocator.free(config_path);

    const dense_weight_hf = try allocator.alloc(f32, head.hidden_size * head.hidden_size);
    defer allocator.free(dense_weight_hf);
    const out_proj_weight_hf = try allocator.alloc(f32, head.num_labels * head.hidden_size);
    defer allocator.free(out_proj_weight_hf);
    transpose2DF32(dense_weight_hf, head.dense_weight, head.hidden_size, head.hidden_size);
    transpose2DF32(out_proj_weight_hf, head.out_proj_weight, head.hidden_size, head.num_labels);

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "classifier.dense.weight", .shape = &.{ head.hidden_size, head.hidden_size }, .data = dense_weight_hf },
        .{ .name = "classifier.dense.bias", .shape = &.{head.hidden_size}, .data = head.dense_bias },
        .{ .name = "classifier.out_proj.weight", .shape = &.{ head.num_labels, head.hidden_size }, .data = out_proj_weight_hf },
        .{ .name = "classifier.out_proj.bias", .shape = &.{head.num_labels}, .data = head.out_proj_bias },
    });
    try writeTaskHeadConfigJson(allocator, config_path, "sequence_classification", head.hidden_size, head.label_vocab);
}

fn saveTokenTaskHead(allocator: std.mem.Allocator, head: *const TokenTaskHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, token_head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, token_head_config_file_name });
    defer allocator.free(config_path);

    const classifier_weight_hf = try allocator.alloc(f32, head.num_labels * head.hidden_size);
    defer allocator.free(classifier_weight_hf);
    transpose2DF32(classifier_weight_hf, head.classifier_weight, head.hidden_size, head.num_labels);

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "classifier.weight", .shape = &.{ head.num_labels, head.hidden_size }, .data = classifier_weight_hf },
        .{ .name = "classifier.bias", .shape = &.{head.num_labels}, .data = head.classifier_bias },
    });
    try writeTaskHeadConfigJson(allocator, config_path, "token_classification", head.hidden_size, head.label_vocab);
}

fn loadTokenTaskHeadIfPresent(
    allocator: std.mem.Allocator,
    model_input: ?[]const u8,
    expected_hidden_size: usize,
    expected_label_vocab: []const []const u8,
) !?TokenTaskHead {
    const input = model_input orelse return null;
    const model_dir = blk: {
        const stat = compat.cwd().statFile(compat.io(), input, .{}) catch break :blk input;
        if (stat.kind == .directory) break :blk input;
        break :blk (std.fs.path.dirname(input) orelse ".");
    };
    const checkpoint_path = try optionalPathInDir(allocator, model_dir, token_head_checkpoint_file_name);
    defer if (checkpoint_path) |path| allocator.free(path);
    const config_path = try optionalPathInDir(allocator, model_dir, token_head_config_file_name);
    defer if (config_path) |path| allocator.free(path);
    if (checkpoint_path == null or config_path == null) return null;

    const raw_cfg = try compat.cwd().readFileAlloc(compat.io(), config_path.?, allocator, .limited(8 * 1024 * 1024));
    defer allocator.free(raw_cfg);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_cfg, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidConfigJson;
    const root = parsed.value.object;
    const hidden_size_value = root.get("hidden_size") orelse return error.MissingHiddenSize;
    if (hidden_size_value != .integer) return error.InvalidConfigJson;
    const hidden_size: usize = @intCast(hidden_size_value.integer);
    if (hidden_size != expected_hidden_size) return error.HiddenSizeMismatch;
    const labels_value = root.get("labels") orelse return error.MissingLabelVocabulary;
    if (labels_value != .array or labels_value.array.items.len != expected_label_vocab.len) return error.LabelVocabularyMismatch;
    for (labels_value.array.items, expected_label_vocab) |item, expected| {
        if (item != .string or !std.mem.eql(u8, item.string, expected)) return error.LabelVocabularyMismatch;
    }

    var access = try openTensorAccessForFile(allocator, checkpoint_path.?);
    defer access.deinit();
    var classifier_weight_tensor = try loadTensorAsF32(allocator, access, "classifier.weight");
    defer classifier_weight_tensor.deinit();
    var classifier_bias_tensor = try loadTensorAsF32(allocator, access, "classifier.bias");
    defer classifier_bias_tensor.deinit();
    if (classifier_weight_tensor.shape.len != 2 or classifier_bias_tensor.shape.len != 1) return error.InvalidAdapterTensorShape;
    if (classifier_weight_tensor.shape[0] != expected_label_vocab.len or classifier_weight_tensor.shape[1] != expected_hidden_size) {
        return error.HiddenSizeMismatch;
    }
    if (classifier_bias_tensor.shape[0] != expected_label_vocab.len) return error.HiddenSizeMismatch;

    const classifier_weight = try allocator.alloc(f32, expected_hidden_size * expected_label_vocab.len);
    errdefer allocator.free(classifier_weight);
    transpose2DF32(classifier_weight, classifier_weight_tensor.asFloat32(), expected_label_vocab.len, expected_hidden_size);
    const classifier_bias = try allocator.dupe(f32, classifier_bias_tensor.asFloat32());
    errdefer allocator.free(classifier_bias);

    return .{
        .hidden_size = expected_hidden_size,
        .num_labels = expected_label_vocab.len,
        .label_vocab = try dupeStringSlice(allocator, expected_label_vocab),
        .classifier_weight = classifier_weight,
        .classifier_bias = classifier_bias,
    };
}

fn writeTaskHeadConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    task_type: []const u8,
    hidden_size: usize,
    label_vocab: []const []const u8,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .task_type = task_type,
        .hidden_size = hidden_size,
        .num_labels = label_vocab.len,
        .labels = label_vocab,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn computeTokenFeatures(out: *[6]f32, tok: document_data.TokenBox, token_idx: usize, token_count: usize) void {
    const x0 = @as(f32, @floatFromInt(tok.bbox[0])) / 1000.0;
    const y0 = @as(f32, @floatFromInt(tok.bbox[1])) / 1000.0;
    const x1 = @as(f32, @floatFromInt(tok.bbox[2])) / 1000.0;
    const y1 = @as(f32, @floatFromInt(tok.bbox[3])) / 1000.0;
    out[0] = 1.0;
    out[1] = @as(f32, @floatFromInt(tok.text.len)) / 64.0;
    out[2] = x1 - x0;
    out[3] = y1 - y0;
    out[4] = @as(f32, @floatFromInt(token_idx)) / @as(f32, @floatFromInt(@max(token_count, 1)));
    out[5] = @sin((x0 + y0 + x1 + y1) * 3.14159);
}

/// NEFTune hook parameters. alpha == 0 is a no-op, matching eval-path defaults.
/// Pass a nonzero alpha only from training code paths.
const NeftuneParams = struct {
    alpha: f32 = 0.0,
    step: u64 = 0,
};

fn encodeFeaturesThroughBundle(
    allocator: std.mem.Allocator,
    bundle: *const LoadedLoRABundle,
    features: []const f32,
    hidden_out: []f32,
    layer_name: ?[]const u8,
    neftune_params: NeftuneParams,
) !usize {
    buildLayerInputVector(hidden_out, features);
    // NEFTune: inject uniform noise into the text-token hidden vector, which is
    // the closest analogue to the post-text-embedding buffer in this feature-based
    // LayoutLMv3 LoRA path. Applied only when caller passes alpha > 0 (training).
    // Noise is scoped to the text channel only (no separate layout/visual embeddings
    // exist in this feature path), matching the paper's text-only SFT recipe.
    if (neftune_params.alpha > 0.0) {
        neftune.applyInPlace(
            hidden_out,
            null,
            1,
            hidden_out.len,
            neftune_params.alpha,
            neftune_params.step,
        );
    }
    var residual = try allocator.alloc(f32, hidden_out.len);
    defer allocator.free(residual);
    @memset(residual, 0.0);
    var used: usize = 0;
    for (bundle.layers) |layer| {
        if (!layerMatchesScope(layer.base_tensor_name, layer_name)) continue;
        if (layer.input_dim != hidden_out.len or layer.output_dim != hidden_out.len) continue;
        const effective = try allocator.alloc(f32, layer.base_weight.len);
        defer allocator.free(effective);
        lora.mergeInto(
            .{ .rows = layer.input_dim, .cols = layer.output_dim, .data = layer.base_weight },
            .{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a },
            .{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b },
            bundle.lora_alpha,
            effective,
        );
        for (0..layer.input_dim) |row| {
            const row_slice = effective[row * layer.output_dim .. (row + 1) * layer.output_dim];
            const x = hidden_out[row];
            for (row_slice, 0..) |w, col| residual[col] += w * x;
        }
        used += 1;
    }
    if (used > 0) {
        const scale = 1.0 / @as(f32, @floatFromInt(used));
        for (hidden_out, residual) |*dst, res| dst.* += res * scale;
    }
    return used;
}

fn forwardSequenceHead(
    head: *const SequenceTaskHead,
    hidden: []const f32,
    dense_out: []f32,
    logits: []f32,
) void {
    @memset(dense_out, 0.0);
    for (0..head.hidden_size) |row| {
        const row_slice = head.dense_weight[row * head.hidden_size .. (row + 1) * head.hidden_size];
        const x = hidden[row];
        for (row_slice, 0..) |w, col| dense_out[col] += w * x;
    }
    for (dense_out, head.dense_bias) |*value, bias| value.* = std.math.tanh(value.* + bias);

    @memset(logits, 0.0);
    for (0..head.hidden_size) |row| {
        const row_slice = head.out_proj_weight[row * head.num_labels .. (row + 1) * head.num_labels];
        const x = dense_out[row];
        for (row_slice, 0..) |w, col| logits[col] += w * x;
    }
    for (logits, head.out_proj_bias) |*value, bias| value.* += bias;
}

fn forwardTokenHead(head: *const TokenTaskHead, hidden: []const f32, logits: []f32) void {
    @memset(logits, 0.0);
    for (0..head.hidden_size) |row| {
        const row_slice = head.classifier_weight[row * head.num_labels .. (row + 1) * head.num_labels];
        const x = hidden[row];
        for (row_slice, 0..) |w, col| logits[col] += w * x;
    }
    for (logits, head.classifier_bias) |*value, bias| value.* += bias;
}

fn fillTokenHidden(
    allocator: std.mem.Allocator,
    bundle: *const LoadedLoRABundle,
    example: document_data.TokenTaskExample,
    tok: document_data.TokenBox,
    token_idx: usize,
    hidden_out: []f32,
    layer_name: ?[]const u8,
    neftune_params: NeftuneParams,
) !usize {
    if (example.teacher_token_hidden) |teacher_hidden| {
        if (token_idx < teacher_hidden.len and teacher_hidden[token_idx].len == hidden_out.len) {
            @memcpy(hidden_out, teacher_hidden[token_idx]);
            // NEFTune also applies when a teacher provides the hidden vector directly:
            // the buffer still represents the post-text-embedding features that the
            // LoRA layers will consume downstream.
            if (neftune_params.alpha > 0.0) {
                neftune.applyInPlace(
                    hidden_out,
                    null,
                    1,
                    hidden_out.len,
                    neftune_params.alpha,
                    neftune_params.step,
                );
            }
            return 0;
        }
    }
    var features: [6]f32 = undefined;
    computeTokenFeatures(&features, tok, token_idx, example.tokens.len);
    return encodeFeaturesThroughBundle(allocator, bundle, features[0..], hidden_out, layer_name, neftune_params);
}

fn evaluateSequenceExamples(
    allocator: std.mem.Allocator,
    bundle: *const LoadedLoRABundle,
    head: *const SequenceTaskHead,
    examples: []const document_data.SequenceExample,
    max_examples: usize,
    layer_name: ?[]const u8,
) !SequenceMetrics {
    var metrics = SequenceMetrics{};
    const limit = if (max_examples > 0 and max_examples < examples.len) max_examples else examples.len;
    if (limit == 0 or head.num_labels == 0) return metrics;
    var correct: usize = 0;
    var loss_sum: f64 = 0;
    for (examples[0..limit]) |*example| {
        const gold_idx = indexOfString(head.label_vocab, example.label) orelse continue;
        var features: [8]f32 = undefined;
        computeSequenceFeatures(&features, example.*);
        const hidden = try allocator.alloc(f32, head.hidden_size);
        defer allocator.free(hidden);
        _ = try encodeFeaturesThroughBundle(allocator, bundle, features[0..], hidden, layer_name, .{});
        const dense_out = try allocator.alloc(f32, head.hidden_size);
        defer allocator.free(dense_out);
        const logits = try allocator.alloc(f32, head.num_labels);
        defer allocator.free(logits);
        forwardSequenceHead(head, hidden, dense_out, logits);
        const probs = try allocator.alloc(f32, head.num_labels);
        defer allocator.free(probs);
        softmax(logits, probs);
        const best_idx = argmaxF32(probs);
        if (best_idx == gold_idx) correct += 1;
        loss_sum += -@as(f64, @log(@max(probs[gold_idx], 1e-6)));
        metrics.examples_seen += 1;
    }
    if (metrics.examples_seen > 0) {
        metrics.average_loss = loss_sum / @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.accuracy = @as(f64, @floatFromInt(correct)) / @as(f64, @floatFromInt(metrics.examples_seen));
    }
    return metrics;
}

fn evaluateTokenExamples(
    allocator: std.mem.Allocator,
    bundle: *const LoadedLoRABundle,
    head: *const TokenTaskHead,
    examples: []const document_data.TokenTaskExample,
    max_examples: usize,
    layer_name: ?[]const u8,
) !TokenMetrics {
    var metrics = TokenMetrics{};
    const limit = if (max_examples > 0 and max_examples < examples.len) max_examples else examples.len;
    if (limit == 0 or head.num_labels == 0) return metrics;
    var correct: usize = 0;
    var exact_match_correct: usize = 0;
    var loss_sum: f64 = 0;
    for (examples[0..limit]) |example| {
        metrics.examples_seen += 1;
        var example_all_correct = true;
        var example_tokens_scored: usize = 0;
        for (example.tokens, example.token_labels, 0..) |tok, label, token_idx| {
            const gold_idx = indexOfString(head.label_vocab, label) orelse continue;
            const hidden = try allocator.alloc(f32, head.hidden_size);
            defer allocator.free(hidden);
            _ = try fillTokenHidden(allocator, bundle, example, tok, token_idx, hidden, layer_name, .{});
            const logits = try allocator.alloc(f32, head.num_labels);
            defer allocator.free(logits);
            forwardTokenHead(head, hidden, logits);
            const probs = try allocator.alloc(f32, head.num_labels);
            defer allocator.free(probs);
            softmax(logits, probs);
            const best_idx = argmaxF32(probs);
            if (best_idx == gold_idx) correct += 1;
            if (best_idx != gold_idx) example_all_correct = false;
            loss_sum += -@as(f64, @log(@max(probs[gold_idx], 1e-6)));
            metrics.tokens_seen += 1;
            example_tokens_scored += 1;
        }
        if (example_tokens_scored > 0 and example_all_correct) exact_match_correct += 1;
    }
    if (metrics.tokens_seen > 0) {
        metrics.average_loss = loss_sum / @as(f64, @floatFromInt(metrics.tokens_seen));
        metrics.accuracy = @as(f64, @floatFromInt(correct)) / @as(f64, @floatFromInt(metrics.tokens_seen));
    }
    if (metrics.examples_seen > 0) {
        metrics.exact_match_accuracy = @as(f64, @floatFromInt(exact_match_correct)) / @as(f64, @floatFromInt(metrics.examples_seen));
    }
    return metrics;
}

fn trainSequenceEpoch(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    head: *SequenceTaskHead,
    examples: []const document_data.SequenceExample,
    max_examples: usize,
    learning_rate: f32,
    target_margin: f32,
    layer_name: ?[]const u8,
    llrd_decay: f32,
    num_layers: usize,
    use_schedule_free: bool,
    provided_cb: ?*const ComputeBackend,
    neftune_alpha: f32,
    step_base: *u64,
) !SequenceMetrics {
    var metrics = SequenceMetrics{};
    const limit = if (max_examples > 0 and max_examples < examples.len) max_examples else examples.len;
    _ = target_margin;
    const lora_opt: optimizers.Optimizer = if (use_schedule_free)
        .{ .schedule_free_adamw = .{} }
    else
        .{ .adamw = .{} };
    if (limit == 0 or head.num_labels < 2) return metrics;

    // Use provided backend, or fall back to internal BlasCompute.
    var blas_weight_store = blas_mod.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var blas_compute = blas_mod.BlasCompute.init(allocator, &blas_weight_store, null);
    var blas_cb = blas_compute.computeBackend();
    const graph_cb: *const ComputeBackend = provided_cb orelse &blas_cb;
    var graph_optimizer_state = optimizers.OptimizerState.init(allocator);
    defer graph_optimizer_state.deinit();

    var lora_states = try allocator.alloc(optimizers.OptimizerState, bundle.layers.len);
    for (lora_states) |*s| s.* = optimizers.OptimizerState.init(allocator);
    defer {
        for (lora_states) |*s| s.deinit();
        allocator.free(lora_states);
    }

    for (examples[0..limit]) |*example| {
        const gold_idx = indexOfString(head.label_vocab, example.label) orelse continue;
        var features: [8]f32 = undefined;
        computeSequenceFeatures(&features, example.*);
        const hidden = try allocator.alloc(f32, head.hidden_size);
        defer allocator.free(hidden);
        // NEFTune is a training-only regularizer; alpha <= 0 from eval paths.
        const nef_params: NeftuneParams = .{ .alpha = neftune_alpha, .step = step_base.* };
        step_base.* += 1;
        const used_layers = try encodeFeaturesThroughBundle(allocator, bundle, features[0..], hidden, layer_name, nef_params);
        const dense_out = try allocator.alloc(f32, head.hidden_size);
        defer allocator.free(dense_out);
        const logits = try allocator.alloc(f32, head.num_labels);
        defer allocator.free(logits);
        forwardSequenceHead(head, hidden, dense_out, logits);
        const probs = try allocator.alloc(f32, head.num_labels);
        defer allocator.free(probs);
        softmax(logits, probs);
        const best_idx = argmaxF32(probs);
        const loss = -@as(f64, @log(@max(probs[gold_idx], 1e-6)));

        const grad_logits = try allocator.alloc(f32, head.num_labels);
        defer allocator.free(grad_logits);
        for (probs, 0..) |p, idx| grad_logits[idx] = p - (if (idx == gold_idx) @as(f32, 1.0) else @as(f32, 0.0));
        const grad_out_proj_weight = try allocator.alloc(f32, head.out_proj_weight.len);
        defer allocator.free(grad_out_proj_weight);
        const grad_out_proj_bias = try allocator.alloc(f32, head.out_proj_bias.len);
        defer allocator.free(grad_out_proj_bias);
        const grad_dense_out = try allocator.alloc(f32, head.hidden_size);
        defer allocator.free(grad_dense_out);
        @memset(grad_out_proj_weight, 0.0);
        @memset(grad_out_proj_bias, 0.0);
        @memset(grad_dense_out, 0.0);
        for (0..head.hidden_size) |row| {
            for (0..head.num_labels) |col| {
                grad_out_proj_weight[row * head.num_labels + col] += dense_out[row] * grad_logits[col];
                grad_dense_out[row] += head.out_proj_weight[row * head.num_labels + col] * grad_logits[col];
            }
        }
        for (grad_out_proj_bias, grad_logits) |*dst, grad| dst.* = grad;

        const grad_dense_weight = try allocator.alloc(f32, head.dense_weight.len);
        defer allocator.free(grad_dense_weight);
        const grad_dense_bias = try allocator.alloc(f32, head.dense_bias.len);
        defer allocator.free(grad_dense_bias);
        const grad_hidden = try allocator.alloc(f32, head.hidden_size);
        defer allocator.free(grad_hidden);
        @memset(grad_dense_weight, 0.0);
        @memset(grad_dense_bias, 0.0);
        @memset(grad_hidden, 0.0);
        for (0..head.hidden_size) |idx| {
            const grad_act = grad_dense_out[idx] * (1.0 - dense_out[idx] * dense_out[idx]);
            grad_dense_bias[idx] = grad_act;
            for (0..head.hidden_size) |row| {
                grad_dense_weight[row * head.hidden_size + idx] += hidden[row] * grad_act;
                grad_hidden[row] += head.dense_weight[row * head.hidden_size + idx] * grad_act;
            }
        }

        _ = try trainSequenceHeadGraphOneStep(
            allocator,
            graph_cb,
            &graph_optimizer_state,
            head,
            hidden,
            gold_idx,
            learning_rate,
        );

        if (used_layers > 0) {
            const scale = 1.0 / @as(f32, @floatFromInt(used_layers));
            for (bundle.layers, 0..) |*layer, layer_idx| {
                if (!layerMatchesScope(layer.base_tensor_name, layer_name)) continue;
                if (layer.input_dim != head.hidden_size or layer.output_dim != head.hidden_size) continue;
                const output_grads = try allocator.alloc(f32, head.hidden_size);
                defer allocator.free(output_grads);
                for (output_grads, grad_hidden) |*dst, grad| dst.* = grad * scale;
                var lora_graph = try graph_bridge.LoRALinearGraph.init(
                    allocator,
                    1,
                    layer.input_dim,
                    layer.output_dim,
                    layer.rank,
                    bundle.lora_alpha,
                );
                defer lora_graph.deinit();
                _ = try graph_bridge.trainLoRALinearFromOutputGradOneStep(
                    allocator,
                    graph_cb,
                    &lora_graph,
                    lora_opt,
                    &lora_states[layer_idx],
                    layer.base_weight,
                    layer.adapter_a,
                    layer.adapter_b,
                    hidden,
                    output_grads,
                    layerLearningRate(layer.base_tensor_name, learning_rate, llrd_decay, num_layers),
                );
            }
        }
        metrics.examples_seen += 1;
        metrics.average_loss += loss;
        if (best_idx == gold_idx) metrics.accuracy += 1.0;
    }
    if (metrics.examples_seen > 0) {
        metrics.average_loss /= @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.accuracy /= @as(f64, @floatFromInt(metrics.examples_seen));
    }
    return metrics;
}

fn trainTokenEpoch(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    head: *TokenTaskHead,
    examples: []const document_data.TokenTaskExample,
    max_examples: usize,
    learning_rate: f32,
    target_margin: f32,
    teacher_target_blend: f32,
    layer_name: ?[]const u8,
    llrd_decay: f32,
    num_layers: usize,
    max_grad_norm: f32,
    grad_accum_steps: u32,
    use_schedule_free: bool,
    provided_cb: ?*const ComputeBackend,
    pjrt_lora_steps: if (build_options.enable_pjrt) ?[]?graph_bridge.LoRAPjrtTrainStep else void,
    neftune_alpha: f32,
    step_base: *u64,
) !TokenMetrics {
    var metrics = TokenMetrics{};
    const limit = if (max_examples > 0 and max_examples < examples.len) max_examples else examples.len;
    const max_inner_steps: usize = 6;
    if (limit == 0 or head.num_labels < 2) return metrics;

    // Use provided backend, or fall back to internal BlasCompute.
    var blas_weight_store = blas_mod.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var blas_compute = blas_mod.BlasCompute.init(allocator, &blas_weight_store, null);
    var blas_cb = blas_compute.computeBackend();
    const graph_cb: *const ComputeBackend = provided_cb orelse &blas_cb;
    var graph_optimizer_state = optimizers.OptimizerState.init(allocator);
    defer graph_optimizer_state.deinit();

    const lora_adam_a = try allocator.alloc(?LoRAAdamState, bundle.layers.len);
    defer allocator.free(lora_adam_a);
    const lora_adam_b = try allocator.alloc(?LoRAAdamState, bundle.layers.len);
    defer allocator.free(lora_adam_b);
    for (lora_adam_a, lora_adam_b) |*a, *b| {
        a.* = null;
        b.* = null;
    }
    for (bundle.layers, 0..) |layer, idx| {
        if (layer.adapter_a.len > 0) {
            lora_adam_a[idx] = try LoRAAdamState.init(allocator, layer.adapter_a.len);
        }
        if (layer.adapter_b.len > 0) {
            lora_adam_b[idx] = try LoRAAdamState.init(allocator, layer.adapter_b.len);
        }
    }
    defer {
        for (lora_adam_a) |*maybe| if (maybe.*) |*s| s.deinit();
        for (lora_adam_b) |*maybe| if (maybe.*) |*s| s.deinit();
    }

    const lora_sf_a = try allocator.alloc(?LoRAScheduleFreeState, bundle.layers.len);
    defer allocator.free(lora_sf_a);
    const lora_sf_b = try allocator.alloc(?LoRAScheduleFreeState, bundle.layers.len);
    defer allocator.free(lora_sf_b);
    for (lora_sf_a, lora_sf_b) |*sfa, *sfb| {
        sfa.* = null;
        sfb.* = null;
    }
    if (use_schedule_free) {
        for (bundle.layers, 0..) |layer, idx| {
            if (layer.adapter_a.len > 0) lora_sf_a[idx] = try LoRAScheduleFreeState.init(allocator, layer.adapter_a);
            if (layer.adapter_b.len > 0) lora_sf_b[idx] = try LoRAScheduleFreeState.init(allocator, layer.adapter_b);
        }
    }
    defer {
        for (lora_sf_a) |*maybe| if (maybe.*) |*s| s.deinit();
        for (lora_sf_b) |*maybe| if (maybe.*) |*s| s.deinit();
    }

    // Grad accum outer buffers (per-layer, sized same as adapter_a/b)
    const accum_grad_a = try allocator.alloc(?[]f32, bundle.layers.len);
    defer allocator.free(accum_grad_a);
    const accum_grad_b = try allocator.alloc(?[]f32, bundle.layers.len);
    defer allocator.free(accum_grad_b);
    for (accum_grad_a, accum_grad_b, bundle.layers) |*ga, *gb, layer| {
        ga.* = if (layer.adapter_a.len > 0) blk: {
            const buf = try allocator.alloc(f32, layer.adapter_a.len);
            @memset(buf, 0);
            break :blk buf;
        } else null;
        gb.* = if (layer.adapter_b.len > 0) blk: {
            const buf = try allocator.alloc(f32, layer.adapter_b.len);
            @memset(buf, 0);
            break :blk buf;
        } else null;
    }
    defer {
        for (accum_grad_a) |maybe| if (maybe) |buf| allocator.free(buf);
        for (accum_grad_b) |maybe| if (maybe) |buf| allocator.free(buf);
    }
    var accum_doc_count: u32 = 0;

    for (examples[0..limit], 0..) |example, example_idx| {
        const is_last_example = example_idx + 1 == limit;
        metrics.examples_seen += 1;
        var step_idx: usize = 0;
        var recorded_metrics = false;
        var final_example_all_correct = false;
        while (step_idx < max_inner_steps) : (step_idx += 1) {
            var applicable_layer_count: usize = 0;
            for (bundle.layers) |layer| {
                if (!layerMatchesScope(layer.base_tensor_name, layer_name)) continue;
                if (layer.input_dim != head.hidden_size or layer.output_dim != head.hidden_size) continue;
                applicable_layer_count += 1;
            }

            const grad_classifier_weight = try allocator.alloc(f32, head.classifier_weight.len);
            defer allocator.free(grad_classifier_weight);
            const grad_classifier_bias = try allocator.alloc(f32, head.classifier_bias.len);
            defer allocator.free(grad_classifier_bias);
            @memset(grad_classifier_weight, 0.0);
            @memset(grad_classifier_bias, 0.0);

            const layer_grad_a = try allocator.alloc(?[]f32, bundle.layers.len);
            defer allocator.free(layer_grad_a);
            const layer_grad_b = try allocator.alloc(?[]f32, bundle.layers.len);
            defer allocator.free(layer_grad_b);
            for (layer_grad_a, layer_grad_b) |*ga, *gb| {
                ga.* = null;
                gb.* = null;
            }
            defer {
                for (layer_grad_a) |maybe_grad| if (maybe_grad) |grad| allocator.free(grad);
                for (layer_grad_b) |maybe_grad| if (maybe_grad) |grad| allocator.free(grad);
            }
            for (bundle.layers, 0..) |layer, idx| {
                if (!layerMatchesScope(layer.base_tensor_name, layer_name)) continue;
                if (layer.input_dim != head.hidden_size or layer.output_dim != head.hidden_size) continue;
                layer_grad_a[idx] = allocator.alloc(f32, layer.adapter_a.len) catch null;
                layer_grad_b[idx] = allocator.alloc(f32, layer.adapter_b.len) catch null;
                if (layer_grad_a[idx]) |grad| @memset(grad, 0.0);
                if (layer_grad_b[idx]) |grad| @memset(grad, 0.0);
            }

            var example_all_correct = true;
            var example_tokens_scored: usize = 0;
            var example_tokens_correct: usize = 0;
            const feature_rows = try allocator.alloc(f32, example.tokens.len * head.hidden_size);
            defer allocator.free(feature_rows);
            const feature_labels = try allocator.alloc(usize, example.tokens.len);
            defer allocator.free(feature_labels);
            for (example.tokens, example.token_labels, 0..) |tok, label, token_idx| {
                const gold_idx = indexOfString(head.label_vocab, label) orelse continue;
                const hidden = try allocator.alloc(f32, head.hidden_size);
                defer allocator.free(hidden);
                // NEFTune step is bumped per token so noise is unique per embedding.
                const nef_params: NeftuneParams = .{ .alpha = neftune_alpha, .step = step_base.* };
                step_base.* += 1;
                const used_layers = try fillTokenHidden(allocator, bundle, example, tok, token_idx, hidden, layer_name, nef_params);
                const logits = try allocator.alloc(f32, head.num_labels);
                defer allocator.free(logits);
                forwardTokenHead(head, hidden, logits);
                const probs = try allocator.alloc(f32, head.num_labels);
                defer allocator.free(probs);
                softmax(logits, probs);
                const best_idx = argmaxF32(probs);
                const loss = -@as(f64, @log(@max(probs[gold_idx], 1e-6)));
                if (best_idx != gold_idx) example_all_correct = false;

                const grad_logits = try allocator.alloc(f32, head.num_labels);
                defer allocator.free(grad_logits);
                var teacher_used = false;
                if (example.teacher_token_probs) |teacher_probs| {
                    if (token_idx < teacher_probs.len and teacher_probs[token_idx].len == head.num_labels and teacher_target_blend > 0) {
                        const blend = std.math.clamp(teacher_target_blend, 0.0, 1.0);
                        for (probs, 0..) |p, idx| {
                            const hard_target: f32 = if (idx == gold_idx) 1.0 else 0.0;
                            const teacher_target = teacher_probs[token_idx][idx];
                            const blended_target = (1.0 - blend) * hard_target + blend * teacher_target;
                            grad_logits[idx] = p - blended_target;
                        }
                        teacher_used = true;
                    }
                }
                if (!teacher_used) {
                    for (probs, 0..) |p, idx| grad_logits[idx] = p - (if (idx == gold_idx) @as(f32, 1.0) else @as(f32, 0.0));
                }
                const token_weight = if (example.runtime_token_weights) |weights| weights[token_idx] else 1.0;
                if (token_weight != 1.0) {
                    for (grad_logits) |*grad| grad.* *= token_weight;
                }

                const grad_hidden = try allocator.alloc(f32, head.hidden_size);
                defer allocator.free(grad_hidden);
                @memset(grad_hidden, 0.0);
                for (0..head.hidden_size) |row| {
                    for (0..head.num_labels) |col| {
                        grad_classifier_weight[row * head.num_labels + col] += hidden[row] * grad_logits[col];
                        grad_hidden[row] += head.classifier_weight[row * head.num_labels + col] * grad_logits[col];
                    }
                }
                for (grad_classifier_bias, grad_logits) |*dst, grad| dst.* += grad;

                if (used_layers > 0) {
                    const scale = 1.0 / @as(f32, @floatFromInt(used_layers));
                    for (bundle.layers, 0..) |*layer, layer_idx| {
                        if (!layerMatchesScope(layer.base_tensor_name, layer_name)) continue;
                        if (layer.input_dim != head.hidden_size or layer.output_dim != head.hidden_size) continue;
                        const grad_a = layer_grad_a[layer_idx] orelse continue;
                        const grad_b = layer_grad_b[layer_idx] orelse continue;
                        const output_grads = try allocator.alloc(f32, head.hidden_size);
                        defer allocator.free(output_grads);
                        for (output_grads, grad_hidden) |*dst, grad| dst.* = grad * scale;
                        // Try PJRT path first; fall back to CPU on error or when disabled.
                        var used_pjrt_lora = false;
                        if (comptime build_options.enable_pjrt) {
                            if (pjrt_lora_steps) |pjrt_steps| {
                                if (pjrt_steps[layer_idx]) |*pjrt_step| {
                                    if (graph_bridge.computeLoRALinearGradsWithPjrt(
                                        allocator,
                                        pjrt_step,
                                        layer.base_weight,
                                        layer.adapter_a,
                                        layer.adapter_b,
                                        hidden,
                                        output_grads,
                                    )) |grads| {
                                        defer allocator.free(grads.grad_a);
                                        defer allocator.free(grads.grad_b);
                                        for (grad_a, grads.grad_a) |*acc, g| acc.* += g;
                                        for (grad_b, grads.grad_b) |*acc, g| acc.* += g;
                                        used_pjrt_lora = true;
                                    } else |_| {}
                                }
                            }
                        }
                        if (!used_pjrt_lora) {
                            lora.accumulateLinearLoRAGrads(
                                grad_a,
                                grad_b,
                                1,
                                layer.input_dim,
                                hidden,
                                layer.output_dim,
                                output_grads,
                                .{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a },
                                .{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b },
                                bundle.lora_alpha,
                            );
                        }
                    }
                }
                if (!recorded_metrics) {
                    metrics.tokens_seen += 1;
                    metrics.average_loss += loss;
                    if (best_idx == gold_idx) metrics.accuracy += 1.0;
                }
                if (best_idx == gold_idx) example_tokens_correct += 1;
                @memcpy(feature_rows[example_tokens_scored * head.hidden_size .. (example_tokens_scored + 1) * head.hidden_size], hidden);
                feature_labels[example_tokens_scored] = gold_idx;
                example_tokens_scored += 1;
            }
            if (!recorded_metrics) recorded_metrics = true;
            if (example_tokens_scored == 0) break;
            if (example_all_correct) {
                final_example_all_correct = true;
                break;
            }

            const token_count_scale = 1.0 / @as(f32, @floatFromInt(example_tokens_scored));
            const doc_accuracy = @as(f32, @floatFromInt(example_tokens_correct)) /
                @as(f32, @floatFromInt(example_tokens_scored));
            const doc_error = 1.0 - doc_accuracy;
            const repeat_boost = 1.0 + @as(f32, @floatFromInt(step_idx));
            const doc_scale = token_count_scale * (1.0 + target_margin * doc_error) * repeat_boost;
            for (grad_classifier_weight) |*grad| grad.* *= doc_scale;
            for (grad_classifier_bias) |*grad| grad.* *= doc_scale;
            _ = try trainTokenHeadGraphOneStep(
                allocator,
                graph_cb,
                &graph_optimizer_state,
                head,
                feature_rows[0 .. example_tokens_scored * head.hidden_size],
                feature_labels[0..example_tokens_scored],
                learning_rate * doc_scale,
            );
            if (applicable_layer_count > 0) {
                for (bundle.layers, 0..) |_, layer_idx| {
                    const grad_a = layer_grad_a[layer_idx] orelse continue;
                    const grad_b = layer_grad_b[layer_idx] orelse continue;
                    for (grad_a) |*grad| grad.* *= doc_scale;
                    for (grad_b) |*grad| grad.* *= doc_scale;
                }
                // Joint gradient norm clipping across all LoRA grads
                if (max_grad_norm > 0) {
                    var joint_sq: f64 = 0;
                    for (bundle.layers, 0..) |_, idx| {
                        if (layer_grad_a[idx]) |ga| for (ga) |g| {
                            joint_sq += @as(f64, g) * @as(f64, g);
                        };
                        if (layer_grad_b[idx]) |gb| for (gb) |g| {
                            joint_sq += @as(f64, g) * @as(f64, g);
                        };
                    }
                    const joint_norm = @sqrt(joint_sq);
                    if (joint_norm > @as(f64, max_grad_norm)) {
                        const scale: f32 = @floatCast(@as(f64, max_grad_norm) / joint_norm);
                        for (bundle.layers, 0..) |_, idx| {
                            if (layer_grad_a[idx]) |ga| for (ga) |*g| {
                                g.* *= scale;
                            };
                            if (layer_grad_b[idx]) |gb| for (gb) |*g| {
                                g.* *= scale;
                            };
                        }
                    }
                }
                // Accumulate into outer grad buffers for grad accumulation across documents
                for (bundle.layers, 0..) |_, idx| {
                    if (layer_grad_a[idx]) |ga| {
                        if (accum_grad_a[idx]) |acc| for (acc, ga) |*dst, src| {
                            dst.* += src;
                        };
                    }
                    if (layer_grad_b[idx]) |gb| {
                        if (accum_grad_b[idx]) |acc| for (acc, gb) |*dst, src| {
                            dst.* += src;
                        };
                    }
                }
            }
        }
        if (final_example_all_correct) metrics.exact_match_accuracy += 1.0;
        accum_doc_count += 1;
        if (accum_doc_count % grad_accum_steps == 0 or is_last_example) {
            const inv_accum: f32 = 1.0 / @as(f32, @floatFromInt(grad_accum_steps));
            for (bundle.layers, 0..) |*layer, idx| {
                if (accum_grad_a[idx]) |acc| for (acc) |*g| {
                    g.* *= inv_accum;
                };
                if (accum_grad_b[idx]) |acc| for (acc) |*g| {
                    g.* *= inv_accum;
                };
                const layer_lr = layerLearningRate(layer.base_tensor_name, learning_rate, llrd_decay, num_layers);
                if (use_schedule_free) {
                    if (accum_grad_a[idx]) |acc| {
                        if (lora_sf_a[idx]) |*sfa| applyScheduleFreeStep(layer.adapter_a, acc, sfa, layer_lr);
                    }
                    if (accum_grad_b[idx]) |acc| {
                        if (lora_sf_b[idx]) |*sfb| applyScheduleFreeStep(layer.adapter_b, acc, sfb, layer_lr);
                    }
                } else {
                    if (accum_grad_a[idx]) |acc| {
                        if (lora_adam_a[idx]) |*state_a| {
                            applyAdamWStep(layer.adapter_a, acc, state_a, layer_lr);
                        }
                    }
                    if (accum_grad_b[idx]) |acc| {
                        if (lora_adam_b[idx]) |*state_b| {
                            applyAdamWStep(layer.adapter_b, acc, state_b, layer_lr);
                        }
                    }
                }
            }
            // Reset accum buffers
            for (accum_grad_a) |maybe| if (maybe) |buf| @memset(buf, 0);
            for (accum_grad_b) |maybe| if (maybe) |buf| @memset(buf, 0);
        }
    }
    if (metrics.tokens_seen > 0) {
        metrics.average_loss /= @as(f64, @floatFromInt(metrics.tokens_seen));
        metrics.accuracy /= @as(f64, @floatFromInt(metrics.tokens_seen));
    }
    if (metrics.examples_seen > 0) {
        metrics.exact_match_accuracy /= @as(f64, @floatFromInt(metrics.examples_seen));
    }
    return metrics;
}

fn argmaxF32(values: []const f32) usize {
    var best_idx: usize = 0;
    var best_value = values[0];
    for (values[1..], 1..) |value, idx| {
        if (value > best_value) {
            best_value = value;
            best_idx = idx;
        }
    }
    return best_idx;
}

fn softmax(logits: []const f32, out: []f32) void {
    std.debug.assert(logits.len == out.len);
    var max_logit = logits[0];
    for (logits[1..]) |value| {
        if (value > max_logit) max_logit = value;
    }
    var sum: f32 = 0;
    for (logits, 0..) |value, idx| {
        const e = @exp(value - max_logit);
        out[idx] = e;
        sum += e;
    }
    if (sum == 0) {
        const uniform = 1.0 / @as(f32, @floatFromInt(@max(out.len, 1)));
        @memset(out, uniform);
        return;
    }
    for (out) |*value| value.* /= sum;
}

fn indexOfString(items: []const []const u8, value: []const u8) ?usize {
    for (items, 0..) |item, idx| {
        if (std.mem.eql(u8, item, value)) return idx;
    }
    return null;
}

fn writeSyntheticLayoutLMv3BaseModel(
    allocator: std.mem.Allocator,
    dir_path: []const u8,
    hidden_size: usize,
) !void {
    try compat.cwd().createDirPath(compat.io(), dir_path);
    const config_path = try std.fs.path.join(allocator, &.{ dir_path, config_file_name });
    defer allocator.free(config_path);
    const config = try std.json.Stringify.valueAlloc(allocator, .{
        .model_type = "layoutlmv3",
        .architectures = &.{"LayoutLMv3Model"},
        .hidden_size = hidden_size,
        .intermediate_size = hidden_size,
        .num_attention_heads = 1,
        .num_hidden_layers = 12,
    }, .{ .whitespace = .indent_2 });
    defer allocator.free(config);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = config });

    const query = try allocator.alloc(f32, hidden_size * hidden_size);
    defer allocator.free(query);
    const key = try allocator.alloc(f32, hidden_size * hidden_size);
    defer allocator.free(key);
    fillDeterministicMatrix(query, hidden_size, hidden_size, 0.013, 0.0);
    fillDeterministicMatrix(key, hidden_size, hidden_size, 0.017, 0.1);

    const model_path = try std.fs.path.join(allocator, &.{ dir_path, checkpoint_file_name });
    defer allocator.free(model_path);
    try writeHeaderAndTensorsF32(allocator, model_path, &.{
        .{
            .name = "encoder.layer.11.attention.self.query.weight",
            .shape = &.{ hidden_size, hidden_size },
            .data = query,
        },
        .{
            .name = "encoder.layer.11.attention.self.key.weight",
            .shape = &.{ hidden_size, hidden_size },
            .data = key,
        },
    });
}

fn bootstrapSyntheticLoRABundle(
    allocator: std.mem.Allocator,
    base_dir: []const u8,
    adapter_dir: []const u8,
    hidden_size: usize,
) !void {
    try writeSyntheticLayoutLMv3BaseModel(allocator, base_dir, hidden_size);
    var bootstrap = try bootstrapLoRABundle(allocator, base_dir, adapter_dir, .{
        .rank = 2,
        .alpha = 4.0,
        .target_modules = &.{"query"},
    });
    defer freeBootstrapSummary(allocator, &bootstrap);
}

fn testScratchDir(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const root = try std.fmt.allocPrint(allocator, "termite-finetune-tests-{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    const dir_path = try std.fs.path.join(allocator, &.{ "/tmp", root, name });
    errdefer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    return dir_path;
}

test "module name detection matches layoutlmv3 lora targets" {
    try std.testing.expectEqualStrings("query", moduleNameForTensor("encoder.layer.0.attention.self.query.weight").?);
    try std.testing.expectEqualStrings("intermediate.dense", moduleNameForTensor("encoder.layer.0.intermediate.dense.weight").?);
    try std.testing.expect(moduleNameForTensor("embeddings.word_embeddings.weight") == null);
}

test "parse adapter tensor names" {
    const parsed = parseLoRAAdapterTensorName("encoder.layer.0.attention.self.query.lora_A.weight").?;
    try std.testing.expectEqualStrings("encoder.layer.0.attention.self.query", parsed.base_tensor_base_name);
    try std.testing.expectEqualStrings("query", parsed.module_name);
    try std.testing.expectEqual(.a, parsed.kind);
}

test "layoutlmv3 scope presets match expected layers" {
    try std.testing.expect(layerMatchesScope(
        "encoder.layer.11.attention.self.query",
        "@layoutlmv3_token_top1",
    ));
    try std.testing.expect(!layerMatchesScope(
        "encoder.layer.8.attention.self.query",
        "@layoutlmv3_token_top3",
    ));
    try std.testing.expect(layerMatchesScope(
        "encoder.layer.10.intermediate.dense",
        "@layoutlmv3_sequence_top3",
    ));
}

test "bootstrap save and materialize layoutlmv3 lora bundle" {
    const allocator = std.testing.allocator;
    const root_dir = try testScratchDir(allocator, "layoutlmv3-materialize");
    defer allocator.free(root_dir);
    defer compat.cwd().deleteTree(compat.io(), root_dir) catch {};

    const base_dir = try std.fs.path.join(allocator, &.{ root_dir, "base" });
    defer allocator.free(base_dir);
    const adapter_dir = try std.fs.path.join(allocator, &.{ root_dir, "adapter" });
    defer allocator.free(adapter_dir);
    const out_dir = try std.fs.path.join(allocator, &.{ root_dir, "out" });
    defer allocator.free(out_dir);

    try compat.cwd().createDirPath(compat.io(), base_dir);
    try compat.cwd().createDirPath(compat.io(), adapter_dir);

    const base_config =
        \\{
        \\  "model_type": "layoutlmv3",
        \\  "architectures": ["LayoutLMv3Model"],
        \\  "hidden_size": 2,
        \\  "intermediate_size": 2,
        \\  "num_attention_heads": 1,
        \\  "num_hidden_layers": 1
        \\}
    ;
    const base_config_path = try std.fs.path.join(allocator, &.{ base_dir, "config.json" });
    defer allocator.free(base_config_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = base_config_path, .data = base_config });

    const base_tensors = [_]WriteTensorF32{
        .{
            .name = "encoder.layer.0.attention.self.query.weight",
            .shape = &.{ 2, 2 },
            .data = &.{ 1.0, 2.0, 3.0, 4.0 },
        },
        .{
            .name = "encoder.layer.0.attention.self.key.weight",
            .shape = &.{ 2, 2 },
            .data = &.{ 5.0, 6.0, 7.0, 8.0 },
        },
    };
    const base_model_path = try std.fs.path.join(allocator, &.{ base_dir, checkpoint_file_name });
    defer allocator.free(base_model_path);
    try writeHeaderAndTensorsF32(allocator, base_model_path, &base_tensors);

    var bootstrap = try bootstrapLoRABundle(allocator, base_dir, adapter_dir, .{
        .rank = 1,
        .alpha = 1.0,
        .target_modules = &.{"query"},
    });
    defer freeBootstrapSummary(allocator, &bootstrap);

    var bundle = try loadLoRABundle(allocator, base_dir, adapter_dir);
    defer bundle.deinit();
    try std.testing.expectEqual(@as(usize, 1), bundle.layers.len);
    bundle.layers[0].adapter_a[0] = 0.5;
    bundle.layers[0].adapter_a[1] = 0.25;
    bundle.layers[0].adapter_b[0] = 2.0;
    bundle.layers[0].adapter_b[1] = -1.0;
    const dora_magnitude = try allocator.alloc(f32, bundle.layers[0].output_dim);
    lora.doraColumnNorms(.{
        .base = .{ .rows = bundle.layers[0].input_dim, .cols = bundle.layers[0].output_dim, .data = bundle.layers[0].base_weight },
        .adapter_a = .{ .rows = bundle.layers[0].input_dim, .cols = bundle.layers[0].rank, .data = bundle.layers[0].adapter_a },
        .adapter_b = .{ .rows = bundle.layers[0].rank, .cols = bundle.layers[0].output_dim, .data = bundle.layers[0].adapter_b },
        .magnitude = dora_magnitude,
        .alpha = bundle.lora_alpha,
    }, dora_magnitude);
    bundle.layers[0].dora_magnitude = dora_magnitude;
    try saveLoRABundle(&bundle, adapter_dir);

    var lora_inspection = try inspectLoRABundle(allocator, base_dir, adapter_dir);
    defer freeLoRABundleInspectionSummary(allocator, &lora_inspection);
    try std.testing.expectEqual(@as(usize, 1), lora_inspection.dora_magnitude_tensor_count);
    try std.testing.expectEqual(@as(usize, 2), lora_inspection.dora_magnitude_parameter_count);
    try std.testing.expectEqual(@as(usize, 6), lora_inspection.trainable_parameter_count);
    try std.testing.expect(lora_inspection.use_dora.?);

    const head_model_path = try std.fs.path.join(allocator, &.{ adapter_dir, sequence_head_checkpoint_file_name });
    defer allocator.free(head_model_path);
    const head_tensors = [_]WriteTensorF32{
        .{
            .name = "classifier.dense.weight",
            .shape = &.{ 2, 2 },
            .data = &.{ 0.1, 0.2, 0.3, 0.4 },
        },
        .{
            .name = "classifier.dense.bias",
            .shape = &.{2},
            .data = &.{ 0.5, 0.6 },
        },
        .{
            .name = "classifier.out_proj.weight",
            .shape = &.{ 2, 2 },
            .data = &.{ 0.7, 0.8, 0.9, 1.0 },
        },
        .{
            .name = "classifier.out_proj.bias",
            .shape = &.{2},
            .data = &.{ 1.1, 1.2 },
        },
    };
    try writeHeaderAndTensorsF32(allocator, head_model_path, &head_tensors);
    const head_cfg =
        \\{
        \\  "task_type": "sequence_classification",
        \\  "hidden_size": 2,
        \\  "num_labels": 2,
        \\  "labels": ["NEG", "POS"]
        \\}
    ;
    const head_cfg_path = try std.fs.path.join(allocator, &.{ adapter_dir, "sequence_head_config.json" });
    defer allocator.free(head_cfg_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = head_cfg_path, .data = head_cfg });

    var summary = try materializeMergedModel(allocator, base_dir, adapter_dir, "sequence", out_dir);
    defer freeMaterializeSummary(allocator, &summary);
    try std.testing.expectEqual(@as(usize, 1), summary.merged_lora_tensor_count);
    try std.testing.expectEqual(@as(usize, 1), summary.merged_dora_tensor_count);
    try std.testing.expectEqual(@as(usize, 4), summary.attached_head_tensor_count);

    var out_access = try openTensorAccessForModelDir(allocator, out_dir);
    defer out_access.deinit();
    var merged_query = try loadTensorAsF32(allocator, out_access, "encoder.layer.0.attention.self.query.weight");
    defer merged_query.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), merged_query.asFloat32()[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), merged_query.asFloat32()[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), merged_query.asFloat32()[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 3.75), merged_query.asFloat32()[3], 1e-5);

    var untouched_key = try loadTensorAsF32(allocator, out_access, "encoder.layer.0.attention.self.key.weight");
    defer untouched_key.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 5.0, 6.0, 7.0, 8.0 }, untouched_key.asFloat32());

    var head_bias = try loadTensorAsF32(allocator, out_access, "classifier.out_proj.bias");
    defer head_bias.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 1.1, 1.2 }, head_bias.asFloat32());

    const out_config_path = try std.fs.path.join(allocator, &.{ out_dir, config_file_name });
    defer allocator.free(out_config_path);
    const out_config_bytes = try c_file.readFile(allocator, out_config_path);
    defer allocator.free(out_config_bytes);
    try std.testing.expect(std.mem.indexOf(u8, out_config_bytes, "\"LayoutLMv3ForSequenceClassification\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out_config_bytes, "\"NEG\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out_config_bytes, "\"POS\"") != null);
}

test "train eval layoutlmv3 sequence lora bundle saves sequence head" {
    const allocator = std.testing.allocator;
    const root_dir = try testScratchDir(allocator, "layoutlmv3-sequence-train");
    defer allocator.free(root_dir);
    defer compat.cwd().deleteTree(compat.io(), root_dir) catch {};

    const base_dir = try std.fs.path.join(allocator, &.{ root_dir, "base_seq" });
    defer allocator.free(base_dir);
    const adapter_dir = try std.fs.path.join(allocator, &.{ root_dir, "adapter_seq" });
    defer allocator.free(adapter_dir);
    const out_dir = try std.fs.path.join(allocator, &.{ root_dir, "out_seq" });
    defer allocator.free(out_dir);

    try compat.cwd().createDirPath(compat.io(), base_dir);
    try compat.cwd().createDirPath(compat.io(), adapter_dir);
    try bootstrapSyntheticLoRABundle(allocator, base_dir, adapter_dir, 8);

    var bundle = try loadLoRABundle(allocator, base_dir, adapter_dir);
    defer bundle.deinit();

    const train_examples = [_]document_data.SequenceExample{
        .{
            .image_path = "doc1.png",
            .resolved_image_path = "doc1.png",
            .label = "NEG",
            .num_tokens = 12,
            .image_size_bytes = 100,
            .image_width = 640,
            .image_height = 480,
            .image_components = 3,
            .mean_darkness = 0.2,
            .std_darkness = 0.1,
            .top_darkness = 0.1,
            .bottom_darkness = 0.3,
            .left_darkness = 0.15,
            .right_darkness = 0.25,
            .center_darkness = 0.2,
        },
        .{
            .image_path = "doc2.png",
            .resolved_image_path = "doc2.png",
            .label = "POS",
            .num_tokens = 40,
            .image_size_bytes = 120,
            .image_width = 900,
            .image_height = 700,
            .image_components = 3,
            .mean_darkness = 0.6,
            .std_darkness = 0.2,
            .top_darkness = 0.7,
            .bottom_darkness = 0.4,
            .left_darkness = 0.55,
            .right_darkness = 0.45,
            .center_darkness = 0.5,
        },
    };
    const val_examples = [_]document_data.SequenceExample{ train_examples[0], train_examples[1] };
    const labels = [_][]const u8{ "NEG", "POS" };

    var summary = try trainEvalSequenceLoRABundle(
        allocator,
        &bundle,
        train_examples[0..],
        val_examples[0..],
        labels[0..],
        out_dir,
        .{ .epochs = 2, .max_train_examples = 2, .max_val_examples = 2, .learning_rate = 0.01 },
    );
    defer freeSequenceTrainEvalSummary(allocator, &summary);

    try std.testing.expectEqual(@as(usize, 2), summary.epochs);
    try std.testing.expect(summary.before_train.examples_seen > 0);
    try std.testing.expect(summary.after_val.examples_seen > 0);
    try std.testing.expect(summary.epoch_history.len == 2);

    const seq_head_path = try std.fs.path.join(allocator, &.{ out_dir, sequence_head_checkpoint_file_name });
    defer allocator.free(seq_head_path);
    const seq_head_cfg_path = try std.fs.path.join(allocator, &.{ out_dir, sequence_head_config_file_name });
    defer allocator.free(seq_head_cfg_path);
    try compat.cwd().access(compat.io(), seq_head_path, .{});
    try compat.cwd().access(compat.io(), seq_head_cfg_path, .{});
}

test "train eval layoutlmv3 token lora bundle saves and reloads token head" {
    const allocator = std.testing.allocator;
    const root_dir = try testScratchDir(allocator, "layoutlmv3-token-train");
    defer allocator.free(root_dir);
    defer compat.cwd().deleteTree(compat.io(), root_dir) catch {};

    const base_dir = try std.fs.path.join(allocator, &.{ root_dir, "base_tok" });
    defer allocator.free(base_dir);
    const adapter_dir = try std.fs.path.join(allocator, &.{ root_dir, "adapter_tok" });
    defer allocator.free(adapter_dir);
    const out_dir = try std.fs.path.join(allocator, &.{ root_dir, "out_tok" });
    defer allocator.free(out_dir);

    try compat.cwd().createDirPath(compat.io(), base_dir);
    try compat.cwd().createDirPath(compat.io(), adapter_dir);
    try bootstrapSyntheticLoRABundle(allocator, base_dir, adapter_dir, 8);

    var bundle = try loadLoRABundle(allocator, base_dir, adapter_dir);
    defer bundle.deinit();

    var tokens1 = [_]document_data.TokenBox{
        .{ .text = "alpha", .bbox = .{ 0, 0, 100, 100 } },
        .{ .text = "beta", .bbox = .{ 100, 0, 220, 100 } },
    };
    var labels1 = [_][]const u8{ "O", "NAME" };
    var token_weights1 = [_]f32{ 1.0, 1.0 };
    var tokens2 = [_]document_data.TokenBox{
        .{ .text = "gamma", .bbox = .{ 0, 200, 120, 300 } },
        .{ .text = "delta", .bbox = .{ 130, 200, 260, 300 } },
    };
    var labels2 = [_][]const u8{ "NAME", "O" };
    var token_weights2 = [_]f32{ 1.0, 1.0 };

    const train_examples = [_]document_data.TokenTaskExample{
        .{
            .image_path = "doc1.png",
            .tokens = tokens1[0..],
            .token_labels = labels1[0..],
            .runtime_token_weights = token_weights1[0..],
            .teacher_token_hidden = null,
            .teacher_token_probs = null,
        },
        .{
            .image_path = "doc2.png",
            .tokens = tokens2[0..],
            .token_labels = labels2[0..],
            .runtime_token_weights = token_weights2[0..],
            .teacher_token_hidden = null,
            .teacher_token_probs = null,
        },
    };
    const val_examples = [_]document_data.TokenTaskExample{ train_examples[0], train_examples[1] };
    const labels = [_][]const u8{ "NAME", "O" };

    var summary = try trainEvalTokenLoRABundle(
        allocator,
        &bundle,
        train_examples[0..],
        val_examples[0..],
        labels[0..],
        out_dir,
        .{ .epochs = 2, .max_train_examples = 2, .max_val_examples = 2, .learning_rate = 0.01 },
    );
    defer freeTokenTrainEvalSummary(allocator, &summary);

    try std.testing.expectEqual(@as(usize, 2), summary.epochs);
    try std.testing.expect(summary.before_train.tokens_seen > 0);
    try std.testing.expect(summary.after_val.examples_seen > 0);
    try std.testing.expect(summary.epoch_history.len == 2);

    var loaded_head = (try loadTokenTaskHeadIfPresent(allocator, out_dir, 8, labels[0..])).?;
    defer loaded_head.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 8), loaded_head.hidden_size);
    try std.testing.expectEqual(@as(usize, 2), loaded_head.num_labels);
}
