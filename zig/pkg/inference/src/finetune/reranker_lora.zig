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
const manifest_mod = @import("../models/manifest.zig");
const safetensors = @import("../models/safetensors.zig");
const tensor_access = @import("../models/tensor_access.zig");
const weight_source = @import("../models/weight_source.zig");
const deberta_model = @import("../models/deberta.zig");
const tensor_mod = @import("../backends/tensor.zig");
const Tensor = tensor_mod.Tensor;
const DType = tensor_mod.DType;
const session_factory = @import("../architectures/session_factory.zig");
const lora = @import("lora.zig");
const reranker_data = @import("reranker_data.zig");
const reranker_head = @import("reranker_head.zig");
const graph_bridge = @import("graph_bridge.zig");
const neftune = @import("neftune.zig");
const optimizers = @import("ml").graph.optimizers;

pub const artifact_family_version = "reranker_cross_encoder_lora/v1alpha1";
pub const checkpoint_file_name = "model.safetensors";
pub const config_file_name = "config.json";
pub const adapter_checkpoint_file_name = "adapter_model.safetensors";
pub const adapter_config_file_name = "adapter_config.json";
pub const tokenizer_config_file_name = "tokenizer_config.json";
pub const tokenizer_file_name = "tokenizer.json";
pub const special_tokens_map_file_name = "special_tokens_map.json";
pub const vocab_json_file_name = "vocab.json";
pub const merges_txt_file_name = "merges.txt";

pub const default_lora_target_modules = [_][]const u8{
    "query",
    "key",
    "value",
    "attention.output.dense",
};

pub const AdapterConfig = struct {
    base_model_name_or_path: ?[]const u8 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    r: ?usize = null,
    lora_alpha: ?f64 = null,
    target_modules: ?[]const []const u8 = null,
    top_layer_count: ?usize = null,
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
    top_layer_count: usize = 1,
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
    top_layer_count: usize,
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
    top_layer_count: ?usize = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    use_dora: ?bool = null,
    resolved_tensor_count: usize = 0,
    dora_magnitude_tensor_count: usize = 0,
    dora_magnitude_parameter_count: usize = 0,
    trainable_parameter_count: usize = 0,
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
    top_layer_count: usize = 1,
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
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    output_checkpoint_path: []const u8,
    merged_lora_tensor_count: usize,
    merged_dora_tensor_count: usize = 0,
    copied_base_tensor_count: usize,
};

pub const SurrogateTrainOptions = struct {
    learning_rate: f32 = 0.001,
    max_examples: usize = 128,
    epochs: usize = 1,
    layer_name: ?[]const u8 = null,
    max_grad_norm: f32 = 1.0,
    use_schedule_free: bool = false,
    /// Linear LR warmup steps. LR ramps from 0 → learning_rate over the first warmup_steps
    /// optimizer updates. 0 = no warmup. (Currently a config field; wiring TBD.)
    warmup_steps: u32 = 0,
    /// NEFTune uniform-noise scale applied to token embeddings during training.
    /// 0.0 disables (default). NEFTune (Jain et al., NeurIPS 2023) was defined for generative
    /// SFT; on a cross-encoder classifier it is extrapolated from the paper and behaves like
    /// input-space dropout on the embedding layer.
    ///
    /// TODO(neftune): all training entry points in this file operate from *precomputed*
    /// caches — either pooled features (`trainSurrogateEpochCached`) or mid-encoder
    /// block-boundary hidden states replayed through the top N layers
    /// (`trainTopLayerCached{Bert,Deberta}Exact{,All}`). The token-embedding lookup happens
    /// inside `reranker_head.precomputePooledExamples` / `cachedExamplesFromSummary`, which
    /// run once before any LoRA parameters are touched, so noising embeddings here would not
    /// reach any real forward pass through the encoder. When a non-cached training path is
    /// added (or the precompute is moved inside the training loop), call
    /// `neftune.applyInPlace(token_embed_buf, attn_mask_f32, num_tokens, hidden_size,
    /// options.neftune_alpha, step)` immediately after the token-embedding lookup and before
    /// the first transformer layer. Until then this field is accepted but has no effect.
    neftune_alpha: f32 = 0.0,
};

fn warnIfNeftuneIneffective(alpha: f32) void {
    if (alpha > 0.0) {
        std.log.warn(
            "NEFTune is configured (alpha={d:.3}) but this trainer runs from cached features; " ++
                "the noise injection has no effect. To use NEFTune, switch to a trainer with a " ++
                "real end-to-end forward pass (see reranker_real_forward.zig).",
            .{alpha},
        );
    }
}

pub const SurrogateMetrics = struct {
    examples_seen: usize = 0,
    average_loss: f64 = 0,
    mean_score: f64 = 0,
    mean_abs_error: f64 = 0,
};

pub const SurrogateEpochSummary = struct {
    epoch: usize,
    examples_seen: usize,
    average_loss: f64,
    mean_score: f64,
    mean_abs_error: f64,
};

pub const SurrogateTrainEvalSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    adapter_model_dir: []const u8,
    head_input: []const u8,
    output_dir: []const u8,
    selection_mode: []const u8,
    selected_layer_name: []const u8,
    selected_layer_count: usize,
    learning_rate: f32,
    epochs: usize,
    max_examples: usize,
    before_eval: SurrogateMetrics,
    after_eval: SurrogateMetrics,
    train: SurrogateMetrics,
    epoch_history: []SurrogateEpochSummary,
};

const LayerSelectionMode = enum {
    single,
    replayed_block,
};

const LayerSelection = struct {
    layer_idx: usize,
    selected_layer_name: []const u8,
    selected_layer_count: usize,
    mode: LayerSelectionMode,

    fn selectionModeName(self: LayerSelection) []const u8 {
        return @tagName(self.mode);
    }

    fn selectionModeNameOwned(self: LayerSelection, allocator: std.mem.Allocator) ![]const u8 {
        return try allocator.dupe(u8, self.selectionModeName());
    }
};

pub fn bootstrapLoRABundle(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    out_dir: []const u8,
    options: BootstrapOptions,
) !BootstrapSummary {
    if (options.rank == 0) return error.InvalidLoRARank;
    if (options.top_layer_count == 0) return error.InvalidTopLayerCount;

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const checkpoint_path = manifest.safetensors_path orelse return error.MissingMergedCheckpoint;

    const requested_target_modules = options.target_modules orelse default_lora_target_modules[0..];
    const resolved_tensors = try inferLoRATargetTensors(allocator, checkpoint_path, requested_target_modules, options.top_layer_count);
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
    try writeAdapterConfigJson(allocator, adapter_config_path, base_model_name_or_path, options.rank, options.alpha, requested_target_modules, options.top_layer_count, false);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, config_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, vocab_json_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, merges_txt_file_name);

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
        .top_layer_count = options.top_layer_count,
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

        const dora_name = try doraMagnitudeTensorName(allocator, parsed.base_tensor_base_name);
        defer allocator.free(dora_name);
        const dora_info = adapter_reader.header.tensors.get(dora_name);
        if (dora_info) |info| {
            if (info.shape.len != 1 or info.shape[0] != base_info.shape[0]) return error.InvalidAdapterTensorShape;
        }
        const dora_parameter_count: usize = if (dora_info != null) output_dim else 0;

        try tensors.append(allocator, .{
            .base_tensor_name = try allocator.dupe(u8, base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, adapter_a_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, adapter_b_name),
            .dora_magnitude_tensor_name = if (dora_info != null) try allocator.dupe(u8, dora_name) else null,
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
        .top_layer_count = if (adapter_config) |cfg| cfg.top_layer_count else null,
        .target_module_count = if (adapter_config) |cfg| if (cfg.target_modules) |items| items.len else 0 else 0,
        .target_modules = if (adapter_config) |cfg| try dupeOptionalStringSlice(allocator, cfg.target_modules) else null,
        .use_dora = if (adapter_config) |cfg| cfg.use_dora else null,
        .resolved_tensor_count = tensors.items.len,
        .dora_magnitude_tensor_count = dora_magnitude_tensor_count,
        .dora_magnitude_parameter_count = dora_magnitude_parameter_count,
        .trainable_parameter_count = trainable_parameter_count,
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
        .top_layer_count = inspected.top_layer_count orelse 1,
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
    var owned_data: std.ArrayListUnmanaged([]const f32) = .empty;
    defer {
        for (owned_data.items) |item| allocator.free(item);
        owned_data.deinit(allocator);
    }
    var owned_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned_names.items) |item| allocator.free(item);
        owned_names.deinit(allocator);
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
            const m_shape = try allocator.dupe(usize, &.{layer.output_dim});
            try owned_shapes.append(allocator, m_shape);
            const magnitude_name = if (layer.dora_magnitude_tensor_name) |name|
                name
            else
                try doraMagnitudeTensorName(allocator, tensorBaseName(layer.base_tensor_name));
            if (layer.dora_magnitude_tensor_name == null) try owned_names.append(allocator, magnitude_name);
            tensors[tensor_idx] = .{ .name = magnitude_name, .shape = m_shape, .data = magnitude };
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
        bundle.top_layer_count,
        bundleHasDoRA(bundle),
    );
}

pub fn materializeMergedModel(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    out_dir: []const u8,
) !MaterializeSummary {
    var bundle = try loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();

    var base_access = try openTensorAccessForModelDir(allocator, base_model_dir);
    defer base_access.deinit();
    const base_names = try base_access.listNames(allocator);
    defer allocator.free(base_names);

    var merged = std.StringArrayHashMapUnmanaged(Tensor){};
    defer {
        var it = merged.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        merged.deinit(allocator);
    }

    var merged_dora_tensor_count: usize = 0;
    for (bundle.layers) |layer| {
        const merged_weight = try allocator.alloc(f32, layer.base_weight.len);
        const matrix = lora.Matrix{ .rows = layer.input_dim, .cols = layer.output_dim, .data = layer.base_weight };
        const adapter_a = lora.Matrix{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a };
        const adapter_b = lora.Matrix{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b };
        if (layer.dora_magnitude) |magnitude| {
            lora.doraMergeInto(.{
                .base = matrix,
                .adapter_a = adapter_a,
                .adapter_b = adapter_b,
                .magnitude = magnitude,
                .alpha = bundle.lora_alpha,
            }, merged_weight);
            merged_dora_tensor_count += 1;
        } else {
            lora.mergeInto(
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
        transpose2DF32(hf_weight, merged_weight, out_cols, out_rows);
        allocator.free(merged_weight);

        const shape = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_rows)), @as(i64, @intCast(out_cols)) });
        defer allocator.free(shape);
        const tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, shape, hf_weight);
        allocator.free(hf_weight);
        try merged.put(allocator, try allocator.dupe(u8, layer.base_tensor_name), tensor);
    }

    const output_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(output_checkpoint_path);
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const bytes = try buildMergedSafetensorsFile(allocator, base_access, base_names, &merged);
    defer allocator.free(bytes);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = output_checkpoint_path, .data = bytes });

    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, vocab_json_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, merges_txt_file_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .output_checkpoint_path = output_checkpoint_path,
        .merged_lora_tensor_count = bundle.layers.len,
        .merged_dora_tensor_count = merged_dora_tensor_count,
        .copied_base_tensor_count = base_names.len - bundle.layers.len,
    };
}

pub fn trainEvalSurrogate(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    adapter_model_dir: []const u8,
    head_input: []const u8,
    train_examples: []const reranker_data.Example,
    eval_examples: []const reranker_data.Example,
    out_dir: []const u8,
    backend: reranker_head.BackendChoice,
    options: SurrogateTrainOptions,
) !SurrogateTrainEvalSummary {
    warnIfNeftuneIneffective(options.neftune_alpha);
    var bundle = try loadLoRABundle(allocator, model_dir, adapter_model_dir);
    defer bundle.deinit();
    const hidden_size = try reranker_head.resolveModelHiddenSize(allocator, model_dir);
    var head = (try reranker_head.loadHeadFromInput(allocator, head_input, hidden_size)) orelse return error.MissingRerankerHead;
    defer head.deinit();
    const selection = try resolveLayerSelection(allocator, model_dir, &bundle, null, options.layer_name);
    defer allocator.free(selection.selected_layer_name);

    const cached_train = try reranker_head.precomputePooledExamples(allocator, model_dir, train_examples, backend, options.max_examples);
    defer reranker_head.freeCachedPooledExamples(allocator, cached_train);
    const cached_eval = try reranker_head.precomputePooledExamples(allocator, model_dir, eval_examples, backend, options.max_examples);
    defer reranker_head.freeCachedPooledExamples(allocator, cached_eval);

    const before_eval = try evaluateSurrogateCached(&bundle, &head, cached_eval, selection.layer_idx);
    var history = try allocator.alloc(SurrogateEpochSummary, options.epochs);
    errdefer allocator.free(history);

    var train_metrics = SurrogateMetrics{};
    for (0..options.epochs) |epoch| {
        train_metrics = try trainSurrogateEpochCached(allocator, model_dir, &bundle, &head, cached_train, backend, options, selection.layer_idx);
        history[epoch] = .{
            .epoch = epoch + 1,
            .examples_seen = train_metrics.examples_seen,
            .average_loss = train_metrics.average_loss,
            .mean_score = train_metrics.mean_score,
            .mean_abs_error = train_metrics.mean_abs_error,
        };
        std.log.info("reranker surrogate train: epoch={d}/{d} loss={d:.4} mae={d:.4} examples={d}", .{ epoch + 1, options.epochs, train_metrics.average_loss, train_metrics.mean_abs_error, train_metrics.examples_seen });
    }

    try saveLoRABundle(&bundle, out_dir);
    std.log.info("reranker surrogate checkpoint: saved={s}", .{out_dir});
    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .head_input = try allocator.dupe(u8, head_input),
        .output_dir = try allocator.dupe(u8, out_dir),
        .selection_mode = try selection.selectionModeNameOwned(allocator),
        .selected_layer_name = try allocator.dupe(u8, selection.selected_layer_name),
        .selected_layer_count = selection.selected_layer_count,
        .learning_rate = options.learning_rate,
        .epochs = options.epochs,
        .max_examples = options.max_examples,
        .before_eval = before_eval,
        .after_eval = try evaluateSurrogateCached(&bundle, &head, cached_eval, selection.layer_idx),
        .train = train_metrics,
        .epoch_history = history,
    };
}

pub fn trainEvalSurrogateCachedSummaries(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    adapter_model_dir: []const u8,
    head_input: []const u8,
    train_summary: *const reranker_head.CachedPooledSummary,
    eval_summary: *const reranker_head.CachedPooledSummary,
    out_dir: []const u8,
    backend: reranker_head.BackendChoice,
    options: SurrogateTrainOptions,
) !SurrogateTrainEvalSummary {
    warnIfNeftuneIneffective(options.neftune_alpha);
    var bundle = try loadLoRABundle(allocator, model_dir, adapter_model_dir);
    defer bundle.deinit();
    const hidden_size = try reranker_head.resolveModelHiddenSize(allocator, model_dir);
    var head = (try reranker_head.loadHeadFromInput(allocator, head_input, hidden_size)) orelse return error.MissingRerankerHead;
    defer head.deinit();
    const selection = try resolveLayerSelection(allocator, model_dir, &bundle, null, options.layer_name);
    defer allocator.free(selection.selected_layer_name);

    const cached_train = try cachedExamplesFromSummary(allocator, train_summary);
    defer reranker_head.freeCachedPooledExamples(allocator, cached_train);
    const cached_eval = try cachedExamplesFromSummary(allocator, eval_summary);
    defer reranker_head.freeCachedPooledExamples(allocator, cached_eval);

    const before_eval = try evaluateSurrogateCached(&bundle, &head, cached_eval, selection.layer_idx);
    var history = try allocator.alloc(SurrogateEpochSummary, options.epochs);
    errdefer allocator.free(history);

    var train_metrics = SurrogateMetrics{};
    for (0..options.epochs) |epoch| {
        train_metrics = try trainSurrogateEpochCached(allocator, model_dir, &bundle, &head, cached_train, backend, options, selection.layer_idx);
        history[epoch] = .{
            .epoch = epoch + 1,
            .examples_seen = train_metrics.examples_seen,
            .average_loss = train_metrics.average_loss,
            .mean_score = train_metrics.mean_score,
            .mean_abs_error = train_metrics.mean_abs_error,
        };
        std.log.info("reranker surrogate train: epoch={d}/{d} loss={d:.4} mae={d:.4} examples={d}", .{ epoch + 1, options.epochs, train_metrics.average_loss, train_metrics.mean_abs_error, train_metrics.examples_seen });
    }

    try saveLoRABundle(&bundle, out_dir);
    std.log.info("reranker surrogate checkpoint: saved={s}", .{out_dir});
    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .head_input = try allocator.dupe(u8, head_input),
        .output_dir = try allocator.dupe(u8, out_dir),
        .selection_mode = try selection.selectionModeNameOwned(allocator),
        .selected_layer_name = try allocator.dupe(u8, selection.selected_layer_name),
        .selected_layer_count = selection.selected_layer_count,
        .learning_rate = options.learning_rate,
        .epochs = options.epochs,
        .max_examples = options.max_examples,
        .before_eval = before_eval,
        .after_eval = try evaluateSurrogateCached(&bundle, &head, cached_eval, selection.layer_idx),
        .train = train_metrics,
        .epoch_history = history,
    };
}

pub fn trainEvalTopLayerCachedSurrogate(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    adapter_model_dir: []const u8,
    head_input: []const u8,
    train_summary: *const reranker_head.CachedTopLayerSummary,
    eval_summary: *const reranker_head.CachedTopLayerSummary,
    out_dir: []const u8,
    backend: reranker_head.BackendChoice,
    options: SurrogateTrainOptions,
) !SurrogateTrainEvalSummary {
    warnIfNeftuneIneffective(options.neftune_alpha);
    var bundle = try loadLoRABundle(allocator, model_dir, adapter_model_dir);
    defer bundle.deinit();
    const hidden_size = try reranker_head.resolveModelHiddenSize(allocator, model_dir);
    var head = (try reranker_head.loadHeadFromInput(allocator, head_input, hidden_size)) orelse return error.MissingRerankerHead;
    defer head.deinit();
    if (train_summary.hidden_size != hidden_size or eval_summary.hidden_size != hidden_size) return error.ShapeMismatch;

    const selection = try resolveLayerSelection(allocator, model_dir, &bundle, train_summary, options.layer_name);
    defer allocator.free(selection.selected_layer_name);

    const before_eval = try evaluateTopLayerCachedSurrogate(allocator, model_dir, &bundle, &head, eval_summary, backend, selection);
    var history = try allocator.alloc(SurrogateEpochSummary, options.epochs);
    errdefer allocator.free(history);

    var train_metrics = SurrogateMetrics{};
    for (0..options.epochs) |epoch| {
        train_metrics = try trainTopLayerCachedSurrogateEpoch(allocator, model_dir, &bundle, &head, train_summary, backend, options, selection);
        history[epoch] = .{
            .epoch = epoch + 1,
            .examples_seen = train_metrics.examples_seen,
            .average_loss = train_metrics.average_loss,
            .mean_score = train_metrics.mean_score,
            .mean_abs_error = train_metrics.mean_abs_error,
        };
        std.log.info("reranker top_layer train: epoch={d}/{d} loss={d:.4} mae={d:.4} examples={d}", .{ epoch + 1, options.epochs, train_metrics.average_loss, train_metrics.mean_abs_error, train_metrics.examples_seen });
    }

    try saveLoRABundle(&bundle, out_dir);
    std.log.info("reranker top_layer checkpoint: saved={s}", .{out_dir});
    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .head_input = try allocator.dupe(u8, head_input),
        .output_dir = try allocator.dupe(u8, out_dir),
        .selection_mode = try selection.selectionModeNameOwned(allocator),
        .selected_layer_name = try allocator.dupe(u8, selection.selected_layer_name),
        .selected_layer_count = selection.selected_layer_count,
        .learning_rate = options.learning_rate,
        .epochs = options.epochs,
        .max_examples = options.max_examples,
        .before_eval = before_eval,
        .after_eval = try evaluateTopLayerCachedSurrogate(allocator, model_dir, &bundle, &head, eval_summary, backend, selection),
        .train = train_metrics,
        .epoch_history = history,
    };
}

pub fn freeSurrogateTrainEvalSummary(allocator: std.mem.Allocator, summary: *SurrogateTrainEvalSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.head_input);
    allocator.free(summary.output_dir);
    allocator.free(summary.selection_mode);
    allocator.free(summary.selected_layer_name);
    allocator.free(summary.epoch_history);
    summary.* = undefined;
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

pub fn freeMaterializeSummary(allocator: std.mem.Allocator, summary: *MaterializeSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.output_checkpoint_path);
    summary.* = undefined;
}

fn inferLoRATargetTensors(
    allocator: std.mem.Allocator,
    checkpoint_path: []const u8,
    requested_target_modules: []const []const u8,
    top_layer_count: usize,
) ![]LoRATargetTensor {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, checkpoint_path);
    defer reader.deinit();

    var all_targets: std.ArrayListUnmanaged(LoRATargetTensor) = .empty;
    defer {
        if (all_targets.items.len == 0) all_targets.deinit(allocator);
    }
    errdefer {
        for (all_targets.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        all_targets.deinit(allocator);
    }

    var max_layer: ?usize = null;
    var it = reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const tensor_name = entry.key_ptr.*;
        const module_name = moduleNameForTensor(tensor_name) orelse continue;
        if (!stringSliceContains(requested_target_modules, module_name)) continue;
        const layer_idx = parseEncoderLayerIndex(tensor_name) orelse continue;
        max_layer = if (max_layer) |current| @max(current, layer_idx) else layer_idx;
        const info = entry.value_ptr.*;
        if (info.shape.len != 2) continue;
        try all_targets.append(allocator, .{
            .tensor_name = try allocator.dupe(u8, tensor_name),
            .module_name = try allocator.dupe(u8, module_name),
            .output_dim = @intCast(info.shape[0]),
            .input_dim = @intCast(info.shape[1]),
        });
    }
    const max_layer_idx = max_layer orelse return error.NoLoRATargetTensorsResolved;
    const min_layer = if (top_layer_count > max_layer_idx + 1) 0 else (max_layer_idx + 1 - top_layer_count);

    var filtered: std.ArrayListUnmanaged(LoRATargetTensor) = .empty;
    errdefer {
        for (filtered.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        filtered.deinit(allocator);
    }
    for (all_targets.items) |item| {
        const layer_idx = parseEncoderLayerIndex(item.tensor_name) orelse continue;
        if (layer_idx < min_layer) {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
            continue;
        }
        try filtered.append(allocator, .{
            .tensor_name = item.tensor_name,
            .module_name = item.module_name,
            .input_dim = item.input_dim,
            .output_dim = item.output_dim,
        });
    }
    allocator.free(all_targets.items);
    std.mem.sort(LoRATargetTensor, filtered.items, {}, struct {
        fn lessThan(_: void, lhs: LoRATargetTensor, rhs: LoRATargetTensor) bool {
            return std.mem.lessThan(u8, lhs.tensor_name, rhs.tensor_name);
        }
    }.lessThan);
    return filtered.toOwnedSlice(allocator);
}

fn trainSurrogateEpochCached(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    cached: []const reranker_head.CachedPooledExample,
    backend: reranker_head.BackendChoice,
    options: SurrogateTrainOptions,
    layer_idx: usize,
) !SurrogateMetrics {
    var encoder = try reranker_head.openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    var graph = try graph_bridge.LoRALinearGraph.init(
        allocator,
        1,
        bundle.layers[layer_idx].input_dim,
        bundle.layers[layer_idx].output_dim,
        bundle.layers[layer_idx].rank,
        bundle.lora_alpha,
    );
    defer graph.deinit();
    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();

    var metrics = SurrogateMetrics{};
    for (cached) |example| {
        const layer = &bundle.layers[layer_idx];
        if (example.pooled.len != layer.input_dim or layer.input_dim != layer.output_dim or head.weight.len != layer.output_dim) {
            return error.ShapeMismatch;
        }

        const predicted = scoreAdaptedExample(layer, bundle.lora_alpha, example.pooled, head);
        const diff = predicted - @as(f64, example.score);
        const output_grad = try allocator.alloc(f32, layer.output_dim);
        defer allocator.free(output_grad);
        for (output_grad, head.weight) |*dst, weight| {
            dst.* = @floatCast(2.0 * diff * @as(f64, weight));
        }

        const summary = try graph_bridge.trainLoRALinearFromOutputGradOneStep(
            allocator,
            &encoder.compute_backend,
            &graph,
            .{ .adam = .{} },
            &optimizer_state,
            layer.base_weight,
            layer.adapter_a,
            layer.adapter_b,
            example.pooled,
            output_grad,
            options.learning_rate,
        );
        metrics.examples_seen += 1;
        metrics.average_loss += summary.loss_after;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn evaluateSurrogateCached(
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    cached: []const reranker_head.CachedPooledExample,
    layer_idx: usize,
) !SurrogateMetrics {
    var metrics = SurrogateMetrics{};
    const layer = &bundle.layers[layer_idx];
    for (cached) |example| {
        const predicted = scoreAdaptedExample(layer, bundle.lora_alpha, example.pooled, head);
        const diff = predicted - @as(f64, example.score);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn trainTopLayerCachedSurrogateEpoch(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    backend: reranker_head.BackendChoice,
    options: SurrogateTrainOptions,
    selection: LayerSelection,
) !SurrogateMetrics {
    if (try trainTopLayerCachedExactIfSupported(allocator, model_dir, bundle, head, summary, backend, options, selection)) |metrics| {
        return metrics;
    }

    var encoder = try reranker_head.openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    var graph = try graph_bridge.LoRALinearGraph.init(
        allocator,
        1,
        bundle.layers[selection.layer_idx].input_dim,
        bundle.layers[selection.layer_idx].output_dim,
        bundle.layers[selection.layer_idx].rank,
        bundle.lora_alpha,
    );
    defer graph.deinit();
    var optimizer_state = optimizers.OptimizerState.init(allocator);
    defer optimizer_state.deinit();

    var metrics = SurrogateMetrics{};
    const limit = @min(summary.examples.len, options.max_examples);
    for (summary.examples[0..limit]) |*entry| {
        const hidden = try reranker_head.replayTopLayersFromBoundary(allocator, model_dir, backend, entry, summary.top_layer_count);
        defer allocator.free(hidden);
        const pooled = hidden[0..summary.hidden_size];
        const layer = &bundle.layers[selection.layer_idx];
        if (pooled.len != layer.input_dim or layer.input_dim != layer.output_dim or head.weight.len != layer.output_dim) {
            return error.ShapeMismatch;
        }

        const predicted = scoreAdaptedExample(layer, bundle.lora_alpha, pooled, head);
        const diff = predicted - @as(f64, entry.score);
        const output_grad = try allocator.alloc(f32, layer.output_dim);
        defer allocator.free(output_grad);
        for (output_grad, head.weight) |*dst, weight| {
            dst.* = @floatCast(2.0 * diff * @as(f64, weight));
        }

        const train_summary = try graph_bridge.trainLoRALinearFromOutputGradOneStep(
            allocator,
            &encoder.compute_backend,
            &graph,
            .{ .adam = .{} },
            &optimizer_state,
            layer.base_weight,
            layer.adapter_a,
            layer.adapter_b,
            pooled,
            output_grad,
            options.learning_rate,
        );
        metrics.examples_seen += 1;
        metrics.average_loss += train_summary.loss_after;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn trainTopLayerCachedExactIfSupported(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    backend: reranker_head.BackendChoice,
    options: SurrogateTrainOptions,
    selection: LayerSelection,
) !?SurrogateMetrics {
    _ = backend;

    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    switch (encoder.arch_config) {
        .bert => {
            if (selection.mode == .replayed_block) {
                return try trainTopLayerCachedBertExactAll(allocator, model_dir, bundle, head, summary, options);
            }
            return try trainTopLayerCachedBertExact(allocator, model_dir, bundle, head, summary, options, selection.layer_idx);
        },
        .deberta => {
            if (selection.mode == .replayed_block) {
                return try trainTopLayerCachedDebertaExactAll(allocator, model_dir, bundle, head, summary, options);
            }
            return try trainTopLayerCachedDebertaExactIfSupported(allocator, model_dir, bundle, head, summary, options, selection.layer_idx);
        },
    }
}

fn evaluateTopLayerCachedSurrogate(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    backend: reranker_head.BackendChoice,
    selection: LayerSelection,
) !SurrogateMetrics {
    if (try evaluateTopLayerCachedExactIfSupported(allocator, model_dir, bundle, head, summary, backend, selection)) |metrics| {
        return metrics;
    }

    var metrics = SurrogateMetrics{};
    const layer = &bundle.layers[selection.layer_idx];
    for (summary.examples) |*entry| {
        const hidden = try reranker_head.replayTopLayersFromBoundary(allocator, model_dir, backend, entry, summary.top_layer_count);
        defer allocator.free(hidden);
        const pooled = hidden[0..summary.hidden_size];
        if (pooled.len != layer.input_dim or layer.input_dim != layer.output_dim or head.weight.len != layer.output_dim) {
            return error.ShapeMismatch;
        }
        const predicted = scoreAdaptedExample(layer, bundle.lora_alpha, pooled, head);
        const diff = predicted - @as(f64, entry.score);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

const ExactLoRAPairRef = struct {
    adapter_a: []f32,
    adapter_b: []f32,
    rank: usize,
};

const ExactLoRAAdamState = struct {
    allocator: std.mem.Allocator,
    m_a: []f32, // first moment for adapter_a
    v_a: []f32, // second moment for adapter_a
    m_b: []f32, // first moment for adapter_b
    v_b: []f32, // second moment for adapter_b
    z_a: []f32, // schedule-free base iterate for adapter_a
    z_b: []f32, // schedule-free base iterate for adapter_b
    step: u64,

    fn init(allocator: std.mem.Allocator, size_a: usize, size_b: usize) !ExactLoRAAdamState {
        const m_a = try allocator.alloc(f32, size_a);
        errdefer allocator.free(m_a);
        const v_a = try allocator.alloc(f32, size_a);
        errdefer allocator.free(v_a);
        const m_b = try allocator.alloc(f32, size_b);
        errdefer allocator.free(m_b);
        const v_b = try allocator.alloc(f32, size_b);
        errdefer allocator.free(v_b);
        const z_a = try allocator.alloc(f32, size_a);
        errdefer allocator.free(z_a);
        const z_b = try allocator.alloc(f32, size_b);
        errdefer allocator.free(z_b);
        @memset(m_a, 0);
        @memset(v_a, 0);
        @memset(m_b, 0);
        @memset(v_b, 0);
        @memset(z_a, 0);
        @memset(z_b, 0);
        return .{ .allocator = allocator, .m_a = m_a, .v_a = v_a, .m_b = m_b, .v_b = v_b, .z_a = z_a, .z_b = z_b, .step = 0 };
    }

    fn deinit(self: *ExactLoRAAdamState) void {
        self.allocator.free(self.m_a);
        self.allocator.free(self.v_a);
        self.allocator.free(self.m_b);
        self.allocator.free(self.v_b);
        self.allocator.free(self.z_a);
        self.allocator.free(self.z_b);
        self.* = undefined;
    }
};

const BertExactTopLayerRuntime = struct {
    allocator: std.mem.Allocator,
    hidden_size: usize,
    num_heads: usize,
    intermediate_size: usize,
    layer_norm_eps: f32,
    query_weight: []f32,
    query_bias: []f32,
    key_weight: []f32,
    key_bias: []f32,
    value_weight: []f32,
    value_bias: []f32,
    output_dense_weight: []f32,
    output_dense_bias: []f32,
    attn_norm_weight: []f32,
    attn_norm_bias: []f32,
    intermediate_weight: []f32,
    intermediate_bias: []f32,
    output_ff_weight: []f32,
    output_ff_bias: []f32,
    output_norm_weight: []f32,
    output_norm_bias: []f32,
    query_lora: ?ExactLoRAPairRef = null,
    key_lora: ?ExactLoRAPairRef = null,
    value_lora: ?ExactLoRAPairRef = null,
    output_dense_lora: ?ExactLoRAPairRef = null,
    query_lora_adam: ?ExactLoRAAdamState = null,
    key_lora_adam: ?ExactLoRAAdamState = null,
    value_lora_adam: ?ExactLoRAAdamState = null,
    output_dense_lora_adam: ?ExactLoRAAdamState = null,

    fn deinit(self: *BertExactTopLayerRuntime) void {
        self.allocator.free(self.query_weight);
        self.allocator.free(self.query_bias);
        self.allocator.free(self.key_weight);
        self.allocator.free(self.key_bias);
        self.allocator.free(self.value_weight);
        self.allocator.free(self.value_bias);
        self.allocator.free(self.output_dense_weight);
        self.allocator.free(self.output_dense_bias);
        self.allocator.free(self.attn_norm_weight);
        self.allocator.free(self.attn_norm_bias);
        self.allocator.free(self.intermediate_weight);
        self.allocator.free(self.intermediate_bias);
        self.allocator.free(self.output_ff_weight);
        self.allocator.free(self.output_ff_bias);
        self.allocator.free(self.output_norm_weight);
        self.allocator.free(self.output_norm_bias);
        if (self.query_lora_adam) |*s| s.deinit();
        if (self.key_lora_adam) |*s| s.deinit();
        if (self.value_lora_adam) |*s| s.deinit();
        if (self.output_dense_lora_adam) |*s| s.deinit();
        self.* = undefined;
    }
};

const BertLastLayerForwardCache = struct {
    allocator: std.mem.Allocator,
    hidden_in: []const f32,
    query: []f32,
    key: []f32,
    value: []f32,
    probs: []f32,
    attn_output: []f32,
    projected: []f32,
    attn_norm_input: []f32,
    attn_norm: []f32,
    intermediate_pre: []f32,
    intermediate_act: []f32,
    ff_out: []f32,
    output_norm_input: []f32,
    out: []f32,

    fn deinit(self: *BertLastLayerForwardCache) void {
        self.allocator.free(self.query);
        self.allocator.free(self.key);
        self.allocator.free(self.value);
        self.allocator.free(self.probs);
        self.allocator.free(self.attn_output);
        self.allocator.free(self.projected);
        self.allocator.free(self.attn_norm_input);
        self.allocator.free(self.attn_norm);
        self.allocator.free(self.intermediate_pre);
        self.allocator.free(self.intermediate_act);
        self.allocator.free(self.ff_out);
        self.allocator.free(self.output_norm_input);
        self.allocator.free(self.out);
        self.* = undefined;
    }
};

const DebertaExactTopLayerRuntime = struct {
    allocator: std.mem.Allocator,
    hidden_size: usize,
    num_heads: usize,
    intermediate_size: usize,
    layer_norm_eps: f32,
    position_buckets: u32,
    max_position_embeddings: u32,
    rel_embeddings: []f32,
    query_weight: []f32,
    query_bias: []f32,
    key_weight: []f32,
    key_bias: []f32,
    value_weight: []f32,
    value_bias: []f32,
    output_dense_weight: []f32,
    output_dense_bias: []f32,
    attn_norm_weight: []f32,
    attn_norm_bias: []f32,
    intermediate_weight: []f32,
    intermediate_bias: []f32,
    output_ff_weight: []f32,
    output_ff_bias: []f32,
    output_norm_weight: []f32,
    output_norm_bias: []f32,
    query_lora: ?ExactLoRAPairRef = null,
    key_lora: ?ExactLoRAPairRef = null,
    value_lora: ?ExactLoRAPairRef = null,
    output_dense_lora: ?ExactLoRAPairRef = null,
    query_lora_adam: ?ExactLoRAAdamState = null,
    key_lora_adam: ?ExactLoRAAdamState = null,
    value_lora_adam: ?ExactLoRAAdamState = null,
    output_dense_lora_adam: ?ExactLoRAAdamState = null,

    fn deinit(self: *DebertaExactTopLayerRuntime) void {
        self.allocator.free(self.rel_embeddings);
        self.allocator.free(self.query_weight);
        self.allocator.free(self.query_bias);
        self.allocator.free(self.key_weight);
        self.allocator.free(self.key_bias);
        self.allocator.free(self.value_weight);
        self.allocator.free(self.value_bias);
        self.allocator.free(self.output_dense_weight);
        self.allocator.free(self.output_dense_bias);
        self.allocator.free(self.attn_norm_weight);
        self.allocator.free(self.attn_norm_bias);
        self.allocator.free(self.intermediate_weight);
        self.allocator.free(self.intermediate_bias);
        self.allocator.free(self.output_ff_weight);
        self.allocator.free(self.output_ff_bias);
        self.allocator.free(self.output_norm_weight);
        self.allocator.free(self.output_norm_bias);
        if (self.query_lora_adam) |*s| s.deinit();
        if (self.key_lora_adam) |*s| s.deinit();
        if (self.value_lora_adam) |*s| s.deinit();
        if (self.output_dense_lora_adam) |*s| s.deinit();
        self.* = undefined;
    }
};

const DebertaLastLayerForwardCache = struct {
    allocator: std.mem.Allocator,
    hidden_in: []const f32,
    rel_embeddings: []f32,
    query: []f32,
    key: []f32,
    value: []f32,
    q_r: []f32,
    k_r: []f32,
    probs: []f32,
    attn_output: []f32,
    projected: []f32,
    attn_norm_input: []f32,
    attn_norm: []f32,
    intermediate_pre: []f32,
    intermediate_act: []f32,
    ff_out: []f32,
    output_norm_input: []f32,
    out: []f32,

    fn deinit(self: *DebertaLastLayerForwardCache) void {
        self.allocator.free(self.rel_embeddings);
        self.allocator.free(self.query);
        self.allocator.free(self.key);
        self.allocator.free(self.value);
        self.allocator.free(self.q_r);
        self.allocator.free(self.k_r);
        self.allocator.free(self.probs);
        self.allocator.free(self.attn_output);
        self.allocator.free(self.projected);
        self.allocator.free(self.attn_norm_input);
        self.allocator.free(self.attn_norm);
        self.allocator.free(self.intermediate_pre);
        self.allocator.free(self.intermediate_act);
        self.allocator.free(self.ff_out);
        self.allocator.free(self.output_norm_input);
        self.allocator.free(self.out);
        self.* = undefined;
    }
};

fn evaluateTopLayerCachedExactIfSupported(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    backend: reranker_head.BackendChoice,
    selection: LayerSelection,
) !?SurrogateMetrics {
    _ = backend;

    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    switch (encoder.arch_config) {
        .bert => {
            if (selection.mode == .replayed_block) {
                return try evaluateTopLayerCachedBertExactAll(allocator, model_dir, bundle, head, summary);
            }
            return try evaluateTopLayerCachedBertExact(allocator, model_dir, bundle, head, summary, selection.layer_idx);
        },
        .deberta => {
            if (selection.mode == .replayed_block) {
                return try evaluateTopLayerCachedDebertaExactAll(allocator, model_dir, bundle, head, summary);
            }
            return try evaluateTopLayerCachedDebertaExactIfSupported(allocator, model_dir, bundle, head, summary, selection.layer_idx);
        },
    }
}

fn trainTopLayerCachedBertExact(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    options: SurrogateTrainOptions,
    layer_idx: usize,
) !?SurrogateMetrics {
    const target_layer_idx = parseEncoderLayerIndex(bundle.layers[layer_idx].base_tensor_name) orelse return null;
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .bert => |value| value,
        .deberta => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    if (target_layer_idx < top_start or target_layer_idx >= cfg.num_hidden_layers) return null;
    var runtimes = try loadBertExactRuntimeRange(allocator, model_dir, bundle, target_layer_idx, cfg.num_hidden_layers);
    defer deinitBertRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    const limit = @min(summary.examples.len, options.max_examples);
    for (summary.examples[0..limit]) |*entry| {
        const layer_hidden_in = try reranker_head.replayBoundaryToLayerInputWithRuntime(allocator, &encoder, entry, summary.top_layer_count, target_layer_idx);
        defer allocator.free(layer_hidden_in);
        const caches = try forwardBertExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, layer_hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitBertCacheSlice(allocator, caches);
        const final_cache = &caches[caches.len - 1];
        const cls = final_cache.out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, cls);
        const diff = predicted - @as(f64, entry.score);
        var grad_out = try allocator.alloc(f32, final_cache.out.len);
        defer allocator.free(grad_out);
        @memset(grad_out, 0);
        for (0..summary.hidden_size) |i| grad_out[i] = @floatCast(2.0 * diff * @as(f64, head.weight[i]));
        var grad_hidden = grad_out;
        var owns_grad_hidden = true;
        defer if (owns_grad_hidden) allocator.free(grad_hidden);
        var rev_i: usize = caches.len;
        while (rev_i > 0) {
            rev_i -= 1;
            const update_enabled = rev_i == 0;
            const next_grad_hidden = try backwardBertExactTopLayerFromGrad(
                allocator,
                &runtimes[rev_i],
                bundle.lora_alpha,
                &caches[rev_i],
                grad_hidden,
                options.learning_rate,
                options.max_grad_norm,
                options.use_schedule_free,
                update_enabled,
            );
            if (owns_grad_hidden) allocator.free(grad_hidden);
            grad_hidden = next_grad_hidden;
            owns_grad_hidden = true;
        }
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn trainTopLayerCachedBertExactAll(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    options: SurrogateTrainOptions,
) !?SurrogateMetrics {
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .bert => |value| value,
        else => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    var runtimes = try loadBertExactRuntimeRange(allocator, model_dir, bundle, top_start, cfg.num_hidden_layers);
    defer deinitBertRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    const limit = @min(summary.examples.len, options.max_examples);
    for (summary.examples[0..limit]) |*entry| {
        const caches = try forwardBertExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, entry.hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitBertCacheSlice(allocator, caches);
        const final_cache = &caches[caches.len - 1];
        const cls = final_cache.out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, cls);
        const diff = predicted - @as(f64, entry.score);
        var grad_hidden = try allocator.alloc(f32, final_cache.out.len);
        @memset(grad_hidden, 0);
        for (0..summary.hidden_size) |i| grad_hidden[i] = @floatCast(2.0 * diff * @as(f64, head.weight[i]));

        var rev_i: usize = caches.len;
        while (rev_i > 0) {
            rev_i -= 1;
            const next_grad_hidden = try backwardBertExactTopLayerFromGrad(
                allocator,
                &runtimes[rev_i],
                bundle.lora_alpha,
                &caches[rev_i],
                grad_hidden,
                options.learning_rate,
                options.max_grad_norm,
                options.use_schedule_free,
                true,
            );
            allocator.free(grad_hidden);
            grad_hidden = next_grad_hidden;
        }
        allocator.free(grad_hidden);

        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn trainTopLayerCachedDebertaExactIfSupported(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    options: SurrogateTrainOptions,
    layer_idx: usize,
) !?SurrogateMetrics {
    const target_layer_idx = parseEncoderLayerIndex(bundle.layers[layer_idx].base_tensor_name) orelse return null;
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .deberta => |value| value,
        else => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    if (target_layer_idx < top_start or target_layer_idx >= cfg.num_hidden_layers) return null;
    var runtimes = try loadDebertaExactRuntimeRange(allocator, model_dir, bundle, target_layer_idx, cfg.num_hidden_layers);
    defer deinitDebertaRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    const limit = @min(summary.examples.len, options.max_examples);
    for (summary.examples[0..limit]) |*entry| {
        const layer_hidden_in = try reranker_head.replayBoundaryToLayerInputWithRuntime(allocator, &encoder, entry, summary.top_layer_count, target_layer_idx);
        defer allocator.free(layer_hidden_in);
        const caches = try forwardDebertaExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, layer_hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitDebertaCacheSlice(allocator, caches);
        const final_cache = &caches[caches.len - 1];
        const cls = final_cache.out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, cls);
        const diff = predicted - @as(f64, entry.score);
        var grad_hidden = try allocator.alloc(f32, final_cache.out.len);
        @memset(grad_hidden, 0);
        for (0..summary.hidden_size) |i| grad_hidden[i] = @floatCast(2.0 * diff * @as(f64, head.weight[i]));
        var rev_i: usize = caches.len;
        while (rev_i > 0) {
            rev_i -= 1;
            const update_enabled = rev_i == 0;
            const next_grad_hidden = try backwardDebertaExactTopLayerFromGrad(
                allocator,
                &runtimes[rev_i],
                bundle.lora_alpha,
                &caches[rev_i],
                grad_hidden,
                entry.seq_len,
                options.learning_rate,
                options.max_grad_norm,
                options.use_schedule_free,
                update_enabled,
            );
            allocator.free(grad_hidden);
            grad_hidden = next_grad_hidden;
        }
        allocator.free(grad_hidden);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn evaluateTopLayerCachedBertExact(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    layer_idx: usize,
) !?SurrogateMetrics {
    const target_layer_idx = parseEncoderLayerIndex(bundle.layers[layer_idx].base_tensor_name) orelse return null;
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .bert => |value| value,
        .deberta => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    if (target_layer_idx < top_start or target_layer_idx >= cfg.num_hidden_layers) return null;
    var runtimes = try loadBertExactRuntimeRange(allocator, model_dir, bundle, target_layer_idx, cfg.num_hidden_layers);
    defer deinitBertRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    for (summary.examples) |*entry| {
        const layer_hidden_in = try reranker_head.replayBoundaryToLayerInputWithRuntime(allocator, &encoder, entry, summary.top_layer_count, target_layer_idx);
        defer allocator.free(layer_hidden_in);
        const caches = try forwardBertExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, layer_hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitBertCacheSlice(allocator, caches);
        const pooled = caches[caches.len - 1].out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, pooled);
        const diff = predicted - @as(f64, entry.score);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn evaluateTopLayerCachedBertExactAll(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
) !?SurrogateMetrics {
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .bert => |value| value,
        else => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    var runtimes = try loadBertExactRuntimeRange(allocator, model_dir, bundle, top_start, cfg.num_hidden_layers);
    defer deinitBertRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    for (summary.examples) |*entry| {
        const caches = try forwardBertExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, entry.hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitBertCacheSlice(allocator, caches);
        const pooled = caches[caches.len - 1].out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, pooled);
        const diff = predicted - @as(f64, entry.score);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn evaluateTopLayerCachedDebertaExactIfSupported(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    layer_idx: usize,
) !?SurrogateMetrics {
    const target_layer_idx = parseEncoderLayerIndex(bundle.layers[layer_idx].base_tensor_name) orelse return null;
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .deberta => |value| value,
        else => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    if (target_layer_idx < top_start or target_layer_idx >= cfg.num_hidden_layers) return null;
    var runtimes = try loadDebertaExactRuntimeRange(allocator, model_dir, bundle, target_layer_idx, cfg.num_hidden_layers);
    defer deinitDebertaRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    for (summary.examples) |*entry| {
        const layer_hidden_in = try reranker_head.replayBoundaryToLayerInputWithRuntime(allocator, &encoder, entry, summary.top_layer_count, target_layer_idx);
        defer allocator.free(layer_hidden_in);
        const caches = try forwardDebertaExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, layer_hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitDebertaCacheSlice(allocator, caches);
        const pooled = caches[caches.len - 1].out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, pooled);
        const diff = predicted - @as(f64, entry.score);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn trainTopLayerCachedDebertaExactAll(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
    options: SurrogateTrainOptions,
) !?SurrogateMetrics {
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .deberta => |value| value,
        else => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    var runtimes = try loadDebertaExactRuntimeRange(allocator, model_dir, bundle, top_start, cfg.num_hidden_layers);
    defer deinitDebertaRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    const limit = @min(summary.examples.len, options.max_examples);
    for (summary.examples[0..limit]) |*entry| {
        const caches = try forwardDebertaExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, entry.hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitDebertaCacheSlice(allocator, caches);
        const final_cache = &caches[caches.len - 1];
        const cls = final_cache.out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, cls);
        const diff = predicted - @as(f64, entry.score);
        var grad_hidden = try allocator.alloc(f32, final_cache.out.len);
        @memset(grad_hidden, 0);
        for (0..summary.hidden_size) |i| grad_hidden[i] = @floatCast(2.0 * diff * @as(f64, head.weight[i]));
        var rev_i: usize = caches.len;
        while (rev_i > 0) {
            rev_i -= 1;
            const next_grad_hidden = try backwardDebertaExactTopLayerFromGrad(
                allocator,
                &runtimes[rev_i],
                bundle.lora_alpha,
                &caches[rev_i],
                grad_hidden,
                entry.seq_len,
                options.learning_rate,
                options.max_grad_norm,
                options.use_schedule_free,
                true,
            );
            allocator.free(grad_hidden);
            grad_hidden = next_grad_hidden;
        }
        allocator.free(grad_hidden);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn evaluateTopLayerCachedDebertaExactAll(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    head: *const reranker_head.RerankerHead,
    summary: *const reranker_head.CachedTopLayerSummary,
) !?SurrogateMetrics {
    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .deberta => |value| value,
        else => return null,
    };
    const top_start = cfg.num_hidden_layers - @min(summary.top_layer_count, cfg.num_hidden_layers);
    var runtimes = try loadDebertaExactRuntimeRange(allocator, model_dir, bundle, top_start, cfg.num_hidden_layers);
    defer deinitDebertaRuntimeSlice(&runtimes);

    var metrics = SurrogateMetrics{};
    for (summary.examples) |*entry| {
        const caches = try forwardDebertaExactRuntimeRange(allocator, runtimes, bundle.lora_alpha, entry.hidden_in, entry.attention_mask, entry.seq_len);
        defer deinitDebertaCacheSlice(allocator, caches);
        const pooled = caches[caches.len - 1].out[0..summary.hidden_size];
        const predicted = reranker_head.scoreHead(head, pooled);
        const diff = predicted - @as(f64, entry.score);
        metrics.examples_seen += 1;
        metrics.average_loss += diff * diff;
        metrics.mean_score += predicted;
        metrics.mean_abs_error += @abs(diff);
    }
    if (metrics.examples_seen > 0) {
        const denom = @as(f64, @floatFromInt(metrics.examples_seen));
        metrics.average_loss /= denom;
        metrics.mean_score /= denom;
        metrics.mean_abs_error /= denom;
    }
    return metrics;
}

fn loadDebertaExactTopLayerRuntime(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    layer_idx: usize,
) !DebertaExactTopLayerRuntime {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const hidden_size = manifest.hidden_size;
    if (hidden_size == 0) return error.MissingHiddenSize;

    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .deberta => |value| value,
        else => return error.UnsupportedArchitecture,
    };

    var access = try openTensorAccessForModelDir(allocator, model_dir);
    defer access.deinit();

    var rel_embeddings_tensor = try loadTensorAsF32(allocator, access, "encoder.rel_embeddings.weight");
    defer rel_embeddings_tensor.deinit();
    const rel_embeddings = try allocator.dupe(f32, rel_embeddings_tensor.asFloat32());
    errdefer allocator.free(rel_embeddings);
    var rel_norm_weight_tensor = try loadTensorAsF32(allocator, access, "encoder.LayerNorm.weight");
    defer rel_norm_weight_tensor.deinit();
    var rel_norm_bias_tensor = try loadTensorAsF32(allocator, access, "encoder.LayerNorm.bias");
    defer rel_norm_bias_tensor.deinit();
    applyLayerNormInPlace(
        rel_embeddings,
        @intCast(rel_embeddings_tensor.shape[0]),
        hidden_size,
        rel_norm_weight_tensor.asFloat32(),
        rel_norm_bias_tensor.asFloat32(),
        cfg.layer_norm_eps,
    );

    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .num_heads = cfg.num_attention_heads,
        .intermediate_size = cfg.intermediate_size,
        .layer_norm_eps = cfg.layer_norm_eps,
        .position_buckets = cfg.position_buckets,
        .max_position_embeddings = cfg.max_position_embeddings,
        .rel_embeddings = rel_embeddings,
        .query_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.self.query_proj.weight"),
        .query_bias = try loadLayerVector(allocator, access, layer_idx, "attention.self.query_proj.bias"),
        .key_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.self.key_proj.weight"),
        .key_bias = try loadLayerVector(allocator, access, layer_idx, "attention.self.key_proj.bias"),
        .value_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.self.value_proj.weight"),
        .value_bias = try loadLayerVector(allocator, access, layer_idx, "attention.self.value_proj.bias"),
        .output_dense_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.output.dense.weight"),
        .output_dense_bias = try loadLayerVector(allocator, access, layer_idx, "attention.output.dense.bias"),
        .attn_norm_weight = try loadLayerVector(allocator, access, layer_idx, "attention.output.LayerNorm.weight"),
        .attn_norm_bias = try loadLayerVector(allocator, access, layer_idx, "attention.output.LayerNorm.bias"),
        .intermediate_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "intermediate.dense.weight"),
        .intermediate_bias = try loadLayerVector(allocator, access, layer_idx, "intermediate.dense.bias"),
        .output_ff_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "output.dense.weight"),
        .output_ff_bias = try loadLayerVector(allocator, access, layer_idx, "output.dense.bias"),
        .output_norm_weight = try loadLayerVector(allocator, access, layer_idx, "output.LayerNorm.weight"),
        .output_norm_bias = try loadLayerVector(allocator, access, layer_idx, "output.LayerNorm.bias"),
        .query_lora = exactLoRAPairFor(bundle, layer_idx, "query"),
        .key_lora = exactLoRAPairFor(bundle, layer_idx, "key"),
        .value_lora = exactLoRAPairFor(bundle, layer_idx, "value"),
        .output_dense_lora = exactLoRAPairFor(bundle, layer_idx, "attention.output.dense"),
    };
}

fn forwardDebertaExactTopLayerFromBoundary(
    allocator: std.mem.Allocator,
    runtime: *const DebertaExactTopLayerRuntime,
    lora_alpha: f32,
    hidden_in: []const f32,
    attention_mask: []const i64,
    seq_len: usize,
) !DebertaLastLayerForwardCache {
    const hidden = runtime.hidden_size;
    const num_heads = runtime.num_heads;
    const head_dim = hidden / num_heads;

    const query = try allocator.alloc(f32, seq_len * hidden);
    defer allocator.free(query);
    const key = try allocator.alloc(f32, seq_len * hidden);
    defer allocator.free(key);
    const value = try allocator.alloc(f32, seq_len * hidden);
    defer allocator.free(value);
    computeLinearOutputWithBias(query, hidden_in, runtime.query_weight, runtime.query_bias, hidden, hidden, seq_len);
    computeLinearOutputWithBias(key, hidden_in, runtime.key_weight, runtime.key_bias, hidden, hidden, seq_len);
    computeLinearOutputWithBias(value, hidden_in, runtime.value_weight, runtime.value_bias, hidden, hidden, seq_len);
    if (runtime.query_lora) |pair| applyAdapterDelta(query, hidden_in, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);
    if (runtime.key_lora) |pair| applyAdapterDelta(key, hidden_in, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);
    if (runtime.value_lora) |pair| applyAdapterDelta(value, hidden_in, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);

    const num_rel = 2 * seq_len - 1;
    const rel_embeddings = try allocator.alloc(f32, num_rel * hidden);
    errdefer allocator.free(rel_embeddings);
    for (0..num_rel) |idx| {
        const rel_pos: i64 = @as(i64, @intCast(idx)) - @as(i64, @intCast(seq_len - 1));
        const bucket: usize = @intCast(deberta_model.relativePositionBucket(rel_pos, runtime.position_buckets, runtime.max_position_embeddings));
        const src = runtime.rel_embeddings[bucket * hidden ..][0..hidden];
        const dst = rel_embeddings[idx * hidden ..][0..hidden];
        @memcpy(dst, src);
    }

    const q_r = try allocator.alloc(f32, num_rel * hidden);
    errdefer allocator.free(q_r);
    const k_r = try allocator.alloc(f32, num_rel * hidden);
    errdefer allocator.free(k_r);
    computeLinearOutputWithBias(q_r, rel_embeddings, runtime.query_weight, runtime.query_bias, hidden, hidden, num_rel);
    computeLinearOutputWithBias(k_r, rel_embeddings, runtime.key_weight, runtime.key_bias, hidden, hidden, num_rel);

    const attn_output = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(attn_output);
    @memset(attn_output, 0);
    const scores = try allocator.alloc(f32, seq_len);
    defer allocator.free(scores);
    const probs = try allocator.alloc(f32, num_heads * seq_len * seq_len);
    errdefer allocator.free(probs);
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)) * 3.0);

    for (0..num_heads) |h| {
        for (0..seq_len) |q_idx| {
            for (0..seq_len) |k_idx| {
                const rel_idx = @as(usize, @intCast(@as(i64, @intCast(q_idx)) - @as(i64, @intCast(k_idx)) + @as(i64, @intCast(seq_len - 1))));
                var score: f32 = 0;
                for (0..head_dim) |d| {
                    const hoff = h * head_dim + d;
                    const qoff = q_idx * hidden + hoff;
                    const koff = k_idx * hidden + hoff;
                    const roff = rel_idx * hidden + hoff;
                    score += query[qoff] * key[koff];
                    score += query[qoff] * k_r[roff];
                    score += q_r[roff] * key[koff];
                }
                scores[k_idx] = if (attention_mask[k_idx] == 0) -std.math.inf(f32) else score * scale;
            }
            const prob_row = probs[(h * seq_len + q_idx) * seq_len ..][0..seq_len];
            softmaxInto(scores, prob_row);
            for (0..seq_len) |k_idx| {
                const p = prob_row[k_idx];
                if (p == 0) continue;
                for (0..head_dim) |d| {
                    const hoff = h * head_dim + d;
                    const voff = k_idx * hidden + hoff;
                    const ooff = q_idx * hidden + hoff;
                    attn_output[ooff] += p * value[voff];
                }
            }
        }
    }

    const projected = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(projected);
    computeLinearOutputWithBias(projected, attn_output, runtime.output_dense_weight, runtime.output_dense_bias, hidden, hidden, seq_len);
    if (runtime.output_dense_lora) |pair| applyAdapterDelta(projected, attn_output, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);

    const attn_norm_input = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(attn_norm_input);
    for (0..attn_norm_input.len) |i| attn_norm_input[i] = hidden_in[i] + projected[i];
    const attn_norm = try allocator.dupe(f32, attn_norm_input);
    errdefer allocator.free(attn_norm);
    applyLayerNormInPlace(attn_norm, seq_len, hidden, runtime.attn_norm_weight, runtime.attn_norm_bias, runtime.layer_norm_eps);

    const intermediate_pre = try allocator.alloc(f32, seq_len * runtime.intermediate_size);
    errdefer allocator.free(intermediate_pre);
    computeLinearOutputWithBias(intermediate_pre, attn_norm, runtime.intermediate_weight, runtime.intermediate_bias, hidden, runtime.intermediate_size, seq_len);
    const intermediate_act = try allocator.dupe(f32, intermediate_pre);
    errdefer allocator.free(intermediate_act);
    geluInPlace(intermediate_act);

    const ff_out = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(ff_out);
    computeLinearOutputWithBias(ff_out, intermediate_act, runtime.output_ff_weight, runtime.output_ff_bias, runtime.intermediate_size, hidden, seq_len);

    const output_norm_input = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(output_norm_input);
    for (0..output_norm_input.len) |i| output_norm_input[i] = attn_norm[i] + ff_out[i];
    const out = try allocator.dupe(f32, output_norm_input);
    errdefer allocator.free(out);
    applyLayerNormInPlace(out, seq_len, hidden, runtime.output_norm_weight, runtime.output_norm_bias, runtime.layer_norm_eps);

    return .{
        .allocator = allocator,
        .hidden_in = hidden_in,
        .rel_embeddings = rel_embeddings,
        .query = query,
        .key = key,
        .value = value,
        .q_r = q_r,
        .k_r = k_r,
        .probs = probs,
        .attn_output = attn_output,
        .projected = projected,
        .attn_norm_input = attn_norm_input,
        .attn_norm = attn_norm,
        .intermediate_pre = intermediate_pre,
        .intermediate_act = intermediate_act,
        .ff_out = ff_out,
        .output_norm_input = output_norm_input,
        .out = out,
    };
}

fn installExactDebertaLayerOverrides(
    allocator: std.mem.Allocator,
    encoder: *reranker_head.EncoderRuntime,
    bundle: *const LoadedLoRABundle,
    target_layer_idx: usize,
) !void {
    for (bundle.layers) |layer| {
        const parsed_layer_idx = parseEncoderLayerIndex(layer.base_tensor_name) orelse continue;
        if (parsed_layer_idx != target_layer_idx) continue;
        const weight = try makeMergedResidentWeight(allocator, bundle.lora_alpha, &layer);
        errdefer {
            var cleanup = weight;
            cleanup.deinit();
        }
        try session_factory.replaceBlasResidentWeight(encoder.session, layer.base_tensor_name, weight);
    }
}

fn makeMergedResidentWeight(
    allocator: std.mem.Allocator,
    lora_alpha: f32,
    layer: *const LoadedLoRALayer,
) !weight_source.LoadedWeight {
    const merged_weight = try allocator.alloc(f32, layer.base_weight.len);
    errdefer allocator.free(merged_weight);
    const matrix = lora.Matrix{
        .rows = layer.input_dim,
        .cols = layer.output_dim,
        .data = layer.base_weight,
    };
    const adapter_a = lora.Matrix{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a };
    const adapter_b = lora.Matrix{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b };
    if (layer.dora_magnitude) |magnitude| {
        lora.doraMergeInto(.{
            .base = matrix,
            .adapter_a = adapter_a,
            .adapter_b = adapter_b,
            .magnitude = magnitude,
            .alpha = lora_alpha,
        }, merged_weight);
    } else {
        lora.mergeInto(
            matrix,
            adapter_a,
            adapter_b,
            lora_alpha,
            merged_weight,
        );
    }

    const hf_weight = try allocator.alloc(f32, merged_weight.len);
    errdefer allocator.free(hf_weight);
    transpose2DF32(hf_weight, merged_weight, layer.input_dim, layer.output_dim);
    allocator.free(merged_weight);

    const shape = [_]i64{
        @as(i64, @intCast(layer.output_dim)),
        @as(i64, @intCast(layer.input_dim)),
    };
    const tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, &shape, hf_weight);
    allocator.free(hf_weight);
    return .{ .tensor = tensor, .quantized = false };
}

fn loadBertExactTopLayerRuntime(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    layer_idx: usize,
) !BertExactTopLayerRuntime {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const hidden_size = manifest.hidden_size;
    if (hidden_size == 0) return error.MissingHiddenSize;

    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .bert => |value| value,
        .deberta => return error.UnsupportedArchitecture,
    };

    var access = try openTensorAccessForModelDir(allocator, model_dir);
    defer access.deinit();
    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .num_heads = cfg.num_attention_heads,
        .intermediate_size = cfg.intermediate_size,
        .layer_norm_eps = 1e-12,
        .query_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.self.query.weight"),
        .query_bias = try loadLayerVector(allocator, access, layer_idx, "attention.self.query.bias"),
        .key_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.self.key.weight"),
        .key_bias = try loadLayerVector(allocator, access, layer_idx, "attention.self.key.bias"),
        .value_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.self.value.weight"),
        .value_bias = try loadLayerVector(allocator, access, layer_idx, "attention.self.value.bias"),
        .output_dense_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "attention.output.dense.weight"),
        .output_dense_bias = try loadLayerVector(allocator, access, layer_idx, "attention.output.dense.bias"),
        .attn_norm_weight = try loadLayerVector(allocator, access, layer_idx, "attention.output.LayerNorm.weight"),
        .attn_norm_bias = try loadLayerVector(allocator, access, layer_idx, "attention.output.LayerNorm.bias"),
        .intermediate_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "intermediate.dense.weight"),
        .intermediate_bias = try loadLayerVector(allocator, access, layer_idx, "intermediate.dense.bias"),
        .output_ff_weight = try loadLayerWeightTransposed(allocator, access, layer_idx, "output.dense.weight"),
        .output_ff_bias = try loadLayerVector(allocator, access, layer_idx, "output.dense.bias"),
        .output_norm_weight = try loadLayerVector(allocator, access, layer_idx, "output.LayerNorm.weight"),
        .output_norm_bias = try loadLayerVector(allocator, access, layer_idx, "output.LayerNorm.bias"),
        .query_lora = exactLoRAPairFor(bundle, layer_idx, "query"),
        .key_lora = exactLoRAPairFor(bundle, layer_idx, "key"),
        .value_lora = exactLoRAPairFor(bundle, layer_idx, "value"),
        .output_dense_lora = exactLoRAPairFor(bundle, layer_idx, "attention.output.dense"),
    };
}

fn forwardBertExactTopLayerFromBoundary(
    allocator: std.mem.Allocator,
    runtime: *const BertExactTopLayerRuntime,
    lora_alpha: f32,
    hidden_in: []const f32,
    attention_mask: []const i64,
    seq_len: usize,
) !BertLastLayerForwardCache {
    const hidden = runtime.hidden_size;
    const num_heads = runtime.num_heads;
    const head_dim = hidden / num_heads;

    const query = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(query);
    const key = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(key);
    const value = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(value);
    computeLinearOutputWithBias(query, hidden_in, runtime.query_weight, runtime.query_bias, hidden, hidden, seq_len);
    computeLinearOutputWithBias(key, hidden_in, runtime.key_weight, runtime.key_bias, hidden, hidden, seq_len);
    computeLinearOutputWithBias(value, hidden_in, runtime.value_weight, runtime.value_bias, hidden, hidden, seq_len);
    if (runtime.query_lora) |pair| applyAdapterDelta(query, hidden_in, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);
    if (runtime.key_lora) |pair| applyAdapterDelta(key, hidden_in, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);
    if (runtime.value_lora) |pair| applyAdapterDelta(value, hidden_in, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);

    const attn_output = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(attn_output);
    @memset(attn_output, 0);
    const scores = try allocator.alloc(f32, seq_len);
    defer allocator.free(scores);
    const probs = try allocator.alloc(f32, num_heads * seq_len * seq_len);
    errdefer allocator.free(probs);
    for (0..num_heads) |h| {
        for (0..seq_len) |q_idx| {
            for (0..seq_len) |k_idx| {
                var score: f32 = 0;
                for (0..head_dim) |d| {
                    const qoff = q_idx * hidden + h * head_dim + d;
                    const koff = k_idx * hidden + h * head_dim + d;
                    score += query[qoff] * key[koff];
                }
                if (attention_mask[k_idx] == 0) score += -10000.0;
                scores[k_idx] = score / @sqrt(@as(f32, @floatFromInt(head_dim)));
            }
            const prob_row = probs[(h * seq_len + q_idx) * seq_len ..][0..seq_len];
            softmaxInto(scores, prob_row);
            for (0..seq_len) |k_idx| {
                const p = prob_row[k_idx];
                for (0..head_dim) |d| {
                    const voff = k_idx * hidden + h * head_dim + d;
                    const ooff = q_idx * hidden + h * head_dim + d;
                    attn_output[ooff] += p * value[voff];
                }
            }
        }
    }

    const projected = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(projected);
    computeLinearOutputWithBias(projected, attn_output, runtime.output_dense_weight, runtime.output_dense_bias, hidden, hidden, seq_len);
    if (runtime.output_dense_lora) |pair| applyAdapterDelta(projected, attn_output, pair.adapter_a, pair.adapter_b, hidden, hidden, pair.rank, lora_alpha);

    const attn_norm_input = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(attn_norm_input);
    for (0..attn_norm_input.len) |i| attn_norm_input[i] = hidden_in[i] + projected[i];
    const attn_norm = try allocator.dupe(f32, attn_norm_input);
    errdefer allocator.free(attn_norm);
    applyLayerNormInPlace(attn_norm, seq_len, hidden, runtime.attn_norm_weight, runtime.attn_norm_bias, runtime.layer_norm_eps);

    const intermediate_pre = try allocator.alloc(f32, seq_len * runtime.intermediate_size);
    errdefer allocator.free(intermediate_pre);
    computeLinearOutputWithBias(intermediate_pre, attn_norm, runtime.intermediate_weight, runtime.intermediate_bias, hidden, runtime.intermediate_size, seq_len);
    const intermediate_act = try allocator.dupe(f32, intermediate_pre);
    errdefer allocator.free(intermediate_act);
    geluInPlace(intermediate_act);

    const ff_out = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(ff_out);
    computeLinearOutputWithBias(ff_out, intermediate_act, runtime.output_ff_weight, runtime.output_ff_bias, runtime.intermediate_size, hidden, seq_len);

    const output_norm_input = try allocator.alloc(f32, seq_len * hidden);
    errdefer allocator.free(output_norm_input);
    for (0..output_norm_input.len) |i| output_norm_input[i] = attn_norm[i] + ff_out[i];
    const out = try allocator.dupe(f32, output_norm_input);
    errdefer allocator.free(out);
    applyLayerNormInPlace(out, seq_len, hidden, runtime.output_norm_weight, runtime.output_norm_bias, runtime.layer_norm_eps);
    return .{
        .allocator = allocator,
        .hidden_in = hidden_in,
        .query = query,
        .key = key,
        .value = value,
        .probs = probs,
        .attn_output = attn_output,
        .projected = projected,
        .attn_norm_input = attn_norm_input,
        .attn_norm = attn_norm,
        .intermediate_pre = intermediate_pre,
        .intermediate_act = intermediate_act,
        .ff_out = ff_out,
        .output_norm_input = output_norm_input,
        .out = out,
    };
}

fn exactLoRAPairFor(bundle: *const LoadedLoRABundle, layer_idx: usize, module_name: []const u8) ?ExactLoRAPairRef {
    for (bundle.layers) |layer| {
        const parsed_layer_idx = parseEncoderLayerIndex(layer.base_tensor_name) orelse continue;
        if (parsed_layer_idx != layer_idx) continue;
        if (!std.mem.eql(u8, layer.module_name, module_name)) continue;
        return .{
            .adapter_a = layer.adapter_a,
            .adapter_b = layer.adapter_b,
            .rank = layer.rank,
        };
    }
    return null;
}

fn loadLayerWeightTransposed(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, layer_idx: usize, suffix: []const u8) ![]f32 {
    const name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.{s}", .{ layer_idx, suffix });
    defer allocator.free(name);
    var tensor = try loadTensorAsF32(allocator, access, name);
    defer tensor.deinit();
    if (tensor.shape.len != 2) return error.ShapeMismatch;
    const rows: usize = @intCast(tensor.shape[0]);
    const cols: usize = @intCast(tensor.shape[1]);
    const out = try allocator.alloc(f32, rows * cols);
    transpose2DF32(out, tensor.asFloat32(), rows, cols);
    return out;
}

fn loadLayerVector(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, layer_idx: usize, suffix: []const u8) ![]f32 {
    const name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.{s}", .{ layer_idx, suffix });
    defer allocator.free(name);
    var tensor = try loadTensorAsF32(allocator, access, name);
    defer tensor.deinit();
    return try allocator.dupe(f32, tensor.asFloat32());
}

fn computeLinearOutputWithBias(dst: []f32, input: []const f32, weight: []const f32, bias: []const f32, input_dim: usize, output_dim: usize, rows: usize) void {
    for (0..rows) |r| {
        const out_row = dst[r * output_dim ..][0..output_dim];
        @memcpy(out_row, bias[0..output_dim]);
        const in_row = input[r * input_dim ..][0..input_dim];
        for (0..input_dim) |i| {
            const x = in_row[i];
            const w_row = weight[i * output_dim ..][0..output_dim];
            for (w_row, 0..) |w, j| out_row[j] += x * w;
        }
    }
}

fn geluInPlace(values: []f32) void {
    for (values) |*value| {
        const x = value.*;
        const inner = 0.7978845608 * (x + 0.044715 * x * x * x);
        value.* = 0.5 * x * (1.0 + std.math.tanh(inner));
    }
}

fn softmaxInto(scores: []const f32, out: []f32) void {
    var max_score = scores[0];
    for (scores[1..]) |score| max_score = @max(max_score, score);
    var sum: f32 = 0;
    for (scores, 0..) |score, idx| {
        const expv = @exp(score - max_score);
        out[idx] = expv;
        sum += expv;
    }
    if (sum == 0) return;
    for (out) |*value| value.* /= sum;
}

fn applyLayerNormInPlace(data_rows: []f32, rows: usize, width: usize, gamma: []const f32, beta: []const f32, eps: f32) void {
    for (0..rows) |row_idx| {
        const row = data_rows[row_idx * width ..][0..width];
        var mean: f32 = 0;
        for (row) |value| mean += value;
        mean /= @as(f32, @floatFromInt(width));
        var variance: f32 = 0;
        for (row) |value| {
            const centered = value - mean;
            variance += centered * centered;
        }
        variance /= @as(f32, @floatFromInt(width));
        const inv_std = 1.0 / @sqrt(variance + eps);
        for (row, 0..) |*value, idx| {
            value.* = ((value.* - mean) * inv_std) * gamma[idx] + beta[idx];
        }
    }
}

fn applyBertExactTopLayerLoRAUpdate(
    allocator: std.mem.Allocator,
    runtime: *BertExactTopLayerRuntime,
    lora_alpha: f32,
    cache: *const BertLastLayerForwardCache,
    classifier_weight: []const f32,
    grad_pred: f32,
    learning_rate: f32,
    max_grad_norm: f32,
    use_schedule_free: bool,
) !void {
    const grad_out = try allocator.alloc(f32, cache.out.len);
    defer allocator.free(grad_out);
    @memset(grad_out, 0);
    for (0..classifier_weight.len) |i| grad_out[i] = grad_pred * classifier_weight[i];
    const grad_hidden = try backwardBertExactTopLayerFromGrad(
        allocator,
        runtime,
        lora_alpha,
        cache,
        grad_out,
        learning_rate,
        max_grad_norm,
        use_schedule_free,
        true,
    );
    allocator.free(grad_hidden);
}

fn backwardBertExactTopLayerFromGrad(
    allocator: std.mem.Allocator,
    runtime: *BertExactTopLayerRuntime,
    lora_alpha: f32,
    cache: *const BertLastLayerForwardCache,
    grad_out: []const f32,
    learning_rate: f32,
    max_grad_norm: f32,
    use_schedule_free: bool,
    update_enabled: bool,
) ![]f32 {
    const hidden = runtime.hidden_size;
    const seq_len = cache.out.len / hidden;
    const num_heads = runtime.num_heads;
    const head_dim = hidden / num_heads;
    const intermediate_size = runtime.intermediate_size;

    const grad_output_norm_input = try allocator.alloc(f32, cache.out.len);
    errdefer allocator.free(grad_output_norm_input);
    backwardLayerNorm(grad_out, cache.output_norm_input, seq_len, hidden, runtime.output_norm_weight, runtime.layer_norm_eps, grad_output_norm_input);

    const grad_attn_norm = try allocator.dupe(f32, grad_output_norm_input);
    defer allocator.free(grad_attn_norm);

    const grad_intermediate_act = try allocator.alloc(f32, seq_len * intermediate_size);
    defer allocator.free(grad_intermediate_act);
    backwardLinearRowsInput(grad_output_norm_input, seq_len, hidden, runtime.output_ff_weight, intermediate_size, grad_intermediate_act);
    for (0..grad_intermediate_act.len) |idx| grad_intermediate_act[idx] *= geluDerivative(cache.intermediate_pre[idx]);

    const grad_attn_norm_from_ff = try allocator.alloc(f32, cache.attn_norm.len);
    defer allocator.free(grad_attn_norm_from_ff);
    backwardLinearRowsInput(grad_intermediate_act, seq_len, intermediate_size, runtime.intermediate_weight, hidden, grad_attn_norm_from_ff);
    addInPlace(grad_attn_norm, grad_attn_norm_from_ff);

    const grad_attn_norm_input = try allocator.alloc(f32, cache.attn_norm_input.len);
    defer allocator.free(grad_attn_norm_input);
    backwardLayerNorm(grad_attn_norm, cache.attn_norm_input, seq_len, hidden, runtime.attn_norm_weight, runtime.layer_norm_eps, grad_attn_norm_input);

    const grad_attn_output = try allocator.alloc(f32, cache.attn_output.len);
    defer allocator.free(grad_attn_output);
    backwardLinearRowsInputWithLoRA(
        allocator,
        grad_attn_norm_input,
        seq_len,
        hidden,
        runtime.output_dense_weight,
        hidden,
        runtime.output_dense_lora,
        lora_alpha,
        grad_attn_output,
    );

    const grad_query = try allocator.alloc(f32, cache.query.len);
    defer allocator.free(grad_query);
    @memset(grad_query, 0);
    const grad_key = try allocator.alloc(f32, cache.key.len);
    defer allocator.free(grad_key);
    @memset(grad_key, 0);
    const grad_value = try allocator.alloc(f32, cache.value.len);
    defer allocator.free(grad_value);
    @memset(grad_value, 0);
    const grad_probs = try allocator.alloc(f32, seq_len);
    defer allocator.free(grad_probs);
    const grad_scores = try allocator.alloc(f32, seq_len);
    defer allocator.free(grad_scores);
    const inv_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));

    for (0..num_heads) |h| {
        for (0..seq_len) |q_idx| {
            @memset(grad_probs, 0);
            for (0..seq_len) |k_idx| {
                for (0..head_dim) |d| {
                    const voff = k_idx * hidden + h * head_dim + d;
                    const ooff = q_idx * hidden + h * head_dim + d;
                    grad_value[voff] += cache.probs[(h * seq_len + q_idx) * seq_len + k_idx] * grad_attn_output[ooff];
                    grad_probs[k_idx] += cache.value[voff] * grad_attn_output[ooff];
                }
            }
            backwardSoftmax(cache.probs[(h * seq_len + q_idx) * seq_len ..][0..seq_len], grad_probs, grad_scores);
            for (0..seq_len) |k_idx| {
                const grad_score = grad_scores[k_idx] * inv_scale;
                for (0..head_dim) |d| {
                    const qoff = q_idx * hidden + h * head_dim + d;
                    const koff = k_idx * hidden + h * head_dim + d;
                    grad_query[qoff] += grad_score * cache.key[koff];
                    grad_key[koff] += grad_score * cache.query[qoff];
                }
            }
        }
    }

    const grad_hidden_from_query = try allocator.alloc(f32, cache.hidden_in.len);
    defer allocator.free(grad_hidden_from_query);
    backwardLinearRowsInputWithLoRA(allocator, grad_query, seq_len, hidden, runtime.query_weight, hidden, runtime.query_lora, lora_alpha, grad_hidden_from_query);
    const grad_hidden_from_key = try allocator.alloc(f32, cache.hidden_in.len);
    defer allocator.free(grad_hidden_from_key);
    backwardLinearRowsInputWithLoRA(allocator, grad_key, seq_len, hidden, runtime.key_weight, hidden, runtime.key_lora, lora_alpha, grad_hidden_from_key);
    const grad_hidden_from_value = try allocator.alloc(f32, cache.hidden_in.len);
    defer allocator.free(grad_hidden_from_value);
    backwardLinearRowsInputWithLoRA(allocator, grad_value, seq_len, hidden, runtime.value_weight, hidden, runtime.value_lora, lora_alpha, grad_hidden_from_value);

    const grad_hidden_in = try allocator.dupe(f32, grad_attn_norm_input);
    addInPlace(grad_hidden_in, grad_hidden_from_query);
    addInPlace(grad_hidden_in, grad_hidden_from_key);
    addInPlace(grad_hidden_in, grad_hidden_from_value);

    if (update_enabled) {
        if (runtime.query_lora) |pair| {
            if (runtime.query_lora_adam == null) runtime.query_lora_adam = try ExactLoRAAdamState.init(allocator, pair.adapter_a.len, pair.adapter_b.len);
            try applyLoRAGradientStepExact(allocator, cache.hidden_in, seq_len, hidden, pair, lora_alpha, grad_query, hidden, learning_rate, &runtime.query_lora_adam.?, max_grad_norm, use_schedule_free);
        }
        if (runtime.key_lora) |pair| {
            if (runtime.key_lora_adam == null) runtime.key_lora_adam = try ExactLoRAAdamState.init(allocator, pair.adapter_a.len, pair.adapter_b.len);
            try applyLoRAGradientStepExact(allocator, cache.hidden_in, seq_len, hidden, pair, lora_alpha, grad_key, hidden, learning_rate, &runtime.key_lora_adam.?, max_grad_norm, use_schedule_free);
        }
        if (runtime.value_lora) |pair| {
            if (runtime.value_lora_adam == null) runtime.value_lora_adam = try ExactLoRAAdamState.init(allocator, pair.adapter_a.len, pair.adapter_b.len);
            try applyLoRAGradientStepExact(allocator, cache.hidden_in, seq_len, hidden, pair, lora_alpha, grad_value, hidden, learning_rate, &runtime.value_lora_adam.?, max_grad_norm, use_schedule_free);
        }
        if (runtime.output_dense_lora) |pair| {
            if (runtime.output_dense_lora_adam == null) runtime.output_dense_lora_adam = try ExactLoRAAdamState.init(allocator, pair.adapter_a.len, pair.adapter_b.len);
            try applyLoRAGradientStepExact(allocator, cache.attn_output, seq_len, hidden, pair, lora_alpha, grad_attn_norm_input, hidden, learning_rate, &runtime.output_dense_lora_adam.?, max_grad_norm, use_schedule_free);
        }
    }

    return grad_hidden_in;
}

fn loadBertExactRuntimeRange(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    start_layer_idx: usize,
    end_layer_exclusive: usize,
) ![]BertExactTopLayerRuntime {
    if (end_layer_exclusive <= start_layer_idx) return error.InvalidTopLayerCount;
    const count = end_layer_exclusive - start_layer_idx;
    const runtimes = try allocator.alloc(BertExactTopLayerRuntime, count);
    var built: usize = 0;
    errdefer {
        for (runtimes[0..built]) |*runtime| runtime.deinit();
        allocator.free(runtimes);
    }
    for (start_layer_idx..end_layer_exclusive) |layer_idx| {
        runtimes[built] = try loadBertExactTopLayerRuntime(allocator, model_dir, bundle, layer_idx);
        built += 1;
    }
    return runtimes;
}

fn deinitBertRuntimeSlice(runtimes: *[]BertExactTopLayerRuntime) void {
    const allocator = runtimes.*[0].allocator;
    for (runtimes.*) |*runtime| runtime.deinit();
    allocator.free(runtimes.*);
    runtimes.* = &.{};
}

fn forwardBertExactRuntimeRange(
    allocator: std.mem.Allocator,
    runtimes: []const BertExactTopLayerRuntime,
    lora_alpha: f32,
    hidden_in: []const f32,
    attention_mask: []const i64,
    seq_len: usize,
) ![]BertLastLayerForwardCache {
    const caches = try allocator.alloc(BertLastLayerForwardCache, runtimes.len);
    var built: usize = 0;
    errdefer {
        for (caches[0..built]) |*cache| cache.deinit();
        allocator.free(caches);
    }
    for (runtimes, 0..) |*runtime, idx| {
        const layer_input = if (idx == 0) hidden_in else caches[idx - 1].out;
        caches[idx] = try forwardBertExactTopLayerFromBoundary(allocator, runtime, lora_alpha, layer_input, attention_mask, seq_len);
        built += 1;
    }
    return caches;
}

fn deinitBertCacheSlice(allocator: std.mem.Allocator, caches: []BertLastLayerForwardCache) void {
    for (caches) |*cache| cache.deinit();
    allocator.free(caches);
}

fn loadDebertaExactRuntimeRange(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    start_layer_idx: usize,
    end_layer_exclusive: usize,
) ![]DebertaExactTopLayerRuntime {
    if (end_layer_exclusive <= start_layer_idx) return error.InvalidTopLayerCount;
    const count = end_layer_exclusive - start_layer_idx;
    const runtimes = try allocator.alloc(DebertaExactTopLayerRuntime, count);
    var built: usize = 0;
    errdefer {
        for (runtimes[0..built]) |*runtime| runtime.deinit();
        allocator.free(runtimes);
    }
    for (start_layer_idx..end_layer_exclusive) |layer_idx| {
        runtimes[built] = try loadDebertaExactTopLayerRuntime(allocator, model_dir, bundle, layer_idx);
        built += 1;
    }
    return runtimes;
}

fn deinitDebertaRuntimeSlice(runtimes: *[]DebertaExactTopLayerRuntime) void {
    const allocator = runtimes.*[0].allocator;
    for (runtimes.*) |*runtime| runtime.deinit();
    allocator.free(runtimes.*);
    runtimes.* = &.{};
}

fn forwardDebertaExactRuntimeRange(
    allocator: std.mem.Allocator,
    runtimes: []const DebertaExactTopLayerRuntime,
    lora_alpha: f32,
    hidden_in: []const f32,
    attention_mask: []const i64,
    seq_len: usize,
) ![]DebertaLastLayerForwardCache {
    const caches = try allocator.alloc(DebertaLastLayerForwardCache, runtimes.len);
    var built: usize = 0;
    errdefer {
        for (caches[0..built]) |*cache| cache.deinit();
        allocator.free(caches);
    }
    for (runtimes, 0..) |*runtime, idx| {
        const layer_input = if (idx == 0) hidden_in else caches[idx - 1].out;
        caches[idx] = try forwardDebertaExactTopLayerFromBoundary(allocator, runtime, lora_alpha, layer_input, attention_mask, seq_len);
        built += 1;
    }
    return caches;
}

fn deinitDebertaCacheSlice(allocator: std.mem.Allocator, caches: []DebertaLastLayerForwardCache) void {
    for (caches) |*cache| cache.deinit();
    allocator.free(caches);
}

fn applyDebertaExactOutputDenseLoRAUpdate(
    allocator: std.mem.Allocator,
    runtime: *DebertaExactTopLayerRuntime,
    lora_alpha: f32,
    cache: *const DebertaLastLayerForwardCache,
    classifier_weight: []const f32,
    grad_pred: f32,
    seq_len: usize,
    learning_rate: f32,
    max_grad_norm: f32,
    use_schedule_free: bool,
) !void {
    const grad_out = try allocator.alloc(f32, cache.out.len);
    defer allocator.free(grad_out);
    @memset(grad_out, 0);
    for (0..runtime.hidden_size) |i| grad_out[i] = grad_pred * classifier_weight[i];
    const grad_hidden = try backwardDebertaExactTopLayerFromGrad(
        allocator,
        runtime,
        lora_alpha,
        cache,
        grad_out,
        seq_len,
        learning_rate,
        max_grad_norm,
        use_schedule_free,
        true,
    );
    allocator.free(grad_hidden);
}

fn backwardDebertaExactTopLayerFromGrad(
    allocator: std.mem.Allocator,
    runtime: *DebertaExactTopLayerRuntime,
    lora_alpha: f32,
    cache: *const DebertaLastLayerForwardCache,
    grad_out: []const f32,
    seq_len: usize,
    learning_rate: f32,
    max_grad_norm: f32,
    use_schedule_free: bool,
    update_enabled: bool,
) ![]f32 {
    const hidden = runtime.hidden_size;
    const intermediate_size = runtime.intermediate_size;

    const grad_output_norm_input = try allocator.alloc(f32, cache.out.len);
    errdefer allocator.free(grad_output_norm_input);
    backwardLayerNorm(grad_out, cache.output_norm_input, seq_len, hidden, runtime.output_norm_weight, runtime.layer_norm_eps, grad_output_norm_input);

    const grad_attn_norm = try allocator.dupe(f32, grad_output_norm_input);
    defer allocator.free(grad_attn_norm);

    const grad_intermediate_act = try allocator.alloc(f32, seq_len * intermediate_size);
    defer allocator.free(grad_intermediate_act);
    backwardLinearRowsInput(grad_output_norm_input, seq_len, hidden, runtime.output_ff_weight, intermediate_size, grad_intermediate_act);
    for (0..grad_intermediate_act.len) |idx| grad_intermediate_act[idx] *= geluDerivative(cache.intermediate_pre[idx]);

    const grad_attn_norm_from_ff = try allocator.alloc(f32, cache.attn_norm.len);
    defer allocator.free(grad_attn_norm_from_ff);
    backwardLinearRowsInput(grad_intermediate_act, seq_len, intermediate_size, runtime.intermediate_weight, hidden, grad_attn_norm_from_ff);
    addInPlace(grad_attn_norm, grad_attn_norm_from_ff);

    const grad_attn_norm_input = try allocator.alloc(f32, cache.attn_norm_input.len);
    defer allocator.free(grad_attn_norm_input);
    backwardLayerNorm(grad_attn_norm, cache.attn_norm_input, seq_len, hidden, runtime.attn_norm_weight, runtime.layer_norm_eps, grad_attn_norm_input);

    const grad_attn_output = try allocator.alloc(f32, cache.attn_output.len);
    defer allocator.free(grad_attn_output);
    backwardLinearRowsInputWithLoRA(
        allocator,
        grad_attn_norm_input,
        seq_len,
        hidden,
        runtime.output_dense_weight,
        hidden,
        runtime.output_dense_lora,
        lora_alpha,
        grad_attn_output,
    );

    const num_heads = runtime.num_heads;
    const head_dim = hidden / num_heads;
    const num_rel = 2 * seq_len - 1;
    const grad_query = try allocator.alloc(f32, cache.query.len);
    defer allocator.free(grad_query);
    @memset(grad_query, 0);
    const grad_key = try allocator.alloc(f32, cache.key.len);
    defer allocator.free(grad_key);
    @memset(grad_key, 0);
    const grad_value = try allocator.alloc(f32, cache.value.len);
    defer allocator.free(grad_value);
    @memset(grad_value, 0);
    const grad_q_r = try allocator.alloc(f32, num_rel * hidden);
    defer allocator.free(grad_q_r);
    @memset(grad_q_r, 0);
    const grad_k_r = try allocator.alloc(f32, num_rel * hidden);
    defer allocator.free(grad_k_r);
    @memset(grad_k_r, 0);
    const grad_probs = try allocator.alloc(f32, seq_len);
    defer allocator.free(grad_probs);
    const grad_scores = try allocator.alloc(f32, seq_len);
    defer allocator.free(grad_scores);
    const attn_scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)) * 3.0);

    for (0..num_heads) |h| {
        for (0..seq_len) |q_idx| {
            @memset(grad_probs, 0);
            for (0..seq_len) |k_idx| {
                for (0..head_dim) |d| {
                    const hoff = h * head_dim + d;
                    const voff = k_idx * hidden + hoff;
                    const ooff = q_idx * hidden + hoff;
                    grad_value[voff] += cache.probs[(h * seq_len + q_idx) * seq_len + k_idx] * grad_attn_output[ooff];
                    grad_probs[k_idx] += cache.value[voff] * grad_attn_output[ooff];
                }
            }
            backwardSoftmax(cache.probs[(h * seq_len + q_idx) * seq_len ..][0..seq_len], grad_probs, grad_scores);
            for (0..seq_len) |k_idx| {
                const grad_score = grad_scores[k_idx] * attn_scale;
                const rel_idx = @as(usize, @intCast(@as(i64, @intCast(q_idx)) - @as(i64, @intCast(k_idx)) + @as(i64, @intCast(seq_len - 1))));
                for (0..head_dim) |d| {
                    const hoff = h * head_dim + d;
                    const qoff = q_idx * hidden + hoff;
                    const koff = k_idx * hidden + hoff;
                    const roff = rel_idx * hidden + hoff;
                    grad_query[qoff] += grad_score * (cache.key[koff] + cache.k_r[roff]);
                    grad_key[koff] += grad_score * (cache.query[qoff] + cache.q_r[roff]);
                    grad_q_r[roff] += grad_score * cache.key[koff];
                    grad_k_r[roff] += grad_score * cache.query[qoff];
                }
            }
        }
    }

    const grad_hidden_from_query = try allocator.alloc(f32, cache.hidden_in.len);
    defer allocator.free(grad_hidden_from_query);
    backwardLinearRowsInputWithLoRA(allocator, grad_query, seq_len, hidden, runtime.query_weight, hidden, runtime.query_lora, lora_alpha, grad_hidden_from_query);
    const grad_hidden_from_key = try allocator.alloc(f32, cache.hidden_in.len);
    defer allocator.free(grad_hidden_from_key);
    backwardLinearRowsInputWithLoRA(allocator, grad_key, seq_len, hidden, runtime.key_weight, hidden, runtime.key_lora, lora_alpha, grad_hidden_from_key);
    const grad_hidden_from_value = try allocator.alloc(f32, cache.hidden_in.len);
    defer allocator.free(grad_hidden_from_value);
    backwardLinearRowsInputWithLoRA(allocator, grad_value, seq_len, hidden, runtime.value_weight, hidden, runtime.value_lora, lora_alpha, grad_hidden_from_value);

    const grad_hidden_in = try allocator.dupe(f32, grad_attn_norm_input);
    addInPlace(grad_hidden_in, grad_hidden_from_query);
    addInPlace(grad_hidden_in, grad_hidden_from_key);
    addInPlace(grad_hidden_in, grad_hidden_from_value);

    if (update_enabled) {
        if (runtime.query_lora) |query_pair| {
            if (runtime.query_lora_adam == null) runtime.query_lora_adam = try ExactLoRAAdamState.init(allocator, query_pair.adapter_a.len, query_pair.adapter_b.len);
            try applyLoRAGradientStepExactAccumulated2(
                allocator,
                cache.hidden_in,
                seq_len,
                hidden,
                cache.rel_embeddings,
                num_rel,
                hidden,
                query_pair,
                lora_alpha,
                grad_query,
                grad_q_r,
                hidden,
                learning_rate,
                &runtime.query_lora_adam.?,
                max_grad_norm,
                use_schedule_free,
            );
        }
        if (runtime.key_lora) |key_pair| {
            if (runtime.key_lora_adam == null) runtime.key_lora_adam = try ExactLoRAAdamState.init(allocator, key_pair.adapter_a.len, key_pair.adapter_b.len);
            try applyLoRAGradientStepExactAccumulated2(
                allocator,
                cache.hidden_in,
                seq_len,
                hidden,
                cache.rel_embeddings,
                num_rel,
                hidden,
                key_pair,
                lora_alpha,
                grad_key,
                grad_k_r,
                hidden,
                learning_rate,
                &runtime.key_lora_adam.?,
                max_grad_norm,
                use_schedule_free,
            );
        }
        if (runtime.value_lora) |value_pair| {
            if (runtime.value_lora_adam == null) runtime.value_lora_adam = try ExactLoRAAdamState.init(allocator, value_pair.adapter_a.len, value_pair.adapter_b.len);
            try applyLoRAGradientStepExact(
                allocator,
                cache.hidden_in,
                seq_len,
                hidden,
                value_pair,
                lora_alpha,
                grad_value,
                hidden,
                learning_rate,
                &runtime.value_lora_adam.?,
                max_grad_norm,
                use_schedule_free,
            );
        }
        if (runtime.output_dense_lora) |pair| {
            if (runtime.output_dense_lora_adam == null) runtime.output_dense_lora_adam = try ExactLoRAAdamState.init(allocator, pair.adapter_a.len, pair.adapter_b.len);
            try applyLoRAGradientStepExact(
                allocator,
                cache.attn_output,
                seq_len,
                hidden,
                pair,
                lora_alpha,
                grad_attn_norm_input,
                hidden,
                learning_rate,
                &runtime.output_dense_lora_adam.?,
                max_grad_norm,
                use_schedule_free,
            );
        }
    }

    return grad_hidden_in;
}

fn applyLoRAGradientStepExact(
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    pair: ExactLoRAPairRef,
    lora_alpha: f32,
    grad_out: []const f32,
    out_dim: usize,
    learning_rate: f32,
    adam_state: ?*ExactLoRAAdamState,
    max_grad_norm: f32,
    use_schedule_free: bool,
) !void {
    const grad_a = try allocator.alloc(f32, pair.adapter_a.len);
    defer allocator.free(grad_a);
    @memset(grad_a, 0);
    const grad_b = try allocator.alloc(f32, pair.adapter_b.len);
    defer allocator.free(grad_b);
    @memset(grad_b, 0);
    const tmp_rank = try allocator.alloc(f32, pair.rank);
    defer allocator.free(tmp_rank);
    const grad_tmp = try allocator.alloc(f32, pair.rank);
    defer allocator.free(grad_tmp);
    const scale = lora_alpha / @as(f32, @floatFromInt(pair.rank));

    for (0..rows) |r| {
        const in_row = input[r * in_dim ..][0..in_dim];
        const grad_row = grad_out[r * out_dim ..][0..out_dim];
        @memset(tmp_rank, 0);
        for (0..in_dim) |i| {
            const a_row = pair.adapter_a[i * pair.rank ..][0..pair.rank];
            for (0..pair.rank) |rr| tmp_rank[rr] += in_row[i] * a_row[rr];
        }
        for (0..pair.rank) |rr| {
            const grad_b_row = grad_b[rr * out_dim ..][0..out_dim];
            const scaled_tmp = scale * tmp_rank[rr];
            for (0..out_dim) |j| grad_b_row[j] += scaled_tmp * grad_row[j];
        }
        @memset(grad_tmp, 0);
        for (0..pair.rank) |rr| {
            const b_row = pair.adapter_b[rr * out_dim ..][0..out_dim];
            var sum: f32 = 0;
            for (0..out_dim) |j| sum += grad_row[j] * b_row[j];
            grad_tmp[rr] = scale * sum;
        }
        for (0..in_dim) |i| {
            const grad_a_row = grad_a[i * pair.rank ..][0..pair.rank];
            for (0..pair.rank) |rr| grad_a_row[rr] += in_row[i] * grad_tmp[rr];
        }
    }

    if (max_grad_norm > 0) {
        // Clip grad_a
        var norm_a: f64 = 0;
        for (grad_a) |g| norm_a += @as(f64, g) * @as(f64, g);
        norm_a = @sqrt(norm_a);
        if (norm_a > @as(f64, max_grad_norm)) {
            const clip_scale_a: f32 = @floatCast(@as(f64, max_grad_norm) / norm_a);
            for (grad_a) |*g| g.* *= clip_scale_a;
        }
        // Clip grad_b
        var norm_b: f64 = 0;
        for (grad_b) |g| norm_b += @as(f64, g) * @as(f64, g);
        norm_b = @sqrt(norm_b);
        if (norm_b > @as(f64, max_grad_norm)) {
            const clip_scale_b: f32 = @floatCast(@as(f64, max_grad_norm) / norm_b);
            for (grad_b) |*g| g.* *= clip_scale_b;
        }
    }

    if (adam_state) |s| {
        s.step += 1;
        const beta2: f32 = 0.999;
        const epsilon: f32 = 1e-8;
        const weight_decay: f32 = 0.01;
        if (use_schedule_free) {
            // Schedule-free AdamW (Defazio et al. 2024).
            // x (pair.adapter_*) is the Polyak-averaged iterate; z_* is the base iterate.
            // On first step, initialise z from x so the base iterate starts at the same point.
            const beta1: f32 = 0.9;
            const bc2 = 1.0 - std.math.pow(f32, beta2, @as(f32, @floatFromInt(s.step)));
            const c = @min(beta1, 1.0 / @as(f32, @floatFromInt(s.step)));
            if (s.step == 1) {
                @memcpy(s.z_a, pair.adapter_a);
                @memcpy(s.z_b, pair.adapter_b);
            }
            for (pair.adapter_a, grad_a, s.v_a, s.z_a) |*x, g, *v, *z| {
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                z.* = z.* - learning_rate * g / (@sqrt(v.* / bc2) + epsilon) - learning_rate * weight_decay * z.*;
                x.* = (1.0 - c) * x.* + c * z.*;
            }
            for (pair.adapter_b, grad_b, s.v_b, s.z_b) |*x, g, *v, *z| {
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                z.* = z.* - learning_rate * g / (@sqrt(v.* / bc2) + epsilon) - learning_rate * weight_decay * z.*;
                x.* = (1.0 - c) * x.* + c * z.*;
            }
        } else {
            const beta1: f32 = 0.9;
            const t: f32 = @floatFromInt(s.step);
            const bc1 = 1.0 - std.math.pow(f32, beta1, t);
            const bc2 = 1.0 - std.math.pow(f32, beta2, t);
            for (pair.adapter_a, grad_a, s.m_a, s.v_a) |*p, g, *m, *v| {
                m.* = beta1 * m.* + (1.0 - beta1) * g;
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                p.* -= learning_rate * (m.* / bc1 / (@sqrt(v.* / bc2) + epsilon) + weight_decay * p.*);
            }
            for (pair.adapter_b, grad_b, s.m_b, s.v_b) |*p, g, *m, *v| {
                m.* = beta1 * m.* + (1.0 - beta1) * g;
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                p.* -= learning_rate * (m.* / bc1 / (@sqrt(v.* / bc2) + epsilon) + weight_decay * p.*);
            }
        }
    } else {
        for (pair.adapter_a, grad_a) |*value, grad| value.* -= learning_rate * grad;
        for (pair.adapter_b, grad_b) |*value, grad| value.* -= learning_rate * grad;
    }
}

fn applyLoRAGradientStepExactAccumulated2(
    allocator: std.mem.Allocator,
    input_a: []const f32,
    rows_a: usize,
    in_dim_a: usize,
    input_b: []const f32,
    rows_b: usize,
    in_dim_b: usize,
    pair: ExactLoRAPairRef,
    lora_alpha: f32,
    grad_out_a: []const f32,
    grad_out_b: []const f32,
    out_dim: usize,
    learning_rate: f32,
    adam_state: ?*ExactLoRAAdamState,
    max_grad_norm: f32,
    use_schedule_free: bool,
) !void {
    const grad_a = try allocator.alloc(f32, pair.adapter_a.len);
    defer allocator.free(grad_a);
    @memset(grad_a, 0);
    const grad_b = try allocator.alloc(f32, pair.adapter_b.len);
    defer allocator.free(grad_b);
    @memset(grad_b, 0);

    try accumulateLoRAGradients(allocator, input_a, rows_a, in_dim_a, pair, lora_alpha, grad_out_a, out_dim, grad_a, grad_b);
    try accumulateLoRAGradients(allocator, input_b, rows_b, in_dim_b, pair, lora_alpha, grad_out_b, out_dim, grad_a, grad_b);

    applyAccumulatedLoRAUpdate(pair, grad_a, grad_b, learning_rate, adam_state, max_grad_norm, use_schedule_free);
}

fn accumulateLoRAGradients(
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    pair: ExactLoRAPairRef,
    lora_alpha: f32,
    grad_out: []const f32,
    out_dim: usize,
    grad_a: []f32,
    grad_b: []f32,
) !void {
    const tmp_rank = try allocator.alloc(f32, pair.rank);
    defer allocator.free(tmp_rank);
    const grad_tmp = try allocator.alloc(f32, pair.rank);
    defer allocator.free(grad_tmp);
    const scale = lora_alpha / @as(f32, @floatFromInt(pair.rank));

    for (0..rows) |r| {
        const in_row = input[r * in_dim ..][0..in_dim];
        const grad_row = grad_out[r * out_dim ..][0..out_dim];
        @memset(tmp_rank, 0);
        for (0..in_dim) |i| {
            const a_row = pair.adapter_a[i * pair.rank ..][0..pair.rank];
            for (0..pair.rank) |rr| tmp_rank[rr] += in_row[i] * a_row[rr];
        }
        for (0..pair.rank) |rr| {
            const grad_b_row = grad_b[rr * out_dim ..][0..out_dim];
            const scaled_tmp = scale * tmp_rank[rr];
            for (0..out_dim) |j| grad_b_row[j] += scaled_tmp * grad_row[j];
        }
        @memset(grad_tmp, 0);
        for (0..pair.rank) |rr| {
            const b_row = pair.adapter_b[rr * out_dim ..][0..out_dim];
            var sum: f32 = 0;
            for (0..out_dim) |j| sum += grad_row[j] * b_row[j];
            grad_tmp[rr] = scale * sum;
        }
        for (0..in_dim) |i| {
            const grad_a_row = grad_a[i * pair.rank ..][0..pair.rank];
            for (0..pair.rank) |rr| grad_a_row[rr] += in_row[i] * grad_tmp[rr];
        }
    }
}

fn applyAccumulatedLoRAUpdate(
    pair: ExactLoRAPairRef,
    grad_a: []f32,
    grad_b: []f32,
    learning_rate: f32,
    adam_state: ?*ExactLoRAAdamState,
    max_grad_norm: f32,
    use_schedule_free: bool,
) void {
    if (max_grad_norm > 0) {
        var norm_a: f64 = 0;
        for (grad_a) |g| norm_a += @as(f64, g) * @as(f64, g);
        norm_a = @sqrt(norm_a);
        if (norm_a > @as(f64, max_grad_norm)) {
            const clip_scale_a: f32 = @floatCast(@as(f64, max_grad_norm) / norm_a);
            for (grad_a) |*g| g.* *= clip_scale_a;
        }
        var norm_b: f64 = 0;
        for (grad_b) |g| norm_b += @as(f64, g) * @as(f64, g);
        norm_b = @sqrt(norm_b);
        if (norm_b > @as(f64, max_grad_norm)) {
            const clip_scale_b: f32 = @floatCast(@as(f64, max_grad_norm) / norm_b);
            for (grad_b) |*g| g.* *= clip_scale_b;
        }
    }

    if (adam_state) |s| {
        s.step += 1;
        const beta2: f32 = 0.999;
        const epsilon: f32 = 1e-8;
        const weight_decay: f32 = 0.01;
        if (use_schedule_free) {
            // Schedule-free AdamW (Defazio et al. 2024).
            // x (pair.adapter_*) is the Polyak-averaged iterate; z_* is the base iterate.
            // On first step, initialise z from x so the base iterate starts at the same point.
            const beta1: f32 = 0.9;
            const bc2 = 1.0 - std.math.pow(f32, beta2, @as(f32, @floatFromInt(s.step)));
            const c = @min(beta1, 1.0 / @as(f32, @floatFromInt(s.step)));
            if (s.step == 1) {
                @memcpy(s.z_a, pair.adapter_a);
                @memcpy(s.z_b, pair.adapter_b);
            }
            for (pair.adapter_a, grad_a, s.v_a, s.z_a) |*x, g, *v, *z| {
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                z.* = z.* - learning_rate * g / (@sqrt(v.* / bc2) + epsilon) - learning_rate * weight_decay * z.*;
                x.* = (1.0 - c) * x.* + c * z.*;
            }
            for (pair.adapter_b, grad_b, s.v_b, s.z_b) |*x, g, *v, *z| {
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                z.* = z.* - learning_rate * g / (@sqrt(v.* / bc2) + epsilon) - learning_rate * weight_decay * z.*;
                x.* = (1.0 - c) * x.* + c * z.*;
            }
        } else {
            const beta1: f32 = 0.9;
            const t: f32 = @floatFromInt(s.step);
            const bc1 = 1.0 - std.math.pow(f32, beta1, t);
            const bc2 = 1.0 - std.math.pow(f32, beta2, t);
            for (pair.adapter_a, grad_a, s.m_a, s.v_a) |*p, g, *m, *v| {
                m.* = beta1 * m.* + (1.0 - beta1) * g;
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                p.* -= learning_rate * (m.* / bc1 / (@sqrt(v.* / bc2) + epsilon) + weight_decay * p.*);
            }
            for (pair.adapter_b, grad_b, s.m_b, s.v_b) |*p, g, *m, *v| {
                m.* = beta1 * m.* + (1.0 - beta1) * g;
                v.* = beta2 * v.* + (1.0 - beta2) * g * g;
                p.* -= learning_rate * (m.* / bc1 / (@sqrt(v.* / bc2) + epsilon) + weight_decay * p.*);
            }
        }
    } else {
        for (pair.adapter_a, grad_a) |*value, grad| value.* -= learning_rate * grad;
        for (pair.adapter_b, grad_b) |*value, grad| value.* -= learning_rate * grad;
    }
}

fn backwardLinearRowsInputWithLoRA(
    allocator: std.mem.Allocator,
    grad_out: []const f32,
    rows: usize,
    out_dim: usize,
    weight: []const f32,
    in_dim: usize,
    pair: ?ExactLoRAPairRef,
    lora_alpha: f32,
    grad_input: []f32,
) void {
    _ = allocator;
    @memset(grad_input, 0);
    for (0..rows) |r| {
        const grad_row = grad_out[r * out_dim ..][0..out_dim];
        const grad_in_row = grad_input[r * in_dim ..][0..in_dim];
        for (0..in_dim) |i| {
            const w_row = weight[i * out_dim ..][0..out_dim];
            var sum: f32 = 0;
            for (0..out_dim) |j| sum += grad_row[j] * w_row[j];
            grad_in_row[i] += sum;
        }
        if (pair) |lora_pair| {
            var tmp_rank = std.heap.stackFallback(4096, std.heap.page_allocator);
            const alloc = tmp_rank.get();
            const tmp = alloc.alloc(f32, lora_pair.rank) catch continue;
            defer alloc.free(tmp);
            const scale = lora_alpha / @as(f32, @floatFromInt(lora_pair.rank));
            @memset(tmp, 0);
            for (0..lora_pair.rank) |rr| {
                const b_row = lora_pair.adapter_b[rr * out_dim ..][0..out_dim];
                var sum: f32 = 0;
                for (0..out_dim) |j| sum += grad_row[j] * b_row[j];
                tmp[rr] = (scale) * sum;
            }
            for (0..in_dim) |i| {
                const a_row = lora_pair.adapter_a[i * lora_pair.rank ..][0..lora_pair.rank];
                var sum: f32 = 0;
                for (0..lora_pair.rank) |rr| sum += tmp[rr] * a_row[rr];
                grad_in_row[i] += sum;
            }
        }
    }
}

fn backwardLinearRowsInput(
    grad_out: []const f32,
    rows: usize,
    out_dim: usize,
    weight: []const f32,
    in_dim: usize,
    grad_input: []f32,
) void {
    @memset(grad_input, 0);
    for (0..rows) |r| {
        const grad_row = grad_out[r * out_dim ..][0..out_dim];
        const grad_in_row = grad_input[r * in_dim ..][0..in_dim];
        for (0..in_dim) |i| {
            const w_row = weight[i * out_dim ..][0..out_dim];
            var sum: f32 = 0;
            for (0..out_dim) |j| sum += grad_row[j] * w_row[j];
            grad_in_row[i] += sum;
        }
    }
}

fn backwardLayerNorm(
    grad_out: []const f32,
    input: []const f32,
    rows: usize,
    width: usize,
    gamma: []const f32,
    eps: f32,
    grad_input: []f32,
) void {
    for (0..rows) |r| {
        const in_row = input[r * width ..][0..width];
        const grad_out_row = grad_out[r * width ..][0..width];
        const grad_in_row = grad_input[r * width ..][0..width];
        var mean: f32 = 0;
        for (in_row) |v| mean += v;
        mean /= @as(f32, @floatFromInt(width));
        var var_sum: f32 = 0;
        for (in_row) |v| {
            const diff = v - mean;
            var_sum += diff * diff;
        }
        const inv_std = 1.0 / @sqrt(var_sum / @as(f32, @floatFromInt(width)) + eps);
        var sum_gx: f32 = 0;
        var sum_gx_xhat: f32 = 0;
        for (0..width) |i| {
            const xhat = (in_row[i] - mean) * inv_std;
            const gx = grad_out_row[i] * gamma[i];
            sum_gx += gx;
            sum_gx_xhat += gx * xhat;
        }
        const width_f = @as(f32, @floatFromInt(width));
        for (0..width) |i| {
            const xhat = (in_row[i] - mean) * inv_std;
            const gx = grad_out_row[i] * gamma[i];
            grad_in_row[i] = (inv_std / width_f) * (width_f * gx - sum_gx - xhat * sum_gx_xhat);
        }
    }
}

fn backwardSoftmax(probs: []const f32, grad_probs: []const f32, grad_scores: []f32) void {
    var dot: f32 = 0;
    for (probs, grad_probs) |p, g| dot += p * g;
    for (probs, grad_probs, 0..) |p, g, i| grad_scores[i] = p * (g - dot);
}

fn geluDerivative(x: f32) f32 {
    const c: f32 = @sqrt(2.0 / std.math.pi);
    const x2 = x * x;
    const inner = c * (x + 0.044715 * x * x2);
    const tanh_inner = std.math.tanh(inner);
    const sech2 = 1.0 - tanh_inner * tanh_inner;
    return 0.5 * (1.0 + tanh_inner) + 0.5 * x * sech2 * c * (1.0 + 3.0 * 0.044715 * x2);
}

fn addInPlace(dst: []f32, src: []const f32) void {
    for (dst, src) |*d, s| d.* += s;
}

fn cachedExamplesFromSummary(
    allocator: std.mem.Allocator,
    summary: *const reranker_head.CachedPooledSummary,
) ![]reranker_head.CachedPooledExample {
    const cached = try allocator.alloc(reranker_head.CachedPooledExample, summary.examples.len);
    var built: usize = 0;
    errdefer {
        for (cached[0..built]) |*entry| entry.deinit(allocator);
        allocator.free(cached);
    }
    for (summary.examples, 0..) |entry, idx| {
        cached[idx] = .{
            .pooled = try allocator.dupe(f32, entry.pooled),
            .score = entry.score,
        };
        built += 1;
    }
    return cached;
}

fn scoreAdaptedExample(
    layer: *const LoadedLoRALayer,
    lora_alpha: f32,
    pooled: []const f32,
    head: *const reranker_head.RerankerHead,
) f64 {
    var transformed = std.heap.stackFallback(8192, std.heap.page_allocator);
    const alloc = transformed.get();
    const output = alloc.alloc(f32, layer.output_dim) catch return reranker_head.scoreHead(head, pooled);
    defer alloc.free(output);
    computeLinearOutput(output, pooled, layer.base_weight, layer.input_dim, layer.output_dim);
    applyAdapterDelta(output, pooled, layer.adapter_a, layer.adapter_b, layer.input_dim, layer.output_dim, layer.rank, lora_alpha);
    return reranker_head.scoreHead(head, output);
}

fn computeLinearOutput(dst: []f32, input: []const f32, weight: []const f32, input_dim: usize, output_dim: usize) void {
    @memset(dst, 0);
    for (0..input_dim) |i| {
        const x = input[i];
        const row = weight[i * output_dim .. (i + 1) * output_dim];
        for (row, 0..) |w, j| dst[j] += x * w;
    }
}

fn applyAdapterDelta(
    dst: []f32,
    input: []const f32,
    adapter_a: []const f32,
    adapter_b: []const f32,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    alpha: f32,
) void {
    const scale = alpha / @as(f32, @floatFromInt(rank));
    var low_rank = std.heap.stackFallback(4096, std.heap.page_allocator);
    const alloc = low_rank.get();
    const tmp = alloc.alloc(f32, rank) catch return;
    defer alloc.free(tmp);
    @memset(tmp, 0);
    for (0..input_dim) |i| {
        const x = input[i];
        const row = adapter_a[i * rank .. (i + 1) * rank];
        for (row, 0..) |a, r| tmp[r] += x * a;
    }
    for (0..rank) |r| {
        const row = adapter_b[r * output_dim .. (r + 1) * output_dim];
        const m = tmp[r] * scale;
        for (row, 0..) |b, j| dst[j] += m * b;
    }
}

fn selectTrainLayer(layers: []const LoadedLoRALayer, layer_name: ?[]const u8) ?usize {
    if (layer_name) |needle| {
        for (layers, 0..) |layer, idx| {
            if (std.mem.eql(u8, layer.base_tensor_name, needle)) return idx;
        }
        return null;
    }
    if (layers.len == 0) return null;
    return layers.len - 1;
}

fn resolveLayerSelection(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle: *const LoadedLoRABundle,
    top_layer_summary: ?*const reranker_head.CachedTopLayerSummary,
    layer_name: ?[]const u8,
) !LayerSelection {
    const layer_idx = selectTrainLayer(bundle.layers, layer_name) orelse return error.UnknownLoRALayer;
    if (layer_name != null or top_layer_summary == null) {
        return .{
            .layer_idx = layer_idx,
            .selected_layer_name = try allocator.dupe(u8, bundle.layers[layer_idx].base_tensor_name),
            .selected_layer_count = 1,
            .mode = .single,
        };
    }

    var encoder = try reranker_head.openEncoder(allocator, model_dir, .native);
    defer encoder.deinit();
    const cfg = switch (encoder.arch_config) {
        .bert => |value| value,
        else => {
            return .{
                .layer_idx = layer_idx,
                .selected_layer_name = try allocator.dupe(u8, bundle.layers[layer_idx].base_tensor_name),
                .selected_layer_count = 1,
                .mode = .single,
            };
        },
    };
    const top_layer_count = top_layer_summary.?.top_layer_count;
    const top_start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
    const selected_layer_count = countDistinctAdapterLayersInRange(bundle.layers, top_start, cfg.num_hidden_layers);
    if (selected_layer_count <= 1) {
        return .{
            .layer_idx = layer_idx,
            .selected_layer_name = try allocator.dupe(u8, bundle.layers[layer_idx].base_tensor_name),
            .selected_layer_count = 1,
            .mode = .single,
        };
    }
    return .{
        .layer_idx = layer_idx,
        .selected_layer_name = try allocator.dupe(u8, "<all_replayed_bert_layers>"),
        .selected_layer_count = selected_layer_count,
        .mode = .replayed_block,
    };
}

fn countDistinctAdapterLayersInRange(layers: []const LoadedLoRALayer, start_layer_idx: usize, end_layer_exclusive: usize) usize {
    var count: usize = 0;
    var seen: [256]bool = @as([256]bool, @splat(false));
    for (layers) |layer| {
        const layer_idx = parseEncoderLayerIndex(layer.base_tensor_name) orelse continue;
        if (layer_idx < start_layer_idx or layer_idx >= end_layer_exclusive) continue;
        if (layer_idx >= seen.len) continue;
        if (!seen[layer_idx]) {
            seen[layer_idx] = true;
            count += 1;
        }
    }
    return count;
}

fn moduleNameForTensor(tensor_name: []const u8) ?[]const u8 {
    const normalized = if (std.mem.endsWith(u8, tensor_name, ".weight"))
        tensor_name[0 .. tensor_name.len - ".weight".len]
    else
        tensor_name;
    const ordered_modules = [_][]const u8{
        "attention.output.dense",
        "query",
        "key",
        "value",
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
    const parsed = lora.parseLayerTensorName(tensor_name) orelse return null;
    const module = moduleNameForTensor(parsed.layer_name) orelse return null;
    return .{
        .base_tensor_base_name = parsed.layer_name,
        .module_name = module,
        .kind = switch (parsed.kind) {
            .a => .a,
            .b => .b,
        },
    };
}

fn parseEncoderLayerIndex(tensor_name: []const u8) ?usize {
    const candidates = [_][]const u8{
        "encoder.layer.",
        "roberta.encoder.layer.",
        "bert.encoder.layer.",
    };
    inline for (candidates) |prefix| {
        if (std.mem.indexOf(u8, tensor_name, prefix)) |start| {
            const digits = tensor_name[start + prefix.len ..];
            var end: usize = 0;
            while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
            if (end == 0) return null;
            return std.fmt.parseUnsigned(usize, digits[0..end], 10) catch null;
        }
    }
    return null;
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

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

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    target_modules: []const []const u8,
    top_layer_count: usize,
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
        .top_layer_count = top_layer_count,
        .use_dora = use_dora,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
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
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    return tensor_name;
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
    errdefer for (out[0..built]) |item| allocator.free(item);
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

fn stringSliceContains(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
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

test "reranker lora bootstrap inspect load save materialize" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_reranker_lora_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const config_path = try std.fs.path.join(allocator, &.{ root, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = "{\"model_type\":\"xlm-roberta\",\"hidden_size\":8,\"num_hidden_layers\":2,\"num_attention_heads\":2}" });
    const tokenizer_path = try std.fs.path.join(allocator, &.{ root, tokenizer_file_name });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_path, .data = "{}" });

    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "roberta.encoder.layer.0.attention.self.query.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.0.attention.self.key.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.0.attention.self.value.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.1.attention.self.query.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.1.attention.self.key.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.1.attention.self.value.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
        .{ .name = "roberta.encoder.layer.1.attention.output.dense.weight", .shape = &.{ 8, 8 }, .data = &@as([64]f32, @splat(0)) },
    });

    const adapter_dir = try std.fs.path.join(allocator, &.{ root, "adapter" });
    defer allocator.free(adapter_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, adapter_dir, .{ .rank = 4, .alpha = 8, .top_layer_count = 1 });
    defer freeBootstrapSummary(allocator, &bootstrap);
    try std.testing.expectEqual(@as(usize, 4), bootstrap.resolved_tensors.len);

    var inspect = try inspectLoRABundle(allocator, root, adapter_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspect);
    try std.testing.expectEqual(@as(usize, 4), inspect.resolved_tensor_count);
    try std.testing.expectEqual(@as(?usize, 1), inspect.top_layer_count);

    var bundle = try loadLoRABundle(allocator, root, adapter_dir);
    defer bundle.deinit();
    try saveLoRABundle(&bundle, adapter_dir);

    const merged_dir = try std.fs.path.join(allocator, &.{ root, "merged" });
    defer allocator.free(merged_dir);
    var materialized = try materializeMergedModel(allocator, root, adapter_dir, merged_dir);
    defer freeMaterializeSummary(allocator, &materialized);
    try std.testing.expectEqual(@as(usize, 4), materialized.merged_lora_tensor_count);
}
