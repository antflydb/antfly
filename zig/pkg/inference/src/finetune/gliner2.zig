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
const builtin = @import("builtin");
const tensor_mod = @import("../backends/tensor.zig");
const Tensor = tensor_mod.Tensor;
const DType = tensor_mod.DType;
const c_file = @import("../util/c_file.zig");
const compat = @import("../io/compat.zig");
const lora = @import("lora.zig");
const gliner2_boundary = @import("gliner2_boundary.zig");
const entity_cleanup_model = @import("entity_cleanup_model.zig");
const safetensors = @import("../models/safetensors.zig");
const tensor_access = @import("../models/tensor_access.zig");
const weight_source = @import("../models/weight_source.zig");

pub const artifact_family_version = "gliner2_lora/v1";
pub const legacy_artifact_family_version = "gliner2_lora/v1alpha1";
pub const checkpoint_file_name = "model.safetensors";
pub const config_file_name = "config.json";
pub const encoder_config_file_name = "encoder_config/config.json";
pub const adapter_checkpoint_file_name = "adapter_model.safetensors";
pub const adapter_config_file_name = "adapter_config.json";
pub const task_head_checkpoint_file_name = "task_head.safetensors";
pub const tokenizer_file_name = "tokenizer.json";
pub const tokenizer_config_file_name = "tokenizer_config.json";
pub const special_tokens_map_file_name = "special_tokens_map.json";
pub const added_tokens_file_name = "added_tokens.json";
pub const sentencepiece_model_file_name = "spm.model";
pub const materialization_manifest_file_name = "materialization_manifest.json";
pub const materialization_schema_version = "gliner2_materialized_model/v1";

pub fn isSupportedArtifactFamilyVersion(version: []const u8) bool {
    return std.mem.eql(u8, version, artifact_family_version) or
        std.mem.eql(u8, version, legacy_artifact_family_version);
}

pub const default_lora_target_modules = [_][]const u8{
    "encoder",
    "span_rep",
    "classifier",
    "count_embed",
    "count_pred",
};

pub const default_lora_dropout: f32 = 0.0;

pub const expanded_encoder_lora_target_modules = [_][]const u8{
    "query_proj",
    "key_proj",
    "value_proj",
    "attention.output.dense",
    "intermediate.dense",
    "output.dense",
};

pub const BackboneConfig = struct {
    allocator: std.mem.Allocator,
    model_name: []const u8,
    model_type: []const u8,
    counting_layer: []const u8,
    token_pooling: []const u8,
    max_width: usize,
    vocab_size: usize,
    hidden_size: usize,
    num_hidden_layers: usize,
    num_attention_heads: usize,
    intermediate_size: usize,
    max_position_embeddings: usize,
    type_vocab_size: usize,
    position_buckets: usize,
    relative_attention: bool,
    hidden_dropout_prob: f32,
    attention_probs_dropout_prob: f32,
    layer_norm_eps: f32,
    count_embed_dim: usize,
    count_embed_layers: usize,
    count_embed_heads: usize,
    count_embed_ffn: usize,
    max_count_embed: usize,

    pub fn deinit(self: *BackboneConfig) void {
        self.allocator.free(self.model_name);
        self.allocator.free(self.model_type);
        self.allocator.free(self.counting_layer);
        self.allocator.free(self.token_pooling);
        self.* = undefined;
    }
};

pub const ArtifactPaths = struct {
    allocator: std.mem.Allocator,
    model_dir: []u8,
    checkpoint_path: []u8,
    config_path: []u8,
    encoder_config_path: []u8,

    pub fn deinit(self: *ArtifactPaths) void {
        self.allocator.free(self.model_dir);
        self.allocator.free(self.checkpoint_path);
        self.allocator.free(self.config_path);
        self.allocator.free(self.encoder_config_path);
        self.* = undefined;
    }
};

pub const CheckpointInspection = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    checkpoint_path: []const u8,
    config_path: []const u8,
    encoder_config_path: []const u8,
    model_name: []const u8,
    model_type: []const u8,
    counting_layer: []const u8,
    token_pooling: []const u8,
    max_width: usize,
    hidden_size: usize,
    num_hidden_layers: usize,
    num_attention_heads: usize,
    base_tensor_count: usize,
    base_total_params: u64,
    word_embeddings_found: bool,
    final_layernorm_found: bool,
    rel_embeddings_found: bool,
    query_proj_weights_found: usize,
    key_proj_weights_found: usize,
    value_proj_weights_found: usize,
    output_dense_weights_found: usize,
    span_rep_tensors_found: usize,
    count_embed_tensors_found: usize,
    core_backbone_loadable: bool,
    lora_present: bool,
    lora_tensor_count: usize,
    lora_pair_count: usize,
    lora_query_pairs: usize,
    lora_value_pairs: usize,
    task_head_passthrough_tensors: usize,
};

pub const AdapterConfig = struct {
    base_model_name_or_path: ?[]const u8 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    r: ?usize = null,
    lora_alpha: ?f64 = null,
    lora_dropout: ?f64 = null,
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
    dropout: f32 = default_lora_dropout,
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
    lora_dropout: f32,
    target_modules: []const []const u8,
    resolved_target_modules: []const []const u8,
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
    lora_dropout: ?f64 = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    resolved_target_module_count: usize = 0,
    resolved_target_modules: ?[]const []const u8 = null,
    resolved_tensor_count: usize = 0,
    trainable_parameter_count: usize = 0,
    dora_magnitude_tensor_count: usize = 0,
    dora_magnitude_parameter_count: usize = 0,
    task_head_passthrough_tensors: usize = 0,
    span_rep_passthrough_tensors: usize = 0,
    count_embed_passthrough_tensors: usize = 0,
    boundary_head_present: bool = false,
    boundary_task_head_present: bool = false,
    cleanup_head_present: bool = false,
    use_dora: ?bool = null,
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

pub const LoadedPassthroughTensor = struct {
    name: []const u8,
    tensor: Tensor,
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
    lora_dropout: f32 = default_lora_dropout,
    target_modules: []const []const u8,
    layers: []LoadedLoRALayer,
    passthrough_tensors: []LoadedPassthroughTensor,

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
            if (layer.dora_magnitude_tensor_name) |value| self.allocator.free(value);
            self.allocator.free(layer.module_name);
            self.allocator.free(layer.base_weight);
            self.allocator.free(layer.adapter_a);
            self.allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |value| self.allocator.free(value);
        }
        self.allocator.free(self.layers);
        for (self.passthrough_tensors) |item| {
            self.allocator.free(item.name);
            var tensor = item.tensor;
            tensor.deinit();
        }
        self.allocator.free(self.passthrough_tensors);
        self.* = undefined;
    }
};

pub fn validateLoRADropout(dropout: f32) !void {
    if (!std.math.isFinite(dropout) or dropout < 0.0 or dropout >= 1.0) return error.InvalidLoRADropout;
}

pub fn expandLoRATargetModules(
    allocator: std.mem.Allocator,
    requested_target_modules: []const []const u8,
) ![]const []const u8 {
    var expanded = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (expanded.items) |item| allocator.free(item);
        expanded.deinit(allocator);
    }

    for (requested_target_modules) |raw| {
        const target = std.mem.trim(u8, raw, " \t\r\n");
        if (target.len == 0) return error.InvalidLoRATargetModule;
        if (std.mem.eql(u8, target, "encoder")) {
            for (expanded_encoder_lora_target_modules) |item| try appendUniqueString(allocator, &expanded, item);
        } else if (std.mem.eql(u8, target, "encoder.query")) {
            try appendUniqueString(allocator, &expanded, "query_proj");
        } else if (std.mem.eql(u8, target, "encoder.key")) {
            try appendUniqueString(allocator, &expanded, "key_proj");
        } else if (std.mem.eql(u8, target, "encoder.value")) {
            try appendUniqueString(allocator, &expanded, "value_proj");
        } else if (std.mem.eql(u8, target, "encoder.dense")) {
            try appendUniqueString(allocator, &expanded, "attention.output.dense");
            try appendUniqueString(allocator, &expanded, "intermediate.dense");
            try appendUniqueString(allocator, &expanded, "output.dense");
        } else if (std.mem.eql(u8, target, "span_rep")) {
            try appendUniqueString(allocator, &expanded, "span_rep.span_rep_layer.project_start.0");
            try appendUniqueString(allocator, &expanded, "span_rep.span_rep_layer.project_start.3");
            try appendUniqueString(allocator, &expanded, "span_rep.span_rep_layer.project_end.0");
            try appendUniqueString(allocator, &expanded, "span_rep.span_rep_layer.project_end.3");
            try appendUniqueString(allocator, &expanded, "span_rep.span_rep_layer.out_project.0");
            try appendUniqueString(allocator, &expanded, "span_rep.span_rep_layer.out_project.3");
        } else if (std.mem.eql(u8, target, "classifier")) {
            try appendUniqueString(allocator, &expanded, "classifier.0");
            try appendUniqueString(allocator, &expanded, "classifier.2");
        } else if (std.mem.eql(u8, target, "count_embed")) {
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.in_projector");
            // Official GLiNER2 wraps this module, but PyTorch MultiheadAttention
            // reads out_proj.weight directly and does not call the LoRA forward.
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.transformer.layers.0.linear1");
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.transformer.layers.0.linear2");
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.transformer.layers.1.linear1");
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.transformer.layers.1.linear2");
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.out_projector.0");
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.out_projector.2");
            try appendUniqueString(allocator, &expanded, "count_embed.transformer.out_projector.4");
        } else if (std.mem.eql(u8, target, "count_pred")) {
            try appendUniqueString(allocator, &expanded, "count_pred.0");
            try appendUniqueString(allocator, &expanded, "count_pred.2");
        } else {
            try appendUniqueString(allocator, &expanded, target);
        }
    }

    return expanded.toOwnedSlice(allocator);
}

fn appendUniqueString(
    allocator: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (list.items) |item| {
        if (std.mem.eql(u8, item, value)) return;
    }
    try list.append(allocator, try allocator.dupe(u8, value));
}

pub const AutodiffAdapterParam = struct {
    name: []const u8,
    dims: []const i32,
    weights: []const f32,
};

pub const AutodiffAdapterExportSummary = struct {
    artifact_family_version: []const u8,
    output_dir: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: []const u8,
    exported_tensor_count: usize,
    lora_rank: usize,
    lora_alpha: f32,
    lora_dropout: f32,
    target_modules: []const []const u8,
    resolved_target_modules: []const []const u8,
};

pub const AutodiffRegularParamExportSummary = struct {
    artifact_family_version: []const u8,
    output_dir: []const u8,
    checkpoint_path: []const u8,
    exported_tensor_count: usize,
};

pub const MaterializeSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    output_checkpoint_path: []const u8,
    merged_lora_tensor_count: usize,
    merged_dora_tensor_count: usize = 0,
    task_head_passthrough_tensor_count: usize,
    attached_task_head_tensor_count: usize = 0,
    span_rep_passthrough_tensor_count: usize,
    count_embed_passthrough_tensor_count: usize,
    copied_boundary_head: bool,
    copied_boundary_task_head: bool,
    copied_cleanup_head: bool,
    copied_base_tensor_count: usize,
};

const MaterializationInventory = struct {
    config: bool,
    encoder_config: bool,
    tokenizer: bool,
    tokenizer_config: bool,
    special_tokens_map: bool,
    added_tokens: bool,
    sentencepiece_model: bool,
};

const MaterializationManifest = struct {
    schema_version: []const u8 = materialization_schema_version,
    status: []const u8 = "complete",
    artifact_family_version: []const u8,
    checkpoint_file: []const u8 = checkpoint_file_name,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    merged_lora_tensor_count: usize,
    merged_dora_tensor_count: usize,
    task_head_passthrough_tensor_count: usize,
    attached_task_head_tensor_count: usize,
    copied_boundary_head: bool,
    copied_boundary_task_head: bool,
    copied_cleanup_head: bool,
    supporting_artifacts: MaterializationInventory,
};

pub const ClassifierTaskHead = struct {
    allocator: std.mem.Allocator,
    weight: []f32,
    bias: []f32,
    num_classes: usize,
    hidden_size: usize,

    pub fn deinit(self: *ClassifierTaskHead) void {
        self.allocator.free(self.weight);
        self.allocator.free(self.bias);
        self.* = undefined;
    }

    pub fn scoreRowsAlloc(
        self: *const ClassifierTaskHead,
        allocator: std.mem.Allocator,
        hidden_rows: []const f32,
    ) ![]f32 {
        if (self.hidden_size == 0 or hidden_rows.len % self.hidden_size != 0) return error.InvalidTaskHeadInputShape;
        const row_count = hidden_rows.len / self.hidden_size;
        const logits = try allocator.alloc(f32, row_count * self.num_classes);
        for (0..row_count) |row_idx| {
            const hidden = hidden_rows[row_idx * self.hidden_size ..][0..self.hidden_size];
            for (0..self.num_classes) |class_idx| {
                const weights = self.weight[class_idx * self.hidden_size ..][0..self.hidden_size];
                var value = self.bias[class_idx];
                for (hidden, weights) |h, w| value += h * w;
                logits[row_idx * self.num_classes + class_idx] = value;
            }
        }
        return logits;
    }

    pub fn predictRowsAlloc(
        self: *const ClassifierTaskHead,
        allocator: std.mem.Allocator,
        hidden_rows: []const f32,
    ) ![]usize {
        const logits = try self.scoreRowsAlloc(allocator, hidden_rows);
        defer allocator.free(logits);
        const row_count = hidden_rows.len / self.hidden_size;
        const predictions = try allocator.alloc(usize, row_count);
        for (0..row_count) |row_idx| {
            const row = logits[row_idx * self.num_classes ..][0..self.num_classes];
            var best_idx: usize = 0;
            var best = row[0];
            for (row[1..], 1..) |value, class_idx| {
                if (value > best) {
                    best = value;
                    best_idx = class_idx;
                }
            }
            predictions[row_idx] = best_idx;
        }
        return predictions;
    }
};

pub fn resolveArtifactPaths(allocator: std.mem.Allocator, model_input: []const u8) !ArtifactPaths {
    const input_is_dir = isDirectoryPath(model_input);
    const model_dir = if (input_is_dir)
        try allocator.dupe(u8, model_input)
    else
        try allocator.dupe(u8, std.fs.path.dirname(model_input) orelse ".");
    errdefer allocator.free(model_dir);

    const checkpoint_path = if (input_is_dir)
        try std.fs.path.join(allocator, &.{ model_input, checkpoint_file_name })
    else
        try allocator.dupe(u8, model_input);
    errdefer allocator.free(checkpoint_path);

    const encoder_config_path = try std.fs.path.join(allocator, &.{ model_dir, encoder_config_file_name });
    errdefer allocator.free(encoder_config_path);
    const root_config_path = try std.fs.path.join(allocator, &.{ model_dir, config_file_name });
    const config_path = if (isRegularFilePath(root_config_path)) root_config_path else if (isRegularFilePath(encoder_config_path)) blk: {
        allocator.free(root_config_path);
        break :blk try allocator.dupe(u8, encoder_config_path);
    } else {
        allocator.free(root_config_path);
        return error.MissingModelConfig;
    };
    errdefer allocator.free(config_path);

    if (!isRegularFilePath(checkpoint_path)) return error.MissingModelCheckpoint;
    if (!isRegularFilePath(encoder_config_path)) return error.MissingEncoderConfig;

    return .{
        .allocator = allocator,
        .model_dir = model_dir,
        .checkpoint_path = checkpoint_path,
        .config_path = config_path,
        .encoder_config_path = encoder_config_path,
    };
}

pub fn loadBackboneConfig(allocator: std.mem.Allocator, model_input: []const u8) !BackboneConfig {
    var paths = try resolveArtifactPaths(allocator, model_input);
    defer paths.deinit();
    return try loadBackboneConfigFromPaths(allocator, &paths);
}

pub fn resolveLoRACheckpointPath(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    if (isRegularFilePath(input)) return try allocator.dupe(u8, input);
    const candidates = [_][]const u8{
        "lora_weights.safetensors",
        adapter_checkpoint_file_name,
    };
    for (candidates) |name| {
        const path = try std.fs.path.join(allocator, &.{ input, name });
        defer allocator.free(path);
        if (isRegularFilePath(path)) return try allocator.dupe(u8, path);
    }
    return error.MissingLoRACheckpoint;
}

pub fn inspectCheckpoint(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    maybe_lora_input: ?[]const u8,
) !CheckpointInspection {
    var paths = try resolveArtifactPaths(allocator, model_input);
    defer paths.deinit();
    var config = try loadBackboneConfigFromPaths(allocator, &paths);
    defer config.deinit();

    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, paths.checkpoint_path);
    defer reader.deinit();

    var summary = CheckpointInspection{
        .artifact_family_version = try inspectionArtifactFamilyVersion(allocator, paths.model_dir),
        .model_dir = try allocator.dupe(u8, paths.model_dir),
        .checkpoint_path = try allocator.dupe(u8, paths.checkpoint_path),
        .config_path = try allocator.dupe(u8, paths.config_path),
        .encoder_config_path = try allocator.dupe(u8, paths.encoder_config_path),
        .model_name = try allocator.dupe(u8, config.model_name),
        .model_type = try allocator.dupe(u8, config.model_type),
        .counting_layer = try allocator.dupe(u8, config.counting_layer),
        .token_pooling = try allocator.dupe(u8, config.token_pooling),
        .max_width = config.max_width,
        .hidden_size = config.hidden_size,
        .num_hidden_layers = config.num_hidden_layers,
        .num_attention_heads = config.num_attention_heads,
        .base_tensor_count = reader.header.tensors.count(),
        .base_total_params = 0,
        .word_embeddings_found = false,
        .final_layernorm_found = false,
        .rel_embeddings_found = false,
        .query_proj_weights_found = 0,
        .key_proj_weights_found = 0,
        .value_proj_weights_found = 0,
        .output_dense_weights_found = 0,
        .span_rep_tensors_found = 0,
        .count_embed_tensors_found = 0,
        .core_backbone_loadable = false,
        .lora_present = false,
        .lora_tensor_count = 0,
        .lora_pair_count = 0,
        .lora_query_pairs = 0,
        .lora_value_pairs = 0,
        .task_head_passthrough_tensors = 0,
    };
    errdefer freeCheckpointInspection(allocator, &summary);

    var it = reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const info = entry.value_ptr.*;
        summary.base_total_params += tensorInfoElementCount(info);
        if (isWordEmbeddingsName(name)) summary.word_embeddings_found = true;
        if (isFinalLayerNormName(name)) summary.final_layernorm_found = true;
        if (isRelativeEmbeddingsName(name)) summary.rel_embeddings_found = true;
        if (isQueryProjWeightName(name)) summary.query_proj_weights_found += 1;
        if (isKeyProjWeightName(name)) summary.key_proj_weights_found += 1;
        if (isValueProjWeightName(name)) summary.value_proj_weights_found += 1;
        if (isOutputDenseWeightName(name)) summary.output_dense_weights_found += 1;
        if (isSpanRepName(name)) summary.span_rep_tensors_found += 1;
        if (isCountEmbedName(name)) summary.count_embed_tensors_found += 1;
    }
    summary.core_backbone_loadable = summary.word_embeddings_found and
        summary.final_layernorm_found and
        summary.rel_embeddings_found and
        summary.query_proj_weights_found >= config.num_hidden_layers and
        summary.key_proj_weights_found >= config.num_hidden_layers and
        summary.value_proj_weights_found >= config.num_hidden_layers;

    if (maybe_lora_input) |lora_input| {
        const lora_checkpoint = try resolveLoRACheckpointPath(allocator, lora_input);
        defer allocator.free(lora_checkpoint);
        var lora_reader = try safetensors.MMapReader.openFileAbsolute(allocator, lora_checkpoint);
        defer lora_reader.deinit();
        summary.lora_present = true;
        summary.lora_tensor_count = lora_reader.header.tensors.count();
        var lora_it = lora_reader.header.tensors.iterator();
        while (lora_it.next()) |entry| {
            const name = entry.key_ptr.*;
            const parsed = parseLoRAAdapterTensorName(name) orelse {
                if (isDoRAMagnitudeTensorName(name)) continue;
                summary.task_head_passthrough_tensors += 1;
                continue;
            };
            if (parsed.kind == .a) {
                summary.lora_pair_count += 1;
                if (std.mem.eql(u8, parsed.module_name, "query_proj")) summary.lora_query_pairs += 1;
                if (std.mem.eql(u8, parsed.module_name, "value_proj")) summary.lora_value_pairs += 1;
            }
        }
    }

    return summary;
}

pub fn bootstrapLoRABundle(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    out_dir: []const u8,
    options: BootstrapOptions,
) !BootstrapSummary {
    var inspect = try inspectCheckpoint(allocator, model_input, null);
    defer freeCheckpointInspection(allocator, &inspect);

    if (options.rank == 0) return error.InvalidLoRARank;
    try validateLoRADropout(options.dropout);
    const requested_target_modules = options.target_modules orelse default_lora_target_modules[0..];
    const resolved_target_modules = try expandLoRATargetModules(allocator, requested_target_modules);
    errdefer {
        for (resolved_target_modules) |item| allocator.free(item);
        allocator.free(resolved_target_modules);
    }
    const resolved_tensors = try inferLoRATargetTensors(allocator, inspect.checkpoint_path, resolved_target_modules);
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
        try allocator.dupe(u8, inspect.model_dir);
    errdefer allocator.free(base_model_name_or_path);

    try writeBootstrapAdapterCheckpoint(allocator, adapter_checkpoint_path, resolved_tensors, options.rank);
    try writeAdapterConfigJson(allocator, adapter_config_path, base_model_name_or_path, options.rank, options.alpha, options.dropout, requested_target_modules, false);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, inspect.model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .checkpoint_path = try allocator.dupe(u8, inspect.checkpoint_path),
        .adapter_checkpoint_path = adapter_checkpoint_path,
        .adapter_config_path = adapter_config_path,
        .base_model_name_or_path = base_model_name_or_path,
        .lora_rank = options.rank,
        .lora_alpha = options.alpha,
        .lora_dropout = options.dropout,
        .target_modules = try dupeStringSlice(allocator, requested_target_modules),
        .resolved_target_modules = resolved_target_modules,
        .resolved_tensors = resolved_tensors,
    };
}

pub fn inspectLoRABundle(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
) !LoRABundleInspectionSummary {
    var base_inspect = try inspectCheckpoint(allocator, base_model_input, null);
    defer freeCheckpointInspection(allocator, &base_inspect);
    var adapter_config = try inspectAdapterConfig(allocator, adapter_model_input);
    defer freeAdapterInspection(allocator, &adapter_config);

    var base_reader = try safetensors.MMapReader.openFileAbsolute(allocator, base_inspect.checkpoint_path);
    defer base_reader.deinit();
    var adapter_reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_config.adapter_checkpoint_path);
    defer adapter_reader.deinit();

    var tensors: std.ArrayListUnmanaged(LoRATensorSummary) = .empty;
    errdefer {
        for (tensors.items) |*item| freeLoRATensorSummary(allocator, item);
        tensors.deinit(allocator);
    }
    var task_head_passthrough_tensors: usize = 0;
    var span_rep_passthrough_tensors: usize = 0;
    var count_embed_passthrough_tensors: usize = 0;

    var it = adapter_reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const adapter_a_name = entry.key_ptr.*;
        const parsed = parseLoRAAdapterTensorName(adapter_a_name) orelse {
            if (isTaskHeadPassthroughName(adapter_a_name)) {
                task_head_passthrough_tensors += 1;
                if (isSpanRepName(adapter_a_name)) span_rep_passthrough_tensors += 1;
                if (isCountEmbedName(adapter_a_name)) count_embed_passthrough_tensors += 1;
            }
            continue;
        };
        if (parsed.kind != .a) continue;
        const adapter_a_info = entry.value_ptr.*;
        const adapter_b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{parsed.adapter_tensor_base_name});
        defer allocator.free(adapter_b_name);
        const base_tensor_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(base_tensor_name);
        const adapter_b_info = adapter_reader.header.tensors.get(adapter_b_name) orelse return error.MissingAdapterPair;
        if (adapter_a_info.shape.len != 2 or adapter_b_info.shape.len != 2) return error.InvalidAdapterTensorShape;
        const base_output_dim, const base_input_dim = if (isGraphOwnedLoRABaseModule(parsed.module_name)) blk: {
            break :blk .{ adapter_b_info.shape[0], adapter_a_info.shape[1] };
        } else if (base_reader.header.tensors.get(base_tensor_name)) |base_info| blk: {
            if (base_info.shape.len != 2) return error.InvalidAdapterTensorShape;
            break :blk .{ base_info.shape[0], base_info.shape[1] };
        } else return error.MissingBaseTensorForAdapter;
        if (adapter_a_info.shape[1] != base_input_dim) return error.AdapterInputDimMismatch;
        if (adapter_b_info.shape[0] != base_output_dim) return error.AdapterOutputDimMismatch;
        if (adapter_a_info.shape[0] != adapter_b_info.shape[1]) return error.AdapterRankMismatch;

        const maybe_dora_name = try std.fmt.allocPrint(allocator, "{s}.lora_magnitude_vector.weight", .{parsed.adapter_tensor_base_name});
        defer allocator.free(maybe_dora_name);
        var dora_name_for_summary: ?[]const u8 = null;
        var dora_parameter_count: usize = 0;
        if (adapter_reader.header.tensors.get(maybe_dora_name)) |dora_info| {
            if (dora_info.shape.len != 1) return error.InvalidAdapterTensorShape;
            if (dora_info.shape[0] != base_output_dim) return error.AdapterOutputDimMismatch;
            dora_name_for_summary = try allocator.dupe(u8, maybe_dora_name);
            dora_parameter_count = @intCast(dora_info.shape[0]);
        }

        try tensors.append(allocator, .{
            .base_tensor_name = try allocator.dupe(u8, base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, adapter_a_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, adapter_b_name),
            .dora_magnitude_tensor_name = dora_name_for_summary,
            .module_name = try allocator.dupe(u8, parsed.module_name),
            .input_dim = @intCast(base_input_dim),
            .output_dim = @intCast(base_output_dim),
            .rank = @intCast(adapter_a_info.shape[0]),
            .adapter_parameter_count = @as(usize, @intCast(adapter_a_info.shape[0])) * @as(usize, @intCast(adapter_a_info.shape[1])) +
                @as(usize, @intCast(adapter_b_info.shape[0])) * @as(usize, @intCast(adapter_b_info.shape[1])) +
                dora_parameter_count,
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
    const boundary_head_path = try std.fs.path.join(allocator, &.{ adapter_model_input, gliner2_boundary.boundary_head_file_name });
    defer allocator.free(boundary_head_path);
    const boundary_task_head_path = try std.fs.path.join(allocator, &.{ adapter_model_input, gliner2_boundary.boundary_task_head_file_name });
    defer allocator.free(boundary_task_head_path);
    const cleanup_head_path = try std.fs.path.join(allocator, &.{ adapter_model_input, entity_cleanup_model.head_file_name });
    defer allocator.free(cleanup_head_path);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, base_inspect.model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_config.model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, base_inspect.checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, adapter_config.adapter_checkpoint_path),
        .adapter_config_path = try dupeOptionalString(allocator, adapter_config.adapter_config_path),
        .base_model_name_or_path = try dupeOptionalString(allocator, adapter_config.base_model_name_or_path),
        .lora_rank = adapter_config.lora_rank,
        .lora_alpha = adapter_config.lora_alpha,
        .lora_dropout = adapter_config.lora_dropout,
        .target_module_count = adapter_config.target_module_count,
        .target_modules = try dupeOptionalStringSlice(allocator, adapter_config.target_modules),
        .resolved_target_module_count = adapter_config.resolved_target_module_count,
        .resolved_target_modules = try dupeOptionalStringSlice(allocator, adapter_config.resolved_target_modules),
        .resolved_tensor_count = tensors.items.len,
        .trainable_parameter_count = trainable_parameter_count,
        .dora_magnitude_tensor_count = dora_magnitude_tensor_count,
        .dora_magnitude_parameter_count = dora_magnitude_parameter_count,
        .task_head_passthrough_tensors = task_head_passthrough_tensors,
        .span_rep_passthrough_tensors = span_rep_passthrough_tensors,
        .count_embed_passthrough_tensors = count_embed_passthrough_tensors,
        .boundary_head_present = isRegularFilePath(boundary_head_path),
        .boundary_task_head_present = isRegularFilePath(boundary_task_head_path),
        .cleanup_head_present = isRegularFilePath(cleanup_head_path),
        .use_dora = adapter_config.use_dora,
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
            if (layer.dora_magnitude_tensor_name) |value| allocator.free(value);
            allocator.free(layer.module_name);
            allocator.free(layer.base_weight);
            allocator.free(layer.adapter_a);
            allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |value| allocator.free(value);
        }
        allocator.free(layers);
    }
    var passthrough_tensors = std.ArrayListUnmanaged(LoadedPassthroughTensor).empty;
    errdefer {
        for (passthrough_tensors.items) |item| {
            allocator.free(item.name);
            var tensor = item.tensor;
            tensor.deinit();
        }
        passthrough_tensors.deinit(allocator);
    }

    var base_access = try openTensorAccessForFile(allocator, inspected.base_checkpoint_path);
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
            if (magnitude_tensor.shape.len != 1 or magnitude_tensor.shape[0] != @as(i64, @intCast(tensor_summary.output_dim))) {
                return error.InvalidAdapterTensorShape;
            }
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
    const adapter_names = try adapter_access.listNames(allocator);
    defer allocator.free(adapter_names);
    for (adapter_names) |name| {
        if (parseLoRAAdapterTensorName(name) != null) continue;
        if (!isTaskHeadPassthroughName(name)) continue;
        var tensor = try loadTensorAsF32(allocator, adapter_access, name);
        errdefer tensor.deinit();
        try passthrough_tensors.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .tensor = tensor,
        });
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
        .lora_dropout = if (inspected.lora_dropout) |value| @floatCast(value) else 0.0,
        .target_modules = if (inspected.target_modules) |items|
            try dupeStringSlice(allocator, items)
        else
            try dupeStringSlice(allocator, default_lora_target_modules[0..]),
        .layers = layers,
        .passthrough_tensors = try passthrough_tensors.toOwnedSlice(allocator),
    };
}

pub fn saveLoRABundle(bundle: *const LoadedLoRABundle, out_dir: []const u8) !void {
    const allocator = bundle.allocator;
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    defer allocator.free(config_path);

    var tensors = try allocator.alloc(WriteTensorF32, bundle.layers.len * 3 + bundle.passthrough_tensors.len);
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
    for (bundle.passthrough_tensors) |item| {
        const data = try allocator.dupe(f32, item.tensor.asFloat32());
        try owned_data.append(allocator, data);
        const shape = try tensorShapeToOwnedUsize(allocator, item.tensor.shape);
        try owned_shapes.append(allocator, shape);
        tensors[tensor_idx] = .{ .name = item.name, .shape = shape, .data = data };
        tensor_idx += 1;
    }

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, tensors[0..tensor_idx]);
    try writeAdapterConfigJson(
        allocator,
        config_path,
        bundle.base_model_name_or_path orelse bundle.base_model_dir,
        bundle.lora_rank,
        bundle.lora_alpha,
        bundle.lora_dropout,
        bundle.target_modules,
        bundleHasDoRA(bundle),
    );
    try copySupportingArtifactIfPresent(allocator, bundle.base_model_dir, out_dir, config_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.base_model_dir, out_dir, encoder_config_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.adapter_model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.adapter_model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.adapter_model_dir, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.base_model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.base_model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, bundle.base_model_dir, out_dir, special_tokens_map_file_name);
}

pub fn exportAutodiffAdaptersAsPeftBundle(
    allocator: std.mem.Allocator,
    out_dir: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    dropout: f32,
    target_modules: []const []const u8,
    params: []const AutodiffAdapterParam,
) !AutodiffAdapterExportSummary {
    try validateLoRADropout(dropout);
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    errdefer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    errdefer allocator.free(config_path);

    var tensors = try allocator.alloc(WriteTensorF32, params.len);
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
    var peft_target_modules: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer {
        for (peft_target_modules.items) |item| allocator.free(item);
        peft_target_modules.deinit(allocator);
    }

    var tensor_count: usize = 0;
    for (params) |param| {
        const peft_name = try autodiffParamNameToPeftName(allocator, param.name);
        errdefer allocator.free(peft_name);
        const module_name = peftModuleNameForAdapterTensor(peft_name) orelse return error.InvalidAutodiffAdapterName;
        try appendUniqueString(allocator, &peft_target_modules, module_name);
        const shape = try i32DimsToUsize(allocator, param.dims);
        errdefer allocator.free(shape);
        if (elementCount(shape) != param.weights.len) return error.AdapterTensorShapeMismatch;

        try owned_names.append(allocator, peft_name);
        try owned_shapes.append(allocator, shape);
        tensors[tensor_count] = .{
            .name = peft_name,
            .shape = shape,
            .data = param.weights,
        };
        tensor_count += 1;
    }
    if (peft_target_modules.items.len == 0) return error.NoLoRATargetTensorsResolved;
    const resolved_target_modules = try peft_target_modules.toOwnedSlice(allocator);
    errdefer {
        for (resolved_target_modules) |item| allocator.free(item);
        allocator.free(resolved_target_modules);
    }

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, tensors[0..tensor_count]);
    try writeAdapterConfigJson(allocator, config_path, base_model_name_or_path, rank, alpha, dropout, resolved_target_modules, false);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .output_dir = try allocator.dupe(u8, out_dir),
        .adapter_checkpoint_path = checkpoint_path,
        .adapter_config_path = config_path,
        .exported_tensor_count = tensor_count,
        .lora_rank = rank,
        .lora_alpha = alpha,
        .lora_dropout = dropout,
        .target_modules = try dupeStringSlice(allocator, target_modules),
        .resolved_target_modules = resolved_target_modules,
    };
}

pub fn exportAutodiffRegularParamsAsSafetensors(
    allocator: std.mem.Allocator,
    out_dir: []const u8,
    params: []const AutodiffAdapterParam,
) !AutodiffRegularParamExportSummary {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, task_head_checkpoint_file_name });
    errdefer allocator.free(checkpoint_path);

    var tensors = try allocator.alloc(WriteTensorF32, params.len);
    defer allocator.free(tensors);
    var owned_shapes: std.ArrayListUnmanaged([]const usize) = .empty;
    defer {
        for (owned_shapes.items) |item| allocator.free(item);
        owned_shapes.deinit(allocator);
    }

    for (params, 0..) |param, idx| {
        const shape = try i32DimsToUsize(allocator, param.dims);
        errdefer allocator.free(shape);
        if (elementCount(shape) != param.weights.len) return error.AdapterTensorShapeMismatch;
        try owned_shapes.append(allocator, shape);
        tensors[idx] = .{
            .name = param.name,
            .shape = shape,
            .data = param.weights,
        };
    }

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, tensors);
    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .output_dir = try allocator.dupe(u8, out_dir),
        .checkpoint_path = checkpoint_path,
        .exported_tensor_count = params.len,
    };
}

pub fn loadClassifierTaskHead(
    allocator: std.mem.Allocator,
    checkpoint_path: []const u8,
) !ClassifierTaskHead {
    var access = try openTensorAccessForFile(allocator, checkpoint_path);
    defer access.deinit();

    var weight_tensor = try loadTensorAsF32(allocator, access, "classifier.weight");
    defer weight_tensor.deinit();
    var bias_tensor = try loadTensorAsF32(allocator, access, "classifier.bias");
    defer bias_tensor.deinit();

    if (weight_tensor.shape.len != 2 or bias_tensor.shape.len != 1) return error.InvalidTaskHeadTensorShape;
    const num_classes = positiveShapeDim(weight_tensor.shape[0]) orelse return error.InvalidTaskHeadTensorShape;
    const hidden_size = positiveShapeDim(weight_tensor.shape[1]) orelse return error.InvalidTaskHeadTensorShape;
    const bias_classes = positiveShapeDim(bias_tensor.shape[0]) orelse return error.InvalidTaskHeadTensorShape;
    if (bias_classes != num_classes) return error.InvalidTaskHeadTensorShape;

    return .{
        .allocator = allocator,
        .weight = try allocator.dupe(f32, weight_tensor.asFloat32()),
        .bias = try allocator.dupe(f32, bias_tensor.asFloat32()),
        .num_classes = num_classes,
        .hidden_size = hidden_size,
    };
}

pub fn materializeMergedModel(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    out_dir: []const u8,
) !MaterializeSummary {
    if (pathExists(out_dir)) return error.OutputDirectoryAlreadyExists;
    const staging_dir = try materializationStagingPath(allocator, out_dir);
    defer allocator.free(staging_dir);
    if (pathExists(staging_dir)) return error.MaterializationStagingDirectoryExists;

    var bundle = try loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();

    var base_paths = try resolveArtifactPaths(allocator, base_model_dir);
    defer base_paths.deinit();
    var base_access = try openTensorAccessForFile(allocator, base_paths.checkpoint_path);
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
            lora.mergeInto(matrix, adapter_a, adapter_b, bundle.lora_alpha, merged_weight);
        }

        const out_rows = layer.output_dim;
        const out_cols = layer.input_dim;
        const hf_weight = try allocator.alloc(f32, merged_weight.len);
        transpose2DF32(hf_weight, merged_weight, out_cols, out_rows);
        allocator.free(merged_weight);

        const shape = [_]i64{ @as(i64, @intCast(out_rows)), @as(i64, @intCast(out_cols)) };
        const tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, &shape, hf_weight);
        allocator.free(hf_weight);
        try merged.put(allocator, try allocator.dupe(u8, layer.base_tensor_name), tensor);
    }
    for (bundle.passthrough_tensors) |item| {
        const tensor = try Tensor.initFloat32(allocator, item.name, item.tensor.shape, item.tensor.asFloat32());
        try merged.put(allocator, try allocator.dupe(u8, item.name), tensor);
    }
    const attached_task_head_tensor_count = try attachTaskHeadCheckpointIfPresent(allocator, adapter_model_dir, &merged);

    try compat.cwd().createDirPath(compat.io(), staging_dir);
    var staging_published = false;
    defer if (!staging_published) compat.cwd().deleteTree(compat.io(), staging_dir) catch {};

    const staged_checkpoint_path = try std.fs.path.join(allocator, &.{ staging_dir, checkpoint_file_name });
    defer allocator.free(staged_checkpoint_path);
    const bytes = try buildMergedSafetensorsFile(allocator, base_access, base_names, &merged);
    defer allocator.free(bytes);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = staged_checkpoint_path, .data = bytes });

    const inventory = MaterializationInventory{
        .config = try copySupportingArtifactFromPreferredSource(allocator, base_model_dir, null, staging_dir, config_file_name, true),
        .encoder_config = try copySupportingArtifactFromPreferredSource(allocator, base_model_dir, null, staging_dir, encoder_config_file_name, true),
        .tokenizer = try copySupportingArtifactFromPreferredSource(allocator, adapter_model_dir, base_model_dir, staging_dir, tokenizer_file_name, true),
        .tokenizer_config = try copySupportingArtifactFromPreferredSource(allocator, adapter_model_dir, base_model_dir, staging_dir, tokenizer_config_file_name, true),
        .special_tokens_map = try copySupportingArtifactFromPreferredSource(allocator, adapter_model_dir, base_model_dir, staging_dir, special_tokens_map_file_name, true),
        .added_tokens = try copySupportingArtifactFromPreferredSource(allocator, adapter_model_dir, base_model_dir, staging_dir, added_tokens_file_name, true),
        .sentencepiece_model = try copySupportingArtifactFromPreferredSource(allocator, adapter_model_dir, base_model_dir, staging_dir, sentencepiece_model_file_name, false),
    };
    try copySupportingArtifactIfPresent(allocator, adapter_model_dir, staging_dir, gliner2_boundary.boundary_head_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_model_dir, staging_dir, gliner2_boundary.boundary_task_head_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_model_dir, staging_dir, entity_cleanup_model.head_file_name);

    var span_rep_passthrough_tensor_count: usize = 0;
    var count_embed_passthrough_tensor_count: usize = 0;
    for (bundle.passthrough_tensors) |item| {
        if (isSpanRepName(item.name)) span_rep_passthrough_tensor_count += 1;
        if (isCountEmbedName(item.name)) count_embed_passthrough_tensor_count += 1;
    }
    const copied_boundary_head = blk: {
        const path = try std.fs.path.join(allocator, &.{ staging_dir, gliner2_boundary.boundary_head_file_name });
        defer allocator.free(path);
        break :blk isRegularFilePath(path);
    };
    const copied_boundary_task_head = blk: {
        const path = try std.fs.path.join(allocator, &.{ staging_dir, gliner2_boundary.boundary_task_head_file_name });
        defer allocator.free(path);
        break :blk isRegularFilePath(path);
    };
    const copied_cleanup_head = blk: {
        const path = try std.fs.path.join(allocator, &.{ staging_dir, entity_cleanup_model.head_file_name });
        defer allocator.free(path);
        break :blk isRegularFilePath(path);
    };

    var reloaded = try inspectCheckpoint(allocator, staging_dir, null);
    defer freeCheckpointInspection(allocator, &reloaded);
    if (!reloaded.core_backbone_loadable) return error.MaterializedCheckpointNotLoadable;

    const output_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(output_checkpoint_path);
    const summary_family_version = try allocator.dupe(u8, artifact_family_version);
    errdefer allocator.free(summary_family_version);
    const summary_base_model_dir = try allocator.dupe(u8, base_model_dir);
    errdefer allocator.free(summary_base_model_dir);
    const summary_adapter_model_dir = try allocator.dupe(u8, adapter_model_dir);
    errdefer allocator.free(summary_adapter_model_dir);
    const summary_output_dir = try allocator.dupe(u8, out_dir);
    errdefer allocator.free(summary_output_dir);
    const summary = MaterializeSummary{
        .artifact_family_version = summary_family_version,
        .base_model_dir = summary_base_model_dir,
        .adapter_model_dir = summary_adapter_model_dir,
        .output_dir = summary_output_dir,
        .output_checkpoint_path = output_checkpoint_path,
        .merged_lora_tensor_count = bundle.layers.len + bundle.passthrough_tensors.len,
        .merged_dora_tensor_count = merged_dora_tensor_count,
        .task_head_passthrough_tensor_count = bundle.passthrough_tensors.len,
        .attached_task_head_tensor_count = attached_task_head_tensor_count,
        .span_rep_passthrough_tensor_count = span_rep_passthrough_tensor_count,
        .count_embed_passthrough_tensor_count = count_embed_passthrough_tensor_count,
        .copied_boundary_head = copied_boundary_head,
        .copied_boundary_task_head = copied_boundary_task_head,
        .copied_cleanup_head = copied_cleanup_head,
        .copied_base_tensor_count = base_names.len - bundle.layers.len,
    };

    try syncMaterializationFiles(allocator, staging_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ staging_dir, materialization_manifest_file_name });
    defer allocator.free(manifest_path);
    try writeSyncedJsonFile(allocator, manifest_path, MaterializationManifest{
        .artifact_family_version = artifact_family_version,
        .base_model_dir = base_model_dir,
        .adapter_model_dir = adapter_model_dir,
        .merged_lora_tensor_count = summary.merged_lora_tensor_count,
        .merged_dora_tensor_count = summary.merged_dora_tensor_count,
        .task_head_passthrough_tensor_count = summary.task_head_passthrough_tensor_count,
        .attached_task_head_tensor_count = summary.attached_task_head_tensor_count,
        .copied_boundary_head = summary.copied_boundary_head,
        .copied_boundary_task_head = summary.copied_boundary_task_head,
        .copied_cleanup_head = summary.copied_cleanup_head,
        .supporting_artifacts = inventory,
    });
    try syncDirectoryPath(staging_dir);
    try std.Io.Dir.rename(compat.cwd(), staging_dir, compat.cwd(), out_dir, compat.io());
    staging_published = true;
    try syncDirectoryPath(std.fs.path.dirname(out_dir) orelse ".");
    return summary;
}

pub fn freeCheckpointInspection(allocator: std.mem.Allocator, summary: *CheckpointInspection) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.checkpoint_path);
    allocator.free(summary.config_path);
    allocator.free(summary.encoder_config_path);
    allocator.free(summary.model_name);
    allocator.free(summary.model_type);
    allocator.free(summary.counting_layer);
    allocator.free(summary.token_pooling);
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
    for (summary.resolved_target_modules) |item| allocator.free(item);
    allocator.free(summary.resolved_target_modules);
    freeLoRATargetTensors(allocator, summary.resolved_tensors);
    summary.* = undefined;
}

pub fn freeLoRABundleInspectionSummary(allocator: std.mem.Allocator, summary: *LoRABundleInspectionSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.base_checkpoint_path);
    allocator.free(summary.adapter_checkpoint_path);
    if (summary.adapter_config_path) |value| allocator.free(value);
    if (summary.base_model_name_or_path) |value| allocator.free(value);
    if (summary.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    if (summary.resolved_target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    for (summary.tensors) |*item| freeLoRATensorSummary(allocator, item);
    allocator.free(summary.tensors);
    summary.* = undefined;
}

pub fn freeAutodiffAdapterExportSummary(allocator: std.mem.Allocator, summary: *AutodiffAdapterExportSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.output_dir);
    allocator.free(summary.adapter_checkpoint_path);
    allocator.free(summary.adapter_config_path);
    for (summary.target_modules) |item| allocator.free(item);
    allocator.free(summary.target_modules);
    for (summary.resolved_target_modules) |item| allocator.free(item);
    allocator.free(summary.resolved_target_modules);
    summary.* = undefined;
}

pub fn freeAutodiffRegularParamExportSummary(allocator: std.mem.Allocator, summary: *AutodiffRegularParamExportSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.output_dir);
    allocator.free(summary.checkpoint_path);
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
    const ordered_modules = [_][]const u8{
        "attention.output.dense",
        "intermediate.dense",
        "output.dense",
        "query_proj",
        "key_proj",
        "value_proj",
        "classifier",
        "span_rep",
        "count_embed",
        "count_pred",
    };
    if (std.mem.eql(u8, tensor_name, "classifier.weight")) return "classifier";
    if (std.mem.startsWith(u8, tensor_name, "classifier.") and std.mem.endsWith(u8, tensor_name, ".weight")) return "classifier";
    if (std.mem.eql(u8, tensor_name, "span_rep.weight")) return "span_rep";
    if (std.mem.eql(u8, tensor_name, "count_embed.weight")) return "count_embed";
    if (std.mem.eql(u8, tensor_name, "count_pred.weight")) return "count_pred";
    inline for (ordered_modules) |module_name| {
        const dot_suffix = "." ++ module_name ++ ".weight";
        const slash_suffix = "/" ++ module_name ++ "/weight";
        if (std.mem.endsWith(u8, tensor_name, dot_suffix)) return module_name;
        if (std.mem.endsWith(u8, tensor_name, slash_suffix)) return module_name;
    }
    if (std.mem.indexOf(u8, tensor_name, "span_rep") != null and std.mem.endsWith(u8, tensor_name, ".weight")) return "span_rep";
    if (std.mem.indexOf(u8, tensor_name, "count_embed") != null and std.mem.endsWith(u8, tensor_name, ".weight")) return "count_embed";
    if (std.mem.indexOf(u8, tensor_name, "count_pred") != null and std.mem.endsWith(u8, tensor_name, ".weight")) return "count_pred";
    return null;
}

const LoRAAdapterTensorKind = enum { a, b };

const ParsedLoRAAdapterTensorName = struct {
    adapter_tensor_base_name: []const u8,
    base_tensor_base_name: []const u8,
    module_name: []const u8,
    kind: LoRAAdapterTensorKind,
};

fn parseLoRAAdapterTensorName(tensor_name: []const u8) ?ParsedLoRAAdapterTensorName {
    const peft_prefix = "base_model.model.";
    if (std.mem.endsWith(u8, tensor_name, ".lora_A.weight")) {
        const adapter_base = tensor_name[0 .. tensor_name.len - ".lora_A.weight".len];
        const base = if (std.mem.startsWith(u8, adapter_base, peft_prefix)) adapter_base[peft_prefix.len..] else adapter_base;
        const module = moduleNameForBaseTensor(base) orelse return null;
        return .{ .adapter_tensor_base_name = adapter_base, .base_tensor_base_name = base, .module_name = module, .kind = .a };
    }
    if (std.mem.endsWith(u8, tensor_name, ".lora_B.weight")) {
        const adapter_base = tensor_name[0 .. tensor_name.len - ".lora_B.weight".len];
        const base = if (std.mem.startsWith(u8, adapter_base, peft_prefix)) adapter_base[peft_prefix.len..] else adapter_base;
        const module = moduleNameForBaseTensor(base) orelse return null;
        return .{ .adapter_tensor_base_name = adapter_base, .base_tensor_base_name = base, .module_name = module, .kind = .b };
    }
    return null;
}

pub fn autodiffParamNameToPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        const base = name[0 .. name.len - ".lora_A".len];
        return autodiffParamBaseToPeftName(allocator, tensorBaseName(base), "lora_A");
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        const base = name[0 .. name.len - ".lora_B".len];
        return autodiffParamBaseToPeftName(allocator, tensorBaseName(base), "lora_B");
    }
    return error.InvalidAutodiffAdapterName;
}

fn peftModuleNameForAdapterTensor(name: []const u8) ?[]const u8 {
    const prefix = "base_model.model.";
    if (!std.mem.startsWith(u8, name, prefix)) return null;
    const suffix = if (std.mem.endsWith(u8, name, ".lora_A.weight"))
        ".lora_A.weight"
    else if (std.mem.endsWith(u8, name, ".lora_B.weight"))
        ".lora_B.weight"
    else
        return null;
    return name[prefix.len .. name.len - suffix.len];
}

fn autodiffParamBaseToPeftName(allocator: std.mem.Allocator, base_no_weight: []const u8, adapter_name: []const u8) ![]const u8 {
    // The autodiff trainer strips the outer HF "encoder." prefix before
    // graph execution, while GLiNER2 checkpoint/bundle tools validate
    // against the original HF checkpoint names.
    if (std.mem.startsWith(u8, base_no_weight, "encoder.layer.")) {
        return std.fmt.allocPrint(allocator, "base_model.model.encoder.{s}.{s}.weight", .{ base_no_weight, adapter_name });
    }
    return std.fmt.allocPrint(allocator, "base_model.model.{s}.{s}.weight", .{ base_no_weight, adapter_name });
}

fn moduleNameForBaseTensor(base_tensor_name: []const u8) ?[]const u8 {
    const ordered_modules = [_][]const u8{
        "attention.output.dense",
        "intermediate.dense",
        "output.dense",
        "query_proj",
        "key_proj",
        "value_proj",
        "classifier",
        "span_rep",
        "count_embed",
        "count_pred",
    };
    if (std.mem.eql(u8, base_tensor_name, "classifier")) return "classifier";
    if (std.mem.startsWith(u8, base_tensor_name, "classifier.")) return "classifier";
    if (std.mem.eql(u8, base_tensor_name, "span_rep")) return "span_rep";
    if (std.mem.eql(u8, base_tensor_name, "count_embed")) return "count_embed";
    if (std.mem.eql(u8, base_tensor_name, "count_pred")) return "count_pred";
    inline for (ordered_modules) |module_name| {
        const dot_suffix = "." ++ module_name;
        const slash_suffix = "/" ++ module_name;
        if (std.mem.endsWith(u8, base_tensor_name, dot_suffix)) return module_name;
        if (std.mem.endsWith(u8, base_tensor_name, slash_suffix)) return module_name;
    }
    if (std.mem.indexOf(u8, base_tensor_name, "span_rep") != null) return "span_rep";
    if (std.mem.indexOf(u8, base_tensor_name, "count_embed") != null) return "count_embed";
    if (std.mem.indexOf(u8, base_tensor_name, "count_pred") != null) return "count_pred";
    return null;
}

fn isGraphOwnedLoRABaseModule(module_name: []const u8) bool {
    return std.mem.eql(u8, module_name, "classifier") or
        std.mem.eql(u8, module_name, "span_rep") or
        std.mem.eql(u8, module_name, "count_embed") or
        std.mem.eql(u8, module_name, "count_pred");
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

        const base_name = tensorBaseName(target.tensor_name);
        const a_name = try std.fmt.allocPrint(allocator, "{s}.lora_A.weight", .{base_name});
        const b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{base_name});
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

    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);
    compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    errdefer compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    var file = try compat.cwd().createFile(compat.io(), tmp_path, .{ .truncate = true });
    var closed = false;
    errdefer if (!closed) file.close(compat.io());

    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(compat.io(), &len_buf);
    try file.writeStreamingAll(compat.io(), header_buf.written());
    for (tensors) |tensor| {
        try file.writeStreamingAll(compat.io(), std.mem.sliceAsBytes(tensor.data));
    }
    file.close(compat.io());
    closed = true;
    try std.Io.Dir.rename(compat.cwd(), tmp_path, compat.cwd(), path, compat.io());
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

fn i32DimsToUsize(allocator: std.mem.Allocator, dims: []const i32) ![]usize {
    const out = try allocator.alloc(usize, dims.len);
    errdefer allocator.free(out);
    for (dims, 0..) |dim, idx| {
        if (dim <= 0) return error.InvalidAdapterTensorShape;
        out[idx] = @intCast(dim);
    }
    return out;
}

fn elementCount(shape: []const usize) usize {
    var count: usize = 1;
    for (shape) |dim| count *= dim;
    return count;
}

fn positiveShapeDim(dim: i64) ?usize {
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    if (std.mem.endsWith(u8, tensor_name, "/weight")) return tensor_name[0 .. tensor_name.len - "/weight".len];
    return tensor_name;
}

fn doraMagnitudeTensorName(allocator: std.mem.Allocator, base_tensor_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.lora_magnitude_vector.weight", .{tensorBaseName(base_tensor_name)});
}

fn isDoRAMagnitudeTensorName(tensor_name: []const u8) bool {
    return std.mem.endsWith(u8, tensor_name, ".lora_magnitude_vector.weight");
}

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    dropout: f32,
    target_modules: []const []const u8,
    use_dora: bool,
) !void {
    try validateLoRADropout(dropout);
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .auto_mapping = .{
            .base_model_class = "Extractor",
            .parent_library = "gliner2.model",
        },
        .base_model_name_or_path = base_model_name_or_path,
        .bias = "none",
        .inference_mode = true,
        .peft_type = "LORA",
        .task_type = @as(?[]const u8, null),
        .r = rank,
        .lora_alpha = alpha,
        .lora_dropout = dropout,
        .target_modules = target_modules,
        .use_dora = use_dora,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try writeFileAtomic(allocator, path, buffer.written());
}

fn writeFileAtomic(allocator: std.mem.Allocator, path: []const u8, data: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);
    compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    errdefer compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tmp_path, .data = data });
    try std.Io.Dir.rename(compat.cwd(), tmp_path, compat.cwd(), path, compat.io());
}

fn bundleHasDoRA(bundle: *const LoadedLoRABundle) bool {
    for (bundle.layers) |layer| {
        if (layer.dora_magnitude != null) return true;
    }
    return false;
}

const AdapterInspection = struct {
    model_dir: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    lora_rank: ?usize = null,
    lora_alpha: ?f64 = null,
    lora_dropout: ?f64 = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    resolved_target_module_count: usize = 0,
    resolved_target_modules: ?[]const []const u8 = null,
    use_dora: ?bool = null,
};

fn inspectAdapterConfig(allocator: std.mem.Allocator, input: []const u8) !AdapterInspection {
    const model_dir = if (isDirectoryPath(input))
        try allocator.dupe(u8, input)
    else
        try allocator.dupe(u8, std.fs.path.dirname(input) orelse ".");
    errdefer allocator.free(model_dir);

    const adapter_checkpoint_path = if (isRegularFilePath(input))
        try allocator.dupe(u8, input)
    else
        try resolveLoRACheckpointPath(allocator, input);
    errdefer allocator.free(adapter_checkpoint_path);

    const adapter_config_path = try optionalPathInDir(allocator, model_dir, adapter_config_file_name);
    errdefer if (adapter_config_path) |path| allocator.free(path);
    var adapter_config = if (adapter_config_path) |path|
        try loadOptionalAdapterConfig(allocator, path)
    else
        null;
    defer if (adapter_config) |*cfg| freeOwnedAdapterConfig(allocator, cfg);

    return .{
        .model_dir = model_dir,
        .adapter_checkpoint_path = adapter_checkpoint_path,
        .adapter_config_path = adapter_config_path,
        .base_model_name_or_path = if (adapter_config) |cfg| try dupeOptionalString(allocator, cfg.base_model_name_or_path) else null,
        .lora_rank = if (adapter_config) |cfg| cfg.r else null,
        .lora_alpha = if (adapter_config) |cfg| cfg.lora_alpha else null,
        .lora_dropout = if (adapter_config) |cfg| cfg.lora_dropout else null,
        .target_module_count = if (adapter_config) |cfg| if (cfg.target_modules) |items| items.len else 0 else 0,
        .target_modules = if (adapter_config) |cfg| try dupeOptionalStringSlice(allocator, cfg.target_modules) else null,
        .resolved_target_module_count = if (adapter_config) |cfg| if (cfg.target_modules) |items| blk: {
            const resolved = try expandLoRATargetModules(allocator, items);
            defer {
                for (resolved) |item| allocator.free(item);
                allocator.free(resolved);
            }
            break :blk resolved.len;
        } else 0 else 0,
        .resolved_target_modules = if (adapter_config) |cfg| if (cfg.target_modules) |items| try expandLoRATargetModules(allocator, items) else null else null,
        .use_dora = if (adapter_config) |cfg| cfg.use_dora else null,
    };
}

fn freeAdapterInspection(allocator: std.mem.Allocator, inspection: *AdapterInspection) void {
    allocator.free(inspection.model_dir);
    allocator.free(inspection.adapter_checkpoint_path);
    if (inspection.adapter_config_path) |value| allocator.free(value);
    if (inspection.base_model_name_or_path) |value| allocator.free(value);
    if (inspection.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    if (inspection.resolved_target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    inspection.* = undefined;
}

fn loadBackboneConfigFromPaths(allocator: std.mem.Allocator, paths: *const ArtifactPaths) !BackboneConfig {
    const top_bytes = try c_file.readFile(allocator, paths.config_path);
    defer allocator.free(top_bytes);
    const encoder_bytes = try c_file.readFile(allocator, paths.encoder_config_path);
    defer allocator.free(encoder_bytes);

    var top = try std.json.parseFromSlice(std.json.Value, allocator, top_bytes, .{ .ignore_unknown_fields = true });
    defer top.deinit();
    var encoder = try std.json.parseFromSlice(std.json.Value, allocator, encoder_bytes, .{ .ignore_unknown_fields = true });
    defer encoder.deinit();
    const top_obj = top.value.object;
    const encoder_obj = encoder.value.object;

    return .{
        .allocator = allocator,
        .model_name = try allocator.dupe(u8, jsonObjectString(top_obj, "model_name") orelse "unknown"),
        .model_type = try allocator.dupe(u8, jsonObjectString(top_obj, "model_type") orelse "extractor"),
        .counting_layer = try allocator.dupe(u8, jsonObjectString(top_obj, "counting_layer") orelse "unknown"),
        .token_pooling = try allocator.dupe(u8, jsonObjectString(top_obj, "token_pooling") orelse "first"),
        .max_width = jsonObjectUsize(top_obj, "max_width") orelse 8,
        .vocab_size = jsonObjectUsize(encoder_obj, "vocab_size") orelse 0,
        .hidden_size = jsonObjectUsize(encoder_obj, "hidden_size") orelse 0,
        .num_hidden_layers = jsonObjectUsize(encoder_obj, "num_hidden_layers") orelse 0,
        .num_attention_heads = jsonObjectUsize(encoder_obj, "num_attention_heads") orelse 0,
        .intermediate_size = jsonObjectUsize(encoder_obj, "intermediate_size") orelse 0,
        .max_position_embeddings = jsonObjectUsize(encoder_obj, "max_position_embeddings") orelse 0,
        .type_vocab_size = jsonObjectUsize(encoder_obj, "type_vocab_size") orelse 0,
        .position_buckets = jsonObjectUsize(encoder_obj, "position_buckets") orelse 0,
        .relative_attention = jsonObjectBool(encoder_obj, "relative_attention") orelse false,
        .hidden_dropout_prob = jsonObjectF32(encoder_obj, "hidden_dropout_prob") orelse 0,
        .attention_probs_dropout_prob = jsonObjectF32(encoder_obj, "attention_probs_dropout_prob") orelse 0,
        .layer_norm_eps = jsonObjectF32(encoder_obj, "layer_norm_eps") orelse 0,
        .count_embed_dim = jsonObjectUsize(top_obj, "count_embed_dim") orelse 128,
        .count_embed_layers = jsonObjectUsize(top_obj, "count_embed_layers") orelse 2,
        .count_embed_heads = jsonObjectUsize(top_obj, "count_embed_heads") orelse 4,
        .count_embed_ffn = jsonObjectUsize(top_obj, "count_embed_ffn") orelse 256,
        .max_count_embed = jsonObjectUsize(top_obj, "max_count_embed") orelse 20,
    };
}

fn jsonObjectString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => |s| s,
        else => null,
    };
}

fn jsonObjectUsize(obj: std.json.ObjectMap, key: []const u8) ?usize {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => |v| if (v >= 0) @intCast(v) else null,
        .float => |v| if (v >= 0) @intFromFloat(v) else null,
        else => null,
    };
}

fn jsonObjectF32(obj: std.json.ObjectMap, key: []const u8) ?f32 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| @floatCast(v),
        else => null,
    };
}

fn jsonObjectBool(obj: std.json.ObjectMap, key: []const u8) ?bool {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .bool => |v| v,
        else => null,
    };
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

fn requiredPathInDir(allocator: std.mem.Allocator, dir_path: []const u8, basename: []const u8) ![]u8 {
    return (try optionalPathInDir(allocator, dir_path, basename)) orelse error.FileNotFound;
}

fn loadOptionalAdapterConfig(allocator: std.mem.Allocator, path: []const u8) !?AdapterConfig {
    const bytes = c_file.readFile(allocator, path) catch return null;
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(AdapterConfig, allocator, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return try dupeAdapterConfig(allocator, parsed.value);
}

fn dupeAdapterConfig(allocator: std.mem.Allocator, config: AdapterConfig) !AdapterConfig {
    return .{
        .base_model_name_or_path = try dupeOptionalString(allocator, config.base_model_name_or_path),
        .peft_type = try dupeOptionalString(allocator, config.peft_type),
        .task_type = try dupeOptionalString(allocator, config.task_type),
        .r = config.r,
        .lora_alpha = config.lora_alpha,
        .lora_dropout = config.lora_dropout,
        .target_modules = try dupeOptionalStringSlice(allocator, config.target_modules),
        .use_dora = config.use_dora,
    };
}

fn freeOwnedAdapterConfig(allocator: std.mem.Allocator, config: *AdapterConfig) void {
    if (config.base_model_name_or_path) |value| allocator.free(value);
    if (config.peft_type) |value| allocator.free(value);
    if (config.task_type) |value| allocator.free(value);
    if (config.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    config.* = undefined;
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
    if (item.dora_magnitude_tensor_name) |value| allocator.free(value);
    allocator.free(item.module_name);
    item.* = undefined;
}

fn attachTaskHeadCheckpointIfPresent(
    allocator: std.mem.Allocator,
    adapter_model_dir: []const u8,
    merged: *std.StringArrayHashMapUnmanaged(Tensor),
) !usize {
    const task_head_path = try optionalPathInDir(allocator, adapter_model_dir, task_head_checkpoint_file_name);
    defer if (task_head_path) |path| allocator.free(path);
    const path = task_head_path orelse return 0;

    var access = try openTensorAccessForFile(allocator, path);
    defer access.deinit();
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    if (!stringSliceContains(names, "classifier.weight")) return error.MissingTaskHeadTensors;
    if (!stringSliceContains(names, "classifier.bias")) return error.MissingTaskHeadTensors;

    for (names) |name| {
        var tensor = try loadTensorAsF32(allocator, access, name);
        errdefer tensor.deinit();
        try merged.put(allocator, try allocator.dupe(u8, name), tensor);
    }
    return names.len;
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
    const parent = std.fs.path.dirname(dst);
    if (parent) |dir_path| try compat.cwd().createDirPath(compat.io(), dir_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst, .data = bytes });
}

fn copySupportingArtifactFromPreferredSource(
    allocator: std.mem.Allocator,
    preferred_source_dir: []const u8,
    fallback_source_dir: ?[]const u8,
    out_dir: []const u8,
    file_name: []const u8,
    required: bool,
) !bool {
    const preferred_path = try optionalPathInDir(allocator, preferred_source_dir, file_name);
    defer if (preferred_path) |path| allocator.free(path);
    const fallback_path = if (preferred_path == null and fallback_source_dir != null)
        try optionalPathInDir(allocator, fallback_source_dir.?, file_name)
    else
        null;
    defer if (fallback_path) |path| allocator.free(path);
    const source_path = preferred_path orelse fallback_path orelse {
        if (required) return error.RequiredSupportingArtifactMissing;
        return false;
    };

    const bytes = try c_file.readFile(allocator, source_path);
    defer allocator.free(bytes);
    const dst = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst);
    if (std.fs.path.dirname(dst)) |parent| try compat.cwd().createDirPath(compat.io(), parent);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst, .data = bytes });
    return true;
}

fn materializationStagingPath(allocator: std.mem.Allocator, out_dir: []const u8) ![]const u8 {
    const name = std.fs.path.basename(out_dir);
    if (name.len == 0 or std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) {
        return error.InvalidOutputDirectory;
    }
    const staging_name = try std.fmt.allocPrint(allocator, ".{s}.staging-{d}", .{ name, std.posix.system.getpid() });
    defer allocator.free(staging_name);
    return std.fs.path.join(allocator, &.{ std.fs.path.dirname(out_dir) orelse ".", staging_name });
}

fn pathExists(path: []const u8) bool {
    _ = compat.cwd().statFile(compat.io(), path, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return true,
    };
    return true;
}

fn inspectionArtifactFamilyVersion(allocator: std.mem.Allocator, model_dir: []const u8) ![]const u8 {
    const manifest_path = try std.fs.path.join(allocator, &.{ model_dir, materialization_manifest_file_name });
    defer allocator.free(manifest_path);
    if (!isRegularFilePath(manifest_path)) return allocator.dupe(u8, artifact_family_version);

    const bytes = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(1024 * 1024));
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidMaterializationManifest;
    const object = parsed.value.object;
    const schema_value = object.get("schema_version") orelse return error.InvalidMaterializationManifest;
    const status_value = object.get("status") orelse return error.InvalidMaterializationManifest;
    const family_value = object.get("artifact_family_version") orelse return error.InvalidMaterializationManifest;
    if (schema_value != .string or status_value != .string or family_value != .string) {
        return error.InvalidMaterializationManifest;
    }
    if (!std.mem.eql(u8, schema_value.string, materialization_schema_version) or
        !std.mem.eql(u8, status_value.string, "complete"))
    {
        return error.InvalidMaterializationManifest;
    }
    if (!isSupportedArtifactFamilyVersion(family_value.string)) return error.UnsupportedArtifactFamilyVersion;
    return allocator.dupe(u8, family_value.string);
}

fn syncMaterializationFiles(allocator: std.mem.Allocator, staging_dir: []const u8) !void {
    const file_names = [_][]const u8{
        checkpoint_file_name,
        config_file_name,
        encoder_config_file_name,
        tokenizer_file_name,
        tokenizer_config_file_name,
        special_tokens_map_file_name,
        added_tokens_file_name,
        sentencepiece_model_file_name,
        gliner2_boundary.boundary_head_file_name,
        gliner2_boundary.boundary_task_head_file_name,
        entity_cleanup_model.head_file_name,
    };
    for (file_names) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ staging_dir, file_name });
        defer allocator.free(path);
        if (!isRegularFilePath(path)) continue;
        var file = try compat.cwd().openFile(compat.io(), path, .{});
        defer file.close(compat.io());
        try file.sync(compat.io());
    }
    const encoder_dir = try std.fs.path.join(allocator, &.{ staging_dir, "encoder_config" });
    defer allocator.free(encoder_dir);
    try syncDirectoryPath(encoder_dir);
    try syncDirectoryPath(staging_dir);
}

fn writeSyncedJsonFile(allocator: std.mem.Allocator, path: []const u8, value: anytype) !void {
    const rendered = try std.json.Stringify.valueAlloc(allocator, value, .{ .whitespace = .indent_2 });
    defer allocator.free(rendered);
    var file = try compat.cwd().createFile(compat.io(), path, .{ .truncate = true });
    defer file.close(compat.io());
    try file.writeStreamingAll(compat.io(), rendered);
    try file.sync(compat.io());
}

fn syncDirectoryPath(path: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;
    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(compat.io(), path, .{ .iterate = true })
    else
        try compat.cwd().openDir(compat.io(), path, .{ .iterate = true });
    defer dir.close(compat.io());
    while (true) switch (std.posix.errno(std.posix.system.fsync(dir.handle))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return,
        .BADF => return error.InvalidFileDescriptor,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
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

fn transpose2DF32(out: []f32, input: []const f32, rows: usize, cols: usize) void {
    std.debug.assert(out.len == rows * cols);
    std.debug.assert(input.len == rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            out[col * rows + row] = input[row * cols + col];
        }
    }
}

fn tensorShapeToOwnedUsize(allocator: std.mem.Allocator, shape: []const i64) ![]usize {
    const out = try allocator.alloc(usize, shape.len);
    for (shape, 0..) |dim, idx| out[idx] = @intCast(dim);
    return out;
}

fn stringSliceContains(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn tensorInfoElementCount(info: safetensors.TensorMeta) u64 {
    var total: u64 = 1;
    for (info.shape) |dim| total *= @intCast(dim);
    return total;
}

fn isWordEmbeddingsName(name: []const u8) bool {
    return std.mem.eql(u8, name, "gliner2/encoder/embeddings/word_embeddings") or
        std.mem.eql(u8, name, "encoder.embeddings.word_embeddings.weight");
}

fn isFinalLayerNormName(name: []const u8) bool {
    return std.mem.eql(u8, name, "gliner2/encoder/LayerNorm/weight") or
        std.mem.eql(u8, name, "encoder.encoder.LayerNorm.weight");
}

fn isRelativeEmbeddingsName(name: []const u8) bool {
    return std.mem.eql(u8, name, "gliner2/encoder/rel_embeddings/weight") or
        std.mem.eql(u8, name, "encoder.encoder.rel_embeddings.weight");
}

fn isQueryProjWeightName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, "/attention/self/query_proj/weight") or
        std.mem.endsWith(u8, name, ".attention.self.query_proj.weight");
}

fn isKeyProjWeightName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, "/attention/self/key_proj/weight") or
        std.mem.endsWith(u8, name, ".attention.self.key_proj.weight");
}

fn isValueProjWeightName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, "/attention/self/value_proj/weight") or
        std.mem.endsWith(u8, name, ".attention.self.value_proj.weight");
}

fn isOutputDenseWeightName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, "/output/dense/weight") or
        std.mem.endsWith(u8, name, ".output.dense.weight");
}

fn isSpanRepName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, "gliner2/span_rep/") != null or
        std.mem.indexOf(u8, name, "span_rep.span_rep_layer.") != null;
}

fn isCountEmbedName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, "gliner2/count_embed/") != null or
        std.mem.startsWith(u8, name, "count_embed.");
}

fn isTaskHeadPassthroughName(name: []const u8) bool {
    return isSpanRepName(name) or isCountEmbedName(name);
}

fn isDirectoryPath(path: []const u8) bool {
    const stat = compat.cwd().statFile(compat.io(), path, .{}) catch return false;
    return stat.kind == .directory;
}

fn isRegularFilePath(path: []const u8) bool {
    const stat = compat.cwd().statFile(compat.io(), path, .{}) catch return false;
    return stat.kind == .file;
}

test "gliner2 checkpoint inspection reads config and tensor summary" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_inspect_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const encoder_dir = try std.fs.path.join(allocator, &.{ root, "encoder_config" });
    defer allocator.free(encoder_dir);
    try compat.cwd().createDirPath(compat.io(), encoder_dir);
    const config_path = try std.fs.path.join(allocator, &.{ root, "config.json" });
    defer allocator.free(config_path);
    const encoder_config_path = try std.fs.path.join(allocator, &.{ root, "encoder_config", "config.json" });
    defer allocator.free(encoder_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_name":"urchade/gliner2","model_type":"gliner2","counting_layer":"count_embed","token_pooling":"first","max_width":12,"count_embed_dim":128,"count_embed_layers":2,"count_embed_heads":4,"count_embed_ffn":256,"max_count_embed":20}
        ,
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = encoder_config_path,
        .data =
        \\{"vocab_size":30522,"hidden_size":128,"num_hidden_layers":2,"num_attention_heads":4,"intermediate_size":256,"max_position_embeddings":512,"type_vocab_size":2,"position_buckets":32,"relative_attention":true,"hidden_dropout_prob":0.1,"attention_probs_dropout_prob":0.1,"layer_norm_eps":1e-7}
        ,
    });
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 4, 128 }, .data = &@as([(4 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 32, 32 }, .data = &@as([(32 * 32)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{128}, .data = &@as([128]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.1.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.1.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.1.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .shape = &.{ 32, 128 }, .data = &@as([(32 * 128)]f32, @splat(0)) },
        .{ .name = "count_embed.pos_embedding.weight", .shape = &.{ 8, 128 }, .data = &@as([(8 * 128)]f32, @splat(0)) },
    });

    var summary = try inspectCheckpoint(allocator, root, null);
    defer freeCheckpointInspection(allocator, &summary);
    try std.testing.expect(summary.word_embeddings_found);
    try std.testing.expect(summary.rel_embeddings_found);
    try std.testing.expect(summary.final_layernorm_found);
    try std.testing.expectEqual(@as(usize, 2), summary.query_proj_weights_found);
    try std.testing.expect(summary.core_backbone_loadable);
}

test "gliner2 upstream lora target groups expand to concrete modules" {
    const allocator = std.testing.allocator;
    const expanded = try expandLoRATargetModules(allocator, default_lora_target_modules[0..]);
    defer {
        for (expanded) |item| allocator.free(item);
        allocator.free(expanded);
    }

    try std.testing.expect(stringSliceContains(expanded, "query_proj"));
    try std.testing.expect(stringSliceContains(expanded, "key_proj"));
    try std.testing.expect(stringSliceContains(expanded, "value_proj"));
    try std.testing.expect(stringSliceContains(expanded, "attention.output.dense"));
    try std.testing.expect(stringSliceContains(expanded, "intermediate.dense"));
    try std.testing.expect(stringSliceContains(expanded, "output.dense"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_start.0"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_start.3"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_end.0"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_end.3"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.out_project.0"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.out_project.3"));
    try std.testing.expect(stringSliceContains(expanded, "classifier.0"));
    try std.testing.expect(stringSliceContains(expanded, "classifier.2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.in_projector"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.0.linear1"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.0.linear2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.1.linear1"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.1.linear2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.out_projector.0"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.out_projector.2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.out_projector.4"));
    try std.testing.expect(stringSliceContains(expanded, "count_pred.0"));
    try std.testing.expect(stringSliceContains(expanded, "count_pred.2"));
    try std.testing.expect(!stringSliceContains(expanded, "count_embed.gru"));
    try std.testing.expect(!stringSliceContains(expanded, "count_embed.transformer.transformer.layers.0.self_attn.out_proj"));
    try std.testing.expect(!stringSliceContains(expanded, "count_embed.transformer.transformer.layers.1.self_attn.out_proj"));
    try std.testing.expect(!stringSliceContains(expanded, "self_attn.in_proj_weight"));
    try std.testing.expect(!stringSliceContains(expanded, "task_classifier"));
}

test "gliner2 lora dropout validates python-compatible range" {
    try validateLoRADropout(0.0);
    try validateLoRADropout(0.1);
    try std.testing.expectError(error.InvalidLoRADropout, validateLoRADropout(-0.1));
    try std.testing.expectError(error.InvalidLoRADropout, validateLoRADropout(1.0));
}

test "gliner2 lora artifact family writes v1 and accepts legacy v1alpha1" {
    try std.testing.expectEqualStrings("gliner2_lora/v1", artifact_family_version);
    try std.testing.expect(isSupportedArtifactFamilyVersion(artifact_family_version));
    try std.testing.expect(isSupportedArtifactFamilyVersion(legacy_artifact_family_version));
    try std.testing.expect(!isSupportedArtifactFamilyVersion("gliner2_lora/v2"));
}

test "gliner2 bootstrap and inspect lora bundle" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_bootstrap_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const encoder_dir = try std.fs.path.join(allocator, &.{ root, "encoder_config" });
    defer allocator.free(encoder_dir);
    try compat.cwd().createDirPath(compat.io(), encoder_dir);
    const config_path = try std.fs.path.join(allocator, &.{ root, "config.json" });
    defer allocator.free(config_path);
    const encoder_config_path = try std.fs.path.join(allocator, &.{ root, "encoder_config", "config.json" });
    defer allocator.free(encoder_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data = "{\"model_name\":\"urchade/gliner2\",\"model_type\":\"gliner2\",\"counting_layer\":\"count_embed\",\"token_pooling\":\"first\",\"max_width\":12}",
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = encoder_config_path,
        .data = "{\"hidden_size\":128,\"num_hidden_layers\":1,\"num_attention_heads\":4}",
    });
    for ([_][]const u8{
        tokenizer_file_name,
        tokenizer_config_file_name,
        special_tokens_map_file_name,
        added_tokens_file_name,
    }) |file_name| {
        const tokenizer_artifact_path = try std.fs.path.join(allocator, &.{ root, file_name });
        defer allocator.free(tokenizer_artifact_path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_artifact_path, .data = "{}" });
    }
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 4, 128 }, .data = &@as([(4 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 32, 32 }, .data = &@as([(32 * 32)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{128}, .data = &@as([128]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
    });

    const out_dir = try std.fs.path.join(allocator, &.{ root, "lora" });
    defer allocator.free(out_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, out_dir, .{ .rank = 8, .alpha = 16 });
    defer freeBootstrapSummary(allocator, &bootstrap);
    try std.testing.expectEqual(@as(usize, 3), bootstrap.resolved_tensors.len);

    var bundle = try loadLoRABundle(allocator, root, out_dir);
    defer bundle.deinit();
    for (bundle.layers) |*layer| {
        const magnitude = try allocator.alloc(f32, layer.output_dim);
        @memset(magnitude, 1.0);
        layer.dora_magnitude = magnitude;
        layer.dora_magnitude_tensor_name = try doraMagnitudeTensorName(allocator, layer.base_tensor_name);
    }
    try saveLoRABundle(&bundle, out_dir);
    const task_head_path = try std.fs.path.join(allocator, &.{ out_dir, task_head_checkpoint_file_name });
    defer allocator.free(task_head_path);
    const classifier_weight = @as([(3 * 128)]f32, @splat(0.25));
    const classifier_bias = [_]f32{ 0.5, -0.25, 0.75 };
    try writeHeaderAndTensorsF32(allocator, task_head_path, &.{
        .{ .name = "classifier.weight", .shape = &.{ 3, 128 }, .data = &classifier_weight },
        .{ .name = "classifier.bias", .shape = &.{3}, .data = &classifier_bias },
    });

    var cleanup_head = try entity_cleanup_model.CleanupHead.init(allocator, 8, 4, 0);
    defer cleanup_head.deinit();
    try entity_cleanup_model.saveHead(allocator, &cleanup_head, out_dir);

    var inspect = try inspectLoRABundle(allocator, root, out_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspect);
    try std.testing.expectEqual(@as(usize, 3), inspect.resolved_tensor_count);
    try std.testing.expectEqual(@as(?usize, 8), inspect.lora_rank);
    try std.testing.expectEqual(@as(usize, 3), inspect.dora_magnitude_tensor_count);
    try std.testing.expectEqual(@as(usize, 384), inspect.dora_magnitude_parameter_count);
    try std.testing.expectEqual(@as(?bool, true), inspect.use_dora);
    try std.testing.expect(inspect.cleanup_head_present);

    const materialized_dir = try std.fs.path.join(allocator, &.{ root, "materialized" });
    defer allocator.free(materialized_dir);
    var materialize = try materializeMergedModel(allocator, root, out_dir, materialized_dir);
    defer freeMaterializeSummary(allocator, &materialize);
    try std.testing.expect(materialize.copied_cleanup_head);
    try std.testing.expectEqual(@as(usize, 3), materialize.merged_dora_tensor_count);
    try std.testing.expectEqual(@as(usize, 2), materialize.attached_task_head_tensor_count);
    try std.testing.expectError(error.OutputDirectoryAlreadyExists, materializeMergedModel(allocator, root, out_dir, materialized_dir));

    const completion_path = try std.fs.path.join(allocator, &.{ materialized_dir, materialization_manifest_file_name });
    defer allocator.free(completion_path);
    const completion_bytes = try compat.cwd().readFileAlloc(compat.io(), completion_path, allocator, .limited(64 * 1024));
    defer allocator.free(completion_bytes);
    var completion = try std.json.parseFromSlice(std.json.Value, allocator, completion_bytes, .{});
    defer completion.deinit();
    try std.testing.expectEqualStrings("complete", completion.value.object.get("status").?.string);
    try std.testing.expectEqualStrings(artifact_family_version, completion.value.object.get("artifact_family_version").?.string);
    const inventory = completion.value.object.get("supporting_artifacts").?.object;
    try std.testing.expect(inventory.get("tokenizer").?.bool);
    try std.testing.expect(inventory.get("added_tokens").?.bool);
    try std.testing.expect(!inventory.get("sentencepiece_model").?.bool);

    const manifest_inventory = MaterializationInventory{
        .config = true,
        .encoder_config = true,
        .tokenizer = true,
        .tokenizer_config = true,
        .special_tokens_map = true,
        .added_tokens = true,
        .sentencepiece_model = false,
    };
    try writeSyncedJsonFile(allocator, completion_path, MaterializationManifest{
        .artifact_family_version = legacy_artifact_family_version,
        .base_model_dir = root,
        .adapter_model_dir = out_dir,
        .merged_lora_tensor_count = materialize.merged_lora_tensor_count,
        .merged_dora_tensor_count = materialize.merged_dora_tensor_count,
        .task_head_passthrough_tensor_count = materialize.task_head_passthrough_tensor_count,
        .attached_task_head_tensor_count = materialize.attached_task_head_tensor_count,
        .copied_boundary_head = materialize.copied_boundary_head,
        .copied_boundary_task_head = materialize.copied_boundary_task_head,
        .copied_cleanup_head = materialize.copied_cleanup_head,
        .supporting_artifacts = manifest_inventory,
    });
    var legacy_reload = try inspectCheckpoint(allocator, materialized_dir, null);
    defer freeCheckpointInspection(allocator, &legacy_reload);
    try std.testing.expectEqualStrings(legacy_artifact_family_version, legacy_reload.artifact_family_version);

    try writeSyncedJsonFile(allocator, completion_path, MaterializationManifest{
        .artifact_family_version = "gliner2_lora/v2",
        .base_model_dir = root,
        .adapter_model_dir = out_dir,
        .merged_lora_tensor_count = materialize.merged_lora_tensor_count,
        .merged_dora_tensor_count = materialize.merged_dora_tensor_count,
        .task_head_passthrough_tensor_count = materialize.task_head_passthrough_tensor_count,
        .attached_task_head_tensor_count = materialize.attached_task_head_tensor_count,
        .copied_boundary_head = materialize.copied_boundary_head,
        .copied_boundary_task_head = materialize.copied_boundary_task_head,
        .copied_cleanup_head = materialize.copied_cleanup_head,
        .supporting_artifacts = manifest_inventory,
    });
    try std.testing.expectError(error.UnsupportedArtifactFamilyVersion, inspectCheckpoint(allocator, materialized_dir, null));

    var materialized_access = try openTensorAccessForFile(allocator, materialize.output_checkpoint_path);
    defer materialized_access.deinit();
    var materialized_classifier_weight = try loadTensorAsF32(allocator, materialized_access, "classifier.weight");
    defer materialized_classifier_weight.deinit();
    var materialized_classifier_bias = try loadTensorAsF32(allocator, materialized_access, "classifier.bias");
    defer materialized_classifier_bias.deinit();
    try std.testing.expectEqualSlices(i64, &.{ 3, 128 }, materialized_classifier_weight.shape);
    try std.testing.expectEqualSlices(i64, &.{3}, materialized_classifier_bias.shape);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), materialized_classifier_weight.asFloat32()[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -0.25), materialized_classifier_bias.asFloat32()[1], 1e-6);

    var adapter_head = try loadClassifierTaskHead(allocator, task_head_path);
    defer adapter_head.deinit();
    var materialized_head = try loadClassifierTaskHead(allocator, materialize.output_checkpoint_path);
    defer materialized_head.deinit();
    try std.testing.expectEqual(adapter_head.num_classes, materialized_head.num_classes);
    try std.testing.expectEqual(adapter_head.hidden_size, materialized_head.hidden_size);
    try std.testing.expectEqualSlices(f32, adapter_head.weight, materialized_head.weight);
    try std.testing.expectEqualSlices(f32, adapter_head.bias, materialized_head.bias);

    const hidden_rows = @as([(2 * 128)]f32, @splat(0.5));
    const adapter_logits = try adapter_head.scoreRowsAlloc(allocator, &hidden_rows);
    defer allocator.free(adapter_logits);
    const materialized_logits = try materialized_head.scoreRowsAlloc(allocator, &hidden_rows);
    defer allocator.free(materialized_logits);
    try std.testing.expectEqualSlices(f32, adapter_logits, materialized_logits);
    try std.testing.expectApproxEqAbs(@as(f32, 16.5), adapter_logits[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 15.75), adapter_logits[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 16.75), adapter_logits[2], 1e-5);

    // A missing required inventory file must leave neither a published model
    // nor the same-process staging directory behind.
    const added_tokens_path = try std.fs.path.join(allocator, &.{ root, added_tokens_file_name });
    defer allocator.free(added_tokens_path);
    try compat.cwd().deleteFile(compat.io(), added_tokens_path);
    const failed_materialized_dir = try std.fs.path.join(allocator, &.{ root, "materialized-missing-inventory" });
    defer allocator.free(failed_materialized_dir);
    const failed_staging_dir = try materializationStagingPath(allocator, failed_materialized_dir);
    defer allocator.free(failed_staging_dir);
    try std.testing.expectError(
        error.RequiredSupportingArtifactMissing,
        materializeMergedModel(allocator, root, out_dir, failed_materialized_dir),
    );
    try std.testing.expect(!pathExists(failed_materialized_dir));
    try std.testing.expect(!pathExists(failed_staging_dir));
}

test "gliner2 exports autodiff adapter params as inspectable PEFT bundle" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_autodiff_export_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const encoder_dir = try std.fs.path.join(allocator, &.{ root, "encoder_config" });
    defer allocator.free(encoder_dir);
    try compat.cwd().createDirPath(compat.io(), encoder_dir);
    const config_path = try std.fs.path.join(allocator, &.{ root, config_file_name });
    defer allocator.free(config_path);
    const encoder_config_path = try std.fs.path.join(allocator, &.{ root, encoder_config_file_name });
    defer allocator.free(encoder_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data = "{\"model_name\":\"urchade/gliner2\",\"model_type\":\"gliner2\",\"counting_layer\":\"count_embed\",\"token_pooling\":\"first\",\"max_width\":12}",
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = encoder_config_path,
        .data = "{\"hidden_size\":128,\"num_hidden_layers\":1,\"num_attention_heads\":4}",
    });
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 4, 128 }, .data = &@as([(4 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 32, 32 }, .data = &@as([(32 * 32)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{128}, .data = &@as([128]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &@as([(128 * 128)]f32, @splat(0)) },
    });

    const out_dir = try std.fs.path.join(allocator, &.{ root, "autodiff_lora" });
    defer allocator.free(out_dir);
    const a_data = @as([(2 * 128)]f32, @splat(0.01));
    const b_data = @as([(128 * 2)]f32, @splat(0.02));
    const params = [_]AutodiffAdapterParam{
        .{
            .name = "encoder.layer.0.attention.self.query_proj.weight.lora_A",
            .dims = &.{ 2, 128 },
            .weights = &a_data,
        },
        .{
            .name = "encoder.layer.0.attention.self.query_proj.weight.lora_B",
            .dims = &.{ 128, 2 },
            .weights = &b_data,
        },
    };
    var exported = try exportAutodiffAdaptersAsPeftBundle(
        allocator,
        out_dir,
        root,
        2,
        4,
        0.0,
        &.{"query"},
        &params,
    );
    defer freeAutodiffAdapterExportSummary(allocator, &exported);
    try std.testing.expectEqual(@as(usize, 2), exported.exported_tensor_count);

    const config_bytes = try compat.cwd().readFileAlloc(compat.io(), exported.adapter_config_path, allocator, .limited(64 * 1024));
    defer allocator.free(config_bytes);
    var config = try std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{});
    defer config.deinit();
    try std.testing.expect(config.value.object.get("task_type").? == .null);
    try std.testing.expectEqualStrings("Extractor", config.value.object.get("auto_mapping").?.object.get("base_model_class").?.string);
    try std.testing.expectEqualStrings("encoder.encoder.layer.0.attention.self.query_proj", config.value.object.get("target_modules").?.array.items[0].string);

    var inspected = try inspectLoRABundle(allocator, root, out_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspected);
    try std.testing.expectEqual(@as(usize, 1), inspected.resolved_tensor_count);
    try std.testing.expectEqual(@as(usize, 512), inspected.trainable_parameter_count);
    try std.testing.expectEqualStrings("encoder.encoder.layer.0.attention.self.query_proj.weight", inspected.tensors[0].base_tensor_name);
    try std.testing.expectEqualStrings("base_model.model.encoder.encoder.layer.0.attention.self.query_proj.lora_A.weight", inspected.tensors[0].adapter_a_tensor_name);
}

test "gliner2 classifier task head reloads and scores golden hidden rows" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_task_head_score_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, task_head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const weight = [_]f32{
        1.0,  0.0,  0.5,  -1.0,
        0.25, 0.25, 0.25, 0.25,
        -1.0, 1.0,  0.0,  0.5,
    };
    const bias = [_]f32{ 0.1, -0.2, 0.0 };
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "classifier.weight", .shape = &.{ 3, 4 }, .data = &weight },
        .{ .name = "classifier.bias", .shape = &.{3}, .data = &bias },
    });

    var head = try loadClassifierTaskHead(allocator, checkpoint_path);
    defer head.deinit();
    try std.testing.expectEqual(@as(usize, 3), head.num_classes);
    try std.testing.expectEqual(@as(usize, 4), head.hidden_size);

    const hidden = [_]f32{
        2.0,  -1.0, 0.5, 1.0,
        -2.0, 3.0,  0.0, 2.0,
    };
    const logits = try head.scoreRowsAlloc(allocator, &hidden);
    defer allocator.free(logits);
    try std.testing.expectApproxEqAbs(@as(f32, 1.35), logits[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.425), logits[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -2.5), logits[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -3.9), logits[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.55), logits[4], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), logits[5], 1e-6);

    const predictions = try head.predictRowsAlloc(allocator, &hidden);
    defer allocator.free(predictions);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2 }, predictions);
}
