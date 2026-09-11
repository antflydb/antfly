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

//! Offline checkpoint operations shared by tools and training.

const std = @import("std");
const platform = @import("antfly_platform");
const compat = @import("../../io/compat.zig");
const lora = @import("../lora.zig");
const lora_init = @import("../lora_init.zig");
const peft = @import("../peft.zig");
const qlora_nf4 = @import("../qlora_nf4.zig");
const recursive_lora = @import("../recursive_lora.zig");
const safetensors = @import("../../models/safetensors.zig");
const tensor_mod = @import("../../backends/tensor.zig");

const DType = tensor_mod.DType;

const Tensor = tensor_mod.Tensor;

const tensor_access = @import("../../models/tensor_access.zig");
const weight_source = @import("../../models/weight_source.zig");
const c_file = @import("../../util/c_file.zig");

pub const artifact_family_version = "gemma4_lora/v1alpha1";

pub const checkpoint_file_name = "model.safetensors";

pub const adapter_checkpoint_file_name = "adapter_model.safetensors";

pub const hf_config_file_name = "config.json";

pub const adapter_config_file_name = "adapter_config.json";

pub const tokenizer_config_file_name = "tokenizer_config.json";

pub const tokenizer_file_name = "tokenizer.json";

pub const special_tokens_map_file_name = "special_tokens_map.json";

pub const default_lora_target_modules = [_][]const u8{
    "q_proj",
    "k_proj",
    "v_proj",
    "o_proj",
    "gate_proj",
    "up_proj",
    "down_proj",
};

pub const Variant = enum {
    merged,
    adapter_only,
    incomplete,
};

pub const Config = struct {
    model_type: ?[]const u8 = null,
    hidden_size: ?usize = null,
    num_hidden_layers: ?usize = null,
    num_attention_heads: ?usize = null,
    vocab_size: ?usize = null,
    torch_dtype: ?[]const u8 = null,
    dtype: ?[]const u8 = null,
    text_config: ?TextConfig = null,
};

pub const TextConfig = struct {
    model_type: ?[]const u8 = null,
    hidden_size: ?usize = null,
    num_hidden_layers: ?usize = null,
    num_attention_heads: ?usize = null,
    vocab_size: ?usize = null,
    dtype: ?[]const u8 = null,
    torch_dtype: ?[]const u8 = null,
};

pub const AdapterConfig = struct {
    base_model_name_or_path: ?[]const u8 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    r: ?usize = null,
    lora_alpha: ?f64 = null,
    target_modules: ?[]const []const u8 = null,
    target_preset: ?[]const u8 = null,
    use_dora: ?bool = null,
    init_lora_weights: ?[]const u8 = null,
    recursive_lora: ?recursive_lora.Config = null,
};

pub const TokenizerConfig = struct {
    model_max_length: ?f64 = null,
    tokenizer_class: ?[]const u8 = null,
};

pub const InspectionSummary = struct {
    artifact_family_version: []const u8,
    variant: Variant,
    model_dir: []const u8,
    checkpoint_path: ?[]const u8 = null,
    gguf_path: ?[]const u8 = null,
    adapter_checkpoint_path: ?[]const u8 = null,
    config_path: ?[]const u8 = null,
    adapter_config_path: ?[]const u8 = null,
    tokenizer_config_path: ?[]const u8 = null,
    tokenizer_path: ?[]const u8 = null,
    special_tokens_map_path: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    model_type: ?[]const u8 = null,
    hidden_size: ?usize = null,
    num_hidden_layers: ?usize = null,
    num_attention_heads: ?usize = null,
    vocab_size: ?usize = null,
    torch_dtype: ?[]const u8 = null,
    tokenizer_class: ?[]const u8 = null,
    tokenizer_model_max_length: ?usize = null,
    lora_rank: ?usize = null,
    lora_alpha: ?f64 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    target_preset: ?[]const u8 = null,
    use_dora: ?bool = null,
    init_lora_weights: ?[]const u8 = null,
    recursive_lora_enabled: bool = false,
    recursive_source_num_layers: ?usize = null,
    recursive_shared_block_size: ?usize = null,
    recursive_loop_count: ?usize = null,
    recursive_init_strategy: ?[]const u8 = null,
    has_merged_weights: bool = false,
    has_gguf_weights: bool = false,
    has_adapter_weights: bool = false,
    has_tokenizer: bool = false,
};

pub const ArtifactPaths = struct {
    allocator: std.mem.Allocator,
    model_dir: []u8,
    checkpoint_path: ?[]u8 = null,
    gguf_path: ?[]u8 = null,
    adapter_checkpoint_path: ?[]u8 = null,
    config_path: ?[]u8 = null,
    adapter_config_path: ?[]u8 = null,
    tokenizer_config_path: ?[]u8 = null,
    tokenizer_path: ?[]u8 = null,
    special_tokens_map_path: ?[]u8 = null,

    pub fn deinit(self: *ArtifactPaths) void {
        self.allocator.free(self.model_dir);
        if (self.checkpoint_path) |p| self.allocator.free(p);
        if (self.gguf_path) |p| self.allocator.free(p);
        if (self.adapter_checkpoint_path) |p| self.allocator.free(p);
        if (self.config_path) |p| self.allocator.free(p);
        if (self.adapter_config_path) |p| self.allocator.free(p);
        if (self.tokenizer_config_path) |p| self.allocator.free(p);
        if (self.tokenizer_path) |p| self.allocator.free(p);
        if (self.special_tokens_map_path) |p| self.allocator.free(p);
        self.* = undefined;
    }
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
    target_preset: ?peft.TargetPreset = null,
    use_dora: bool = false,
    init_lora_weights: ?[]const u8 = null,
    eva_stats_path: ?[]const u8 = null,
    lora_ga_stats_path: ?[]const u8 = null,
    layer_name: ?[]const u8 = null,
    recursive_shared_block_size: ?usize = null,
    recursive_init_strategy: []const u8 = "average_residual_svd",
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
    target_preset: ?[]const u8 = null,
    use_dora: bool = false,
    init_lora_weights: ?[]const u8 = null,
    eva_stats_path: ?[]const u8 = null,
    lora_ga_stats_path: ?[]const u8 = null,
    resolved_tensors: []LoRATargetTensor,
};

pub const RecursiveCompressedBaseOptions = struct {
    metadata_file_name: []const u8 = "recursive_lora_base_config.json",
};

pub const RecursiveCompressedBaseSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    source_checkpoint_path: []const u8,
    compressed_checkpoint_path: []const u8,
    metadata_path: []const u8,
    source_num_layers: usize,
    shared_block_size: usize,
    loop_count: usize,
    tensors_written: usize,
    tensors_skipped: usize,
    source_checkpoint_bytes: u64,
    compressed_checkpoint_bytes: u64,
    compression_ratio: f64,
};

pub const LoRATensorSummary = struct {
    base_tensor_name: []const u8,
    adapter_a_tensor_name: []const u8,
    adapter_b_tensor_name: []const u8,
    dora_magnitude_tensor_name: ?[]const u8 = null,
    module_name: []const u8,
    loop_index: ?usize = null,
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
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    target_preset: ?[]const u8 = null,
    use_dora: ?bool = null,
    init_lora_weights: ?[]const u8 = null,
    resolved_tensor_count: usize = 0,
    trainable_parameter_count: usize = 0,
    dora_magnitude_tensor_count: usize = 0,
    dora_magnitude_parameter_count: usize = 0,
    tensors: []LoRATensorSummary,
};

pub const MaterializeSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    output_checkpoint_path: []const u8,
    merged_lora_tensor_count: usize,
    merged_dora_tensor_count: usize,
    copied_base_tensor_count: usize,
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
        if (self.adapter_config_path) |p| self.allocator.free(p);
        if (self.base_model_name_or_path) |p| self.allocator.free(p);
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

pub const LoRAInitKind = enum {
    default,
    pissa,
    loftq_nf4,
    eva,
    lora_ga,
};

pub fn resolveArtifactPaths(allocator: std.mem.Allocator, input: []const u8) !ArtifactPaths {
    const stat = compat.cwd().statFile(compat.io(), input, .{}) catch return error.InputNotFound;
    const model_dir = if (stat.kind == .directory)
        try allocator.dupe(u8, input)
    else
        try allocator.dupe(u8, std.fs.path.dirname(input) orelse ".");
    errdefer allocator.free(model_dir);

    var paths = ArtifactPaths{
        .allocator = allocator,
        .model_dir = model_dir,
        .checkpoint_path = try optionalPathInDir(allocator, model_dir, checkpoint_file_name),
        .gguf_path = null,
        .adapter_checkpoint_path = try optionalPathInDir(allocator, model_dir, adapter_checkpoint_file_name),
        .config_path = try optionalPathInDir(allocator, model_dir, hf_config_file_name),
        .adapter_config_path = try optionalPathInDir(allocator, model_dir, adapter_config_file_name),
        .tokenizer_config_path = try optionalPathInDir(allocator, model_dir, tokenizer_config_file_name),
        .tokenizer_path = try optionalPathInDir(allocator, model_dir, tokenizer_file_name),
        .special_tokens_map_path = try optionalPathInDir(allocator, model_dir, special_tokens_map_file_name),
    };

    if (stat.kind == .file) {
        if (std.mem.eql(u8, std.fs.path.basename(input), checkpoint_file_name)) {
            if (paths.checkpoint_path) |p| allocator.free(p);
            paths.checkpoint_path = try allocator.dupe(u8, input);
        } else if (std.mem.endsWith(u8, input, ".gguf")) {
            paths.gguf_path = try allocator.dupe(u8, input);
        } else if (std.mem.eql(u8, std.fs.path.basename(input), adapter_checkpoint_file_name)) {
            if (paths.adapter_checkpoint_path) |p| allocator.free(p);
            paths.adapter_checkpoint_path = try allocator.dupe(u8, input);
        }
    }

    if (paths.gguf_path == null) {
        paths.gguf_path = try findDecoderGgufPathInDir(allocator, model_dir);
    }

    return paths;
}

pub fn inspectCheckpoint(allocator: std.mem.Allocator, input: []const u8) !InspectionSummary {
    var paths = try resolveArtifactPaths(allocator, input);
    defer paths.deinit();

    const config_bytes = if (paths.config_path) |p| try c_file.readFile(allocator, p) else null;
    defer if (config_bytes) |b| allocator.free(b);
    const adapter_config_bytes = if (paths.adapter_config_path) |p| try c_file.readFile(allocator, p) else null;
    defer if (adapter_config_bytes) |b| allocator.free(b);
    const tokenizer_config_bytes = if (paths.tokenizer_config_path) |p| try c_file.readFile(allocator, p) else null;
    defer if (tokenizer_config_bytes) |b| allocator.free(b);

    var parsed_config = if (config_bytes) |b|
        try std.json.parseFromSlice(Config, allocator, b, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_config) |*p| p.deinit();

    var parsed_adapter = if (adapter_config_bytes) |b|
        try std.json.parseFromSlice(AdapterConfig, allocator, b, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_adapter) |*p| p.deinit();

    var parsed_tokenizer = if (tokenizer_config_bytes) |b|
        try std.json.parseFromSlice(TokenizerConfig, allocator, b, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_tokenizer) |*p| p.deinit();

    const config = if (parsed_config) |*p| &p.value else null;
    const adapter_config = if (parsed_adapter) |*p| &p.value else null;
    const tokenizer_config = if (parsed_tokenizer) |*p| &p.value else null;
    const recursive_config = if (adapter_config) |ac| ac.recursive_lora else null;
    const text_config = if (config) |c| c.text_config else null;

    const variant: Variant = if (paths.checkpoint_path != null or paths.gguf_path != null) .merged else if (paths.adapter_checkpoint_path != null) .adapter_only else .incomplete;

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .variant = variant,
        .model_dir = try allocator.dupe(u8, paths.model_dir),
        .checkpoint_path = try dupeOptionalString(allocator, paths.checkpoint_path),
        .gguf_path = try dupeOptionalString(allocator, paths.gguf_path),
        .adapter_checkpoint_path = try dupeOptionalString(allocator, paths.adapter_checkpoint_path),
        .config_path = try dupeOptionalString(allocator, paths.config_path),
        .adapter_config_path = try dupeOptionalString(allocator, paths.adapter_config_path),
        .tokenizer_config_path = try dupeOptionalString(allocator, paths.tokenizer_config_path),
        .tokenizer_path = try dupeOptionalString(allocator, paths.tokenizer_path),
        .special_tokens_map_path = try dupeOptionalString(allocator, paths.special_tokens_map_path),
        .base_model_name_or_path = if (adapter_config) |ac| try dupeOptionalString(allocator, ac.base_model_name_or_path) else null,
        .model_type = if (text_config) |tc|
            try dupeOptionalString(allocator, tc.model_type orelse if (config) |c| c.model_type else null)
        else if (config) |c|
            try dupeOptionalString(allocator, c.model_type)
        else
            null,
        .hidden_size = if (text_config) |tc| tc.hidden_size orelse if (config) |c| c.hidden_size else null else if (config) |c| c.hidden_size else null,
        .num_hidden_layers = if (text_config) |tc| tc.num_hidden_layers orelse if (config) |c| c.num_hidden_layers else null else if (config) |c| c.num_hidden_layers else null,
        .num_attention_heads = if (text_config) |tc| tc.num_attention_heads orelse if (config) |c| c.num_attention_heads else null else if (config) |c| c.num_attention_heads else null,
        .vocab_size = if (text_config) |tc| tc.vocab_size orelse if (config) |c| c.vocab_size else null else if (config) |c| c.vocab_size else null,
        .torch_dtype = if (text_config) |tc|
            try dupeOptionalString(allocator, tc.torch_dtype orelse tc.dtype orelse if (config) |c| c.torch_dtype orelse c.dtype else null)
        else if (config) |c|
            try dupeOptionalString(allocator, c.torch_dtype orelse c.dtype)
        else
            null,
        .tokenizer_class = if (tokenizer_config) |tc| try dupeOptionalString(allocator, tc.tokenizer_class) else null,
        .tokenizer_model_max_length = if (tokenizer_config) |tc| blk: {
            const v = tc.model_max_length orelse break :blk null;
            // HF uses a sentinel ~1e30 to mean "no limit"; treat anything above usize max as null
            if (v <= 0 or v > @as(f64, @floatFromInt(std.math.maxInt(usize)))) break :blk null;
            break :blk @intFromFloat(v);
        } else null,
        .lora_rank = if (adapter_config) |ac| ac.r else null,
        .lora_alpha = if (adapter_config) |ac| ac.lora_alpha else null,
        .peft_type = if (adapter_config) |ac| try dupeOptionalString(allocator, ac.peft_type) else null,
        .task_type = if (adapter_config) |ac| try dupeOptionalString(allocator, ac.task_type) else null,
        .target_module_count = if (adapter_config) |ac| if (ac.target_modules) |items| items.len else 0 else 0,
        .target_modules = if (adapter_config) |ac| try dupeOptionalStringSlice(allocator, ac.target_modules) else null,
        .target_preset = if (adapter_config) |ac| try dupeOptionalString(allocator, ac.target_preset) else null,
        .use_dora = if (adapter_config) |ac| ac.use_dora else null,
        .init_lora_weights = if (adapter_config) |ac| try dupeOptionalString(allocator, ac.init_lora_weights) else null,
        .recursive_lora_enabled = if (recursive_config) |rc| rc.enabled else false,
        .recursive_source_num_layers = if (recursive_config) |rc| if (rc.enabled) rc.source_num_layers else null else null,
        .recursive_shared_block_size = if (recursive_config) |rc| if (rc.enabled) rc.shared_block_size else null else null,
        .recursive_loop_count = if (recursive_config) |rc| if (rc.enabled) rc.loop_count else null else null,
        .recursive_init_strategy = if (recursive_config) |rc| if (rc.enabled) try allocator.dupe(u8, rc.init_strategy) else null else null,
        .has_merged_weights = paths.checkpoint_path != null or paths.gguf_path != null,
        .has_gguf_weights = paths.gguf_path != null,
        .has_adapter_weights = paths.adapter_checkpoint_path != null,
        .has_tokenizer = paths.tokenizer_path != null,
    };
}

pub fn freeInspectionSummary(allocator: std.mem.Allocator, summary: *InspectionSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    if (summary.checkpoint_path) |p| allocator.free(p);
    if (summary.gguf_path) |p| allocator.free(p);
    if (summary.adapter_checkpoint_path) |p| allocator.free(p);
    if (summary.config_path) |p| allocator.free(p);
    if (summary.adapter_config_path) |p| allocator.free(p);
    if (summary.tokenizer_config_path) |p| allocator.free(p);
    if (summary.tokenizer_path) |p| allocator.free(p);
    if (summary.special_tokens_map_path) |p| allocator.free(p);
    if (summary.base_model_name_or_path) |p| allocator.free(p);
    if (summary.model_type) |p| allocator.free(p);
    if (summary.torch_dtype) |p| allocator.free(p);
    if (summary.tokenizer_class) |p| allocator.free(p);
    if (summary.peft_type) |p| allocator.free(p);
    if (summary.task_type) |p| allocator.free(p);
    if (summary.target_preset) |p| allocator.free(p);
    if (summary.init_lora_weights) |p| allocator.free(p);
    if (summary.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    if (summary.recursive_init_strategy) |p| allocator.free(p);
    summary.* = undefined;
}

pub fn bootstrapLoRABundle(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    out_dir: []const u8,
    options: BootstrapOptions,
) !BootstrapSummary {
    var inspect = try inspectCheckpoint(allocator, model_input);
    defer freeInspectionSummary(allocator, &inspect);

    const checkpoint_path = inspect.checkpoint_path orelse inspect.gguf_path orelse return error.MissingMergedCheckpoint;
    if (options.rank == 0) return error.InvalidLoRARank;
    const recursive_config = try makeRecursiveConfig(inspect, options);
    try recursive_lora.validate(recursive_config);

    const requested_modules = options.target_modules orelse if (options.target_preset) |preset|
        peft.targetPresetPatterns(preset)
    else
        default_lora_target_modules[0..];
    const all_resolved_tensors = try inferLoRATargetTensorsForModelInput(allocator, model_input, checkpoint_path, requested_modules, options.target_preset);
    defer freeLoRATargetTensors(allocator, all_resolved_tensors);
    var filtered = std.ArrayListUnmanaged(LoRATargetTensor).empty;
    errdefer {
        for (filtered.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        filtered.deinit(allocator);
    }
    for (all_resolved_tensors) |item| {
        if (!layerMatchesScope(item.tensor_name, options.layer_name)) continue;
        if (recursive_config.enabled) {
            const layer_idx = parseGemma4LayerIndex(item.tensor_name) orelse continue;
            if (layer_idx >= recursive_config.shared_block_size) continue;
        }
        try filtered.append(allocator, .{
            .tensor_name = try allocator.dupe(u8, item.tensor_name),
            .module_name = try allocator.dupe(u8, item.module_name),
            .input_dim = item.input_dim,
            .output_dim = item.output_dim,
        });
    }
    const resolved_tensors = try filtered.toOwnedSlice(allocator);
    errdefer freeLoRATargetTensors(allocator, resolved_tensors);
    if (resolved_tensors.len == 0) return error.NoLoRATargetTensorsResolved;

    try compat.cwd().createDirPath(compat.io(), out_dir);

    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    errdefer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    errdefer allocator.free(adapter_config_path);

    const base_model_name_or_path = if (options.base_model_name_or_path) |v|
        try allocator.dupe(u8, v)
    else if (inspect.base_model_name_or_path) |v|
        try allocator.dupe(u8, v)
    else
        try allocator.dupe(u8, inspect.model_dir);
    errdefer allocator.free(base_model_name_or_path);

    try writeBootstrapAdapterCheckpointAtomic(allocator, adapter_checkpoint_path, checkpoint_path, resolved_tensors, options.rank, options.use_dora, options.init_lora_weights, options.eva_stats_path, options.lora_ga_stats_path, recursive_config);
    try writeAdapterConfigJson(allocator, adapter_config_path, .{
        .base_model_name_or_path = base_model_name_or_path,
        .rank = options.rank,
        .alpha = options.alpha,
        .target_modules = requested_modules,
        .target_preset = if (options.target_preset) |preset| targetPresetName(preset) else null,
        .use_dora = options.use_dora,
        .init_lora_weights = options.init_lora_weights,
        .recursive_lora = recursive_config,
    });
    try copySupportingArtifactIfPresent(allocator, inspect.tokenizer_config_path, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, inspect.tokenizer_path, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, inspect.special_tokens_map_path, out_dir, special_tokens_map_file_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, inspect.model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .checkpoint_path = try allocator.dupe(u8, checkpoint_path),
        .adapter_checkpoint_path = adapter_checkpoint_path,
        .adapter_config_path = adapter_config_path,
        .base_model_name_or_path = base_model_name_or_path,
        .lora_rank = options.rank,
        .lora_alpha = options.alpha,
        .target_modules = try dupeStringSlice(allocator, requested_modules),
        .target_preset = if (options.target_preset) |preset| try allocator.dupe(u8, targetPresetName(preset)) else null,
        .use_dora = options.use_dora,
        .init_lora_weights = try dupeOptionalString(allocator, options.init_lora_weights),
        .eva_stats_path = try dupeOptionalString(allocator, options.eva_stats_path),
        .lora_ga_stats_path = try dupeOptionalString(allocator, options.lora_ga_stats_path),
        .resolved_tensors = resolved_tensors,
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
    if (summary.target_preset) |value| allocator.free(value);
    if (summary.init_lora_weights) |value| allocator.free(value);
    if (summary.eva_stats_path) |value| allocator.free(value);
    if (summary.lora_ga_stats_path) |value| allocator.free(value);
    freeLoRATargetTensors(allocator, summary.resolved_tensors);
    summary.* = undefined;
}

fn makeRecursiveConfig(inspect: InspectionSummary, options: BootstrapOptions) !recursive_lora.Config {
    const shared_block_size = options.recursive_shared_block_size orelse return .{};
    const source_num_layers = inspect.num_hidden_layers orelse return error.InvalidRecursiveLoRAConfig;
    const loop_count = try recursive_lora.inferLoopCount(source_num_layers, shared_block_size);
    return .{
        .enabled = true,
        .source_num_layers = source_num_layers,
        .shared_block_size = shared_block_size,
        .loop_count = loop_count,
        .init_strategy = options.recursive_init_strategy,
    };
}

pub fn materializeRecursiveCompressedBase(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
    out_dir: []const u8,
    options: RecursiveCompressedBaseOptions,
) !RecursiveCompressedBaseSummary {
    var base_inspect = try inspectCheckpoint(allocator, base_model_input);
    defer freeInspectionSummary(allocator, &base_inspect);
    var adapter_inspect = try inspectCheckpoint(allocator, adapter_model_input);
    defer freeInspectionSummary(allocator, &adapter_inspect);

    if (!adapter_inspect.recursive_lora_enabled) return error.AdapterIsNotRecursiveLoRA;
    const source_num_layers = adapter_inspect.recursive_source_num_layers orelse return error.InvalidRecursiveLoRAConfig;
    const shared_block_size = adapter_inspect.recursive_shared_block_size orelse return error.InvalidRecursiveLoRAConfig;
    const loop_count = adapter_inspect.recursive_loop_count orelse return error.InvalidRecursiveLoRAConfig;
    try recursive_lora.validate(.{
        .enabled = true,
        .source_num_layers = source_num_layers,
        .shared_block_size = shared_block_size,
        .loop_count = loop_count,
        .init_strategy = adapter_inspect.recursive_init_strategy orelse "average_residual_svd",
    });

    const checkpoint_path = base_inspect.checkpoint_path orelse return error.MissingMergedCheckpoint;
    if (base_inspect.gguf_path != null) return error.UnsupportedRecursiveCompressedBaseSource;

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const compressed_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(compressed_checkpoint_path);
    const metadata_path = try std.fs.path.join(allocator, &.{ out_dir, options.metadata_file_name });
    errdefer allocator.free(metadata_path);

    var access = try openTensorAccessForFile(allocator, checkpoint_path);
    defer access.deinit();
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    var raw_tensors = std.ArrayListUnmanaged(WriteTensorRaw).empty;
    defer raw_tensors.deinit(allocator);
    var owned_records = std.ArrayListUnmanaged(tensor_access.Record).empty;
    defer {
        for (owned_records.items) |*record| record.deinit();
        owned_records.deinit(allocator);
    }

    var tensors_skipped: usize = 0;
    for (names) |name| {
        if (!keepTensorInRecursiveCompressedBase(name, shared_block_size)) {
            tensors_skipped += 1;
            continue;
        }
        var record = try access.getRecord(allocator, name);
        errdefer record.deinit();
        try raw_tensors.append(allocator, .{
            .name = record.descriptor.name,
            .dtype = denseRecordDType(record.descriptor.encoding) orelse return error.UnsupportedTensorEncoding,
            .shape = record.descriptor.shape,
            .raw_bytes = record.raw_bytes,
        });
        try owned_records.append(allocator, record);
    }
    if (raw_tensors.items.len == 0) return error.NoTensorsSelected;

    try writeHeaderAndRawTensors(allocator, compressed_checkpoint_path, raw_tensors.items);
    try copyCompressedBaseSupportFiles(allocator, base_inspect.model_dir, out_dir);
    const source_checkpoint_bytes = try c_file.fileSize(allocator, checkpoint_path);
    const compressed_checkpoint_bytes = try c_file.fileSize(allocator, compressed_checkpoint_path);
    const compression_ratio = if (source_checkpoint_bytes == 0)
        0
    else
        @as(f64, @floatFromInt(compressed_checkpoint_bytes)) / @as(f64, @floatFromInt(source_checkpoint_bytes));

    try writeRecursiveCompressedBaseMetadata(
        allocator,
        metadata_path,
        base_inspect.model_dir,
        adapter_inspect.model_dir,
        checkpoint_path,
        compressed_checkpoint_path,
        source_num_layers,
        shared_block_size,
        loop_count,
        raw_tensors.items.len,
        tensors_skipped,
        source_checkpoint_bytes,
        compressed_checkpoint_bytes,
        compression_ratio,
    );

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, base_inspect.model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_inspect.model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .source_checkpoint_path = try allocator.dupe(u8, checkpoint_path),
        .compressed_checkpoint_path = compressed_checkpoint_path,
        .metadata_path = metadata_path,
        .source_num_layers = source_num_layers,
        .shared_block_size = shared_block_size,
        .loop_count = loop_count,
        .tensors_written = raw_tensors.items.len,
        .tensors_skipped = tensors_skipped,
        .source_checkpoint_bytes = source_checkpoint_bytes,
        .compressed_checkpoint_bytes = compressed_checkpoint_bytes,
        .compression_ratio = compression_ratio,
    };
}

pub fn freeRecursiveCompressedBaseSummary(allocator: std.mem.Allocator, summary: *RecursiveCompressedBaseSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.source_checkpoint_path);
    allocator.free(summary.compressed_checkpoint_path);
    allocator.free(summary.metadata_path);
    summary.* = undefined;
}

pub fn inspectLoRABundle(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
) !LoRABundleInspectionSummary {
    var base_inspect = try inspectCheckpoint(allocator, base_model_input);
    defer freeInspectionSummary(allocator, &base_inspect);
    var adapter_inspect = try inspectCheckpoint(allocator, adapter_model_input);
    defer freeInspectionSummary(allocator, &adapter_inspect);

    const base_checkpoint_path = base_inspect.checkpoint_path orelse base_inspect.gguf_path orelse return error.MissingMergedCheckpoint;
    const adapter_checkpoint_path = adapter_inspect.adapter_checkpoint_path orelse return error.MissingAdapterCheckpoint;

    var base_access = try openTensorAccessForFile(allocator, base_checkpoint_path);
    defer base_access.deinit();
    var adapter_access = try openTensorAccessForFile(allocator, adapter_checkpoint_path);
    defer adapter_access.deinit();

    var tensors: std.ArrayListUnmanaged(LoRATensorSummary) = .empty;
    errdefer {
        for (tensors.items) |*item| freeLoRATensorSummary(allocator, item);
        tensors.deinit(allocator);
    }

    const adapter_names = try adapter_access.listNames(allocator);
    defer allocator.free(adapter_names);
    for (adapter_names) |adapter_a_name| {
        const parsed = parseLoRAAdapterTensorName(adapter_a_name) orelse continue;
        if (parsed.kind != .a) continue;

        const adapter_b_name = if (parsed.loop_index) |loop_idx|
            try recursive_lora.formatLoopAdapterTensorName(allocator, parsed.base_tensor_base_name, loop_idx, .b)
        else
            try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(adapter_b_name);
        const base_tensor_name = parsed.base_tensor_base_name;

        var adapter_a = try adapter_access.getRecord(allocator, adapter_a_name);
        defer adapter_a.deinit();
        var adapter_b = adapter_access.getRecord(allocator, adapter_b_name) catch return error.MissingAdapterPair;
        defer adapter_b.deinit();
        var base = base_access.getRecord(allocator, base_tensor_name) catch return error.MissingBaseTensorForAdapter;
        defer base.deinit();
        if (adapter_a.descriptor.shape.len != 2 or adapter_b.descriptor.shape.len != 2 or base.descriptor.shape.len != 2) return error.InvalidAdapterTensorShape;
        if (adapter_a.descriptor.shape[1] != base.descriptor.shape[1]) return error.AdapterInputDimMismatch;
        if (adapter_b.descriptor.shape[0] != base.descriptor.shape[0]) return error.AdapterOutputDimMismatch;
        if (adapter_a.descriptor.shape[0] != adapter_b.descriptor.shape[1]) return error.AdapterRankMismatch;

        const maybe_dora_name = try doraMagnitudeTensorName(allocator, base_tensor_name);
        defer allocator.free(maybe_dora_name);
        var dora_name_for_summary: ?[]const u8 = null;
        var dora_parameter_count: usize = 0;
        if (adapter_access.getRecord(allocator, maybe_dora_name)) |record| {
            var magnitude = record;
            defer magnitude.deinit();
            if (magnitude.descriptor.shape.len != 1) return error.InvalidAdapterTensorShape;
            if (magnitude.descriptor.shape[0] != base.descriptor.shape[0]) return error.AdapterOutputDimMismatch;
            dora_name_for_summary = try allocator.dupe(u8, maybe_dora_name);
            dora_parameter_count = @intCast(magnitude.descriptor.shape[0]);
        } else |err| switch (err) {
            error.TensorNotFound => {},
            else => return err,
        }

        try tensors.append(allocator, .{
            .base_tensor_name = try allocator.dupe(u8, base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, adapter_a_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, adapter_b_name),
            .dora_magnitude_tensor_name = dora_name_for_summary,
            .module_name = try allocator.dupe(u8, parsed.module_name),
            .loop_index = parsed.loop_index,
            .input_dim = @intCast(base.descriptor.shape[1]),
            .output_dim = @intCast(base.descriptor.shape[0]),
            .rank = @intCast(adapter_a.descriptor.shape[0]),
            .adapter_parameter_count = @as(usize, @intCast(adapter_a.descriptor.shape[0])) *
                @as(usize, @intCast(adapter_a.descriptor.shape[1])) +
                @as(usize, @intCast(adapter_b.descriptor.shape[0])) *
                    @as(usize, @intCast(adapter_b.descriptor.shape[1])) +
                dora_parameter_count,
            .dora_magnitude_parameter_count = dora_parameter_count,
        });
    }

    std.mem.sort(LoRATensorSummary, tensors.items, {}, struct {
        fn lt(_: void, a: LoRATensorSummary, b: LoRATensorSummary) bool {
            return std.mem.lessThan(u8, a.base_tensor_name, b.base_tensor_name);
        }
    }.lt);

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
        .base_model_dir = try allocator.dupe(u8, base_inspect.model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_inspect.model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, base_checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, adapter_checkpoint_path),
        .adapter_config_path = try dupeOptionalString(allocator, adapter_inspect.adapter_config_path),
        .base_model_name_or_path = try dupeOptionalString(allocator, adapter_inspect.base_model_name_or_path),
        .lora_rank = adapter_inspect.lora_rank,
        .lora_alpha = adapter_inspect.lora_alpha,
        .target_module_count = adapter_inspect.target_module_count,
        .target_modules = try dupeOptionalStringSlice(allocator, adapter_inspect.target_modules),
        .target_preset = try dupeOptionalString(allocator, adapter_inspect.target_preset),
        .use_dora = adapter_inspect.use_dora,
        .init_lora_weights = try dupeOptionalString(allocator, adapter_inspect.init_lora_weights),
        .resolved_tensor_count = tensors.items.len,
        .trainable_parameter_count = trainable_parameter_count,
        .dora_magnitude_tensor_count = dora_magnitude_tensor_count,
        .dora_magnitude_parameter_count = dora_magnitude_parameter_count,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
}

pub fn freeLoRABundleInspectionSummary(allocator: std.mem.Allocator, summary: *LoRABundleInspectionSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.base_checkpoint_path);
    allocator.free(summary.adapter_checkpoint_path);
    if (summary.adapter_config_path) |p| allocator.free(p);
    if (summary.base_model_name_or_path) |p| allocator.free(p);
    if (summary.target_preset) |p| allocator.free(p);
    if (summary.init_lora_weights) |p| allocator.free(p);
    if (summary.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
    for (summary.tensors) |*item| freeLoRATensorSummary(allocator, item);
    allocator.free(summary.tensors);
    summary.* = undefined;
}

pub fn loadLoRABundle(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
) !LoadedLoRABundle {
    return loadLoRABundleScoped(allocator, base_model_input, adapter_model_input, null);
}

pub fn loadLoRABundleScoped(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
    layer_name: ?[]const u8,
) !LoadedLoRABundle {
    var inspected = try inspectLoRABundle(allocator, base_model_input, adapter_model_input);
    defer freeLoRABundleInspectionSummary(allocator, &inspected);

    if (inspected.lora_rank == null or inspected.lora_alpha == null) return error.MissingAdapterConfig;

    var scoped_tensor_count: usize = 0;
    for (inspected.tensors) |ts| {
        if (!layerMatchesScope(ts.base_tensor_name, layer_name)) continue;
        scoped_tensor_count += 1;
    }
    const layers = try allocator.alloc(LoadedLoRALayer, scoped_tensor_count);
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

    var base_access = try openTensorAccessForFile(allocator, inspected.base_checkpoint_path);
    defer base_access.deinit();
    var adapter_access = try openTensorAccessForFile(allocator, inspected.adapter_checkpoint_path);
    defer adapter_access.deinit();

    for (inspected.tensors) |ts| {
        if (!layerMatchesScope(ts.base_tensor_name, layer_name)) continue;
        var a_tensor = try loadTensorAsF32(allocator, adapter_access, ts.adapter_a_tensor_name);
        defer a_tensor.deinit();
        var b_tensor = try loadTensorAsF32(allocator, adapter_access, ts.adapter_b_tensor_name);
        defer b_tensor.deinit();
        var base_tensor = try loadTensorAsF32(allocator, base_access, ts.base_tensor_name);
        defer base_tensor.deinit();
        if (a_tensor.shape.len != 2 or b_tensor.shape.len != 2 or base_tensor.shape.len != 2) {
            return error.InvalidAdapterTensorShape;
        }

        const adapter_a = try allocator.alloc(f32, ts.input_dim * ts.rank);
        errdefer allocator.free(adapter_a);
        transpose2DF32(adapter_a, a_tensor.asFloat32(), ts.rank, ts.input_dim);

        const adapter_b = try allocator.alloc(f32, ts.rank * ts.output_dim);
        errdefer allocator.free(adapter_b);
        transpose2DF32(adapter_b, b_tensor.asFloat32(), ts.output_dim, ts.rank);

        const base_weight = try allocator.alloc(f32, ts.input_dim * ts.output_dim);
        errdefer allocator.free(base_weight);
        transpose2DF32(base_weight, base_tensor.asFloat32(), ts.output_dim, ts.input_dim);

        var dora_magnitude_name: ?[]const u8 = null;
        var dora_magnitude: ?[]f32 = null;
        if (ts.dora_magnitude_tensor_name) |name| {
            var magnitude_tensor = try loadTensorAsF32(allocator, adapter_access, name);
            defer magnitude_tensor.deinit();
            if (magnitude_tensor.shape.len != 1 or magnitude_tensor.shape[0] != @as(i64, @intCast(ts.output_dim))) return error.InvalidAdapterTensorShape;
            const magnitude = try allocator.dupe(f32, magnitude_tensor.asFloat32());
            errdefer allocator.free(magnitude);
            dora_magnitude_name = try allocator.dupe(u8, name);
            errdefer if (dora_magnitude_name) |owned| allocator.free(owned);
            dora_magnitude = magnitude;
        }

        layers[loaded_count] = .{
            .base_tensor_name = try allocator.dupe(u8, ts.base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, ts.adapter_a_tensor_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, ts.adapter_b_tensor_name),
            .dora_magnitude_tensor_name = dora_magnitude_name,
            .module_name = try allocator.dupe(u8, ts.module_name),
            .input_dim = ts.input_dim,
            .output_dim = ts.output_dim,
            .rank = ts.rank,
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

    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);

    var tensors = try allocator.alloc(WriteTensorF32, bundle.layers.len * 3);
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
    var owned_data: std.ArrayListUnmanaged([]f32) = .empty;
    defer {
        for (owned_data.items) |item| allocator.free(item);
        owned_data.deinit(allocator);
    }

    var tensor_idx: usize = 0;
    for (bundle.layers) |layer| {
        // Transpose back to HuggingFace layout [rank, input_dim] and [output_dim, rank].
        const a_data = try allocator.alloc(f32, layer.adapter_a.len);
        transpose2DF32(a_data, layer.adapter_a, layer.input_dim, layer.rank);
        try owned_data.append(allocator, a_data);

        const b_data = try allocator.alloc(f32, layer.adapter_b.len);
        transpose2DF32(b_data, layer.adapter_b, layer.rank, layer.output_dim);
        try owned_data.append(allocator, b_data);

        const a_name = try allocator.dupe(u8, layer.adapter_a_tensor_name);
        const b_name = try allocator.dupe(u8, layer.adapter_b_tensor_name);
        try owned_names.append(allocator, a_name);
        try owned_names.append(allocator, b_name);

        const a_shape = try allocator.dupe(usize, &.{ layer.rank, layer.input_dim });
        const b_shape = try allocator.dupe(usize, &.{ layer.output_dim, layer.rank });
        try owned_shapes.append(allocator, a_shape);
        try owned_shapes.append(allocator, b_shape);

        tensors[tensor_idx] = .{ .name = a_name, .shape = a_shape, .data = a_data };
        tensor_idx += 1;
        tensors[tensor_idx] = .{ .name = b_name, .shape = b_shape, .data = b_data };
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

    try writeHeaderAndTensorsF32(allocator, adapter_checkpoint_path, tensors[0..tensor_idx]);

    // Write adapter_config.json if we have configuration.
    const adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    defer allocator.free(adapter_config_path);
    const base_name = bundle.base_model_name_or_path orelse bundle.base_model_dir;
    try writeAdapterConfigJson(allocator, adapter_config_path, .{
        .base_model_name_or_path = base_name,
        .rank = bundle.lora_rank,
        .alpha = bundle.lora_alpha,
        .target_modules = bundle.target_modules,
        .use_dora = bundleHasDoRA(bundle),
    });
}

pub fn materializeMergedModel(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    out_dir: []const u8,
) !MaterializeSummary {
    var bundle = try loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();

    var base_paths = try resolveArtifactPaths(allocator, base_model_dir);
    defer base_paths.deinit();
    const base_checkpoint_path = base_paths.checkpoint_path orelse base_paths.gguf_path orelse return error.MissingMergedCheckpoint;
    var base_access = try openTensorAccessForFile(allocator, base_checkpoint_path);
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
        defer allocator.free(merged_weight);
        const base_matrix = lora.Matrix{ .rows = layer.input_dim, .cols = layer.output_dim, .data = layer.base_weight };
        const adapter_a = lora.Matrix{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a };
        const adapter_b = lora.Matrix{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b };
        if (layer.dora_magnitude) |magnitude| {
            lora.doraMergeInto(.{
                .base = base_matrix,
                .adapter_a = adapter_a,
                .adapter_b = adapter_b,
                .magnitude = magnitude,
                .alpha = bundle.lora_alpha,
            }, merged_weight);
            merged_dora_tensor_count += 1;
        } else {
            lora.mergeInto(base_matrix, adapter_a, adapter_b, bundle.lora_alpha, merged_weight);
        }

        const hf_weight = try allocator.alloc(f32, merged_weight.len);
        defer allocator.free(hf_weight);
        transpose2DF32(hf_weight, merged_weight, layer.input_dim, layer.output_dim);

        const shape = [_]i64{ @as(i64, @intCast(layer.output_dim)), @as(i64, @intCast(layer.input_dim)) };
        const tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, &shape, hf_weight);
        try merged.put(allocator, try allocator.dupe(u8, layer.base_tensor_name), tensor);
    }

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const output_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(output_checkpoint_path);
    const bytes = try buildMergedSafetensorsFile(allocator, base_access, base_names, &merged);
    defer allocator.free(bytes);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = output_checkpoint_path, .data = bytes });

    try copySupportingArtifactIfPresent(allocator, base_paths.config_path, out_dir, hf_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.tokenizer_config_path, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.tokenizer_path, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.special_tokens_map_path, out_dir, special_tokens_map_file_name);

    var adapter_paths = try resolveArtifactPaths(allocator, adapter_model_dir);
    defer adapter_paths.deinit();
    try copySupportingArtifactIfPresent(allocator, adapter_paths.tokenizer_config_path, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_paths.tokenizer_path, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_paths.special_tokens_map_path, out_dir, special_tokens_map_file_name);

    var copied_base_tensor_count: usize = 0;
    for (base_names) |name| {
        if (!merged.contains(name)) copied_base_tensor_count += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, bundle.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, bundle.adapter_model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .output_checkpoint_path = output_checkpoint_path,
        .merged_lora_tensor_count = bundle.layers.len,
        .merged_dora_tensor_count = merged_dora_tensor_count,
        .copied_base_tensor_count = copied_base_tensor_count,
    };
}

pub fn freeMaterializeSummary(allocator: std.mem.Allocator, summary: *MaterializeSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.output_checkpoint_path);
    summary.* = undefined;
}

pub fn layerMatchesScope(layer_base_tensor_name: []const u8, layer_name: ?[]const u8) bool {
    const selector = layer_name orelse return true;
    if (parseLayerSelectorIndex(selector)) |want_idx| {
        return parseGemma4LayerIndex(layer_base_tensor_name) == want_idx;
    }
    return std.mem.indexOf(u8, layer_base_tensor_name, selector) != null;
}

pub fn parseGemma4LayerIndex(tensor_name: []const u8) ?usize {
    const prefix = "model.layers.";
    if (std.mem.indexOf(u8, tensor_name, prefix)) |idx| {
        const digits = tensor_name[idx + prefix.len ..];
        var end: usize = 0;
        while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
        if (end == 0) return null;
        return std.fmt.parseUnsigned(usize, digits[0..end], 10) catch null;
    }

    const gguf_prefix = "blk.";
    const gguf_idx = std.mem.indexOf(u8, tensor_name, gguf_prefix) orelse return null;
    const digits = tensor_name[gguf_idx + gguf_prefix.len ..];
    var end: usize = 0;
    while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseUnsigned(usize, digits[0..end], 10) catch null;
}

fn keepTensorInRecursiveCompressedBase(tensor_name: []const u8, shared_block_size: usize) bool {
    const layer_idx = parseGemma4LayerIndex(tensor_name) orelse return true;
    return layer_idx < shared_block_size;
}

fn parseLayerSelectorIndex(selector: []const u8) ?usize {
    const prefix = "model.layers.";
    if (!std.mem.startsWith(u8, selector, prefix)) return null;
    const digits = selector[prefix.len..];
    var end: usize = 0;
    while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
    if (end == 0) return null;
    if (end != digits.len) return null;
    return std.fmt.parseUnsigned(usize, digits, 10) catch null;
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

const WriteTensorRaw = struct {
    name: []const u8,
    dtype: tensor_mod.DType,
    shape: []const i64,
    raw_bytes: []const u8,
};

fn inferLoRATargetTensorsForModelInput(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    weights_path: []const u8,
    requested_modules: []const []const u8,
    target_preset: ?peft.TargetPreset,
) ![]LoRATargetTensor {
    _ = model_input;
    if (!std.mem.endsWith(u8, weights_path, ".gguf")) {
        return inferLoRATargetTensors(weights_path, allocator, requested_modules, target_preset);
    }

    var targets: std.ArrayListUnmanaged(LoRATargetTensor) = .empty;
    errdefer {
        for (targets.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        targets.deinit(allocator);
    }

    // GGUF target discovery only needs tensor metadata. Prefer libc's allocator
    // when available to avoid Debug allocator overhead on large headers.
    const gguf_allocator = platform.allocator.processAllocator(allocator);
    var access = try tensor_access.GgufAccess.initAbsolute(gguf_allocator, weights_path);
    defer access.tensorAccess().deinit();

    for (access.store.parsed.tensors) |tensor| {
        const tensor_name = tensor.name;
        const module_name = moduleNameForTensorWithPreset(tensor_name, target_preset) orelse continue;
        if (!targetMatchesRequest(tensor_name, module_name, requested_modules, target_preset)) continue;
        if (tensor.dimensions.len != 2) continue;
        try targets.append(allocator, .{
            .tensor_name = try allocator.dupe(u8, tensor_name),
            .module_name = try allocator.dupe(u8, module_name),
            .output_dim = @intCast(tensor.dimensions[1]),
            .input_dim = @intCast(tensor.dimensions[0]),
        });
    }

    std.mem.sort(LoRATargetTensor, targets.items, {}, struct {
        fn lt(_: void, a: LoRATargetTensor, b: LoRATargetTensor) bool {
            return std.mem.lessThan(u8, a.tensor_name, b.tensor_name);
        }
    }.lt);
    return targets.toOwnedSlice(allocator);
}

pub fn inferLoRATargetTensors(
    checkpoint_path: []const u8,
    allocator: std.mem.Allocator,
    requested_modules: []const []const u8,
    target_preset: ?peft.TargetPreset,
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
        const module_name = moduleNameForTensorWithPreset(tensor_name, target_preset) orelse continue;
        if (!targetMatchesRequest(tensor_name, module_name, requested_modules, target_preset)) continue;
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
        fn lt(_: void, a: LoRATargetTensor, b: LoRATargetTensor) bool {
            return std.mem.lessThan(u8, a.tensor_name, b.tensor_name);
        }
    }.lt);
    return targets.toOwnedSlice(allocator);
}

fn moduleNameForTensor(tensor_name: []const u8) ?[]const u8 {
    const ordered_modules = [_][]const u8{
        "q_proj",    "k_proj",  "v_proj",    "o_proj",
        "gate_proj", "up_proj", "down_proj",
    };
    inline for (ordered_modules) |module_name| {
        const dot_suffix = "." ++ module_name ++ ".weight";
        const slash_suffix = "/" ++ module_name ++ "/weight";
        if (std.mem.endsWith(u8, tensor_name, dot_suffix)) return module_name;
        if (std.mem.endsWith(u8, tensor_name, slash_suffix)) return module_name;
    }
    const gguf_aliases = [_]struct { suffix: []const u8, module_name: []const u8 }{
        .{ .suffix = ".attn_q.weight", .module_name = "q_proj" },
        .{ .suffix = ".attn_k.weight", .module_name = "k_proj" },
        .{ .suffix = ".attn_v.weight", .module_name = "v_proj" },
        .{ .suffix = ".attn_output.weight", .module_name = "o_proj" },
        .{ .suffix = ".ffn_gate.weight", .module_name = "gate_proj" },
        .{ .suffix = ".ffn_up.weight", .module_name = "up_proj" },
        .{ .suffix = ".ffn_down.weight", .module_name = "down_proj" },
    };
    inline for (gguf_aliases) |alias| {
        if (std.mem.endsWith(u8, tensor_name, alias.suffix)) return alias.module_name;
    }
    return null;
}

fn moduleNameForTensorWithPreset(tensor_name: []const u8, target_preset: ?peft.TargetPreset) ?[]const u8 {
    if (target_preset == .moe_experts and peft.matchesMoEExpertTensor(tensor_name)) return "moe_expert";
    return moduleNameForTensor(tensor_name);
}

fn targetMatchesRequest(
    tensor_name: []const u8,
    module_name: []const u8,
    requested_modules: []const []const u8,
    target_preset: ?peft.TargetPreset,
) bool {
    if (target_preset) |preset| return peft.matchesTargetPreset(tensor_name, preset);
    return stringSliceContains(requested_modules, module_name);
}

const LoRAAdapterTensorKind = enum { a, b };

const ParsedLoRAAdapterTensorName = struct {
    base_tensor_base_name: []const u8,
    module_name: []const u8,
    kind: LoRAAdapterTensorKind,
    loop_index: ?usize = null,
};

fn parseLoRAAdapterTensorName(tensor_name: []const u8) ?ParsedLoRAAdapterTensorName {
    if (recursive_lora.parseLoopAdapterTensorName(tensor_name)) |parsed| {
        const module = moduleNameForTensor(parsed.base_tensor_name) orelse return null;
        return .{
            .base_tensor_base_name = parsed.base_tensor_name,
            .module_name = module,
            .kind = if (parsed.kind == .a) .a else .b,
            .loop_index = parsed.loop_index,
        };
    }
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
    base_checkpoint_path: []const u8,
    resolved_tensors: []const LoRATargetTensor,
    rank: usize,
    use_dora: bool,
    init_lora_weights: ?[]const u8,
    eva_stats_path: ?[]const u8,
    lora_ga_stats_path: ?[]const u8,
    recursive_config: recursive_lora.Config,
) !void {
    const init_kind = try parseLoRAInitKind(init_lora_weights);
    const loop_count = if (recursive_config.enabled) recursive_config.loop_count else 1;
    const tensors_per_target: usize = (2 * loop_count) + if (use_dora) @as(usize, 1) else 0;
    var tensors = try allocator.alloc(WriteTensorF32, resolved_tensors.len * tensors_per_target);
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

    const needs_base_tensor = use_dora or init_kind == .pissa or init_kind == .loftq_nf4;
    var base_access: ?tensor_access.TensorAccess = null;
    if (needs_base_tensor) {
        base_access = try openTensorAccessForFile(allocator, base_checkpoint_path);
    }
    defer if (base_access) |*access| access.deinit();

    var eva_stats_access: ?tensor_access.TensorAccess = null;
    if (init_kind == .eva) {
        eva_stats_access = try openTensorAccessForFile(allocator, eva_stats_path orelse return error.MissingInitializerStats);
    }
    defer if (eva_stats_access) |*access| access.deinit();

    var lora_ga_stats_access: ?tensor_access.TensorAccess = null;
    if (init_kind == .lora_ga) {
        lora_ga_stats_access = try openTensorAccessForFile(allocator, lora_ga_stats_path orelse return error.MissingInitializerStats);
    }
    defer if (lora_ga_stats_access) |*access| access.deinit();

    var tensor_idx: usize = 0;
    for (resolved_tensors) |target| {
        var base_tensor: ?Tensor = null;
        if (needs_base_tensor) {
            base_tensor = try loadTensorAsF32(allocator, base_access.?, target.tensor_name);
            if (base_tensor.?.shape.len != 2 or
                base_tensor.?.shape[0] != @as(i64, @intCast(target.output_dim)) or
                base_tensor.?.shape[1] != @as(i64, @intCast(target.input_dim)))
            {
                base_tensor.?.deinit();
                return error.InvalidAdapterTensorShape;
            }
        }
        defer if (base_tensor) |*tensor| tensor.deinit();

        var eva_stats_tensor: ?Tensor = null;
        if (eva_stats_access) |access| {
            eva_stats_tensor = try loadInitializerStatsTensor(allocator, access, target.tensor_name, &.{
                ".eva_activation_covariance",
                ".activation_covariance",
            });
        }
        defer if (eva_stats_tensor) |*tensor| tensor.deinit();

        var lora_ga_stats_tensor: ?Tensor = null;
        if (lora_ga_stats_access) |access| {
            lora_ga_stats_tensor = try loadInitializerStatsTensor(allocator, access, target.tensor_name, &.{
                ".lora_ga_gradient",
                ".weight_gradient",
            });
        }
        defer if (lora_ga_stats_tensor) |*tensor| tensor.deinit();

        const init = try buildInitialLoRAFactors(
            allocator,
            init_kind,
            if (base_tensor) |tensor| tensor.asFloat32() else null,
            if (eva_stats_tensor) |tensor| tensor.asFloat32() else null,
            if (lora_ga_stats_tensor) |tensor| tensor.asFloat32() else null,
            target.output_dim,
            target.input_dim,
            rank,
        );
        const a_data = init.a;
        const b_data = init.b;
        try owned_data.append(allocator, a_data);
        try owned_data.append(allocator, b_data);

        const a_shape = try allocator.dupe(usize, &.{ rank, target.input_dim });
        const b_shape = try allocator.dupe(usize, &.{ target.output_dim, rank });
        try owned_shapes.append(allocator, a_shape);
        try owned_shapes.append(allocator, b_shape);

        for (0..loop_count) |loop_idx| {
            const a_name = if (recursive_config.enabled)
                try recursive_lora.formatLoopAdapterTensorName(allocator, target.tensor_name, loop_idx, .a)
            else
                try std.fmt.allocPrint(allocator, "{s}.lora_A.weight", .{target.tensor_name});
            errdefer allocator.free(a_name);
            const b_name = if (recursive_config.enabled)
                try recursive_lora.formatLoopAdapterTensorName(allocator, target.tensor_name, loop_idx, .b)
            else
                try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{target.tensor_name});
            errdefer allocator.free(b_name);
            try owned_names.append(allocator, a_name);
            try owned_names.append(allocator, b_name);

            tensors[tensor_idx] = .{ .name = a_name, .shape = a_shape, .data = a_data };
            tensor_idx += 1;
            tensors[tensor_idx] = .{ .name = b_name, .shape = b_shape, .data = b_data };
            tensor_idx += 1;
        }

        if (use_dora) {
            const base = base_tensor orelse return error.MissingBaseTensorForAdapter;
            const magnitude_data = try buildDoraMagnitudeFromBaseRowMajor(allocator, base.asFloat32(), target.output_dim, target.input_dim);
            try owned_data.append(allocator, magnitude_data);

            const magnitude_name = try doraMagnitudeTensorName(allocator, target.tensor_name);
            try owned_names.append(allocator, magnitude_name);

            const magnitude_shape = try allocator.dupe(usize, &.{target.output_dim});
            try owned_shapes.append(allocator, magnitude_shape);

            tensors[tensor_idx] = .{ .name = magnitude_name, .shape = magnitude_shape, .data = magnitude_data };
            tensor_idx += 1;
        }
    }

    try writeHeaderAndTensorsF32(allocator, output_path, tensors[0..tensor_idx]);
}

fn writeBootstrapAdapterCheckpointAtomic(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    base_checkpoint_path: []const u8,
    resolved_tensors: []const LoRATargetTensor,
    rank: usize,
    use_dora: bool,
    init_lora_weights: ?[]const u8,
    eva_stats_path: ?[]const u8,
    lora_ga_stats_path: ?[]const u8,
    recursive_config: recursive_lora.Config,
) !void {
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{output_path});
    defer allocator.free(tmp_path);
    compat.cwd().deleteFile(compat.io(), tmp_path) catch {};
    errdefer compat.cwd().deleteFile(compat.io(), tmp_path) catch {};

    try writeBootstrapAdapterCheckpoint(
        allocator,
        tmp_path,
        base_checkpoint_path,
        resolved_tensors,
        rank,
        use_dora,
        init_lora_weights,
        eva_stats_path,
        lora_ga_stats_path,
        recursive_config,
    );
    try std.Io.Dir.rename(compat.cwd(), tmp_path, compat.cwd(), output_path, compat.io());
}

const InitialLoRAFactors = struct {
    a: []f32,
    b: []f32,
};

pub fn parseLoRAInitKind(value: ?[]const u8) !LoRAInitKind {
    const text = value orelse return .default;
    if (std.mem.eql(u8, text, "default")) return .default;
    if (std.mem.eql(u8, text, "pissa")) return .pissa;
    if (std.mem.eql(u8, text, "eva")) return .eva;
    if (std.mem.eql(u8, text, "lora-ga") or std.mem.eql(u8, text, "loraga") or std.mem.eql(u8, text, "lora_ga")) return .lora_ga;
    if (std.mem.eql(u8, text, "loftq-nf4")) return .loftq_nf4;
    if (std.mem.eql(u8, text, "loftq")) return .loftq_nf4;
    return error.UnsupportedLoRAInitializer;
}

pub fn buildInitialLoRAFactors(
    allocator: std.mem.Allocator,
    init_kind: LoRAInitKind,
    base_weight: ?[]const f32,
    eva_activation_covariance: ?[]const f32,
    lora_ga_gradient: ?[]const f32,
    output_dim: usize,
    input_dim: usize,
    rank: usize,
) !InitialLoRAFactors {
    switch (init_kind) {
        .default => {
            return .{
                .a = try buildDeterministicLoraA(allocator, rank, input_dim),
                .b = try buildZeroF32(allocator, output_dim * rank),
            };
        },
        .pissa => {
            if (rank > @min(output_dim, input_dim)) return error.InvalidLoRARank;
            const base = base_weight orelse return error.MissingBaseTensorForAdapter;
            var result = try lora_init.pissaInit(allocator, base, output_dim, input_dim, rank, 2, 0x9e37_79b9);
            defer result.deinit();
            return .{
                .a = try allocator.dupe(f32, result.a),
                .b = try allocator.dupe(f32, result.b),
            };
        },
        .eva => {
            if (rank > input_dim) return error.InvalidLoRARank;
            const stats = eva_activation_covariance orelse return error.MissingInitializerStats;
            if (stats.len != input_dim * input_dim) return error.InvalidInitializerStatsShape;
            var result = try lora_init.evaInit(allocator, stats, output_dim, input_dim, rank, 4, 0x3e8a_0001);
            defer result.deinit();
            return .{
                .a = try allocator.dupe(f32, result.a),
                .b = try allocator.dupe(f32, result.b),
            };
        },
        .lora_ga => {
            if (rank > @min(output_dim, input_dim)) return error.InvalidLoRARank;
            const stats = lora_ga_gradient orelse return error.MissingInitializerStats;
            if (stats.len != output_dim * input_dim) return error.InvalidInitializerStatsShape;
            var result = try lora_init.loraGaInit(allocator, stats, output_dim, input_dim, rank, 1.0, 4, 0x6a09_e667);
            defer result.deinit();
            return .{
                .a = try allocator.dupe(f32, result.a),
                .b = try allocator.dupe(f32, result.b),
            };
        },
        .loftq_nf4 => {
            if (rank > @min(output_dim, input_dim)) return error.InvalidLoRARank;
            const base = base_weight orelse return error.MissingBaseTensorForAdapter;
            var result = try qlora_nf4.loftqNf4Init(allocator, base, output_dim, input_dim, .{
                .rank = rank,
                .num_iter = 1,
                .power_iters = 2,
                .seed = 0x10f7_0004,
            });
            defer result.deinit();
            return .{
                .a = try allocator.dupe(f32, result.a),
                .b = try allocator.dupe(f32, result.b),
            };
        },
    }
}

fn doraMagnitudeTensorName(allocator: std.mem.Allocator, base_tensor_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.lora_magnitude_vector.weight", .{base_tensor_name});
}

fn buildDoraMagnitudeFromBaseRowMajor(
    allocator: std.mem.Allocator,
    base_data: []const f32,
    rows: usize,
    cols: usize,
) ![]f32 {
    if (base_data.len != rows * cols) return error.InvalidAdapterTensorShape;
    const magnitude = try allocator.alloc(f32, rows);
    for (0..rows) |row| {
        const values = base_data[row * cols .. (row + 1) * cols];
        var sum: f32 = 0;
        for (values) |value| sum += value * value;
        magnitude[row] = @sqrt(sum + 1e-12);
    }
    return magnitude;
}

pub fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
    _ = allocator;
    var header_buf: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
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

    var file = try compat.cwd().createFile(compat.io(), path, .{ .truncate = true });
    defer file.close(compat.io());

    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(compat.io(), &len_buf);
    try file.writeStreamingAll(compat.io(), header_buf.written());
    for (tensors) |tensor| {
        for (tensor.data) |item| {
            const bits: u32 = @bitCast(item);
            var bits_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &bits_buf, bits, .little);
            try file.writeStreamingAll(compat.io(), &bits_buf);
        }
    }
}

fn writeHeaderAndRawTensors(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorRaw) !void {
    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;
    try writer.writeByte('{');
    var offset: u64 = 0;
    for (tensors, 0..) |tensor, idx| {
        if (idx != 0) try writer.writeByte(',');
        try writer.print("\"{s}\":{{\"dtype\":\"{s}\",\"shape\":[", .{ tensor.name, dtypeName(tensor.dtype) });
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        const byte_len: u64 = @intCast(tensor.raw_bytes.len);
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
    for (tensors) |tensor| try writeFileBytesChunked(io, &file, tensor.raw_bytes);
}

const AdapterConfigWriteOptions = struct {
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    target_modules: []const []const u8,
    target_preset: ?[]const u8 = null,
    use_dora: bool = false,
    init_lora_weights: ?[]const u8 = null,
    recursive_lora: recursive_lora.Config = .{},
};

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    options: AdapterConfigWriteOptions,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    if (options.recursive_lora.enabled) {
        try std.json.Stringify.value(.{
            .base_model_name_or_path = options.base_model_name_or_path,
            .peft_type = "LORA",
            .task_type = "CAUSAL_LM",
            .r = options.rank,
            .lora_alpha = options.alpha,
            .target_modules = options.target_modules,
            .target_preset = options.target_preset,
            .use_dora = options.use_dora,
            .init_lora_weights = options.init_lora_weights,
            .recursive_lora = .{
                .enabled = true,
                .source_num_layers = options.recursive_lora.source_num_layers,
                .shared_block_size = options.recursive_lora.shared_block_size,
                .loop_count = options.recursive_lora.loop_count,
                .init_strategy = options.recursive_lora.init_strategy,
            },
        }, .{ .whitespace = .indent_2 }, &buffer.writer);
    } else {
        try std.json.Stringify.value(.{
            .base_model_name_or_path = options.base_model_name_or_path,
            .peft_type = "LORA",
            .task_type = "CAUSAL_LM",
            .r = options.rank,
            .lora_alpha = options.alpha,
            .target_modules = options.target_modules,
            .target_preset = options.target_preset,
            .use_dora = options.use_dora,
            .init_lora_weights = options.init_lora_weights,
        }, .{ .whitespace = .indent_2 }, &buffer.writer);
    }
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn bundleHasDoRA(bundle: *const LoadedLoRABundle) bool {
    for (bundle.layers) |layer| {
        if (layer.dora_magnitude != null) return true;
    }
    return false;
}

fn targetPresetName(preset: peft.TargetPreset) []const u8 {
    return switch (preset) {
        .all_linear => "all-linear",
        .attention_only => "attention-only",
        .mlp_only => "mlp-only",
        .moe_experts => "moe-experts",
    };
}

fn copySupportingArtifactIfPresent(
    allocator: std.mem.Allocator,
    maybe_src_path: ?[]const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src_path = maybe_src_path orelse return;
    const dst_path = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst_path);

    const size = try c_file.fileSize(allocator, src_path);
    if (size == 0) {
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst_path, .data = "" });
        return;
    }
    if (size <= 100 * 1024 * 1024) {
        const contents = try c_file.readFile(allocator, src_path);
        defer allocator.free(contents);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst_path, .data = contents });
        return;
    }

    var mapped = try c_file.MmapRegion.init(allocator, src_path);
    defer mapped.deinit();
    mapped.adviseSequentialPrefix(mapped.data.len);

    const io = compat.io();
    var file = try compat.cwd().createFile(io, dst_path, .{ .truncate = true });
    defer file.close(io);
    try writeFileBytesChunked(io, &file, mapped.data);
}

fn copyCompressedBaseSupportFiles(allocator: std.mem.Allocator, base_model_dir: []const u8, out_dir: []const u8) !void {
    inline for (.{
        hf_config_file_name,
        tokenizer_config_file_name,
        tokenizer_file_name,
        special_tokens_map_file_name,
        "tokenizer.model",
        "generation_config.json",
        "preprocessor_config.json",
    }) |file_name| {
        const src_path = try std.fs.path.join(allocator, &.{ base_model_dir, file_name });
        defer allocator.free(src_path);
        copySupportingArtifactIfPresent(allocator, src_path, out_dir, file_name) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
}

fn writeRecursiveCompressedBaseMetadata(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    source_checkpoint_path: []const u8,
    compressed_checkpoint_path: []const u8,
    source_num_layers: usize,
    shared_block_size: usize,
    loop_count: usize,
    tensors_written: usize,
    tensors_skipped: usize,
    source_checkpoint_bytes: u64,
    compressed_checkpoint_bytes: u64,
    compression_ratio: f64,
) !void {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    try std.json.Stringify.value(.{
        .artifact_family_version = artifact_family_version,
        .base_model_dir = base_model_dir,
        .adapter_model_dir = adapter_model_dir,
        .source_checkpoint_path = source_checkpoint_path,
        .compressed_checkpoint_path = compressed_checkpoint_path,
        .source_num_layers = source_num_layers,
        .shared_block_size = shared_block_size,
        .loop_count = loop_count,
        .tensors_written = tensors_written,
        .tensors_skipped = tensors_skipped,
        .source_checkpoint_bytes = source_checkpoint_bytes,
        .compressed_checkpoint_bytes = compressed_checkpoint_bytes,
        .compression_ratio = compression_ratio,
    }, .{ .whitespace = .indent_2 }, &buf.writer);
    try buf.writer.writeByte('\n');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buf.written() });
}

fn writeFileBytesChunked(io: std.Io, file: *std.Io.File, bytes: []const u8) !void {
    const chunk_size: usize = 8 * 1024 * 1024;
    var offset: usize = 0;
    while (offset < bytes.len) {
        const end = @min(offset + chunk_size, bytes.len);
        try file.writeStreamingAll(io, bytes[offset..end]);
        offset = end;
    }
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

fn loadInitializerStatsTensor(
    allocator: std.mem.Allocator,
    access: tensor_access.TensorAccess,
    base_tensor_name: []const u8,
    suffixes: []const []const u8,
) !Tensor {
    for (suffixes) |suffix| {
        const name = try std.fmt.allocPrint(allocator, "{s}{s}", .{ base_tensor_name, suffix });
        defer allocator.free(name);
        return loadTensorAsF32(allocator, access, name) catch |err| switch (err) {
            error.TensorNotFound => continue,
            else => return err,
        };
    }
    return error.MissingInitializerStats;
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
        const byte_len = tensor.data.len;
        if (idx != 0) try header_buf.writer.writeByte(',');
        try header_buf.writer.print("\"{s}\":{{\"dtype\":\"{s}\",\"shape\":[", .{ name, dtypeName(tensor.dtype) });
        for (tensor.shape, 0..) |dim, dim_idx| {
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

fn denseRecordDType(encoding: tensor_access.Encoding) ?DType {
    return switch (encoding) {
        .dense => |dtype| dtype,
        .gguf => null,
    };
}

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    if (std.mem.endsWith(u8, path, ".index.json")) {
        const access = try tensor_access.ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    if (std.mem.endsWith(u8, path, ".gguf")) {
        // GGUF access objects mostly serve metadata lookup and lazy record fetch.
        // Prefer libc's allocator when available to avoid Debug allocator overhead
        // while parsing and later deinitializing large headers.
        const gguf_allocator = platform.allocator.processAllocator(allocator);
        const access = try tensor_access.GgufAccess.initAbsolute(gguf_allocator, path);
        return access.tensorAccess();
    }
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
}

fn transpose2DF32(out: []f32, input: []const f32, rows: usize, cols: usize) void {
    std.debug.assert(out.len == rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            out[col * rows + row] = input[row * cols + col];
        }
    }
}

fn buildDeterministicLoraA(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]f32 {
    const data = try allocator.alloc(f32, rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            const idx = row * cols + col;
            const angle: f32 = @floatFromInt((row + 1) * (col + 3));
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

fn optionalPathInDir(allocator: std.mem.Allocator, dir_path: []const u8, basename: []const u8) !?[]u8 {
    const path = try std.fs.path.join(allocator, &.{ dir_path, basename });
    errdefer allocator.free(path);
    compat.cwd().access(compat.io(), path, .{}) catch {
        allocator.free(path);
        return null;
    };
    return path;
}

pub fn findDecoderGgufPathInDir(allocator: std.mem.Allocator, dir_path: []const u8) !?[]u8 {
    var dir = compat.cwd().openDir(compat.io(), dir_path, .{ .iterate = true }) catch return null;
    defer dir.close(compat.io());

    var candidates = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (candidates.items) |path| allocator.free(path);
        candidates.deinit(allocator);
    }

    var iter = dir.iterate();
    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file and entry.kind != .sym_link) continue;
        if (!std.mem.endsWith(u8, entry.name, ".gguf")) continue;
        if (isProjectorGgufName(entry.name)) continue;

        const path = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
        errdefer allocator.free(path);
        try candidates.append(allocator, path);
    }

    if (candidates.items.len == 0) return null;
    if (candidates.items.len > 1) {
        std.mem.sort([]u8, candidates.items, {}, struct {
            fn lt(_: void, a: []u8, b: []u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);
        return error.AmbiguousDecoderGguf;
    }
    const only = candidates.items[0];
    candidates.items.len = 0;
    return only;
}

fn isProjectorGgufName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, "mmproj") != null or
        std.mem.indexOf(u8, name, "projector") != null;
}

pub fn freeLoRATargetTensors(allocator: std.mem.Allocator, tensors: []LoRATargetTensor) void {
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

pub fn dupeOptionalString(allocator: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    const item = value orelse return null;
    return try allocator.dupe(u8, item);
}

fn stringSliceContains(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}
