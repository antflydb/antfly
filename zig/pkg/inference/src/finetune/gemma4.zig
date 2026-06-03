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

// Gemma4 text-only LoRA finetuning module.
//
// Follows the same surrogate-gradient pattern as colqwen2.zig:
//   1. offline tokenization  (prepare-gemma4-lora-inputs)
//   2. surrogate gradient training (train-eval-gemma4-lora-bundle)
//
// The surrogate score hashes prompt/response token IDs into synthetic feature
// rows per LoRA layer, then uses a deterministic probe vector to measure the
// layer's response. The training loss is 0.5*(score - 1)^2, pushing the model
// to fit each given response.
const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const compat = @import("../io/compat.zig");
const lora = @import("lora.zig");
const lora_init = @import("lora_init.zig");
const peft = @import("peft.zig");
const qlora_nf4 = @import("qlora_nf4.zig");
const graph_bridge = @import("graph_bridge.zig");
const recursive_lora = @import("recursive_lora.zig");
const safetensors = @import("../models/safetensors.zig");
const tensor_mod = @import("../backends/tensor.zig");
const DType = tensor_mod.DType;
const Tensor = tensor_mod.Tensor;
const tensor_access = @import("../models/tensor_access.zig");
const manifest_mod = @import("../models/manifest.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source = @import("../models/weight_source.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const tokenizer_mod = @import("inference_tokenizer");
const c_file = @import("../util/c_file.zig");
const ml = @import("ml");
const optimizers = ml.graph.optimizers;
const native_compute = @import("../ops/native_compute.zig");
const gemma_data = @import("gemma_data.zig");
const gemma_chat_data = @import("gemma_chat_data.zig");
const chat_template = @import("chat_template.zig");
const gemma4_mm = @import("../architectures/gemma4_multimodal.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const model_manager_mod = @import("../server/model_manager.zig");

pub const artifact_family_version = "gemma4_lora/v1alpha1";
pub const prepared_schema_v2 = "gemma4_prepared/v2";
pub const prepared_schema_v3 = "gemma4_prepared/v3";
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

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub const Variant = enum {
    merged,
    adapter_only,
    incomplete,
};

pub const PreparedExampleInput = struct {
    mode: gemma_data.Mode,
    prompt_input_ids: []i32,
    response_input_ids: []i32,
    num_prompt_tokens: usize,
    num_response_tokens: usize,
    input_ids: []i32 = &.{},
    labels: []i32 = &.{},
    num_input_tokens: usize = 0,
    num_supervised_tokens: usize = 0,
    turn_count: usize = 0,
    has_tool_calls: bool = false,
    has_tool_messages: bool = false,
    image_paths: []const []const u8 = &.{},
    audio_paths: []const []const u8 = &.{},
    image_token_counts: []const usize = &.{},
    audio_token_counts: []const usize = &.{},
    teacher_top_k_token_ids: []i32 = &.{},
    teacher_top_k_probs: []f32 = &.{},
    teacher_top_k: usize = 0,
    teacher_temperature: f32 = 1.0,
    was_truncated: bool = false,
    turns_dropped_from_left: usize = 0,
    policy_version: ?[]const u8 = null,
};

pub const PreparedInputsSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    schema_version: []const u8 = prepared_schema_v2,
    gguf_projector_path: ?[]const u8 = null,
    gguf_projector_sha256: ?[]const u8 = null,
    gguf_projector_size_bytes: ?u64 = null,
    max_examples: usize,
    examples_seen: usize,
    tokenizer_class: ?[]const u8 = null,
    max_seq_len: usize = 512,
    max_prompt_tokens: usize = 0,
    max_response_tokens: usize = 0,
    max_input_tokens: usize = 0,
    max_supervised_tokens: usize = 0,
    examples_with_tool_calls: usize = 0,
    examples_with_tool_messages: usize = 0,
    examples_with_multiturn: usize = 0,
    examples_with_images: usize = 0,
    examples_with_audio: usize = 0,
    examples_truncated: usize = 0,
    max_turns_dropped: usize = 0,
    examples: []PreparedExampleInput,
};

pub const SurrogateMetrics = struct {
    examples_seen: usize = 0,
    examples_skipped_no_supervision: usize = 0,
    supervised_tokens_seen: usize = 0,
    average_loss: f64 = 0,
    mse: f64 = 0,
    mae: f64 = 0,
    mean_score: f64 = 0,
};

pub const ProjectorFingerprint = struct {
    path: []const u8,
    sha256: []const u8,
    size_bytes: u64,
};

const PrepareMediaKind = enum { image, audio };

const PrepareMediaTokenCache = struct {
    items: std.StringHashMapUnmanaged(usize) = .empty,

    fn deinit(self: *PrepareMediaTokenCache, allocator: std.mem.Allocator) void {
        var it = self.items.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        self.items.deinit(allocator);
        self.* = undefined;
    }
};

pub const TrainEpochOptions = struct {
    learning_rate: f32 = 0.001,
    max_examples: usize = 32,
    layer_name: ?[]const u8 = null,
    max_grad_norm: f32 = 1.0,
    grad_accum_steps: u32 = 1,
    llrd_decay: f32 = 1.0,
    use_schedule_free: bool = false,
    warmup_steps: u32 = 0,
    compute_backend: ?*const @import("../ops/ops.zig").ComputeBackend = null,
    mlx_dist_group: if (build_options.enable_mlx) ?@import("../backends/mlx.zig").DistributedGroup else void =
        if (build_options.enable_mlx) null else {},
    world_size: u32 = 1,
    ddp_rank: u32 = 0,
    pjrt_lora_steps: if (build_options.enable_pjrt) ?[]?graph_bridge.LoRAPjrtTrainStep else void =
        if (build_options.enable_pjrt) null else {},
};

pub const TrainEpochSummary = struct {
    examples_seen: usize = 0,
    examples_skipped_no_supervision: usize = 0,
    supervised_tokens_seen: usize = 0,
    updates_applied: usize = 0,
    average_loss: f64 = 0,
    mean_score: f64 = 0,
    mean_abs_error: f64 = 0,
    max_grad_norm: f32 = 0,
    llrd_decay: f32 = 0,
    grad_accum_steps: u32 = 0,
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

const EvalOptions = struct {
    max_examples: usize,
    layer_name: ?[]const u8 = null,
};

const LoRAInitKind = enum {
    default,
    pissa,
    loftq_nf4,
    eva,
    lora_ga,
};

const PreparedInputsSummaryFile = struct {
    summary: PreparedInputsSummary,
};

// ---------------------------------------------------------------------------
// Checkpoint inspection
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Load / save LoRA bundle
// ---------------------------------------------------------------------------

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

const LoadedGemmaTokenizer = union(enum) {
    hf: *hf_tokenizer.HfTokenizer,
    sp: *sentencepiece.Processor,

    fn deinit(self: *LoadedGemmaTokenizer, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .hf => |tok| tok.deinitSelf(),
            .sp => |sp| {
                sp.deinit();
                allocator.destroy(sp);
            },
        }
        self.* = undefined;
    }

    fn tokenizer(self: *const LoadedGemmaTokenizer) tokenizer_mod.Tokenizer {
        return switch (self.*) {
            .hf => |tok| tok.tokenizer(),
            .sp => |sp| sp.tokenizer(),
        };
    }
};

fn loadGemmaTokenizerForModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !LoadedGemmaTokenizer {
    const direct_gguf_path = try findDecoderGgufPathInDir(allocator, model_dir);
    defer if (direct_gguf_path) |path| allocator.free(path);
    if (direct_gguf_path) |gguf_path| {
        const has_hf_tokenizer = c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json") or
            c_file.fileExistsInDir(allocator, model_dir, "vocab.txt") or
            c_file.fileExistsInDir(allocator, model_dir, "vocab.json");
        if (!has_hf_tokenizer and !c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) {
            const sp = try model_manager_mod.loadSentencePieceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
            sp.setPreserveInlineSpecialsAfterLiteralBos(true);
            try model_manager_mod.loadSentencePieceAddedTokens(model_dir, allocator, sp);
            return .{ .sp = sp };
        }
    }

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    const tokenizer_type = blk: {
        if (model_manager_mod.shouldPreferSentencePieceOverride(manifest, model_dir, allocator)) {
            break :blk manifest_mod.TokenizerType.sentencepiece;
        }
        break :blk manifest.tokenizer_type orelse return error.NoTokenizerFound;
    };

    return switch (tokenizer_type) {
        .huggingface => .{
            .hf = try model_manager_mod.loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, manifest.gguf_path),
        },
        .sentencepiece => blk: {
            const sp = try model_manager_mod.loadSentencePieceTokenizerFromDirOrGguf(allocator, model_dir, manifest.gguf_path);
            if (model_manager_mod.shouldEnableGemmaSentencePieceCompat(manifest, model_dir, allocator)) {
                sp.setPreserveInlineSpecialsAfterLiteralBos(true);
            }
            try model_manager_mod.loadSentencePieceAddedTokens(model_dir, allocator, sp);
            break :blk .{ .sp = sp };
        },
    };
}

// ---------------------------------------------------------------------------
// Prepare inputs from text dataset
// ---------------------------------------------------------------------------

pub fn prepareInputsFromData(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    loaded_examples: []const gemma_data.Example,
    max_examples: usize,
    max_seq_len: usize,
) !PreparedInputsSummary {
    const chat_examples = try allocator.alloc(gemma_chat_data.Example, loaded_examples.len);
    var converted_count: usize = 0;
    defer {
        var i: usize = 0;
        while (i < converted_count) : (i += 1) allocator.free(chat_examples[i].messages);
        allocator.free(chat_examples);
    }
    for (loaded_examples, 0..) |ex, idx| {
        chat_examples[idx] = try legacyExampleToChat(allocator, ex);
        converted_count += 1;
    }
    return prepareInputsFromChatData(allocator, model_dir, chat_examples, max_examples, max_seq_len);
}

pub fn prepareInputsFromChatData(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    loaded_examples: []const gemma_chat_data.Example,
    max_examples: usize,
    max_seq_len: usize,
) !PreparedInputsSummary {
    var loaded_tokenizer = try loadGemmaTokenizerForModelDir(allocator, model_dir);
    defer loaded_tokenizer.deinit(allocator);
    const tok = loaded_tokenizer.tokenizer();

    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ model_dir, tokenizer_config_file_name });
    defer allocator.free(tokenizer_config_path);
    const tokenizer_config_bytes = if (isRegularFilePath(tokenizer_config_path))
        try c_file.readFile(allocator, tokenizer_config_path)
    else
        null;
    defer if (tokenizer_config_bytes) |b| allocator.free(b);
    var parsed_tokenizer_config = if (tokenizer_config_bytes) |b|
        try std.json.parseFromSlice(TokenizerConfig, allocator, b, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_tokenizer_config) |*p| p.deinit();
    const tokenizer_class = if (parsed_tokenizer_config) |*p| p.value.tokenizer_class else null;

    const limit = if (max_examples > 0 and max_examples < loaded_examples.len) max_examples else loaded_examples.len;

    const prepared = try allocator.alloc(PreparedExampleInput, limit);
    errdefer allocator.free(prepared);

    var summary = PreparedInputsSummary{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .max_examples = max_examples,
        .examples_seen = limit,
        .tokenizer_class = try dupeOptionalString(allocator, tokenizer_class),
        .max_seq_len = max_seq_len,
        .examples = prepared[0..0],
    };
    errdefer freePreparedInputsSummary(allocator, &summary);
    var prepared_count: usize = 0;

    for (loaded_examples[0..limit], 0..) |ex, idx| {
        const item = try tokenizeChatExample(allocator, tok, ex, max_seq_len);
        prepared[idx] = item;
        prepared_count += 1;
        summary.examples = prepared[0..prepared_count];
        summary.max_prompt_tokens = @max(summary.max_prompt_tokens, item.num_prompt_tokens);
        summary.max_response_tokens = @max(summary.max_response_tokens, item.num_response_tokens);
        summary.max_input_tokens = @max(summary.max_input_tokens, item.num_input_tokens);
        summary.max_supervised_tokens = @max(summary.max_supervised_tokens, item.num_supervised_tokens);
        if (item.has_tool_calls) summary.examples_with_tool_calls += 1;
        if (item.has_tool_messages) summary.examples_with_tool_messages += 1;
        if (item.turn_count > 2) summary.examples_with_multiturn += 1;
        if (item.image_paths.len > 0) summary.examples_with_images += 1;
        if (item.audio_paths.len > 0) summary.examples_with_audio += 1;
        if (item.was_truncated) summary.examples_truncated += 1;
        summary.max_turns_dropped = @max(summary.max_turns_dropped, item.turns_dropped_from_left);
    }

    return summary;
}

pub fn prepareMultimodalInputsFromChatData(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    gguf_projector_path: []const u8,
    loaded_examples: []const gemma_chat_data.Example,
    max_examples: usize,
    max_seq_len: usize,
) !PreparedInputsSummary {
    var loaded_tokenizer = try loadGemmaTokenizerForModelDir(allocator, model_dir);
    defer loaded_tokenizer.deinit(allocator);
    const tok = loaded_tokenizer.tokenizer();

    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ model_dir, tokenizer_config_file_name });
    defer allocator.free(tokenizer_config_path);
    const tokenizer_config_bytes = if (isRegularFilePath(tokenizer_config_path))
        try c_file.readFile(allocator, tokenizer_config_path)
    else
        null;
    defer if (tokenizer_config_bytes) |b| allocator.free(b);
    var parsed_tokenizer_config = if (tokenizer_config_bytes) |b|
        try std.json.parseFromSlice(TokenizerConfig, allocator, b, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_tokenizer_config) |*p| p.deinit();
    const tokenizer_class = if (parsed_tokenizer_config) |*p| p.value.tokenizer_class else null;

    const limit = if (max_examples > 0 and max_examples < loaded_examples.len) max_examples else loaded_examples.len;
    const prepared = try allocator.alloc(PreparedExampleInput, limit);
    errdefer allocator.free(prepared);
    const projector_fingerprint = try fingerprintProjectorFile(allocator, gguf_projector_path);
    defer freeProjectorFingerprint(allocator, &projector_fingerprint);

    var summary = PreparedInputsSummary{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .schema_version = prepared_schema_v3,
        .gguf_projector_path = try allocator.dupe(u8, projector_fingerprint.path),
        .gguf_projector_sha256 = try allocator.dupe(u8, projector_fingerprint.sha256),
        .gguf_projector_size_bytes = projector_fingerprint.size_bytes,
        .max_examples = max_examples,
        .examples_seen = limit,
        .tokenizer_class = try dupeOptionalString(allocator, tokenizer_class),
        .max_seq_len = max_seq_len,
        .examples = prepared[0..0],
    };
    errdefer freePreparedInputsSummary(allocator, &summary);
    var prepared_count: usize = 0;

    var dummy_ws = native_compute.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    defer dummy_ws.resident_weights.deinit(allocator);
    defer dummy_ws.lazy_weights.deinit(allocator);
    var native_engine = native_compute.NativeCompute.init(allocator, &dummy_ws, null);
    const projector_cb = native_engine.computeBackend();
    var media_token_cache = PrepareMediaTokenCache{};
    defer media_token_cache.deinit(allocator);

    for (loaded_examples[0..limit], 0..) |ex, idx| {
        const item = if (ex.image_paths.len == 0 and ex.audio_paths.len == 0)
            try tokenizeChatExample(allocator, tok, ex, max_seq_len)
        else
            try tokenizeMultimodalChatExample(allocator, tok, projector_cb, gguf_projector_path, projector_fingerprint.sha256, &media_token_cache, ex, max_seq_len);
        prepared[idx] = item;
        prepared_count += 1;
        summary.examples = prepared[0..prepared_count];
        summary.max_prompt_tokens = @max(summary.max_prompt_tokens, item.num_prompt_tokens);
        summary.max_response_tokens = @max(summary.max_response_tokens, item.num_response_tokens);
        summary.max_input_tokens = @max(summary.max_input_tokens, item.num_input_tokens);
        summary.max_supervised_tokens = @max(summary.max_supervised_tokens, item.num_supervised_tokens);
        if (item.has_tool_calls) summary.examples_with_tool_calls += 1;
        if (item.has_tool_messages) summary.examples_with_tool_messages += 1;
        if (item.turn_count > 2) summary.examples_with_multiturn += 1;
        if (item.image_paths.len > 0) summary.examples_with_images += 1;
        if (item.audio_paths.len > 0) summary.examples_with_audio += 1;
        if (item.was_truncated) summary.examples_truncated += 1;
        summary.max_turns_dropped = @max(summary.max_turns_dropped, item.turns_dropped_from_left);
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

pub fn freePreparedInputsSummary(allocator: std.mem.Allocator, summary: *const PreparedInputsSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    if (summary.gguf_projector_path) |p| allocator.free(p);
    if (summary.gguf_projector_sha256) |p| allocator.free(p);
    if (summary.tokenizer_class) |p| allocator.free(p);
    for (summary.examples) |*item| freePreparedExampleInput(allocator, item);
    allocator.free(summary.examples);
}

pub fn freeProjectorFingerprint(allocator: std.mem.Allocator, fingerprint: *const ProjectorFingerprint) void {
    allocator.free(fingerprint.path);
    allocator.free(fingerprint.sha256);
}

pub fn fingerprintProjectorFile(allocator: std.mem.Allocator, projector_path: []const u8) !ProjectorFingerprint {
    var mapped = try c_file.MmapRegion.init(allocator, projector_path);
    defer mapped.deinit();
    const sha256 = try sha256HexAlloc(allocator, mapped.data);
    errdefer allocator.free(sha256);
    return .{
        .path = try allocator.dupe(u8, projector_path),
        .sha256 = sha256,
        .size_bytes = mapped.data.len,
    };
}

pub fn sha256HexAlloc(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(bytes);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return allocator.dupe(u8, &hex);
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

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
    var total_score: f64 = 0;
    var total_abs_error: f64 = 0;
    var total_token_weight: f64 = 0;

    for (examples[0..limit]) |*example| {
        if (example.num_supervised_tokens == 0) {
            metrics.examples_skipped_no_supervision += 1;
            continue;
        }
        const token_weight: f64 = @floatFromInt(example.num_supervised_tokens);
        const predicted = try scorePreparedExample(allocator, bundle, example, options.layer_name);
        const target = exampleTarget(example);
        const err = predicted - target;
        const loss = 0.5 * err * err;
        total_loss += loss * token_weight;
        total_abs_error += @abs(err) * token_weight;
        total_score += predicted * token_weight;
        metrics.examples_seen += 1;
        metrics.supervised_tokens_seen += example.num_supervised_tokens;
        total_token_weight += token_weight;
    }

    if (total_token_weight > 0) {
        const denom = total_token_weight;
        metrics.average_loss = total_loss / denom;
        metrics.mse = (total_loss * 2.0) / denom;
        metrics.mae = total_abs_error / denom;
        metrics.mean_score = total_score / denom;
    }
    return metrics;
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

// ---------------------------------------------------------------------------
// Training loop
// ---------------------------------------------------------------------------

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

pub fn trainPreparedExamplesEpoch(
    allocator: std.mem.Allocator,
    bundle: *LoadedLoRABundle,
    examples: []const PreparedExampleInput,
    options: TrainEpochOptions,
) !TrainEpochSummary {
    var summary = TrainEpochSummary{
        .max_grad_norm = options.max_grad_norm,
        .llrd_decay = options.llrd_decay,
        .grad_accum_steps = options.grad_accum_steps,
    };
    const limit = if (options.max_examples > 0 and options.max_examples < examples.len) options.max_examples else examples.len;
    if (limit == 0) return summary;

    const num_layers = bundle.layers.len;

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

    const accum_grad_a = try allocator.alloc([]f32, num_layers);
    defer allocator.free(accum_grad_a);
    const accum_grad_b = try allocator.alloc([]f32, num_layers);
    defer allocator.free(accum_grad_b);
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

    var max_layer_idx: usize = 0;
    for (bundle.layers) |*layer| {
        if (parseGemma4LayerIndex(layer.base_tensor_name)) |li| {
            if (li > max_layer_idx) max_layer_idx = li;
        }
    }

    const accum_steps = if (options.grad_accum_steps == 0) 1 else options.grad_accum_steps;
    var accum_count: u32 = 0;
    var accum_supervised_tokens: usize = 0;

    for (examples[0..limit], 0..) |*example, ex_idx| {
        if (example.num_supervised_tokens == 0) {
            summary.examples_skipped_no_supervision += 1;
            continue;
        }
        const is_last = (ex_idx == limit - 1);
        const token_weight: f64 = @floatFromInt(example.num_supervised_tokens);

        const predicted = try scorePreparedExample(allocator, bundle, example, options.layer_name);
        const target = exampleTarget(example);
        const error_value = predicted - target;
        const loss = 0.5 * error_value * error_value;
        summary.examples_seen += 1;
        summary.supervised_tokens_seen += example.num_supervised_tokens;
        summary.average_loss += loss * token_weight;
        summary.mean_score += predicted * token_weight;
        summary.mean_abs_error += @abs(error_value) * token_weight;

        for (bundle.layers, 0..) |*layer, li| {
            if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;

            const input_rows: usize = 4;
            const inputs = try buildLayerFeatureRows(allocator, layer.input_dim, input_rows, example);
            defer allocator.free(inputs);
            const probe = try buildProbeVector(allocator, layer.base_tensor_name, layer.output_dim);
            defer allocator.free(probe);
            const output_grads = try allocator.alloc(f32, input_rows * layer.output_dim);
            defer allocator.free(output_grads);

            const row_scale = @as(f32, @floatCast(error_value)) /
                @as(f32, @floatFromInt(input_rows * @max(layer.output_dim, 1)));
            for (0..input_rows) |row_idx| {
                for (0..layer.output_dim) |out_idx| {
                    output_grads[row_idx * layer.output_dim + out_idx] = probe[out_idx] * row_scale;
                }
            }

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
        accum_supervised_tokens += example.num_supervised_tokens;

        if (accum_count % accum_steps == 0 or is_last) {
            if (comptime build_options.enable_mlx) {
                if (options.mlx_dist_group) |group| {
                    const mlx_mod = @import("../backends/mlx.zig");
                    const stream_handle = mlx_mod.openDefaultStream();
                    defer stream_handle.deinit();
                    for (0..bundle.layers.len) |li| {
                        if (accum_grad_a[li].len > 0) try mlx_mod.allSumFloat32InPlaceOnStream(accum_grad_a[li], stream_handle.stream, group);
                        if (accum_grad_b[li].len > 0) try mlx_mod.allSumFloat32InPlaceOnStream(accum_grad_b[li], stream_handle.stream, group);
                    }
                }
            }

            const eff_world_size: u32 = if (comptime build_options.enable_mlx) options.world_size else 1;
            const token_denom = @max(accum_supervised_tokens, 1);
            const norm_factor = 1.0 / (@as(f32, @floatFromInt(token_denom)) * @as(f32, @floatFromInt(eff_world_size)));
            for (bundle.layers, 0..) |*layer, li| {
                if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;
                for (accum_grad_a[li]) |*g| g.* *= norm_factor;
                for (accum_grad_b[li]) |*g| g.* *= norm_factor;
            }

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

            for (bundle.layers, 0..) |*layer, li| {
                if (!layerMatchesScope(layer.base_tensor_name, options.layer_name)) continue;

                var layer_lr = options.learning_rate;
                if (options.llrd_decay < 1.0) {
                    const layer_depth = parseGemma4LayerIndex(layer.base_tensor_name) orelse max_layer_idx;
                    const depth_from_top: f32 = @floatFromInt(max_layer_idx - @min(layer_depth, max_layer_idx));
                    layer_lr = options.learning_rate * std.math.pow(f32, options.llrd_decay, depth_from_top);
                }

                if (options.use_schedule_free) {
                    if (sf_states[li]) |*sf| {
                        sf.step += 1;
                        const lr = warmupAdjustedLR(layer_lr, sf.step, options.warmup_steps);
                        applyScheduleFreeInPlace(layer.adapter_a, accum_grad_a[li], sf.z_a, sf.v_a, sf.step, lr);
                        applyScheduleFreeInPlace(layer.adapter_b, accum_grad_b[li], sf.z_b, sf.v_b, sf.step, lr);
                    }
                } else {
                    adam_states[li].step += 1;
                    const lr = warmupAdjustedLR(layer_lr, adam_states[li].step, options.warmup_steps);
                    applyAdamWInPlace(layer.adapter_a, accum_grad_a[li], adam_states[li].m_a, adam_states[li].v_a, adam_states[li].step, lr);
                    applyAdamWInPlace(layer.adapter_b, accum_grad_b[li], adam_states[li].m_b, adam_states[li].v_b, adam_states[li].step, lr);
                }
                summary.updates_applied += 1;
            }

            for (0..num_layers) |li| {
                @memset(accum_grad_a[li], 0);
                @memset(accum_grad_b[li], 0);
            }
            accum_count = 0;
            accum_supervised_tokens = 0;
        }
    }

    if (summary.supervised_tokens_seen > 0) {
        const denom: f64 = @floatFromInt(summary.supervised_tokens_seen);
        summary.average_loss /= denom;
        summary.mean_score /= denom;
        summary.mean_abs_error /= denom;
    }
    return summary;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn tokenizeExample(
    allocator: std.mem.Allocator,
    tok: anytype,
    example: gemma_data.Example,
    max_seq_len: usize,
) !PreparedExampleInput {
    const prompt_text = if (example.mode == .instruction) example.prompt else "";
    const response_text = example.response;

    var prompt_result = try tok.encodeForModel(allocator, prompt_text, max_seq_len);
    defer prompt_result.deinit();
    var response_result = try tok.encodeForModel(allocator, response_text, max_seq_len);
    defer response_result.deinit();

    const prompt_ids = try allocator.dupe(i32, prompt_result.ids);
    errdefer allocator.free(prompt_ids);
    const response_ids = try allocator.dupe(i32, response_result.ids);
    errdefer allocator.free(response_ids);

    return .{
        .mode = example.mode,
        .prompt_input_ids = prompt_ids,
        .response_input_ids = response_ids,
        .num_prompt_tokens = prompt_result.ids.len,
        .num_response_tokens = response_result.ids.len,
    };
}

fn legacyExampleToChat(allocator: std.mem.Allocator, example: gemma_data.Example) !gemma_chat_data.Example {
    if (example.mode == .instruction) {
        const messages = try allocator.alloc(gemma_chat_data.Message, 2);
        messages[0] = .{ .role = .user, .content = example.prompt };
        messages[1] = .{ .role = .assistant, .content = example.response };
        return .{ .messages = messages };
    }
    const messages = try allocator.alloc(gemma_chat_data.Message, 1);
    messages[0] = .{ .role = .assistant, .content = example.response };
    return .{ .messages = messages };
}

fn tokenizeChatExample(
    allocator: std.mem.Allocator,
    tok: anytype,
    example: gemma_chat_data.Example,
    max_seq_len: usize,
) !PreparedExampleInput {
    const selected = try selectRenderableGemmaMessageWindow(allocator, tok, example, max_seq_len);
    defer allocator.free(selected.messages);

    const render_messages = try allocator.alloc(chat_template.Message, selected.messages.len);
    defer allocator.free(render_messages);

    var tool_call_json_bufs = try allocator.alloc(?[]u8, selected.messages.len);
    defer allocator.free(tool_call_json_bufs);
    @memset(tool_call_json_bufs, null);
    defer {
        for (tool_call_json_bufs) |maybe| {
            if (maybe) |buf| allocator.free(buf);
        }
    }

    var has_tool_calls = false;
    var has_tool_messages = false;
    for (selected.messages, 0..) |msg, idx| {
        if (msg.tool_calls.len > 0) {
            has_tool_calls = true;
            tool_call_json_bufs[idx] = try stringifyToolCalls(allocator, msg.tool_calls);
        }
        if (msg.role == .tool) has_tool_messages = true;
        render_messages[idx] = .{
            .role = switch (msg.role) {
                .system => .system,
                .user => .user,
                .assistant => .assistant,
                .tool => .tool,
            },
            .content = msg.content,
            .name = msg.name,
            .tool_call_id = msg.tool_call_id,
            .tool_calls_json = tool_call_json_bufs[idx],
        };
    }

    var rendered = try chat_template.render(allocator, .gemma, render_messages, .{});
    defer rendered.deinit();

    var encoded = try tok.encodeForModel(allocator, rendered.text, max_seq_len);
    defer encoded.deinit();

    const input_ids = try allocator.dupe(i32, encoded.ids);
    errdefer allocator.free(input_ids);

    var labels = if (encoded.offsets) |offsets| blk: {
        const token_offsets = try allocator.alloc(usize, encoded.ids.len);
        defer allocator.free(token_offsets);
        for (offsets, 0..) |off, idx| token_offsets[idx] = off[0];
        break :blk try chat_template.makeCompletionLabels(allocator, input_ids, token_offsets, rendered.assistant_spans, -100);
    } else blk: {
        break :blk try makeCompletionLabelsWithoutOffsets(
            allocator,
            tok,
            render_messages,
            input_ids,
            max_seq_len,
        );
    };
    errdefer allocator.free(labels);

    var prompt_count: usize = 0;
    var response_count: usize = 0;
    var input_count: usize = 0;
    for (labels, encoded.attention_mask, 0..) |label, attn, idx| {
        if (attn == 0) {
            labels[idx] = -100;
            continue;
        }
        input_count += 1;
        if (label == -100) {
            prompt_count += 1;
        } else {
            response_count += 1;
        }
    }

    const prompt_ids = try allocator.alloc(i32, prompt_count);
    errdefer allocator.free(prompt_ids);
    const response_ids = try allocator.alloc(i32, response_count);
    errdefer allocator.free(response_ids);
    var p_idx: usize = 0;
    var r_idx: usize = 0;
    for (input_ids, labels, encoded.attention_mask) |id, label, attn| {
        if (attn == 0) continue;
        if (label == -100) {
            prompt_ids[p_idx] = id;
            p_idx += 1;
        } else {
            response_ids[r_idx] = id;
            r_idx += 1;
        }
    }

    return .{
        .mode = if (prompt_count > 0) .instruction else .completion,
        .prompt_input_ids = prompt_ids,
        .response_input_ids = response_ids,
        .num_prompt_tokens = prompt_count,
        .num_response_tokens = response_count,
        .input_ids = input_ids,
        .labels = labels,
        .num_input_tokens = input_count,
        .num_supervised_tokens = response_count,
        .turn_count = selected.messages.len,
        .has_tool_calls = has_tool_calls,
        .has_tool_messages = has_tool_messages,
        .image_paths = try cloneStringSlice(allocator, example.image_paths),
        .audio_paths = try cloneStringSlice(allocator, example.audio_paths),
        .was_truncated = selected.turns_dropped_from_left > 0 or encoded.ids.len == max_seq_len and selected.messages.len < example.messages.len,
        .turns_dropped_from_left = selected.turns_dropped_from_left,
        .policy_version = try dupeOptionalString(allocator, example.metadata.policy_version),
    };
}

fn tokenizeMultimodalChatExample(
    allocator: std.mem.Allocator,
    tok: anytype,
    cb: @import("../ops/ops.zig").ComputeBackend,
    gguf_projector_path: []const u8,
    gguf_projector_sha256: []const u8,
    media_token_cache: *PrepareMediaTokenCache,
    example: gemma_chat_data.Example,
    max_seq_len: usize,
) !PreparedExampleInput {
    try validateMultimodalExampleShape(example);
    const image_bytes = try loadMediaBytes(allocator, example.image_paths);
    defer freeMediaBytes(allocator, image_bytes);
    const audio_bytes = try loadMediaBytes(allocator, example.audio_paths);
    defer freeMediaBytes(allocator, audio_bytes);

    const image_token_counts = try prepareMediaTokenCounts(allocator, media_token_cache, .image, &cb, gguf_projector_path, gguf_projector_sha256, image_bytes);
    defer allocator.free(image_token_counts);
    const audio_token_counts = try prepareMediaTokenCounts(allocator, media_token_cache, .audio, &cb, gguf_projector_path, gguf_projector_sha256, audio_bytes);
    defer allocator.free(audio_token_counts);

    var expanded = try expandMultimodalExample(
        allocator,
        example,
        image_token_counts,
        audio_token_counts,
    );
    defer freeExpandedMultimodalExample(allocator, &expanded);

    var prepared = try tokenizeChatExample(allocator, tok, expanded, max_seq_len);
    prepared.image_token_counts = try cloneUsizeSlice(allocator, image_token_counts);
    prepared.audio_token_counts = try cloneUsizeSlice(allocator, audio_token_counts);
    return prepared;
}

fn prepareMediaTokenCounts(
    allocator: std.mem.Allocator,
    cache: *PrepareMediaTokenCache,
    kind: PrepareMediaKind,
    cb: *const @import("../ops/ops.zig").ComputeBackend,
    gguf_projector_path: []const u8,
    gguf_projector_sha256: []const u8,
    items: []const []const u8,
) ![]usize {
    const counts = try allocator.alloc(usize, items.len);
    errdefer allocator.free(counts);
    for (items, 0..) |bytes, idx| {
        counts[idx] = try cachedPrepareMediaTokenCount(allocator, cache, kind, cb, gguf_projector_path, gguf_projector_sha256, bytes);
    }
    return counts;
}

fn cachedPrepareMediaTokenCount(
    allocator: std.mem.Allocator,
    cache: *PrepareMediaTokenCache,
    kind: PrepareMediaKind,
    cb: *const @import("../ops/ops.zig").ComputeBackend,
    gguf_projector_path: []const u8,
    gguf_projector_sha256: []const u8,
    bytes: []const u8,
) !usize {
    const media_sha256 = try sha256HexAlloc(allocator, bytes);
    defer allocator.free(media_sha256);
    const kind_name = switch (kind) {
        .image => "image",
        .audio => "audio",
    };
    const lookup_key = try std.fmt.allocPrint(allocator, "{s}:{s}:{s}", .{ gguf_projector_sha256, kind_name, media_sha256 });
    defer allocator.free(lookup_key);
    if (cache.items.get(lookup_key)) |tokens| return tokens;

    const tokens = switch (kind) {
        .image => blk: {
            var projected = try gemma4_projector.encodeProjectedImages(cb, allocator, gguf_projector_path, &.{bytes});
            defer projected.deinit();
            if (projected.tokens_per_image.len != 1) return error.InvalidPreparedPrompt;
            break :blk projected.tokens_per_image[0];
        },
        .audio => blk: {
            var projected = try gemma4_projector.encodeProjectedAudio(cb, allocator, gguf_projector_path, &.{bytes});
            defer projected.deinit();
            if (projected.tokens_per_audio.len != 1) return error.InvalidPreparedPrompt;
            break :blk projected.tokens_per_audio[0];
        },
    };
    const owned_key = try allocator.dupe(u8, lookup_key);
    errdefer allocator.free(owned_key);
    const entry = try cache.items.getOrPut(allocator, owned_key);
    if (entry.found_existing) {
        allocator.free(owned_key);
    } else {
        entry.key_ptr.* = owned_key;
        entry.value_ptr.* = tokens;
    }
    return entry.value_ptr.*;
}

fn validateMultimodalExampleShape(example: gemma_chat_data.Example) !void {
    var image_markers: usize = 0;
    var audio_markers: usize = 0;
    for (example.messages) |msg| {
        image_markers += countSubstring(msg.content, "<|image|>");
        audio_markers += countSubstring(msg.content, "<|audio|>");
    }
    if (image_markers != example.image_paths.len) return error.ImagePlaceholderCountMismatch;
    if (audio_markers != example.audio_paths.len) return error.AudioPlaceholderCountMismatch;
}

fn countSubstring(haystack: []const u8, needle: []const u8) usize {
    if (needle.len == 0) return 0;
    var count: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, haystack, pos, needle)) |idx| {
        count += 1;
        pos = idx + needle.len;
    }
    return count;
}

fn expandMultimodalExample(
    allocator: std.mem.Allocator,
    example: gemma_chat_data.Example,
    image_token_counts: []const usize,
    audio_token_counts: []const usize,
) !gemma_chat_data.Example {
    const messages = try allocator.alloc(gemma_chat_data.Message, example.messages.len);
    errdefer allocator.free(messages);

    var image_idx: usize = 0;
    var audio_idx: usize = 0;
    for (example.messages, 0..) |msg, idx| {
        const expanded_content = try expandMessageMediaMarkers(
            allocator,
            msg.content,
            image_token_counts,
            &image_idx,
            audio_token_counts,
            &audio_idx,
        );
        messages[idx] = .{
            .role = msg.role,
            .content = expanded_content,
            .tool_call_id = msg.tool_call_id,
            .name = msg.name,
            .tool_calls = msg.tool_calls,
        };
    }
    if (image_idx != image_token_counts.len) return error.ImagePlaceholderCountMismatch;
    if (audio_idx != audio_token_counts.len) return error.AudioPlaceholderCountMismatch;

    return .{
        .id = example.id,
        .messages = messages,
        .tools = example.tools,
        .image_paths = example.image_paths,
        .audio_paths = example.audio_paths,
        .metadata = example.metadata,
    };
}

fn freeExpandedMultimodalExample(allocator: std.mem.Allocator, example: *const gemma_chat_data.Example) void {
    for (example.messages) |msg| allocator.free(msg.content);
    allocator.free(example.messages);
}

fn expandMessageMediaMarkers(
    allocator: std.mem.Allocator,
    content: []const u8,
    image_token_counts: []const usize,
    image_idx: *usize,
    audio_token_counts: []const usize,
    audio_idx: *usize,
) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var cursor: usize = 0;
    while (cursor < content.len) {
        if (std.mem.startsWith(u8, content[cursor..], "<|image|>")) {
            if (image_idx.* >= image_token_counts.len) return error.ImagePlaceholderCountMismatch;
            try appendExpandedMarker(allocator, &out, "<|image>", "<|image|>", "<image|>", image_token_counts[image_idx.*]);
            image_idx.* += 1;
            cursor += "<|image|>".len;
            continue;
        }
        if (std.mem.startsWith(u8, content[cursor..], "<|audio|>")) {
            if (audio_idx.* >= audio_token_counts.len) return error.AudioPlaceholderCountMismatch;
            try appendExpandedMarker(allocator, &out, "<|audio>", "<|audio|>", "<audio|>", audio_token_counts[audio_idx.*]);
            audio_idx.* += 1;
            cursor += "<|audio|>".len;
            continue;
        }
        try out.append(allocator, content[cursor]);
        cursor += 1;
    }
    return try out.toOwnedSlice(allocator);
}

fn appendExpandedMarker(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    begin_marker: []const u8,
    marker: []const u8,
    end_marker: []const u8,
    token_count: usize,
) !void {
    try out.appendSlice(allocator, begin_marker);
    for (0..token_count) |_| try out.appendSlice(allocator, marker);
    try out.appendSlice(allocator, end_marker);
}

fn loadMediaBytes(allocator: std.mem.Allocator, paths: []const []const u8) ![]const []const u8 {
    if (paths.len == 0) return &.{};
    const out = try allocator.alloc([]const u8, paths.len);
    errdefer allocator.free(out);
    var loaded: usize = 0;
    errdefer {
        for (out[0..loaded]) |item| allocator.free(item);
    }
    for (paths, 0..) |path, idx| {
        out[idx] = try c_file.readFile(allocator, path);
        loaded += 1;
    }
    return out;
}

fn freeMediaBytes(allocator: std.mem.Allocator, data: []const []const u8) void {
    if (data.len == 0) return;
    for (data) |item| allocator.free(item);
    allocator.free(data);
}

fn cloneStringSlice(allocator: std.mem.Allocator, items: []const []const u8) ![]const []const u8 {
    if (items.len == 0) return &.{};
    const out = try allocator.alloc([]const u8, items.len);
    for (items, 0..) |item, idx| out[idx] = try allocator.dupe(u8, item);
    return out;
}

fn cloneUsizeSlice(allocator: std.mem.Allocator, items: []const usize) ![]const usize {
    if (items.len == 0) return &.{};
    return try allocator.dupe(usize, items);
}

fn cloneI32Slice(allocator: std.mem.Allocator, items: []const i32) ![]i32 {
    if (items.len == 0) return &.{};
    return try allocator.dupe(i32, items);
}

fn cloneF32Slice(allocator: std.mem.Allocator, items: []const f32) ![]f32 {
    if (items.len == 0) return &.{};
    return try allocator.dupe(f32, items);
}

fn stringifyToolCalls(allocator: std.mem.Allocator, tool_calls: []const gemma_chat_data.ToolCall) ![]u8 {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    try buf.writer.writeByte('[');
    for (tool_calls, 0..) |tool_call, idx| {
        if (idx != 0) try buf.writer.writeByte(',');
        try buf.writer.writeAll("{\"id\":");
        try std.json.Stringify.value(tool_call.id, .{}, &buf.writer);
        try buf.writer.writeAll(",\"type\":\"function\",\"function\":{\"name\":");
        try std.json.Stringify.value(tool_call.name, .{}, &buf.writer);
        try buf.writer.writeAll(",\"arguments\":");
        try std.json.Stringify.value(tool_call.arguments_json, .{}, &buf.writer);
        try buf.writer.writeAll("}}");
    }
    try buf.writer.writeByte(']');
    return try allocator.dupe(u8, buf.written());
}

fn makeCompletionLabelsWithoutOffsets(
    allocator: std.mem.Allocator,
    tok: anytype,
    render_messages: []const chat_template.Message,
    input_ids: []const i32,
    max_seq_len: usize,
) ![]i32 {
    if (render_messages.len == 0 or render_messages[render_messages.len - 1].role != .assistant) {
        return error.TokenOffsetsUnavailable;
    }
    var assistant_message_count: usize = 0;
    for (render_messages) |msg| {
        if (msg.role == .assistant) assistant_message_count += 1;
    }
    if (assistant_message_count != 1) return error.TokenOffsetsUnavailable;

    var prefix_rendered = try chat_template.render(
        allocator,
        .gemma,
        render_messages[0 .. render_messages.len - 1],
        .{ .add_generation_prompt = true },
    );
    defer prefix_rendered.deinit();

    var prefix_encoded = try tok.encodeForModel(allocator, prefix_rendered.text, max_seq_len);
    defer prefix_encoded.deinit();

    var prefix_len: usize = 0;
    for (prefix_encoded.attention_mask) |attn| {
        if (attn == 0) break;
        prefix_len += 1;
    }
    prefix_len = @min(prefix_len, input_ids.len);
    const labels = try allocator.alloc(i32, input_ids.len);
    for (input_ids, 0..) |id, idx| {
        labels[idx] = if (idx < prefix_len) -100 else id;
    }
    return labels;
}

fn freePreparedExampleInput(allocator: std.mem.Allocator, item: *const PreparedExampleInput) void {
    allocator.free(item.prompt_input_ids);
    allocator.free(item.response_input_ids);
    allocator.free(item.input_ids);
    allocator.free(item.labels);
    for (item.image_paths) |path| allocator.free(path);
    if (item.image_paths.len > 0) allocator.free(item.image_paths);
    for (item.audio_paths) |path| allocator.free(path);
    if (item.audio_paths.len > 0) allocator.free(item.audio_paths);
    if (item.image_token_counts.len > 0) allocator.free(item.image_token_counts);
    if (item.audio_token_counts.len > 0) allocator.free(item.audio_token_counts);
    if (item.teacher_top_k_token_ids.len > 0) allocator.free(item.teacher_top_k_token_ids);
    if (item.teacher_top_k_probs.len > 0) allocator.free(item.teacher_top_k_probs);
    if (item.policy_version) |p| allocator.free(p);
}

fn clonePreparedInputsSummary(allocator: std.mem.Allocator, source: *const PreparedInputsSummary) !PreparedInputsSummary {
    const normalized_schema = try normalizePreparedSchemaVersion(source.schema_version);
    const examples = try allocator.alloc(PreparedExampleInput, source.examples.len);
    var cloned_count: usize = 0;
    errdefer {
        for (examples[0..cloned_count]) |*item| freePreparedExampleInput(allocator, item);
        allocator.free(examples);
    }
    for (source.examples, 0..) |item, idx| {
        examples[idx] = .{
            .mode = item.mode,
            .prompt_input_ids = try allocator.dupe(i32, item.prompt_input_ids),
            .response_input_ids = try allocator.dupe(i32, item.response_input_ids),
            .num_prompt_tokens = item.num_prompt_tokens,
            .num_response_tokens = item.num_response_tokens,
            .input_ids = try allocator.dupe(i32, item.input_ids),
            .labels = try allocator.dupe(i32, item.labels),
            .num_input_tokens = item.num_input_tokens,
            .num_supervised_tokens = item.num_supervised_tokens,
            .turn_count = item.turn_count,
            .has_tool_calls = item.has_tool_calls,
            .has_tool_messages = item.has_tool_messages,
            .image_paths = try cloneStringSlice(allocator, item.image_paths),
            .audio_paths = try cloneStringSlice(allocator, item.audio_paths),
            .image_token_counts = try cloneUsizeSlice(allocator, item.image_token_counts),
            .audio_token_counts = try cloneUsizeSlice(allocator, item.audio_token_counts),
            .teacher_top_k_token_ids = try cloneI32Slice(allocator, item.teacher_top_k_token_ids),
            .teacher_top_k_probs = try cloneF32Slice(allocator, item.teacher_top_k_probs),
            .teacher_top_k = item.teacher_top_k,
            .teacher_temperature = item.teacher_temperature,
            .was_truncated = item.was_truncated,
            .turns_dropped_from_left = item.turns_dropped_from_left,
            .policy_version = try dupeOptionalString(allocator, item.policy_version),
        };
        cloned_count += 1;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, source.artifact_family_version),
        .model_dir = try allocator.dupe(u8, source.model_dir),
        .schema_version = normalized_schema,
        .gguf_projector_path = try dupeOptionalString(allocator, source.gguf_projector_path),
        .gguf_projector_sha256 = try dupeOptionalString(allocator, source.gguf_projector_sha256),
        .gguf_projector_size_bytes = source.gguf_projector_size_bytes,
        .max_examples = source.max_examples,
        .examples_seen = source.examples_seen,
        .tokenizer_class = try dupeOptionalString(allocator, source.tokenizer_class),
        .max_seq_len = source.max_seq_len,
        .max_prompt_tokens = source.max_prompt_tokens,
        .max_response_tokens = source.max_response_tokens,
        .max_input_tokens = source.max_input_tokens,
        .max_supervised_tokens = source.max_supervised_tokens,
        .examples_with_tool_calls = source.examples_with_tool_calls,
        .examples_with_tool_messages = source.examples_with_tool_messages,
        .examples_with_multiturn = source.examples_with_multiturn,
        .examples_with_images = source.examples_with_images,
        .examples_with_audio = source.examples_with_audio,
        .examples_truncated = source.examples_truncated,
        .max_turns_dropped = source.max_turns_dropped,
        .examples = examples,
    };
}

fn normalizePreparedSchemaVersion(schema_version: []const u8) ![]const u8 {
    if (std.mem.eql(u8, schema_version, prepared_schema_v2)) return prepared_schema_v2;
    if (std.mem.eql(u8, schema_version, prepared_schema_v3)) return prepared_schema_v3;
    return error.UnsupportedPreparedInputsSchema;
}

fn scoreLayerExample(
    allocator: std.mem.Allocator,
    layer: *const LoadedLoRALayer,
    alpha: f32,
    example: *const PreparedExampleInput,
) !f64 {
    const input_rows: usize = 4;
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
            var merged: f64 = 0;
            for (0..layer.input_dim) |i| {
                merged += @as(f64, row[i]) * @as(f64, layer.base_weight[j * layer.input_dim + i]);
            }
            row_score += merged * probe[j];
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
            const scaled = @as(f64, tmp_rank[r] * scale);
            const b_row = layer.adapter_b[r * layer.output_dim .. (r + 1) * layer.output_dim];
            for (b_row, 0..) |b, j| row_score += scaled * b * probe[j];
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
    const prompt_ids = if (example.prompt_input_ids.len > 0) example.prompt_input_ids else example.input_ids;
    const response_ids = if (example.response_input_ids.len > 0) example.response_input_ids else example.labels;
    // Row 0: prompt tokens
    hashTokenIdsIntoRow(rows[0..input_dim], prompt_ids, 1.0);
    // Row 1: response tokens
    if (input_rows > 1) hashNonIgnoreTokenIdsIntoRow(rows[input_dim .. input_dim * 2], response_ids, 0.8);
    // Row 2: combined prompt + response
    if (input_rows > 2) {
        hashTokenIdsIntoRow(rows[input_dim * 2 .. input_dim * 3], prompt_ids, 0.5);
        hashNonIgnoreTokenIdsIntoRow(rows[input_dim * 2 .. input_dim * 3], response_ids, 0.5);
    }
    // Row 3: causal transitions across supervised assistant tokens.
    if (input_rows > 3) {
        hashSupervisedTokenTransitionsIntoRow(
            rows[input_dim * 3 .. input_dim * 4],
            example.input_ids,
            example.labels,
            1.0,
        );
    }
    return rows;
}

fn hashTokenIdsIntoRow(row: []f32, ids: []const i32, scale: f32) void {
    if (row.len == 0) return;
    for (ids, 0..) |id, idx| {
        const id_bits: u32 = @bitCast(id);
        const hash_seed = (@as(u64, id_bits) *% 0x9E3779B185EBCA87) ^ (@as(u64, idx) *% 1315423911);
        const pos: usize = @intCast(hash_seed % row.len);
        row[pos] += scale;
    }
}

fn hashNonIgnoreTokenIdsIntoRow(row: []f32, ids: []const i32, scale: f32) void {
    if (row.len == 0) return;
    var idx: usize = 0;
    for (ids) |id| {
        if (id == -100) continue;
        const id_bits: u32 = @bitCast(id);
        const hash_seed = (@as(u64, id_bits) *% 0x9E3779B185EBCA87) ^ (@as(u64, idx) *% 1315423911);
        const pos: usize = @intCast(hash_seed % row.len);
        row[pos] += scale;
        idx += 1;
    }
}

fn hashSupervisedTokenTransitionsIntoRow(
    row: []f32,
    input_ids: []const i32,
    labels: []const i32,
    scale: f32,
) void {
    if (row.len == 0 or input_ids.len < 2 or labels.len != input_ids.len) return;
    var transition_idx: usize = 0;
    var i: usize = 1;
    while (i < input_ids.len) : (i += 1) {
        if (labels[i] == -100) continue;
        const prev_bits: u32 = @bitCast(input_ids[i - 1]);
        const next_bits: u32 = @bitCast(input_ids[i]);
        const hash_seed = (@as(u64, prev_bits) *% 0x9E3779B185EBCA87) ^
            (@as(u64, next_bits) *% 0xC2B2AE3D27D4EB4F) ^
            (@as(u64, transition_idx) *% 0x165667B19E3779F9);
        const pos: usize = @intCast(hash_seed % row.len);
        row[pos] += scale;
        transition_idx += 1;
    }
}

const SelectedGemmaMessages = struct {
    messages: []gemma_chat_data.Message,
    turns_dropped_from_left: usize,
};

fn selectRenderableGemmaMessageWindow(
    allocator: std.mem.Allocator,
    tok: anytype,
    example: gemma_chat_data.Example,
    max_seq_len: usize,
) !SelectedGemmaMessages {
    if (example.messages.len == 0) {
        return .{ .messages = try allocator.alloc(gemma_chat_data.Message, 0), .turns_dropped_from_left = 0 };
    }

    const last_assistant_idx = findLastAssistantMessageIndex(example.messages) orelse example.messages.len - 1;
    var start_idx: usize = 0;
    while (start_idx <= last_assistant_idx) : (start_idx += 1) {
        const window = example.messages[start_idx..];
        if (!containsAssistantMessage(window)) continue;
        if (try renderedGemmaMessagesFitWithinBudget(allocator, tok, window, max_seq_len)) {
            return .{
                .messages = try allocator.dupe(gemma_chat_data.Message, window),
                .turns_dropped_from_left = start_idx,
            };
        }
    }

    const fallback_start = if (last_assistant_idx < example.messages.len) last_assistant_idx else example.messages.len - 1;
    return .{
        .messages = try allocator.dupe(gemma_chat_data.Message, example.messages[fallback_start..]),
        .turns_dropped_from_left = fallback_start,
    };
}

fn renderedGemmaMessagesFitWithinBudget(
    allocator: std.mem.Allocator,
    tok: anytype,
    messages: []const gemma_chat_data.Message,
    max_seq_len: usize,
) !bool {
    const render_messages = try allocator.alloc(chat_template.Message, messages.len);
    defer allocator.free(render_messages);

    var tool_call_json_bufs = try allocator.alloc(?[]u8, messages.len);
    defer allocator.free(tool_call_json_bufs);
    @memset(tool_call_json_bufs, null);
    defer {
        for (tool_call_json_bufs) |maybe| {
            if (maybe) |buf| allocator.free(buf);
        }
    }

    for (messages, 0..) |msg, idx| {
        if (msg.tool_calls.len > 0) {
            tool_call_json_bufs[idx] = try stringifyToolCalls(allocator, msg.tool_calls);
        }
        render_messages[idx] = .{
            .role = switch (msg.role) {
                .system => .system,
                .user => .user,
                .assistant => .assistant,
                .tool => .tool,
            },
            .content = msg.content,
            .name = msg.name,
            .tool_call_id = msg.tool_call_id,
            .tool_calls_json = tool_call_json_bufs[idx],
        };
    }

    var rendered = try chat_template.render(allocator, .gemma, render_messages, .{});
    defer rendered.deinit();
    const encoded = try tok.encode(allocator, rendered.text);
    defer allocator.free(encoded);
    return encoded.len <= max_seq_len;
}

fn containsAssistantMessage(messages: []const gemma_chat_data.Message) bool {
    for (messages) |msg| {
        if (msg.role == .assistant) return true;
    }
    return false;
}

fn findLastAssistantMessageIndex(messages: []const gemma_chat_data.Message) ?usize {
    var idx = messages.len;
    while (idx > 0) {
        idx -= 1;
        if (messages[idx].role == .assistant) return idx;
    }
    return null;
}

fn buildProbeVector(allocator: std.mem.Allocator, layer_name: []const u8, output_dim: usize) ![]f32 {
    const probe = try allocator.alloc(f32, output_dim);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(layer_name);
    const base = hasher.final();
    for (probe, 0..) |*value, idx| {
        const angle: f32 = @floatFromInt((base % 997) + idx + 1);
        value.* = @sin(angle * 0.017) * 0.5 + @cos(angle * 0.009) * 0.5;
    }
    return probe;
}

fn exampleTarget(_: *const PreparedExampleInput) f64 {
    return 1.0;
}

fn layerMatchesScope(layer_base_tensor_name: []const u8, layer_name: ?[]const u8) bool {
    const selector = layer_name orelse return true;
    if (parseLayerSelectorIndex(selector)) |want_idx| {
        return parseGemma4LayerIndex(layer_base_tensor_name) == want_idx;
    }
    return std.mem.indexOf(u8, layer_base_tensor_name, selector) != null;
}

fn parseGemma4LayerIndex(tensor_name: []const u8) ?usize {
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

test "hash supervised token transitions ignores masked labels" {
    var row: [32]f32 = [_]f32{0} ** 32;
    const input_ids = [_]i32{ 10, 11, 12, 13, 14 };
    const labels = [_]i32{ -100, -100, 12, -100, 14 };
    hashSupervisedTokenTransitionsIntoRow(&row, &input_ids, &labels, 1.0);

    var non_zero: usize = 0;
    var total: f32 = 0;
    for (row) |value| {
        if (value != 0) non_zero += 1;
        total += value;
    }
    try std.testing.expect(non_zero > 0);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), total, 1e-6);
}

test "selectRenderableGemmaMessageWindow drops oldest turns first" {
    const allocator = std.testing.allocator;
    var messages = [_]gemma_chat_data.Message{
        .{ .role = .system, .content = "system" },
        .{ .role = .user, .content = "first user turn with enough words to overflow budget" },
        .{ .role = .assistant, .content = "first assistant answer with enough words to overflow budget" },
        .{ .role = .user, .content = "keep me" },
        .{ .role = .assistant, .content = "keep me too" },
    };
    const example = gemma_chat_data.Example{ .messages = messages[0..] };
    const tok = TestWhitespaceTokenizer{};

    const selected = try selectRenderableGemmaMessageWindow(allocator, tok, example, 12);
    defer allocator.free(selected.messages);

    try std.testing.expectEqual(@as(usize, 3), selected.turns_dropped_from_left);
    try std.testing.expectEqual(@as(usize, 2), selected.messages.len);
    try std.testing.expectEqual(gemma_chat_data.Role.user, selected.messages[0].role);
    try std.testing.expectEqual(gemma_chat_data.Role.assistant, selected.messages[1].role);
}

test "expandMessageMediaMarkers expands image and audio runs" {
    const allocator = std.testing.allocator;
    var image_idx: usize = 0;
    var audio_idx: usize = 0;
    const expanded = try expandMessageMediaMarkers(allocator, "look <|image|> then <|audio|>", &.{2}, &image_idx, &.{3}, &audio_idx);
    defer allocator.free(expanded);
    try std.testing.expectEqualStrings("look <|image><|image|><|image|><image|> then <|audio><|audio|><|audio|><|audio|><audio|>", expanded);
    try std.testing.expectEqual(@as(usize, 1), image_idx);
    try std.testing.expectEqual(@as(usize, 1), audio_idx);
}

test "validateMultimodalExampleShape catches placeholder mismatch" {
    const allocator = std.testing.allocator;
    const messages = try allocator.alloc(gemma_chat_data.Message, 1);
    defer allocator.free(messages);
    messages[0] = .{ .role = .user, .content = "missing markers" };
    const ex = gemma_chat_data.Example{
        .messages = messages,
        .image_paths = &.{"img.png"},
    };
    try std.testing.expectError(error.ImagePlaceholderCountMismatch, validateMultimodalExampleShape(ex));
}

test "normalizePreparedSchemaVersion accepts supported versions" {
    try std.testing.expectEqualStrings(prepared_schema_v2, try normalizePreparedSchemaVersion(prepared_schema_v2));
    try std.testing.expectEqualStrings(prepared_schema_v3, try normalizePreparedSchemaVersion(prepared_schema_v3));
}

test "normalizePreparedSchemaVersion rejects unknown version" {
    try std.testing.expectError(error.UnsupportedPreparedInputsSchema, normalizePreparedSchemaVersion("gemma4_prepared/v999"));
}

const TestWhitespaceTokenizer = struct {
    const EncodeResult = struct {
        ids: []i32,

        fn deinit(self: *EncodeResult) void {
            std.testing.allocator.free(self.ids);
            self.* = undefined;
        }
    };
    fn encode(_: TestWhitespaceTokenizer, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        var tokens = std.ArrayList(i32).empty;
        errdefer tokens.deinit(allocator);
        var it = std.mem.tokenizeAny(u8, text, " \n\t\r");
        var idx: i32 = 0;
        while (it.next() != null) : (idx += 1) {
            try tokens.append(allocator, idx + 1);
        }
        return tokens.toOwnedSlice(allocator);
    }
};

// ---------------------------------------------------------------------------
// Safetensors I/O helpers
// ---------------------------------------------------------------------------

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

fn inferLoRATargetTensors(
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

fn parseLoRAInitKind(value: ?[]const u8) !LoRAInitKind {
    const text = value orelse return .default;
    if (std.mem.eql(u8, text, "default")) return .default;
    if (std.mem.eql(u8, text, "pissa")) return .pissa;
    if (std.mem.eql(u8, text, "eva")) return .eva;
    if (std.mem.eql(u8, text, "lora-ga") or std.mem.eql(u8, text, "loraga") or std.mem.eql(u8, text, "lora_ga")) return .lora_ga;
    if (std.mem.eql(u8, text, "loftq-nf4")) return .loftq_nf4;
    if (std.mem.eql(u8, text, "loftq")) return .loftq_nf4;
    return error.UnsupportedLoRAInitializer;
}

fn buildInitialLoRAFactors(
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

fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
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

fn findDecoderGgufPathInDir(allocator: std.mem.Allocator, dir_path: []const u8) !?[]u8 {
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

fn isRegularFilePath(path: []const u8) bool {
    const stat = compat.cwd().statFile(compat.io(), path, .{}) catch return false;
    return stat.kind == .file;
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

test "findDecoderGgufPathInDir ignores projector ggufs and returns sole decoder" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "mmproj-gemma.gguf", .data = "projector" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "Gemma Q4 KM.gguf", .data = "decoder" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    const decoder = try findDecoderGgufPathInDir(allocator, model_dir) orelse return error.TestExpectedDecoderGguf;
    defer allocator.free(decoder);

    try std.testing.expect(std.mem.endsWith(u8, decoder, "Gemma Q4 KM.gguf"));
}

test "findDecoderGgufPathInDir rejects ambiguous decoder ggufs" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-E2B-it-Q4_K_M.gguf", .data = "decoder-a" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-E2B-it-Q5_K_M.gguf", .data = "decoder-b" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    try std.testing.expectError(error.AmbiguousDecoderGguf, findDecoderGgufPathInDir(allocator, model_dir));
}

test "gemma4 lora initializer frontend uses real EVA LoRA-GA and LoftQ paths" {
    try std.testing.expectEqual(LoRAInitKind.eva, try parseLoRAInitKind("eva"));
    try std.testing.expectEqual(LoRAInitKind.lora_ga, try parseLoRAInitKind("lora-ga"));
    try std.testing.expectEqual(LoRAInitKind.lora_ga, try parseLoRAInitKind("loraga"));
    try std.testing.expectEqual(LoRAInitKind.lora_ga, try parseLoRAInitKind("lora_ga"));
    try std.testing.expectEqual(LoRAInitKind.loftq_nf4, try parseLoRAInitKind("loftq"));

    const allocator = std.testing.allocator;
    const base = [_]f32{
        1.0, 0.0, 0.0,
        0.0, 2.0, 0.0,
    };
    const eva_cov = [_]f32{
        4.0, 0.0, 0.0,
        0.0, 1.0, 0.0,
        0.0, 0.0, 0.5,
    };
    const lora_ga_grad = [_]f32{
        1.0, 0.0, 0.0,
        0.0, 0.0, 0.0,
    };
    const eva = try buildInitialLoRAFactors(allocator, .eva, null, &eva_cov, null, 2, 3, 1);
    defer {
        allocator.free(eva.a);
        allocator.free(eva.b);
    }
    const lora_ga = try buildInitialLoRAFactors(allocator, .lora_ga, null, null, &lora_ga_grad, 2, 3, 1);
    defer {
        allocator.free(lora_ga.a);
        allocator.free(lora_ga.b);
    }
    const loftq = try buildInitialLoRAFactors(allocator, .loftq_nf4, &base, null, null, 2, 3, 1);
    defer {
        allocator.free(loftq.a);
        allocator.free(loftq.b);
    }
    try std.testing.expectEqual(@as(usize, 3), eva.a.len);
    try std.testing.expectEqual(@as(usize, 2), eva.b.len);
    try std.testing.expectEqual(@as(usize, 3), lora_ga.a.len);
    try std.testing.expectEqual(@as(usize, 2), lora_ga.b.len);
    try std.testing.expectEqual(@as(usize, 3), loftq.a.len);
    try std.testing.expectEqual(@as(usize, 2), loftq.b.len);
    try std.testing.expectError(error.MissingInitializerStats, buildInitialLoRAFactors(allocator, .eva, null, null, null, 2, 3, 1));
}

test "gemma4 bootstrap EVA and LoRA-GA require and consume stats files" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gemma4_real_initializer_stats_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const tensor_name = "model.layers.0.self_attn.q_proj.weight";
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = tensor_name, .shape = &.{ 2, 3 }, .data = &.{ 1, 0, 0, 0, 1, 0 } },
    });

    const eva_stats_path = try std.fs.path.join(allocator, &.{ root, "eva_stats.safetensors" });
    defer allocator.free(eva_stats_path);
    const eva_stats_name = try std.fmt.allocPrint(allocator, "{s}.eva_activation_covariance", .{tensor_name});
    defer allocator.free(eva_stats_name);
    try writeHeaderAndTensorsF32(allocator, eva_stats_path, &.{
        .{ .name = eva_stats_name, .shape = &.{ 3, 3 }, .data = &.{ 4, 0, 0, 0, 1, 0, 0, 0, 0.5 } },
    });

    const ga_stats_path = try std.fs.path.join(allocator, &.{ root, "lora_ga_stats.safetensors" });
    defer allocator.free(ga_stats_path);
    const ga_stats_name = try std.fmt.allocPrint(allocator, "{s}.lora_ga_gradient", .{tensor_name});
    defer allocator.free(ga_stats_name);
    try writeHeaderAndTensorsF32(allocator, ga_stats_path, &.{
        .{ .name = ga_stats_name, .shape = &.{ 2, 3 }, .data = &.{ 1, 0, 0, 0, 0, 0 } },
    });

    const targets = [_][]const u8{"q_proj"};
    const missing_out_dir = try std.fs.path.join(allocator, &.{ root, "missing" });
    defer allocator.free(missing_out_dir);
    const eva_out_dir = try std.fs.path.join(allocator, &.{ root, "eva" });
    defer allocator.free(eva_out_dir);
    const ga_out_dir = try std.fs.path.join(allocator, &.{ root, "ga" });
    defer allocator.free(ga_out_dir);
    try std.testing.expectError(error.MissingInitializerStats, bootstrapLoRABundle(allocator, root, missing_out_dir, .{
        .rank = 1,
        .target_modules = targets[0..],
        .init_lora_weights = "eva",
    }));

    var eva_summary = try bootstrapLoRABundle(allocator, root, eva_out_dir, .{
        .rank = 1,
        .target_modules = targets[0..],
        .init_lora_weights = "eva",
        .eva_stats_path = eva_stats_path,
    });
    defer freeBootstrapSummary(allocator, &eva_summary);

    var ga_summary = try bootstrapLoRABundle(allocator, root, ga_out_dir, .{
        .rank = 1,
        .target_modules = targets[0..],
        .init_lora_weights = "lora-ga",
        .lora_ga_stats_path = ga_stats_path,
    });
    defer freeBootstrapSummary(allocator, &ga_summary);

    try std.testing.expectEqualStrings("eva", eva_summary.init_lora_weights.?);
    try std.testing.expectEqualStrings("lora-ga", ga_summary.init_lora_weights.?);

    const adapter_config = try c_file.readFile(allocator, eva_summary.adapter_config_path);
    defer allocator.free(adapter_config);
    try std.testing.expect(std.mem.indexOf(u8, adapter_config, "eva_stats_path") == null);
    try std.testing.expect(std.mem.indexOf(u8, adapter_config, "lora_ga_stats_path") == null);
}

test "gemma4 moe expert preset targets only expert parameter tensors" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gemma4_moe_expert_targets_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const expert_name = "model.layers.0.block_sparse_moe.experts.3.w2.weight";
    const dense_name = "model.layers.0.mlp.down_proj.weight";
    const router_name = "model.layers.0.block_sparse_moe.router.weight";
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = expert_name, .shape = &.{ 2, 3 }, .data = &.{ 1, 2, 3, 4, 5, 6 } },
        .{ .name = dense_name, .shape = &.{ 2, 3 }, .data = &.{ 7, 8, 9, 10, 11, 12 } },
        .{ .name = router_name, .shape = &.{ 4, 3 }, .data = &.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } },
    });

    const targets = try inferLoRATargetTensors(
        checkpoint_path,
        allocator,
        peft.targetPresetPatterns(.moe_experts),
        .moe_experts,
    );
    defer freeLoRATargetTensors(allocator, targets);

    try std.testing.expectEqual(@as(usize, 1), targets.len);
    try std.testing.expectEqualStrings(expert_name, targets[0].tensor_name);
    try std.testing.expectEqualStrings("moe_expert", targets[0].module_name);
    try std.testing.expectEqual(@as(usize, 2), targets[0].output_dim);
    try std.testing.expectEqual(@as(usize, 3), targets[0].input_dim);
}
