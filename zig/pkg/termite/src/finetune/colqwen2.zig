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
const compat = @import("../io/compat.zig");
const lora = @import("lora.zig");
const neftune = @import("neftune.zig");
const graph_bridge = @import("graph_bridge.zig");
const safetensors = @import("../models/safetensors.zig");
const tensor_access = @import("../models/tensor_access.zig");
const weight_source = @import("../models/weight_source.zig");
const image_pipeline = @import("../pipelines/image.zig");
const multimodal_qwen_adapter = @import("../pipelines/multimodal_qwen_adapter.zig");
const hf_tokenizer = @import("termite_hf_tokenizer");
const blas_mod = @import("../ops/blas_compute.zig");
const ml = @import("ml");
const optimizers = ml.graph.optimizers;

pub const artifact_family_version = "multimodal_colqwen2/v2alpha1";
pub const checkpoint_file_name = "model.safetensors";
pub const adapter_checkpoint_file_name = "adapter_model.safetensors";
pub const hf_config_file_name = "config.json";
pub const adapter_config_file_name = "adapter_config.json";
pub const preprocessor_config_file_name = "preprocessor_config.json";
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
    "embedding_proj_layer",
};

pub const focused_lora_scope_name = "@colqwen2_focus_top3";

pub const Variant = enum {
    merged,
    adapter_only,
    incomplete,
};

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
    /// If null, defaults to CPU (pure-Zig) math. Pass an MLX backend for Metal GPU acceleration.
    compute_backend: ?*const @import("../ops/ops.zig").ComputeBackend = null,
    /// MLX distributed group for DDP gradient averaging.
    /// Obtain via mlx_mod.initDistributed() at process startup.
    /// null = single-device training (default).
    mlx_dist_group: if (build_options.enable_mlx) ?@import("../backends/mlx.zig").DistributedGroup else void =
        if (build_options.enable_mlx) null else {},
    /// Number of DDP replicas (world size). Must equal 1 when mlx_dist_group is null.
    world_size: u32 = 1,
    /// DDP rank of this process. Rank 0 is responsible for checkpoint writes.
    /// Set to 0 for single-device training (default).
    ddp_rank: u32 = 0,
    /// Linear LR warmup steps. LR ramps from 0 → learning_rate over the first warmup_steps
    /// optimizer updates. 0 = no warmup.
    warmup_steps: u32 = 0,
    /// Pre-compiled PJRT gradient executors, one per LoRA layer (null = use CPU/MLX path).
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

pub const ArtifactPaths = struct {
    allocator: std.mem.Allocator,
    model_dir: []u8,
    checkpoint_path: ?[]u8 = null,
    adapter_checkpoint_path: ?[]u8 = null,
    config_path: ?[]u8 = null,
    adapter_config_path: ?[]u8 = null,
    preprocessor_config_path: ?[]u8 = null,
    tokenizer_config_path: ?[]u8 = null,
    tokenizer_path: ?[]u8 = null,
    special_tokens_map_path: ?[]u8 = null,

    pub fn deinit(self: *ArtifactPaths) void {
        self.allocator.free(self.model_dir);
        if (self.checkpoint_path) |path| self.allocator.free(path);
        if (self.adapter_checkpoint_path) |path| self.allocator.free(path);
        if (self.config_path) |path| self.allocator.free(path);
        if (self.adapter_config_path) |path| self.allocator.free(path);
        if (self.preprocessor_config_path) |path| self.allocator.free(path);
        if (self.tokenizer_config_path) |path| self.allocator.free(path);
        if (self.tokenizer_path) |path| self.allocator.free(path);
        if (self.special_tokens_map_path) |path| self.allocator.free(path);
        self.* = undefined;
    }
};

pub const Config = struct {
    model_type: ?[]const u8 = null,
    hidden_size: ?usize = null,
    num_hidden_layers: ?usize = null,
    num_attention_heads: ?usize = null,
    vocab_size: ?usize = null,
    torch_dtype: ?[]const u8 = null,
    image_token_id: ?i64 = null,
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

pub const PreprocessorConfig = struct {
    processor_class: ?[]const u8 = null,
    do_resize: ?bool = null,
    do_rescale: ?bool = null,
    do_normalize: ?bool = null,
    rescale_factor: ?f32 = null,
    patch_size: ?usize = null,
    temporal_patch_size: ?usize = null,
    merge_size: ?usize = null,
    max_pixels: ?usize = null,
    min_pixels: ?usize = null,
    query_prefix: ?[]const u8 = null,
    visual_prompt_prefix: ?[]const u8 = null,
};

pub const TokenizerConfig = struct {
    model_max_length: ?usize = null,
    padding_side: ?[]const u8 = null,
    tokenizer_class: ?[]const u8 = null,
};

pub const SpecialTokensMap = struct {
    bos_token: ?[]const u8 = null,
    eos_token: ?[]const u8 = null,
    image_token: ?[]const u8 = null,
    pad_token: ?[]const u8 = null,
};

pub const InspectionSummary = struct {
    artifact_family_version: []const u8,
    variant: Variant,
    model_dir: []const u8,
    checkpoint_path: ?[]const u8 = null,
    adapter_checkpoint_path: ?[]const u8 = null,
    config_path: ?[]const u8 = null,
    adapter_config_path: ?[]const u8 = null,
    preprocessor_config_path: ?[]const u8 = null,
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
    image_token_id: ?i64 = null,
    processor_class: ?[]const u8 = null,
    do_resize: ?bool = null,
    do_rescale: ?bool = null,
    do_normalize: ?bool = null,
    rescale_factor: ?f32 = null,
    patch_size: ?usize = null,
    temporal_patch_size: ?usize = null,
    merge_size: ?usize = null,
    min_pixels: ?usize = null,
    max_pixels: ?usize = null,
    query_prefix: ?[]const u8 = null,
    visual_prompt_prefix: ?[]const u8 = null,
    tokenizer_class: ?[]const u8 = null,
    tokenizer_model_max_length: ?usize = null,
    padding_side: ?[]const u8 = null,
    bos_token: ?[]const u8 = null,
    eos_token: ?[]const u8 = null,
    image_token: ?[]const u8 = null,
    pad_token: ?[]const u8 = null,
    lora_rank: ?usize = null,
    lora_alpha: ?f64 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    use_dora: ?bool = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    has_merged_weights: bool = false,
    has_adapter_weights: bool = false,
    has_tokenizer: bool = false,
    has_preprocessor: bool = false,
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
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    resolved_tensor_count: usize = 0,
    dora_magnitude_tensor_count: usize = 0,
    dora_magnitude_parameter_count: usize = 0,
    trainable_parameter_count: usize = 0,
    use_dora: ?bool = null,
    tensors: []LoRATensorSummary,
};

const PreparedInputsSummaryFile = struct {
    summary: PreparedInputsSummary,
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
        .adapter_checkpoint_path = try optionalPathInDir(allocator, model_dir, adapter_checkpoint_file_name),
        .config_path = try optionalPathInDir(allocator, model_dir, hf_config_file_name),
        .adapter_config_path = try optionalPathInDir(allocator, model_dir, adapter_config_file_name),
        .preprocessor_config_path = try optionalPathInDir(allocator, model_dir, preprocessor_config_file_name),
        .tokenizer_config_path = try optionalPathInDir(allocator, model_dir, tokenizer_config_file_name),
        .tokenizer_path = try optionalPathInDir(allocator, model_dir, tokenizer_file_name),
        .special_tokens_map_path = try optionalPathInDir(allocator, model_dir, special_tokens_map_file_name),
    };

    if (stat.kind == .file) {
        if (std.mem.eql(u8, std.fs.path.basename(input), checkpoint_file_name)) {
            if (paths.checkpoint_path) |path| allocator.free(path);
            paths.checkpoint_path = try allocator.dupe(u8, input);
        } else if (std.mem.eql(u8, std.fs.path.basename(input), adapter_checkpoint_file_name)) {
            if (paths.adapter_checkpoint_path) |path| allocator.free(path);
            paths.adapter_checkpoint_path = try allocator.dupe(u8, input);
        }
    }

    return paths;
}

pub fn resolveLoRACheckpointPath(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    if (isRegularFilePath(input)) return try allocator.dupe(u8, input);
    const path = try std.fs.path.join(allocator, &.{ input, adapter_checkpoint_file_name });
    errdefer allocator.free(path);
    if (!isRegularFilePath(path)) return error.MissingAdapterCheckpoint;
    return path;
}

pub fn inspectCheckpoint(allocator: std.mem.Allocator, input: []const u8) !InspectionSummary {
    var paths = try resolveArtifactPaths(allocator, input);
    defer paths.deinit();

    const config_bytes = if (paths.config_path) |path| try c_file.readFile(allocator, path) else null;
    defer if (config_bytes) |bytes| allocator.free(bytes);
    const adapter_config_bytes = if (paths.adapter_config_path) |path| try c_file.readFile(allocator, path) else null;
    defer if (adapter_config_bytes) |bytes| allocator.free(bytes);
    const preprocessor_bytes = if (paths.preprocessor_config_path) |path| try c_file.readFile(allocator, path) else null;
    defer if (preprocessor_bytes) |bytes| allocator.free(bytes);
    const tokenizer_bytes = if (paths.tokenizer_config_path) |path| try c_file.readFile(allocator, path) else null;
    defer if (tokenizer_bytes) |bytes| allocator.free(bytes);
    const special_tokens_bytes = if (paths.special_tokens_map_path) |path| try c_file.readFile(allocator, path) else null;
    defer if (special_tokens_bytes) |bytes| allocator.free(bytes);

    var parsed_config = if (config_bytes) |bytes|
        try std.json.parseFromSlice(Config, allocator, bytes, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_config) |*parsed| parsed.deinit();
    var parsed_adapter_config = if (adapter_config_bytes) |bytes|
        try std.json.parseFromSlice(AdapterConfig, allocator, bytes, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_adapter_config) |*parsed| parsed.deinit();
    var parsed_preprocessor = if (preprocessor_bytes) |bytes|
        try std.json.parseFromSlice(PreprocessorConfig, allocator, bytes, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_preprocessor) |*parsed| parsed.deinit();
    var parsed_tokenizer = if (tokenizer_bytes) |bytes|
        try std.json.parseFromSlice(TokenizerConfig, allocator, bytes, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_tokenizer) |*parsed| parsed.deinit();
    var parsed_special_tokens = if (special_tokens_bytes) |bytes|
        try std.json.parseFromSlice(SpecialTokensMap, allocator, bytes, .{ .ignore_unknown_fields = true })
    else
        null;
    defer if (parsed_special_tokens) |*parsed| parsed.deinit();

    const config = if (parsed_config) |*parsed| parsed.value else null;
    const adapter_config = if (parsed_adapter_config) |*parsed| parsed.value else null;
    const preprocessor = if (parsed_preprocessor) |*parsed| parsed.value else null;
    const tokenizer = if (parsed_tokenizer) |*parsed| parsed.value else null;
    const special_tokens = if (parsed_special_tokens) |*parsed| parsed.value else null;

    const variant: Variant = if (paths.checkpoint_path != null)
        .merged
    else if (paths.adapter_checkpoint_path != null)
        .adapter_only
    else
        .incomplete;

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .variant = variant,
        .model_dir = try allocator.dupe(u8, paths.model_dir),
        .checkpoint_path = try dupeOptionalString(allocator, paths.checkpoint_path),
        .adapter_checkpoint_path = try dupeOptionalString(allocator, paths.adapter_checkpoint_path),
        .config_path = try dupeOptionalString(allocator, paths.config_path),
        .adapter_config_path = try dupeOptionalString(allocator, paths.adapter_config_path),
        .preprocessor_config_path = try dupeOptionalString(allocator, paths.preprocessor_config_path),
        .tokenizer_config_path = try dupeOptionalString(allocator, paths.tokenizer_config_path),
        .tokenizer_path = try dupeOptionalString(allocator, paths.tokenizer_path),
        .special_tokens_map_path = try dupeOptionalString(allocator, paths.special_tokens_map_path),
        .base_model_name_or_path = if (adapter_config) |value| try dupeOptionalString(allocator, value.base_model_name_or_path) else null,
        .model_type = if (config) |value| try dupeOptionalString(allocator, value.model_type) else null,
        .hidden_size = if (config) |value| value.hidden_size else null,
        .num_hidden_layers = if (config) |value| value.num_hidden_layers else null,
        .num_attention_heads = if (config) |value| value.num_attention_heads else null,
        .vocab_size = if (config) |value| value.vocab_size else null,
        .torch_dtype = if (config) |value| try dupeOptionalString(allocator, value.torch_dtype) else null,
        .image_token_id = if (config) |value| value.image_token_id else null,
        .processor_class = if (preprocessor) |value| try dupeOptionalString(allocator, value.processor_class) else null,
        .do_resize = if (preprocessor) |value| value.do_resize else null,
        .do_rescale = if (preprocessor) |value| value.do_rescale else null,
        .do_normalize = if (preprocessor) |value| value.do_normalize else null,
        .rescale_factor = if (preprocessor) |value| value.rescale_factor else null,
        .patch_size = if (preprocessor) |value| value.patch_size else null,
        .temporal_patch_size = if (preprocessor) |value| value.temporal_patch_size else null,
        .merge_size = if (preprocessor) |value| value.merge_size else null,
        .min_pixels = if (preprocessor) |value| value.min_pixels else null,
        .max_pixels = if (preprocessor) |value| value.max_pixels else null,
        .query_prefix = if (preprocessor) |value| try dupeOptionalString(allocator, value.query_prefix) else null,
        .visual_prompt_prefix = if (preprocessor) |value| try dupeOptionalString(allocator, value.visual_prompt_prefix) else null,
        .tokenizer_class = if (tokenizer) |value| try dupeOptionalString(allocator, value.tokenizer_class) else null,
        .tokenizer_model_max_length = if (tokenizer) |value| value.model_max_length else null,
        .padding_side = if (tokenizer) |value| try dupeOptionalString(allocator, value.padding_side) else null,
        .bos_token = if (special_tokens) |value| try dupeOptionalString(allocator, value.bos_token) else null,
        .eos_token = if (special_tokens) |value| try dupeOptionalString(allocator, value.eos_token) else null,
        .image_token = if (special_tokens) |value| try dupeOptionalString(allocator, value.image_token) else null,
        .pad_token = if (special_tokens) |value| try dupeOptionalString(allocator, value.pad_token) else null,
        .lora_rank = if (adapter_config) |value| value.r else null,
        .lora_alpha = if (adapter_config) |value| value.lora_alpha else null,
        .peft_type = if (adapter_config) |value| try dupeOptionalString(allocator, value.peft_type) else null,
        .task_type = if (adapter_config) |value| try dupeOptionalString(allocator, value.task_type) else null,
        .use_dora = if (adapter_config) |value| value.use_dora else null,
        .target_module_count = if (adapter_config) |value| if (value.target_modules) |items| items.len else 0 else 0,
        .target_modules = if (adapter_config) |value| try dupeOptionalStringSlice(allocator, value.target_modules) else null,
        .has_merged_weights = paths.checkpoint_path != null,
        .has_adapter_weights = paths.adapter_checkpoint_path != null,
        .has_tokenizer = paths.tokenizer_path != null,
        .has_preprocessor = paths.preprocessor_config_path != null,
    };
}

pub fn bootstrapLoRABundle(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    out_dir: []const u8,
    options: BootstrapOptions,
) !BootstrapSummary {
    var inspect = try inspectCheckpoint(allocator, model_input);
    defer freeInspectionSummary(allocator, &inspect);

    const checkpoint_path = inspect.checkpoint_path orelse return error.MissingMergedCheckpoint;
    if (options.rank == 0) return error.InvalidLoRARank;

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
    else if (inspect.base_model_name_or_path) |value|
        try allocator.dupe(u8, value)
    else
        try allocator.dupe(u8, inspect.model_dir);
    errdefer allocator.free(base_model_name_or_path);

    try writeBootstrapAdapterCheckpoint(allocator, adapter_checkpoint_path, resolved_tensors, options.rank);
    try writeAdapterConfigJson(allocator, adapter_config_path, base_model_name_or_path, options.rank, options.alpha, requested_target_modules, false);
    try copySupportingArtifactIfPresent(allocator, inspect.preprocessor_config_path, out_dir, preprocessor_config_file_name);
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
        .target_modules = try dupeStringSlice(allocator, requested_target_modules),
        .resolved_tensors = resolved_tensors,
    };
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

    const base_checkpoint_path = base_inspect.checkpoint_path orelse return error.MissingMergedCheckpoint;
    const adapter_checkpoint_path = adapter_inspect.adapter_checkpoint_path orelse return error.MissingAdapterCheckpoint;

    var base_reader = try safetensors.MMapReader.openFileAbsolute(allocator, base_checkpoint_path);
    defer base_reader.deinit();
    var adapter_reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer adapter_reader.deinit();

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
        const adapter_b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(adapter_b_name);
        const base_tensor_name = parsed.base_tensor_base_name;

        const adapter_b_info = adapter_reader.header.tensors.get(adapter_b_name) orelse return error.MissingAdapterPair;
        const base_info = base_reader.header.tensors.get(base_tensor_name) orelse return error.MissingBaseTensorForAdapter;
        if (adapter_a_info.shape.len != 2 or adapter_b_info.shape.len != 2 or base_info.shape.len != 2) return error.InvalidAdapterTensorShape;
        if (adapter_a_info.shape[1] != base_info.shape[1]) return error.AdapterInputDimMismatch;
        if (adapter_b_info.shape[0] != base_info.shape[0]) return error.AdapterOutputDimMismatch;
        if (adapter_a_info.shape[0] != adapter_b_info.shape[1]) return error.AdapterRankMismatch;

        const dora_name = try doraMagnitudeTensorName(allocator, base_tensor_name);
        defer allocator.free(dora_name);
        const dora_info = adapter_reader.header.tensors.get(dora_name);
        if (dora_info) |info| {
            if (info.shape.len != 1 or info.shape[0] != base_info.shape[0]) return error.InvalidAdapterTensorShape;
        }
        const dora_parameter_count: usize = if (dora_info != null) @intCast(base_info.shape[0]) else 0;

        try tensors.append(allocator, .{
            .base_tensor_name = try allocator.dupe(u8, base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, adapter_a_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, adapter_b_name),
            .dora_magnitude_tensor_name = if (dora_info != null) try allocator.dupe(u8, dora_name) else null,
            .module_name = try allocator.dupe(u8, parsed.module_name),
            .input_dim = @intCast(base_info.shape[1]),
            .output_dim = @intCast(base_info.shape[0]),
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
        .resolved_tensor_count = tensors.items.len,
        .dora_magnitude_tensor_count = dora_magnitude_tensor_count,
        .dora_magnitude_parameter_count = dora_magnitude_parameter_count,
        .trainable_parameter_count = trainable_parameter_count,
        .use_dora = adapter_inspect.use_dora,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
}

pub fn loadLoRABundle(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
) !LoadedLoRABundle {
    var inspected = try inspectLoRABundle(allocator, base_model_input, adapter_model_input);
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

    var source_paths = try resolveArtifactPaths(allocator, bundle.adapter_model_dir);
    defer source_paths.deinit();
    try copySupportingArtifactIfPresent(allocator, source_paths.preprocessor_config_path, out_dir, preprocessor_config_file_name);
    try copySupportingArtifactIfPresent(allocator, source_paths.tokenizer_config_path, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, source_paths.tokenizer_path, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, source_paths.special_tokens_map_path, out_dir, special_tokens_map_file_name);
}

pub fn materializeMergedModel(
    allocator: std.mem.Allocator,
    base_model_input: []const u8,
    adapter_model_input: []const u8,
    out_dir: []const u8,
) !MaterializeSummary {
    var bundle = try loadLoRABundle(allocator, base_model_input, adapter_model_input);
    defer bundle.deinit();

    var base_paths = try resolveArtifactPaths(allocator, base_model_input);
    defer base_paths.deinit();
    const base_checkpoint_path = base_paths.checkpoint_path orelse return error.MissingMergedCheckpoint;
    var adapter_paths = try resolveArtifactPaths(allocator, adapter_model_input);
    defer adapter_paths.deinit();

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

        const shape = [_]i64{
            @as(i64, @intCast(out_rows)),
            @as(i64, @intCast(out_cols)),
        };
        const tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, &shape, hf_weight);
        allocator.free(hf_weight);
        try merged.put(allocator, try allocator.dupe(u8, layer.base_tensor_name), tensor);
    }

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const output_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(output_checkpoint_path);
    const bytes = try buildMergedSafetensorsFile(allocator, base_access, base_names, &merged);
    defer allocator.free(bytes);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = output_checkpoint_path, .data = bytes });

    try copySupportingArtifactIfPresent(allocator, base_paths.config_path, out_dir, hf_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.preprocessor_config_path, out_dir, preprocessor_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.tokenizer_config_path, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.tokenizer_path, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, base_paths.special_tokens_map_path, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_paths.preprocessor_config_path, out_dir, preprocessor_config_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_paths.tokenizer_config_path, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_paths.tokenizer_path, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, adapter_paths.special_tokens_map_path, out_dir, special_tokens_map_file_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, bundle.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, bundle.adapter_model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .output_checkpoint_path = output_checkpoint_path,
        .merged_lora_tensor_count = bundle.layers.len,
        .merged_dora_tensor_count = merged_dora_tensor_count,
        .copied_base_tensor_count = base_names.len - bundle.layers.len,
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

    const inputs = try allocator.alloc(f32, options.input_rows * layer.input_dim);
    defer allocator.free(inputs);
    const targets = try allocator.alloc(f32, options.input_rows * layer.output_dim);
    defer allocator.free(targets);
    fillDeterministicMatrix(inputs, options.input_rows, layer.input_dim, 0.019, 0.011);
    fillDeterministicMatrix(targets, options.input_rows, layer.output_dim, 0.023, -0.017);

    const adapter_a_before = l2Norm(layer.adapter_a);
    const adapter_b_before = l2Norm(layer.adapter_b);
    var weight_store = blas_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    var compute = blas_mod.BlasCompute.init(allocator, &weight_store, null);
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

            // Try PJRT path first; fall back to CPU/MLX on error or when disabled.
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
                // CPU/MLX fallback.
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

            // Normalize accumulated gradients by accum steps and world size before clipping.
            const eff_world_size_norm: u32 = if (comptime build_options.enable_mlx) options.world_size else 1;
            const norm_factor = 1.0 / (@as(f32, @floatFromInt(accum_count)) * @as(f32, @floatFromInt(eff_world_size_norm)));
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

pub fn freeInspectionSummary(allocator: std.mem.Allocator, summary: *InspectionSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    if (summary.checkpoint_path) |value| allocator.free(value);
    if (summary.adapter_checkpoint_path) |value| allocator.free(value);
    if (summary.config_path) |value| allocator.free(value);
    if (summary.adapter_config_path) |value| allocator.free(value);
    if (summary.preprocessor_config_path) |value| allocator.free(value);
    if (summary.tokenizer_config_path) |value| allocator.free(value);
    if (summary.tokenizer_path) |value| allocator.free(value);
    if (summary.special_tokens_map_path) |value| allocator.free(value);
    if (summary.base_model_name_or_path) |value| allocator.free(value);
    if (summary.model_type) |value| allocator.free(value);
    if (summary.torch_dtype) |value| allocator.free(value);
    if (summary.processor_class) |value| allocator.free(value);
    if (summary.query_prefix) |value| allocator.free(value);
    if (summary.visual_prompt_prefix) |value| allocator.free(value);
    if (summary.tokenizer_class) |value| allocator.free(value);
    if (summary.padding_side) |value| allocator.free(value);
    if (summary.bos_token) |value| allocator.free(value);
    if (summary.eos_token) |value| allocator.free(value);
    if (summary.image_token) |value| allocator.free(value);
    if (summary.pad_token) |value| allocator.free(value);
    if (summary.peft_type) |value| allocator.free(value);
    if (summary.task_type) |value| allocator.free(value);
    if (summary.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
    }
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
    if (summary.adapter_config_path) |value| allocator.free(value);
    if (summary.base_model_name_or_path) |value| allocator.free(value);
    if (summary.target_modules) |modules| {
        for (modules) |item| allocator.free(item);
        allocator.free(modules);
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
    if (std.mem.eql(u8, tensor_name, "embedding_proj_layer.weight")) return "embedding_proj_layer";
    const ordered_modules = [_][]const u8{
        "q_proj",
        "k_proj",
        "v_proj",
        "o_proj",
        "gate_proj",
        "up_proj",
        "down_proj",
    };
    inline for (ordered_modules) |module_name| {
        const dot_suffix = "." ++ module_name ++ ".weight";
        const slash_suffix = "/" ++ module_name ++ "/weight";
        if (std.mem.endsWith(u8, tensor_name, dot_suffix)) return module_name;
        if (std.mem.endsWith(u8, tensor_name, slash_suffix)) return module_name;
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

fn bundleHasDoRA(bundle: *const LoadedLoRABundle) bool {
    for (bundle.layers) |layer| {
        if (layer.dora_magnitude != null) return true;
    }
    return false;
}

fn copySupportingArtifactIfPresent(
    allocator: std.mem.Allocator,
    maybe_src_path: ?[]const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src_path = maybe_src_path orelse return;
    const contents = try c_file.readFile(allocator, src_path);
    defer allocator.free(contents);
    const dst_path = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst_path, .data = contents });
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
    if (item.dora_magnitude_tensor_name) |name| allocator.free(name);
    allocator.free(item.module_name);
    item.* = undefined;
}

fn stringSliceContains(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
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

fn transpose2DF32(out: []f32, input: []const f32, rows: usize, cols: usize) void {
    std.debug.assert(out.len == rows * cols);
    std.debug.assert(input.len == rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            out[col * rows + row] = input[row * cols + col];
        }
    }
}

fn buildZeroF32(allocator: std.mem.Allocator, len: usize) ![]f32 {
    const data = try allocator.alloc(f32, len);
    @memset(data, 0.0);
    return data;
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    return tensor_name;
}

fn doraMagnitudeTensorName(allocator: std.mem.Allocator, base_tensor_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.lora_magnitude_vector.weight", .{tensorBaseName(base_tensor_name)});
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

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    if (std.mem.endsWith(u8, path, ".index.json")) {
        const access = try tensor_access.ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
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
