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
const assets = @import("assets/gemma4.zig");

const build_options = @import("build_options");
const compat = @import("../io/compat.zig");
const lora = @import("lora.zig");
const peft = @import("peft.zig");
const graph_bridge = @import("graph_bridge.zig");
const manifest_mod = @import("../models/manifest.zig");
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
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const model_manager_mod = @import("../server/model_manager.zig");

// Checkpoint types and operations are owned by the offline asset layer.
pub const artifact_family_version = assets.artifact_family_version;
pub const checkpoint_file_name = assets.checkpoint_file_name;
pub const adapter_checkpoint_file_name = assets.adapter_checkpoint_file_name;
pub const hf_config_file_name = assets.hf_config_file_name;
pub const adapter_config_file_name = assets.adapter_config_file_name;
pub const tokenizer_config_file_name = assets.tokenizer_config_file_name;
pub const tokenizer_file_name = assets.tokenizer_file_name;
pub const special_tokens_map_file_name = assets.special_tokens_map_file_name;
pub const default_lora_target_modules = assets.default_lora_target_modules;
pub const Variant = assets.Variant;
pub const Config = assets.Config;
pub const TextConfig = assets.TextConfig;
pub const AdapterConfig = assets.AdapterConfig;
pub const TokenizerConfig = assets.TokenizerConfig;
pub const InspectionSummary = assets.InspectionSummary;
pub const ArtifactPaths = assets.ArtifactPaths;
pub const LoRATargetTensor = assets.LoRATargetTensor;
pub const BootstrapOptions = assets.BootstrapOptions;
pub const BootstrapSummary = assets.BootstrapSummary;
pub const RecursiveCompressedBaseOptions = assets.RecursiveCompressedBaseOptions;
pub const RecursiveCompressedBaseSummary = assets.RecursiveCompressedBaseSummary;
pub const LoRATensorSummary = assets.LoRATensorSummary;
pub const LoRABundleInspectionSummary = assets.LoRABundleInspectionSummary;
pub const MaterializeSummary = assets.MaterializeSummary;
pub const LoadedLoRALayer = assets.LoadedLoRALayer;
pub const LoadedLoRABundle = assets.LoadedLoRABundle;
const LoRAInitKind = assets.LoRAInitKind;
pub const resolveArtifactPaths = assets.resolveArtifactPaths;
pub const inspectCheckpoint = assets.inspectCheckpoint;
pub const freeInspectionSummary = assets.freeInspectionSummary;
pub const bootstrapLoRABundle = assets.bootstrapLoRABundle;
pub const freeBootstrapSummary = assets.freeBootstrapSummary;
pub const materializeRecursiveCompressedBase = assets.materializeRecursiveCompressedBase;
pub const freeRecursiveCompressedBaseSummary = assets.freeRecursiveCompressedBaseSummary;
pub const inspectLoRABundle = assets.inspectLoRABundle;
pub const freeLoRABundleInspectionSummary = assets.freeLoRABundleInspectionSummary;
pub const loadLoRABundle = assets.loadLoRABundle;
pub const loadLoRABundleScoped = assets.loadLoRABundleScoped;
pub const saveLoRABundle = assets.saveLoRABundle;
pub const materializeMergedModel = assets.materializeMergedModel;
pub const freeMaterializeSummary = assets.freeMaterializeSummary;
const layerMatchesScope = assets.layerMatchesScope;
const parseGemma4LayerIndex = assets.parseGemma4LayerIndex;
const inferLoRATargetTensors = assets.inferLoRATargetTensors;
const parseLoRAInitKind = assets.parseLoRAInitKind;
const buildInitialLoRAFactors = assets.buildInitialLoRAFactors;
const writeHeaderAndTensorsF32 = assets.writeHeaderAndTensorsF32;
const findDecoderGgufPathInDir = assets.findDecoderGgufPathInDir;
const freeLoRATargetTensors = assets.freeLoRATargetTensors;
const dupeOptionalString = assets.dupeOptionalString;

pub const prepared_schema_v2 = "gemma4_prepared/v2";
pub const prepared_schema_v3 = "gemma4_prepared/v3";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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
    /// Number of DDP replicas. PJRT training falls back to the CPU path when
    /// more than one replica is active because that path has no collectives.
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

const EvalOptions = struct {
    max_examples: usize,
    layer_name: ?[]const u8 = null,
};

const PreparedInputsSummaryFile = struct {
    summary: PreparedInputsSummary,
};

// ---------------------------------------------------------------------------
// Checkpoint inspection
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Load / save LoRA bundle
// ---------------------------------------------------------------------------

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
    defer native_engine.deinit();
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
            const eff_world_size: u32 = if (comptime false) options.world_size else 1;
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

fn isRegularFilePath(path: []const u8) bool {
    const stat = compat.cwd().statFile(compat.io(), path, .{}) catch return false;
    return stat.kind == .file;
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
