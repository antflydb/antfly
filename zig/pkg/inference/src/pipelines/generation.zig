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

// Text generation pipeline.
//
// Two pathways:
// 1. ortgenai (ONNX Runtime GenAI) — for models with genai_config.json
// 2. Native autoregressive decoding — for native/GPU backends using GPT arch forward pass
//
// The native path runs gpt_arch.forward() to get logits, samples the next token,
// and loops until EOS or max_tokens. Matches Go inference's TextGenerationPipeline.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ortgenai = if (build_options.enable_onnx) @import("../backends/ortgenai.zig") else struct {};
const tokenizer_mod = @import("inference_tokenizer");
const gpt_arch = @import("../architectures/gpt.zig");
const gemma4_runtime = @import("../architectures/gemma4_runtime.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const contracts = @import("../graph/backend_contracts.zig");
const ComputeBackend = ops.ComputeBackend;
const activations = @import("../backends/activations.zig");
const backends = @import("../backends/backends.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const decoder_tail_runtime = @import("../backends/decoder_tail_runtime.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const runtime = @import("../runtime/root.zig");
const jinja = @import("jinja");
const grammar_mod = @import("grammar.zig");
const gemma3_mm = @import("gemma3_multimodal.zig");
const gemma4_mm = @import("../architectures/gemma4_multimodal.zig");
const gemma4_mtp = @import("../architectures/gemma4_mtp.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");
const qwen2vl_mm = @import("qwen2vl_multimodal.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");
const hf_tokenizer = tokenizer_mod.hf;
const graph_mod = @import("../graph/root.zig");
const pjrt_executor_mod = if (build_options.enable_pjrt) @import("../graph/pjrt_executor.zig") else struct {};

fn setWorkloadProfileRegime(cb: *const ComputeBackend, regime: ops.WorkloadRegime) !void {
    cb.workloadProfileSetRegime(regime) catch |err| {
        // Profiling metadata must never turn an otherwise usable backend into
        // an inference failure when its optional profiler runtime is absent.
        if (err == error.WorkloadProfileUnavailable) return;
        return err;
    };
}

fn ordinaryMetalGemmaDraftNeedsContiguousState(
    backend: ops.BackendKind,
    config: gpt_mod.Config,
    has_paged_kv_state: bool,
) bool {
    return backend == .metal and
        config.family == .gemma and
        !config.gemma4_mtp_assistant and
        !has_paged_kv_state;
}

fn supportsSpeculativeTargetVerification(
    backend: ops.BackendKind,
    target_config: gpt_mod.Config,
) bool {
    // Qualified A4B Metal decode is qLen=1-only. Keep speculative decoding out
    // of the request before either target or draft state is mutated; the
    // reserve-time check remains as a fail-closed backstop.
    return backend != .metal or
        !gemma4_runtime.isQualifiedA4bArchitecture(target_config);
}

pub var gemma4_mtp_debug_override: bool = false;

pub const Message = struct {
    pub const ContentPart = union(enum) {
        text: []const u8,
        image: usize,
        audio: usize,
    };

    role: []const u8,
    content: []const u8,
    /// Raw image bytes for multimodal messages (decoded from data URIs).
    /// Null or empty for text-only messages.
    image_bytes: ?[]const []const u8 = null,
    /// Raw encoded audio bytes for multimodal messages.
    /// Null or empty for text/image-only messages.
    audio_bytes: ?[]const []const u8 = null,
    /// Optional structured content parts preserving text/image ordering.
    /// Image/audio parts store the index into `image_bytes`/`audio_bytes`.
    content_parts: ?[]const ContentPart = null,

    pub fn hasImages(self: Message) bool {
        if (self.image_bytes) |imgs| return imgs.len > 0;
        return false;
    }

    pub fn hasAudio(self: Message) bool {
        if (self.audio_bytes) |clips| return clips.len > 0;
        return false;
    }
};

/// Check if any message in the batch contains images.
pub fn messagesHaveImages(messages: []const Message) bool {
    for (messages) |m| {
        if (m.hasImages()) return true;
    }
    return false;
}

/// Check if any message in the batch contains audio.
pub fn messagesHaveAudio(messages: []const Message) bool {
    for (messages) |m| {
        if (m.hasAudio()) return true;
    }
    return false;
}

/// Extra positions introduced when model-declared image placeholders expand.
/// Audio expansion is measured after projection because its token count varies
/// with the clip and projector metadata.
pub fn nativeGenerationMediaTokenAllowance(messages: []const Message, config: gpt_mod.Config) usize {
    if (config.mm_tokens_per_image == 0) return 0;
    return nativeGenerationImageCount(messages) *| (@as(usize, config.mm_tokens_per_image) + 1);
}

pub const NativeGenerationMediaAdmission = struct {
    token_allowance: usize = 0,
    host_scratch_bytes: usize = 0,
    backend_scratch_bytes: usize = 0,
};

/// One source of truth for multimodal scheduler and process-wide admission.
/// Qwen3-VL probes encoded image headers and applies the exact smart-resize
/// geometry used by its projector; no image is decoded or projected here.
pub fn nativeGenerationMediaAdmission(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    messages: []const Message,
    config: gpt_mod.Config,
) !NativeGenerationMediaAdmission {
    if (config.family == .qwen3_vl) {
        const images = try collectImagesInPromptOrder(allocator, messages);
        defer allocator.free(images);
        if (images.len == 0) return .{};
        const projector = try qwen3vl_projector.estimateAdmission(images, config, .{});
        return .{
            // The text encode already contains an image placeholder. Retaining
            // one additional token per image preserves the existing
            // conservative allowance contract while the visual span itself is
            // exact.
            .token_allowance = std.math.add(usize, projector.visual_tokens, images.len) catch
                return error.PromptTooLong,
            .host_scratch_bytes = projector.host_scratch_bytes,
            .backend_scratch_bytes = projector.backend_scratch_bytes,
        };
    }
    if (config.family == .qwen3_5) {
        const image_count = nativeGenerationImageCount(messages);
        if (image_count == 0) return .{};
        const prep_config = try qwen2vl_mm.loadPreprocessorConfig(allocator, model_dir);
        const max_image_tokens = try qwen2vl_mm.maxImageTokenCount(prep_config);
        return .{ .token_allowance = image_count *| (max_image_tokens +| 1) };
    }
    return .{ .token_allowance = nativeGenerationMediaTokenAllowance(messages, config) };
}

/// Conservative media-token count used for scheduling and memory admission.
/// Qwen image spans are dynamic, so derive their bound from the same
/// preprocessor configuration that produces the final visual tokens.
pub fn nativeGenerationAdmissionMediaTokenAllowance(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    messages: []const Message,
    config: gpt_mod.Config,
) !usize {
    return (try nativeGenerationMediaAdmission(allocator, model_dir, messages, config)).token_allowance;
}

/// Media allowance that can safely constrain the text-only encode before
/// dynamic image preprocessing determines the exact expanded prompt length.
pub fn nativeGenerationPreliminaryMediaTokenAllowance(messages: []const Message, config: gpt_mod.Config) usize {
    if (config.family == .qwen3_5 or config.family == .qwen3_vl) return 0;
    return nativeGenerationMediaTokenAllowance(messages, config);
}

fn nativeGenerationImageCount(messages: []const Message) usize {
    var image_count: usize = 0;
    for (messages) |message| {
        if (message.content_parts) |parts| {
            for (parts) |part| switch (part) {
                .image => image_count +|= 1,
                else => {},
            };
        } else if (message.image_bytes) |images| {
            image_count +|= images.len;
        }
    }
    return image_count;
}

/// Bound idle prefill chunks only for geometries with qualified measurements.
/// Keep the general scheduler ceiling intact for every other model and backend;
/// explicit request limits and memory admission are applied separately.
pub fn nativeGenerationPrefillChunkCeiling(
    backend: runtime.kv.pool.BackendKind,
    config: gpt_mod.Config,
    configured_ceiling: usize,
) usize {
    const ceiling = @max(configured_ceiling, 1);
    if (backend == .metal and
        config.family == .gemma and
        config.hasPle() and
        config.num_attention_heads == 8 and
        config.effectiveKVHeads() == 2 and
        config.headDim() == 256 and
        config.global_head_dim == 512 and
        config.sliding_window == 512 and
        !config.gemma4_mtp_assistant)
    {
        return @min(ceiling, 512);
    }
    // Gemma4 E2B CUDA evidence currently selects the fixed 512-row schedule.
    // A forced 2051-row pass triggered a >2 GiB deferred-free drain, while
    // balanced 411-row and 513-row candidates were both slower. Keep the
    // fingerprint-specific winner until a new plan is promoted by paired E2E
    // evidence.
    if (backend == .cuda and
        config.family == .gemma and
        config.hidden_size == 1536 and
        config.num_hidden_layers == 35 and
        config.num_attention_heads == 8 and
        config.effectiveKVHeads() == 1 and
        config.headDim() == 256 and
        config.global_head_dim == 512 and
        config.intermediate_size == 6144 and
        config.shared_layer_intermediate_size == 12288 and
        config.num_kv_shared_layers == 20 and
        config.sliding_window == 512 and
        config.hasPle() and
        !config.gemma4_mtp_assistant)
    {
        return @min(ceiling, 512);
    }
    return ceiling;
}

pub fn userFacingErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.AudioInputTooLong => "audio input is too long for the Gemma4 direct audio projector; trim the clip or raise clip.audio.max_tokens in the projector metadata",
        else => @errorName(err),
    };
}

pub const SpeculationPolicy = enum {
    auto,
    force,
    off,

    pub fn name(self: SpeculationPolicy) []const u8 {
        return switch (self) {
            .auto => "auto",
            .force => "force",
            .off => "off",
        };
    }
};

pub fn parseSpeculationPolicy(raw: []const u8) ?SpeculationPolicy {
    if (std.mem.eql(u8, raw, "auto")) return .auto;
    if (std.mem.eql(u8, raw, "force")) return .force;
    if (std.mem.eql(u8, raw, "off")) return .off;
    return null;
}

pub const SpeculationCalibration = enum {
    none,
    probe,
    positive,

    pub fn name(self: SpeculationCalibration) []const u8 {
        return switch (self) {
            .none => "none",
            .probe => "probe",
            .positive => "positive",
        };
    }
};

pub fn parseSpeculationCalibration(raw: []const u8) ?SpeculationCalibration {
    if (std.mem.eql(u8, raw, "none") or std.mem.eql(u8, raw, "off")) return .none;
    if (std.mem.eql(u8, raw, "probe") or std.mem.eql(u8, raw, "calibrate")) return .probe;
    if (std.mem.eql(u8, raw, "positive") or std.mem.eql(u8, raw, "calibrated")) return .positive;
    return null;
}

pub const GenerationConfig = struct {
    max_tokens: i32 = 256,
    temperature: f32 = 0,
    top_p: f32 = 0,
    top_k: i32 = 0,
    min_p: f32 = 0,
    repetition_penalty: f32 = 1.0,
    frequency_penalty: f32 = 0,
    presence_penalty: f32 = 0,
    prefill_chunk_size: usize = 0,
    /// Borrowed immutable geometry produced by scheduler + memory admission.
    /// Empty preserves the scalar legacy path for embedders that do not yet
    /// participate in server admission.
    prefill_chunk_plan: ?runtime.scheduler.native_generate.PrefillChunkPlan = null,
    /// Grammar constraint mode. null = no constraint, "json" = JSON mode.
    grammar: ?[]const u8 = null,
    /// Path to a smaller draft model for speculative decoding. When set, the
    /// draft model generates `speculative_k` candidate tokens that are then
    /// verified by the target model in a single forward pass.
    draft_model: ?[]const u8 = null,
    /// Number of candidate tokens the draft model proposes per speculation
    /// round (default 4).
    speculative_k: u32 = 4,
    /// True when speculative decoding was requested even if policy prevents
    /// loading or using the draft backend.
    speculation_requested: bool = false,
    /// Production policy for speculative decoding when a draft model is present.
    speculation_policy: SpeculationPolicy = .auto,
    /// Explicit calibration state for auto speculative decoding. Unknown Gemma4
    /// MTP buckets stay target-only unless callers opt into probing or provide
    /// a previously positive calibration result.
    speculation_calibration: SpeculationCalibration = .none,
    /// KV cache quantization format override. null = auto-select based on backend.
    cache_dtype: ?[]const u8 = null,
    /// KV cache compaction ratio after prefill. null = no compaction.
    /// 0.02 = 50x compression, 0.1 = 10x compression.
    cache_compaction_ratio: ?f32 = null,
    prompt_cache_enabled: bool = false,
    prompt_cache_key: ?[]const u8 = null,
    /// Benchmark/compatibility mode: continue decoding when the model emits an
    /// EOS token. Defaults to production stop-on-EOS behavior.
    ignore_eos: bool = false,
    /// Optional chat-template reasoning control. Null preserves the model's
    /// default; false asks templates that support `enable_thinking` to open a
    /// public/final response channel directly.
    enable_thinking: ?bool = null,
    /// Writes deterministic Qwen3-VL preprocessing/expansion evidence for an
    /// offline qualification harness. Ordinary serving must leave this null.
    qwen3vl_parity_json_path: ?[]const u8 = null,
    /// Optional canonical little-endian f32 patch payload paired with the
    /// Qwen3-VL parity JSON. Requires qwen3vl_parity_json_path.
    qwen3vl_parity_patch_path: ?[]const u8 = null,
    /// Optional canonical little-endian f32 logits for the final Qwen3-VL
    /// prefill row. Qualification-only; requires qwen3vl_parity_json_path.
    qwen3vl_parity_logits_path: ?[]const u8 = null,
};

pub fn kvSlidingTrimForced() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_KV_SLIDING_TRIM", false);
}

pub fn metalSplitSwaRingRequestEligible(
    model_config: gpt_mod.Config,
    generation_config: GenerationConfig,
) bool {
    if (comptime !build_options.enable_metal) return false;
    return model_config.supportsSplitSwaGlobalKvRing() and
        !gemma4_runtime.wholeFramePrefillExplicitlyDisabled() and
        !generation_config.prompt_cache_enabled and
        generation_config.cache_compaction_ratio == null and
        !kvSlidingTrimForced() and
        backends.metal_kv_storage.MetalKvStorage.splitSwaKvRingEnabled();
}

pub fn metalSplitSwaRingEligible(
    model_config: gpt_mod.Config,
    generation_config: GenerationConfig,
    kv_dtype: runtime.kv.pool.KvDType,
) bool {
    if (comptime !build_options.enable_metal) return false;
    return metalSplitSwaRingRequestEligible(model_config, generation_config) and
        backends.metal_kv_storage.KeyFormat.fromKvDType(kv_dtype) != null;
}

pub const GenerationKvRoute = enum {
    standard,
    metal_whole_model,
};

fn generationKvCapacityPolicy(
    route: GenerationKvRoute,
    split_swa_ring_eligible: bool,
) runtime.tier.memory.GenerationKvCapacityPolicy {
    return if (route == .metal_whole_model and split_swa_ring_eligible)
        .split_swa_ring
    else
        .full_history;
}

/// Resolves the KV geometry that both request admission and execution must use.
/// Keeping this decision route-scoped prevents a caller from budgeting a
/// different cache layout than the whole-model runtime will allocate.
pub fn generationKvCapacityPolicyForRoute(
    route: GenerationKvRoute,
    model_config: gpt_mod.Config,
    generation_config: GenerationConfig,
    kv_dtype: runtime.kv.pool.KvDType,
) runtime.tier.memory.GenerationKvCapacityPolicy {
    if (route == .standard) return .full_history;
    return generationKvCapacityPolicy(
        route,
        metalSplitSwaRingEligible(model_config, generation_config, kv_dtype),
    );
}

pub fn kvPoolConfig(
    backend: runtime.kv.pool.BackendKind,
    dtype: runtime.kv.pool.KvDType,
    config: gpt_mod.Config,
    force_sliding_trim: bool,
) runtime.kv.pool.KvPoolConfig {
    return .{
        .backend = backend,
        .dtype = dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(config.num_hidden_layers),
        .num_kv_heads = config.maxKvHeads(),
        .head_dim = config.maxHeadDim(),
        .sliding_window_size = config.kvPoolSlidingWindowSize(force_sliding_trim),
    };
}

/// Parsed chat template for rendering messages via Jinja2.
pub const ChatTemplate = struct {
    template: jinja.Template,
    bos_token: []const u8,
    eos_token: []const u8,
    unk_token: []const u8,
    pad_token: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        source: []const u8,
        bos_token: []const u8,
        eos_token: []const u8,
        unk_token: []const u8,
        pad_token: []const u8,
    ) !ChatTemplate {
        return .{
            .template = try jinja.Template.initHuggingFace(allocator, source),
            .bos_token = bos_token,
            .eos_token = eos_token,
            .unk_token = unk_token,
            .pad_token = pad_token,
        };
    }

    pub const ApplyOptions = struct {
        add_generation_prompt: bool = true,
        enable_thinking: ?bool = null,
    };

    pub fn apply(self: *const ChatTemplate, allocator: std.mem.Allocator, messages: []const Message, add_generation_prompt: bool) ![]u8 {
        return self.applyWithOptions(allocator, messages, .{
            .add_generation_prompt = add_generation_prompt,
        });
    }

    pub fn applyWithOptions(self: *const ChatTemplate, allocator: std.mem.Allocator, messages: []const Message, options: ApplyOptions) ![]u8 {
        // Rendering builds a tree of temporary Jinja values and strings. Keep
        // those allocations in a request-local arena and return only the
        // duplicated result to the caller's allocator.
        var render_arena = std.heap.ArenaAllocator.init(allocator);
        defer render_arena.deinit();
        const arena = render_arena.allocator();

        // Convert Message into jinja.ChatMessage, preserving structured
        // image/text parts when available for multimodal chat templates.
        const chat_msgs = try arena.alloc(jinja.ChatMessage, messages.len);
        for (messages, 0..) |m, i| {
            var parts: ?[]const jinja.ChatContentPart = null;
            if (m.content_parts) |message_parts| {
                const chat_parts = try arena.alloc(jinja.ChatContentPart, message_parts.len);
                for (message_parts, 0..) |part, part_idx| {
                    chat_parts[part_idx] = switch (part) {
                        .text => |text| .{ .text = text },
                        .image => .image,
                        .audio => .audio,
                    };
                }
                parts = chat_parts;
            }
            chat_msgs[i] = .{
                .role = m.role,
                .content = m.content,
                .parts = parts,
            };
        }

        var ctx = try jinja.chatTemplateContext(arena, chat_msgs, .{
            .add_generation_prompt = options.add_generation_prompt,
            .bos_token = self.bos_token,
            .eos_token = self.eos_token,
            .unk_token = self.unk_token,
            .pad_token = self.pad_token,
            .enable_thinking = options.enable_thinking,
        });

        const result = try self.template.render(arena, &ctx);
        return try allocator.dupe(u8, result);
    }

    pub fn deinit(self: *ChatTemplate) void {
        self.template.deinit();
    }
};

pub const GenerationResult = struct {
    text: []const u8,
    reasoning_text: ?[]const u8 = null,
    token_ids: ?[]i32 = null,
    prompt_tokens: usize = 0,
    tokens_used: usize,
    finish_reason: []const u8,
    cached_prompt_tokens: usize = 0,
    timing_ms: ?GenerationTimingMs = null,
    speculative: ?SpeculativeDecodeStats = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerationResult) void {
        self.allocator.free(self.text);
        if (self.reasoning_text) |reasoning| self.allocator.free(reasoning);
        if (self.token_ids) |ids| self.allocator.free(ids);
    }
};

pub const GenerationTimingMs = struct {
    prompt_format: u64 = 0,
    tokenize: u64 = 0,
    /// Image/audio collection, preprocessing, projector execution, and
    /// construction of the expanded multimodal prompt embeddings.
    multimodal_prepare: u64 = 0,
    runtime_prepare: u64 = 0,
    prefill: u64 = 0,
    decode: u64 = 0,
    text_decode: u64 = 0,
    total: u64 = 0,
};

pub const MtpQualityStats = struct {
    mismatches: usize = 0,
    mismatches_with_assistant_logits: usize = 0,
    target_in_assistant_top2: usize = 0,
    target_in_assistant_top4: usize = 0,
    target_in_assistant_top8: usize = 0,
    draft_in_target_top2: usize = 0,
    format_or_control_misses: usize = 0,
    near_tie_misses: usize = 0,
    confident_misses: usize = 0,
    assistant_target_margin_sum: f32 = 0,

    pub fn merge(self: *MtpQualityStats, other: MtpQualityStats) void {
        self.mismatches += other.mismatches;
        self.mismatches_with_assistant_logits += other.mismatches_with_assistant_logits;
        self.target_in_assistant_top2 += other.target_in_assistant_top2;
        self.target_in_assistant_top4 += other.target_in_assistant_top4;
        self.target_in_assistant_top8 += other.target_in_assistant_top8;
        self.draft_in_target_top2 += other.draft_in_target_top2;
        self.format_or_control_misses += other.format_or_control_misses;
        self.near_tie_misses += other.near_tie_misses;
        self.confident_misses += other.confident_misses;
        self.assistant_target_margin_sum += other.assistant_target_margin_sum;
    }

    pub fn averageAssistantTargetMargin(self: MtpQualityStats) f32 {
        if (self.mismatches_with_assistant_logits == 0) return 0;
        return self.assistant_target_margin_sum / @as(f32, @floatFromInt(self.mismatches_with_assistant_logits));
    }
};

pub const MtpProfileStats = struct {
    enabled: bool = false,
    sync_enabled: bool = false,
    draft_steps: usize = 0,
    resident_draft_steps: usize = 0,
    host_draft_steps: usize = 0,
    target_verify_calls: usize = 0,
    target_verify_rows: usize = 0,
    target_verify_argmax_calls: usize = 0,
    target_verify_argmax_rows: usize = 0,
    target_verify_argmax_batched_calls: usize = 0,
    target_verify_argmax_syncs: usize = 0,
    dedicated_runtime_hits: usize = 0,
    dedicated_runtime_fallbacks: usize = 0,
    device_verify_commit_hits: usize = 0,
    device_verify_commit_fallbacks: usize = 0,
    device_verify_commit_result_downloads: usize = 0,
    target_choice_downloads: usize = 0,
    commit_forwards_required: usize = 0,
    commit_forwards_avoided: usize = 0,
    accepted_hidden_reuse_rows: usize = 0,
    activation_copies: usize = 0,
    materializations: usize = 0,
    materialization_hidden_only_hits: usize = 0,
    materialization_hidden_only_fallbacks: usize = 0,
    correction_materializations: usize = 0,
    bonus_materializations: usize = 0,
    deferred_materializations: usize = 0,
    pending_flushes: usize = 0,
    bonus_skips: usize = 0,
    fallback_calls: usize = 0,
    draft_embedding_cache_hits: usize = 0,
    draft_embedding_cache_misses: usize = 0,
    draft_embedding_cache_inserts: usize = 0,
    draft_embedding_cache_evictions: usize = 0,
    draft_embedding_cache_disabled: usize = 0,
    draft_target_embedding_cross_copies: usize = 0,
    draft_token_ns: u64 = 0,
    draft_target_embedding_ns: u64 = 0,
    draft_concat_ns: u64 = 0,
    draft_preprojection_ns: u64 = 0,
    draft_assistant_ns: u64 = 0,
    draft_postprojection_ns: u64 = 0,
    draft_argmax_ns: u64 = 0,
    draft_lm_head_ns: u64 = 0,
    draft_selection_ns: u64 = 0,
    target_verify_ns: u64 = 0,
    activation_copy_ns: u64 = 0,
    materialization_ns: u64 = 0,
    fallback_ns: u64 = 0,
};

pub const SpeculativeDecodeStats = struct {
    pub const PolicyDecision = enum {
        inactive,
        active,
        forced,
        disabled_off,
        disabled_unavailable,
        disabled_uncalibrated,
        disabled_low_acceptance,
        disabled_zero_match,
        disabled_slow,
        disabled_insufficient_probe,

        pub fn name(self: PolicyDecision) []const u8 {
            return switch (self) {
                .inactive => "inactive",
                .active => "active",
                .forced => "forced",
                .disabled_off => "disabled_off",
                .disabled_unavailable => "disabled_unavailable",
                .disabled_uncalibrated => "disabled_uncalibrated",
                .disabled_low_acceptance => "disabled_low_acceptance",
                .disabled_zero_match => "disabled_zero_match",
                .disabled_slow => "disabled_slow",
                .disabled_insufficient_probe => "disabled_insufficient_probe",
            };
        }
    };

    speculation_policy: SpeculationPolicy = .auto,
    speculation_calibration: SpeculationCalibration = .none,
    speculation_policy_decision: PolicyDecision = .inactive,
    rounds: usize = 0,
    drafted_tokens: usize = 0,
    matched_draft_tokens: usize = 0,
    accepted_tokens: usize = 0,
    correction_tokens: usize = 0,
    bonus_tokens: usize = 0,
    adaptive_fallbacks: usize = 0,
    mtp_enabled: bool = false,
    mtp_disabled_reason: ?[]const u8 = null,
    mtp_graph_replay_status: []const u8 = "off",
    mtp_acceptance_gate_fallbacks: usize = 0,
    mtp_quality: MtpQualityStats = .{},
    mtp_profile: MtpProfileStats = .{},

    pub fn rejectedDraftTokens(self: SpeculativeDecodeStats) usize {
        return self.drafted_tokens -| self.matched_draft_tokens;
    }

    pub fn acceptancePermille(self: SpeculativeDecodeStats) usize {
        if (self.drafted_tokens == 0) return 0;
        return (self.matched_draft_tokens * 1000) / self.drafted_tokens;
    }
};

/// Streaming token callback. Called with each decoded text delta.
/// Return `true` to continue generation, `false` to stop early.
pub const TokenCallback = *const fn (ctx: *anyopaque, token_text: []const u8) bool;

fn emitCompletedStreamingText(ctx: *anyopaque, on_token: TokenCallback, text: []const u8) bool {
    return on_token(ctx, text);
}

/// Reconcile the final buffered projection with deltas already emitted. This
/// preserves fail-closed streaming while supporting checkpoints that reveal
/// the public answer only through the bare-channel-close convention: their
/// content is emitted once generation proves that no later explicit header
/// supersedes it.
fn emitCompletedProjectionDelta(
    state: *const StreamingTextState,
    ctx: *anyopaque,
    on_token: TokenCallback,
    completed_text: []const u8,
) bool {
    if (!std.mem.startsWith(u8, completed_text, state.emitted_text)) return true;
    const delta = completed_text[state.emitted_text.len..];
    if (delta.len == 0) return true;
    return emitCompletedStreamingText(ctx, on_token, delta);
}

const StreamingTextState = struct {
    emitted_text: []u8,
    /// Set from the model contract, independently of tokenizer resolution.
    /// A channel-aware model must never fall back to decoding its raw generated
    /// tokens merely because one of the protocol tokens is missing or malformed.
    final_channel_required: bool = false,
    /// The prompt itself may already have opened the public final channel.
    /// Projection still remains active so a later channel/turn boundary can
    /// never expose private or malformed continuation text.
    final_channel_preopened: bool = false,
    final_channel_end_token_id: ?i32 = null,
    /// Exact token sequence for `<|channel>final\n<channel|>`. A bare
    /// `<channel|>` also terminates private/intermediate channel headers, so it
    /// is not sufficient evidence that subsequent content is public.
    final_channel_header_token_ids: []const i32 = &.{},
    channel_start_token_id: ?i32 = null,
    turn_end_token_id: ?i32 = null,
    inspected_token_count: usize = 0,
    final_channel_content_start: ?usize = null,
    final_channel_content_end: ?usize = null,
    saw_final_channel: bool = false,
};

const gemma4_thought_channel_prompt_suffix = "<|channel>thought\n<channel|>";
const gemma4_final_channel_prompt_suffix = "<|channel>final\n<channel|>";

fn promptOpensGemma4FinalChannel(prompt: []const u8) bool {
    return std.mem.endsWith(
        u8,
        std.mem.trimEnd(u8, prompt, &std.ascii.whitespace),
        gemma4_final_channel_prompt_suffix,
    );
}

fn configPromptOpensGemma4FinalChannel(config: gpt_mod.Config, prompt: []const u8) bool {
    return config.usesGemma4Channels() and promptOpensGemma4FinalChannel(prompt);
}

/// Grammar-constrained generation must start in a public channel because the
/// grammar applies to the first generated token. Leaving the normal private
/// `thought` channel open would make the grammar reject the final-channel
/// transition and the fail-closed response projection would correctly withhold
/// the entire result.
fn openGemma4FinalChannelForGrammar(
    allocator: std.mem.Allocator,
    prompt: []const u8,
) ![]u8 {
    const trimmed = std.mem.trimEnd(u8, prompt, &std.ascii.whitespace);
    const trailing = prompt[trimmed.len..];
    if (std.mem.endsWith(u8, trimmed, gemma4_final_channel_prompt_suffix)) {
        return allocator.dupe(u8, prompt);
    }
    if (!std.mem.endsWith(u8, trimmed, gemma4_thought_channel_prompt_suffix)) {
        return error.GrammarRequiresGemma4ChannelPrompt;
    }

    const prefix_len = trimmed.len - gemma4_thought_channel_prompt_suffix.len;
    const result = try allocator.alloc(
        u8,
        prefix_len + gemma4_final_channel_prompt_suffix.len + trailing.len,
    );
    errdefer allocator.free(result);
    @memcpy(result[0..prefix_len], trimmed[0..prefix_len]);
    @memcpy(
        result[prefix_len .. prefix_len + gemma4_final_channel_prompt_suffix.len],
        gemma4_final_channel_prompt_suffix,
    );
    @memcpy(result[result.len - trailing.len ..], trailing);
    return result;
}

fn finalChannelContentStart(token_ids: []const i64, marker_id: ?i32) ?usize {
    const marker = marker_id orelse return null;
    var start: ?usize = null;
    for (token_ids, 0..) |token_id, idx| {
        if (token_id == marker) start = idx + 1;
    }
    return start;
}

const FinalChannelRange = struct {
    start: usize,
    end: usize,
};

fn firstPublicChannelBoundary(
    token_ids: []const i64,
    channel_end_id: i32,
    channel_start_id: i32,
    turn_end_id: i32,
) ?usize {
    for (token_ids, 0..) |token_id, idx| {
        if (token_id == channel_end_id or
            token_id == channel_start_id or
            token_id == turn_end_id)
        {
            return idx;
        }
    }
    return null;
}

fn completeFinalChannelRange(
    token_ids: []const i64,
    final_header_token_ids: []const i32,
    channel_end_id: ?i32,
    channel_start_id: ?i32,
    turn_end_id: ?i32,
) ?FinalChannelRange {
    const start = findTokenSequenceEndingAtOrAfter(
        token_ids,
        final_header_token_ids,
        0,
    ) orelse return null;
    const marker = channel_end_id orelse return null;
    const channel_start = channel_start_id orelse return null;
    const turn_end = turn_end_id orelse return null;
    const end = if (firstPublicChannelBoundary(
        token_ids[start..],
        marker,
        channel_start,
        turn_end,
    )) |offset| start + offset else token_ids.len;
    return .{ .start = start, .end = end };
}

fn finalChannelTokenSlice(token_ids: []const i64, marker_id: ?i32) []const i64 {
    const start = finalChannelContentStart(token_ids, marker_id) orelse return token_ids;
    return token_ids[start..];
}

/// Public content for a checkpoint that never emits the explicit
/// `<|channel>final` header: the ggml-org Gemma4 GGUF conversions close the
/// prompt-opened private channel with a bare `<channel|>` and write the public
/// answer directly after it. Content before the first bare close stays
/// private; explicitly opened channels (any `<|channel>` header) keep their
/// content private; the public window ends at the next protocol boundary.
/// Callers must prefer the exact final-channel header when one is present.
fn bareChannelCloseRange(
    token_ids: []const i64,
    channel_end_id: i32,
    channel_start_id: i32,
    turn_end_id: i32,
) ?FinalChannelRange {
    var idx: usize = 0;
    var start: ?usize = null;
    while (idx < token_ids.len) : (idx += 1) {
        const token = token_ids[idx];
        if (token == channel_start_id) {
            // Explicit header: skip to its closing marker; the named channel
            // owns subsequent content, so it never becomes public here.
            while (idx < token_ids.len and token_ids[idx] != channel_end_id) : (idx += 1) {}
            if (idx >= token_ids.len) return null;
            continue;
        }
        if (token == channel_end_id) {
            start = idx + 1;
            break;
        }
    }
    const content_start = start orelse return null;
    var end = token_ids.len;
    for (token_ids[content_start..], content_start..) |token, i| {
        if (token == turn_end_id or token == channel_start_id or token == channel_end_id) {
            end = i;
            break;
        }
    }
    if (end <= content_start) return null;
    return .{ .start = content_start, .end = end };
}

fn finalResponseTokenSlice(
    token_ids: []const i64,
    final_channel_required: bool,
    final_channel_preopened: bool,
    marker_id: ?i32,
    final_header_token_ids: []const i32,
    channel_start_id: ?i32,
    turn_end_id: ?i32,
) []const i64 {
    if (!final_channel_required) return token_ids;
    if (marker_id == null or
        turn_end_id == null or
        channel_start_id == null or
        final_header_token_ids.len == 0)
    {
        return token_ids[0..0];
    }
    if (final_channel_preopened) {
        const end = firstPublicChannelBoundary(
            token_ids,
            marker_id.?,
            channel_start_id.?,
            turn_end_id.?,
        ) orelse token_ids.len;
        return token_ids[0..end];
    }
    if (completeFinalChannelRange(
        token_ids,
        final_header_token_ids,
        marker_id,
        channel_start_id,
        turn_end_id,
    )) |range| {
        return token_ids[range.start..range.end];
    }
    // No explicit final-channel header anywhere in the stream: accept the
    // bare-close convention (see bareChannelCloseRange). Streams that contain
    // headers were already handled above, so a stray bare close in a
    // header-emitting stream cannot leak private channel content here.
    if (bareChannelCloseRange(
        token_ids,
        marker_id.?,
        channel_start_id.?,
        turn_end_id.?,
    )) |range| {
        return token_ids[range.start..range.end];
    }
    // The generation prompt for a channel-aware model opens the private
    // `thought` channel. Without a recognized transition, every generated
    // token is private regardless of whether generation ended by length,
    // EOS, cancellation, or a malformed protocol boundary.
    return token_ids[0..0];
}

/// Private thought-channel content, excluding protocol headers. This mirrors
/// finalResponseTokenSlice so callers can expose reasoning separately without
/// ever decoding channel-control tokens into public text.
fn reasoningResponseTokenSlice(
    token_ids: []const i64,
    final_channel_required: bool,
    final_channel_preopened: bool,
    marker_id: ?i32,
    final_header_token_ids: []const i32,
    channel_start_id: ?i32,
    turn_end_id: ?i32,
) []const i64 {
    if (!final_channel_required or final_channel_preopened) return token_ids[0..0];
    const marker = marker_id orelse return token_ids[0..0];
    const channel_start = channel_start_id orelse return token_ids[0..0];
    const turn_end = turn_end_id orelse return token_ids[0..0];
    if (final_header_token_ids.len == 0) return token_ids[0..0];

    var reasoning_end = token_ids.len;
    if (completeFinalChannelRange(
        token_ids,
        final_header_token_ids,
        marker,
        channel_start,
        turn_end,
    )) |range| {
        reasoning_end = range.start - final_header_token_ids.len;
    } else if (bareChannelCloseRange(
        token_ids,
        marker,
        channel_start,
        turn_end,
    )) |range| {
        reasoning_end = range.start - 1;
    } else if (firstPublicChannelBoundary(
        token_ids,
        marker,
        channel_start,
        turn_end,
    )) |boundary| {
        reasoning_end = boundary;
    }

    // A model may explicitly open or reopen a private channel before
    // transitioning to final. Keep only that channel's content, not its
    // name/header tokens. A bare close or turn end is the first public
    // boundary even if an explicit final header appears later: otherwise the
    // marker and tentative public text would be decoded as reasoning.
    var reasoning_start: usize = 0;
    var idx: usize = 0;
    while (idx < reasoning_end) {
        if (token_ids[idx] == marker or token_ids[idx] == turn_end) {
            reasoning_end = idx;
            break;
        }
        if (token_ids[idx] != channel_start) {
            idx += 1;
            continue;
        }
        var close = idx + 1;
        while (close < reasoning_end and token_ids[close] != marker) : (close += 1) {}
        if (close >= reasoning_end) {
            reasoning_end = idx;
            break;
        }
        reasoning_start = close + 1;
        idx = close + 1;
    }
    while (reasoning_end > reasoning_start and
        (token_ids[reasoning_end - 1] == marker or
            token_ids[reasoning_end - 1] == channel_start or
            token_ids[reasoning_end - 1] == turn_end))
    {
        reasoning_end -= 1;
    }
    if (reasoning_end <= reasoning_start) return token_ids[0..0];
    return token_ids[reasoning_start..reasoning_end];
}

fn generationResultTokenSlice(
    raw_token_ids: []const i64,
    public_token_ids: []const i64,
    return_raw_token_ids: bool,
) []const i64 {
    return if (return_raw_token_ids) raw_token_ids else public_token_ids;
}

fn resolveTokenId(
    tokenizer: tokenizer_mod.Tokenizer,
    allocator: std.mem.Allocator,
    token_text: []const u8,
) !?i32 {
    const ids = try tokenizer.encode(allocator, token_text);
    defer allocator.free(ids);
    if (ids.len != 1) return null;
    return ids[0];
}

fn findTokenSequenceEndingAtOrAfter(
    token_ids: []const i64,
    pattern: []const i32,
    first_uninspected_end: usize,
) ?usize {
    if (pattern.len == 0 or token_ids.len < pattern.len) return null;
    const first_end = @max(first_uninspected_end, pattern.len - 1);
    for (first_end..token_ids.len) |end| {
        if (token_ids[end] != pattern[pattern.len - 1]) continue;
        const start = end + 1 - pattern.len;
        var matches = true;
        for (pattern, 0..) |expected, offset| {
            if (token_ids[start + offset] != expected) {
                matches = false;
                break;
            }
        }
        if (matches) return end + 1;
    }
    return null;
}

fn emitDecodedDeltaForTokenizer(
    tokenizer: tokenizer_mod.Tokenizer,
    allocator: std.mem.Allocator,
    generated_token_ids: []const i64,
    state: *StreamingTextState,
    on_token_fn: TokenCallback,
    on_token_ctx: *anyopaque,
) !bool {
    var projected_ids = generated_token_ids;
    if (state.final_channel_required) {
        if (state.final_channel_end_token_id == null) return true;
        const turn_end = state.turn_end_token_id orelse return true;
        const channel_start_token = state.channel_start_token_id orelse return true;
        if (state.final_channel_header_token_ids.len == 0) return true;

        if (state.final_channel_content_start == null) {
            // Generation grows monotonically. Only consider newly appended
            // sequence ends, while allowing the header itself to straddle
            // callbacks. This is O(new tokens * short header length) and never
            // rescans a potentially long reasoning prefix.
            const first_uninspected_end = @min(state.inspected_token_count, generated_token_ids.len);
            if (findTokenSequenceEndingAtOrAfter(
                generated_token_ids,
                state.final_channel_header_token_ids,
                first_uninspected_end,
            )) |content_start| {
                state.final_channel_content_start = content_start;
                state.saw_final_channel = true;
                // Let the turn-end scan below cover content appended in the
                // same speculative batch as the final-channel header.
                state.inspected_token_count = content_start;
            } else {
                state.inspected_token_count = generated_token_ids.len;
            }
        }

        const channel_start = state.final_channel_content_start orelse return true;
        if (state.final_channel_content_end == null) {
            const scan_start = @max(
                channel_start,
                @min(state.inspected_token_count, generated_token_ids.len),
            );
            const content_end_offset = firstPublicChannelBoundary(
                generated_token_ids[scan_start..],
                state.final_channel_end_token_id.?,
                channel_start_token,
                turn_end,
            );
            if (content_end_offset) |offset| {
                state.final_channel_content_end = scan_start + offset;
            }
            state.inspected_token_count = generated_token_ids.len;
        }
        const channel_end = state.final_channel_content_end orelse generated_token_ids.len;
        projected_ids = generated_token_ids[channel_start..channel_end];
    }

    const decoded_ids = try allocator.alloc(i32, projected_ids.len);
    defer allocator.free(decoded_ids);
    for (projected_ids, 0..) |token_id, idx| decoded_ids[idx] = @intCast(token_id);

    const decoded_text = try tokenizer.decode(allocator, decoded_ids);
    defer allocator.free(decoded_text);

    const prefix_len = std.mem.indexOfDiff(u8, state.emitted_text, decoded_text) orelse @min(state.emitted_text.len, decoded_text.len);
    const delta = decoded_text[prefix_len..];

    try replaceStreamingEmittedText(allocator, state, decoded_text);
    if (delta.len == 0) return true;
    return on_token_fn(on_token_ctx, delta);
}

fn replaceStreamingEmittedText(
    allocator: std.mem.Allocator,
    state: *StreamingTextState,
    decoded_text: []const u8,
) !void {
    // Allocate first so an OOM leaves the old owned buffer intact for the
    // request-level cleanup path.
    const replacement = try allocator.dupe(u8, decoded_text);
    allocator.free(state.emitted_text);
    state.emitted_text = replacement;
}

pub const KvView = struct {
    sequence_id: runtime.kv.manager.SequenceId,
    pool_id: runtime.kv.block.KvPoolId,
    logical_block_count: usize,
    tail_tokens: u16,
    token_count: usize,
    position_offset: usize,
    max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
    logical_blocks: ?[]const runtime.kv.block.KvBlockId = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
};

pub const KvMetadataDelta = struct {
    sequence_replaced: bool = false,
    logical_block_count_before: usize = 0,
    logical_block_count_after: usize = 0,
    position_offset_before: usize = 0,
    position_offset_after: usize = 0,
    retained_tokens: ?usize = null,
};

pub const KvMutationResult = struct {
    token_count: usize,
    kv_view: ?KvView,
    compacted: bool,
    delta: KvMetadataDelta = .{},
};

pub const OwnedBatchDecodeContext = struct {
    allocator: std.mem.Allocator,
    kv_batch: ?[]gpt_arch.DecodeContext.KvBatchView = null,
    context: gpt_arch.DecodeContext,

    pub fn deinit(self: *OwnedBatchDecodeContext) void {
        if (self.kv_batch) |batch| self.allocator.free(batch);
        self.kv_batch = null;
    }
};

const SamplingPenaltyState = struct {
    counts: std.AutoHashMapUnmanaged(u32, u32) = .empty,
    enabled: bool = true,

    fn init(enabled: bool) SamplingPenaltyState {
        return .{ .enabled = enabled };
    }

    fn deinit(self: *SamplingPenaltyState, allocator: std.mem.Allocator) void {
        self.counts.deinit(allocator);
        self.* = .{};
    }

    fn seedFromHistory(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_history: []const i64) !void {
        for (token_history) |token_id| try self.noteToken(allocator, token_id);
    }

    fn noteToken(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_id: i64) !void {
        if (!self.enabled) return;
        if (token_id < 0) return;
        const entry = try self.counts.getOrPut(allocator, @intCast(token_id));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }

    fn noteTokens(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_ids: []const i64) !void {
        for (token_ids) |token_id| try self.noteToken(allocator, token_id);
    }

    fn clone(self: *const SamplingPenaltyState, allocator: std.mem.Allocator) !SamplingPenaltyState {
        var copy = SamplingPenaltyState.init(self.enabled);
        errdefer copy.deinit(allocator);

        var it = self.counts.iterator();
        while (it.next()) |entry| {
            try copy.counts.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
        }
        return copy;
    }

    fn isEmpty(self: *const SamplingPenaltyState) bool {
        return self.counts.count() == 0;
    }
};

fn disablePagedKvDebug() bool {
    return getenvBool("TERMITE_DISABLE_PAGED_KV");
}

fn enableGenerationStageDebug() bool {
    return getenvBool("TERMITE_GEN_STAGE_DEBUG");
}

// Promoted default: the CUDA prefill-first-token path is default-on. The
// route is still guarded by shouldUsePrefillFirstTokenPath (CUDA backend,
// pure-greedy, non-speculative); explicit env values override both ways.
fn enableCudaPrefillFirstToken() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN", true);
}

fn enableCudaPrefillGreedyToken() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PREFILL_GREEDY_TOKEN", true);
}

fn enableCudaPreparedTailPrefillGreedy() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PREFILL_PREPARED_TAIL_GREEDY", false);
}

fn enableCudaGreedyDeviceTokenHandoff() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GREEDY_DEVICE_TOKEN_HANDOFF", true);
}

fn enableCudaGatedTokenTensorDecode() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GATED_TOKEN_TENSOR_DECODE", false);
}

fn metalFinalHiddenFrameOwnsResidentPrefix(retained_prefix_tokens: usize) bool {
    return retained_prefix_tokens == 0;
}

fn cudaReplayLastLogitsEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS", true);
}

// Promoted default: pending-token readback overlaps the previous token's D2H
// copy with the next decode step. Exact-output; still guarded by
// shouldUseCudaPendingTokenReadback (CUDA, pure-greedy, no grammar/speculation
// /graph runtime). Explicit env values override both ways.
fn enableCudaGreedyPendingTokenReadback() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK", true);
}

fn shouldUseCudaPendingTokenReadback(
    backend_kind: ops.BackendKind,
    enabled: bool,
    pure_greedy: bool,
    use_speculative: bool,
    has_grammar: bool,
    graph_runtime_present: bool,
    execution_lock_present: bool,
    max_tokens: usize,
) bool {
    return enabled and
        backend_kind == .cuda and
        pure_greedy and
        !use_speculative and
        !has_grammar and
        !graph_runtime_present and
        !execution_lock_present and
        max_tokens >= 2;
}

fn pendingTokenReadbackStreamingArgsValid(
    on_token_fn: ?TokenCallback,
    on_token_ctx: ?*anyopaque,
    emitted_text: ?*StreamingTextState,
) bool {
    const any_present = on_token_fn != null or on_token_ctx != null or emitted_text != null;
    const all_present = on_token_fn != null and on_token_ctx != null and emitted_text != null;
    return !any_present or all_present;
}

// Promoted default: coalesce the whole prompt into the first-token prefill
// chunk for prompts up to 2048 tokens (tuned profile value; previously 8).
// Explicit env values override both ways; 0 disables coalescing.
fn cudaPrefillFirstTokenCoalesceTokenLimit() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS") orelse 2048;
}

fn enableFirstTokenTrace() bool {
    return getenvBool("ANTFLY_INFERENCE_GENERATE_FIRST_TOKEN_TRACE");
}

fn firstTokenTopKTraceCount() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_GENERATE_FIRST_TOKEN_TOP_K") orelse 0;
}

fn enableGemma4MtpDebug() bool {
    return gemma4_mtp_debug_override or getenvBool("ANTFLY_GEMMA4_MTP_DEBUG") or getenvBool("TERMITE_DEBUG_GEMMA4_MTP");
}

fn enableGemma4MtpVerifyTrace() bool {
    return getenvBool("ANTFLY_GEMMA4_MTP_VERIFY_TRACE");
}

const Gemma4MtpPositionMode = enum {
    target_absolute,
    target_constant,
    legacy_one,

    fn name(self: Gemma4MtpPositionMode) []const u8 {
        return switch (self) {
            .target_absolute => "target_absolute",
            .target_constant => "target_constant",
            .legacy_one => "legacy_one",
        };
    }
};

const Gemma4MtpTargetHiddenSource = enum {
    final,
    pre_norm,

    fn name(self: Gemma4MtpTargetHiddenSource) []const u8 {
        return switch (self) {
            .final => "final",
            .pre_norm => "pre_norm",
        };
    }
};

fn parseGemma4MtpPositionMode(raw: ?[]const u8) Gemma4MtpPositionMode {
    const value = raw orelse return .target_constant;
    if (std.mem.eql(u8, value, "legacy_one")) return .legacy_one;
    if (std.mem.eql(u8, value, "target_constant")) return .target_constant;
    if (std.mem.eql(u8, value, "block_constant")) return .target_constant;
    if (std.mem.eql(u8, value, "target_absolute")) return .target_absolute;
    return .target_constant;
}

fn gemma4MtpPositionMode() Gemma4MtpPositionMode {
    return parseGemma4MtpPositionMode(platform.env.getenv("ANTFLY_GEMMA4_MTP_POSITION_MODE"));
}

fn parseGemma4MtpTargetHiddenSource(raw: ?[]const u8) Gemma4MtpTargetHiddenSource {
    const value = raw orelse return .final;
    if (std.mem.eql(u8, value, "pre_norm")) return .pre_norm;
    if (std.mem.eql(u8, value, "final")) return .final;
    return .final;
}

fn gemma4MtpTargetHiddenSource() Gemma4MtpTargetHiddenSource {
    return parseGemma4MtpTargetHiddenSource(platform.env.getenv("ANTFLY_GEMMA4_MTP_TARGET_HIDDEN_SOURCE"));
}

fn gemma4MtpAssistantTotalSequenceLen(mode: Gemma4MtpPositionMode, seq_len: usize, draft_count: usize) usize {
    return switch (mode) {
        .target_absolute => seq_len + draft_count,
        .target_constant => seq_len,
        .legacy_one => 1,
    };
}

fn gemma4MtpAssistantDecodeContext(
    decode_state: *NativeDecodeState,
    mode: Gemma4MtpPositionMode,
    seq_len: usize,
    draft_count: usize,
) gpt_arch.DecodeContext {
    const context_seq_len = switch (mode) {
        .target_absolute => seq_len + draft_count,
        .target_constant => seq_len,
        .legacy_one => seq_len,
    };
    var ctx = decode_state.gptDecodeContext(context_seq_len, 1);
    if (mode == .legacy_one) {
        ctx.total_sequence_len = 1;
        ctx.query_sequence_len = 1;
    }
    return ctx;
}

fn gemma4MtpTopKDiagnosticCount() usize {
    return @min(platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_TOPK") orelse 0, 16);
}

fn traceGraphExecutorOutputs() bool {
    return getenvBool("TERMITE_GRAPH_EXECUTOR_TRACE_OUTPUTS");
}

fn debugGenerationStage(comptime fmt: []const u8, args: anytype) void {
    if (!enableGenerationStageDebug()) return;
    std.debug.print("gen_debug: " ++ fmt ++ "\n", args);
}

fn debugFirstToken(comptime fmt: []const u8, args: anytype) void {
    if (!enableFirstTokenTrace()) return;
    std.debug.print("first_token_debug: " ++ fmt ++ "\n", args);
}

fn debugGemma4Mtp(comptime fmt: []const u8, args: anytype) void {
    if (!enableGemma4MtpDebug()) return;
    std.debug.print("gemma4_mtp_debug: " ++ fmt ++ "\n", args);
}

fn debugGemma4MtpLogitChoice(prefix: []const u8, index: usize, logits: []const f32, draft_token: ?i64, target_token: usize) void {
    if (!enableGemma4MtpDebug()) return;
    var top1_token: usize = 0;
    var top2_token: usize = 0;
    var top1_score = -std.math.inf(f32);
    var top2_score = -std.math.inf(f32);
    for (logits, 0..) |score, token| {
        if (score > top1_score) {
            top2_score = top1_score;
            top2_token = top1_token;
            top1_score = score;
            top1_token = token;
        } else if (score > top2_score) {
            top2_score = score;
            top2_token = token;
        }
    }
    const margin = top1_score - top2_score;
    if (draft_token) |draft| {
        std.debug.print(
            "gemma4_mtp_debug: {s} index={d} draft={d} target={d} top1={d} top2={d} margin={d:.6}\n",
            .{ prefix, index, draft, target_token, top1_token, top2_token, margin },
        );
    } else {
        std.debug.print(
            "gemma4_mtp_debug: {s} index={d} target={d} top1={d} top2={d} margin={d:.6}\n",
            .{ prefix, index, target_token, top1_token, top2_token, margin },
        );
    }
}

const DebugTopKEntry = struct {
    token: usize = 0,
    score: f32 = -std.math.inf(f32),
};

fn fillDebugTopK(logits: []const f32, entries: []DebugTopKEntry) usize {
    if (entries.len == 0 or logits.len == 0) return 0;
    const count = @min(entries.len, logits.len);
    for (entries[0..count]) |*entry| entry.* = .{};
    for (logits, 0..) |score, token| {
        var insert_at: usize = count;
        for (entries[0..count], 0..) |entry, idx| {
            if (score > entry.score or (score == entry.score and token < entry.token)) {
                insert_at = idx;
                break;
            }
        }
        if (insert_at == count) continue;
        var move_idx = count - 1;
        while (move_idx > insert_at) : (move_idx -= 1) {
            entries[move_idx] = entries[move_idx - 1];
        }
        entries[insert_at] = .{
            .token = token,
            .score = score,
        };
    }
    return count;
}

fn debugTopKRank(entries: []const DebugTopKEntry, token: usize) ?usize {
    for (entries, 0..) |entry, idx| {
        if (entry.token == token) return idx + 1;
    }
    return null;
}

fn debugTopKMargin(entries: []const DebugTopKEntry) f32 {
    if (entries.len < 2) return std.math.inf(f32);
    return entries[0].score - entries[1].score;
}

fn isLikelyMtpFormatOrControlToken(token: usize) bool {
    return token <= 255;
}

fn debugPrintTopK(label: []const u8, entries: []const DebugTopKEntry) void {
    std.debug.print(" {s}_topk=", .{label});
    for (entries, 0..) |entry, idx| {
        if (idx > 0) std.debug.print(",", .{});
        std.debug.print("{d}:{d:.4}", .{ entry.token, entry.score });
    }
}

fn debugPrintOptionalRank(label: []const u8, rank: ?usize) void {
    std.debug.print(" {s}=", .{label});
    if (rank) |value| {
        std.debug.print("{d}", .{value});
    } else {
        std.debug.print("none", .{});
    }
}

const MtpParityTrace = struct {
    top_k: usize,
    assistant_logits: []const ?[]f32,
    assistant_total_sequence_lens: []const usize,
    source_tokens: []const i64,
    logit_sources: []const gemma4_mtp.DraftLogitSource,
    hidden_source: Gemma4MtpTargetHiddenSource,
    concat_order: gemma4_mtp.ConcatOrder,
    kv_donor_mode: gemma4_mtp.KvDonorMode,
};

fn debugGemma4MtpParityTrace(
    trace: MtpParityTrace,
    index: usize,
    target_logits: []const f32,
    draft_token: i64,
    target_token: usize,
) void {
    if (trace.top_k == 0 or index >= trace.assistant_logits.len) return;
    const assistant_logits = trace.assistant_logits[index] orelse return;
    const top_k = @min(trace.top_k, @min(assistant_logits.len, target_logits.len));
    if (top_k == 0) return;
    var assistant_entries_buf: [16]DebugTopKEntry = undefined;
    var target_entries_buf: [16]DebugTopKEntry = undefined;
    const assistant_count = fillDebugTopK(assistant_logits, assistant_entries_buf[0..top_k]);
    const target_count = fillDebugTopK(target_logits, target_entries_buf[0..top_k]);
    const assistant_entries = assistant_entries_buf[0..assistant_count];
    const target_entries = target_entries_buf[0..target_count];
    const target_in_assistant_rank = debugTopKRank(assistant_entries, target_token);
    const draft_in_target_rank = if (draft_token >= 0)
        debugTopKRank(target_entries, @intCast(draft_token))
    else
        null;
    const assistant_total = if (index < trace.assistant_total_sequence_lens.len) trace.assistant_total_sequence_lens[index] else 0;
    const assistant_position = if (assistant_total > 0) assistant_total - 1 else 0;
    const source_token = if (index < trace.source_tokens.len) trace.source_tokens[index] else -1;
    const source_name = if (index < trace.logit_sources.len) trace.logit_sources[index].name() else "unknown";

    std.debug.print(
        "gemma4_mtp_topk: index={d} source={d} assistant_total={d} assistant_position={d} hidden_source={s} concat_order={s} kv_donor_mode={s} source_kind={s} draft={d} target={d}",
        .{ index, source_token, assistant_total, assistant_position, trace.hidden_source.name(), trace.concat_order.name(), trace.kv_donor_mode.name(), source_name, draft_token, target_token },
    );
    debugPrintOptionalRank("target_in_assistant_rank", target_in_assistant_rank);
    debugPrintOptionalRank("draft_in_target_rank", draft_in_target_rank);
    debugPrintTopK("assistant", assistant_entries);
    debugPrintTopK("target", target_entries);
    std.debug.print("\n", .{});
}

const mtp_near_tie_margin_threshold: f32 = 1.5;

fn classifyGemma4MtpMismatchWithoutTargetLogits(
    draft_token: i64,
    target_token: usize,
) MtpQualityStats {
    var stats = MtpQualityStats{ .mismatches = 1 };
    if (isLikelyMtpFormatOrControlToken(target_token) or
        (draft_token >= 0 and isLikelyMtpFormatOrControlToken(@intCast(draft_token))))
    {
        stats.format_or_control_misses = 1;
    }
    return stats;
}

fn classifyGemma4MtpMismatch(
    mtp_parity_trace: ?MtpParityTrace,
    index: usize,
    target_logits: []const f32,
    draft_token: i64,
    target_token: usize,
) MtpQualityStats {
    var stats = MtpQualityStats{ .mismatches = 1 };
    if (isLikelyMtpFormatOrControlToken(target_token) or
        (draft_token >= 0 and isLikelyMtpFormatOrControlToken(@intCast(draft_token))))
    {
        stats.format_or_control_misses = 1;
    }

    const trace = mtp_parity_trace orelse return stats;
    if (index >= trace.assistant_logits.len) return stats;
    const assistant_logits = trace.assistant_logits[index] orelse return stats;
    const top_k = @min(@as(usize, 8), @min(assistant_logits.len, target_logits.len));
    if (top_k == 0) return stats;

    var assistant_entries_buf: [8]DebugTopKEntry = undefined;
    var target_entries_buf: [8]DebugTopKEntry = undefined;
    const assistant_count = fillDebugTopK(assistant_logits, assistant_entries_buf[0..top_k]);
    const target_count = fillDebugTopK(target_logits, target_entries_buf[0..top_k]);
    const assistant_entries = assistant_entries_buf[0..assistant_count];
    const target_entries = target_entries_buf[0..target_count];
    const target_in_assistant_rank = debugTopKRank(assistant_entries, target_token);
    const draft_in_target_rank = if (draft_token >= 0)
        debugTopKRank(target_entries, @intCast(draft_token))
    else
        null;

    stats.mismatches_with_assistant_logits = 1;
    if (target_in_assistant_rank) |rank| {
        if (rank <= 2) stats.target_in_assistant_top2 = 1;
        if (rank <= 4) stats.target_in_assistant_top4 = 1;
        if (rank <= 8) stats.target_in_assistant_top8 = 1;
    }
    if (draft_in_target_rank) |rank| {
        if (rank <= 2) stats.draft_in_target_top2 = 1;
    }

    const assistant_margin = debugTopKMargin(assistant_entries);
    const target_margin = debugTopKMargin(target_entries);
    var assistant_target_margin = assistant_margin;
    if (target_token < assistant_logits.len and assistant_entries.len > 0) {
        assistant_target_margin = assistant_entries[0].score - assistant_logits[target_token];
        if (assistant_target_margin < 0) assistant_target_margin = 0;
        stats.assistant_target_margin_sum = assistant_target_margin;
    }

    const is_near_tie =
        (target_in_assistant_rank != null and target_in_assistant_rank.? <= 2) or
        (draft_in_target_rank != null and draft_in_target_rank.? <= 2) or
        assistant_margin <= mtp_near_tie_margin_threshold or
        target_margin <= mtp_near_tie_margin_threshold or
        assistant_target_margin <= mtp_near_tie_margin_threshold;
    if (is_near_tie) {
        stats.near_tie_misses = 1;
    } else {
        stats.confident_misses = 1;
    }
    return stats;
}

fn gemma4MtpMinAcceptancePermille() usize {
    const configured = platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_MIN_ACCEPTANCE_PERMILLE") orelse 200;
    return @min(configured, 1000);
}

fn gemma4MtpAcceptanceProbeDrafts() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_ACCEPTANCE_PROBE_DRAFTS") orelse 64;
}

fn gemma4MtpAdaptiveKEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ADAPTIVE_K", true);
}

fn gemma4MtpProbeK() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_PROBE_K") orelse 2;
}

fn gemma4MtpAutoMaxK() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_AUTO_MAX_K") orelse 2;
}

fn gemma4MtpAutoCostProbeRounds() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_AUTO_COST_PROBE_ROUNDS") orelse 16;
}

fn gemma4MtpAutoMinAcceptedPerRoundMilli() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_AUTO_MIN_ACCEPTED_PER_ROUND_MILLI") orelse 2000;
}

fn gemma4MtpHiddenOnlyMaterializeEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_HIDDEN_ONLY_MATERIALIZE", true);
}

fn gemma4MtpActiveMaterializeEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ACTIVE_MATERIALIZE", true);
}

fn gemma4MtpActiveMaterializeSupported(
    kind: ops.BackendKind,
    config: gpt_mod.Config,
    total_seq_len: usize,
) bool {
    // Without a whole-model generated route, the Metal active frame can hand
    // a mixed SWA/global model back to generic attention after device-only KV
    // has been written. Past the local window that fallback cannot reconstruct
    // the complete KV span. Use the existing hidden-only materialization path,
    // which owns the full one-token forward, for long-context MTP instead.
    return !(kind == .metal and
        config.supportsSplitSwaGlobalKvRing() and
        total_seq_len > @as(usize, config.sliding_window));
}

fn gemma4MtpAcceptBonusDefault(kind: ops.BackendKind) bool {
    return kind != .metal;
}

fn gemma4MtpAcceptBonusEnabled(kind: ops.BackendKind) bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ACCEPT_BONUS", gemma4MtpAcceptBonusDefault(kind));
}

fn gemma4MtpCachedFirstChoiceAllowed(actual_k: usize, canonical_materialize_available: bool) bool {
    // A cached choice is only safe when an accepted single draft can advance
    // through the canonical one-row target frame. Otherwise a short final
    // round can fall back to the older verifier and drift from target-only.
    return actual_k > 0 and canonical_materialize_available;
}

fn shouldAcceptSpeculativeBonus(enabled: bool, draft_count: usize, remaining_generation_tokens: usize) bool {
    return enabled and draft_count < remaining_generation_tokens;
}

fn shouldEnterSpeculativeDecodeLoop(callback_allows_generation: bool, first_is_eos: bool, grammar_complete: bool) bool {
    return callback_allows_generation and !first_is_eos and !grammar_complete;
}

fn gemma4MtpDedicatedRuntimeEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_DEDICATED_RUNTIME", true);
}

fn gemma4MtpVerifyDeviceResultEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT", false);
}

fn gemma4MtpMaterializeReplayEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_MATERIALIZE_REPLAY", false);
}

/// Fold the correction/bonus materialize forward into the next verify: the
/// committed token stays pending (KV unwritten) and the next round's verify
/// prepends it as row 0, whose suffix write materializes the KV.
fn gemma4MtpDeferMaterializeEnabled(kind: ops.BackendKind) bool {
    _ = kind;
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE", false);
}

/// Reuse the target verify row as the next draft activation after a folded
/// correction/bonus instead of the assistant chain projection.
fn gemma4MtpDeferMaterializeTargetActivationEnabled(kind: ops.BackendKind) bool {
    _ = kind;
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE_TARGET_ACTIVATION", false);
}

fn gemma4MtpDeferredTargetActivationRow(cached_first: bool, logit_base_row: usize, accepted: usize) ?usize {
    if (cached_first) return if (accepted >= 2) accepted - 2 else null;
    return if (accepted != 0) logit_base_row + accepted - 1 else null;
}

fn gemma4MtpDraftEmbeddingCacheSize() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_DRAFT_EMBED_CACHE") orelse 256;
}

fn gemma4MtpResidentDraftBackendAllowed(kind: ops.BackendKind) bool {
    return kind == .cuda or kind == .metal;
}

pub fn gemma4MtpMetalAutoEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO", false);
}

fn gemma4MtpMetalPrefillHiddenCaptureEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ENABLE_METAL_PREFILL_HIDDEN_CAPTURE", false) and
        !platform.env.getenvBool("ANTFLY_GEMMA4_MTP_DISABLE_METAL_PREFILL_HIDDEN_CAPTURE");
}

fn gemma4MetalDirectGreedyDefault() bool {
    return true;
}

fn pipelinedMetalDecodeEnabled() bool {
    return metal_runtime.pipelinedDecodeFrameEnabled();
}

fn gemma4MetalDirectGreedyEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_METAL_DIRECT_GREEDY", gemma4MetalDirectGreedyDefault());
}

pub fn gemma4MtpAutoMinGenerationTokens() usize {
    return platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS") orelse 128;
}

fn gemma4MtpUnsafeTargetReplayEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY", false);
}

pub fn gemma4MtpTargetReplayLikelyActive() bool {
    if (!gemma4MtpUnsafeTargetReplayEnabled()) return false;
    const raw = platform.env.getenv("ANTFLY_GEMMA4_MTP_TARGET_REPLAY") orelse "auto";
    if (std.mem.eql(u8, raw, "off") or std.mem.eql(u8, raw, "0") or std.mem.eql(u8, raw, "false")) return false;
    if (platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD")) |period| {
        if (period > 0) return true;
    }
    return false;
}

fn gemma4MtpGraphReplayStatusName() []const u8 {
    const raw = platform.env.getenv("ANTFLY_GEMMA4_MTP_TARGET_REPLAY") orelse "auto";
    if (std.mem.eql(u8, raw, "off") or std.mem.eql(u8, raw, "0") or std.mem.eql(u8, raw, "false")) return "off";
    if (!gemma4MtpUnsafeTargetReplayEnabled()) return "disabled_unsafe";
    if (std.mem.eql(u8, raw, "force") or std.mem.eql(u8, raw, "1") or std.mem.eql(u8, raw, "true")) return "forced";
    return "auto";
}

fn gemma4MtpEffectiveZeroMatchFallbackRounds(configured_rounds: usize, adaptive_k_enabled: bool) usize {
    if (!adaptive_k_enabled or configured_rounds == 0) return configured_rounds;
    return @max(configured_rounds, @as(usize, 2));
}

const MtpAdaptiveKDecision = enum {
    keep,
    ramped,
    fallback,
};

const MtpAdaptiveKPolicy = struct {
    enabled: bool,
    current_k: usize,
    max_k: usize,
    min_acceptance_permille: usize,
    acceptance_window_drafts: usize,
    window_drafted: usize = 0,
    window_matched: usize = 0,

    fn init(max_k_raw: usize, min_acceptance_permille: usize, acceptance_window_drafts: usize) MtpAdaptiveKPolicy {
        return initFromValues(
            max_k_raw,
            min_acceptance_permille,
            acceptance_window_drafts,
            gemma4MtpAdaptiveKEnabled(),
            gemma4MtpProbeK(),
        );
    }

    fn initFromValues(
        max_k_raw: usize,
        min_acceptance_permille: usize,
        acceptance_window_drafts: usize,
        enabled: bool,
        probe_k_raw: usize,
    ) MtpAdaptiveKPolicy {
        const max_k = std.math.clamp(max_k_raw, @as(usize, 1), @as(usize, 16));
        const probe_k = std.math.clamp(if (probe_k_raw == 0) @as(usize, 1) else probe_k_raw, @as(usize, 1), max_k);
        const policy_enabled = enabled and max_k > 1 and acceptance_window_drafts > 0;
        return .{
            .enabled = policy_enabled,
            .current_k = if (policy_enabled) probe_k else max_k,
            .max_k = max_k,
            .min_acceptance_permille = @min(min_acceptance_permille, 1000),
            .acceptance_window_drafts = acceptance_window_drafts,
        };
    }

    fn nextK(self: MtpAdaptiveKPolicy, remaining: usize) usize {
        return @min(self.current_k, remaining);
    }

    fn observe(self: *MtpAdaptiveKPolicy, drafted: usize, matched: usize) MtpAdaptiveKDecision {
        if (!self.enabled or drafted == 0) return .keep;
        self.window_drafted += drafted;
        self.window_matched += matched;
        if (self.acceptance_window_drafts == 0 or self.window_drafted < self.acceptance_window_drafts) return .keep;

        const acceptance_permille = (self.window_matched * 1000) / self.window_drafted;
        if (acceptance_permille < self.min_acceptance_permille) return .fallback;

        self.window_drafted = 0;
        self.window_matched = 0;
        if (self.current_k >= self.max_k) return .keep;
        self.current_k = @min(self.max_k, self.current_k * 2);
        return .ramped;
    }
};

fn gemma4MtpProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_GEMMA4_MTP_PROFILE");
}

fn gemma4MtpProfileSyncEnabled() bool {
    return platform.env.getenvBool("ANTFLY_GEMMA4_MTP_PROFILE_SYNC");
}

fn mtpProfileTimestamp(enabled: bool, io_opt: ?std.Io) std.Io.Timestamp {
    if (!enabled) return std.Io.Timestamp.zero;
    return if (io_opt) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
}

fn mtpProfileElapsedNs(enabled: bool, io_opt: ?std.Io, started_at: std.Io.Timestamp) u64 {
    if (!enabled) return 0;
    if (std.meta.eql(started_at, std.Io.Timestamp.zero)) return 0;
    const finished_at = if (io_opt) |io| std.Io.Timestamp.now(io, .awake) else return 0;
    const elapsed = started_at.durationTo(finished_at).nanoseconds;
    if (elapsed <= 0) return 0;
    return @intCast(elapsed);
}

fn accumulateMtpDraftStageProfile(profile: *MtpProfileStats, stage: gemma4_mtp.DraftStageProfile) void {
    if (!profile.enabled) return;
    profile.draft_target_embedding_ns +|= stage.target_embedding_ns;
    profile.draft_concat_ns +|= stage.concat_ns;
    profile.draft_preprojection_ns +|= stage.preprojection_ns;
    profile.draft_assistant_ns +|= stage.assistant_ns;
    profile.draft_postprojection_ns +|= stage.postprojection_ns;
    profile.draft_argmax_ns +|= stage.argmax_ns;
    profile.draft_lm_head_ns +|= stage.lm_head_ns;
    profile.draft_selection_ns +|= stage.selection_ns;
    profile.draft_target_embedding_cross_copies +|= stage.target_embedding_cross_copies;
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
}

fn isPureGreedyConfig(config: GenerationConfig) bool {
    return config.temperature <= 0 and !hasSamplingPenalties(config);
}

fn requiresCudaDecodeGraphBeforeEagerFallback(
    backend_kind: ops.BackendKind,
    graph_replay_required: bool,
    pure_greedy: bool,
    query_seq_len: usize,
    has_external_graph_runtime: bool,
    has_grammar: bool,
) bool {
    return graph_replay_required and
        backend_kind == .cuda and
        pure_greedy and
        query_seq_len == 1 and
        !has_external_graph_runtime and
        !has_grammar;
}

fn hasSamplingPenalties(config: GenerationConfig) bool {
    return config.repetition_penalty != 1.0 or
        config.frequency_penalty != 0 or
        config.presence_penalty != 0;
}

/// The only materialization a prefill chunk is allowed to produce. Keeping
/// this separate from chunk position prevents an intermediate chunk from
/// accidentally invoking the vocabulary projection just because its output
/// is represented by the same forward API as the final chunk.
const PrefillOutputIntent = enum {
    kv_only,
    last_logits,
    greedy_token,
    last_logits_and_hidden,
    greedy_token_and_hidden,
};

fn prefillOutputIntent(is_final_chunk: bool, use_greedy_token: bool, capture_last_hidden: bool) PrefillOutputIntent {
    if (!is_final_chunk) return .kv_only;
    if (use_greedy_token) return if (capture_last_hidden) .greedy_token_and_hidden else .greedy_token;
    return if (capture_last_hidden) .last_logits_and_hidden else .last_logits;
}

fn shouldUsePrefillFirstTokenPath(backend_kind: ops.BackendKind, pure_greedy: bool, use_speculative: bool, enabled: bool) bool {
    return enabled and backend_kind == .cuda and pure_greedy and !use_speculative;
}

fn shouldUseCudaPrefillGreedyToken(
    backend_kind: ops.BackendKind,
    config: GenerationConfig,
    allow_resident_greedy_token: bool,
    has_suppress_token_ids: bool,
    enabled: bool,
) bool {
    _ = has_suppress_token_ids;
    return enabled and
        allow_resident_greedy_token and
        backend_kind == .cuda and
        isPureGreedyConfig(config) and
        config.grammar == null;
}

fn directPrefillExecutionMutex(
    prepared_multimodal: bool,
    use_cuda_prefill_greedy_token: bool,
    use_metal_prefill_greedy_token: bool,
    execution_lock: ?*std.atomic.Mutex,
) ?*std.atomic.Mutex {
    return if (prepared_multimodal or use_cuda_prefill_greedy_token or use_metal_prefill_greedy_token) execution_lock else null;
}

fn prefillFinalChunkNeedsDirectExecution(
    total_chunk_end: usize,
    seq_len: usize,
    output_intent: PrefillOutputIntent,
    prefer_cuda_direct_last_logits: bool,
    use_metal_target_last_logits: bool,
) bool {
    return total_chunk_end == seq_len and
        switch (output_intent) {
            .kv_only => false,
            .greedy_token, .greedy_token_and_hidden, .last_logits_and_hidden => true,
            .last_logits => prefer_cuda_direct_last_logits or use_metal_target_last_logits,
        };
}

fn shouldUseMetalGemmaTargetLastLogits(
    kind: ops.BackendKind,
    config: *const gpt_mod.Config,
    capture_last_hidden: bool,
) bool {
    return !capture_last_hidden and
        kind == .metal and
        config.family == .gemma and
        config.hasPle() and
        !config.gemma4_mtp_assistant;
}

fn singletonScheduledGemmaPrefillFrameEnabled() bool {
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_SINGLETON_SCHEDULED_PREFILL_FRAME");
}

fn shouldUseSingletonScheduledGemmaPrefillFrame(
    kind: ops.BackendKind,
    config: *const gpt_mod.Config,
    item_count: usize,
    phase: runtime.scheduler.native_generate.Phase,
    query_sequence_len: usize,
    wants_logits: bool,
) bool {
    return kind == .metal and
        config.family == .gemma and
        !config.usesMoe() and
        config.hasPle() and
        item_count == 1 and
        phase == .prefill and
        query_sequence_len > 1 and
        !wants_logits;
}

/// A scheduled prefill chunk may be followed by a generic final-hidden/logits
/// forward or by another request. Keep the direct runtime's gathered KV span
/// strictly inside this attempt; paged KV storage remains the handoff boundary.
fn runWithDecoderRuntimeResetBoundary(
    cb: *const ComputeBackend,
    context: *anyopaque,
    run: *const fn (context: *anyopaque) anyerror!bool,
) !bool {
    try cb.decoderRuntimeResetState();
    const result = run(context) catch |err| {
        cb.decoderRuntimeResetState() catch {};
        return err;
    };
    try cb.decoderRuntimeResetState();
    return result;
}

fn batchKvMutationMutex(
    execution_lock: ?*std.atomic.Mutex,
    kv_lock: ?*std.atomic.Mutex,
) ?*std.atomic.Mutex {
    return if (kv_lock != null) execution_lock else null;
}

fn coalescedPrefillChunkSizeForFirstToken(
    backend_kind: ops.BackendKind,
    pure_greedy: bool,
    use_speculative: bool,
    enabled: bool,
    seq_len: usize,
    current_chunk_size: usize,
    coalesce_token_limit: usize,
) usize {
    if (!shouldUsePrefillFirstTokenPath(backend_kind, pure_greedy, use_speculative, enabled)) return current_chunk_size;
    if (coalesce_token_limit == 0 or seq_len > coalesce_token_limit) return current_chunk_size;
    return @max(current_chunk_size, seq_len);
}

fn schedulerChunkForPrefillIteration(scheduler_chunk: usize, current_chunk_size: usize, first_token_coalesced: bool) usize {
    if (first_token_coalesced) return current_chunk_size;
    if (scheduler_chunk > 0) return scheduler_chunk;
    return current_chunk_size;
}

fn wholeModelPrefillChunkSize(configured_chunk: usize, scheduler_chunk: usize, prompt_tokens: usize) usize {
    const requested = if (configured_chunk > 0)
        configured_chunk
    else
        runtime.tier.memory.generation_default_prefill_chunk_tokens;
    return @max(@min(@min(requested, if (scheduler_chunk > 0) scheduler_chunk else requested), prompt_tokens), 1);
}

fn validateSpeculativeK(draft_requested: bool, speculative_k: u32) !void {
    if (draft_requested and
        (speculative_k == 0 or speculative_k > runtime.tier.memory.generation_max_speculative_k))
    {
        return error.InvalidSpeculativeK;
    }
}

pub const DecoderRuntimeDebugStats = struct {
    forward_attempts: u64 = 0,
    flag_disabled: u64 = 0,
    backend_not_device_decode: u64 = 0,
    scheduler_blocked: u64 = 0,
    graph_blocked: u64 = 0,
    first_token_blocked: u64 = 0,
    kv_missing: u64 = 0,
    non_greedy: u64 = 0,
    grammar_blocked: u64 = 0,
    device_token_handoff_attempts: u64 = 0,
    device_token_handoff_hits: u64 = 0,
    device_token_handoff_fallbacks: u64 = 0,
    device_token_handoff_seeds: u64 = 0,
    prepare_attempts: u64 = 0,
    prepare_flag_disabled: u64 = 0,
    prepare_backend_not_device_decode: u64 = 0,
    prepare_kv_missing: u64 = 0,
    prepare_scheduler_blocked: u64 = 0,
    prepare_graph_blocked: u64 = 0,
    prepare_arch_blocked: u64 = 0,
    prepare_model_blocked: u64 = 0,
    prepare_calls: u64 = 0,
    input_attempts: u64 = 0,
    input_flag_disabled: u64 = 0,
    input_backend_not_device_decode: u64 = 0,
    input_kv_missing: u64 = 0,
    input_arch_blocked: u64 = 0,
    input_model_blocked: u64 = 0,
    input_seq_empty: u64 = 0,
    input_successes: u64 = 0,
    replay_last_logits_fallbacks: u64 = 0,
};

var decoder_runtime_debug_stats = DecoderRuntimeDebugStats{};
const decoder_runtime_layer_count: usize = 2;

pub fn resetDecoderRuntimeDebugStats() void {
    decoder_runtime_debug_stats = .{};
}

pub fn getDecoderRuntimeDebugStats() DecoderRuntimeDebugStats {
    return decoder_runtime_debug_stats;
}

pub const NativeDecodeState = struct {
    allocator: std.mem.Allocator,
    kv_manager: ?*runtime.kv.manager.KvManager = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
    /// Optional external lock for shared paged KV metadata/storage. Batch
    /// generation wires every decode state in a group to the same manager and
    /// storage runtime, so admission reads and KV mutations must share this
    /// lock even though forward execution is serialized by the scheduler turn.
    kv_lock: ?*std.atomic.Mutex = null,
    sequence_id: ?runtime.kv.manager.SequenceId = null,
    pool_id: ?runtime.kv.block.KvPoolId = null,
    total_tokens: usize = 0,
    kv_view: ?KvView = null,
    kv_compacted: bool = false,
    kv_block_ids: std.ArrayListUnmanaged(runtime.kv.block.KvBlockId) = .empty,
    moe_runtime: runtime.moe.runtime.MoeRuntime,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    qwen35_linear_cache: ?gpt_arch.Qwen35LinearCache = null,
    gemma4_layer_spec_cache: gpt_arch.Gemma4LayerSpecCache = .{},
    deepseek_v4_compressed_cache: ?gpt_arch.DeepSeekV4CompressedCache = null,
    force_full_recompute: bool = false,
    kv_max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
    qwen3vl_mrope_position_delta: ?i64 = null,
    qwen3vl_decode_positions: [3]u32 = .{ 0, 0, 0 },
    qwen3vl_text_only: bool = false,

    pub fn initContiguous(allocator: std.mem.Allocator) NativeDecodeState {
        return .{
            .allocator = allocator,
            .moe_runtime = runtime.moe.runtime.MoeRuntime.init(allocator, null),
        };
    }

    pub fn initPaged(
        allocator: std.mem.Allocator,
        kv_manager: *runtime.kv.manager.KvManager,
        pool_id: runtime.kv.block.KvPoolId,
        shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
    ) NativeDecodeState {
        return .{
            .allocator = allocator,
            .kv_manager = kv_manager,
            .pool_id = pool_id,
            .moe_runtime = runtime.moe.runtime.MoeRuntime.init(allocator, shared_moe_cache),
            .shared_moe_cache = shared_moe_cache,
        };
    }

    fn lockPagedKv(self: *const NativeDecodeState) ?*std.atomic.Mutex {
        const mutex = self.kv_lock orelse return null;
        platform.sync.lockYielding(mutex);
        return mutex;
    }

    pub fn isPaged(self: *const NativeDecodeState) bool {
        return self.kv_manager != null and !self.force_full_recompute;
    }

    pub fn isKvCompacted(self: *const NativeDecodeState) bool {
        return self.kv_compacted;
    }

    pub fn configureForGptConfig(self: *NativeDecodeState, config: gpt_mod.Config) void {
        self.force_full_recompute = false;
        if (self.total_tokens == 0) self.kv_max_inflight_tokens = 0;
        self.allow_swa_ring = config.supportsSplitSwaGlobalKvRing();
        if (config.family != .gemma) self.gemma4_layer_spec_cache.reset();
        if (!requiresDeepSeekV4CompressedCache(config)) self.clearDeepSeekV4CompressedCache();
        self.qwen3vl_mrope_position_delta = if (config.family == .qwen3_vl) 0 else null;
        self.qwen3vl_text_only = config.family == .qwen3_vl;
    }

    fn configureKvMaxInflightTokens(self: *NativeDecodeState, token_count: usize, request_allows_ring: bool) void {
        if (token_count == 0 or self.total_tokens != 0) {
            self.allow_swa_ring = false;
            self.kv_max_inflight_tokens = 0;
            if (self.kv_view) |*view| {
                view.max_inflight_tokens = 0;
                view.allow_swa_ring = false;
            }
            return;
        }
        // Ring page ids are absolute from sequence position zero. A pool that
        // front-trims its logical block table would make them view-relative.
        self.allow_swa_ring = self.allow_swa_ring and request_allows_ring and self.kvSlidingWindowTokens() == null;
        self.kv_max_inflight_tokens = if (self.allow_swa_ring) token_count else 0;
        if (self.kv_view) |*view| {
            view.max_inflight_tokens = self.kv_max_inflight_tokens;
            view.allow_swa_ring = self.allow_swa_ring;
        }
    }

    pub fn requiresFullRecompute(self: *const NativeDecodeState) bool {
        return self.force_full_recompute;
    }

    pub fn requiresDeepSeekV4CompressedCache(config: gpt_mod.Config) bool {
        if (config.family != .deepseek_v4) return false;
        const schedule_len = @min(@as(usize, @intCast(config.deepseek_v4_attention_schedule_len)), config.deepseek_v4_attention_schedule.len);
        if (schedule_len > 0) {
            for (config.deepseek_v4_attention_schedule[0..schedule_len]) |kind| {
                switch (kind) {
                    .compressed_sparse_attention, .heavily_compressed_attention => return true,
                    else => {},
                }
            }
            return false;
        }
        return config.deepseek_v4_compressed_sparse_attention_layers > 0 or
            config.deepseek_v4_heavily_compressed_attention_layers > 0;
    }

    pub fn ensureDeepSeekV4CompressedCache(self: *NativeDecodeState, config: gpt_mod.Config) !void {
        if (!requiresDeepSeekV4CompressedCache(config)) return;
        if (self.deepseek_v4_compressed_cache == null) {
            self.deepseek_v4_compressed_cache = try gpt_arch.DeepSeekV4CompressedCache.init(self.allocator, @intCast(config.num_hidden_layers));
        }
    }

    pub fn resetDeepSeekV4CompressedCache(self: *NativeDecodeState) void {
        if (self.deepseek_v4_compressed_cache) |*cache| cache.reset();
    }

    pub fn clearDeepSeekV4CompressedCache(self: *NativeDecodeState) void {
        if (self.deepseek_v4_compressed_cache) |*cache| cache.deinit();
        self.deepseek_v4_compressed_cache = null;
    }

    fn ensureAttachedUnlocked(self: *NativeDecodeState) !void {
        if (!self.isPaged()) return;
        if (self.sequence_id != null) return;
        self.sequence_id = try self.kv_manager.?.attachSequence(self.pool_id orelse return error.InvalidPoolId);
        if (self.kv_storage) |storage| {
            const storage_sequence_id = try storage.attachSequence(storage.poolId());
            if (storage_sequence_id != self.sequence_id.?) return error.InvalidPagedKvState;
        }
    }

    pub fn seedAttachedPrefix(self: *NativeDecodeState, sequence_id: runtime.kv.manager.SequenceId, token_count: usize) !void {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        if (!self.isPaged()) return error.InvalidPagedKvState;
        if (self.sequence_id != null) return error.InvalidPagedKvState;
        if (self.kv_storage) |storage| {
            if ((storage.tokenCount(sequence_id) orelse return error.InvalidPagedKvState) != token_count) return error.InvalidPagedKvState;
        }
        self.sequence_id = sequence_id;
        self.total_tokens = token_count;
        self.kv_compacted = false;
        self.syncPagedKvViewForPrefill();
    }

    pub fn ensureAttached(self: *NativeDecodeState) !void {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        return self.ensureAttachedUnlocked();
    }

    fn kvPageSizeTokens(self: *const NativeDecodeState) ?usize {
        const manager = self.kv_manager orelse return null;
        const pool_id = self.pool_id orelse return null;
        const pool = manager.getPool(pool_id) orelse return null;
        return pool.config.page_size_tokens;
    }

    fn kvSlidingWindowTokens(self: *const NativeDecodeState) ?usize {
        const manager = self.kv_manager orelse return null;
        const pool_id = self.pool_id orelse return null;
        const pool = manager.getPool(pool_id) orelse return null;
        return if (pool.config.sliding_window_size) |size| @intCast(size) else null;
    }

    fn cudaPagedKvReplayCapacityTokens(self: *const NativeDecodeState) ?usize {
        const storage = self.kv_storage orelse return null;
        switch (storage.storage.config.dtype) {
            .f16, .polar4, .turbo3 => {},
            else => return null,
        }
        const forced = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") orelse return null;
        return if (forced == 0) null else forced;
    }

    fn reservePagedKvReplayCapacity(self: *NativeDecodeState) !void {
        const token_capacity = self.cudaPagedKvReplayCapacityTokens() orelse return;
        const sequence_id = self.sequence_id orelse return;
        const manager = self.kv_manager orelse return;
        try manager.reserveTokenCapacity(sequence_id, token_capacity);
        if (self.kv_storage) |storage| try storage.reserveTokenCapacity(sequence_id, token_capacity);
    }

    fn setPagedKvView(self: *NativeDecodeState, token_count: usize, position_offset: usize) void {
        const sequence_id = self.sequence_id orelse {
            self.kv_view = null;
            return;
        };
        const pool_id = self.pool_id orelse {
            self.kv_view = null;
            return;
        };
        if (token_count == 0) {
            self.kv_view = null;
            return;
        }
        const page_size = self.kvPageSizeTokens() orelse {
            self.kv_view = null;
            return;
        };
        const logical_block_count = (token_count + page_size - 1) / page_size;
        const rem = token_count % page_size;
        const tail_tokens: u16 = @intCast(if (rem == 0) page_size else rem);
        self.kv_view = .{
            .sequence_id = sequence_id,
            .pool_id = pool_id,
            .logical_block_count = logical_block_count,
            .tail_tokens = tail_tokens,
            .token_count = token_count,
            .position_offset = position_offset,
            .max_inflight_tokens = self.kv_max_inflight_tokens,
            .allow_swa_ring = self.allow_swa_ring,
            .logical_blocks = self.kv_block_ids.items,
            .kv_storage = self.kv_storage,
        };
    }

    fn syncPagedKvBlockTable(self: *NativeDecodeState) !void {
        const manager = self.kv_manager orelse {
            self.kv_block_ids.clearRetainingCapacity();
            return;
        };
        const sequence_id = self.sequence_id orelse {
            self.kv_block_ids.clearRetainingCapacity();
            return;
        };
        _ = try manager.logicalBlocksWithReservations(sequence_id, &self.kv_block_ids);
    }

    fn syncPagedKvViewForPrefill(self: *NativeDecodeState) void {
        if (!self.isPaged()) {
            self.kv_view = null;
            return;
        }
        self.syncPagedKvBlockTable() catch {};
        self.setPagedKvView(self.total_tokens, 0);
    }

    fn syncPagedKvViewForDecode(self: *NativeDecodeState) void {
        if (!self.isPaged()) {
            self.kv_view = null;
            return;
        }
        self.syncPagedKvBlockTable() catch {};
        if (self.kv_compacted) {
            const manager = self.kv_manager orelse return;
            const sequence_id = self.sequence_id orelse return;
            const current_kv_tokens = @min(manager.tokenCount(sequence_id) orelse 0, self.total_tokens);
            self.setPagedKvView(current_kv_tokens, self.total_tokens - current_kv_tokens);
            return;
        }
        // The sliding-window trim drops whole KV pages, so the view must
        // track the tokens actually retained (block-aligned), not the exact
        // window size. Kernels index the compacted block table with
        // view-relative positions: a token-granular offset here would
        // misalign every physical KV read and write by (offset % page_size).
        // The attention kernels apply the exact sliding window themselves.
        const retained_tokens = blk: {
            if (self.kvSlidingWindowTokens() == null) break :blk self.total_tokens;
            const manager = self.kv_manager orelse break :blk self.total_tokens;
            const sequence_id = self.sequence_id orelse break :blk self.total_tokens;
            break :blk manager.tokenCount(sequence_id) orelse self.total_tokens;
        };
        const kv_tokens = @min(self.total_tokens, retained_tokens);
        self.setPagedKvView(kv_tokens, self.total_tokens - kv_tokens);
    }

    fn makeKvMutationResult(self: *const NativeDecodeState, before_view: ?KvView, before_sequence_id: ?runtime.kv.manager.SequenceId, retained_tokens: ?usize) KvMutationResult {
        const after_view = self.kvView();
        return .{
            .token_count = self.total_tokens,
            .kv_view = after_view,
            .compacted = self.kv_compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (after_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = if (after_view) |view| view.position_offset else 0,
                .retained_tokens = retained_tokens,
            },
        };
    }

    pub fn deinit(self: *NativeDecodeState) void {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        if (self.kv_manager) |manager| {
            if (self.sequence_id) |sequence_id| {
                manager.releaseSequence(sequence_id) catch {};
                if (self.kv_storage) |storage| storage.releaseSequence(sequence_id) catch {};
            }
        }
        self.moe_runtime.deinit();
        if (self.qwen35_linear_cache) |*cache| cache.deinit();
        self.gemma4_layer_spec_cache.deinit(self.allocator);
        if (self.deepseek_v4_compressed_cache) |*cache| cache.deinit();
        self.kv_block_ids.deinit(self.allocator);
        self.sequence_id = null;
        self.kv_view = null;
        self.kv_compacted = false;
        self.qwen35_linear_cache = null;
        self.deepseek_v4_compressed_cache = null;
    }

    pub fn ensureQwen35LinearCache(self: *NativeDecodeState, config: gpt_mod.Config) !void {
        if (!config.isQwen35() or !config.qwen35_has_linear_attention) return;
        if (self.qwen35_linear_cache == null) {
            self.qwen35_linear_cache = try gpt_arch.Qwen35LinearCache.init(self.allocator, config);
        }
    }

    pub fn resetQwen35LinearCache(self: *NativeDecodeState) void {
        if (self.qwen35_linear_cache) |*cache| cache.reset();
    }

    pub fn notePrefill(self: *NativeDecodeState, token_count: usize) !void {
        self.total_tokens = token_count;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(token_count));
            if (self.kv_storage) |storage| try storage.appendTokens(self.sequence_id.?, @intCast(token_count));
            try self.reservePagedKvReplayCapacity();
            self.kv_compacted = false;
            self.syncPagedKvViewForPrefill();
        }
    }

    pub fn notePrefillWithResult(self: *NativeDecodeState, token_count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.notePrefill(token_count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    pub fn appendPrefillChunk(self: *NativeDecodeState, token_count: usize) !void {
        self.total_tokens += token_count;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(token_count));
            if (self.kv_storage) |storage| try storage.appendTokens(self.sequence_id.?, @intCast(token_count));
            try self.reservePagedKvReplayCapacity();
            self.kv_compacted = false;
            self.syncPagedKvViewForPrefill();
        }
    }

    pub fn appendPrefillChunkWithResult(self: *NativeDecodeState, token_count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.appendPrefillChunk(token_count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    pub fn appendGeneratedToken(self: *NativeDecodeState) !void {
        self.total_tokens += 1;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, 1);
            _ = try self.kv_manager.?.trimSequenceToSlidingWindow(self.sequence_id.?);
            if (self.kv_storage) |storage| {
                try storage.appendTokens(self.sequence_id.?, 1);
                _ = try storage.trimSequenceToSlidingWindow(self.sequence_id.?);
            }
            try self.reservePagedKvReplayCapacity();
            self.syncPagedKvViewForDecode();
        }
    }

    pub fn appendGeneratedTokenWithResult(self: *NativeDecodeState) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.appendGeneratedToken();
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Append `count` generated tokens at once (used by speculative decoding
    /// after accepting a batch of draft tokens).
    pub fn appendGeneratedTokens(self: *NativeDecodeState, count: usize) !void {
        self.total_tokens += count;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(count));
            _ = try self.kv_manager.?.trimSequenceToSlidingWindow(self.sequence_id.?);
            if (self.kv_storage) |storage| {
                try storage.appendTokens(self.sequence_id.?, @intCast(count));
                _ = try storage.trimSequenceToSlidingWindow(self.sequence_id.?);
            }
            try self.reservePagedKvReplayCapacity();
            if (count > 0) {
                self.syncPagedKvViewForDecode();
            }
        }
    }

    pub fn appendGeneratedTokensWithResult(self: *NativeDecodeState, count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.appendGeneratedTokens(count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Roll back `count` tokens from the KV cache (used by speculative
    /// decoding when draft tokens are rejected).
    pub fn truncateTokens(self: *NativeDecodeState, count: usize) !void {
        if (count == 0) return;
        if (count > self.total_tokens) return error.TruncateBeyondStart;
        const was_paged = self.isPaged();
        self.total_tokens -= count;
        if (was_paged) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            const manager = self.kv_manager.?;
            if (self.sequence_id) |seq_id| {
                const removed = try manager.truncateSequence(seq_id, count);
                if (self.kv_storage) |storage| _ = try storage.truncateSequence(seq_id, count);
                const prior_kv_tokens = if (self.kv_view) |view| view.token_count else 0;
                const kv_tokens = prior_kv_tokens - @min(prior_kv_tokens, removed);
                try self.syncPagedKvBlockTable();
                self.setPagedKvView(kv_tokens, self.total_tokens - kv_tokens);
            }
        }
        if (self.deepseek_v4_compressed_cache) |*cache| {
            cache.reset();
            self.force_full_recompute = true;
        }
    }

    pub fn truncateTokensWithResult(self: *NativeDecodeState, count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.truncateTokens(count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Compact the KV cache using Attention Matching. Replaces the current
    /// sequence with a smaller one containing fitted K/V values that preserve
    /// attention output. Call after prefill, before the decode loop.
    pub fn compactKvCache(self: *NativeDecodeState, config: runtime.kv.compaction.CompactionConfig) !usize {
        try config.validate();
        if (self.deepseek_v4_compressed_cache != null) return error.DeepSeekV4CompressedKvCompactionNotSupported;
        if (self.kv_storage != null) return error.KvStorageCompactionNotSupported;
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        const manager = self.kv_manager orelse return 0;
        const seq_id = self.sequence_id orelse return 0;
        const pool_id = self.pool_id orelse return error.InvalidPoolId;
        const pool = manager.getPool(pool_id) orelse return error.InvalidPoolId;

        var compacted = try runtime.kv.compaction.compactSequence(
            self.allocator,
            manager,
            seq_id,
            pool_id,
            config,
        );
        defer compacted.deinit();

        // Create new sequence and write compacted data.
        const new_seq_id = try manager.attachSequence(pool_id);
        errdefer manager.releaseSequence(new_seq_id) catch {};
        try manager.appendTokens(new_seq_id, @intCast(compacted.retained_count));
        for (0..pool.config.num_layers_packed) |layer| {
            try manager.writeFullLayerKv(
                new_seq_id,
                layer,
                compacted.retained_count,
                compacted.k_per_layer[layer],
                compacted.v_per_layer[layer],
            );
        }

        // Complete every fallible step before releasing the old sequence so
        // a failed compaction leaves the original state intact.
        const new_state = try manager.sequenceMut(new_seq_id);
        new_state.compacted = true;
        var new_block_ids = std.ArrayListUnmanaged(runtime.kv.block.KvBlockId).empty;
        errdefer new_block_ids.deinit(self.allocator);
        _ = try manager.logicalBlocksWithReservations(new_seq_id, &new_block_ids);

        // Swap: release old sequence, adopt new one.
        try manager.releaseSequence(seq_id);
        self.sequence_id = new_seq_id;
        self.kv_block_ids.deinit(self.allocator);
        self.kv_block_ids = new_block_ids;
        new_block_ids = .empty;
        self.kv_compacted = true;
        self.setPagedKvView(compacted.retained_count, self.total_tokens - compacted.retained_count);
        // total_tokens stays the same for position encoding continuity.
        return compacted.retained_count;
    }

    pub fn compactKvCacheWithResult(self: *NativeDecodeState, config: runtime.kv.compaction.CompactionConfig) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        const retained_tokens = try self.compactKvCache(config);
        return self.makeKvMutationResult(before_view, before_sequence_id, retained_tokens);
    }

    pub fn kvView(self: *const NativeDecodeState) ?KvView {
        if (self.force_full_recompute) return null;
        return self.kv_view;
    }

    pub fn gptDecodeContext(self: *NativeDecodeState, seq_len: usize, query_seq_len: usize) gpt_arch.DecodeContext {
        const decode_positions: ?[]const u32 = if (self.qwen3vl_mrope_position_delta != null and query_seq_len == 1 and seq_len > 0) blk: {
            const position = @as(i64, @intCast(seq_len - 1)) + self.qwen3vl_mrope_position_delta.?;
            if (position < 0 or position > std.math.maxInt(u32)) break :blk null;
            @memset(&self.qwen3vl_decode_positions, @intCast(position));
            break :blk &self.qwen3vl_decode_positions;
        } else null;
        if (disablePagedKvDebug() or self.force_full_recompute) {
            return .{
                .attention_mode = .full_recompute,
                .total_sequence_len = seq_len,
                .query_sequence_len = seq_len,
                .kv_sequence_len = seq_len,
                .kv_position_offset = 0,
                .moe_runtime = &self.moe_runtime,
                .qwen35_linear_cache = if (self.qwen35_linear_cache) |*cache| cache else null,
                .gemma4_layer_spec_cache = &self.gemma4_layer_spec_cache,
                .deepseek_v4_compressed_cache = if (self.deepseek_v4_compressed_cache) |*cache| cache else null,
                .mrope_positions = decode_positions,
                .qwen3vl_text_only = self.qwen3vl_text_only,
            };
        }
        return .{
            .attention_mode = if (self.kvView() != null)
                (if (query_seq_len < seq_len and query_seq_len == 1) .paged_decode else .paged_prefill)
            else
                .full_recompute,
            .total_sequence_len = seq_len,
            .query_sequence_len = query_seq_len,
            .kv_sequence_len = if (self.kvView()) |view| view.token_count else seq_len,
            .kv_position_offset = if (self.kvView()) |view| view.position_offset else 0,
            .kv_manager = self.kv_manager,
            .kv_storage = self.kv_storage,
            .moe_runtime = &self.moe_runtime,
            .qwen35_linear_cache = if (self.qwen35_linear_cache) |*cache| cache else null,
            .gemma4_layer_spec_cache = &self.gemma4_layer_spec_cache,
            .deepseek_v4_compressed_cache = if (self.deepseek_v4_compressed_cache) |*cache| cache else null,
            .mrope_positions = decode_positions,
            .qwen3vl_text_only = self.qwen3vl_text_only,
            .kv_cache = if (self.kvView()) |view|
                .{
                    .sequence_id = view.sequence_id,
                    .pool_id = view.pool_id,
                    .logical_block_count = view.logical_block_count,
                    .tail_tokens = view.tail_tokens,
                    .position_offset = view.position_offset,
                    .max_inflight_tokens = view.max_inflight_tokens,
                    .allow_swa_ring = view.allow_swa_ring,
                    .logical_blocks = view.logical_blocks,
                    .kv_storage = view.kv_storage,
                }
            else
                null,
        };
    }
};

const BorrowedDecodeStateRuntime = struct {
    state: *NativeDecodeState,

    fn init(state: *NativeDecodeState) BorrowedDecodeStateRuntime {
        return .{ .state = state };
    }

    fn currentTokenCount(self: *const BorrowedDecodeStateRuntime) usize {
        return self.state.total_tokens;
    }

    fn kvView(self: *const BorrowedDecodeStateRuntime) ?KvView {
        return self.state.kvView();
    }

    fn notePrefill(self: *BorrowedDecodeStateRuntime, token_count: usize) !void {
        try self.state.notePrefill(token_count);
    }

    fn appendPrefillChunk(self: *BorrowedDecodeStateRuntime, token_count: usize) !void {
        try self.state.appendPrefillChunk(token_count);
    }

    fn appendGeneratedToken(self: *BorrowedDecodeStateRuntime) !usize {
        try self.state.appendGeneratedToken();
        return self.currentTokenCount();
    }

    fn appendGeneratedTokens(self: *BorrowedDecodeStateRuntime, count: usize) !usize {
        try self.state.appendGeneratedTokens(count);
        return self.currentTokenCount();
    }

    fn truncateGeneratedTokens(self: *BorrowedDecodeStateRuntime, count: usize) !void {
        try self.state.truncateTokens(count);
    }

    fn compactKvCache(self: *BorrowedDecodeStateRuntime, config: runtime.kv.compaction.CompactionConfig) !void {
        _ = try self.state.compactKvCache(config);
    }

    fn validateDecodePosition(self: *const BorrowedDecodeStateRuntime, position: usize) !void {
        if (self.currentTokenCount() != position) return error.InvalidDecodePosition;
    }

    fn makeDecodeContext(
        self: *BorrowedDecodeStateRuntime,
        seq_len: usize,
        query_seq_len: usize,
    ) gpt_arch.DecodeContext {
        return self.state.gptDecodeContext(seq_len, query_seq_len);
    }

    fn reservePrefillTo(self: *BorrowedDecodeStateRuntime, target_total_seq_len: usize) !usize {
        if (target_total_seq_len < self.currentTokenCount()) return error.InvalidPrefillAdvance;
        const missing = target_total_seq_len - self.currentTokenCount();
        if (missing == 0) return 0;
        try self.appendPrefillChunk(missing);
        return missing;
    }

    fn rollbackReservedPrefill(self: *BorrowedDecodeStateRuntime, reserved: usize) !void {
        if (reserved == 0) return;
        try self.truncateGeneratedTokens(reserved);
    }

    fn preparePrefill(
        self: *BorrowedDecodeStateRuntime,
        seq_len: usize,
        query_seq_len: usize,
    ) !gpt_arch.DecodeContext {
        if (self.currentTokenCount() == 0) {
            if (seq_len != query_seq_len) return error.UnsupportedShape;
            try self.notePrefill(query_seq_len);
        } else {
            const expected_prior = seq_len - query_seq_len;
            if (self.currentTokenCount() != expected_prior) return error.InvalidPrefillSequence;
            try self.appendPrefillChunk(query_seq_len);
        }
        return self.makeDecodeContext(seq_len, query_seq_len);
    }

    fn beginDecodeStep(
        self: *BorrowedDecodeStateRuntime,
        position: usize,
    ) !struct {
        seq_len: usize,
        decode_context: gpt_arch.DecodeContext,
    } {
        try self.validateDecodePosition(position);
        const seq_len = try self.appendGeneratedToken();
        return .{
            .seq_len = seq_len,
            .decode_context = self.makeDecodeContext(seq_len, 1),
        };
    }
};

fn reserveSpeculativeTargetVerification(
    decode_runtime: *BorrowedDecodeStateRuntime,
    backend: ops.BackendKind,
    target_config: gpt_mod.Config,
    query_len: usize,
    draft_count: usize,
) !void {
    // The qualified resident A4B Metal runtime owns qLen=1 decode only. A
    // speculative verification pass is genuinely multi-row; do not mutate KV
    // state or disguise sequential decode as batched verification until the
    // backend exposes a qualified multi-row verify operation.
    if (backend == .metal and
        query_len > 1 and
        gemma4_runtime.isQualifiedA4bArchitecture(target_config))
    {
        return error.Gemma4A4bMetalSpeculativeVerificationNotQualified;
    }
    _ = try decode_runtime.appendGeneratedTokens(draft_count);
}

fn retryDirectPrefillAfterMemoryBudgetExceeded(
    decode_runtime: *BorrowedDecodeStateRuntime,
    reserved_tokens: usize,
    attempted_chunk_size: usize,
    current_chunk_size: *usize,
    allow_resize: bool,
    err: anyerror,
) !bool {
    if (err != error.MemoryBudgetExceeded or attempted_chunk_size <= 1) return false;
    try decode_runtime.rollbackReservedPrefill(reserved_tokens);
    if (!allow_resize) return false;
    current_chunk_size.* = @max(attempted_chunk_size / 2, 1);
    return true;
}

pub fn buildOwnedBatchDecodeContext(
    allocator: std.mem.Allocator,
    states: []const *NativeDecodeState,
    seq_len: usize,
    query_seq_len: usize,
) !OwnedBatchDecodeContext {
    if (states.len == 0) return error.EmptyBatch;

    var first_runtime = BorrowedDecodeStateRuntime.init(states[0]);
    const first_ctx = first_runtime.makeDecodeContext(seq_len, query_seq_len);
    if (states.len == 1) {
        return .{
            .allocator = allocator,
            .context = first_ctx,
        };
    }
    if (!first_ctx.usesPagedKv()) {
        return .{
            .allocator = allocator,
            .context = first_ctx,
        };
    }

    const batch = try allocator.alloc(gpt_arch.DecodeContext.KvBatchView, states.len);
    errdefer allocator.free(batch);

    for (states, 0..) |state, idx| {
        var decode_runtime = BorrowedDecodeStateRuntime.init(state);
        const ctx = decode_runtime.makeDecodeContext(seq_len, query_seq_len);
        if (!ctx.usesPagedKv()) return error.MixedBatchDecodeModes;
        if (ctx.total_sequence_len != first_ctx.total_sequence_len) return error.IncompatibleBatchDecodeContext;
        if (ctx.query_sequence_len != first_ctx.query_sequence_len) return error.IncompatibleBatchDecodeContext;
        if (ctx.kv_sequence_len != first_ctx.kv_sequence_len) return error.IncompatibleBatchDecodeContext;
        if (ctx.kv_position_offset != first_ctx.kv_position_offset) return error.IncompatibleBatchDecodeContext;

        batch[idx] = .{
            .kv_cache = .{
                .sequence_id = ctx.kv_cache.?.sequence_id,
                .pool_id = ctx.kv_cache.?.pool_id,
                .logical_block_count = ctx.kv_cache.?.logical_block_count,
                .tail_tokens = ctx.kv_cache.?.tail_tokens,
                .position_offset = ctx.kv_cache.?.position_offset,
                .max_inflight_tokens = ctx.kv_cache.?.max_inflight_tokens,
                .allow_swa_ring = ctx.kv_cache.?.allow_swa_ring,
                .logical_blocks = ctx.kv_cache.?.logical_blocks,
                .kv_storage = ctx.kv_cache.?.kv_storage,
            },
            .kv_manager = ctx.kv_manager.?,
            .kv_storage = ctx.kv_storage orelse ctx.kv_cache.?.kv_storage,
        };
    }

    return .{
        .allocator = allocator,
        .kv_batch = batch,
        .context = .{
            .attention_mode = first_ctx.attention_mode,
            .total_sequence_len = first_ctx.total_sequence_len,
            .query_sequence_len = first_ctx.query_sequence_len,
            .kv_sequence_len = first_ctx.kv_sequence_len,
            .kv_position_offset = first_ctx.kv_position_offset,
            .kv_storage = first_ctx.kv_storage,
            .kv_batch = batch,
            .moe_runtime = first_ctx.moe_runtime,
        },
    };
}

const MixedBatchDecodeItem = struct {
    state: *NativeDecodeState,
    total_sequence_len: usize,
    query_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize,
    attention_mode: gpt_arch.DecodeContext.AttentionMode,
};

pub fn buildOwnedMixedBatchDecodeContext(
    allocator: std.mem.Allocator,
    items: []const MixedBatchDecodeItem,
) !OwnedBatchDecodeContext {
    if (items.len == 0) return error.EmptyBatch;
    if (items.len == 1) {
        const item = items[0];
        var decode_runtime = BorrowedDecodeStateRuntime.init(item.state);
        var ctx = decode_runtime.makeDecodeContext(item.total_sequence_len, item.query_sequence_len);
        if (!ctx.usesPagedKv()) return error.MixedBatchDecodeModes;
        ctx.attention_mode = item.attention_mode;
        ctx.total_sequence_len = item.total_sequence_len;
        ctx.query_sequence_len = item.query_sequence_len;
        ctx.kv_sequence_len = item.kv_sequence_len;
        ctx.kv_position_offset = item.kv_position_offset;
        return .{
            .allocator = allocator,
            .context = ctx,
        };
    }

    const max_query_seq_len = blk: {
        var max_len: usize = 0;
        for (items) |item| max_len = @max(max_len, item.query_sequence_len);
        break :blk max_len;
    };
    const max_total_seq_len = blk: {
        var max_len: usize = 0;
        for (items) |item| max_len = @max(max_len, item.total_sequence_len);
        break :blk max_len;
    };
    const max_kv_seq_len = blk: {
        var max_len: usize = 0;
        for (items) |item| max_len = @max(max_len, item.kv_sequence_len);
        break :blk max_len;
    };

    const batch = try allocator.alloc(gpt_arch.DecodeContext.KvBatchView, items.len);
    errdefer allocator.free(batch);

    for (items, 0..) |item, idx| {
        var decode_runtime = BorrowedDecodeStateRuntime.init(item.state);
        const ctx = decode_runtime.makeDecodeContext(item.total_sequence_len, item.query_sequence_len);
        if (!ctx.usesPagedKv()) return error.MixedBatchDecodeModes;
        batch[idx] = .{
            .kv_cache = .{
                .sequence_id = ctx.kv_cache.?.sequence_id,
                .pool_id = ctx.kv_cache.?.pool_id,
                .logical_block_count = ctx.kv_cache.?.logical_block_count,
                .tail_tokens = ctx.kv_cache.?.tail_tokens,
                .position_offset = ctx.kv_cache.?.position_offset,
                .max_inflight_tokens = ctx.kv_cache.?.max_inflight_tokens,
                .allow_swa_ring = ctx.kv_cache.?.allow_swa_ring,
                .logical_blocks = ctx.kv_cache.?.logical_blocks,
                .kv_storage = ctx.kv_cache.?.kv_storage,
            },
            .kv_manager = ctx.kv_manager.?,
            .kv_storage = ctx.kv_storage orelse ctx.kv_cache.?.kv_storage,
            .per_item_query_len = item.query_sequence_len,
            .per_item_total_len = item.total_sequence_len,
            .per_item_kv_len = item.kv_sequence_len,
            .per_item_kv_position_offset = item.kv_position_offset,
            .per_item_mode = switch (item.attention_mode) {
                .full_recompute => .dense_causal,
                .paged_prefill => .paged_prefill,
                .paged_decode => .paged_decode,
            },
        };
    }

    return .{
        .allocator = allocator,
        .kv_batch = batch,
        .context = .{
            .attention_mode = .paged_prefill,
            .total_sequence_len = max_total_seq_len,
            .query_sequence_len = max_query_seq_len,
            .kv_sequence_len = max_kv_seq_len,
            .kv_position_offset = 0,
            .kv_storage = items[0].state.kv_storage,
            .kv_batch = batch,
            .moe_runtime = &items[0].state.moe_runtime,
        },
    };
}

/// Generation pipeline backed by ortgenai (ONNX Runtime GenAI).
pub const GenerationPipeline = struct {
    allocator: std.mem.Allocator,
    model: if (build_options.enable_onnx) *ortgenai.GenAiModel else void,
    chat_template: ?*const ChatTemplate = null,
    prompt_override: ?[]const u8 = null,

    pub fn generate(self: *GenerationPipeline, messages: []const Message, config: GenerationConfig) !GenerationResult {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;
        if (config.ignore_eos) return error.IgnoreEosUnsupportedByOrtGenAi;

        // Format messages into a prompt
        const prompt = if (self.prompt_override) |override|
            try self.allocator.dupe(u8, override)
        else if (self.chat_template) |ct|
            try ct.applyWithOptions(self.allocator, messages, .{ .enable_thinking = config.enable_thinking })
        else
            try formatMessages(self.allocator, messages);
        defer self.allocator.free(prompt);

        const gen_opts = ortgenai.GenerateOptions{
            .max_tokens = config.max_tokens,
            .temperature = config.temperature,
            .top_p = config.top_p,
            .top_k = config.top_k,
        };

        // Check for multimodal content (images in messages)
        if (messagesHaveImages(messages)) {
            // Collect all image bytes across messages
            var all_images = std.ArrayListUnmanaged([]const u8).empty;
            defer all_images.deinit(self.allocator);
            for (messages) |msg| {
                if (msg.image_bytes) |imgs| {
                    for (imgs) |img| try all_images.append(self.allocator, img);
                }
            }

            const result = try ortgenai.generateWithImages(self.allocator, self.model, prompt, all_images.items, gen_opts);
            return .{
                .text = result.text,
                .token_ids = null,
                .prompt_tokens = result.prompt_tokens,
                .tokens_used = result.tokens_used,
                .finish_reason = result.finish_reason,
                .allocator = result.allocator,
            };
        }

        const result = try ortgenai.generate(self.allocator, self.model, prompt, gen_opts);
        return .{
            .text = result.text,
            .token_ids = null,
            .prompt_tokens = result.prompt_tokens,
            .tokens_used = result.tokens_used,
            .finish_reason = result.finish_reason,
            .allocator = result.allocator,
        };
    }

    /// Streaming generation: calls on_token for each decoded token fragment.
    /// Returns the final result (full text, token count, finish reason).
    /// Text-only; multimodal streaming not yet supported (falls back to non-streaming).
    pub fn generateStreaming(
        self: *GenerationPipeline,
        messages: []const Message,
        config: GenerationConfig,
        on_token_ctx: *anyopaque,
        on_token: TokenCallback,
    ) !GenerationResult {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;
        if (config.ignore_eos) return error.IgnoreEosUnsupportedByOrtGenAi;

        // Multimodal streaming not supported yet — fall back
        if (messagesHaveImages(messages)) {
            const result = try self.generate(messages, config);
            _ = emitCompletedStreamingText(on_token_ctx, on_token, result.text);
            return result;
        }

        const prompt = if (self.prompt_override) |override|
            try self.allocator.dupe(u8, override)
        else if (self.chat_template) |ct|
            try ct.applyWithOptions(self.allocator, messages, .{ .enable_thinking = config.enable_thinking })
        else
            try formatMessages(self.allocator, messages);
        defer self.allocator.free(prompt);

        const gen_opts = ortgenai.GenerateOptions{
            .max_tokens = config.max_tokens,
            .temperature = config.temperature,
            .top_p = config.top_p,
            .top_k = config.top_k,
        };

        const result = try ortgenai.generateStreaming(self.allocator, self.model, prompt, gen_opts, on_token_ctx, on_token);
        return .{
            .text = result.text,
            .token_ids = null,
            .prompt_tokens = result.prompt_tokens,
            .tokens_used = result.tokens_used,
            .finish_reason = result.finish_reason,
            .allocator = result.allocator,
        };
    }

    pub fn deinit(self: *GenerationPipeline) void {
        if (build_options.enable_onnx) {
            self.model.deinit();
        }
    }
};

const Qwen3VlParityImageJson = struct {
    source_width: usize,
    source_height: usize,
    resized_width: usize,
    resized_height: usize,
    grid_thw: [3]u32,
    patch_rows: usize,
    patch_columns: usize,
    spatial_patch_f32le_sha256: []const u8,
    positioned_embedding_f32le_sha256: []const u8,
    vision_trace_layer: ?usize,
    vision_trace_f32le_sha256: ?[]const u8,
};

const Qwen3VlParityTensorDigestJson = struct {
    value_count: usize,
    f32le_sha256: []const u8,
};

fn writeQwen3VlParityEvidence(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    patch_path: ?[]const u8,
    placeholder_token_ids: []const i32,
    prepared: *const qwen3vl_projector.PreparedPrompt,
    projected: *const qwen3vl_projector.ProjectedImages,
) !void {
    if (projected.preprocess_evidence.len != projected.grids.len or
        projected.preprocess_evidence.len != projected.tokens_per_image.len)
    {
        return error.MissingQwen3VlPreprocessEvidence;
    }
    const images = try allocator.alloc(Qwen3VlParityImageJson, projected.preprocess_evidence.len);
    defer allocator.free(images);
    for (projected.preprocess_evidence, 0..) |*evidence, i| {
        images[i] = .{
            .source_width = evidence.source_width,
            .source_height = evidence.source_height,
            .resized_width = evidence.resized_width,
            .resized_height = evidence.resized_height,
            .grid_thw = .{ evidence.grid.temporal, evidence.grid.height, evidence.grid.width },
            .patch_rows = evidence.patch_rows,
            .patch_columns = evidence.patch_columns,
            .spatial_patch_f32le_sha256 = &evidence.spatial_patch_f32le_sha256,
            .positioned_embedding_f32le_sha256 = &evidence.positioned_embedding_f32le_sha256,
            .vision_trace_layer = evidence.vision_trace_layer,
            .vision_trace_f32le_sha256 = if (evidence.vision_trace_f32le_sha256) |*digest| digest else null,
        };
    }
    var visual_token_count: usize = 0;
    for (projected.tokens_per_image) |count| {
        visual_token_count = std.math.add(usize, visual_token_count, count) catch return error.RequestTooLarge;
    }
    var patch_value_count: usize = 0;
    for (projected.preprocess_evidence) |evidence| {
        const image_values = std.math.mul(usize, evidence.patch_rows, evidence.patch_columns) catch
            return error.RequestTooLarge;
        patch_value_count = std.math.add(usize, patch_value_count, image_values) catch return error.RequestTooLarge;
    }
    if (projected.preprocess_spatial_patches.len != patch_value_count) {
        return error.MissingQwen3VlPreprocessEvidence;
    }
    const projected_digest = qwen3vl_projector.sha256F32LeHex(projected.embeddings);
    const deepstack_digest = qwen3vl_projector.sha256F32LeHex(projected.deepstack_embeddings);
    const deepstack_tap_value_count = std.math.mul(usize, visual_token_count, projected.hidden_size) catch
        return error.RequestTooLarge;
    const expected_deepstack_values = std.math.mul(
        usize,
        deepstack_tap_value_count,
        projected.deepstack_layer_count,
    ) catch return error.RequestTooLarge;
    if (projected.deepstack_embeddings.len != expected_deepstack_values) {
        return error.MissingQwen3VlPreprocessEvidence;
    }
    const deepstack_tap_hashes = try allocator.alloc([64]u8, projected.deepstack_layer_count);
    defer allocator.free(deepstack_tap_hashes);
    const deepstack_taps = try allocator.alloc(Qwen3VlParityTensorDigestJson, projected.deepstack_layer_count);
    defer allocator.free(deepstack_taps);
    for (deepstack_taps, deepstack_tap_hashes, 0..) |*tap, *digest, index| {
        const values = projected.deepstack_embeddings[index * deepstack_tap_value_count ..][0..deepstack_tap_value_count];
        digest.* = qwen3vl_projector.sha256F32LeHex(values);
        tap.* = .{ .value_count = values.len, .f32le_sha256 = digest[0..] };
    }
    if (patch_path) |raw_path| try writeF32LeFile(allocator, io, raw_path, projected.preprocess_spatial_patches);
    const json = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.qwen3vl.parity.v1",
        .placeholder_token_ids = placeholder_token_ids,
        .expanded_token_ids = prepared.token_ids,
        .mrope_position_ids = prepared.plan.mrope_positions,
        .images = images,
        .visual_token_count = visual_token_count,
        .deepstack_layer_count = prepared.deepstack_layer_count,
        .projected_embedding_value_count = projected.embeddings.len,
        .projected_embedding_f32le_sha256 = projected_digest[0..],
        .deepstack_embedding_value_count = projected.deepstack_embeddings.len,
        .deepstack_embedding_f32le_sha256 = deepstack_digest[0..],
        .deepstack_taps = deepstack_taps,
        .mrope_position_delta = prepared.plan.mrope_position_delta,
        .spatial_patch_f32le_path = patch_path,
        .spatial_patch_value_count = patch_value_count,
    }, .{});
    defer allocator.free(json);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = json });
}

fn writeF32LeFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8, values: []const f32) !void {
    const byte_count = std.math.mul(usize, values.len, @sizeOf(f32)) catch return error.RequestTooLarge;
    const bytes = try allocator.alloc(u8, byte_count);
    defer allocator.free(bytes);
    for (values, 0..) |value, i| {
        std.mem.writeInt(u32, bytes[i * 4 ..][0..4], @bitCast(value), .little);
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

/// Prompt tokenization computed before the pipeline runs (e.g. by server
/// admission). All slices are borrowed; the owner must keep them alive for the
/// whole generate call and frees them afterwards.
pub const PreEncodedPrompt = struct {
    ids: []const i32,
    attention_mask: []const i32,
    /// Truncation limit the encode was validated against. Reuse requires an
    /// exact match with the limit the pipeline derives for the request.
    prompt_token_limit: usize,
};

/// Native generation pipeline using ComputeBackend + GPT arch directly.
/// Runs autoregressive decoding: tokenize → loop(gpt_arch.forward → sample → append).
pub const NativeGenerationPipeline = struct {
    allocator: std.mem.Allocator,
    io: ?std.Io = null,
    cb: ComputeBackend,
    session: ?backends.Session = null,
    gpt_config: gpt_mod.Config,
    kv_dtype: ?runtime.kv.pool.KvDType = null,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    tokenizer: tokenizer_mod.Tokenizer,
    add_bos_token: bool = false,
    bos_token: []const u8 = "",
    chat_template: ?*const ChatTemplate = null,
    prompt_override: ?[]const u8 = null,
    /// Optional pre-rendered chat prompt (borrowed) from the caller's
    /// admission estimate. Only valid when rendered from the exact messages,
    /// template, and enable_thinking option this generate call receives;
    /// ignored when prompt_override is set.
    preformatted_prompt: ?[]const u8 = null,
    /// Optional pre-encoded tokenization of `preformatted_prompt`. Used only
    /// when its recorded token limit matches the request's derived limit.
    pre_encoded_prompt: ?PreEncodedPrompt = null,
    /// Optional lightweight cancellation/progress hook. Unlike TokenCallback,
    /// this is polled for every decoded step even when channel projection is
    /// intentionally withholding private reasoning text.
    continue_ctx: ?*anyopaque = null,
    continue_fn: ?*const fn (ctx: *anyopaque) bool = null,
    /// Local diagnostics may request the complete generated sequence. Serving
    /// callers must keep the default so private Gemma4 channel tokens stay hidden.
    return_raw_token_ids: bool = false,
    print_timing: bool = false,
    model_dir: ?[]const u8 = null,
    artifact_dir: ?[]const u8 = null,
    gguf_projector_path: ?[]const u8 = null,
    decode_state: ?*NativeDecodeState = null,
    scheduler: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = null,
    scheduler_lease: ?*runtime.scheduler.native_generate.Lease = null,
    /// Serializes the shared model backend for one claimed scheduler step.
    /// Request-local tokenization and sampling remain outside this lock.
    execution_lock: ?*std.atomic.Mutex = null,
    /// Optional smaller draft model for speculative decoding. When set
    /// together with `GenerationConfig.draft_model`, the generate loop
    /// proposes K tokens with the draft and verifies them against the
    /// target (this pipeline) in a single forward pass.
    draft_cb: ?ComputeBackend = null,
    draft_gpt_config: ?gpt_mod.Config = null,
    draft_decode_state: ?*NativeDecodeState = null,
    prompt_cache: ?*runtime.kv.prompt_cache.PromptPrefixCache = null,
    /// Optional graph cache for graph-mode execution. When non-null,
    /// the decode loop traces the forward pass once, caches the graph,
    /// and replays it through the interpreter on subsequent steps.
    graph_cache: ?*graph_mod.cache.GraphCache = null,
    /// Optional device mesh for multi-device inference. graphForward only
    /// dispatches through the multi-device executor when this is set and
    /// parallel_config requests real sharding across multiple devices.
    device_mesh: ?*graph_mod.device_mesh.DeviceMesh = null,
    /// Parallel execution strategy for multi-device graph execution.
    /// `.single` or null falls back to the normal single-device path.
    parallel_config: ?graph_mod.parallel_strategy.ParallelConfig = null,
    /// Optional explicit compiled partition backend. When set, graphForward
    /// partitions the single-device graph and compiles eligible subgraphs
    /// for the requested backend while the host backend handles fallbacks.
    compiled_partition_backend: ?ops.BackendKind = null,
    /// Whether the compiled backend should attach bounded partitions or
    /// only proceed when it can own the whole traced graph shape.
    compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = .partitioned,
    /// Optional PJRT client for TPU/CPU partition execution. Type-erased
    /// pointer to pjrt.Client; cast back in attachPjrtExecutors.
    pjrt_client: ?*anyopaque = null,

    const prefetch_drain_budget_per_step: usize = 4;
    const default_mtp_zero_match_fallback_rounds: usize = 16;

    fn shouldStopOnEos(self: *const NativeGenerationPipeline, config: GenerationConfig, token: usize) bool {
        return !config.ignore_eos and self.gpt_config.isEosToken(token);
    }

    fn rejectUnsupportedDeepSeekV4GraphMode(self: *const NativeGenerationPipeline) !void {
        if (!NativeDecodeState.requiresDeepSeekV4CompressedCache(self.gpt_config)) return;
        if (self.graph_cache != null or self.compiled_partition_backend != null) {
            return error.DeepSeekV4CompressedGraphModeNotSupported;
        }
    }

    fn speculativeUsesDeepSeekV4CompressedCache(self: *const NativeGenerationPipeline) bool {
        if (NativeDecodeState.requiresDeepSeekV4CompressedCache(self.gpt_config)) return true;
        if (self.draft_gpt_config) |draft_config| {
            return NativeDecodeState.requiresDeepSeekV4CompressedCache(draft_config);
        }
        return false;
    }

    fn makeSpeculativeDraftPipeline(
        self: *const NativeGenerationPipeline,
        draft_cb: ComputeBackend,
        draft_gpt_config: gpt_mod.Config,
    ) NativeGenerationPipeline {
        return .{
            .allocator = self.allocator,
            .cb = draft_cb,
            .gpt_config = draft_gpt_config,
            .tokenizer = self.tokenizer,
            .artifact_dir = self.artifact_dir,
            // Graph caches and compiled attachments are model-scoped. A draft
            // model with different geometry must execute through its own
            // backend rather than the target model's traced A4B graph.
            .graph_cache = null,
            .compiled_partition_backend = null,
            .compiled_attachment_target = .partitioned,
            .pjrt_client = null,
        };
    }

    const PendingDecodeBatchWork = struct {
        allocator: std.mem.Allocator,
        decode_state: *NativeDecodeState,
        token_id: i64,
        seq_len: usize,
        logits: ?[]f32 = null,
        failure: ?anyerror = null,
        ready: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };

    const PendingPrefillBatchWork = struct {
        allocator: std.mem.Allocator,
        decode_state: *NativeDecodeState,
        token_ids: []const i64,
        seq_len: usize,
        query_seq_len: usize,
        wants_last_logits: bool,
        logits: ?[]f32 = null,
        failure: ?anyerror = null,
        ready: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };

    pub fn generate(self: *NativeGenerationPipeline, messages: []const Message, config: GenerationConfig) !GenerationResult {
        return self.generateWithCallback(messages, config, null, null);
    }

    pub fn generateStreaming(
        self: *NativeGenerationPipeline,
        messages: []const Message,
        config: GenerationConfig,
        on_token_ctx: *anyopaque,
        on_token: TokenCallback,
    ) !GenerationResult {
        return self.generateWithCallback(messages, config, on_token, on_token_ctx);
    }

    fn generateWithCallback(
        self: *NativeGenerationPipeline,
        messages: []const Message,
        config: GenerationConfig,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
    ) !GenerationResult {
        const allocator = self.allocator;
        const started_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        const grammar_opens_public_final_channel =
            config.grammar != null and
            self.gpt_config.usesGemma4Channels();
        var fallback_decode_state = NativeDecodeState.initContiguous(allocator);
        defer fallback_decode_state.deinit();
        const decode_state = self.decode_state orelse &fallback_decode_state;
        decode_state.configureForGptConfig(self.gpt_config);
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        try decode_state.ensureDeepSeekV4CompressedCache(self.gpt_config);
        if (self.draft_decode_state) |draft_state| {
            if (self.draft_gpt_config) |draft_config| {
                draft_state.configureForGptConfig(draft_config);
                try draft_state.ensureDeepSeekV4CompressedCache(draft_config);
            }
        }

        // Format prompt. A caller-provided pre-render is borrowed as-is; see
        // the preformatted_prompt field contract.
        const use_preformatted_prompt = self.prompt_override == null and self.preformatted_prompt != null;
        var prompt: []const u8 = undefined;
        var prompt_borrowed = use_preformatted_prompt;
        if (self.prompt_override) |override| {
            prompt = try allocator.dupe(u8, override);
        } else if (self.preformatted_prompt) |preformatted| {
            prompt = preformatted;
        } else if (self.chat_template) |ct| {
            prompt = try ct.applyWithOptions(allocator, messages, .{ .enable_thinking = config.enable_thinking });
        } else {
            prompt = try formatMessages(allocator, messages);
        }
        defer if (!prompt_borrowed) allocator.free(prompt);
        if (grammar_opens_public_final_channel) {
            const public_prompt = try openGemma4FinalChannelForGrammar(allocator, prompt);
            if (!prompt_borrowed) allocator.free(prompt);
            prompt = public_prompt;
            prompt_borrowed = false;
        }
        const prompt_opens_public_final_channel =
            configPromptOpensGemma4FinalChannel(self.gpt_config, prompt);
        const formatted_prompt_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;

        const has_images = messagesHaveImages(messages);
        const has_audio = messagesHaveAudio(messages);
        if ((config.qwen3vl_parity_patch_path != null or config.qwen3vl_parity_logits_path != null) and
            config.qwen3vl_parity_json_path == null or
            (config.qwen3vl_parity_json_path != null and
                (self.gpt_config.family != .qwen3_vl or !has_images or has_audio)))
        {
            return error.InvalidQwen3VlParityRequest;
        }
        const requested_max_tokens: usize = @intCast(@max(config.max_tokens, 1));

        // Decide whether the loaded draft participates before deriving the
        // shared context limit. An unavailable/disabled draft must not shorten
        // a target-only request.
        const draft_is_gemma4_mtp = if (self.draft_gpt_config) |cfg| cfg.gemma4_mtp_assistant else false;
        const draft_is_nextn_gemma4_mtp = if (self.draft_gpt_config) |cfg|
            cfg.gemma4_mtp_assistant and cfg.mtp_num_centroids == 0 and cfg.mtp_backbone_hidden_size == self.gpt_config.hidden_size
        else
            false;
        const allow_unshared_gemma4_mtp =
            platform.env.getenvBool("ANTFLY_GEMMA4_MTP_ALLOW_UNSHARED_TARGET") or
            config.speculation_policy == .force or
            config.speculation_calibration != .none;
        const disable_unshared_gemma4_mtp =
            draft_is_gemma4_mtp and
            !draft_is_nextn_gemma4_mtp and
            self.gpt_config.num_kv_shared_layers == 0 and
            !allow_unshared_gemma4_mtp;
        if (disable_unshared_gemma4_mtp) {
            std.log.warn("Gemma4 MTP disabled: target has no shared-KV metadata; set ANTFLY_GEMMA4_MTP_ALLOW_UNSHARED_TARGET=1 to force experimental MTP", .{});
        }
        const loaded_draft_requested = self.draft_cb != null and self.draft_gpt_config != null;
        const draft_requested = loaded_draft_requested or config.speculation_requested;
        try validateSpeculativeK(draft_requested, config.speculative_k);
        const speculation_policy = config.speculation_policy;
        const mtp_auto_uncalibrated =
            speculation_policy == .auto and
            draft_is_gemma4_mtp and
            config.speculation_calibration == .none;
        const mtp_auto_too_short =
            speculation_policy == .auto and
            draft_is_gemma4_mtp and
            requested_max_tokens < gemma4MtpAutoMinGenerationTokens();
        const mtp_metal_auto_disabled =
            speculation_policy == .auto and
            draft_is_gemma4_mtp and
            self.cb.kind() == .metal and
            !gemma4MtpMetalAutoEnabled();
        const target_speculative_verification_unavailable =
            !supportsSpeculativeTargetVerification(self.cb.kind(), self.gpt_config);
        const use_speculative = speculation_policy != .off and
            !mtp_auto_uncalibrated and
            !mtp_auto_too_short and
            !mtp_metal_auto_disabled and
            !target_speculative_verification_unavailable and
            loaded_draft_requested and
            !disable_unshared_gemma4_mtp;
        if ((has_images or has_audio) and use_speculative) return error.MultimodalSpeculativeDecodingNotSupported;
        if (use_speculative and self.speculativeUsesDeepSeekV4CompressedCache()) return error.DeepSeekV4CompressedSpeculativeDecodingNotSupported;

        const relevant_draft_config = if (use_speculative) self.draft_gpt_config else null;
        const speculative_bonus_tokens: usize = if (use_speculative and config.speculative_k > 0) 1 else 0;
        const expanded_prompt_token_limit = try nativeGenerationPromptTokenLimit(
            self.gpt_config,
            relevant_draft_config,
            requested_max_tokens,
            speculative_bonus_tokens,
            0,
        );
        // Qwen replaces textual image markers with exact visual-token spans
        // below, then validates the fully expanded prompt. Reserving its
        // coarse media allowance during this preliminary text-only encode can
        // reject text that the real multimodal encode accepts.
        const preliminary_media_allowance = nativeGenerationPreliminaryMediaTokenAllowance(messages, self.gpt_config);
        const text_prompt_token_limit = try nativeGenerationPromptTokenLimit(
            self.gpt_config,
            relevant_draft_config,
            requested_max_tokens,
            speculative_bonus_tokens,
            preliminary_media_allowance,
        );
        // Reuse the caller's pre-encode only when it tokenized these exact
        // prompt bytes under this exact truncation limit; the grammar channel
        // rewrite above changes the prompt and invalidates both.
        const reusable_pre_encoded: ?PreEncodedPrompt = if (use_preformatted_prompt and !grammar_opens_public_final_channel)
            if (self.pre_encoded_prompt) |pre|
                (if (pre.prompt_token_limit == text_prompt_token_limit) pre else null)
            else
                null
        else
            null;
        var owned_encoded: ?tokenizer_mod.EncodeResult = null;
        defer if (owned_encoded) |*owned| owned.deinit();
        var encoded_ids: []const i32 = undefined;
        var encoded_attention_mask: []const i32 = undefined;
        if (reusable_pre_encoded) |pre| {
            encoded_ids = pre.ids;
            encoded_attention_mask = pre.attention_mask;
        } else {
            owned_encoded = try encodeNativeGenerationPrompt(
                self.tokenizer,
                allocator,
                prompt,
                text_prompt_token_limit,
                self.add_bos_token,
                self.bos_token,
            );
            encoded_ids = owned_encoded.?.ids;
            encoded_attention_mask = owned_encoded.?.attention_mask;
        }
        const encoded_prompt_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;

        var actual_prompt_tokens: usize = 0;
        while (actual_prompt_tokens < encoded_attention_mask.len and encoded_attention_mask[actual_prompt_tokens] != 0) : (actual_prompt_tokens += 1) {}
        if (actual_prompt_tokens == 0) return error.EmptyPrompt;
        debugGenerationStage(
            "encoded prompt chars={d} actual_prompt_tokens={d}",
            .{ prompt.len, actual_prompt_tokens },
        );

        var prepared_multimodal_prompt: ?gemma3_mm.PreparedPrompt = null;
        defer if (prepared_multimodal_prompt) |*prepared| prepared.deinit(&self.cb);
        var prepared_qwen3vl_prompt: ?qwen3vl_projector.PreparedPrompt = null;
        defer if (prepared_qwen3vl_prompt) |*prepared| prepared.deinit(&self.cb);

        const prompt_token_count = blk: {
            if (!has_images and !has_audio) break :blk actual_prompt_tokens;
            debugGenerationStage("multimodal prompt begin has_images={} has_audio={}", .{ has_images, has_audio });
            const images = try collectImagesInPromptOrder(allocator, messages);
            defer allocator.free(images);
            const audio_clips = try collectAudioInPromptOrder(allocator, messages);
            defer allocator.free(audio_clips);
            debugGenerationStage("multimodal collected images={d} audio={d}", .{ images.len, audio_clips.len });

            if (self.gguf_projector_path) |projector_path| {
                const projector_kind = try projector_format_mod.detectPath(allocator, projector_path);
                if (projector_format_mod.isAntfly(projector_kind)) {
                    if (has_audio) return error.NativeAudioGenerationNotImplemented;
                    if (!self.gpt_config.isMultimodal()) return error.InvalidModelForGeneration;
                    const model_dir = self.model_dir orelse return error.MissingModelDirForMultimodal;
                    const expanded_prompt = try gemma3_mm.expandPromptText(allocator, prompt, self.gpt_config, images.len);
                    defer allocator.free(expanded_prompt);
                    var expanded_encoded = try encodeNativeGenerationPrompt(
                        self.tokenizer,
                        allocator,
                        expanded_prompt,
                        expanded_prompt_token_limit,
                        self.add_bos_token,
                        self.bos_token,
                    );
                    defer expanded_encoded.deinit();
                    var expanded_prompt_tokens: usize = 0;
                    while (expanded_prompt_tokens < expanded_encoded.attention_mask.len and expanded_encoded.attention_mask[expanded_prompt_tokens] != 0) : (expanded_prompt_tokens += 1) {}
                    if (expanded_prompt_tokens == 0) return error.EmptyPrompt;

                    prepared_multimodal_prompt = try gemma3_mm.prepareExpandedPromptEmbeddingsWithProjector(
                        &self.cb,
                        allocator,
                        model_dir,
                        projector_path,
                        self.gpt_config,
                        expanded_encoded.ids[0..expanded_prompt_tokens],
                        images.len,
                        images,
                    );
                } else if (projector_kind == .clip_qwen3vl_image) {
                    if (has_audio or !has_images) return error.NativeAudioGenerationNotImplemented;
                    if (self.gpt_config.family != .qwen3_vl or !decode_state.isPaged()) return error.InvalidModelForGeneration;
                    if (images.len == 0 or images.len > 8) return error.ImageLimitExceeded;
                    const per_image_budget = expanded_prompt_token_limit / images.len;
                    // The pinned Qwen smart-resize contract has a 64-token
                    // minimum (65,536 pixels). Reject before media work when
                    // the text budget cannot hold that geometry.
                    if (per_image_budget < 64) return error.InputTokenLimitExceeded;
                    const per_image_limit = @min(@as(usize, 576), per_image_budget);
                    var projected = if (config.qwen3vl_parity_json_path != null)
                        try qwen3vl_projector.encodeProjectedImagesForQualification(
                            &self.cb,
                            allocator,
                            projector_path,
                            images,
                            .{ .max_images = 8, .max_merged_tokens = per_image_limit },
                        )
                    else
                        try qwen3vl_projector.encodeProjectedImages(
                            &self.cb,
                            allocator,
                            projector_path,
                            images,
                            .{ .max_images = 8, .max_merged_tokens = per_image_limit },
                        );
                    defer projected.deinit();
                    var qwen_encoded = try encodeQwenPromptWithImagePlaceholders(
                        self.tokenizer,
                        allocator,
                        prompt,
                        expanded_prompt_token_limit,
                        self.add_bos_token,
                        self.bos_token,
                        self.gpt_config,
                    );
                    defer qwen_encoded.deinit();
                    var qwen_prompt_tokens: usize = 0;
                    while (qwen_prompt_tokens < qwen_encoded.attention_mask.len and qwen_encoded.attention_mask[qwen_prompt_tokens] != 0) : (qwen_prompt_tokens += 1) {}
                    if (qwen_prompt_tokens == 0) return error.EmptyPrompt;
                    prepared_qwen3vl_prompt = try qwen3vl_projector.prepareExpandedPromptEmbeddings(
                        &self.cb,
                        allocator,
                        self.gpt_config,
                        qwen_encoded.ids[0..qwen_prompt_tokens],
                        projected,
                        expanded_prompt_token_limit,
                    );
                    if (config.qwen3vl_parity_json_path) |path| {
                        try writeQwen3VlParityEvidence(
                            allocator,
                            self.io orelse return error.MissingIo,
                            path,
                            config.qwen3vl_parity_patch_path,
                            qwen_encoded.ids[0..qwen_prompt_tokens],
                            &prepared_qwen3vl_prompt.?,
                            &projected,
                        );
                    }
                    decode_state.qwen3vl_mrope_position_delta = prepared_qwen3vl_prompt.?.plan.mrope_position_delta;
                    decode_state.qwen3vl_text_only = false;
                } else {
                    var projected_images = if (images.len > 0)
                        try gemma4_projector.encodeProjectedImages(&self.cb, allocator, projector_path, images)
                    else
                        null;
                    defer if (projected_images) |*projected| projected.deinit();
                    var projected_audio = if (audio_clips.len > 0)
                        try gemma4_projector.encodeProjectedAudio(&self.cb, allocator, projector_path, audio_clips)
                    else
                        null;
                    defer if (projected_audio) |*projected| projected.deinit();
                    const expanded_prompt = try gemma4_mm.expandPromptText(
                        allocator,
                        prompt,
                        if (projected_images) |*projected| projected.tokens_per_image else &.{},
                        if (projected_audio) |*projected| projected.tokens_per_audio else &.{},
                    );
                    defer allocator.free(expanded_prompt);
                    var expanded_encoded = try encodeNativeGenerationPrompt(
                        self.tokenizer,
                        allocator,
                        expanded_prompt,
                        expanded_prompt_token_limit,
                        self.add_bos_token,
                        self.bos_token,
                    );
                    defer expanded_encoded.deinit();
                    var expanded_prompt_tokens: usize = 0;
                    while (expanded_prompt_tokens < expanded_encoded.attention_mask.len and expanded_encoded.attention_mask[expanded_prompt_tokens] != 0) : (expanded_prompt_tokens += 1) {}
                    if (expanded_prompt_tokens == 0) return error.EmptyPrompt;

                    prepared_multimodal_prompt = try gemma4_mm.prepareExpandedPromptEmbeddings(
                        &self.cb,
                        allocator,
                        self.tokenizer,
                        self.gpt_config,
                        expanded_encoded.ids[0..expanded_prompt_tokens],
                        if (projected_images) |*projected| projected else null,
                        if (projected_audio) |*projected| projected else null,
                    );
                }
            } else {
                if (has_audio) return error.NativeAudioGenerationNotImplemented;
                if (!self.gpt_config.isMultimodal()) return error.InvalidModelForGeneration;
                const model_dir = self.model_dir orelse return error.MissingModelDirForMultimodal;
                if (self.gpt_config.family == .qwen3_5) {
                    debugGenerationStage("qwen3.5 multimodal load preprocessor", .{});
                    const prep_cfg = try qwen2vl_mm.loadPreprocessorConfig(allocator, model_dir);
                    debugGenerationStage("qwen3.5 multimodal encode prompt max_tokens={d}", .{expanded_prompt_token_limit});
                    var qwen_encoded = try encodeQwenPromptWithImagePlaceholders(
                        self.tokenizer,
                        allocator,
                        prompt,
                        expanded_prompt_token_limit,
                        self.add_bos_token,
                        self.bos_token,
                        self.gpt_config,
                    );
                    defer qwen_encoded.deinit();
                    var qwen_prompt_tokens: usize = 0;
                    while (qwen_prompt_tokens < qwen_encoded.attention_mask.len and qwen_encoded.attention_mask[qwen_prompt_tokens] != 0) : (qwen_prompt_tokens += 1) {}
                    if (qwen_prompt_tokens == 0) return error.EmptyPrompt;

                    debugGenerationStage("qwen3.5 multimodal prepare embeddings prompt_tokens={d}", .{qwen_prompt_tokens});
                    const qwen_prepared = try qwen2vl_mm.prepareExpandedPromptEmbeddings(
                        &self.cb,
                        allocator,
                        self.gpt_config,
                        prep_cfg,
                        qwen_encoded.ids[0..qwen_prompt_tokens],
                        images,
                    );
                    debugGenerationStage("qwen3.5 multimodal prepared tokens={d}", .{qwen_prepared.token_ids.len});
                    prepared_multimodal_prompt = .{
                        .allocator = qwen_prepared.allocator,
                        .token_ids = qwen_prepared.token_ids,
                        .ple_token_ids = qwen_prepared.ple_token_ids,
                        .input_embeddings = qwen_prepared.input_embeddings,
                        .attn_or_mask = qwen_prepared.attn_or_mask,
                    };
                } else {
                    const expanded_prompt = try gemma3_mm.expandPromptText(allocator, prompt, self.gpt_config, images.len);
                    defer allocator.free(expanded_prompt);
                    var expanded_encoded = try encodeNativeGenerationPrompt(
                        self.tokenizer,
                        allocator,
                        expanded_prompt,
                        expanded_prompt_token_limit,
                        self.add_bos_token,
                        self.bos_token,
                    );
                    defer expanded_encoded.deinit();
                    var expanded_prompt_tokens: usize = 0;
                    while (expanded_prompt_tokens < expanded_encoded.attention_mask.len and expanded_encoded.attention_mask[expanded_prompt_tokens] != 0) : (expanded_prompt_tokens += 1) {}
                    if (expanded_prompt_tokens == 0) return error.EmptyPrompt;

                    prepared_multimodal_prompt = try gemma3_mm.prepareExpandedPromptEmbeddings(
                        &self.cb,
                        allocator,
                        model_dir,
                        self.gpt_config,
                        expanded_encoded.ids[0..expanded_prompt_tokens],
                        images.len,
                        images,
                    );
                }
            }
            break :blk if (prepared_qwen3vl_prompt) |prepared|
                prepared.token_ids.len
            else
                prepared_multimodal_prompt.?.token_ids.len;
        };
        try validateNativeGenerationPromptTokenCount(
            prompt_token_count,
            self.gpt_config,
            relevant_draft_config,
            requested_max_tokens,
            speculative_bonus_tokens,
        );

        // Build token sequence. Add slack only when speculative decoding is
        // active; the bonus token can write one position past max_tokens.
        const max_tokens = requested_max_tokens;
        const spec_slack: usize = if (use_speculative and config.speculative_k > 0) 1 else 0;
        const max_seq = prompt_token_count + max_tokens + spec_slack;
        var token_ids = try allocator.alloc(i64, max_seq);
        defer allocator.free(token_ids);
        if (prepared_qwen3vl_prompt) |prepared| {
            @memcpy(token_ids[0..prepared.token_ids.len], prepared.token_ids);
        } else if (prepared_multimodal_prompt) |prepared| {
            @memcpy(token_ids[0..prepared.token_ids.len], prepared.token_ids);
        } else {
            for (0..actual_prompt_tokens) |i| token_ids[i] = @intCast(encoded_ids[i]);
        }
        var seq_len = prompt_token_count;
        debugGenerationStage(
            "starting prefill prompt_token_count={d} seq_len={d} multimodal={}",
            .{ prompt_token_count, seq_len, prepared_multimodal_prompt != null or prepared_qwen3vl_prompt != null },
        );

        const runtime_prepare_started_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        const whole_model_kv_policy = generationKvCapacityPolicyForRoute(
            if (self.compiled_partition_backend == .metal and
                self.compiled_attachment_target == .whole_model and
                self.graph_cache != null and
                decode_state.isPaged() and
                self.cb.kind() == .metal and
                self.kv_dtype != null)
                .metal_whole_model
            else
                .standard,
            self.gpt_config,
            config,
            self.kv_dtype orelse .f16,
        );
        const runtime_prepare_token_hint = if (self.compiled_partition_backend != null and
            self.compiled_attachment_target == .whole_model and
            self.graph_cache != null and
            decode_state.isPaged() and
            self.cb.kind() == .metal and
            whole_model_kv_policy == .split_swa_ring)
            wholeModelPrefillChunkSize(
                config.prefill_chunk_size,
                if (self.scheduler_lease) |lease| lease.prefill_chunk_size else 0,
                prompt_token_count,
            )
        else
            prompt_token_count;
        debugGenerationStage("prepare compiled generation runtime begin prompt_token_count={d} token_hint={d}", .{ prompt_token_count, runtime_prepare_token_hint });
        const runtime_prepared = try self.prepareCompiledGenerationRuntime(runtime_prepare_token_hint);
        debugGenerationStage("prepare compiled generation runtime end prepared={}", .{runtime_prepared});
        const prefill_started_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        if (runtime_prepared) {
            debugGenerationStage(
                "prepared compiled generation runtime prompt_token_count={d} token_hint={d}",
                .{ prompt_token_count, runtime_prepare_token_hint },
            );
        }

        var cached_prompt_tokens: usize = 0;
        var prefill_ids = token_ids[0..seq_len];
        if (self.prompt_cache) |cache| {
            const cache_eligible =
                config.prompt_cache_enabled and
                config.prompt_cache_key != null and
                prepared_multimodal_prompt == null and
                prepared_qwen3vl_prompt == null and
                !use_speculative and
                config.cache_compaction_ratio == null and
                self.compiled_partition_backend == null and
                !NativeDecodeState.requiresDeepSeekV4CompressedCache(self.gpt_config) and
                !self.gpt_config.isQwen35() and
                decode_state.isPaged() and
                decode_state.kv_manager == cache.managerPtr();
            if (cache_eligible) {
                const page_size = cache.pageSize() orelse 0;
                if (page_size > 0 and seq_len > page_size) {
                    const maybe_hit = cache.attachLongestPrefix(
                        config.prompt_cache_key.?,
                        token_ids[0..seq_len],
                        seq_len - page_size,
                    ) catch |err| blk: {
                        std.log.warn("prompt cache lookup failed; continuing without reuse: {s}", .{@errorName(err)});
                        break :blk null;
                    };
                    if (maybe_hit) |hit| {
                        const seeded = blk: {
                            decode_state.seedAttachedPrefix(hit.sequence_id, hit.token_count) catch |err| {
                                cache.releaseAttachedPrefix(hit);
                                std.log.warn("prompt cache seed failed; continuing without reuse: {s}", .{@errorName(err)});
                                break :blk false;
                            };
                            break :blk true;
                        };
                        if (seeded) {
                            cached_prompt_tokens = hit.token_count;
                            prefill_ids = token_ids[cached_prompt_tokens..seq_len];
                        }
                    }
                }
            }
        }

        // Prefill
        try setWorkloadProfileRegime(&self.cb, .prefill);
        const allow_prefill_greedy_token = !use_speculative or (use_speculative and draft_is_gemma4_mtp);
        const capture_mtp_prefill_hidden = use_speculative and draft_is_gemma4_mtp and gemma4MtpTargetHiddenSource() == .final;
        const prefill_output = if (prepared_qwen3vl_prompt) |*prepared|
            PrefillOutput{ .last_logits = try self.executePreparedQwen3VlPrefill(prepared, seq_len, decode_state) }
        else if (prepared_multimodal_prompt) |*prepared|
            PrefillOutput{ .last_logits = try self.executePreparedMultimodalPrefill(prepared, seq_len, decode_state) }
        else
            try self.executePrefill(prefill_ids, seq_len, cached_prompt_tokens, decode_state, config, allow_prefill_greedy_token, capture_mtp_prefill_hidden);
        var prefill_last_logits = prefill_output.last_logits;
        var prefill_greedy_token = prefill_output.greedy_token;
        const prefill_last_hidden = prefill_output.last_hidden;
        const prefill_last_hidden_rows = prefill_output.last_hidden_rows;
        const finished_prefill_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        defer if (prefill_last_logits) |logits| allocator.free(logits);
        defer if (prefill_last_hidden) |hidden| self.cb.free(hidden);
        if (config.qwen3vl_parity_logits_path) |path| {
            if (prepared_qwen3vl_prompt == null or prefill_last_logits == null or
                prefill_last_logits.?.len != self.gpt_config.vocab_size)
            {
                return error.MissingQwen3VlPrefillLogits;
            }
            try writeF32LeFile(allocator, self.io orelse return error.MissingIo, path, prefill_last_logits.?);
        }
        debugGenerationStage(
            "finished prefill seq_len={d} cached_logits={} greedy_token={}",
            .{ seq_len, prefill_last_logits != null, prefill_greedy_token != null },
        );

        if (self.prompt_cache) |cache| {
            if (config.prompt_cache_enabled and prepared_multimodal_prompt == null and prepared_qwen3vl_prompt == null and decode_state.kv_manager == cache.managerPtr()) {
                if (config.prompt_cache_key) |cache_key| {
                    if (decode_state.sequence_id) |sequence_id| {
                        cache.storeFromSequence(cache_key, token_ids[0..seq_len], sequence_id) catch |err| {
                            // Prompt caching is an optional acceleration. A
                            // failed insertion must not discard valid logits.
                            std.log.warn("prompt cache store failed; continuing without insertion: {s}", .{@errorName(err)});
                        };
                    }
                }
            }
        }

        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                scheduler.beginDecode(lease, seq_len);
            }
        }

        // Compact KV cache after prefill if configured.
        if (config.cache_compaction_ratio) |ratio| {
            if (NativeDecodeState.requiresDeepSeekV4CompressedCache(self.gpt_config)) {
                return error.DeepSeekV4CompressedKvCompactionNotSupported;
            }
            var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
            try decode_runtime.compactKvCache(.{ .target_ratio = ratio });
        }

        const vocab_size = self.gpt_config.vocab_size;
        var finish_reason: []const u8 = "length";
        var tokens_generated: usize = 0;
        var speculative_stats = SpeculativeDecodeStats{};
        speculative_stats.speculation_policy = speculation_policy;
        speculative_stats.speculation_calibration = config.speculation_calibration;
        speculative_stats.mtp_graph_replay_status = gemma4MtpGraphReplayStatusName();
        speculative_stats.mtp_profile.enabled = gemma4MtpProfileEnabled();
        speculative_stats.mtp_profile.sync_enabled = gemma4MtpProfileSyncEnabled();
        if (draft_requested) {
            speculative_stats.mtp_enabled = use_speculative and draft_is_gemma4_mtp;
            speculative_stats.speculation_policy_decision = if (speculation_policy == .off)
                .disabled_off
            else if (mtp_auto_uncalibrated)
                .disabled_uncalibrated
            else if (!use_speculative)
                .disabled_unavailable
            else if (speculation_policy == .force)
                .forced
            else
                .active;
            if (disable_unshared_gemma4_mtp) {
                speculative_stats.mtp_disabled_reason = "target_missing_shared_kv_metadata";
            } else if (speculation_policy == .off) {
                speculative_stats.mtp_disabled_reason = "speculation_policy_off";
            } else if (mtp_auto_uncalibrated) {
                speculative_stats.mtp_disabled_reason = "speculation_calibration_required";
            } else if (mtp_auto_too_short) {
                speculative_stats.mtp_disabled_reason = "mtp_auto_min_tokens";
            } else if (mtp_metal_auto_disabled) {
                speculative_stats.mtp_disabled_reason = "metal_mtp_auto_disabled";
            } else if (target_speculative_verification_unavailable) {
                speculative_stats.mtp_disabled_reason = "a4b_metal_speculative_verify_unqualified";
            }
        }
        const stream_enabled = on_token_fn != null and on_token_ctx != null;
        const final_channel_required = self.gpt_config.usesGemma4Channels();
        // Resolve the channel protocol once for the whole request. Buffered and
        // streaming callers must use the same exact final-header projection;
        // resolving only the generic terminator at completion can expose a
        // malformed trailing private channel.
        const final_channel_end = if (final_channel_required)
            try self.resolveFinalChannelEndTokenId()
        else
            null;
        const turn_end = if (final_channel_end != null)
            try self.resolveTurnEndTokenId()
        else
            null;
        const channel_start = if (final_channel_end != null)
            try self.resolveChannelStartTokenId()
        else
            null;
        const owned_final_channel_header = if (final_channel_end) |marker|
            try self.resolveFinalChannelHeaderTokenIds(marker)
        else
            null;
        defer if (owned_final_channel_header) |ids| allocator.free(ids);
        var streaming_text = StreamingTextState{
            .emitted_text = if (stream_enabled) try allocator.dupe(u8, "") else &.{},
            .final_channel_required = final_channel_required,
            .final_channel_preopened = prompt_opens_public_final_channel,
            .final_channel_end_token_id = final_channel_end,
            .final_channel_header_token_ids = owned_final_channel_header orelse &.{},
            .channel_start_token_id = channel_start,
            .turn_end_token_id = turn_end,
            .final_channel_content_start = if (prompt_opens_public_final_channel) 0 else null,
            .saw_final_channel = prompt_opens_public_final_channel,
        };
        defer if (stream_enabled) allocator.free(streaming_text.emitted_text);
        var penalty_state = SamplingPenaltyState.init(hasSamplingPenalties(config));
        defer penalty_state.deinit(allocator);
        try penalty_state.seedFromHistory(allocator, token_ids[0..seq_len]);

        // Grammar-constrained decoding: initialize JSON FSM or GBNF grammar.
        var json_grammar: ?grammar_mod.JsonGrammar = null;
        var gbnf_grammar: ?grammar_mod.GbnfGrammar = null;
        if (config.grammar) |g| {
            if (std.mem.eql(u8, g, "json")) {
                json_grammar = grammar_mod.JsonGrammar.init();
            } else {
                gbnf_grammar = grammar_mod.GbnfGrammar.parse(allocator, g) catch null;
            }
        }
        defer if (gbnf_grammar) |*gg| gg.deinit();

        const has_any_grammar = json_grammar != null or gbnf_grammar != null;
        var token_table: ?grammar_mod.TokenByteTable = if (has_any_grammar)
            grammar_mod.TokenByteTable.init(allocator, self.tokenizer, vocab_size) catch null
        else
            null;
        defer if (token_table) |*tt| tt.deinit(allocator);

        // Pending token readback consumes the prefill result itself, keeps the
        // token resident, and launches the next decode graph before waiting
        // for the current 4-byte token download. Do not materialize the first
        // token through the ordinary prefill shortcut first: doing so advances
        // `tokens_generated` to one and permanently bypasses the pipelined
        // path for the rest of the request.
        const use_cuda_pending_token_readback = shouldUseCudaPendingTokenReadback(
            self.cb.kind(),
            enableCudaGreedyPendingTokenReadback(),
            isPureGreedyConfig(config),
            use_speculative,
            has_any_grammar,
            self.graph_cache != null or self.compiled_partition_backend != null,
            self.execution_lock != null,
            max_tokens,
        );
        var prefill_decode_complete = false;
        if (!use_speculative and !use_cuda_pending_token_readback) {
            if (try self.tryReturnPrefillFirstToken(
                token_ids,
                &seq_len,
                config,
                &prefill_last_logits,
                &prefill_greedy_token,
                &penalty_state,
                if (token_table) |*tt| tt else null,
                &json_grammar,
                if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                max_tokens,
                prompt_token_count,
                if (stream_enabled) on_token_fn else null,
                if (stream_enabled) on_token_ctx else null,
                if (stream_enabled) &streaming_text else null,
            )) |decode_result| {
                tokens_generated = decode_result.tokens_generated;
                finish_reason = decode_result.finish_reason;
                prefill_decode_complete = tokens_generated >= max_tokens or std.mem.eql(u8, finish_reason, "stop");
                if (!prefill_decode_complete and tokens_generated > 0) {
                    // The first token came from the prompt's final row. Clear
                    // that one-shot output before entering decode so it cannot
                    // be sampled twice, then advance the paged state exactly
                    // as standardDecode would after accepting the token.
                    if (prefill_last_logits) |logits| {
                        allocator.free(logits);
                        prefill_last_logits = null;
                    }
                    prefill_greedy_token = null;
                    var continued_decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
                    const kv_mutation_mutex = batchKvMutationMutex(self.execution_lock, decode_state.kv_lock);
                    if (kv_mutation_mutex) |mutex| platform.sync.lockYielding(mutex);
                    defer if (kv_mutation_mutex) |mutex| mutex.unlock();
                    _ = try continued_decode_runtime.appendGeneratedToken();
                }
            }
        }

        if (use_speculative) {
            const draft_cb = self.draft_cb.?;
            const draft_gpt_config = self.draft_gpt_config.?;
            const use_gemma4_mtp = draft_gpt_config.gemma4_mtp_assistant;
            const mtp_hidden_source = gemma4MtpTargetHiddenSource();

            // Create a temporary draft pipeline (borrows self's allocator/tokenizer)
            var draft_fallback_state = NativeDecodeState.initContiguous(allocator);
            defer draft_fallback_state.deinit();
            const attached_draft_state = self.draft_decode_state;
            const use_contiguous_draft_state = ordinaryMetalGemmaDraftNeedsContiguousState(
                draft_cb.kind(),
                draft_gpt_config,
                if (attached_draft_state) |state| state.isPaged() else false,
            );
            const draft_ds = if (use_contiguous_draft_state)
                &draft_fallback_state
            else
                attached_draft_state orelse &draft_fallback_state;
            var draft_runtime = BorrowedDecodeStateRuntime.init(draft_ds);
            var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);

            // Prefill draft model with the same prompt
            try draft_runtime.notePrefill(seq_len);

            var draft_pipeline = self.makeSpeculativeDraftPipeline(draft_cb, draft_gpt_config);
            var mtp_activation = Gemma4MtpActivationState{
                .allocator = allocator,
                .draft_cb = &draft_pipeline.cb,
            };
            defer mtp_activation.deinit();

            const mtp_top_k_for_resident = gemma4MtpTopKDiagnosticCount();
            const can_use_resident_mtp =
                use_gemma4_mtp and
                gemma4_mtp.deviceResidentDraftAllowed(mtp_top_k_for_resident) and
                self.shouldUseGemma4MtpGreedyDeviceVerifier(
                    config,
                    if (token_table) |*tt| tt else null,
                    &json_grammar,
                    if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                    mtp_top_k_for_resident,
                ) and
                gemma4MtpResidentDraftBackendAllowed(draft_pipeline.cb.kind());
            if (can_use_resident_mtp) debugGemma4Mtp("resident setup enabled", .{});

            // Prefill an ordinary draft model. Gemma 4 MTP assistants are not
            // standalone decoders: they are seeded from target activations and
            // target KV during each draft round.
            if (!use_gemma4_mtp) {
                try setWorkloadProfileRegime(&draft_pipeline.cb, .prefill);
                const draft_ctx = draft_runtime.makeDecodeContext(seq_len, seq_len);
                const draft_prefill_logits = try draft_pipeline.forwardAllLogits(token_ids[0..seq_len], 1, seq_len, &draft_ctx);
                allocator.free(draft_prefill_logits);
            }

            // Also prefill the target if we haven't yet (non-chunked path)
            var mtp_first_outcome: ?SampleOutcome = null;
            if (prefill_last_logits == null) {
                if (use_gemma4_mtp) {
                    if (prefill_greedy_token) |token| {
                        debugGemma4Mtp("first_token_prefill cached_greedy token={d}", .{token});
                        mtp_first_outcome = .{
                            .token = token,
                            .grammar_complete = false,
                        };
                    }
                    if (mtp_first_outcome) |_| {
                        const seeded_from_prefill = if (prefill_last_hidden) |hidden|
                            try self.replaceGemma4MtpActivationFromPrefillHidden(
                                &mtp_activation,
                                hidden,
                                prefill_last_hidden_rows,
                                mtp_hidden_source,
                                can_use_resident_mtp,
                                &draft_pipeline.cb,
                            )
                        else
                            false;
                        if (!seeded_from_prefill) {
                            try self.replaceGemma4MtpActivationFromPrompt(
                                &mtp_activation,
                                token_ids[0..seq_len],
                                seq_len,
                                mtp_hidden_source,
                                can_use_resident_mtp,
                                &draft_pipeline.cb,
                            );
                        }
                    } else {
                        debugGemma4Mtp("first_token_prefill host_logits_fallback", .{});
                        var seed_state = NativeDecodeState.initContiguous(allocator);
                        defer seed_state.deinit();
                        seed_state.configureForGptConfig(self.gpt_config);
                        var seed_runtime = BorrowedDecodeStateRuntime.init(&seed_state);
                        const target_ctx = seed_runtime.makeDecodeContext(seq_len, seq_len);
                        var target_prefill = try self.forwardAllLogitsAndHiddenHost(token_ids[0..seq_len], 1, seq_len, &target_ctx);
                        defer target_prefill.deinit();
                        prefill_last_logits = try allocator.dupe(
                            f32,
                            target_prefill.logits[(seq_len - 1) * vocab_size ..][0..vocab_size],
                        );
                        if (can_use_resident_mtp) {
                            const seeded_from_prefill = if (prefill_last_hidden) |hidden|
                                try self.replaceGemma4MtpActivationFromPrefillHidden(
                                    &mtp_activation,
                                    hidden,
                                    prefill_last_hidden_rows,
                                    mtp_hidden_source,
                                    true,
                                    &draft_pipeline.cb,
                                )
                            else
                                false;
                            if (!seeded_from_prefill) {
                                try self.replaceGemma4MtpActivationFromPrompt(
                                    &mtp_activation,
                                    token_ids[0..seq_len],
                                    seq_len,
                                    mtp_hidden_source,
                                    true,
                                    &draft_pipeline.cb,
                                );
                            }
                        } else {
                            mtp_activation.replaceHost(try self.dupeMtpTargetHiddenRow(&target_prefill, seq_len - 1, mtp_hidden_source));
                        }
                    }
                } else {
                    const target_ctx = decode_runtime.makeDecodeContext(seq_len, seq_len);
                    const target_prefill_logits = try self.forwardAllLogits(token_ids[0..seq_len], 1, seq_len, &target_ctx);
                    defer allocator.free(target_prefill_logits);
                    prefill_last_logits = try allocator.dupe(
                        f32,
                        target_prefill_logits[(seq_len - 1) * vocab_size ..][0..vocab_size],
                    );
                }
            } else if (use_gemma4_mtp and mtp_activation.host == null and mtp_activation.device == null) {
                const seeded_from_prefill = if (prefill_last_hidden) |hidden|
                    try self.replaceGemma4MtpActivationFromPrefillHidden(
                        &mtp_activation,
                        hidden,
                        prefill_last_hidden_rows,
                        mtp_hidden_source,
                        can_use_resident_mtp,
                        &draft_pipeline.cb,
                    )
                else
                    false;
                if (!seeded_from_prefill) {
                    try self.replaceGemma4MtpActivationFromPrompt(
                        &mtp_activation,
                        token_ids[0..seq_len],
                        seq_len,
                        mtp_hidden_source,
                        can_use_resident_mtp,
                        &draft_pipeline.cb,
                    );
                }
            }

            // Use prefill last logits for the first token
            try setWorkloadProfileRegime(&self.cb, .decode);
            const first_outcome = mtp_first_outcome orelse try self.sampleNextToken(
                prefill_last_logits.?,
                config,
                &penalty_state,
                if (token_table) |*tt| tt else null,
                &json_grammar,
                if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
            );
            const first_token = first_outcome.token;
            const first_is_eos = self.shouldStopOnEos(config, first_token);
            var callback_allows_generation = true;
            if (!first_is_eos) {
                token_ids[seq_len] = @intCast(first_token);
                seq_len += 1;
                tokens_generated += 1;
                if (!use_gemma4_mtp) {
                    _ = try decode_runtime.appendGeneratedToken();
                    _ = try draft_runtime.appendGeneratedToken();
                }
                try penalty_state.noteToken(allocator, @intCast(first_token));
                if (stream_enabled) {
                    const keep_streaming = try self.emitDecodedDelta(
                        token_ids[prompt_token_count..seq_len],
                        &streaming_text,
                        on_token_fn.?,
                        on_token_ctx.?,
                    );
                    if (!keep_streaming) {
                        callback_allows_generation = false;
                        finish_reason = "stop";
                    }
                }
            }

            if (first_is_eos or first_outcome.grammar_complete) {
                finish_reason = "stop";
            }

            const enter_speculative_loop = shouldEnterSpeculativeDecodeLoop(
                callback_allows_generation,
                first_is_eos,
                first_outcome.grammar_complete,
            );
            if (use_gemma4_mtp and enter_speculative_loop) {
                mtp_activation.cached_target_choice = try self.materializeAcceptedTokenKvForMtp(token_ids, seq_len, decode_state, mtp_hidden_source);
            }

            // Speculative decode loop
            if (enter_speculative_loop) {
                var mtp_zero_match_rounds: usize = 0;
                const mtp_zero_match_fallback_rounds: usize = if (use_gemma4_mtp)
                    platform.env.getenvUsize("ANTFLY_GEMMA4_MTP_ZERO_MATCH_FALLBACK_ROUNDS") orelse default_mtp_zero_match_fallback_rounds
                else
                    default_mtp_zero_match_fallback_rounds;
                const mtp_acceptance_probe_drafts: usize = if (use_gemma4_mtp)
                    gemma4MtpAcceptanceProbeDrafts()
                else
                    0;
                const mtp_min_acceptance_permille: usize = if (use_gemma4_mtp)
                    gemma4MtpMinAcceptancePermille()
                else
                    0;
                const mtp_adaptive_enabled = use_gemma4_mtp and speculation_policy == .auto and gemma4MtpAdaptiveKEnabled();
                const mtp_policy_max_k: usize = if (use_gemma4_mtp and speculation_policy == .auto)
                    @max(@as(usize, 1), @min(@as(usize, @intCast(config.speculative_k)), gemma4MtpAutoMaxK()))
                else
                    @as(usize, @intCast(config.speculative_k));
                var mtp_adaptive_k = MtpAdaptiveKPolicy.initFromValues(
                    mtp_policy_max_k,
                    mtp_min_acceptance_permille,
                    mtp_acceptance_probe_drafts,
                    mtp_adaptive_enabled,
                    gemma4MtpProbeK(),
                );
                const mtp_effective_zero_match_fallback_rounds =
                    gemma4MtpEffectiveZeroMatchFallbackRounds(mtp_zero_match_fallback_rounds, use_gemma4_mtp and mtp_adaptive_k.enabled);
                const mtp_auto_cost_probe_rounds: usize = if (use_gemma4_mtp and speculation_policy == .auto)
                    gemma4MtpAutoCostProbeRounds()
                else
                    0;
                const mtp_auto_min_accepted_per_round_milli: usize = if (use_gemma4_mtp and speculation_policy == .auto)
                    gemma4MtpAutoMinAcceptedPerRoundMilli()
                else
                    0;
                var mtp_acceptance_gate_checked = false;
                while (tokens_generated < max_tokens) {
                    const remaining = max_tokens - tokens_generated;
                    const step_k = if (use_gemma4_mtp)
                        mtp_adaptive_k.nextK(remaining)
                    else
                        @min(@as(usize, @intCast(config.speculative_k)), remaining);

                    const round_trace = enableGemma4MtpVerifyTrace();
                    const round_started_at = mtpProfileTimestamp(round_trace, self.io);
                    const result = if (use_gemma4_mtp)
                        try self.speculativeDecodeGemma4Mtp(
                            &draft_pipeline,
                            token_ids,
                            &seq_len,
                            decode_state,
                            config,
                            step_k,
                            remaining,
                            &penalty_state,
                            if (token_table) |*tt| tt else null,
                            &json_grammar,
                            if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                            &mtp_activation,
                            &speculative_stats.mtp_profile,
                        )
                    else
                        try self.speculativeDecode(
                            &draft_pipeline,
                            token_ids,
                            &seq_len,
                            decode_state,
                            draft_ds,
                            config,
                            step_k,
                            remaining,
                            &penalty_state,
                            if (token_table) |*tt| tt else null,
                            &json_grammar,
                            if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                        );

                    if (round_trace) {
                        std.debug.print(
                            "mtp-round-trace: round_us={d} drafted={d} accepted={d} bonus={}\n",
                            .{
                                @divTrunc(mtpProfileElapsedNs(true, self.io, round_started_at), std.time.ns_per_us),
                                result.drafted,
                                result.accepted,
                                result.had_bonus,
                            },
                        );
                    }
                    speculative_stats.rounds += 1;
                    speculative_stats.drafted_tokens += result.drafted;
                    speculative_stats.matched_draft_tokens += result.matched_drafts;
                    speculative_stats.accepted_tokens += result.accepted;
                    speculative_stats.correction_tokens += @intFromBool(result.correction_added);
                    speculative_stats.bonus_tokens += @intFromBool(result.had_bonus);
                    speculative_stats.mtp_quality.merge(result.mtp_quality);

                    const mtp_adaptive_decision = if (use_gemma4_mtp)
                        mtp_adaptive_k.observe(result.drafted, result.matched_drafts)
                    else
                        MtpAdaptiveKDecision.keep;

                    if (use_gemma4_mtp and result.drafted > 0) {
                        if (result.matched_drafts == 0) {
                            mtp_zero_match_rounds += 1;
                        } else {
                            mtp_zero_match_rounds = 0;
                        }
                    }

                    tokens_generated += result.accepted;
                    if (stream_enabled and result.accepted > 0) {
                        const keep_streaming = try self.emitDecodedDelta(
                            token_ids[prompt_token_count..seq_len],
                            &streaming_text,
                            on_token_fn.?,
                            on_token_ctx.?,
                        );
                        if (!keep_streaming) {
                            finish_reason = "stop";
                            break;
                        }
                    }

                    if (self.scheduler) |scheduler| {
                        if (self.scheduler_lease) |lease| {
                            scheduler.noteDecodeProgress(lease, tokens_generated);
                        }
                    }

                    if (result.hit_eos or result.hit_grammar_stop) {
                        finish_reason = "stop";
                        break;
                    }

                    const mtp_accepted_per_round_milli: usize = if (speculative_stats.rounds == 0)
                        0
                    else
                        (speculative_stats.accepted_tokens * 1000) / speculative_stats.rounds;
                    const mtp_cost_gate_should_fallback = use_gemma4_mtp and
                        speculation_policy == .auto and
                        mtp_auto_cost_probe_rounds > 0 and
                        mtp_auto_min_accepted_per_round_milli > 0 and
                        speculative_stats.rounds >= mtp_auto_cost_probe_rounds and
                        tokens_generated < max_tokens and
                        mtp_accepted_per_round_milli < mtp_auto_min_accepted_per_round_milli;

                    const mtp_acceptance_gate_should_fallback = use_gemma4_mtp and
                        speculation_policy == .auto and
                        tokens_generated < max_tokens and
                        if (mtp_adaptive_k.enabled)
                            mtp_adaptive_decision == .fallback
                        else
                            (!mtp_acceptance_gate_checked and
                                mtp_acceptance_probe_drafts > 0 and
                                speculative_stats.drafted_tokens >= mtp_acceptance_probe_drafts and
                                speculative_stats.acceptancePermille() < mtp_min_acceptance_permille);

                    if (use_gemma4_mtp and
                        (mtp_cost_gate_should_fallback or mtp_acceptance_gate_should_fallback))
                    {
                        if (!mtp_adaptive_k.enabled) mtp_acceptance_gate_checked = true;
                        speculative_stats.adaptive_fallbacks += 1;
                        if (mtp_acceptance_gate_should_fallback) {
                            speculative_stats.mtp_acceptance_gate_fallbacks += 1;
                        }
                        speculative_stats.speculation_policy_decision = if (mtp_cost_gate_should_fallback)
                            .disabled_slow
                        else
                            .disabled_low_acceptance;
                        debugGemma4Mtp(
                            "auto gate fallback drafted={d} matched={d} acceptance_permille={d} threshold_permille={d} accepted_per_round_milli={d} threshold_accepted_per_round_milli={d}",
                            .{
                                speculative_stats.drafted_tokens,
                                speculative_stats.matched_draft_tokens,
                                speculative_stats.acceptancePermille(),
                                mtp_min_acceptance_permille,
                                mtp_accepted_per_round_milli,
                                mtp_auto_min_accepted_per_round_milli,
                            },
                        );
                        try self.flushPendingGemma4MtpMaterialize(
                            token_ids,
                            seq_len,
                            decode_state,
                            &mtp_activation,
                            mtp_hidden_source,
                            &speculative_stats.mtp_profile,
                        );
                        var fallback_prefill_logits: ?[]f32 = null;
                        defer if (fallback_prefill_logits) |logits| allocator.free(logits);
                        var fallback_greedy_token: ?usize = null;
                        const fallback_started_at = mtpProfileTimestamp(speculative_stats.mtp_profile.enabled, self.io);
                        try setWorkloadProfileRegime(&self.cb, .decode);
                        const fallback = try self.standardDecode(
                            token_ids,
                            &seq_len,
                            decode_state,
                            config,
                            &fallback_prefill_logits,
                            &fallback_greedy_token,
                            &penalty_state,
                            if (token_table) |*tt| tt else null,
                            &json_grammar,
                            if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                            0,
                            max_tokens - tokens_generated,
                            prompt_token_count,
                            if (stream_enabled) on_token_fn else null,
                            if (stream_enabled) on_token_ctx else null,
                            if (stream_enabled) &streaming_text else null,
                        );
                        if (speculative_stats.mtp_profile.enabled) {
                            speculative_stats.mtp_profile.fallback_calls += 1;
                            speculative_stats.mtp_profile.fallback_ns +|= mtpProfileElapsedNs(true, self.io, fallback_started_at);
                        }
                        tokens_generated += fallback.tokens_generated;
                        finish_reason = fallback.finish_reason;
                        break;
                    }

                    if (use_gemma4_mtp and speculation_policy == .auto and mtp_effective_zero_match_fallback_rounds > 0 and mtp_zero_match_rounds >= mtp_effective_zero_match_fallback_rounds and tokens_generated < max_tokens) {
                        speculative_stats.adaptive_fallbacks += 1;
                        speculative_stats.speculation_policy_decision = .disabled_zero_match;
                        try self.flushPendingGemma4MtpMaterialize(
                            token_ids,
                            seq_len,
                            decode_state,
                            &mtp_activation,
                            mtp_hidden_source,
                            &speculative_stats.mtp_profile,
                        );
                        var fallback_prefill_logits: ?[]f32 = null;
                        defer if (fallback_prefill_logits) |logits| allocator.free(logits);
                        var fallback_greedy_token: ?usize = null;
                        const fallback_started_at = mtpProfileTimestamp(speculative_stats.mtp_profile.enabled, self.io);
                        try setWorkloadProfileRegime(&self.cb, .decode);
                        const fallback = try self.standardDecode(
                            token_ids,
                            &seq_len,
                            decode_state,
                            config,
                            &fallback_prefill_logits,
                            &fallback_greedy_token,
                            &penalty_state,
                            if (token_table) |*tt| tt else null,
                            &json_grammar,
                            if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                            0,
                            max_tokens - tokens_generated,
                            prompt_token_count,
                            if (stream_enabled) on_token_fn else null,
                            if (stream_enabled) on_token_ctx else null,
                            if (stream_enabled) &streaming_text else null,
                        );
                        if (speculative_stats.mtp_profile.enabled) {
                            speculative_stats.mtp_profile.fallback_calls += 1;
                            speculative_stats.mtp_profile.fallback_ns +|= mtpProfileElapsedNs(true, self.io, fallback_started_at);
                        }
                        tokens_generated += fallback.tokens_generated;
                        finish_reason = fallback.finish_reason;
                        break;
                    }

                    if (result.accepted == 0) break; // safety valve
                }
            }
        }

        // Standard autoregressive loop (skipped when speculative decoding was used above)
        if (!use_speculative and !prefill_decode_complete) {
            try setWorkloadProfileRegime(&self.cb, .decode);
            debugGenerationStage(
                "entering standard decode max_tokens={d} prompt_token_count={d}",
                .{ max_tokens, prompt_token_count },
            );
            const decode_result = try self.standardDecode(
                token_ids,
                &seq_len,
                decode_state,
                config,
                &prefill_last_logits,
                &prefill_greedy_token,
                &penalty_state,
                if (token_table) |*tt| tt else null,
                &json_grammar,
                if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                tokens_generated,
                max_tokens,
                prompt_token_count,
                if (stream_enabled) on_token_fn else null,
                if (stream_enabled) on_token_ctx else null,
                if (stream_enabled) &streaming_text else null,
            );
            tokens_generated = decode_result.tokens_generated;
            finish_reason = decode_result.finish_reason;
            debugGenerationStage(
                "standard decode returned tokens_generated={d} finish_reason={s} seq_len={d}",
                .{ tokens_generated, finish_reason, seq_len },
            );
        }

        // Radix mode also learns reusable conversation continuations. The
        // final sampled token has a logical KV slot but is not materialized
        // until the next autoregressive forward pass, so retain only the
        // prefix ending immediately before it. storeGeneratedPrefixFromSequence
        // page-aligns that valid prefix and is a no-op for non-radix modes.
        if (tokens_generated > 0) {
            if (self.prompt_cache) |cache| {
                const cache_eligible =
                    config.prompt_cache_enabled and
                    config.prompt_cache_key != null and
                    prepared_multimodal_prompt == null and prepared_qwen3vl_prompt == null and
                    !use_speculative and
                    config.cache_compaction_ratio == null and
                    self.compiled_partition_backend == null and
                    !NativeDecodeState.requiresDeepSeekV4CompressedCache(self.gpt_config) and
                    !self.gpt_config.isQwen35() and
                    decode_state.isPaged() and
                    decode_state.kv_manager == cache.managerPtr();
                if (cache_eligible) {
                    if (decode_state.sequence_id) |sequence_id| {
                        cache.storeGeneratedPrefixFromSequence(
                            config.prompt_cache_key.?,
                            token_ids[0 .. seq_len - 1],
                            sequence_id,
                        ) catch |err| {
                            // A cache admission failure must not discard an
                            // otherwise complete generation response.
                            std.log.warn("radix continuation cache store failed: {s}", .{@errorName(err)});
                        };
                    }
                }
            }
        }

        // Decode only the generated tokens. By default channel-aware models
        // project public text and token IDs from the same slice. The explicit
        // local diagnostic opt-in may retain raw IDs while text stays public.
        const gen_start = prompt_token_count;
        const final_channel_end_token_id = streaming_text.final_channel_end_token_id;
        const turn_end_token_id = streaming_text.turn_end_token_id;
        const projected_gen_token_ids = finalResponseTokenSlice(
            token_ids[gen_start..seq_len],
            streaming_text.final_channel_required,
            streaming_text.final_channel_preopened,
            final_channel_end_token_id,
            streaming_text.final_channel_header_token_ids,
            streaming_text.channel_start_token_id,
            turn_end_token_id,
        );
        const raw_gen_token_ids = token_ids[gen_start..seq_len];
        const reasoning_gen_token_ids = reasoningResponseTokenSlice(
            raw_gen_token_ids,
            streaming_text.final_channel_required,
            streaming_text.final_channel_preopened,
            final_channel_end_token_id,
            streaming_text.final_channel_header_token_ids,
            streaming_text.channel_start_token_id,
            turn_end_token_id,
        );
        const result_gen_token_ids = generationResultTokenSlice(
            raw_gen_token_ids,
            projected_gen_token_ids,
            self.return_raw_token_ids,
        );
        const result_gen_ids = try allocator.alloc(i32, result_gen_token_ids.len);
        errdefer allocator.free(result_gen_ids);
        for (result_gen_token_ids, 0..) |token_id, idx| result_gen_ids[idx] = @intCast(token_id);

        const text_gen_ids = if (self.return_raw_token_ids) blk: {
            const ids = try allocator.alloc(i32, projected_gen_token_ids.len);
            for (projected_gen_token_ids, 0..) |token_id, idx| ids[idx] = @intCast(token_id);
            break :blk ids;
        } else result_gen_ids;
        defer if (self.return_raw_token_ids) allocator.free(text_gen_ids);

        // Channel-protocol debugging: report whether the final-channel marker
        // tokens resolved against this tokenizer. A null marker means the
        // projection strips every generated token regardless of model output.
        // (Raw ids/text are available via return_raw_token_ids.)
        if (getenvBool("ANTFLY_GENERATION_DEBUG_RAW_OUTPUT")) {
            std.debug.print(
                "channel_protocol: required={} channel_end={?d} channel_start={?d} turn_end={?d} header_len={d} saw_final={}\n",
                .{
                    streaming_text.final_channel_required,
                    streaming_text.final_channel_end_token_id,
                    streaming_text.channel_start_token_id,
                    streaming_text.turn_end_token_id,
                    streaming_text.final_channel_header_token_ids.len,
                    streaming_text.saw_final_channel,
                },
            );
        }

        const text_decode_started_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        const text = try self.tokenizer.decode(allocator, text_gen_ids);
        errdefer allocator.free(text);
        const reasoning_text = if (reasoning_gen_token_ids.len > 0) blk: {
            const reasoning_ids = try allocator.alloc(i32, reasoning_gen_token_ids.len);
            defer allocator.free(reasoning_ids);
            for (reasoning_gen_token_ids, 0..) |token_id, idx| reasoning_ids[idx] = @intCast(token_id);
            break :blk try self.tokenizer.decode(allocator, reasoning_ids);
        } else null;
        errdefer if (reasoning_text) |reasoning| allocator.free(reasoning);
        if (stream_enabled) {
            _ = emitCompletedProjectionDelta(
                &streaming_text,
                on_token_ctx.?,
                on_token_fn.?,
                text,
            );
        }
        const finished_generate_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        const timing_ms: ?GenerationTimingMs = if (self.io != null) .{
            .prompt_format = timestampDurationMillis(started_at, formatted_prompt_at),
            .tokenize = timestampDurationMillis(formatted_prompt_at, encoded_prompt_at),
            .multimodal_prepare = timestampDurationMillis(encoded_prompt_at, runtime_prepare_started_at),
            .runtime_prepare = timestampDurationMillis(runtime_prepare_started_at, prefill_started_at),
            .prefill = timestampDurationMillis(prefill_started_at, finished_prefill_at),
            .decode = timestampDurationMillis(finished_prefill_at, finished_generate_at),
            .text_decode = timestampDurationMillis(text_decode_started_at, finished_generate_at),
            .total = timestampDurationMillis(started_at, finished_generate_at),
        } else null;
        if (self.print_timing and timing_ms != null) {
            const timing = timing_ms.?;
            std.debug.print(
                "generate_timing_ms: prompt_format={d} tokenize={d} multimodal_prepare={d} runtime_prepare={d} prefill={d} decode={d} text_decode={d} total={d}\n",
                .{
                    timing.prompt_format,
                    timing.tokenize,
                    timing.multimodal_prepare,
                    timing.runtime_prepare,
                    timing.prefill,
                    timing.decode,
                    timing.text_decode,
                    timing.total,
                },
            );
        }
        const final_mtp_auto_cost_probe_rounds: usize = if (speculative_stats.mtp_enabled and speculation_policy == .auto)
            gemma4MtpAutoCostProbeRounds()
        else
            0;
        const final_mtp_auto_min_accepted_per_round_milli: usize = if (speculative_stats.mtp_enabled and speculation_policy == .auto)
            gemma4MtpAutoMinAcceptedPerRoundMilli()
        else
            0;
        if (speculative_stats.mtp_enabled and
            speculation_policy == .auto and
            speculative_stats.speculation_policy_decision == .active and
            final_mtp_auto_cost_probe_rounds > 0 and
            speculative_stats.rounds < final_mtp_auto_cost_probe_rounds)
        {
            speculative_stats.speculation_policy_decision = .disabled_insufficient_probe;
            speculative_stats.mtp_disabled_reason = "mtp_auto_insufficient_cost_probe";
        } else if (speculative_stats.mtp_enabled and
            speculation_policy == .auto and
            speculative_stats.speculation_policy_decision == .active and
            final_mtp_auto_cost_probe_rounds > 0 and
            final_mtp_auto_min_accepted_per_round_milli > 0 and
            speculative_stats.rounds >= final_mtp_auto_cost_probe_rounds)
        {
            const accepted_per_round_milli = (speculative_stats.accepted_tokens * 1000) / speculative_stats.rounds;
            if (accepted_per_round_milli < final_mtp_auto_min_accepted_per_round_milli) {
                speculative_stats.speculation_policy_decision = .disabled_slow;
                speculative_stats.mtp_disabled_reason = "mtp_auto_cost_probe_slow";
            }
        }
        return .{
            .text = text,
            .reasoning_text = reasoning_text,
            .token_ids = result_gen_ids,
            .prompt_tokens = prompt_token_count,
            .tokens_used = tokens_generated,
            .finish_reason = finish_reason,
            .cached_prompt_tokens = cached_prompt_tokens,
            .timing_ms = timing_ms,
            .speculative = if (use_speculative or draft_requested) speculative_stats else null,
            .allocator = allocator,
        };
    }

    const PrefillOutput = struct {
        last_logits: ?[]f32 = null,
        greedy_token: ?usize = null,
        last_hidden: ?ops.CT = null,
        last_hidden_rows: usize = 0,
    };

    /// Prepared decoder tails expose the input to final RMSNorm. Gemma4 MTP's
    /// `.final` activation contract is the post-norm row used by verification.
    fn capturePreparedTailPostNormHidden(
        cb: *const ComputeBackend,
        capture: bool,
        pre_norm_hidden: ops.CT,
        final_norm_slot: usize,
        hidden_size: usize,
        eps: f32,
    ) !?ops.CT {
        if (!capture) return null;
        return cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = final_norm_slot,
            .input = pre_norm_hidden,
            .hidden_size = hidden_size,
            .eps = eps,
        });
    }

    fn tryCudaPreparedTailPrefillGreedy(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?PrefillOutput {
        if (!enableCudaPreparedTailPrefillGreedy()) return null;
        if (self.cb.kind() != .cuda) return null;
        if (decode_context.attention_mode != .paged_prefill or decode_context.query_sequence_len != input_ids.len) return null;

        const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            &self.cb,
            self.allocator,
            self.gpt_config,
            self.gpt_config.num_hidden_layers,
            input_ids,
            seq_len,
            decode_context,
            false,
        )) orelse return null;
        var owns_hidden = true;
        errdefer if (owns_hidden) self.cb.free(tail.final_hidden);

        var greedy = (try decoder_tail_runtime.forwardGreedyTokenTensorFromFinalHidden(
            &self.cb,
            self.allocator,
            self.gpt_config,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
        )) orelse {
            self.cb.free(tail.final_hidden);
            return null;
        };
        if (greedy.token_tensor) |token_tensor| {
            self.cb.free(token_tensor);
            greedy.token_tensor = null;
        }

        const last_hidden = try capturePreparedTailPostNormHidden(
            &self.cb,
            capture_last_hidden,
            tail.final_hidden,
            tail.final_norm_slot,
            self.gpt_config.hidden_size,
            self.gpt_config.norm_eps,
        );
        if (capture_last_hidden and last_hidden == null) {
            self.cb.free(tail.final_hidden);
            owns_hidden = false;
            return null;
        }
        self.cb.free(tail.final_hidden);
        owns_hidden = false;
        debugGenerationStage(
            "executePrefill prepared_tail_greedy token={d} capture_hidden={}",
            .{ greedy.token_id, capture_last_hidden },
        );
        return .{
            .greedy_token = greedy.token_id,
            .last_hidden = last_hidden,
            .last_hidden_rows = if (last_hidden != null) 1 else 0,
        };
    }

    fn metalPreparedTailOwnsWholePrefill(
        input_len: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) bool {
        return decode_context.attention_mode == .paged_prefill and
            decode_context.total_sequence_len == seq_len and
            decode_context.query_sequence_len == input_len and
            decode_context.kv_sequence_len == input_len and
            decode_context.kv_position_offset == 0 and
            input_len == seq_len;
    }

    /// Metal analog of the CUDA prepared-tail prefill: the planned prefill
    /// frame plus a prepared-slot greedy tail, capturing the last POST-final-
    /// norm hidden row for Gemma4 MTP seeding. Without this the MTP seed path
    /// re-runs the whole prompt through the generic per-op forward (a second
    /// prompt-length forward that scales with prompt size). Returns null on
    /// any miss so the caller falls back to the plain logits prefill.
    fn tryMetalPreparedTailPrefillGreedy(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?PrefillOutput {
        if (self.cb.kind() != .metal) return null;
        if (!gemma4MtpMetalPrefillHiddenCaptureEnabled()) return null;
        if (!metalPreparedTailOwnsWholePrefill(input_ids.len, seq_len, decode_context)) return null;

        const prepared = self.cb.decoderRuntimePrepareOrReuseFamily(
            self.allocator,
            self.gpt_config,
            seq_len,
            self.gpt_config.num_hidden_layers,
        ) catch return null;
        if (!prepared.prepared) return null;

        const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            &self.cb,
            self.allocator,
            self.gpt_config,
            self.gpt_config.num_hidden_layers,
            input_ids,
            seq_len,
            decode_context,
            true,
        )) orelse return null;
        defer self.cb.free(tail.final_hidden);

        const last_hidden = try capturePreparedTailPostNormHidden(
            &self.cb,
            capture_last_hidden,
            tail.final_hidden,
            tail.final_norm_slot,
            self.gpt_config.hidden_size,
            self.gpt_config.norm_eps,
        );
        errdefer if (last_hidden) |hidden| self.cb.free(hidden);
        if (capture_last_hidden and last_hidden == null) return null;

        if (tail.greedy_token_id) |greedy_token| {
            debugGenerationStage(
                "executePrefill metal_prepared_tail_greedy token={d} capture_hidden={}",
                .{ greedy_token, capture_last_hidden },
            );
            return .{
                .greedy_token = @intCast(greedy_token),
                .last_hidden = last_hidden,
                .last_hidden_rows = if (last_hidden != null) 1 else 0,
            };
        }

        const logits = if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
            &self.cb,
            self.gpt_config,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
            tail.final_lm_head_slot,
        )) |prepared_logits|
            prepared_logits
        else
            (try decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
                &self.cb,
                self.gpt_config,
                tail.final_hidden,
                .rms,
                tail.final_norm_slot,
            )) orelse {
                if (last_hidden) |hidden| self.cb.free(hidden);
                return null;
            };
        defer self.cb.free(logits);
        const last_logits = try self.cb.toFloat32(logits, self.allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.gpt_config, last_logits);
        debugGenerationStage(
            "executePrefill metal_prepared_tail_logits capture_hidden={}",
            .{capture_last_hidden},
        );
        return .{
            .last_logits = last_logits,
            .last_hidden = last_hidden,
            .last_hidden_rows = if (last_hidden != null) 1 else 0,
        };
    }

    /// Run the prefill phase: process prompt tokens through the model,
    /// either chunked (for paged KV) or in one pass. Returns the logits
    /// for the last prompt position, or null if handled by the scheduler.
    fn executePrefill(
        self: *NativeGenerationPipeline,
        prompt_ids: []const i64,
        seq_len: usize,
        prefilled_tokens: usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        allow_resident_greedy_token: bool,
        capture_last_hidden: bool,
    ) !PrefillOutput {
        const allocator = self.allocator;
        var prefill_last_logits: ?[]f32 = null;
        var prefill_greedy_token: ?usize = null;
        var prefill_last_hidden: ?ops.CT = null;
        var prefill_last_hidden_rows: usize = 0;
        errdefer if (prefill_last_hidden) |hidden| self.cb.free(hidden);
        const has_scheduler_peers = if (self.scheduler) |scheduler| scheduler.shouldCoordinateCurrentRequests() else false;
        // A sole CUDA request keeps the device-greedy fast path. If a peer
        // arrives after this check, execution_lock still serializes the
        // direct forward against scheduler-driven work.
        const use_cuda_prefill_greedy_token = (self.execution_lock == null or !has_scheduler_peers) and
            shouldUseCudaPrefillGreedyToken(
                self.cb.kind(),
                config,
                allow_resident_greedy_token,
                self.gpt_config.suppressTokenIds().len > 0,
                enableCudaPrefillGreedyToken(),
            );
        // Keep the prepared greedy route MTP-only. Target-only Metal takes the
        // final-row logits path below without touching prepared-tail state.
        const use_metal_prefill_greedy_token = self.cb.kind() == .metal and
            gemma4MtpMetalPrefillHiddenCaptureEnabled() and
            capture_last_hidden and
            allow_resident_greedy_token and
            isPureGreedyConfig(config) and
            config.grammar == null;
        const use_prefill_greedy_token = use_cuda_prefill_greedy_token or use_metal_prefill_greedy_token;
        const use_metal_target_last_logits = shouldUseMetalGemmaTargetLastLogits(
            self.cb.kind(),
            &self.gpt_config,
            capture_last_hidden,
        );
        const can_use_typed_prefill_outputs = self.graph_cache == null and self.compiled_partition_backend == null;
        const prefer_cuda_direct_last_logits = can_use_typed_prefill_outputs and self.cb.kind() == .cuda;
        debugGenerationStage(
            "executePrefill enter seq_len={d} prefilled={d} query_len={d} paged={} scheduler={} compiled_whole_model={} prefill_greedy={}",
            .{
                seq_len,
                prefilled_tokens,
                prompt_ids.len,
                decode_state.isPaged(),
                self.scheduler != null,
                self.compiled_partition_backend != null and self.compiled_attachment_target == .whole_model and self.graph_cache != null,
                use_prefill_greedy_token,
            },
        );
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (self.gpt_config.isQwen35() and decode_state.isPaged()) {
            try decode_state.ensureQwen35LinearCache(self.gpt_config);
            decode_state.resetQwen35LinearCache();
        }

        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                scheduler.notePrefillProgress(lease, prefilled_tokens, seq_len);
            }
        }

        if (self.compiled_partition_backend != null and self.compiled_attachment_target == .whole_model and self.graph_cache != null) {
            if (prefilled_tokens != 0) return error.UnsupportedShape;
            const whole_model_kv_policy = generationKvCapacityPolicyForRoute(
                if (self.compiled_partition_backend == .metal and
                    self.cb.kind() == .metal and decode_state.isPaged() and
                    self.kv_dtype != null)
                    .metal_whole_model
                else
                    .standard,
                self.gpt_config,
                config,
                self.kv_dtype orelse .f16,
            );
            const bounded_swa_prefill = whole_model_kv_policy == .split_swa_ring;
            const prefill_chunk_size = if (bounded_swa_prefill)
                wholeModelPrefillChunkSize(
                    config.prefill_chunk_size,
                    if (self.scheduler_lease) |lease| lease.prefill_chunk_size else 0,
                    prompt_ids.len,
                )
            else
                prompt_ids.len;
            const max_speculative_rows = @min(
                @as(usize, @intCast(config.speculative_k)),
                @as(usize, runtime.tier.memory.generation_max_speculative_k),
            ) + 1;
            decode_state.configureKvMaxInflightTokens(
                @max(prefill_chunk_size, max_speculative_rows),
                bounded_swa_prefill,
            );
            debugGenerationStage(
                "executePrefill whole-model fast path seq_len={d} chunk_size={d} ring={}",
                .{ seq_len, prefill_chunk_size, decode_state.allow_swa_ring },
            );
            var processed: usize = 0;
            while (processed < prompt_ids.len) {
                const chunk_end = @min(prompt_ids.len, processed + prefill_chunk_size);
                const chunk = prompt_ids[processed..chunk_end];
                const total_chunk_end = prefilled_tokens + chunk_end;
                const decode_context = decode_runtime.preparePrefill(total_chunk_end, chunk.len) catch |err| {
                    debugGenerationStage(
                        "executePrefill whole-model KV advance failed chunk_start={d} chunk_end={d} err={s}",
                        .{ processed, chunk_end, @errorName(err) },
                    );
                    return err;
                };
                const is_last_chunk = chunk_end == prompt_ids.len;
                var output = (graph_mod.execution.graphForwardCompiledModelOutput(
                    self,
                    self.graph_cache.?,
                    chunk,
                    1,
                    total_chunk_end,
                    &decode_context,
                ) catch |err| {
                    debugGenerationStage(
                        "executePrefill whole-model chunk failed chunk_start={d} chunk_end={d} err={s}",
                        .{ processed, chunk_end, @errorName(err) },
                    );
                    return err;
                }) orelse {
                    debugGenerationStage(
                        "executePrefill whole-model chunk declined chunk_start={d} chunk_end={d}",
                        .{ processed, chunk_end },
                    );
                    return error.MissingCompiledModelRuntime;
                };
                defer output.deinit(allocator);
                if (is_last_chunk and allow_resident_greedy_token and
                    isPureGreedyConfig(config) and config.grammar == null)
                {
                    const token = try output.greedyToken(allocator, self.gpt_config.vocab_size);
                    if (token < 0) return error.InvalidModelOutput;
                    if (self.scheduler) |scheduler| {
                        if (self.scheduler_lease) |lease| {
                            scheduler.notePrefillProgress(lease, total_chunk_end, seq_len);
                            scheduler.finishTurn(lease, .prefill);
                        }
                    }
                    return .{ .greedy_token = @intCast(token) };
                } else if (is_last_chunk) {
                    prefill_last_logits = try output.takeHostLogits(allocator);
                }
                processed = chunk_end;
                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        scheduler.notePrefillProgress(lease, total_chunk_end, seq_len);
                        if (is_last_chunk) scheduler.finishTurn(lease, .prefill);
                    }
                }
            }
            return .{ .last_logits = prefill_last_logits };
        }

        if (decode_state.isPaged() and seq_len > 1) {
            const query_len = prompt_ids.len;
            const configured_plan = config.prefill_chunk_plan;
            const leased_plan = if (self.scheduler_lease) |lease|
                if (lease.prefill_chunk_plan.isAdmitted()) lease.prefill_chunk_plan else null
            else
                null;
            if (configured_plan != null and leased_plan != null and
                configured_plan.?.fingerprint != leased_plan.?.fingerprint)
            {
                return error.InvalidPrefillChunkPlan;
            }
            const borrowed_plan = configured_plan orelse leased_plan;
            const admitted_plan: ?runtime.scheduler.native_generate.PrefillChunkPlan = if (borrowed_plan) |plan| blk: {
                try plan.validate();
                if (prefilled_tokens != 0) {
                    std.log.info(
                        "prefill_chunk_plan: status=bypassed reason=prompt_cache_hit fingerprint={x} cached_tokens={d}",
                        .{ plan.fingerprint, prefilled_tokens },
                    );
                    break :blk null;
                }
                if (plan.prompt_tokens != query_len) {
                    std.log.info(
                        "prefill_chunk_plan: status=bypassed reason=prompt_token_mismatch fingerprint={x} plan_prompt_tokens={d} pipeline_prompt_tokens={d}",
                        .{ plan.fingerprint, plan.prompt_tokens, query_len },
                    );
                    break :blk null;
                }
                const tail = plan.tail();
                std.log.info(
                    "prefill_chunk_plan: status=execute fingerprint={x} selection={s} fairness={s} prompt_tokens={d} chunks={d} max_chunk_rows={d} max_scratch_bytes={d} forced_drain_budget_bytes={d} tail_rows={d} tail_weight_route={s}",
                    .{
                        plan.fingerprint,
                        @tagName(plan.selection),
                        @tagName(plan.fairness),
                        plan.prompt_tokens,
                        plan.chunks.len,
                        plan.max_chunk_rows,
                        plan.max_scratch_bytes,
                        plan.forced_drain_budget_bytes,
                        tail.rows,
                        @tagName(tail.weight_route_hint),
                    },
                );
                break :blk plan;
            } else null;
            var current_chunk_size = blk: {
                if (admitted_plan) |plan| break :blk plan.max_chunk_rows;
                const scheduler_chunk = if (self.scheduler_lease) |lease| lease.prefill_chunk_size else 0;
                if (config.prefill_chunk_size > 0 and scheduler_chunk > 0) {
                    break :blk @min(config.prefill_chunk_size, scheduler_chunk);
                }
                if (config.prefill_chunk_size > 0) break :blk config.prefill_chunk_size;
                if (scheduler_chunk > 0) break :blk scheduler_chunk;
                break :blk query_len;
            };
            current_chunk_size = @max(@min(current_chunk_size, query_len), 1);
            var first_token_coalesced = false;
            if (borrowed_plan == null) {
                const coalesced_chunk_size = coalescedPrefillChunkSizeForFirstToken(
                    self.cb.kind(),
                    isPureGreedyConfig(config) and config.grammar == null,
                    !allow_resident_greedy_token,
                    enableCudaPrefillFirstToken(),
                    query_len,
                    current_chunk_size,
                    cudaPrefillFirstTokenCoalesceTokenLimit(),
                );
                first_token_coalesced = coalesced_chunk_size > current_chunk_size;
                if (first_token_coalesced) {
                    debugFirstToken(
                        "prefill_first_token legacy_coalesced_prefill_chunk_size from={d} to={d} seq_len={d}",
                        .{ current_chunk_size, coalesced_chunk_size, query_len },
                    );
                    current_chunk_size = coalesced_chunk_size;
                }
            }
            const max_speculative_rows = @min(@as(usize, @intCast(config.speculative_k)), 16) + 1;
            decode_state.configureKvMaxInflightTokens(
                @max(current_chunk_size, max_speculative_rows),
                self.cb.kind() == .metal and
                    prefilled_tokens == 0 and
                    !config.prompt_cache_enabled and
                    config.cache_compaction_ratio == null,
            );
            var processed: usize = 0;
            var plan_chunk_index: usize = 0;
            while (processed < query_len) {
                var direct_prefill_turn_acquired = false;
                defer if (direct_prefill_turn_acquired) {
                    self.scheduler.?.finishTurn(self.scheduler_lease.?, .prefill);
                };
                const planned_chunk = if (admitted_plan) |plan| blk: {
                    if (plan_chunk_index >= plan.chunks.len) return error.InvalidPrefillChunkPlan;
                    break :blk plan.chunks[plan_chunk_index];
                } else runtime.scheduler.native_generate.PrefillChunk{
                    .rows = blk: {
                        const scheduler_chunk = if (self.scheduler_lease) |lease| lease.prefill_chunk_size else current_chunk_size;
                        break :blk @max(@min(
                            current_chunk_size,
                            schedulerChunkForPrefillIteration(scheduler_chunk, current_chunk_size, first_token_coalesced),
                        ), 1);
                    },
                };
                const chunk_size = planned_chunk.rows;
                const chunk_end = @min(query_len, processed + chunk_size);
                if (admitted_plan != null and chunk_end - processed != chunk_size)
                    return error.InvalidPrefillChunkPlan;
                const total_chunk_end = prefilled_tokens + chunk_end;
                const chunk = prompt_ids[processed..chunk_end];
                const output_intent = prefillOutputIntent(
                    total_chunk_end == seq_len,
                    use_prefill_greedy_token,
                    capture_last_hidden,
                );
                debugGenerationStage(
                    "executePrefill chunk start processed={d} chunk_len={d} total_chunk_end={d} current_chunk_size={d} output={s} weight_route_hint={s}",
                    .{ processed, chunk.len, total_chunk_end, current_chunk_size, @tagName(output_intent), @tagName(planned_chunk.weight_route_hint) },
                );
                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        if (self.io) |io| {
                            if (!prefillFinalChunkNeedsDirectExecution(
                                total_chunk_end,
                                seq_len,
                                output_intent,
                                prefer_cuda_direct_last_logits,
                                use_metal_target_last_logits,
                            )) {
                                prefill_last_logits = self.runScheduledPrefillBatch(scheduler, lease, io, decode_state, chunk, total_chunk_end, chunk.len, total_chunk_end == seq_len) catch |err| {
                                    if (admitted_plan == null and err == error.MemoryBudgetExceeded and chunk_size > 1) {
                                        current_chunk_size = @max(chunk_size / 2, 1);
                                        continue;
                                    }
                                    return err;
                                };
                                processed = chunk_end;
                                if (admitted_plan != null) plan_chunk_index += 1;
                                scheduler.notePrefillProgress(lease, prefilled_tokens + processed, seq_len);
                                continue;
                            }
                        }
                    }
                }

                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        if (self.io) |io| {
                            try scheduler.awaitTurn(lease, .prefill, io);
                            direct_prefill_turn_acquired = true;
                        }
                    }
                }
                const direct_execution_mutex = directPrefillExecutionMutex(
                    false,
                    use_cuda_prefill_greedy_token or
                        (total_chunk_end == seq_len and prefer_cuda_direct_last_logits),
                    use_metal_prefill_greedy_token,
                    self.execution_lock,
                );
                if (direct_execution_mutex) |mutex| {
                    platform.sync.lockYielding(mutex);
                }
                defer if (direct_execution_mutex) |mutex| mutex.unlock();
                try decode_runtime.appendPrefillChunk(chunk.len);
                const decode_context = decode_runtime.makeDecodeContext(total_chunk_end, chunk.len);
                const metal_prepared: ?PrefillOutput = if (processed == 0 and total_chunk_end == seq_len and use_metal_prefill_greedy_token)
                    try self.tryMetalPreparedTailPrefillGreedy(chunk, total_chunk_end, &decode_context, capture_last_hidden)
                else
                    null;
                if (metal_prepared) |prepared| {
                    prefill_last_logits = prepared.last_logits;
                    prefill_greedy_token = prepared.greedy_token;
                    prefill_last_hidden = prepared.last_hidden;
                    prefill_last_hidden_rows = prepared.last_hidden_rows;
                } else if (output_intent == .greedy_token or output_intent == .greedy_token_and_hidden) {
                    if (try self.tryCudaPreparedTailPrefillGreedy(chunk, total_chunk_end, &decode_context, capture_last_hidden)) |prepared| {
                        prefill_greedy_token = prepared.greedy_token;
                        prefill_last_hidden = prepared.last_hidden;
                        prefill_last_hidden_rows = prepared.last_hidden_rows;
                    } else if (capture_last_hidden) {
                        var greedy_hidden = gpt_arch.forwardGreedyLastTokenWithFinalHidden(&self.cb, allocator, self.gpt_config, chunk, 1, total_chunk_end, &decode_context) catch |err| {
                            if (try retryDirectPrefillAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, chunk_size, &current_chunk_size, admitted_plan == null, err)) continue;
                            return err;
                        };
                        prefill_greedy_token = greedy_hidden.token_id;
                        if (greedy_hidden.token_tensor) |token| {
                            self.cb.free(token);
                            greedy_hidden.token_tensor = null;
                        }
                        prefill_last_hidden = greedy_hidden.hidden;
                        prefill_last_hidden_rows = greedy_hidden.rows;
                    } else {
                        prefill_greedy_token = gpt_arch.forwardGreedyLastToken(&self.cb, allocator, self.gpt_config, chunk, 1, total_chunk_end, &decode_context) catch |err| {
                            if (try retryDirectPrefillAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, chunk_size, &current_chunk_size, admitted_plan == null, err)) continue;
                            return err;
                        };
                    }
                    debugGenerationStage(
                        "executePrefill captured greedy token={d}",
                        .{prefill_greedy_token.?},
                    );
                } else if (output_intent == .last_logits_and_hidden or
                    (output_intent == .last_logits and use_metal_target_last_logits))
                {
                    var target_hidden = self.forwardFinalHiddenDevice(
                        chunk,
                        1,
                        total_chunk_end,
                        prefilled_tokens,
                        &decode_context,
                    ) catch |err| {
                        if (try retryDirectPrefillAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, chunk_size, &current_chunk_size, admitted_plan == null, err)) continue;
                        return err;
                    };
                    var keep_target_hidden = false;
                    defer {
                        if (!keep_target_hidden) self.cb.free(target_hidden.hidden);
                        if (target_hidden.pre_norm_hidden) |pre_norm| self.cb.free(pre_norm);
                        if (target_hidden.prepared_tail_choices) |choices| target_hidden.choices_allocator.?.free(choices);
                    }
                    const last_hidden = try self.cb.sliceRows2D(
                        allocator,
                        target_hidden.hidden,
                        target_hidden.rows - 1,
                        1,
                        self.gpt_config.hidden_size,
                    );
                    defer self.cb.free(last_hidden);
                    const lm_w = try gpt_arch.getLmHeadWeight(&self.cb, self.gpt_config);
                    defer self.cb.free(lm_w);
                    const last_logits_device = try self.cb.linearNoBias(
                        last_hidden,
                        lm_w,
                        1,
                        self.gpt_config.hidden_size,
                        self.gpt_config.vocab_size,
                    );
                    defer self.cb.free(last_logits_device);
                    const last_logits = try self.cb.toFloat32(last_logits_device, allocator);
                    gpt_arch.applyFinalLogitSoftcapInPlace(self.gpt_config, last_logits);
                    prefill_last_logits = last_logits;
                    if (capture_last_hidden) {
                        prefill_last_hidden = target_hidden.hidden;
                        prefill_last_hidden_rows = target_hidden.rows;
                        keep_target_hidden = true;
                    }
                    if (capture_last_hidden) {
                        debugGenerationStage(
                            "executePrefill captured final logits and MTP hidden vocab_size={d}",
                            .{self.gpt_config.vocab_size},
                        );
                    } else {
                        debugGenerationStage(
                            "executePrefill captured target final-row logits vocab_size={d}",
                            .{self.gpt_config.vocab_size},
                        );
                    }
                } else if (output_intent == .last_logits and prefer_cuda_direct_last_logits) {
                    prefill_last_logits = self.forwardLastLogits(chunk, 1, total_chunk_end, &decode_context) catch |err| {
                        if (try retryDirectPrefillAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, chunk_size, &current_chunk_size, admitted_plan == null, err)) continue;
                        return err;
                    };
                    debugGenerationStage(
                        "executePrefill captured CUDA final-row logits vocab_size={d}",
                        .{self.gpt_config.vocab_size},
                    );
                } else if (output_intent == .kv_only and can_use_typed_prefill_outputs) {
                    const hidden_only_result = if (self.cb.kind() == .cuda and cudaReplayLastLogitsEnabled())
                        gpt_arch.forwardHiddenOnlyWithCudaReplay(
                            &self.cb,
                            allocator,
                            self.gpt_config,
                            chunk,
                            1,
                            total_chunk_end,
                            &decode_context,
                            "gpt.prefill_hidden_only",
                        )
                    else
                        gpt_arch.forwardHiddenOnly(
                            &self.cb,
                            allocator,
                            self.gpt_config,
                            chunk,
                            1,
                            total_chunk_end,
                            &decode_context,
                        );
                    hidden_only_result catch |err| {
                        if (try retryDirectPrefillAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, chunk_size, &current_chunk_size, admitted_plan == null, err)) continue;
                        return err;
                    };
                    debugGenerationStage(
                        "executePrefill chunk complete processed={d} total_chunk_end={d} output=kv_only",
                        .{ processed, total_chunk_end },
                    );
                } else {
                    const logits = self.forwardAllLogits(chunk, 1, total_chunk_end, &decode_context) catch |err| {
                        if (try retryDirectPrefillAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, chunk_size, &current_chunk_size, admitted_plan == null, err)) continue;
                        return err;
                    };
                    defer allocator.free(logits);
                    debugGenerationStage(
                        "executePrefill chunk complete processed={d} total_chunk_end={d} logits_len={d}",
                        .{ processed, total_chunk_end, logits.len },
                    );
                    if (total_chunk_end == seq_len) {
                        prefill_last_logits = try allocator.dupe(f32, logits[(chunk.len - 1) * self.gpt_config.vocab_size ..][0..self.gpt_config.vocab_size]);
                        debugGenerationStage(
                            "executePrefill captured last logits vocab_size={d}",
                            .{self.gpt_config.vocab_size},
                        );
                    }
                }
                processed = chunk_end;
                if (admitted_plan != null) plan_chunk_index += 1;
                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        scheduler.notePrefillProgress(lease, prefilled_tokens + processed, seq_len);
                        scheduler.finishTurn(lease, .prefill);
                        // The scoped defer is the error/continue safety net.
                        // Once the successful path releases the turn, disarm
                        // it so every chunk performs exactly one coordinator
                        // lock/unlock pair for completion.
                        direct_prefill_turn_acquired = false;
                    }
                }
            }
            if (admitted_plan) |plan| {
                if (plan_chunk_index != plan.chunks.len) return error.InvalidPrefillChunkPlan;
            }
        } else {
            if (prefilled_tokens != 0) return error.UnsupportedShape;
            if (self.scheduler) |scheduler| {
                if (self.scheduler_lease) |lease| {
                    if (self.io) |io| {
                        prefill_last_logits = try self.runScheduledPrefillBatch(scheduler, lease, io, decode_state, prompt_ids, seq_len, seq_len, true);
                        scheduler.notePrefillProgress(lease, seq_len, seq_len);
                    } else {
                        _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                    }
                } else {
                    _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                }
            } else {
                _ = try decode_runtime.preparePrefill(seq_len, seq_len);
            }

            if (self.scheduler) |scheduler| {
                if (self.scheduler_lease) |lease| {
                    if (self.io == null) {
                        scheduler.notePrefillProgress(lease, seq_len, seq_len);
                        scheduler.finishTurn(lease, .prefill);
                    }
                }
            }
        }

        debugGenerationStage(
            "executePrefill exit cached_logits={} greedy_token={}",
            .{ prefill_last_logits != null, prefill_greedy_token != null },
        );
        return .{
            .last_logits = prefill_last_logits,
            .greedy_token = prefill_greedy_token,
            .last_hidden = prefill_last_hidden,
            .last_hidden_rows = prefill_last_hidden_rows,
        };
    }

    fn executePreparedMultimodalPrefill(
        self: *NativeGenerationPipeline,
        prepared: *gemma3_mm.PreparedPrompt,
        seq_len: usize,
        decode_state: *NativeDecodeState,
    ) !?[]f32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var direct_prefill_turn_acquired = false;
        defer if (direct_prefill_turn_acquired) {
            self.scheduler.?.finishTurn(self.scheduler_lease.?, .prefill);
        };
        if (self.gpt_config.isQwen35() and decode_state.isPaged()) {
            try decode_state.ensureQwen35LinearCache(self.gpt_config);
            decode_state.resetQwen35LinearCache();
        }
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                scheduler.notePrefillProgress(lease, 0, seq_len);
            }
        }

        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                if (self.io == null) {
                    _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                } else {
                    try scheduler.awaitTurn(lease, .prefill, self.io.?);
                    direct_prefill_turn_acquired = true;
                    _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                }
            } else {
                _ = try decode_runtime.preparePrefill(seq_len, seq_len);
            }
        } else {
            _ = try decode_runtime.preparePrefill(seq_len, seq_len);
        }

        const input_embeddings = prepared.input_embeddings orelse return error.InvalidPreparedPrompt;
        prepared.input_embeddings = null;
        // Match text prefill/decode lock order: scheduler turn, then model.
        // Both stay held until the backend forward and result copy complete.
        const direct_execution_mutex = directPrefillExecutionMutex(true, false, false, self.execution_lock);
        if (direct_execution_mutex) |mutex| platform.sync.lockYielding(mutex);
        defer if (direct_execution_mutex) |mutex| mutex.unlock();
        const ple_token_ids = prepared.ple_token_ids orelse prepared.token_ids;
        const ple_vectors = try gpt_arch.computePleVectors(&self.cb, self.allocator, self.gpt_config, ple_token_ids, input_embeddings, seq_len);
        defer if (ple_vectors) |vectors| self.cb.free(vectors);
        var decode_context = decode_runtime.makeDecodeContext(seq_len, seq_len);
        decode_context.attn_or_mask = prepared.attn_or_mask;
        const logits = try gpt_arch.forwardFromEmbeddings(
            &self.cb,
            self.allocator,
            self.gpt_config,
            input_embeddings,
            1,
            seq_len,
            &decode_context,
            ple_vectors,
        );
        defer self.allocator.free(logits);
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| scheduler.notePrefillProgress(lease, seq_len, seq_len);
        }
        return try self.allocator.dupe(f32, logits[(seq_len - 1) * self.gpt_config.vocab_size ..][0..self.gpt_config.vocab_size]);
    }

    fn executePreparedQwen3VlPrefill(
        self: *NativeGenerationPipeline,
        prepared: *qwen3vl_projector.PreparedPrompt,
        seq_len: usize,
        decode_state: *NativeDecodeState,
    ) !?[]f32 {
        if (prepared.plan.tokenCount() != seq_len or !decode_state.isPaged()) return error.InvalidPreparedPrompt;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var direct_prefill_turn_acquired = false;
        defer if (direct_prefill_turn_acquired) self.scheduler.?.finishTurn(self.scheduler_lease.?, .prefill);
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| scheduler.notePrefillProgress(lease, 0, seq_len);
        }
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                if (self.io == null) {
                    _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                } else {
                    try scheduler.awaitTurn(lease, .prefill, self.io.?);
                    direct_prefill_turn_acquired = true;
                    _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                }
            } else {
                _ = try decode_runtime.preparePrefill(seq_len, seq_len);
            }
        } else {
            _ = try decode_runtime.preparePrefill(seq_len, seq_len);
        }

        const input_embeddings = prepared.input_embeddings orelse return error.InvalidPreparedPrompt;
        prepared.input_embeddings = null;
        const direct_execution_mutex = directPrefillExecutionMutex(true, false, false, self.execution_lock);
        if (direct_execution_mutex) |mutex| platform.sync.lockYielding(mutex);
        defer if (direct_execution_mutex) |mutex| mutex.unlock();
        var decode_context = decode_runtime.makeDecodeContext(seq_len, seq_len);
        decode_context.mrope_positions = prepared.plan.mrope_positions;
        decode_context.qwen3vl_visual_mask = prepared.plan.visual_token_mask;
        decode_context.qwen3vl_deepstack_embeddings = prepared.deepstack_embeddings;
        decode_context.qwen3vl_deepstack_layer_count = prepared.deepstack_layer_count;
        const logits = try gpt_arch.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(
            &self.cb,
            self.allocator,
            self.gpt_config,
            input_embeddings,
            .{},
            1,
            seq_len,
            &decode_context,
            null,
        );
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| scheduler.notePrefillProgress(lease, seq_len, seq_len);
        }
        return logits;
    }

    const DecodeResult = struct {
        tokens_generated: usize,
        finish_reason: []const u8,
    };

    const DeviceDecodeOutcome = struct {
        token: usize,
        token_tensor: ?ops.CT = null,
    };

    fn noteDecodeProgressNoMicrobatch(self: *NativeGenerationPipeline, tokens_generated: usize) void {
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                scheduler.noteDecodeProgress(lease, tokens_generated);
                scheduler.finishTurn(lease, .decode);
            }
        }
    }

    fn resolveFinalChannelEndTokenId(self: *NativeGenerationPipeline) !?i32 {
        if (!self.gpt_config.usesGemma4Channels()) return null;
        return resolveTokenId(self.tokenizer, self.allocator, "<channel|>");
    }

    fn resolveFinalChannelHeaderTokenIds(
        self: *NativeGenerationPipeline,
        final_channel_end_token_id: i32,
    ) !?[]i32 {
        const ids = try self.tokenizer.encode(
            self.allocator,
            "<|channel>final\n<channel|>",
        );
        errdefer self.allocator.free(ids);
        if (ids.len == 0 or ids[ids.len - 1] != final_channel_end_token_id) {
            self.allocator.free(ids);
            return null;
        }
        return ids;
    }

    fn resolveChannelStartTokenId(self: *NativeGenerationPipeline) !?i32 {
        return resolveTokenId(self.tokenizer, self.allocator, "<|channel>");
    }

    fn resolveTurnEndTokenId(self: *NativeGenerationPipeline) !?i32 {
        if (!self.gpt_config.usesGemma4Channels()) return null;
        return resolveTokenId(self.tokenizer, self.allocator, "<turn|>");
    }

    fn emitDecodedDelta(
        self: *NativeGenerationPipeline,
        generated_token_ids: []const i64,
        state: *StreamingTextState,
        on_token_fn: TokenCallback,
        on_token_ctx: *anyopaque,
    ) !bool {
        if (self.continue_fn) |should_continue| {
            const continue_ctx = self.continue_ctx orelse return error.InvalidCallbackContext;
            if (!should_continue(continue_ctx)) return false;
        }
        return emitDecodedDeltaForTokenizer(
            self.tokenizer,
            self.allocator,
            generated_token_ids,
            state,
            on_token_fn,
            on_token_ctx,
        );
    }

    fn tryReturnPrefillFirstToken(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: *usize,
        config: GenerationConfig,
        prefill_last_logits: *?[]f32,
        prefill_greedy_token: *?usize,
        penalty_state: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
        max_tokens: usize,
        prompt_token_count: usize,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
        emitted_text: ?*StreamingTextState,
    ) !?DecodeResult {
        if (!shouldUsePrefillFirstTokenPath(
            self.cb.kind(),
            isPureGreedyConfig(config) and config.grammar == null,
            false,
            enableCudaPrefillFirstToken(),
        )) {
            debugFirstToken(
                "prefill_first_token disabled backend={s} max_tokens={d} pure_greedy={} enabled={}",
                .{ @tagName(self.cb.kind()), max_tokens, isPureGreedyConfig(config), enableCudaPrefillFirstToken() },
            );
            return null;
        }

        const has_grammar = json_grammar.* != null or gbnf_grammar != null;
        const outcome: SampleOutcome = blk: {
            if (!has_grammar) {
                if (prefill_greedy_token.*) |token| {
                    prefill_greedy_token.* = null;
                    debugFirstToken("prefill_first_token source=greedy token={d}", .{token});
                    break :blk .{ .token = token, .grammar_complete = false };
                }
            }
            if (prefill_last_logits.*) |cached_logits| {
                debugFirstToken(
                    "prefill_first_token source=logits len={d} grammar={}",
                    .{ cached_logits.len, has_grammar },
                );
                try self.debugFirstTokenTopLogits("first_token_top_logits_raw", cached_logits);
                const suppress_token_ids = self.gpt_config.suppressTokenIds();
                if (suppress_token_ids.len > 0 and firstTokenTopKTraceCount() > 0) {
                    const masked_logits = try self.allocator.dupe(f32, cached_logits);
                    defer self.allocator.free(masked_logits);
                    applySuppressTokenMask(masked_logits, suppress_token_ids);
                    try self.debugFirstTokenTopLogits("first_token_top_logits_post_suppress", masked_logits);
                }
                break :blk try self.sampleNextToken(
                    cached_logits,
                    config,
                    penalty_state,
                    token_table,
                    json_grammar,
                    gbnf_grammar,
                );
            }
            debugFirstToken(
                "prefill_first_token unavailable greedy={} logits={} grammar={}",
                .{ prefill_greedy_token.* != null, prefill_last_logits.* != null, has_grammar },
            );
            return null;
        };

        const next_token = outcome.token;
        if (self.shouldStopOnEos(config, next_token)) {
            self.finishPrefillFirstTokenSchedulerTurn(0);
            debugFirstToken("prefill_first_token eos token={d}", .{next_token});
            return .{ .tokens_generated = 0, .finish_reason = "stop" };
        }

        token_ids[seq_len.*] = @intCast(next_token);
        seq_len.* += 1;
        try penalty_state.noteToken(self.allocator, @intCast(next_token));

        var finish_reason: []const u8 = "length";
        if (on_token_fn != null and on_token_ctx != null and emitted_text != null) {
            const keep_streaming = try self.emitDecodedDelta(
                token_ids[prompt_token_count..seq_len.*],
                emitted_text.?,
                on_token_fn.?,
                on_token_ctx.?,
            );
            if (!keep_streaming) finish_reason = "stop";
        }
        if (outcome.grammar_complete) finish_reason = "stop";

        self.finishPrefillFirstTokenSchedulerTurn(1);
        debugFirstToken(
            "prefill_first_token returned token={d} seq_len={d} finish_reason={s}",
            .{ next_token, seq_len.*, finish_reason },
        );
        return .{ .tokens_generated = 1, .finish_reason = finish_reason };
    }

    fn finishPrefillFirstTokenSchedulerTurn(self: *NativeGenerationPipeline, tokens_generated: usize) void {
        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                scheduler.noteDecodeProgress(lease, tokens_generated);
                scheduler.finishTurn(lease, .decode);
            }
        }
    }

    const FirstTokenTopLogit = struct {
        id: usize,
        logit: f32,
    };

    fn debugFirstTokenTopLogits(self: *NativeGenerationPipeline, label: []const u8, logits: []const f32) !void {
        const requested = firstTokenTopKTraceCount();
        if (requested == 0) return;
        const count = @min(requested, logits.len);
        var entries = try self.allocator.alloc(FirstTokenTopLogit, logits.len);
        defer self.allocator.free(entries);
        for (logits, 0..) |logit, idx| entries[idx] = .{ .id = idx, .logit = logit };
        std.mem.sort(FirstTokenTopLogit, entries, {}, struct {
            fn lessThan(_: void, lhs: FirstTokenTopLogit, rhs: FirstTokenTopLogit) bool {
                return lhs.logit > rhs.logit;
            }
        }.lessThan);

        std.debug.print("{s}:\n", .{label});
        for (entries[0..count], 0..) |entry, rank| {
            const token_id: i32 = @intCast(entry.id);
            const decoded = self.tokenizer.decode(self.allocator, &.{token_id}) catch |err| {
                std.debug.print("  rank={d} id={d} logit={d:.6} text=<decode_error:{s}>\n", .{
                    rank + 1,
                    entry.id,
                    entry.logit,
                    @errorName(err),
                });
                continue;
            };
            defer self.allocator.free(decoded);
            std.debug.print("  rank={d} id={d} logit={d:.6} text={s}\n", .{ rank + 1, entry.id, entry.logit, decoded });
        }
    }

    /// Standard autoregressive decode loop: generate one token at a time
    /// with grammar masking, sampling, and scheduler coordination.
    fn standardDecode(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: *usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        prefill_last_logits: *?[]f32,
        prefill_greedy_token: *?usize,
        penalty_state: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
        initial_tokens_generated: usize,
        max_tokens: usize,
        prompt_token_count: usize,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
        emitted_text: ?*StreamingTextState,
    ) !DecodeResult {
        const allocator = self.allocator;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var tokens_generated = initial_tokens_generated;
        var finish_reason: []const u8 = "length";
        var device_token_tensor: ?ops.CT = null;
        defer if (device_token_tensor) |tensor| self.freeDeviceTokenTensor(tensor);
        debugGenerationStage(
            "standardDecode enter seq_len={d} max_tokens={d} prefill_cached={}",
            .{ seq_len.*, max_tokens, prefill_last_logits.* != null },
        );
        if (initial_tokens_generated == 0 and self.execution_lock == null) {
            if (try self.standardDecodeCudaPendingTokenReadback(
                token_ids,
                seq_len,
                decode_state,
                config,
                prefill_last_logits,
                prefill_greedy_token,
                penalty_state,
                token_table,
                json_grammar,
                gbnf_grammar,
                max_tokens,
                prompt_token_count,
                on_token_fn,
                on_token_ctx,
                emitted_text,
            )) |pending_result| {
                return pending_result;
            }
        }

        while (tokens_generated < max_tokens) {
            var used_decode_microbatch = false;
            var direct_decode_turn_acquired = false;
            defer if (direct_decode_turn_acquired) {
                self.scheduler.?.finishTurn(self.scheduler_lease.?, .decode);
            };
            var next_device_token_tensor: ?ops.CT = null;
            errdefer if (next_device_token_tensor) |tensor| self.freeDeviceTokenTensor(tensor);
            debugGenerationStage(
                "standardDecode iter={d} seq_len={d} device_token_handoff={}",
                .{ tokens_generated, seq_len.*, device_token_tensor != null },
            );
            if (tokens_generated > 0 and self.pipelinedGreedyEligible(config, token_table, json_grammar, gbnf_grammar, decode_state)) {
                if (try self.runPipelinedGreedyTail(
                    token_ids,
                    seq_len,
                    decode_state,
                    config,
                    penalty_state,
                    max_tokens,
                    prompt_token_count,
                    on_token_fn,
                    on_token_ctx,
                    emitted_text,
                    &tokens_generated,
                    &finish_reason,
                )) {
                    break;
                }
            }
            const outcome: SampleOutcome = blk: {
                if (tokens_generated == 0) {
                    if (prefill_greedy_token.*) |token| {
                        prefill_greedy_token.* = null;
                        break :blk .{ .token = token, .grammar_complete = false };
                    }
                }

                const has_cached_prefill_logits = tokens_generated == 0 and prefill_last_logits.* != null;
                // Once a second request is admitted inside the validated C2
                // envelope, keep both requests on the scheduler path. A
                // compatibility-only check here races with the later
                // enqueue/turn acquisition: one request can commit to a
                // direct step while its peer observes the old decode position
                // and waits for work that will never be enqueued. The
                // scheduler still admits only full-geometry-compatible work,
                // and incompatible peers do not pay a coalescing delay.
                const route_decode_through_scheduler = self.execution_lock != null and self.scheduler_lease != null and
                    (if (self.scheduler) |scheduler| scheduler.shouldCoordinateCurrentRequests() else false);
                const allow_direct_device_decode = !route_decode_through_scheduler;
                if (!has_cached_prefill_logits and allow_direct_device_decode) {
                    const direct_token = direct_token_blk: {
                        // Above the batching rollout cap the scheduler emits
                        // singleton work, but direct device decode must still
                        // claim its fair turn before taking the shared model.
                        if (self.execution_lock != null) {
                            if (self.scheduler) |scheduler| {
                                if (self.scheduler_lease) |lease| {
                                    if (self.io) |io| {
                                        try scheduler.awaitTurn(lease, .decode, io);
                                        direct_decode_turn_acquired = true;
                                    }
                                }
                            }
                        }
                        if (self.execution_lock) |mutex| {
                            platform.sync.lockYielding(mutex);
                        }
                        defer if (self.execution_lock) |mutex| mutex.unlock();
                        break :direct_token_blk try self.forwardGreedyDeviceDecodeToken(
                            token_ids,
                            seq_len.*,
                            tokens_generated,
                            decode_state,
                            config,
                            penalty_state,
                            token_table,
                            json_grammar,
                            gbnf_grammar,
                            device_token_tensor,
                        );
                    };
                    if (direct_token) |token| {
                        next_device_token_tensor = token.token_tensor;
                        break :blk .{ .token = token.token, .grammar_complete = false };
                    }
                    const fallback_query_seq_len = if (decode_runtime.kvView() != null and
                        (tokens_generated > 0 or prefill_last_logits.* == null)) 1 else seq_len.*;
                    if (requiresCudaDecodeGraphBeforeEagerFallback(
                        self.cb.kind(),
                        gpt_arch.cudaDecodeGraphReplayRequired(),
                        isPureGreedyConfig(config),
                        fallback_query_seq_len,
                        self.graph_cache != null or self.compiled_partition_backend != null,
                        token_table != null or json_grammar.* != null or gbnf_grammar != null,
                    )) return error.CudaGraphReplayRequired;
                }

                var owns_last_logits = false;
                const last_logits: []const f32 = logits_blk: {
                    if (tokens_generated == 0) {
                        if (prefill_last_logits.*) |cached| {
                            debugGenerationStage(
                                "standardDecode iter={d} using cached prefill logits len={d}",
                                .{ tokens_generated, cached.len },
                            );
                            break :logits_blk cached;
                        }
                    }

                    if (self.scheduler) |scheduler| {
                        if (self.scheduler_lease) |lease| {
                            if (self.io) |io| {
                                if (self.graph_cache == null and decode_runtime.kvView() != null and (tokens_generated > 0 or prefill_last_logits.* == null)) {
                                    used_decode_microbatch = true;
                                    owns_last_logits = true;
                                    break :logits_blk try self.runScheduledDecodeBatch(scheduler, lease, io, decode_state, token_ids[seq_len.* - 1], seq_len.*);
                                }
                                try scheduler.awaitTurn(lease, .decode, io);
                                direct_decode_turn_acquired = true;
                            }
                        }
                    }

                    self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
                    const query_seq_len = if (decode_runtime.kvView() != null and (tokens_generated > 0 or prefill_last_logits.* == null)) 1 else seq_len.*;
                    const decode_context = decode_runtime.makeDecodeContext(seq_len.*, query_seq_len);
                    const input_ids = if (query_seq_len == seq_len.*)
                        token_ids[0..seq_len.*]
                    else
                        token_ids[seq_len.* - query_seq_len .. seq_len.*];
                    debugGenerationStage(
                        "standardDecode iter={d} requesting logits query_seq_len={d} input_len={d}",
                        .{ tokens_generated, query_seq_len, input_ids.len },
                    );

                    if (try self.forwardGreedyCompiledModelToken(
                        input_ids,
                        1,
                        seq_len.*,
                        &decode_context,
                        config,
                        token_table != null or json_grammar.* != null or gbnf_grammar != null,
                    )) |token| {
                        break :blk .{ .token = token, .grammar_complete = false };
                    }

                    owns_last_logits = true;
                    break :logits_blk try self.forwardLastLogits(input_ids, 1, seq_len.*, &decode_context);
                };

                defer if (owns_last_logits) allocator.free(@constCast(last_logits));
                debugGenerationStage(
                    "standardDecode iter={d} logits ready len={d} owns={}",
                    .{ tokens_generated, last_logits.len, owns_last_logits },
                );

                break :blk try self.sampleNextToken(
                    last_logits,
                    config,
                    penalty_state,
                    token_table,
                    json_grammar,
                    gbnf_grammar,
                );
            };
            const next_token = outcome.token;
            debugGenerationStage(
                "standardDecode iter={d} sampled next_token={d} grammar_complete={}",
                .{ tokens_generated, next_token, outcome.grammar_complete },
            );

            // Check EOS
            if (self.shouldStopOnEos(config, next_token)) {
                if (next_device_token_tensor) |tensor| {
                    self.freeDeviceTokenTensor(tensor);
                    next_device_token_tensor = null;
                }
                finish_reason = "stop";
                break;
            }

            if (next_device_token_tensor == null and self.shouldSeedDeviceTokenHandoff(tokens_generated, decode_state, config, token_table, json_grammar, gbnf_grammar)) {
                next_device_token_tensor = try self.makeDeviceTokenTensor(next_token);
                if (next_device_token_tensor != null) decoder_runtime_debug_stats.device_token_handoff_seeds += 1;
            }
            if (device_token_tensor) |tensor| self.freeDeviceTokenTensor(tensor);
            device_token_tensor = next_device_token_tensor;
            next_device_token_tensor = null;

            token_ids[seq_len.*] = @intCast(next_token);
            seq_len.* += 1;
            tokens_generated += 1;
            {
                const kv_mutation_mutex = batchKvMutationMutex(self.execution_lock, decode_state.kv_lock);
                if (kv_mutation_mutex) |mutex| platform.sync.lockYielding(mutex);
                defer if (kv_mutation_mutex) |mutex| mutex.unlock();
                _ = try decode_runtime.appendGeneratedToken();
            }
            try penalty_state.noteToken(allocator, @intCast(next_token));
            debugGenerationStage(
                "standardDecode iter={d} appended token new_seq_len={d}",
                .{ tokens_generated, seq_len.* },
            );
            if (on_token_fn != null and on_token_ctx != null and emitted_text != null) {
                const keep_streaming = try self.emitDecodedDelta(
                    token_ids[prompt_token_count..seq_len.*],
                    emitted_text.?,
                    on_token_fn.?,
                    on_token_ctx.?,
                );
                if (!keep_streaming) {
                    finish_reason = "stop";
                    break;
                }
            }
            if (outcome.grammar_complete) {
                finish_reason = "stop";
                break;
            }
            if (self.scheduler) |scheduler| {
                if (self.scheduler_lease) |lease| {
                    scheduler.noteDecodeProgress(lease, tokens_generated);
                    if (!used_decode_microbatch) {
                        scheduler.finishTurn(lease, .decode);
                    }
                }
            }
        }

        debugGenerationStage(
            "standardDecode exit tokens_generated={d} finish_reason={s}",
            .{ tokens_generated, finish_reason },
        );
        return .{ .tokens_generated = tokens_generated, .finish_reason = finish_reason };
    }

    fn pipelinedGreedyEligible(
        self: *NativeGenerationPipeline,
        config: GenerationConfig,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
        decode_state: *NativeDecodeState,
    ) bool {
        if (!pipelinedMetalDecodeEnabled()) return false;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (self.cb.kind() != .metal) return false;
        if (self.scheduler != null) return false;
        if (self.graph_cache != null or self.compiled_partition_backend != null) return false;
        if (decode_runtime.kvView() == null) return false;
        if (!isPureGreedyConfig(config)) return false;
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) return false;
        if (self.gpt_config.family != .gemma or !self.gpt_config.hasPle()) return false;
        if (!gemma4MetalDirectGreedyEnabled()) return false;
        return true;
    }

    fn runPipelinedGreedyTail(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: *usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        penalty_state: *SamplingPenaltyState,
        max_tokens: usize,
        prompt_token_count: usize,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
        emitted_text: ?*StreamingTextState,
        tokens_generated: *usize,
        finish_reason: *[]const u8,
    ) !bool {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (tokens_generated.* >= max_tokens) return false;
        if (seq_len.* == 0) return false;
        self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
        {
            const arm_ctx = decode_runtime.makeDecodeContext(seq_len.*, 1);
            const armed = decoder_gated_runtime.forwardGreedyTokenPipelinedArm(
                &self.cb,
                self.allocator,
                self.gpt_config,
                self.gpt_config.num_hidden_layers,
                token_ids[seq_len.* - 1],
                seq_len.*,
                &arm_ctx,
            ) catch false;
            if (!armed) return false;
        }
        errdefer {
            _ = decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .cancel_pending) catch {};
            _ = decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .await_only) catch {};
        }
        while (tokens_generated.* < max_tokens) {
            const remaining = max_tokens - tokens_generated.*;
            var next_token: i64 = -1;
            var speculative_appended = false;
            var stepped = false;
            if (remaining > 1) {
                _ = try decode_runtime.appendGeneratedToken();
                speculative_appended = true;
                self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
                const step_ctx = decode_runtime.makeDecodeContext(seq_len.* + 1, 1);
                if (try decoder_gated_runtime.forwardGreedyTokenPipelinedStep(
                    &self.cb,
                    self.allocator,
                    self.gpt_config,
                    self.gpt_config.num_hidden_layers,
                    seq_len.* + 1,
                    &step_ctx,
                )) |tok| {
                    next_token = tok;
                    stepped = true;
                } else {
                    next_token = (try decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .await_only)) orelse return error.InvalidModelOutput;
                }
            } else {
                next_token = (try decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .await_only)) orelse return error.InvalidModelOutput;
            }
            if (next_token < 0) return error.InvalidModelOutput;
            const token: usize = @intCast(next_token);
            if (self.shouldStopOnEos(config, token)) {
                if (stepped) {
                    _ = decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .cancel_pending) catch {};
                }
                finish_reason.* = "stop";
                return true;
            }
            if (stepped) {
                const submit_ok = ((decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .submit_pending) catch null) != null);
                if (!submit_ok) {
                    _ = decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .cancel_pending) catch {};
                    stepped = false;
                }
            }
            token_ids[seq_len.*] = @intCast(token);
            seq_len.* += 1;
            tokens_generated.* += 1;
            if (!speculative_appended) {
                _ = try decode_runtime.appendGeneratedToken();
            }
            try penalty_state.noteToken(self.allocator, @intCast(token));
            if (on_token_fn != null and on_token_ctx != null and emitted_text != null) {
                const keep_streaming = try self.emitDecodedDelta(
                    token_ids[prompt_token_count..seq_len.*],
                    emitted_text.?,
                    on_token_fn.?,
                    on_token_ctx.?,
                );
                if (!keep_streaming) {
                    _ = decoder_gated_runtime.decoderRuntimePipelinedControl(&self.cb, .await_only) catch {};
                    finish_reason.* = "stop";
                    return true;
                }
            }
            if (!stepped) {
                if (tokens_generated.* >= max_tokens) break;
                const rearm_ctx = decode_runtime.makeDecodeContext(seq_len.*, 1);
                const rearmed = decoder_gated_runtime.forwardGreedyTokenPipelinedArm(
                    &self.cb,
                    self.allocator,
                    self.gpt_config,
                    self.gpt_config.num_hidden_layers,
                    token_ids[seq_len.* - 1],
                    seq_len.*,
                    &rearm_ctx,
                ) catch false;
                if (!rearmed) return true;
            }
        }
        return true;
    }

    fn standardDecodeCudaPendingTokenReadback(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: *usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        prefill_last_logits: *?[]f32,
        prefill_greedy_token: *?usize,
        penalty_state: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
        max_tokens: usize,
        prompt_token_count: usize,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
        emitted_text: ?*StreamingTextState,
    ) !?DecodeResult {
        if (!enableCudaGreedyPendingTokenReadback()) return null;
        if (self.cb.kind() != .cuda) return null;
        if (self.graph_cache != null or self.compiled_partition_backend != null) return null;
        if (!pendingTokenReadbackStreamingArgsValid(on_token_fn, on_token_ctx, emitted_text)) return null;
        if (!isPureGreedyConfig(config)) return null;
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) return null;
        if (max_tokens < 2) return null;

        const allocator = self.allocator;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_runtime.kvView() == null) return null;

        var tokens_generated: usize = 0;
        var finish_reason: []const u8 = "length";
        var input_device_token_tensor: ?ops.CT = null;
        var candidate_token_tensor: ?ops.CT = null;
        defer if (input_device_token_tensor) |tensor| self.cb.free(tensor);
        defer if (candidate_token_tensor) |tensor| self.cb.free(tensor);

        debugGenerationStage(
            "standardDecode pending-token-readback enter seq_len={d} max_tokens={d}",
            .{ seq_len.*, max_tokens },
        );

        while (tokens_generated < max_tokens) {
            if (candidate_token_tensor == null) {
                if (tokens_generated == 0) {
                    if (prefill_greedy_token.*) |token| {
                        prefill_greedy_token.* = null;
                        if (self.shouldStopOnEos(config, token)) {
                            finish_reason = "stop";
                            break;
                        }
                        if (self.shouldSeedDeviceTokenHandoff(tokens_generated, decode_state, config, token_table, json_grammar, gbnf_grammar)) {
                            input_device_token_tensor = try self.makeDeviceTokenTensor(token);
                            if (input_device_token_tensor != null) decoder_runtime_debug_stats.device_token_handoff_seeds += 1;
                        }
                        token_ids[seq_len.*] = @intCast(token);
                        seq_len.* += 1;
                        tokens_generated += 1;
                        _ = try decode_runtime.appendGeneratedToken();
                        try penalty_state.noteToken(allocator, @intCast(token));
                        self.noteDecodeProgressNoMicrobatch(tokens_generated);
                        if (!try self.emitCudaPendingTokenReadbackDelta(
                            token_ids,
                            seq_len.*,
                            prompt_token_count,
                            on_token_fn,
                            on_token_ctx,
                            emitted_text,
                        )) {
                            finish_reason = "stop";
                            break;
                        }
                        continue;
                    }
                    if (prefill_last_logits.*) |cached| {
                        const outcome = try self.sampleNextToken(
                            cached,
                            config,
                            penalty_state,
                            token_table,
                            json_grammar,
                            gbnf_grammar,
                        );
                        const token = outcome.token;
                        if (self.shouldStopOnEos(config, token)) {
                            finish_reason = "stop";
                            break;
                        }
                        if (self.shouldSeedDeviceTokenHandoff(tokens_generated, decode_state, config, token_table, json_grammar, gbnf_grammar)) {
                            input_device_token_tensor = try self.makeDeviceTokenTensor(token);
                            if (input_device_token_tensor != null) decoder_runtime_debug_stats.device_token_handoff_seeds += 1;
                        }
                        token_ids[seq_len.*] = @intCast(token);
                        seq_len.* += 1;
                        tokens_generated += 1;
                        _ = try decode_runtime.appendGeneratedToken();
                        try penalty_state.noteToken(allocator, @intCast(token));
                        self.noteDecodeProgressNoMicrobatch(tokens_generated);
                        if (!try self.emitCudaPendingTokenReadbackDelta(
                            token_ids,
                            seq_len.*,
                            prompt_token_count,
                            on_token_fn,
                            on_token_ctx,
                            emitted_text,
                        )) {
                            finish_reason = "stop";
                            break;
                        }
                        if (outcome.grammar_complete) {
                            finish_reason = "stop";
                            break;
                        }
                        continue;
                    }
                }

                if (input_device_token_tensor) |input_tensor| {
                    if (try self.forwardGreedyDeviceDecodeTokenTensor(
                        seq_len.*,
                        tokens_generated,
                        decode_state,
                        config,
                        token_table,
                        json_grammar,
                        gbnf_grammar,
                        input_tensor,
                    )) |candidate| {
                        candidate_token_tensor = candidate;
                        self.cb.free(input_tensor);
                        input_device_token_tensor = null;
                        continue;
                    }

                    if (try self.forwardGreedyDeviceDecodeToken(
                        token_ids,
                        seq_len.*,
                        tokens_generated,
                        decode_state,
                        config,
                        penalty_state,
                        token_table,
                        json_grammar,
                        gbnf_grammar,
                        input_tensor,
                    )) |resolved| {
                        var next_device_token_tensor = resolved.token_tensor;
                        errdefer if (next_device_token_tensor) |tensor| self.cb.free(tensor);
                        if (self.shouldStopOnEos(config, resolved.token)) {
                            if (next_device_token_tensor) |tensor| self.cb.free(tensor);
                            finish_reason = "stop";
                            break;
                        }
                        if (next_device_token_tensor == null and self.shouldSeedDeviceTokenHandoff(tokens_generated, decode_state, config, token_table, json_grammar, gbnf_grammar)) {
                            next_device_token_tensor = try self.makeDeviceTokenTensor(resolved.token);
                            if (next_device_token_tensor != null) decoder_runtime_debug_stats.device_token_handoff_seeds += 1;
                        }
                        self.cb.free(input_tensor);
                        input_device_token_tensor = next_device_token_tensor;
                        next_device_token_tensor = null;
                        token_ids[seq_len.*] = @intCast(resolved.token);
                        seq_len.* += 1;
                        tokens_generated += 1;
                        _ = try decode_runtime.appendGeneratedToken();
                        try penalty_state.noteToken(allocator, @intCast(resolved.token));
                        self.noteDecodeProgressNoMicrobatch(tokens_generated);
                        if (!try self.emitCudaPendingTokenReadbackDelta(
                            token_ids,
                            seq_len.*,
                            prompt_token_count,
                            on_token_fn,
                            on_token_ctx,
                            emitted_text,
                        )) {
                            finish_reason = "stop";
                            break;
                        }
                        continue;
                    }
                }

                break;
            }

            const candidate = candidate_token_tensor.?;
            if (tokens_generated + 1 >= max_tokens) {
                const token = try gpt_arch.resolveGreedyDeviceTokenTensor(&self.cb, allocator, candidate);
                if (self.shouldStopOnEos(config, token)) {
                    finish_reason = "stop";
                    break;
                }
                token_ids[seq_len.*] = @intCast(token);
                seq_len.* += 1;
                tokens_generated += 1;
                _ = try decode_runtime.appendGeneratedToken();
                try penalty_state.noteToken(allocator, @intCast(token));
                self.noteDecodeProgressNoMicrobatch(tokens_generated);
                if (!try self.emitCudaPendingTokenReadbackDelta(
                    token_ids,
                    seq_len.*,
                    prompt_token_count,
                    on_token_fn,
                    on_token_ctx,
                    emitted_text,
                )) {
                    finish_reason = "stop";
                    break;
                }
                input_device_token_tensor = candidate;
                candidate_token_tensor = null;
                continue;
            }

            const pending = (try self.cb.beginI32ScalarDownload(candidate)) orelse {
                const token = try gpt_arch.resolveGreedyDeviceTokenTensor(&self.cb, allocator, candidate);
                if (self.shouldStopOnEos(config, token)) {
                    finish_reason = "stop";
                    break;
                }
                token_ids[seq_len.*] = @intCast(token);
                seq_len.* += 1;
                tokens_generated += 1;
                _ = try decode_runtime.appendGeneratedToken();
                try penalty_state.noteToken(allocator, @intCast(token));
                self.noteDecodeProgressNoMicrobatch(tokens_generated);
                if (!try self.emitCudaPendingTokenReadbackDelta(
                    token_ids,
                    seq_len.*,
                    prompt_token_count,
                    on_token_fn,
                    on_token_ctx,
                    emitted_text,
                )) {
                    finish_reason = "stop";
                    break;
                }
                input_device_token_tensor = candidate;
                candidate_token_tensor = null;
                continue;
            };
            var pending_active = true;
            errdefer if (pending_active) self.cb.cancelI32ScalarDownload(pending);

            token_ids[seq_len.*] = 0;
            seq_len.* += 1;
            var appended_placeholder = true;
            errdefer if (appended_placeholder) {
                seq_len.* -= 1;
                decode_runtime.truncateGeneratedTokens(1) catch {};
            };
            _ = try decode_runtime.appendGeneratedToken();

            var next_candidate: ?ops.CT = null;
            errdefer if (next_candidate) |tensor| self.cb.free(tensor);
            if (try self.forwardGreedyDeviceDecodeTokenTensor(
                seq_len.*,
                tokens_generated + 1,
                decode_state,
                config,
                token_table,
                json_grammar,
                gbnf_grammar,
                candidate,
            )) |next| {
                next_candidate = next;
            }

            const raw_token = try self.cb.finishI32ScalarDownload(pending);
            pending_active = false;
            if (raw_token < 0) return error.InvalidTensorShape;
            const token: usize = @intCast(raw_token);
            if (self.shouldStopOnEos(config, token)) {
                if (next_candidate) |tensor| {
                    self.cb.evalTensor(tensor) catch {};
                    self.cb.free(tensor);
                    next_candidate = null;
                }
                self.cb.free(candidate);
                candidate_token_tensor = null;
                seq_len.* -= 1;
                appended_placeholder = false;
                try decode_runtime.truncateGeneratedTokens(1);
                finish_reason = "stop";
                break;
            }

            token_ids[seq_len.* - 1] = @intCast(token);
            appended_placeholder = false;
            tokens_generated += 1;
            try penalty_state.noteToken(allocator, @intCast(token));
            self.noteDecodeProgressNoMicrobatch(tokens_generated);

            if (!try self.emitCudaPendingTokenReadbackDelta(
                token_ids,
                seq_len.*,
                prompt_token_count,
                on_token_fn,
                on_token_ctx,
                emitted_text,
            )) {
                if (next_candidate) |tensor| {
                    self.cb.evalTensor(tensor) catch {};
                    self.cb.free(tensor);
                    next_candidate = null;
                }
                self.cb.free(candidate);
                candidate_token_tensor = null;
                finish_reason = "stop";
                break;
            }

            if (next_candidate) |next| {
                self.cb.free(candidate);
                candidate_token_tensor = next;
                next_candidate = null;
            } else {
                input_device_token_tensor = candidate;
                candidate_token_tensor = null;
            }
        }

        debugGenerationStage(
            "standardDecode pending-token-readback exit tokens_generated={d} finish_reason={s}",
            .{ tokens_generated, finish_reason },
        );
        return .{ .tokens_generated = tokens_generated, .finish_reason = finish_reason };
    }

    fn emitCudaPendingTokenReadbackDelta(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        seq_len: usize,
        prompt_token_count: usize,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
        emitted_text: ?*StreamingTextState,
    ) !bool {
        if (on_token_fn) |callback| {
            return self.emitDecodedDelta(
                token_ids[prompt_token_count..seq_len],
                emitted_text.?,
                callback,
                on_token_ctx.?,
            );
        }
        return true;
    }

    fn forwardGreedyDeviceDecodeToken(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        seq_len: usize,
        tokens_generated: usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        penalty_state: *const SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
        input_token_tensor: ?ops.CT,
    ) !?DeviceDecodeOutcome {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        decoder_runtime_debug_stats.forward_attempts += 1;
        const backend_kind = self.cb.kind();
        if (backend_kind != .cuda and backend_kind != .metal) {
            decoder_runtime_debug_stats.backend_not_device_decode += 1;
            return null;
        }
        if (self.graph_cache != null or self.compiled_partition_backend != null) {
            decoder_runtime_debug_stats.graph_blocked += 1;
            return null;
        }
        if (tokens_generated == 0 and decode_runtime.kvView() == null) {
            decoder_runtime_debug_stats.first_token_blocked += 1;
            return null;
        }
        if (decode_runtime.kvView() == null) {
            decoder_runtime_debug_stats.kv_missing += 1;
            return null;
        }
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) {
            decoder_runtime_debug_stats.grammar_blocked += 1;
            return null;
        }
        if (!isPureGreedyConfig(config)) {
            // Sampled decoding must stay on the fused decoder frame; without
            // this it fell off onto the per-op eager path, making sampled chat
            // ~15x slower than greedy on metal.
            if (backend_kind == .metal and self.gpt_config.family == .gemma and self.gpt_config.hasPle() and gemma4MetalDirectGreedyEnabled()) {
                self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
                const sampled_decode_context = decode_runtime.makeDecodeContext(seq_len, 1);
                // Preferred: the backend-owned sampled frame (device-resident
                // logit sampling / prepared sampled tail).
                const sampling = graph_mod.model_runtime.SamplingConfig{
                    .temperature = config.temperature,
                    .top_p = config.top_p,
                    .top_k = config.top_k,
                    .min_p = config.min_p,
                    .repetition_penalty = config.repetition_penalty,
                    .frequency_penalty = config.frequency_penalty,
                    .presence_penalty = config.presence_penalty,
                };
                if (try decoder_gated_runtime.forwardSampledToken(
                    &self.cb,
                    self.allocator,
                    self.gpt_config,
                    self.gpt_config.num_hidden_layers,
                    token_ids[seq_len - 1],
                    seq_len,
                    &sampled_decode_context,
                    sampling,
                    token_ids[0..seq_len],
                )) |sampled_token_id| {
                    if (sampled_token_id < 0) return error.InvalidModelOutput;
                    return .{ .token = @intCast(sampled_token_id) };
                }
                // Fallback: same fused forward, host-side sampling from the
                // read-back logits.
                if (try decoder_gated_runtime.forwardLastLogits(
                    &self.cb,
                    self.allocator,
                    self.gpt_config,
                    self.gpt_config.num_hidden_layers,
                    token_ids[seq_len - 1],
                    seq_len,
                    &sampled_decode_context,
                )) |logits| {
                    defer self.allocator.free(logits);
                    // Keep every token constraint on the canonical host sampler
                    // when the backend-owned sampler cannot honor it. Grammars
                    // are already excluded above, but model suppression still
                    // applies to this fused-forward fallback.
                    var no_json_grammar: ?grammar_mod.JsonGrammar = null;
                    const outcome = try self.sampleNextToken(
                        logits,
                        config,
                        penalty_state,
                        null,
                        &no_json_grammar,
                        null,
                    );
                    return .{ .token = outcome.token };
                }
            }
            decoder_runtime_debug_stats.non_greedy += 1;
            return null;
        }

        self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
        const decode_context = decode_runtime.makeDecodeContext(seq_len, 1);
        if (backend_kind == .metal and self.gpt_config.family == .gemma and self.gpt_config.hasPle() and gemma4MetalDirectGreedyEnabled()) {
            if (try decoder_gated_runtime.forwardGreedyToken(
                &self.cb,
                self.allocator,
                self.gpt_config,
                self.gpt_config.num_hidden_layers,
                token_ids[seq_len - 1],
                seq_len,
                &decode_context,
            )) |token_id| {
                if (token_id < 0) return error.InvalidModelOutput;
                return .{ .token = @intCast(token_id) };
            }
        }
        if (input_token_tensor) |token_tensor| {
            if (enableCudaGreedyDeviceTokenHandoff()) {
                decoder_runtime_debug_stats.device_token_handoff_attempts += 1;
                if (enableCudaGatedTokenTensorDecode() and
                    (backend_kind != .cuda or !gpt_arch.cudaDecodeGraphReplayRequired()))
                {
                    if (try decoder_gated_runtime.forwardGreedyTokenFromTokenTensor(
                        &self.cb,
                        self.allocator,
                        self.gpt_config,
                        self.gpt_config.num_hidden_layers,
                        token_ids[seq_len - 1],
                        token_tensor,
                        seq_len,
                        &decode_context,
                    )) |result| {
                        decoder_runtime_debug_stats.device_token_handoff_hits += 1;
                        return .{
                            .token = result.token_id,
                            .token_tensor = result.token_tensor,
                        };
                    }
                }
                if (try gpt_arch.forwardGreedyLastTokenFromTokenTensor(
                    &self.cb,
                    self.allocator,
                    self.gpt_config,
                    token_tensor,
                    1,
                    seq_len,
                    &decode_context,
                )) |result| {
                    decoder_runtime_debug_stats.device_token_handoff_hits += 1;
                    return .{
                        .token = result.token_id,
                        .token_tensor = result.token_tensor,
                    };
                }
                decoder_runtime_debug_stats.device_token_handoff_fallbacks += 1;
            }
        }

        const input_ids = token_ids[seq_len - 1 .. seq_len];
        return .{ .token = try gpt_arch.forwardGreedyLastToken(
            &self.cb,
            self.allocator,
            self.gpt_config,
            input_ids,
            1,
            seq_len,
            &decode_context,
        ) };
    }

    fn forwardGreedyDeviceDecodeTokenTensor(
        self: *NativeGenerationPipeline,
        seq_len: usize,
        tokens_generated: usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
        input_token_tensor: ops.CT,
    ) !?ops.CT {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        decoder_runtime_debug_stats.forward_attempts += 1;
        const backend_kind = self.cb.kind();
        if (backend_kind != .cuda) {
            decoder_runtime_debug_stats.backend_not_device_decode += 1;
            return null;
        }
        if (self.graph_cache != null or self.compiled_partition_backend != null) {
            decoder_runtime_debug_stats.graph_blocked += 1;
            return null;
        }
        if (tokens_generated == 0 and decode_runtime.kvView() == null) {
            decoder_runtime_debug_stats.first_token_blocked += 1;
            return null;
        }
        if (decode_runtime.kvView() == null) {
            decoder_runtime_debug_stats.kv_missing += 1;
            return null;
        }
        if (!isPureGreedyConfig(config)) {
            decoder_runtime_debug_stats.non_greedy += 1;
            return null;
        }
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) {
            decoder_runtime_debug_stats.grammar_blocked += 1;
            return null;
        }
        if (!enableCudaGreedyDeviceTokenHandoff()) return null;

        self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
        const decode_context = decode_runtime.makeDecodeContext(seq_len, 1);
        decoder_runtime_debug_stats.device_token_handoff_attempts += 1;
        if (enableCudaGatedTokenTensorDecode() and !gpt_arch.cudaDecodeGraphReplayRequired()) {
            if (try decoder_gated_runtime.forwardGreedyTokenTensorOnlyFromTokenTensor(
                &self.cb,
                self.allocator,
                self.gpt_config,
                self.gpt_config.num_hidden_layers,
                0,
                input_token_tensor,
                seq_len,
                &decode_context,
            )) |token_tensor| {
                decoder_runtime_debug_stats.device_token_handoff_hits += 1;
                return token_tensor;
            }
        }
        if (try gpt_arch.forwardGreedyLastTokenTensorOnlyFromTokenTensor(
            &self.cb,
            self.allocator,
            self.gpt_config,
            input_token_tensor,
            1,
            seq_len,
            &decode_context,
        )) |token_tensor| {
            decoder_runtime_debug_stats.device_token_handoff_hits += 1;
            return token_tensor;
        }
        decoder_runtime_debug_stats.device_token_handoff_fallbacks += 1;
        return null;
    }

    fn shouldSeedDeviceTokenHandoff(
        self: *NativeGenerationPipeline,
        tokens_generated: usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
    ) bool {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        _ = tokens_generated;
        const backend_kind = self.cb.kind();
        if (!enableCudaGreedyDeviceTokenHandoff()) return false;
        if (backend_kind != .cuda) return false;
        if (self.graph_cache != null or self.compiled_partition_backend != null) return false;
        if (decode_runtime.kvView() == null) return false;
        if (!isPureGreedyConfig(config)) return false;
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) return false;
        return true;
    }

    fn makeDeviceTokenTensor(self: *NativeGenerationPipeline, token_id: usize) !?ops.CT {
        if (self.execution_lock) |mutex| {
            platform.sync.lockYielding(mutex);
        }
        defer if (self.execution_lock) |mutex| mutex.unlock();
        const data = [_]i32{@intCast(token_id)};
        const shape = [_]i32{1};
        return try self.cb.fromInt32Shape(&data, &shape);
    }

    fn freeDeviceTokenTensor(self: *NativeGenerationPipeline, tensor: ops.CT) void {
        if (self.execution_lock) |mutex| {
            platform.sync.lockYielding(mutex);
        }
        defer if (self.execution_lock) |mutex| mutex.unlock();
        self.cb.free(tensor);
    }

    fn forwardLastLogits(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        const query_seq_len = decode_context.query_sequence_len;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.graph_cache) |cache| {
            return self.graphForward(cache, input_ids, batch, seq_len, decode_context);
        }
        if (self.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;

        // CUDA fast path: route the layer stack through the decoder-runtime
        // forward (graph-replay capable, fused kernels, paged attention).
        // This is what keeps sampled (non-greedy) decoding near greedy
        // throughput; the interpreter below runs one op at a time with heavy
        // per-op host overhead. Escape hatch:
        // ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS=0.
        if (self.cb.kind() == .cuda and cudaReplayLastLogitsEnabled()) {
            if (gpt_arch.forwardLastLogitsWithCudaReplay(
                &self.cb,
                self.allocator,
                self.gpt_config,
                input_ids,
                batch,
                seq_len,
                decode_context,
                "gpt.standard_last_logits",
            )) |logits| {
                return logits;
            } else |err| {
                decoder_runtime_debug_stats.replay_last_logits_fallbacks += 1;
                debugGenerationStage(
                    "forwardLastLogits replay path failed err={s}; falling back to interpreter",
                    .{@errorName(err)},
                );
            }
        }

        const logits = try gpt_arch.forward(&self.cb, self.allocator, self.gpt_config, input_ids, batch, seq_len, decode_context);
        defer self.allocator.free(logits);
        const last_pos_offset = (query_seq_len - 1) * @as(usize, @intCast(self.gpt_config.vocab_size));
        return try self.allocator.dupe(f32, logits[last_pos_offset..][0..@intCast(self.gpt_config.vocab_size)]);
    }

    fn forwardGreedyCompiledModelToken(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        config: GenerationConfig,
        has_grammar: bool,
    ) !?usize {
        if (!isPureGreedyConfig(config)) return null;
        if (has_grammar or config.grammar != null) return null;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        const cache = self.graph_cache orelse return null;
        if (self.compiled_partition_backend == null or self.compiled_attachment_target != .whole_model) return null;

        const token_id = (try graph_mod.execution.graphForwardCompiledModelGreedyToken(
            self,
            cache,
            input_ids,
            batch,
            seq_len,
            decode_context,
            self.gpt_config.vocab_size,
        )) orelse return null;
        if (token_id < 0) return error.InvalidModelOutput;
        return @intCast(token_id);
    }

    fn prepareCompiledGenerationRuntime(
        self: *NativeGenerationPipeline,
        kv_tokens_hint: usize,
    ) !bool {
        const cache = self.graph_cache orelse return false;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.compiled_partition_backend == null or self.compiled_attachment_target != .whole_model) return false;
        // ponytail: Metal prefill prepares lazily; eager prepare is only a warmup and can be skipped.
        if (self.compiled_partition_backend == .metal) return false;
        return graph_mod.execution.prepareCompiledModelRuntime(self, cache, kv_tokens_hint);
    }

    fn forwardAllLogits(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.graph_cache) |cache| {
            return graph_mod.execution.graphForwardAll(self, cache, input_ids, batch, seq_len, decode_context);
        }
        if (self.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;

        // Single-item single-token step: "all logits" is exactly one row, so
        // the decoder-runtime replay path applies (see forwardLastLogits).
        // This is the hot path for sampled decoding under the scheduler.
        if (self.cb.kind() == .cuda and batch == 1 and
            decode_context.query_sequence_len == 1 and cudaReplayLastLogitsEnabled())
        {
            if (gpt_arch.forwardLastLogitsWithCudaReplay(
                &self.cb,
                self.allocator,
                self.gpt_config,
                input_ids,
                batch,
                seq_len,
                decode_context,
                "gpt.standard_last_logits",
            )) |logits| {
                return logits;
            } else |err| {
                decoder_runtime_debug_stats.replay_last_logits_fallbacks += 1;
                debugGenerationStage(
                    "forwardAllLogits replay path failed err={s}; falling back to interpreter",
                    .{@errorName(err)},
                );
            }
        }
        return gpt_arch.forward(&self.cb, self.allocator, self.gpt_config, input_ids, batch, seq_len, decode_context);
    }

    const ForwardAllWithHiddenHost = struct {
        allocator: std.mem.Allocator,
        logits: []f32,
        hidden: []f32,
        pre_norm_hidden: []f32,
        rows: usize,

        fn deinit(self: *ForwardAllWithHiddenHost) void {
            self.allocator.free(self.logits);
            self.allocator.free(self.hidden);
            self.allocator.free(self.pre_norm_hidden);
            self.* = undefined;
        }
    };

    const ForwardAllWithHiddenDevice = struct {
        cb: *const ComputeBackend,
        logits: ops.CT,
        hidden: ops.CT,
        pre_norm_hidden: ops.CT,
        rows: usize,

        fn deinit(self: *ForwardAllWithHiddenDevice) void {
            self.cb.free(self.logits);
            self.cb.free(self.hidden);
            self.cb.free(self.pre_norm_hidden);
            self.* = undefined;
        }
    };

    const ForwardHiddenDevice = struct {
        cb: *const ComputeBackend,
        hidden: ops.CT,
        pre_norm_hidden: ?ops.CT,
        rows: usize,
        greedy_token_id: ?u32 = null,
        prepared_tail_choices: ?[]u32 = null,
        choices_allocator: ?std.mem.Allocator = null,

        fn deinit(self: *ForwardHiddenDevice) void {
            self.cb.free(self.hidden);
            if (self.pre_norm_hidden) |pre_norm| self.cb.free(pre_norm);
            if (self.prepared_tail_choices) |choices| self.choices_allocator.?.free(choices);
            self.* = undefined;
        }
    };

    const Gemma4MtpActivationState = struct {
        const DraftEmbeddingCacheEntry = struct {
            token_id: i64,
            tensor: ops.CT,
        };

        allocator: std.mem.Allocator,
        draft_cb: *const ComputeBackend,
        host: ?[]f32 = null,
        device: ?ops.CT = null,
        cached_target_choice: ?u32 = null,
        /// Defer-materialize fold: a committed token (already counted by
        /// seq_len and present in token_ids) whose target forward/KV write is
        /// deferred to the next verify round. Invariant at every round
        /// boundary: decode_state.total_tokens + pendingCount() == seq_len.
        pending_unmaterialized: bool = false,
        draft_embedding_cache: std.ArrayListUnmanaged(DraftEmbeddingCacheEntry) = .empty,

        fn deinit(self: *Gemma4MtpActivationState) void {
            if (self.host) |activation| self.allocator.free(activation);
            if (self.device) |tensor| self.draft_cb.free(tensor);
            for (self.draft_embedding_cache.items) |entry| self.draft_cb.free(entry.tensor);
            self.draft_embedding_cache.deinit(self.allocator);
            self.* = undefined;
        }

        fn replaceHost(self: *Gemma4MtpActivationState, activation: []f32) void {
            if (self.host) |old| self.allocator.free(old);
            if (self.device) |old| self.draft_cb.free(old);
            self.host = activation;
            self.device = null;
            self.cached_target_choice = null;
        }

        fn replaceDevice(self: *Gemma4MtpActivationState, tensor: ops.CT) void {
            if (self.host) |old| self.allocator.free(old);
            if (self.device) |old| self.draft_cb.free(old);
            self.host = null;
            self.device = tensor;
            self.cached_target_choice = null;
        }

        fn residentReady(self: *const Gemma4MtpActivationState) bool {
            return self.device != null;
        }

        fn pendingCount(self: *const Gemma4MtpActivationState) usize {
            return @intFromBool(self.pending_unmaterialized);
        }

        /// Every committed token except the pending one must have KV written.
        /// This is a production invariant: a mismatch shifts the verify suffix
        /// write onto the wrong KV rows, so fail closed in ReleaseFast too.
        fn validateKvTokensInSync(self: *const Gemma4MtpActivationState, decode_state: *const NativeDecodeState, seq_len: usize) !void {
            const accounted_tokens = std.math.add(usize, decode_state.total_tokens, self.pendingCount()) catch
                return error.InvalidSpeculativeState;
            if (accounted_tokens != seq_len) return error.InvalidSpeculativeState;
        }

        fn notePendingUnmaterialized(self: *Gemma4MtpActivationState) !void {
            if (self.pending_unmaterialized) return error.InvalidSpeculativeState;
            self.pending_unmaterialized = true;
        }

        fn notePendingKvWritten(self: *Gemma4MtpActivationState) void {
            self.pending_unmaterialized = false;
        }

        fn cachedDraftEmbedding(self: *const Gemma4MtpActivationState, token_id: i64) ?ops.CT {
            for (self.draft_embedding_cache.items) |entry| {
                if (entry.token_id == token_id) return entry.tensor;
            }
            return null;
        }

        fn rememberDraftEmbedding(self: *Gemma4MtpActivationState, token_id: i64, tensor: ops.CT, limit: usize) !usize {
            if (limit == 0) {
                self.draft_cb.free(tensor);
                return 0;
            }
            if (self.cachedDraftEmbedding(token_id) != null) {
                self.draft_cb.free(tensor);
                return 0;
            }
            var evictions: usize = 0;
            while (self.draft_embedding_cache.items.len >= limit) {
                const old = self.draft_embedding_cache.orderedRemove(0);
                self.draft_cb.free(old.tensor);
                evictions += 1;
            }
            try self.draft_embedding_cache.append(self.allocator, .{
                .token_id = token_id,
                .tensor = tensor,
            });
            return evictions;
        }
    };

    fn replaceGemma4MtpActivationFromPrompt(
        self: *NativeGenerationPipeline,
        mtp_activation: *Gemma4MtpActivationState,
        prompt_ids: []const i64,
        seq_len: usize,
        hidden_source: Gemma4MtpTargetHiddenSource,
        use_resident_draft: bool,
        draft_cb: *const ComputeBackend,
    ) !void {
        if (seq_len == 0 or prompt_ids.len < seq_len) return error.InvalidTensorShape;
        var seed_state = NativeDecodeState.initContiguous(self.allocator);
        defer seed_state.deinit();
        seed_state.configureForGptConfig(self.gpt_config);
        var seed_runtime = BorrowedDecodeStateRuntime.init(&seed_state);
        const seed_ctx = seed_runtime.makeDecodeContext(seq_len, seq_len);
        var target_hidden = try self.forwardMtpTargetHiddenDevice(
            prompt_ids[0..seq_len],
            1,
            seq_len,
            &seed_ctx,
            hidden_source,
            "gpt.mtp_seed_prompt_hidden",
        );
        defer target_hidden.deinit();

        const row = seq_len - 1;
        if (use_resident_draft) {
            if (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, draft_cb)) |activation| {
                mtp_activation.replaceDevice(activation);
                return;
            }
        }
        mtp_activation.replaceHost(try self.dupeMtpTargetHiddenRowFromHiddenDevice(&target_hidden, row, hidden_source));
    }

    fn replaceGemma4MtpActivationFromPrefillHidden(
        self: *NativeGenerationPipeline,
        mtp_activation: *Gemma4MtpActivationState,
        hidden: ops.CT,
        rows: usize,
        hidden_source: Gemma4MtpTargetHiddenSource,
        use_resident_draft: bool,
        draft_cb: *const ComputeBackend,
    ) !bool {
        if (rows == 0) return error.InvalidTensorShape;
        if (hidden_source != .final) return false;
        const target_hidden = ForwardHiddenDevice{
            .cb = &self.cb,
            .hidden = hidden,
            .pre_norm_hidden = null,
            .rows = rows,
        };
        const row = rows - 1;
        if (use_resident_draft) {
            if (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, draft_cb)) |activation| {
                mtp_activation.replaceDevice(activation);
                return true;
            }
        }
        mtp_activation.replaceHost(try self.dupeMtpTargetHiddenRowFromHiddenDevice(&target_hidden, row, hidden_source));
        return true;
    }

    fn cachedMtpDraftTargetEmbedding(
        self: *NativeGenerationPipeline,
        draft_pipeline: *NativeGenerationPipeline,
        mtp_activation: *Gemma4MtpActivationState,
        token_id: i64,
        mtp_profile: ?*MtpProfileStats,
    ) !?ops.CT {
        const limit = gemma4MtpDraftEmbeddingCacheSize();
        if (limit == 0) {
            if (mtp_profile) |profile| {
                if (profile.enabled) profile.draft_embedding_cache_disabled += 1;
            }
            return null;
        }
        if (mtp_activation.cachedDraftEmbedding(token_id)) |cached| {
            if (mtp_profile) |profile| {
                if (profile.enabled) profile.draft_embedding_cache_hits += 1;
            }
            return cached;
        }
        if (mtp_profile) |profile| {
            if (profile.enabled) profile.draft_embedding_cache_misses += 1;
        }

        const backbone_hidden: usize = @intCast(draft_pipeline.gpt_config.mtp_backbone_hidden_size);
        const target_hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (backbone_hidden == 0 or target_hidden_size != backbone_hidden) return null;
        const target_embed_w = try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config);
        defer self.cb.free(target_embed_w);
        const token_arr = [_]i64{token_id};
        const target_embedded = try self.cb.embeddingLookup(target_embed_w, &token_arr, 1, backbone_hidden);
        const target_embedding = try gpt_arch.maybeScaleTokenEmbeddings(
            &self.cb,
            self.allocator,
            self.gpt_config,
            target_embedded,
            1,
            backbone_hidden,
        );
        defer self.cb.free(target_embedding);
        const copied = (try draft_pipeline.cb.copyTensorFromBackend(&self.cb, target_embedding)) orelse return null;
        errdefer draft_pipeline.cb.free(copied);
        const evictions = try mtp_activation.rememberDraftEmbedding(token_id, copied, limit);
        if (mtp_profile) |profile| {
            if (profile.enabled) {
                profile.draft_embedding_cache_inserts += 1;
                profile.draft_embedding_cache_evictions += evictions;
            }
        }
        return copied;
    }

    fn forwardAllLogitsAndHiddenHost(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardAllWithHiddenHost {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        const embed_w = try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config);
        defer self.cb.free(embed_w);
        const embedded = try self.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(&self.cb, allocator, self.gpt_config, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(&self.cb, allocator, self.gpt_config, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.cb.free(pv);

        const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
            &self.cb,
            allocator,
            self.gpt_config,
            hidden_input,
            .{},
            batch,
            seq_len,
            decode_context,
            ple_vectors,
        );
        defer self.cb.free(hidden_result.final_hidden);
        defer self.cb.free(hidden_result.pre_norm_hidden);

        const lm_w = try gpt_arch.getLmHeadWeight(&self.cb, self.gpt_config);
        defer self.cb.free(lm_w);

        const logits_ct = try self.cb.linearNoBias(
            hidden_result.final_hidden,
            lm_w,
            hidden_result.total_rows,
            self.gpt_config.hidden_size,
            self.gpt_config.vocab_size,
        );
        defer self.cb.free(logits_ct);

        const logits_host = try self.cb.toFloat32(logits_ct, allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.gpt_config, logits_host);
        errdefer allocator.free(logits_host);
        const hidden_host = try self.cb.toFloat32(hidden_result.final_hidden, allocator);
        errdefer allocator.free(hidden_host);
        const pre_norm_hidden_host = try self.cb.toFloat32(hidden_result.pre_norm_hidden, allocator);
        errdefer allocator.free(pre_norm_hidden_host);
        return .{
            .allocator = allocator,
            .logits = logits_host,
            .hidden = hidden_host,
            .pre_norm_hidden = pre_norm_hidden_host,
            .rows = hidden_result.total_rows,
        };
    }

    fn forwardHiddenDevice(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardHiddenDevice {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        const embed_w = try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config);
        defer self.cb.free(embed_w);
        const embedded = try self.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(&self.cb, allocator, self.gpt_config, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(&self.cb, allocator, self.gpt_config, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.cb.free(pv);

        const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
            &self.cb,
            allocator,
            self.gpt_config,
            hidden_input,
            .{},
            batch,
            seq_len,
            decode_context,
            ple_vectors,
        );
        errdefer self.cb.free(hidden_result.final_hidden);
        errdefer self.cb.free(hidden_result.pre_norm_hidden);

        return .{
            .cb = &self.cb,
            .hidden = hidden_result.final_hidden,
            .pre_norm_hidden = hidden_result.pre_norm_hidden,
            .rows = hidden_result.total_rows,
        };
    }

    fn tryMetalGemmaFinalHiddenFrame(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        retained_prefix_tokens: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !?ForwardHiddenDevice {
        if (self.compiled_partition_backend != null or
            self.cb.kind() != .metal or
            self.gpt_config.family != .gemma or
            self.gpt_config.usesMoe() or
            !self.gpt_config.hasPle() or
            batch != 1 or
            input_ids.len == 0 or
            // Prompt-cache hits attach retained host blocks to a fresh
            // sequence. Until those rows have been materialized into the new
            // Metal slot, the direct frame may only own uncached prefixes.
            !metalFinalHiddenFrameOwnsResidentPrefix(retained_prefix_tokens) or
            decode_context.attention_mode != .paged_prefill or
            decode_context.query_sequence_len != input_ids.len)
        {
            return null;
        }

        const Attempt = struct {
            pipeline: *NativeGenerationPipeline,
            input_ids: []const i64,
            seq_len: usize,
            decode_context: *const gpt_arch.DecodeContext,
            result: ?ForwardHiddenDevice = null,

            fn run(context: *anyopaque) !bool {
                const attempt: *@This() = @ptrCast(@alignCast(context));
                const pipeline = attempt.pipeline;
                const prepared = try pipeline.cb.decoderRuntimePrepareOrReuseFamily(
                    pipeline.allocator,
                    pipeline.gpt_config,
                    attempt.decode_context.kv_sequence_len,
                    pipeline.gpt_config.num_hidden_layers,
                );
                if (!prepared.prepared) return false;

                const direct = (try decoder_gated_runtime.forwardFinalHiddenRows(
                    &pipeline.cb,
                    pipeline.allocator,
                    pipeline.gpt_config,
                    pipeline.gpt_config.num_hidden_layers,
                    attempt.input_ids,
                    attempt.seq_len,
                    attempt.decode_context,
                )) orelse return false;
                attempt.result = .{
                    .cb = &pipeline.cb,
                    .hidden = direct.final_hidden,
                    .pre_norm_hidden = null,
                    .rows = direct.rows,
                    .prepared_tail_choices = direct.prepared_tail_choices,
                    .choices_allocator = direct.choices_allocator,
                };
                return true;
            }
        };

        var attempt = Attempt{
            .pipeline = self,
            .input_ids = input_ids,
            .seq_len = seq_len,
            .decode_context = decode_context,
        };
        errdefer if (attempt.result) |result_value| {
            var result = result_value;
            result.deinit();
        };
        if (!try runWithDecoderRuntimeResetBoundary(&self.cb, @ptrCast(&attempt), Attempt.run)) return null;
        const result = attempt.result orelse return null;
        attempt.result = null;
        return result;
    }

    fn forwardFinalHiddenDevice(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        retained_prefix_tokens: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardHiddenDevice {
        return (try self.tryMetalGemmaFinalHiddenFrame(input_ids, batch, seq_len, retained_prefix_tokens, decode_context)) orelse
            self.forwardHiddenDevice(input_ids, batch, seq_len, decode_context);
    }

    fn forwardMtpTargetHiddenDevice(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: Gemma4MtpTargetHiddenSource,
        replay_label: []const u8,
    ) !ForwardHiddenDevice {
        return self.forwardMtpTargetHiddenDeviceReplayMode(
            input_ids,
            batch,
            seq_len,
            decode_context,
            hidden_source,
            replay_label,
            gemma4MtpUnsafeTargetReplayEnabled(),
            null,
        );
    }

    fn forwardMtpTargetHiddenDevicePreparedArgmax(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: Gemma4MtpTargetHiddenSource,
        replay_label: []const u8,
        suppress_token_ids: []const i32,
    ) !ForwardHiddenDevice {
        return self.forwardMtpTargetHiddenDeviceReplayMode(
            input_ids,
            batch,
            seq_len,
            decode_context,
            hidden_source,
            replay_label,
            gemma4MtpUnsafeTargetReplayEnabled(),
            suppress_token_ids,
        );
    }

    fn forwardMtpTargetHiddenDeviceNoReplay(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: Gemma4MtpTargetHiddenSource,
    ) !ForwardHiddenDevice {
        return self.forwardMtpTargetHiddenDeviceReplayMode(
            input_ids,
            batch,
            seq_len,
            decode_context,
            hidden_source,
            "",
            false,
            null,
        );
    }

    fn forwardMtpTargetHiddenDeviceReplayMode(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: Gemma4MtpTargetHiddenSource,
        replay_label: []const u8,
        allow_replay: bool,
        prepared_tail_suppress_token_ids: ?[]const i32,
    ) !ForwardHiddenDevice {
        if (hidden_source != .final) {
            return self.forwardHiddenDevice(input_ids, batch, seq_len, decode_context);
        }
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        if (self.cb.kind() == .metal) {
            const verify_trace = enableGemma4MtpVerifyTrace();
            const trace_stats_before = if (verify_trace) decoder_gated_runtime.getTimingStats() else undefined;
            const trace_started_at = mtpProfileTimestamp(verify_trace, self.io);
            const direct_result = if (prepared_tail_suppress_token_ids) |suppress_token_ids|
                try decoder_gated_runtime.forwardFinalHiddenRowsPreparedArgmax(
                    &self.cb,
                    allocator,
                    self.gpt_config,
                    self.gpt_config.num_hidden_layers,
                    input_ids,
                    seq_len,
                    decode_context,
                    suppress_token_ids,
                )
            else
                try decoder_gated_runtime.forwardFinalHiddenRows(
                    &self.cb,
                    allocator,
                    self.gpt_config,
                    self.gpt_config.num_hidden_layers,
                    input_ids,
                    seq_len,
                    decode_context,
                );
            if (direct_result) |direct| {
                if (verify_trace) {
                    const wall_ns = mtpProfileElapsedNs(true, self.io, trace_started_at);
                    const after = decoder_gated_runtime.getTimingStats();
                    std.debug.print(
                        "mtp-verify-trace: rows={d} wall_us={d} embed_us={d} ple_us={d} frame_begin_us={d} layer_spec_us={d} plan_us={d} execute_us={d} block_us={d} finish_us={d}\n",
                        .{
                            direct.rows,
                            @divTrunc(wall_ns, std.time.ns_per_us),
                            @as(u64, @intCast(@divTrunc(after.prefill_embed_nanos - trace_stats_before.prefill_embed_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_ple_prepare_nanos - trace_stats_before.prefill_ple_prepare_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_begin_nanos - trace_stats_before.prefill_frame_begin_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_layer_spec_nanos - trace_stats_before.prefill_frame_layer_spec_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_plan_nanos - trace_stats_before.prefill_frame_plan_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_execute_nanos - trace_stats_before.prefill_frame_execute_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_block_nanos - trace_stats_before.prefill_block_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_finish_nanos - trace_stats_before.prefill_frame_finish_nanos, std.time.ns_per_us))),
                        },
                    );
                }
                return .{
                    .cb = &self.cb,
                    .hidden = direct.final_hidden,
                    .pre_norm_hidden = null,
                    .rows = direct.rows,
                    .prepared_tail_choices = direct.prepared_tail_choices,
                    .choices_allocator = direct.choices_allocator,
                };
            }
        }

        const embed_w = try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config);
        defer self.cb.free(embed_w);
        const embedded = try self.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(&self.cb, allocator, self.gpt_config, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(&self.cb, allocator, self.gpt_config, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.cb.free(pv);

        const hidden_result = if (allow_replay)
            try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0OverridesCudaReplay(
                &self.cb,
                allocator,
                self.gpt_config,
                hidden_input,
                .{},
                batch,
                seq_len,
                decode_context,
                ple_vectors,
                replay_label,
            )
        else
            try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
                &self.cb,
                allocator,
                self.gpt_config,
                hidden_input,
                .{},
                batch,
                seq_len,
                decode_context,
                ple_vectors,
            );
        errdefer self.cb.free(hidden_result.hidden);

        return .{
            .cb = &self.cb,
            .hidden = hidden_result.hidden,
            .pre_norm_hidden = null,
            .rows = hidden_result.total_rows,
        };
    }

    fn forwardAllLogitsAndHiddenDevice(
        self: *NativeGenerationPipeline,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardAllWithHiddenDevice {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        const embed_w = try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config);
        defer self.cb.free(embed_w);
        const embedded = try self.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(&self.cb, allocator, self.gpt_config, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(&self.cb, allocator, self.gpt_config, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.cb.free(pv);

        const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
            &self.cb,
            allocator,
            self.gpt_config,
            hidden_input,
            .{},
            batch,
            seq_len,
            decode_context,
            ple_vectors,
        );
        errdefer self.cb.free(hidden_result.final_hidden);
        errdefer self.cb.free(hidden_result.pre_norm_hidden);

        const lm_w = try gpt_arch.getLmHeadWeight(&self.cb, self.gpt_config);
        defer self.cb.free(lm_w);

        const logits_ct = try self.cb.linearNoBias(
            hidden_result.final_hidden,
            lm_w,
            hidden_result.total_rows,
            self.gpt_config.hidden_size,
            self.gpt_config.vocab_size,
        );
        errdefer self.cb.free(logits_ct);

        return .{
            .cb = &self.cb,
            .logits = logits_ct,
            .hidden = hidden_result.final_hidden,
            .pre_norm_hidden = hidden_result.pre_norm_hidden,
            .rows = hidden_result.total_rows,
        };
    }

    fn dupeMtpTargetHiddenRow(
        self: *NativeGenerationPipeline,
        result: *const ForwardAllWithHiddenHost,
        row: usize,
        source: Gemma4MtpTargetHiddenSource,
    ) ![]f32 {
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden,
        };
        if (hidden.len != result.rows * hidden_size) return error.InvalidTensorShape;
        return try self.allocator.dupe(f32, hidden[row * hidden_size ..][0..hidden_size]);
    }

    fn dupeMtpTargetHiddenRowFromDevice(
        self: *NativeGenerationPipeline,
        result: *const ForwardAllWithHiddenDevice,
        row: usize,
        source: Gemma4MtpTargetHiddenSource,
    ) ![]f32 {
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden,
        };
        const row_ct = try self.cb.sliceRows2D(self.allocator, hidden, row, 1, hidden_size);
        defer self.cb.free(row_ct);
        const row_host = try self.cb.toFloat32(row_ct, self.allocator);
        errdefer self.allocator.free(row_host);
        if (row_host.len != hidden_size) return error.InvalidTensorShape;
        return row_host;
    }

    fn dupeMtpTargetHiddenRowFromHiddenDevice(
        self: *NativeGenerationPipeline,
        result: *const ForwardHiddenDevice,
        row: usize,
        source: Gemma4MtpTargetHiddenSource,
    ) ![]f32 {
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden orelse return error.MissingMaterializedHiddenState,
        };
        const row_ct = try self.cb.sliceRows2D(self.allocator, hidden, row, 1, hidden_size);
        defer self.cb.free(row_ct);
        const row_host = try self.cb.toFloat32(row_ct, self.allocator);
        errdefer self.allocator.free(row_host);
        if (row_host.len != hidden_size) return error.InvalidTensorShape;
        return row_host;
    }

    fn copyMtpTargetHiddenRowToBackend(
        self: *NativeGenerationPipeline,
        result: *const ForwardAllWithHiddenDevice,
        row: usize,
        source: Gemma4MtpTargetHiddenSource,
        dst_cb: *const ComputeBackend,
    ) !?ops.CT {
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden,
        };
        const row_ct = try self.cb.sliceRows2D(self.allocator, hidden, row, 1, hidden_size);
        defer self.cb.free(row_ct);
        return try dst_cb.copyTensorFromBackend(&self.cb, row_ct);
    }

    fn copyMtpTargetHiddenRowFromHiddenToBackend(
        self: *NativeGenerationPipeline,
        result: *const ForwardHiddenDevice,
        row: usize,
        source: Gemma4MtpTargetHiddenSource,
        dst_cb: *const ComputeBackend,
    ) !?ops.CT {
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden orelse return error.MissingMaterializedHiddenState,
        };
        const row_ct = try self.cb.sliceRows2D(self.allocator, hidden, row, 1, hidden_size);
        defer self.cb.free(row_ct);
        return try dst_cb.copyTensorFromBackend(&self.cb, row_ct);
    }

    /// Run the forward pass through the graph IR: trace once, cache the
    /// graph, and replay it via the interpreter on subsequent calls.
    /// Returns logits for the last position, same as the eager path.
    fn graphForward(
        self: *NativeGenerationPipeline,
        cache: *graph_mod.cache.GraphCache,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        const allocator = self.allocator;
        if (try graph_mod.execution.graphForwardCompiledModelLast(self, cache, input_ids, batch, seq_len, decode_context)) |last_logits| {
            return last_logits;
        }
        if (self.compiled_partition_backend != null and self.compiled_attachment_target == .whole_model) {
            return error.MissingCompiledModelRuntime;
        }

        const logits = try graph_mod.execution.graphForwardAll(self, cache, input_ids, batch, seq_len, decode_context);
        defer allocator.free(logits);
        const query_seq_len = decode_context.query_sequence_len;
        const vocab_size = self.gpt_config.vocab_size;

        // Build cache key.
        const attn_mode: graph_mod.cache.AttentionMode = switch (decode_context.attention_mode) {
            .full_recompute => .full_recompute,
            .paged_prefill => .paged_prefill,
            .paged_decode => .paged_decode,
        };
        const key = graph_mod.cache.CacheKey{
            .config_hash = graph_mod.cache.hashConfigBytes(std.mem.asBytes(&self.gpt_config)),
            .batch = @intCast(batch),
            .seq_len = if (attn_mode == .paged_prefill) graph_mod.cache.bucketSeqLen(@intCast(query_seq_len)) else @intCast(query_seq_len),
            .attention_mode = attn_mode,
        };

        // Cache lookup — trace on miss.
        if (cache.get(key) == null) {
            var tc = graph_mod.tracing_compute.TracingCompute.init(allocator);
            var tc_cb = tc.backend();

            // Trace the forward pass. Null out moe_runtime so tracing
            // takes the local-batches path (traces grouped MoE ops).
            var trace_dc = decode_context.*;
            trace_dc.moe_runtime = null;
            const dummy_logits = try gpt_arch.forward(&tc_cb, allocator, self.gpt_config, input_ids, batch, seq_len, &trace_dc);
            allocator.free(dummy_logits);

            // Extract the raw traced graph, then deinit the tracer.
            var raw_graph = tc.extractGraph();
            tc.deinit();

            // Run optimization passes (constant folding, algebraic
            // simplifications, linear pair fusion, CSE) before caching.
            const optimized = try graph_mod.passes.pipeline.Pipeline.default.run(allocator, &raw_graph);
            raw_graph.deinit();
            // Cache takes ownership of the optimized graph.
            try cache.put(key, optimized.graph);
        }

        const entry = cache.getEntry(key).?;
        const graph = &entry.graph;

        // Populate caches on first execution: weight tensors and
        // graph analysis (reachable set + last-use). Both are
        // invariant across decode steps and expensive to recompute.
        if (entry.weight_inputs == null) {
            const params = graph.parameters.items;
            const wc = try allocator.alloc(graph_mod.interpreter.RuntimeInput, params.len);
            for (params, 0..) |param_id, idx| {
                const name = graph.parameterName(graph.node(param_id));
                const value = self.graphWeight(name) catch |err| {
                    std.log.err("graph mode missing parameter: {s}", .{name});
                    return err;
                };
                wc[idx] = .{
                    .node_id = param_id,
                    .value = value,
                };
            }
            entry.weight_inputs = wc;
        }
        if (entry.cached_analysis == null) {
            entry.cached_analysis = try graph_mod.interpreter.CachedAnalysis.compute(allocator, graph);
        }

        // Build interpreter options with current decode state.
        const exec_options = graph_mod.interpreter.ExecuteOptions{
            .attention = gpt_arch.attentionContextFromDecode(decode_context),
            .embedding_ids = input_ids,
            .runtime_inputs = entry.weight_inputs,
            .cached_analysis = entry.cached_analysis,
        };

        // Multi-device path: partition the graph across the device mesh.
        if (self.device_mesh) |mesh| {
            const config = self.parallel_config orelse graph_mod.parallel_strategy.ParallelConfig{
                .strategy = .single,
                .num_devices = @intCast(mesh.deviceCount()),
            };
            var dpp = try graph_mod.parallel_strategy.planParallel(allocator, graph, config);
            defer dpp.deinit();

            // Compile PJRT/HLO executors for eligible partitions (cached across steps).
            if (build_options.enable_pjrt) {
                if (self.pjrt_client) |client| {
                    try attachPjrtExecutors(allocator, entry, graph, &dpp, &self.cb, client);
                }
            }

            var multi_result = try graph_mod.multi_executor.executeMultiDevice(allocator, graph, &dpp, mesh, exec_options);
            defer multi_result.deinit(mesh);

            if (traceGraphExecutorOutputs()) std.debug.print(
                "graph_executor_output_trace: multi_result outputs={d} first_device={d}\n",
                .{ multi_result.outputs.len, multi_result.output_devices[0] },
            );
            // Output is on whichever device produced it; transfer to f32.
            const out_dev = mesh.device(multi_result.output_devices[0]).?;
            if (traceGraphExecutorOutputs()) std.debug.print(
                "graph_executor_output_trace: to_float32_begin backend={s}\n",
                .{@tagName(out_dev.backend.kind())},
            );
            const multi_logits = try out_dev.backend.toFloat32(multi_result.outputs[0], allocator);
            defer allocator.free(multi_logits);
            if (traceGraphExecutorOutputs()) std.debug.print(
                "graph_executor_output_trace: to_float32_end len={d} query_seq_len={d} vocab_size={d}\n",
                .{ multi_logits.len, query_seq_len, vocab_size },
            );

            const last_pos_offset = (query_seq_len - 1) * @as(usize, @intCast(vocab_size));
            if (traceGraphExecutorOutputs()) std.debug.print(
                "graph_executor_output_trace: slice offset={d} len={d}\n",
                .{ last_pos_offset, @as(usize, @intCast(vocab_size)) },
            );
            return try allocator.dupe(f32, multi_logits[last_pos_offset..][0..@intCast(vocab_size)]);
        }

        // Single-device interpreter path (no partitioning).
        var result = try graph_mod.interpreter.execute(allocator, graph, &self.cb, exec_options);
        defer result.deinit(&self.cb);

        // The graph's last output is the logits toFloat32 tensor.
        // Earlier outputs may be spurious (from CPU-side toFloat32 calls in
        // norm weight adjustment, expert scales, etc.). Use the last one.
        const compiled_logits = try self.cb.toFloat32(result.outputs[result.outputs.len - 1], allocator);
        defer allocator.free(compiled_logits);

        // Extract last position logits (same as eager path).
        const last_pos_offset = (query_seq_len - 1) * @as(usize, @intCast(vocab_size));
        return try self.allocator.dupe(f32, compiled_logits[last_pos_offset..][0..@intCast(vocab_size)]);
    }

    pub fn graphWeight(self: *NativeGenerationPipeline, name: []const u8) !ops.CT {
        const weight = if (self.gpt_config.weight_prefix.len != 0 and std.mem.startsWith(u8, name, "model."))
            gpt_arch.getModelWeight(&self.cb, self.gpt_config, name)
        else
            self.cb.getWeight(name);
        return weight catch |err| switch (err) {
            error.MissingWeight, error.WeightNotFound => {
                if (std.mem.eql(u8, name, "lm_head.weight")) {
                    return switch (self.gpt_config.family) {
                        .gpt2 => self.cb.getWeight("wte.weight"),
                        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config),
                        else => self.cb.getWeight("model.embed_tokens.weight") catch try self.cb.getWeight("wte.weight"),
                    };
                }
                var fallback_buf: [128]u8 = undefined;
                if (self.graphOmittedVProjFallback(name, &fallback_buf)) |fallback_name| {
                    return self.cb.getWeight(fallback_name);
                }
                if (self.graphOptionalRouterInputScale(name)) {
                    const ones = try self.allocator.alloc(f32, self.gpt_config.hidden_size);
                    defer self.allocator.free(ones);
                    @memset(ones, 1.0);
                    const shape = [_]i32{@intCast(self.gpt_config.hidden_size)};
                    return self.cb.fromFloat32Shape(ones, &shape);
                }
                if (self.graphOptionalExpertOutputScale(name)) {
                    const ones = try self.allocator.alloc(f32, self.gpt_config.num_local_experts);
                    defer self.allocator.free(ones);
                    @memset(ones, 1.0);
                    const shape = [_]i32{@intCast(self.gpt_config.num_local_experts)};
                    return self.cb.fromFloat32Shape(ones, &shape);
                }
                return err;
            },
            else => return err,
        };
    }

    fn graphOptionalRouterInputScale(_: *NativeGenerationPipeline, name: []const u8) bool {
        const prefix = "model.layers.";
        const suffix = ".block_sparse_moe.gate.input_scale";
        return std.mem.startsWith(u8, name, prefix) and std.mem.endsWith(u8, name, suffix);
    }

    fn graphOptionalExpertOutputScale(self: *NativeGenerationPipeline, name: []const u8) bool {
        if (self.gpt_config.num_local_experts == 0) return false;
        const prefix = "model.layers.";
        const suffix = ".block_sparse_moe.expert_output_scale";
        return std.mem.startsWith(u8, name, prefix) and std.mem.endsWith(u8, name, suffix);
    }

    fn graphOmittedVProjFallback(self: *NativeGenerationPipeline, name: []const u8, buf: *[128]u8) ?[]const u8 {
        const prefix = "model.layers.";
        const suffix = ".self_attn.v_proj.weight";
        if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
        const layer_text = name[prefix.len .. name.len - suffix.len];
        const layer = std.fmt.parseInt(usize, layer_text, 10) catch return null;
        if (!self.gpt_config.layerOmitsVProj(layer)) return null;
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer}) catch null;
    }

    /// Speculative decoding: use `draft_pipeline` to propose `k` candidate
    /// tokens, then verify them against the target model in one forward pass.
    ///
    /// Returns the number of accepted tokens (0..k+1, where k+1 means all
    /// drafts matched and the target model provided the bonus k+1-th token).
    /// The accepted tokens are written into `token_ids[seq_len..]` and
    /// `seq_len` is advanced accordingly.
    fn speculativeDecode(
        self: *NativeGenerationPipeline,
        draft_pipeline: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: *usize,
        decode_state: *NativeDecodeState,
        draft_decode_state: *NativeDecodeState,
        config: GenerationConfig,
        k: usize,
        remaining_generation_tokens: usize,
        penalty_state: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
    ) !SpeculativeRoundResult {
        const allocator = self.allocator;
        const round_start = seq_len.*;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var draft_runtime = BorrowedDecodeStateRuntime.init(draft_decode_state);
        var round_penalties = try penalty_state.clone(allocator);
        defer round_penalties.deinit(allocator);

        // --- Draft phase: generate K candidate tokens autoregressively ---
        var draft_tokens: [16]i64 = undefined;
        const actual_k = @min(k, 16);
        var draft_count: usize = 0;

        try setWorkloadProfileRegime(&self.cb, .speculative_draft);
        try setWorkloadProfileRegime(&draft_pipeline.cb, .speculative_draft);

        for (0..actual_k) |di| {
            _ = di;
            // Run draft model forward on the last token
            const draft_seq = seq_len.* + draft_count;
            const draft_query_len: usize = if (draft_runtime.kvView() != null and draft_count > 0) 1 else if (draft_runtime.kvView() != null) 1 else draft_seq;
            const draft_ctx = draft_runtime.makeDecodeContext(draft_seq, draft_query_len);
            const draft_input = if (draft_query_len == draft_seq)
                token_ids[0..draft_seq]
            else
                token_ids[draft_seq - 1 .. draft_seq];

            const draft_logits = try draft_pipeline.forwardAllLogits(draft_input, 1, draft_seq, &draft_ctx);
            defer allocator.free(draft_logits);

            // Greedy sample from draft
            const last_offset = (draft_query_len - 1) * draft_pipeline.gpt_config.vocab_size;
            const draft_token = activations.argmax(draft_logits[last_offset..][0..draft_pipeline.gpt_config.vocab_size]);

            draft_tokens[draft_count] = @intCast(draft_token);
            token_ids[seq_len.* + draft_count] = @intCast(draft_token);
            draft_count += 1;

            // Advance draft KV cache
            _ = try draft_runtime.appendGeneratedToken();
        }

        if (draft_count == 0) return .{
            .drafted = 0,
            .matched_drafts = 0,
            .accepted = 0,
            .correction_added = false,
            .had_bonus = false,
            .hit_eos = false,
            .hit_grammar_stop = false,
        };

        try setWorkloadProfileRegime(&self.cb, .speculative_verify);

        // --- Verify phase: run target model on all draft positions at once ---
        // We need logits for positions seq_len-1 .. seq_len+draft_count-1
        // (seq_len-1 gives us the logit that should predict token_ids[seq_len],
        //  and so on through seq_len+draft_count-1 which predicts the bonus token)
        const verify_len = draft_count + 1; // +1 so we get logits for the last draft token too
        const verify_seq = seq_len.* + draft_count;

        const target_query_len: usize = if (decode_runtime.kvView() != null) verify_len else verify_seq;
        // Validate the real batched shape before extending target KV. The A4B
        // prepared decoder is currently qLen=1-only and must fail closed here.
        try reserveSpeculativeTargetVerification(
            &decode_runtime,
            self.cb.kind(),
            self.gpt_config,
            target_query_len,
            draft_count,
        );
        const target_ctx = decode_runtime.makeDecodeContext(verify_seq, target_query_len);
        // Input: the last token before drafts + all draft tokens
        const verify_start = if (target_query_len == verify_seq) 0 else verify_seq - target_query_len;
        const verify_input = token_ids[verify_start..verify_seq];

        const target_logits = self.forwardAllLogits(
            verify_input,
            1,
            verify_seq,
            &target_ctx,
        ) catch |err| {
            // On failure, roll back the KV extensions
            decode_runtime.truncateGeneratedTokens(draft_count) catch {};
            draft_runtime.truncateGeneratedTokens(draft_count) catch {};
            return err;
        };
        defer allocator.free(target_logits);

        const verify_result = try self.acceptVerifiedDraftTokens(
            token_ids,
            seq_len.*,
            draft_tokens[0..draft_count],
            target_logits,
            target_query_len,
            config,
            &round_penalties,
            token_table,
            json_grammar,
            gbnf_grammar,
            shouldAcceptSpeculativeBonus(true, draft_count, remaining_generation_tokens),
            null,
        );
        const matched_drafts = verify_result.matched_drafts;
        const accepted = verify_result.accepted;
        const hit_eos = verify_result.hit_eos;
        const hit_grammar_stop = verify_result.hit_grammar_stop;
        const correction_added = verify_result.correction_added;
        const had_bonus = verify_result.had_bonus;

        // Roll back to the matched draft prefix. Any correction token or bonus
        // token must be materialized separately because the verify pass only
        // wrote KV for the actual draft inputs it consumed.
        const rollback = draft_count - matched_drafts;
        if (rollback > 0) {
            try decode_runtime.truncateGeneratedTokens(rollback);
            try draft_runtime.truncateGeneratedTokens(rollback);
        }

        if (correction_added or had_bonus) {
            const accepted_seq_len = seq_len.* + accepted;
            try self.materializeAcceptedTokenKv(token_ids, accepted_seq_len, decode_state);
            try draft_pipeline.materializeAcceptedTokenKv(token_ids, accepted_seq_len, draft_decode_state);
        }

        seq_len.* += accepted;
        try penalty_state.noteTokens(allocator, token_ids[round_start..seq_len.*]);

        return .{
            .drafted = draft_count,
            .matched_drafts = matched_drafts,
            .accepted = accepted,
            .correction_added = correction_added,
            .had_bonus = had_bonus,
            .hit_eos = hit_eos,
            .hit_grammar_stop = hit_grammar_stop,
        };
    }

    fn speculativeDecodeGemma4Mtp(
        self: *NativeGenerationPipeline,
        draft_pipeline: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: *usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        k: usize,
        remaining_generation_tokens: usize,
        penalty_state: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
        mtp_activation: *Gemma4MtpActivationState,
        mtp_profile: *MtpProfileStats,
    ) !SpeculativeRoundResult {
        const allocator = self.allocator;
        const round_start = seq_len.*;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var round_penalties = try penalty_state.clone(allocator);
        defer round_penalties.deinit(allocator);

        const defer_materialize = gemma4MtpDeferMaterializeEnabled(self.cb.kind());
        const entry_pending = mtp_activation.pendingCount();
        try mtp_activation.validateKvTokensInSync(decode_state, seq_len.*);

        var draft_tokens: [16]i64 = undefined;
        var draft_logits: [16]?[]f32 = [_]?[]f32{null} ** 16;
        defer for (draft_logits) |maybe_logits| {
            if (maybe_logits) |logits| allocator.free(logits);
        };
        var draft_source_tokens: [16]i64 = undefined;
        var assistant_total_sequence_lens: [16]usize = undefined;
        var draft_logit_sources: [16]gemma4_mtp.DraftLogitSource = undefined;
        const actual_k = @min(k, 16);
        var draft_count: usize = 0;
        try setWorkloadProfileRegime(&self.cb, .speculative_draft);
        try setWorkloadProfileRegime(&draft_pipeline.cb, .speculative_draft);
        const position_mode = gemma4MtpPositionMode();
        const hidden_source = gemma4MtpTargetHiddenSource();
        const concat_order = gemma4_mtp.concatOrderFromEnv();
        const kv_donor_mode = gemma4_mtp.kvDonorModeFromEnv();
        const mtp_top_k = gemma4MtpTopKDiagnosticCount();
        const use_greedy_device_verifier = self.shouldUseGemma4MtpGreedyDeviceVerifier(config, token_table, json_grammar, gbnf_grammar, mtp_top_k);
        const canonical_materialize_available = gemma4MtpActiveMaterializeEnabled() and
            hidden_source == .final and
            self.cb.kind() == .metal;
        const cached_first_target_choice = if (use_greedy_device_verifier and decode_runtime.kvView() != null and gemma4MtpCachedFirstChoiceAllowed(actual_k, canonical_materialize_available))
            mtp_activation.cached_target_choice
        else
            null;
        // A pending round must take the full non-cached verify shape: its row 0
        // is the pending token, which the cached fast path would never forward.
        if (entry_pending != 0 and cached_first_target_choice != null) return error.InvalidSpeculativeState;
        const use_resident_draft =
            mtp_activation.residentReady() and
            gemma4_mtp.deviceResidentDraftAllowed(mtp_top_k) and
            use_greedy_device_verifier and
            gemma4MtpResidentDraftBackendAllowed(draft_pipeline.cb.kind());
        if (!use_resident_draft and mtp_activation.host == null) return error.MissingGemma4MtpActivation;

        var host_chain_activation: ?[]f32 = null;
        defer if (!defer_materialize) {
            if (host_chain_activation) |activation| allocator.free(activation);
        };
        var device_chain_activation: ?ops.CT = null;
        defer if (!defer_materialize) {
            if (device_chain_activation) |activation| draft_pipeline.cb.free(activation);
        };
        // Defer-materialize keeps every draft step's projected activation (the
        // chain vars above become borrows of these slots) so a correction or
        // bonus round can adopt the committed position's chain activation
        // instead of running the materialize forward.
        var draft_step_device_activations: [16]?ops.CT = [_]?ops.CT{null} ** 16;
        defer for (&draft_step_device_activations) |*slot| {
            if (slot.*) |tensor| draft_pipeline.cb.free(tensor);
        };
        var draft_step_host_activations: [16]?[]f32 = [_]?[]f32{null} ** 16;
        defer for (&draft_step_host_activations) |*slot| {
            if (slot.*) |activation| allocator.free(activation);
        };
        const DraftStep = struct {
            token: usize,
            logits: ?[]f32,
            logit_source: gemma4_mtp.DraftLogitSource,
            profile: gemma4_mtp.DraftStageProfile = .{},
        };

        for (0..actual_k) |_| {
            const source_token = token_ids[seq_len.* + draft_count - 1];
            const assistant_total_sequence_len = gemma4MtpAssistantTotalSequenceLen(position_mode, seq_len.*, draft_count);
            const assistant_ctx = gemma4MtpAssistantDecodeContext(decode_state, position_mode, seq_len.*, draft_count);
            const draft_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
            const draft_step: DraftStep = if (use_resident_draft) blk: {
                const activation = device_chain_activation orelse mtp_activation.device.?;
                const target_embedding_on_draft = try self.cachedMtpDraftTargetEmbedding(
                    draft_pipeline,
                    mtp_activation,
                    source_token,
                    mtp_profile,
                );
                const draft_result = try gemma4_mtp.draftTokenDevice(.{
                    .allocator = allocator,
                    .target_cb = &self.cb,
                    .draft_cb = &draft_pipeline.cb,
                    .target_config = self.gpt_config,
                    .draft_config = draft_pipeline.gpt_config,
                    .token_id = source_token,
                    .activation = activation,
                    .target_embedding_on_draft = target_embedding_on_draft,
                    .decode_context = &assistant_ctx,
                    .debug_top_k = mtp_top_k,
                    .profile_enabled = mtp_profile.enabled,
                    .profile_sync = mtp_profile.sync_enabled,
                });
                if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(draft_result.projected_activation) catch {};
                if (defer_materialize) {
                    draft_step_device_activations[draft_count] = draft_result.projected_activation;
                } else if (device_chain_activation) |old| {
                    draft_pipeline.cb.free(old);
                }
                device_chain_activation = draft_result.projected_activation;
                break :blk .{
                    .token = draft_result.token,
                    .logits = draft_result.logits,
                    .logit_source = draft_result.logit_source,
                    .profile = draft_result.profile,
                };
            } else blk: {
                const activation = host_chain_activation orelse mtp_activation.host.?;
                const draft_result = try gemma4_mtp.draftToken(.{
                    .allocator = allocator,
                    .target_cb = &self.cb,
                    .draft_cb = &draft_pipeline.cb,
                    .target_config = self.gpt_config,
                    .draft_config = draft_pipeline.gpt_config,
                    .token_id = source_token,
                    .activation = activation,
                    .decode_context = &assistant_ctx,
                    .debug_top_k = mtp_top_k,
                    .profile_enabled = mtp_profile.enabled,
                    .profile_sync = mtp_profile.sync_enabled,
                });
                if (defer_materialize) {
                    draft_step_host_activations[draft_count] = draft_result.projected_activation;
                } else if (host_chain_activation) |old| {
                    allocator.free(old);
                }
                host_chain_activation = draft_result.projected_activation;
                break :blk .{
                    .token = draft_result.token,
                    .logits = draft_result.logits,
                    .logit_source = draft_result.logit_source,
                    .profile = draft_result.profile,
                };
            };
            if (mtp_profile.enabled) {
                mtp_profile.draft_steps += 1;
                if (use_resident_draft) {
                    mtp_profile.resident_draft_steps += 1;
                } else {
                    mtp_profile.host_draft_steps += 1;
                }
                mtp_profile.draft_token_ns +|= mtpProfileElapsedNs(true, self.io, draft_started_at);
                accumulateMtpDraftStageProfile(mtp_profile, draft_step.profile);
            }
            draft_tokens[draft_count] = @intCast(draft_step.token);
            draft_logits[draft_count] = draft_step.logits;
            draft_source_tokens[draft_count] = source_token;
            assistant_total_sequence_lens[draft_count] = assistant_total_sequence_len;
            draft_logit_sources[draft_count] = draft_step.logit_source;
            token_ids[seq_len.* + draft_count] = @intCast(draft_step.token);
            draft_count += 1;
            if (draft_count == 1) {
                if (cached_first_target_choice) |cached_choice| {
                    if (draft_step.token != @as(usize, @intCast(cached_choice))) break;
                }
            }
        }
        if (enableGemma4MtpDebug()) {
            std.debug.print("gemma4_mtp_debug: seq={d} source={d} position_mode={s} hidden_source={s} concat_order={s} kv_donor_mode={s} topk={d} drafted", .{
                seq_len.*,
                token_ids[seq_len.* - 1],
                position_mode.name(),
                hidden_source.name(),
                concat_order.name(),
                kv_donor_mode.name(),
                mtp_top_k,
            });
            for (draft_tokens[0..draft_count]) |draft_token| {
                std.debug.print(" {d}", .{draft_token});
            }
            std.debug.print("\n", .{});
            for (0..draft_count) |idx| {
                const total = assistant_total_sequence_lens[idx];
                std.debug.print(
                    "gemma4_mtp_debug: draft index={d} source={d} assistant_total={d} assistant_position={d} logit_source={s}\n",
                    .{
                        idx,
                        draft_source_tokens[idx],
                        total,
                        if (total > 0) total - 1 else 0,
                        draft_logit_sources[idx].name(),
                    },
                );
            }
        }

        if (draft_count == 0) return .{
            .drafted = 0,
            .matched_drafts = 0,
            .accepted = 0,
            .correction_added = false,
            .had_bonus = false,
            .hit_eos = false,
            .hit_grammar_stop = false,
        };

        try setWorkloadProfileRegime(&self.cb, .speculative_verify);

        const verify_len = draft_count + 1;
        const verify_seq = seq_len.* + draft_count;
        // Reserve KV slots for the drafted tokens plus the pending
        // unmaterialized token: the verify's suffix write covers its query
        // rows, so row 0 writes the pending token's KV. The KV layer cannot
        // detect a miscounted append (the suffix write would silently shift),
        // hence the release-mode check.
        _ = try decode_runtime.appendGeneratedTokens(draft_count + entry_pending);
        if (decode_state.total_tokens != verify_seq) return error.InvalidSpeculativeState;
        mtp_activation.cached_target_choice = null;
        const target_forward_len = if (cached_first_target_choice != null) draft_count else verify_len;
        const target_query_len: usize = if (decode_runtime.kvView() != null) target_forward_len else verify_seq;
        const target_ctx = decode_runtime.makeDecodeContext(verify_seq, target_query_len);
        const verify_start = if (target_query_len == verify_seq) 0 else if (cached_first_target_choice != null) seq_len.* else verify_seq - target_query_len;

        var verify_result: SpeculativeVerificationResult = undefined;
        var next_host_activation: ?[]f32 = null;
        errdefer if (next_host_activation) |activation| allocator.free(activation);
        var next_device_activation: ?ops.CT = null;
        errdefer if (next_device_activation) |activation| draft_pipeline.cb.free(activation);
        var next_cached_target_choice: ?u32 = null;
        const accept_bonus = shouldAcceptSpeculativeBonus(
            gemma4MtpAcceptBonusEnabled(self.cb.kind()),
            draft_count,
            remaining_generation_tokens,
        );
        var used_cached_first_reject = false;
        if (cached_first_target_choice) |cached_choice| {
            if (try self.rejectMtpDraftFromCachedFirstChoice(
                token_ids,
                seq_len.*,
                draft_tokens[0..draft_count],
                cached_choice,
                config,
            )) |cached_reject| {
                verify_result = cached_reject;
                used_cached_first_reject = true;
            }
        }
        if (use_greedy_device_verifier and !used_cached_first_reject) {
            var used_fused_verify = false;
            // A cached k=1 match has already been target-verified for its
            // first choice. Advance that accepted token through the same
            // one-row frame as ordinary greedy decode so its next choice and
            // hidden state cannot drift from target-only generation.
            if (cached_first_target_choice) |cached_choice| {
                if (draft_count == 1 and target_query_len == 1) {
                    const verify_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                    const canonical_result = self.materializeAcceptedTokenKvActiveHiddenForMtp(
                        token_ids,
                        verify_seq,
                        &target_ctx,
                        hidden_source,
                    ) catch |err| {
                        decode_runtime.truncateGeneratedTokens(draft_count + mtp_activation.pendingCount()) catch {};
                        return err;
                    };
                    if (canonical_result == null) {
                        decode_runtime.truncateGeneratedTokens(draft_count + mtp_activation.pendingCount()) catch {};
                        return error.UnsupportedBackend;
                    }
                    if (canonical_result) |owned| {
                        var canonical_hidden = owned;
                        defer canonical_hidden.deinit();
                        mtp_activation.notePendingKvWritten();

                        const next_choice = canonical_hidden.greedy_token_id orelse
                            try self.greedyTargetChoiceFromFinalHiddenDevice(canonical_hidden.hidden) orelse
                            return error.UnsupportedBackend;
                        const combined_choices = [2]u32{ cached_choice, next_choice };
                        if (verify_len != combined_choices.len) return error.InvalidSpeculativeState;
                        verify_result = try self.acceptVerifiedDraftTokenChoicesGreedy(
                            token_ids,
                            seq_len.*,
                            draft_tokens[0..draft_count],
                            &combined_choices,
                            &round_penalties,
                            accept_bonus,
                            config,
                        );
                        if (mtp_profile.sync_enabled) self.cb.evalTensor(canonical_hidden.hidden) catch {};
                        if (mtp_profile.enabled) {
                            mtp_profile.target_verify_calls += 1;
                            mtp_profile.target_verify_rows += 1;
                            mtp_profile.target_verify_argmax_calls += 1;
                            mtp_profile.target_verify_argmax_rows += 1;
                            mtp_profile.target_verify_argmax_syncs += 1;
                            mtp_profile.target_choice_downloads += 1;
                            mtp_profile.target_verify_ns +|= mtpProfileElapsedNs(true, self.io, verify_started_at);
                        }
                        if (!verify_result.correction_added and !verify_result.had_bonus and verify_result.accepted == 1) {
                            next_cached_target_choice = next_choice;
                        }

                        if (verify_result.accepted > 0 and
                            !verify_result.hit_eos and
                            !verify_result.hit_grammar_stop and
                            !verify_result.correction_added and
                            !verify_result.had_bonus)
                        {
                            const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                            if (use_resident_draft) {
                                next_device_activation = (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&canonical_hidden, 0, hidden_source, &draft_pipeline.cb)) orelse return error.UnsupportedBackend;
                                if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(next_device_activation.?) catch {};
                            } else {
                                next_host_activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&canonical_hidden, 0, hidden_source);
                            }
                            if (mtp_profile.enabled) {
                                mtp_profile.activation_copies += 1;
                                mtp_profile.accepted_hidden_reuse_rows += 1;
                                mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                            }
                        }
                        used_fused_verify = true;
                    }
                }
            }
            if (!used_fused_verify) {
                const verify_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                const target_lm_rows = if (cached_first_target_choice != null) draft_count else verify_len;
                const logit_base_row = target_query_len - target_lm_rows;
                const suppress_token_ids = self.gpt_config.suppressTokenIds();
                const target_hidden_result = if (logit_base_row == 0)
                    self.forwardMtpTargetHiddenDevicePreparedArgmax(
                        token_ids[verify_start..verify_seq],
                        1,
                        verify_seq,
                        &target_ctx,
                        hidden_source,
                        "gpt.mtp_verify_final_hidden",
                        suppress_token_ids,
                    )
                else
                    self.forwardMtpTargetHiddenDevice(
                        token_ids[verify_start..verify_seq],
                        1,
                        verify_seq,
                        &target_ctx,
                        hidden_source,
                        "gpt.mtp_verify_final_hidden",
                    );
                var target_hidden = target_hidden_result catch |err| {
                    decode_runtime.truncateGeneratedTokens(draft_count + mtp_activation.pendingCount()) catch {};
                    return err;
                };
                defer target_hidden.deinit();
                var prepared_tail_choices = target_hidden.prepared_tail_choices;
                target_hidden.prepared_tail_choices = null;
                target_hidden.choices_allocator = null;
                defer if (prepared_tail_choices) |choices| allocator.free(choices);
                mtp_activation.notePendingKvWritten();

                const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
                const vocab_size_usize: usize = @intCast(self.gpt_config.vocab_size);
                const verify_trace = enableGemma4MtpVerifyTrace();
                const tail_trace_started_at = mtpProfileTimestamp(verify_trace, self.io);
                var lm_input = target_hidden.hidden;
                var lm_input_owned = false;
                defer if (lm_input_owned) self.cb.free(lm_input);
                if (logit_base_row != 0 or target_hidden.rows != target_lm_rows) {
                    lm_input = try self.cb.sliceRows2D(self.allocator, target_hidden.hidden, logit_base_row, target_lm_rows, hidden_size);
                    lm_input_owned = true;
                }
                const trace_slice_us = if (verify_trace) @divTrunc(mtpProfileElapsedNs(true, self.io, tail_trace_started_at), std.time.ns_per_us) else 0;

                const lm_w = try self.graphWeight("lm_head.weight");
                defer self.cb.free(lm_w);
                const trace_lmw_us = if (verify_trace) @divTrunc(mtpProfileElapsedNs(true, self.io, tail_trace_started_at), std.time.ns_per_us) else 0;
                // Prefer the prepared final-lm-head slot (same quantized weight
                // the planned decode frames use for their tail) over the dense
                // lm_head fallback inside the generic linearNoBias path.
                if (prepared_tail_choices == null and self.cb.kind() == .metal) {
                    prepared_tail_choices = try decoder_gated_runtime.forwardPreparedLmHeadArgmaxRows(
                        &self.cb,
                        self.gpt_config,
                        self.gpt_config.num_hidden_layers,
                        lm_input,
                        target_lm_rows,
                        suppress_token_ids,
                        allocator,
                    );
                }
                if (cached_first_target_choice) |cached_choice| {
                    const target_tail_choices = blk: {
                        if (prepared_tail_choices) |choices| {
                            prepared_tail_choices = null;
                            break :blk choices;
                        }
                        break :blk (try self.cb.linearNoBiasArgmaxRowsSuppress(
                            lm_input,
                            lm_w,
                            target_lm_rows,
                            hidden_size,
                            vocab_size_usize,
                            suppress_token_ids,
                            allocator,
                        )) orelse {
                            std.log.err(
                                "gemma4_mtp_target_tail_argmax_unsupported: backend={s} rows={d} hidden={d} vocab={d} suppress={d} weight_tying={}",
                                .{ @tagName(self.cb.kind()), target_lm_rows, hidden_size, vocab_size_usize, suppress_token_ids.len, self.gpt_config.weight_tying },
                            );
                            return error.UnsupportedBackend;
                        };
                    };
                    defer allocator.free(target_tail_choices);
                    if (verify_trace) {
                        const argmax_done_us = @divTrunc(mtpProfileElapsedNs(true, self.io, tail_trace_started_at), std.time.ns_per_us);
                        std.debug.print(
                            "mtp-verify-tail-trace: rows={d} slice_us={d} lmw_us={d} argmax_done_us={d}\n",
                            .{ target_lm_rows, trace_slice_us, trace_lmw_us, argmax_done_us },
                        );
                    }
                    if (target_tail_choices.len < target_lm_rows) return error.InvalidTensorShape;
                    var combined_choices_buf: [17]u32 = undefined;
                    if (verify_len > combined_choices_buf.len) return error.InvalidSpeculativeK;
                    combined_choices_buf[0] = cached_choice;
                    for (target_tail_choices[0..target_lm_rows], 0..) |choice, idx| {
                        combined_choices_buf[idx + 1] = choice;
                    }
                    const combined_choices = combined_choices_buf[0..verify_len];
                    verify_result = try self.acceptVerifiedDraftTokenChoicesGreedy(
                        token_ids,
                        seq_len.*,
                        draft_tokens[0..draft_count],
                        combined_choices,
                        &round_penalties,
                        accept_bonus,
                        config,
                    );
                    if (mtp_profile.sync_enabled) {
                        self.cb.evalTensor(target_hidden.hidden) catch {};
                        if (target_hidden.pre_norm_hidden) |pre_norm| self.cb.evalTensor(pre_norm) catch {};
                    }
                    if (mtp_profile.enabled) {
                        mtp_profile.target_verify_calls += 1;
                        mtp_profile.target_verify_rows += target_query_len;
                        mtp_profile.target_verify_argmax_calls += 1;
                        mtp_profile.target_verify_argmax_rows += verify_len;
                        mtp_profile.target_verify_argmax_batched_calls += 1;
                        mtp_profile.target_verify_argmax_syncs += 1;
                        mtp_profile.target_choice_downloads += 1;
                        mtp_profile.target_verify_ns +|= mtpProfileElapsedNs(true, self.io, verify_started_at);
                    }
                    if (!verify_result.correction_added and !verify_result.had_bonus and verify_result.accepted > 0 and verify_result.accepted < verify_len) {
                        next_cached_target_choice = combined_choices[verify_result.accepted];
                    }

                    if (verify_result.accepted > 0 and
                        !verify_result.hit_eos and
                        !verify_result.hit_grammar_stop and
                        !verify_result.correction_added and
                        !verify_result.had_bonus)
                    {
                        const row = logit_base_row + verify_result.accepted - 1;
                        const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                        if (use_resident_draft) {
                            next_device_activation = (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, &draft_pipeline.cb)) orelse return error.UnsupportedBackend;
                            if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(next_device_activation.?) catch {};
                        } else {
                            next_host_activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&target_hidden, row, hidden_source);
                        }
                        if (mtp_profile.enabled) {
                            mtp_profile.activation_copies += 1;
                            mtp_profile.accepted_hidden_reuse_rows += 1;
                            mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                        }
                    }
                    used_fused_verify = true;
                } else if (prepared_tail_choices == null and gemma4MtpDedicatedRuntimeEnabled()) {
                    // The dedicated-runtime verify commit recomputes lm_head
                    // logits internally through the dense fallback weight;
                    // when the prepared-slot argmax already produced the
                    // choices, the plain accept path below consumes them at a
                    // fraction of the cost.
                    var eos_token_ids_buf: [1 + gpt_mod.max_extra_eos_token_ids]i32 = [_]i32{-1} ** (1 + gpt_mod.max_extra_eos_token_ids);
                    var eos_token_ids_len: usize = 0;
                    if (!config.ignore_eos and self.gpt_config.eos_token_id >= 0) {
                        eos_token_ids_buf[eos_token_ids_len] = self.gpt_config.eos_token_id;
                        eos_token_ids_len += 1;
                    }
                    if (!config.ignore_eos) {
                        for (self.gpt_config.extra_eos_token_ids[0..self.gpt_config.extra_eos_token_ids_len]) |eos| {
                            if (eos < 0 or eos_token_ids_len >= eos_token_ids_buf.len) continue;
                            eos_token_ids_buf[eos_token_ids_len] = eos;
                            eos_token_ids_len += 1;
                        }
                    }
                    if (try self.cb.gemma4MtpVerifyCommit(&.{
                        .input = lm_input,
                        .weight = lm_w,
                        .rows = verify_len,
                        .in_dim = hidden_size,
                        .out_dim = vocab_size_usize,
                        .suppress_token_ids = suppress_token_ids,
                        .draft_tokens = draft_tokens[0..draft_count],
                        .eos_token_ids = eos_token_ids_buf[0..eos_token_ids_len],
                        .accept_bonus = accept_bonus,
                        .allocator = allocator,
                    })) |runtime_result_owned| {
                        var runtime_result = runtime_result_owned;
                        defer runtime_result.deinit(allocator);
                        verify_result = try self.acceptGemma4MtpVerifyCommitResultGreedy(
                            token_ids,
                            seq_len.*,
                            draft_tokens[0..draft_count],
                            &runtime_result,
                            &round_penalties,
                        );
                        if (mtp_profile.sync_enabled) {
                            self.cb.evalTensor(target_hidden.hidden) catch {};
                            if (target_hidden.pre_norm_hidden) |pre_norm| self.cb.evalTensor(pre_norm) catch {};
                        }
                        if (mtp_profile.enabled) {
                            mtp_profile.target_verify_calls += 1;
                            mtp_profile.target_verify_rows += target_query_len;
                            mtp_profile.target_verify_argmax_calls += 1;
                            mtp_profile.target_verify_argmax_rows += verify_len;
                            mtp_profile.target_verify_argmax_batched_calls += 1;
                            mtp_profile.target_verify_argmax_syncs += 1;
                            mtp_profile.dedicated_runtime_hits += 1;
                            if (runtime_result.compact_device_result) {
                                mtp_profile.device_verify_commit_hits += 1;
                                mtp_profile.device_verify_commit_result_downloads += 1;
                            } else {
                                mtp_profile.target_choice_downloads += 1;
                            }
                            if (runtime_result.accepted_hidden_row != null) {
                                mtp_profile.accepted_hidden_reuse_rows += 1;
                            }
                            mtp_profile.target_verify_ns +|= mtpProfileElapsedNs(true, self.io, verify_started_at);
                        }

                        if (verify_result.accepted > 0 and
                            !verify_result.hit_eos and
                            !verify_result.hit_grammar_stop and
                            !verify_result.correction_added and
                            !verify_result.had_bonus)
                        {
                            const row = logit_base_row + verify_result.accepted - 1;
                            const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                            if (use_resident_draft) {
                                next_device_activation = (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, &draft_pipeline.cb)) orelse return error.UnsupportedBackend;
                                if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(next_device_activation.?) catch {};
                            } else {
                                next_host_activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&target_hidden, row, hidden_source);
                            }
                            if (mtp_profile.enabled) {
                                mtp_profile.activation_copies += 1;
                                mtp_profile.accepted_hidden_reuse_rows += 1;
                                mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                            }
                        }
                        used_fused_verify = true;
                    } else if (mtp_profile.enabled) {
                        mtp_profile.dedicated_runtime_fallbacks += 1;
                        if (gemma4MtpVerifyDeviceResultEnabled()) {
                            mtp_profile.device_verify_commit_fallbacks += 1;
                        }
                    }
                }
                if (!used_fused_verify) {
                    const maybe_target_choices: ?[]u32 = blk: {
                        if (prepared_tail_choices) |choices| {
                            prepared_tail_choices = null;
                            break :blk choices;
                        }
                        break :blk try self.cb.linearNoBiasArgmaxRowsSuppress(
                            lm_input,
                            lm_w,
                            verify_len,
                            hidden_size,
                            vocab_size_usize,
                            suppress_token_ids,
                            allocator,
                        );
                    };
                    if (maybe_target_choices) |target_choices| {
                        defer allocator.free(target_choices);
                        verify_result = try self.acceptVerifiedDraftTokenChoicesGreedy(
                            token_ids,
                            seq_len.*,
                            draft_tokens[0..draft_count],
                            target_choices,
                            &round_penalties,
                            accept_bonus,
                            config,
                        );
                        if (!verify_result.correction_added and !verify_result.had_bonus and verify_result.accepted > 0 and verify_result.accepted < verify_len) {
                            next_cached_target_choice = target_choices[verify_result.accepted];
                        }
                        if (mtp_profile.sync_enabled) {
                            self.cb.evalTensor(target_hidden.hidden) catch {};
                            if (target_hidden.pre_norm_hidden) |pre_norm| self.cb.evalTensor(pre_norm) catch {};
                        }
                        if (mtp_profile.enabled) {
                            mtp_profile.target_verify_calls += 1;
                            mtp_profile.target_verify_rows += target_query_len;
                            mtp_profile.target_verify_argmax_calls += 1;
                            mtp_profile.target_verify_argmax_rows += verify_len;
                            mtp_profile.target_verify_argmax_batched_calls += 1;
                            mtp_profile.target_verify_argmax_syncs += 1;
                            mtp_profile.target_choice_downloads += 1;
                            mtp_profile.target_verify_ns +|= mtpProfileElapsedNs(true, self.io, verify_started_at);
                        }

                        if (verify_result.accepted > 0 and
                            !verify_result.hit_eos and
                            !verify_result.hit_grammar_stop and
                            !verify_result.correction_added and
                            !verify_result.had_bonus)
                        {
                            const row = logit_base_row + verify_result.accepted - 1;
                            const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                            if (use_resident_draft) {
                                next_device_activation = (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, &draft_pipeline.cb)) orelse return error.UnsupportedBackend;
                                if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(next_device_activation.?) catch {};
                            } else {
                                next_host_activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&target_hidden, row, hidden_source);
                            }
                            if (mtp_profile.enabled) {
                                mtp_profile.activation_copies += 1;
                                mtp_profile.accepted_hidden_reuse_rows += 1;
                                mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                            }
                        }
                        used_fused_verify = true;
                    }
                }
                if (used_fused_verify and
                    defer_materialize and
                    gemma4MtpDeferMaterializeTargetActivationEnabled(self.cb.kind()) and
                    (verify_result.correction_added or verify_result.had_bonus) and
                    !verify_result.hit_eos and
                    !verify_result.hit_grammar_stop)
                {
                    // Option C: the verify already produced the target hidden
                    // that scored the pending token, so reuse it directly.
                    const row = gemma4MtpDeferredTargetActivationRow(
                        cached_first_target_choice != null,
                        logit_base_row,
                        verify_result.accepted,
                    ) orelse return error.InvalidTensorShape;
                    if (row >= target_hidden.rows) return error.InvalidTensorShape;
                    const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                    if (use_resident_draft) {
                        next_device_activation = (try self.copyMtpTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, &draft_pipeline.cb)) orelse return error.UnsupportedBackend;
                        if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(next_device_activation.?) catch {};
                    } else {
                        next_host_activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&target_hidden, row, hidden_source);
                    }
                    if (mtp_profile.enabled) {
                        mtp_profile.activation_copies += 1;
                        mtp_profile.accepted_hidden_reuse_rows += 1;
                        mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                    }
                }
            }

            if (!used_fused_verify) {
                const verify_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                var target_result = self.forwardAllLogitsAndHiddenDevice(
                    token_ids[verify_start..verify_seq],
                    1,
                    verify_seq,
                    &target_ctx,
                ) catch |err| {
                    decode_runtime.truncateGeneratedTokens(draft_count + mtp_activation.pendingCount()) catch {};
                    return err;
                };
                defer target_result.deinit();
                mtp_activation.notePendingKvWritten();

                verify_result = try self.acceptVerifiedDraftTokensGreedyDevice(
                    token_ids,
                    seq_len.*,
                    draft_tokens[0..draft_count],
                    &target_result,
                    target_query_len,
                    &round_penalties,
                    accept_bonus,
                    mtp_profile,
                    config,
                );
                if (mtp_profile.sync_enabled) {
                    self.cb.evalTensor(target_result.logits) catch {};
                    self.cb.evalTensor(target_result.hidden) catch {};
                }
                if (mtp_profile.enabled) {
                    mtp_profile.target_verify_calls += 1;
                    mtp_profile.target_verify_rows += target_query_len;
                    mtp_profile.target_verify_ns +|= mtpProfileElapsedNs(true, self.io, verify_started_at);
                }

                if (verify_result.accepted > 0 and
                    !verify_result.hit_eos and
                    !verify_result.hit_grammar_stop and
                    !verify_result.correction_added and
                    !verify_result.had_bonus)
                {
                    const row = verify_result.accepted - 1;
                    const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                    if (use_resident_draft) {
                        next_device_activation = (try self.copyMtpTargetHiddenRowToBackend(&target_result, row, hidden_source, &draft_pipeline.cb)) orelse return error.UnsupportedBackend;
                        if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(next_device_activation.?) catch {};
                    } else {
                        next_host_activation = try self.dupeMtpTargetHiddenRowFromDevice(&target_result, row, hidden_source);
                    }
                    if (mtp_profile.enabled) {
                        mtp_profile.activation_copies += 1;
                        mtp_profile.accepted_hidden_reuse_rows += 1;
                        mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                    }
                }
            }
        } else if (!used_cached_first_reject) {
            const verify_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
            var target_result = self.forwardAllLogitsAndHiddenHost(
                token_ids[verify_start..verify_seq],
                1,
                verify_seq,
                &target_ctx,
            ) catch |err| {
                decode_runtime.truncateGeneratedTokens(draft_count + mtp_activation.pendingCount()) catch {};
                return err;
            };
            defer target_result.deinit();
            mtp_activation.notePendingKvWritten();

            const parity_trace: ?MtpParityTrace = if (mtp_top_k > 0)
                .{
                    .top_k = mtp_top_k,
                    .assistant_logits = draft_logits[0..draft_count],
                    .assistant_total_sequence_lens = assistant_total_sequence_lens[0..draft_count],
                    .source_tokens = draft_source_tokens[0..draft_count],
                    .logit_sources = draft_logit_sources[0..draft_count],
                    .hidden_source = hidden_source,
                    .concat_order = concat_order,
                    .kv_donor_mode = kv_donor_mode,
                }
            else
                null;

            verify_result = try self.acceptVerifiedDraftTokens(
                token_ids,
                seq_len.*,
                draft_tokens[0..draft_count],
                target_result.logits,
                target_query_len,
                config,
                &round_penalties,
                token_table,
                json_grammar,
                gbnf_grammar,
                accept_bonus,
                parity_trace,
            );
            if (mtp_profile.enabled) {
                mtp_profile.target_verify_calls += 1;
                mtp_profile.target_verify_rows += target_query_len;
                mtp_profile.target_verify_ns +|= mtpProfileElapsedNs(true, self.io, verify_started_at);
            }

            if (verify_result.accepted > 0 and
                !verify_result.hit_eos and
                !verify_result.hit_grammar_stop and
                !verify_result.correction_added and
                !verify_result.had_bonus)
            {
                const row = verify_result.accepted - 1;
                const activation_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                next_host_activation = try self.dupeMtpTargetHiddenRow(&target_result, row, hidden_source);
                if (mtp_profile.enabled) {
                    mtp_profile.activation_copies += 1;
                    mtp_profile.accepted_hidden_reuse_rows += 1;
                    mtp_profile.activation_copy_ns +|= mtpProfileElapsedNs(true, self.io, activation_started_at);
                }
            }
        }

        const matched_drafts = verify_result.matched_drafts;
        const accepted = verify_result.accepted;
        if (mtp_profile.enabled and verify_result.bonus_skipped) {
            mtp_profile.bonus_skips += 1;
        }
        if (mtp_profile.enabled) {
            if (verify_result.correction_added or verify_result.had_bonus) {
                mtp_profile.commit_forwards_required += 1;
            } else if (verify_result.accepted > 0) {
                mtp_profile.commit_forwards_avoided += 1;
            }
        }
        const rollback = draft_count - matched_drafts;
        if (rollback > 0) {
            try decode_runtime.truncateGeneratedTokens(rollback);
        }

        if (verify_result.correction_added or verify_result.had_bonus) {
            if (defer_materialize) {
                // Fold: leave the committed token un-forwarded. The next
                // round's verify prepends it as row 0 (the non-cached verify
                // shape) and the suffix write materializes its KV. Draft
                // quality only (the verify still decides every token): the
                // next round drafts without the committed token's donor KV
                // row. The target verify activation above is preferred; this
                // retained assistant-chain activation is its fallback.
                if (next_device_activation == null and next_host_activation == null) {
                    const step_index = if (verify_result.correction_added) matched_drafts else draft_count - 1;
                    if (use_resident_draft) {
                        const retained = draft_step_device_activations[step_index] orelse return error.MissingGemma4MtpActivation;
                        draft_step_device_activations[step_index] = null;
                        next_device_activation = retained;
                    } else {
                        const retained = draft_step_host_activations[step_index] orelse return error.MissingGemma4MtpActivation;
                        draft_step_host_activations[step_index] = null;
                        next_host_activation = retained;
                    }
                }
                next_cached_target_choice = null;
                try mtp_activation.notePendingUnmaterialized();
                if (mtp_profile.enabled) {
                    mtp_profile.deferred_materializations += 1;
                }
            } else {
                const accepted_seq_len = seq_len.* + accepted;
                const materialization_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
                const hidden_only_materialize = gemma4MtpHiddenOnlyMaterializeEnabled();
                const have_predictor_activation = next_device_activation != null or next_host_activation != null;
                if (have_predictor_activation) {
                    next_cached_target_choice = try self.materializeAcceptedTokenKvForMtp(token_ids, accepted_seq_len, decode_state, hidden_source);
                } else {
                    if (use_resident_draft) {
                        const materialized = try self.materializeAcceptedTokenKvAndCopyHiddenToBackend(
                            token_ids,
                            accepted_seq_len,
                            decode_state,
                            hidden_source,
                            &draft_pipeline.cb,
                        );
                        const materialized_hidden = materialized.activation orelse return error.UnsupportedBackend;
                        if (mtp_profile.sync_enabled) draft_pipeline.cb.evalTensor(materialized_hidden) catch {};
                        if (next_device_activation) |old| draft_pipeline.cb.free(old);
                        next_device_activation = materialized_hidden;
                        next_cached_target_choice = materialized.next_target_choice;
                        if (next_host_activation) |old| {
                            allocator.free(old);
                            next_host_activation = null;
                        }
                    } else {
                        const materialized = try self.materializeAcceptedTokenKvAndReturnHidden(
                            token_ids,
                            accepted_seq_len,
                            decode_state,
                            hidden_source,
                        );
                        if (next_host_activation) |old| allocator.free(old);
                        next_host_activation = materialized.activation;
                        next_cached_target_choice = materialized.next_target_choice;
                    }
                }
                if (mtp_profile.enabled) {
                    mtp_profile.materializations += 1;
                    if (verify_result.correction_added) {
                        mtp_profile.correction_materializations += 1;
                    } else if (verify_result.had_bonus) {
                        mtp_profile.bonus_materializations += 1;
                    }
                    if (hidden_only_materialize) {
                        mtp_profile.materialization_hidden_only_hits += 1;
                    } else {
                        mtp_profile.materialization_hidden_only_fallbacks += 1;
                    }
                    mtp_profile.materialization_ns +|= mtpProfileElapsedNs(true, self.io, materialization_started_at);
                }
            }
        }

        if (next_device_activation) |activation| {
            mtp_activation.replaceDevice(activation);
            next_device_activation = null;
        } else if (next_host_activation) |activation| {
            mtp_activation.replaceHost(activation);
            next_host_activation = null;
        }
        mtp_activation.cached_target_choice = next_cached_target_choice;
        seq_len.* += accepted;
        try mtp_activation.validateKvTokensInSync(decode_state, seq_len.*);
        try penalty_state.noteTokens(allocator, token_ids[round_start..seq_len.*]);

        return .{
            .drafted = draft_count,
            .matched_drafts = matched_drafts,
            .accepted = accepted,
            .correction_added = verify_result.correction_added,
            .had_bonus = verify_result.had_bonus,
            .hit_eos = verify_result.hit_eos,
            .hit_grammar_stop = verify_result.hit_grammar_stop,
            .mtp_quality = verify_result.mtp_quality,
        };
    }

    const SampleOutcome = struct {
        token: usize,
        grammar_complete: bool,
    };

    const SpeculativeRoundResult = struct {
        drafted: usize,
        matched_drafts: usize,
        accepted: usize,
        correction_added: bool,
        had_bonus: bool,
        hit_eos: bool,
        hit_grammar_stop: bool,
        mtp_quality: MtpQualityStats = .{},
    };

    const SpeculativeVerificationResult = struct {
        matched_drafts: usize,
        accepted: usize,
        hit_eos: bool,
        hit_grammar_stop: bool,
        correction_added: bool,
        had_bonus: bool,
        bonus_skipped: bool = false,
        mtp_quality: MtpQualityStats = .{},
    };

    fn shouldUseGemma4MtpGreedyDeviceVerifier(
        self: *const NativeGenerationPipeline,
        config: GenerationConfig,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
        mtp_top_k: usize,
    ) bool {
        if (!gemma4MtpResidentDraftBackendAllowed(self.cb.kind())) return false;
        if (self.graph_cache != null or self.compiled_partition_backend != null) return false;
        if (mtp_top_k != 0) return false;
        if (!isPureGreedyConfig(config)) return false;
        if (config.grammar != null) return false;
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) return false;
        if (!gpt_arch.canUseFastGreedyArgmaxForConfig(self.gpt_config)) return false;
        return true;
    }

    fn rejectMtpDraftFromCachedFirstChoice(
        self: *const NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: usize,
        draft_tokens: []const i64,
        cached_choice: u32,
        config: GenerationConfig,
    ) !?SpeculativeVerificationResult {
        if (draft_tokens.len == 0) return null;
        const first_draft_raw = draft_tokens[0];
        if (first_draft_raw < 0) return error.InvalidModelOutput;
        const first_draft: usize = @intCast(first_draft_raw);
        const target_choice: usize = @intCast(cached_choice);
        if (target_choice == first_draft) return null;
        if (seq_len >= token_ids.len) return error.InvalidTensorShape;

        const hit_eos = self.shouldStopOnEos(config, target_choice);
        if (!hit_eos) token_ids[seq_len] = @intCast(cached_choice);
        return .{
            .matched_drafts = 0,
            .accepted = @intFromBool(!hit_eos),
            .hit_eos = hit_eos,
            .hit_grammar_stop = false,
            .correction_added = !hit_eos,
            .had_bonus = false,
            .bonus_skipped = false,
            .mtp_quality = classifyGemma4MtpMismatchWithoutTargetLogits(first_draft_raw, target_choice),
        };
    }

    fn argmaxDeviceLogitRow(
        self: *NativeGenerationPipeline,
        result: *const ForwardAllWithHiddenDevice,
        row: usize,
    ) !usize {
        const vocab_size: usize = @intCast(self.gpt_config.vocab_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const row_logits = try self.cb.sliceRows2D(self.allocator, result.logits, row, 1, vocab_size);
        defer self.cb.free(row_logits);
        const suppress_token_ids = self.gpt_config.suppressTokenIds();
        if (suppress_token_ids.len > 0) {
            const token_tensor = (try self.cb.argmaxLastRowSuppressTensor(row_logits, 1, vocab_size, suppress_token_ids)) orelse return error.UnsupportedBackend;
            defer self.cb.free(token_tensor);
            const token_host = try self.cb.toFloat32(token_tensor, self.allocator);
            defer self.allocator.free(token_host);
            if (token_host.len != 1 or token_host[0] < 0) return error.InvalidModelOutput;
            return @intFromFloat(token_host[0]);
        }
        const token = (try self.cb.argmaxLastRow(row_logits, 1, vocab_size)) orelse return error.UnsupportedBackend;
        return @intCast(token);
    }

    fn acceptVerifiedDraftTokensGreedyDevice(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: usize,
        draft_tokens: []const i64,
        target_result: *const ForwardAllWithHiddenDevice,
        target_query_len: usize,
        round_penalties: *SamplingPenaltyState,
        accept_bonus: bool,
        mtp_profile: *MtpProfileStats,
        config: GenerationConfig,
    ) !SpeculativeVerificationResult {
        const verify_len = draft_tokens.len + 1;
        if (target_query_len < verify_len or target_result.rows < target_query_len) return error.InvalidTensorShape;
        const logit_base_row = target_query_len - verify_len;
        const vocab_size: usize = @intCast(self.gpt_config.vocab_size);
        const suppress_token_ids = self.gpt_config.suppressTokenIds();
        const batched_choices: ?[]u32 = if (suppress_token_ids.len == 0)
            try self.cb.argmaxRows(target_result.logits, logit_base_row, verify_len, vocab_size, self.allocator)
        else
            try self.cb.argmaxRowsSuppress(target_result.logits, logit_base_row, verify_len, vocab_size, suppress_token_ids, self.allocator);
        defer if (batched_choices) |choices| self.allocator.free(choices);
        if (batched_choices != null and mtp_profile.enabled) {
            mtp_profile.target_verify_argmax_calls += 1;
            mtp_profile.target_verify_argmax_rows += verify_len;
            mtp_profile.target_verify_argmax_batched_calls += 1;
            mtp_profile.target_verify_argmax_syncs += 1;
            mtp_profile.target_choice_downloads += 1;
        }

        if (batched_choices) |choices| {
            return try self.acceptVerifiedDraftTokenChoicesGreedy(
                token_ids,
                seq_len,
                draft_tokens,
                choices,
                round_penalties,
                accept_bonus,
                config,
            );
        }

        var fallback_choices_buf: [17]u32 = undefined;
        if (verify_len > fallback_choices_buf.len) return error.InvalidSpeculativeK;
        for (0..verify_len) |i| {
            if (mtp_profile.enabled) {
                mtp_profile.target_verify_argmax_calls += 1;
                mtp_profile.target_verify_argmax_rows += 1;
                mtp_profile.target_verify_argmax_syncs += 1;
                mtp_profile.target_choice_downloads += 1;
            }
            fallback_choices_buf[i] = @intCast(try self.argmaxDeviceLogitRow(target_result, logit_base_row + i));
        }
        return try self.acceptVerifiedDraftTokenChoicesGreedy(
            token_ids,
            seq_len,
            draft_tokens,
            fallback_choices_buf[0..verify_len],
            round_penalties,
            accept_bonus,
            config,
        );
    }

    fn acceptVerifiedDraftTokenChoicesGreedy(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: usize,
        draft_tokens: []const i64,
        target_choices: []const u32,
        round_penalties: *SamplingPenaltyState,
        accept_bonus: bool,
        config: GenerationConfig,
    ) !SpeculativeVerificationResult {
        const verify_len = draft_tokens.len + 1;
        if (target_choices.len < verify_len) return error.InvalidTensorShape;
        var matched_drafts: usize = 0;
        var accepted: usize = 0;
        var hit_eos = false;
        var correction_added = false;
        var mtp_quality = MtpQualityStats{};

        for (0..draft_tokens.len) |i| {
            const target_choice = @as(usize, @intCast(target_choices[i]));
            debugGemma4Mtp("verify_device index={d} draft={d} target={d}", .{
                i,
                draft_tokens[i],
                target_choice,
            });

            if (target_choice == @as(usize, @intCast(draft_tokens[i]))) {
                if (self.shouldStopOnEos(config, target_choice)) {
                    hit_eos = true;
                    break;
                }
                matched_drafts += 1;
                accepted += 1;
                try round_penalties.noteToken(self.allocator, draft_tokens[i]);
            } else {
                mtp_quality.merge(classifyGemma4MtpMismatchWithoutTargetLogits(draft_tokens[i], target_choice));
                if (self.shouldStopOnEos(config, target_choice)) {
                    hit_eos = true;
                } else {
                    token_ids[seq_len + matched_drafts] = @intCast(target_choice);
                    accepted = matched_drafts + 1;
                    correction_added = true;
                }
                break;
            }
        }

        const can_bonus = matched_drafts == draft_tokens.len and !hit_eos;
        var had_bonus = false;
        const bonus_skipped = can_bonus and !accept_bonus;
        if (can_bonus and accept_bonus) {
            const bonus_token = @as(usize, @intCast(target_choices[draft_tokens.len]));
            debugGemma4Mtp("bonus_device index={d} target={d}", .{
                draft_tokens.len,
                bonus_token,
            });

            if (self.shouldStopOnEos(config, bonus_token)) {
                hit_eos = true;
            } else {
                token_ids[seq_len + accepted] = @intCast(bonus_token);
                accepted += 1;
                had_bonus = true;
            }
        }

        return .{
            .matched_drafts = matched_drafts,
            .accepted = accepted,
            .hit_eos = hit_eos,
            .hit_grammar_stop = false,
            .correction_added = correction_added,
            .had_bonus = had_bonus,
            .bonus_skipped = bonus_skipped,
            .mtp_quality = mtp_quality,
        };
    }

    fn acceptGemma4MtpVerifyCommitResultGreedy(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: usize,
        draft_tokens: []const i64,
        runtime_result: *const ops.Gemma4MtpVerifyCommitResult,
        round_penalties: *SamplingPenaltyState,
    ) !SpeculativeVerificationResult {
        const verify_len = draft_tokens.len + 1;
        const has_target_choices = runtime_result.target_choices.len != 0;
        if (has_target_choices and runtime_result.target_choices.len < verify_len) return error.InvalidTensorShape;
        if (!has_target_choices and !runtime_result.compact_device_result) return error.InvalidTensorShape;
        if (runtime_result.matched_drafts > draft_tokens.len) return error.InvalidTensorShape;
        if (runtime_result.accepted > verify_len) return error.InvalidTensorShape;
        if (runtime_result.correction_added and runtime_result.had_bonus) return error.InvalidSpeculativeState;
        if (runtime_result.correction_added) {
            if (runtime_result.accepted != runtime_result.matched_drafts + 1) return error.InvalidSpeculativeState;
        } else if (runtime_result.had_bonus) {
            if (runtime_result.matched_drafts != draft_tokens.len) return error.InvalidSpeculativeState;
            if (runtime_result.accepted != draft_tokens.len + 1) return error.InvalidSpeculativeState;
        } else {
            if (runtime_result.accepted != runtime_result.matched_drafts) return error.InvalidSpeculativeState;
        }
        if (runtime_result.bonus_skipped and
            (runtime_result.correction_added or runtime_result.had_bonus or runtime_result.matched_drafts != draft_tokens.len))
        {
            return error.InvalidSpeculativeState;
        }
        if (runtime_result.accepted_hidden_row) |row| {
            if (row >= verify_len) return error.InvalidTensorShape;
            if (runtime_result.correction_added or runtime_result.had_bonus) return error.InvalidSpeculativeState;
            if (runtime_result.accepted == 0 or row != runtime_result.accepted - 1) return error.InvalidSpeculativeState;
        }

        var matched_drafts = runtime_result.matched_drafts;
        var accepted = runtime_result.accepted;
        var correction_added = runtime_result.correction_added;
        var had_bonus = runtime_result.had_bonus;
        if (runtime_result.hit_eos) {
            if (accepted == 0) return error.InvalidSpeculativeState;
            accepted -= 1;
            if (correction_added) {
                correction_added = false;
            } else if (had_bonus) {
                had_bonus = false;
            } else {
                if (matched_drafts == 0) return error.InvalidSpeculativeState;
                matched_drafts -= 1;
            }
        }

        var mtp_quality = MtpQualityStats{};
        for (0..matched_drafts) |i| {
            if (draft_tokens[i] < 0) return error.InvalidModelOutput;
            if (has_target_choices and runtime_result.target_choices[i] != @as(u32, @intCast(draft_tokens[i]))) return error.InvalidSpeculativeState;
            try round_penalties.noteToken(self.allocator, draft_tokens[i]);
        }

        if (runtime_result.correction_added) {
            if (runtime_result.matched_drafts >= draft_tokens.len) return error.InvalidSpeculativeState;
            if (draft_tokens[runtime_result.matched_drafts] < 0) return error.InvalidModelOutput;
            const target_choice = if (has_target_choices)
                @as(usize, @intCast(runtime_result.target_choices[runtime_result.matched_drafts]))
            else
                @as(usize, @intCast(runtime_result.correction_token orelse return error.InvalidSpeculativeState));
            if (correction_added) token_ids[seq_len + matched_drafts] = @intCast(target_choice);
            mtp_quality.merge(classifyGemma4MtpMismatchWithoutTargetLogits(
                draft_tokens[runtime_result.matched_drafts],
                target_choice,
            ));
        }

        if (had_bonus) {
            if (runtime_result.matched_drafts != draft_tokens.len) return error.InvalidSpeculativeState;
            const bonus_token = if (has_target_choices)
                @as(usize, @intCast(runtime_result.target_choices[draft_tokens.len]))
            else
                @as(usize, @intCast(runtime_result.bonus_token orelse return error.InvalidSpeculativeState));
            token_ids[seq_len + draft_tokens.len] = @intCast(bonus_token);
        }

        return .{
            .matched_drafts = matched_drafts,
            .accepted = accepted,
            .hit_eos = runtime_result.hit_eos,
            .hit_grammar_stop = false,
            .correction_added = correction_added,
            .had_bonus = had_bonus,
            .bonus_skipped = runtime_result.bonus_skipped,
            .mtp_quality = mtp_quality,
        };
    }

    fn sampleNextToken(
        self: *NativeGenerationPipeline,
        logits: []const f32,
        config: GenerationConfig,
        penalty_state: *const SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
    ) !SampleOutcome {
        const has_grammar = json_grammar.* != null or gbnf_grammar != null;
        const suppress_token_ids = self.gpt_config.suppressTokenIds();
        const needs_mutable_logits = has_grammar or suppress_token_ids.len > 0;
        const working_logits = if (needs_mutable_logits)
            try self.allocator.dupe(f32, logits)
        else
            @constCast(logits);
        defer if (needs_mutable_logits) self.allocator.free(working_logits);

        if (has_grammar) {
            try self.applyGrammarMask(working_logits, token_table, json_grammar, gbnf_grammar);
        }
        applySuppressTokenMask(working_logits, suppress_token_ids);

        const next_token = sample(working_logits, config, penalty_state, self.allocator);
        const grammar_complete = if (has_grammar)
            try self.advanceGrammarWithToken(next_token, json_grammar, gbnf_grammar)
        else
            false;

        return .{
            .token = next_token,
            .grammar_complete = grammar_complete,
        };
    }

    fn applySuppressTokenMask(logits: []f32, token_ids: []const i32) void {
        for (token_ids) |token_id| {
            if (token_id < 0) continue;
            const idx: usize = @intCast(token_id);
            if (idx >= logits.len) continue;
            logits[idx] = -std.math.inf(f32);
        }
    }

    fn isSuppressedToken(token_id: u32, token_ids: []const i32) bool {
        for (token_ids) |candidate| {
            if (candidate >= 0 and @as(u32, @intCast(candidate)) == token_id) return true;
        }
        return false;
    }

    fn acceptVerifiedDraftTokens(
        self: *NativeGenerationPipeline,
        token_ids: []i64,
        seq_len: usize,
        draft_tokens: []const i64,
        target_logits: []const f32,
        target_query_len: usize,
        config: GenerationConfig,
        round_penalties: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
        accept_bonus: bool,
        mtp_parity_trace: ?MtpParityTrace,
    ) !SpeculativeVerificationResult {
        const vocab_size = self.gpt_config.vocab_size;
        const verify_len = draft_tokens.len + 1;
        const logit_base_offset = (target_query_len - verify_len) * vocab_size;

        var matched_drafts: usize = 0;
        var accepted: usize = 0;
        var hit_eos = false;
        var hit_grammar_stop = false;
        var correction_added = false;
        var mtp_quality = MtpQualityStats{};

        for (0..draft_tokens.len) |i| {
            const pos_offset = logit_base_offset + i * vocab_size;
            const pos_logits = target_logits[pos_offset..][0..vocab_size];
            const outcome = try self.sampleNextToken(
                pos_logits,
                config,
                round_penalties,
                token_table,
                json_grammar,
                gbnf_grammar,
            );
            const target_choice = outcome.token;
            debugGemma4MtpLogitChoice("verify", i, pos_logits, draft_tokens[i], target_choice);
            debugGemma4Mtp("verify index={d} draft={d} target={d}", .{
                i,
                draft_tokens[i],
                target_choice,
            });
            if (mtp_parity_trace) |trace| {
                debugGemma4MtpParityTrace(trace, i, pos_logits, draft_tokens[i], target_choice);
            }

            if (target_choice == @as(usize, @intCast(draft_tokens[i]))) {
                if (self.shouldStopOnEos(config, target_choice)) {
                    hit_eos = true;
                    break;
                }
                matched_drafts += 1;
                accepted += 1;
                try round_penalties.noteToken(self.allocator, draft_tokens[i]);
                if (outcome.grammar_complete) {
                    hit_grammar_stop = true;
                    break;
                }
            } else {
                mtp_quality.merge(classifyGemma4MtpMismatch(mtp_parity_trace, i, pos_logits, draft_tokens[i], target_choice));
                if (self.shouldStopOnEos(config, target_choice)) {
                    hit_eos = true;
                } else {
                    token_ids[seq_len + matched_drafts] = @intCast(target_choice);
                    accepted = matched_drafts + 1;
                    correction_added = true;
                }
                if (outcome.grammar_complete) {
                    hit_grammar_stop = true;
                }
                break;
            }
        }

        const can_bonus = matched_drafts == draft_tokens.len and !hit_eos and !hit_grammar_stop;
        var had_bonus = false;
        const bonus_skipped = can_bonus and !accept_bonus;
        if (can_bonus and accept_bonus) {
            const bonus_offset = logit_base_offset + draft_tokens.len * vocab_size;
            const bonus_logits = target_logits[bonus_offset..][0..vocab_size];
            const outcome = try self.sampleNextToken(
                bonus_logits,
                config,
                round_penalties,
                token_table,
                json_grammar,
                gbnf_grammar,
            );
            const bonus_token = outcome.token;
            debugGemma4MtpLogitChoice("bonus", draft_tokens.len, bonus_logits, null, bonus_token);
            debugGemma4Mtp("bonus index={d} target={d}", .{
                draft_tokens.len,
                bonus_token,
            });

            if (self.shouldStopOnEos(config, bonus_token)) {
                hit_eos = true;
            } else {
                token_ids[seq_len + accepted] = @intCast(bonus_token);
                accepted += 1;
                had_bonus = true;
            }
            if (outcome.grammar_complete) {
                hit_grammar_stop = true;
            }
        }

        return .{
            .matched_drafts = matched_drafts,
            .accepted = accepted,
            .hit_eos = hit_eos,
            .hit_grammar_stop = hit_grammar_stop,
            .correction_added = correction_added,
            .had_bonus = had_bonus,
            .bonus_skipped = bonus_skipped,
            .mtp_quality = mtp_quality,
        };
    }

    fn applyGrammarMask(
        self: *NativeGenerationPipeline,
        logits: []f32,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
    ) !void {
        if (json_grammar.* != null) {
            const mask = if (token_table) |tt|
                try json_grammar.*.?.allowedTokenMaskFast(self.allocator, tt, self.gpt_config.vocab_size)
            else
                try json_grammar.*.?.allowedTokenMask(self.allocator, self.tokenizer, self.gpt_config.vocab_size);
            defer self.allocator.free(mask);
            grammar_mod.JsonGrammar.applyMask(mask, logits);
        } else if (gbnf_grammar) |gg| {
            const mask = if (token_table) |tt|
                try gg.allowedTokenMaskFast(self.allocator, tt, self.gpt_config.vocab_size)
            else
                try gg.allowedTokenMask(self.allocator, self.tokenizer, self.gpt_config.vocab_size);
            defer self.allocator.free(mask);
            grammar_mod.GbnfGrammar.applyMask(mask, logits);
        }
    }

    fn advanceGrammarWithToken(
        self: *NativeGenerationPipeline,
        token_id: usize,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
    ) !bool {
        const token_id_arr = [1]i32{@intCast(token_id)};
        const token_bytes = self.tokenizer.decode(self.allocator, &token_id_arr) catch return false;
        defer self.allocator.free(token_bytes);

        if (json_grammar.* != null) json_grammar.*.?.advance(token_bytes);
        if (gbnf_grammar) |gg| gg.advance(token_bytes);

        if (json_grammar.* != null) return json_grammar.*.?.isComplete();
        if (gbnf_grammar) |gg| return gg.isComplete();
        return false;
    }

    fn materializeAcceptedTokenKv(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
    ) !void {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) {
            _ = try decode_runtime.appendGeneratedToken();
            return;
        }
        // Run a forward pass to populate the KV cache for the correction/bonus
        // token. Paged CUDA writes the KV suffix at decode_context.kv_sequence_len,
        // so advance first; otherwise the device KV layer stays one token short.
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        const logits = try self.forwardAllLogits(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        self.allocator.free(logits);
    }

    fn greedyTargetChoiceFromFinalHiddenDevice(
        self: *NativeGenerationPipeline,
        final_hidden: ops.CT,
    ) !?u32 {
        const lm_w = try self.graphWeight("lm_head.weight");
        defer self.cb.free(lm_w);
        const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
        const vocab_size: usize = @intCast(self.gpt_config.vocab_size);
        const suppress_token_ids = self.gpt_config.suppressTokenIds();
        if (suppress_token_ids.len == 0) {
            if (try self.cb.linearNoBiasArgmaxLastRow(
                final_hidden,
                lm_w,
                1,
                hidden_size,
                vocab_size,
            )) |token| {
                return token;
            }
        }
        if (try self.cb.linearNoBiasArgmaxRowsSuppress(
            final_hidden,
            lm_w,
            1,
            hidden_size,
            vocab_size,
            suppress_token_ids,
            self.allocator,
        )) |choices| {
            defer self.allocator.free(choices);
            if (choices.len == 0) return error.InvalidModelOutput;
            return choices[0];
        }
        return null;
    }

    fn materializeAcceptedTokenKvActiveHiddenForMtp(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: Gemma4MtpTargetHiddenSource,
    ) !?ForwardHiddenDevice {
        if (!gemma4MtpActiveMaterializeEnabled()) return null;
        if (hidden_source != .final) return null;
        if (self.cb.kind() != .metal) return null;
        if (!gemma4MtpActiveMaterializeSupported(self.cb.kind(), self.gpt_config, total_seq_len)) return null;
        if (total_seq_len == 0 or token_ids.len < total_seq_len) return error.InvalidTensorShape;
        var result = (try decoder_gated_runtime.forwardGreedyTokenAndFinalHidden(
            &self.cb,
            self.allocator,
            self.gpt_config,
            self.gpt_config.num_hidden_layers,
            token_ids[total_seq_len - 1],
            total_seq_len,
            decode_context,
        )) orelse return null;
        errdefer result.deinit(&self.cb);
        var greedy_token_id: ?u32 = null;
        if (result.token_id >= 0 and result.token_id <= std.math.maxInt(u32)) {
            const raw_token: u32 = @intCast(result.token_id);
            if (!isSuppressedToken(raw_token, self.gpt_config.suppressTokenIds())) {
                greedy_token_id = raw_token;
            }
        }
        return .{
            .cb = &self.cb,
            .hidden = result.final_hidden,
            .pre_norm_hidden = null,
            .rows = 1,
            .greedy_token_id = greedy_token_id,
        };
    }

    fn materializeAcceptedTokenKvForMtp(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
        hidden_source: Gemma4MtpTargetHiddenSource,
    ) !?u32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) {
            _ = try decode_runtime.appendGeneratedToken();
            return null;
        }
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(
            token_ids,
            total_seq_len,
            &decode_context,
            hidden_source,
        )) |active_result| {
            var result = active_result;
            defer result.deinit();
            return result.greedy_token_id orelse try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden);
        }
        if (gemma4MtpHiddenOnlyMaterializeEnabled()) {
            var result = if (gemma4MtpMaterializeReplayEnabled())
                try self.forwardMtpTargetHiddenDevice(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                    "gpt.mtp_materialize_choice_hidden",
                )
            else
                try self.forwardMtpTargetHiddenDeviceNoReplay(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                );
            defer result.deinit();
            return try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden);
        }
        var result = try self.forwardAllLogitsAndHiddenDevice(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        return try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden);
    }

    /// Flush a pending unmaterialized token (defer-materialize fold) before
    /// entering any decode path that assumes decode_state.total_tokens ==
    /// seq_len — e.g. the mid-generation standardDecode fallbacks. Restores
    /// exactly the fold-off state: the token's KV written via a one-row
    /// materialize forward.
    fn flushPendingGemma4MtpMaterialize(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        seq_len: usize,
        decode_state: *NativeDecodeState,
        mtp_activation: *Gemma4MtpActivationState,
        hidden_source: Gemma4MtpTargetHiddenSource,
        mtp_profile: *MtpProfileStats,
    ) !void {
        if (mtp_activation.pendingCount() == 0) return;
        try mtp_activation.validateKvTokensInSync(decode_state, seq_len);
        const flush_started_at = mtpProfileTimestamp(mtp_profile.enabled, self.io);
        mtp_activation.cached_target_choice = try self.materializeAcceptedTokenKvForMtp(token_ids, seq_len, decode_state, hidden_source);
        mtp_activation.notePendingKvWritten();
        try mtp_activation.validateKvTokensInSync(decode_state, seq_len);
        if (mtp_profile.enabled) {
            mtp_profile.pending_flushes += 1;
            mtp_profile.materialization_ns +|= mtpProfileElapsedNs(true, self.io, flush_started_at);
        }
    }

    const MtpMaterializedHostActivation = struct {
        activation: []f32,
        next_target_choice: ?u32 = null,
    };

    fn materializeAcceptedTokenKvAndReturnHidden(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
        hidden_source: Gemma4MtpTargetHiddenSource,
    ) !MtpMaterializedHostActivation {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) return error.MissingMaterializedHiddenState;
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(
            token_ids,
            total_seq_len,
            &decode_context,
            hidden_source,
        )) |active_result| {
            var result = active_result;
            defer result.deinit();
            const activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&result, 0, hidden_source);
            errdefer self.allocator.free(activation);
            return .{
                .activation = activation,
                .next_target_choice = result.greedy_token_id orelse try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        if (gemma4MtpHiddenOnlyMaterializeEnabled()) {
            var result = if (gemma4MtpMaterializeReplayEnabled())
                try self.forwardMtpTargetHiddenDevice(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                    "gpt.mtp_materialize_host_hidden",
                )
            else
                try self.forwardMtpTargetHiddenDeviceNoReplay(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                );
            defer result.deinit();
            const activation = try self.dupeMtpTargetHiddenRowFromHiddenDevice(&result, 0, hidden_source);
            errdefer self.allocator.free(activation);
            return .{
                .activation = activation,
                .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        var result = try self.forwardAllLogitsAndHiddenDevice(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        const activation = try self.dupeMtpTargetHiddenRowFromDevice(&result, 0, hidden_source);
        errdefer self.allocator.free(activation);
        return .{
            .activation = activation,
            .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
        };
    }

    const MtpMaterializedDeviceActivation = struct {
        activation: ?ops.CT,
        next_target_choice: ?u32 = null,
    };

    fn materializeAcceptedTokenKvAndCopyHiddenToBackend(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
        hidden_source: Gemma4MtpTargetHiddenSource,
        dst_cb: *const ComputeBackend,
    ) !MtpMaterializedDeviceActivation {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) return .{ .activation = null };
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(
            token_ids,
            total_seq_len,
            &decode_context,
            hidden_source,
        )) |active_result| {
            var result = active_result;
            defer result.deinit();
            const activation = try self.copyMtpTargetHiddenRowFromHiddenToBackend(&result, 0, hidden_source, dst_cb);
            errdefer if (activation) |tensor| dst_cb.free(tensor);
            return .{
                .activation = activation,
                .next_target_choice = result.greedy_token_id orelse try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        if (gemma4MtpHiddenOnlyMaterializeEnabled()) {
            var result = if (gemma4MtpMaterializeReplayEnabled())
                try self.forwardMtpTargetHiddenDevice(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                    "gpt.mtp_materialize_device_hidden",
                )
            else
                try self.forwardMtpTargetHiddenDeviceNoReplay(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                );
            defer result.deinit();
            const activation = try self.copyMtpTargetHiddenRowFromHiddenToBackend(&result, 0, hidden_source, dst_cb);
            errdefer if (activation) |tensor| dst_cb.free(tensor);
            return .{
                .activation = activation,
                .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        var result = try self.forwardAllLogitsAndHiddenDevice(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        const activation = try self.copyMtpTargetHiddenRowToBackend(&result, 0, hidden_source, dst_cb);
        errdefer if (activation) |tensor| dst_cb.free(tensor);
        return .{
            .activation = activation,
            .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
        };
    }

    pub fn deinit(self: *NativeGenerationPipeline) void {
        _ = self;
        // ComputeBackend and tokenizer are borrowed — caller manages their lifetime.
    }

    fn runScheduledDecodeBatch(
        self: *NativeGenerationPipeline,
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        io: std.Io,
        decode_state: *NativeDecodeState,
        token_id: i64,
        seq_len: usize,
    ) ![]f32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        const decode_ctx = decode_runtime.makeDecodeContext(seq_len, 1);
        var work = PendingDecodeBatchWork{
            .allocator = self.allocator,
            .decode_state = decode_state,
            .token_id = token_id,
            .seq_len = seq_len,
        };
        defer if (work.logits) |logits| work.allocator.free(logits);
        const pending_options = pendingWorkOptionsFromState(decode_state, 1);
        try scheduler.enqueueDecodeWork(lease.*, @ptrCast(&work), seq_len, decode_ctx.kv_sequence_len, decode_ctx.kv_position_offset, pending_options);
        defer if (!work.ready.load(.acquire)) scheduler.cancelDecodeWork(@ptrCast(&work));

        const coalesce_delay_us = if (self.execution_lock != null)
            scheduler.decodeCoalesceDelayUs(lease.*)
        else
            0;
        var coalesce_wait_error: ?anyerror = null;
        if (coalesce_delay_us > 0) {
            scheduler.noteDecodeCoalesceWait(coalesce_delay_us);
            io.sleep(std.Io.Duration.fromMicroseconds(coalesce_delay_us), .awake) catch |err| {
                coalesce_wait_error = err;
            };
        }

        var driver = DecodeStepDriver{
            .pipeline = self,
            .scheduler = scheduler,
            .work = &work,
            .decode_state = decode_state,
        };
        try runStepLoop(self.allocator, scheduler, lease, @ptrCast(&work), .decode, io, coalesce_wait_error, &driver);

        if (work.failure) |err| return err;
        const logits = work.logits orelse return error.InvalidBatchDecodeState;
        work.logits = null;
        return logits;
    }

    fn runScheduledPrefillBatch(
        self: *NativeGenerationPipeline,
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        io: std.Io,
        decode_state: *NativeDecodeState,
        token_ids: []const i64,
        seq_len: usize,
        query_seq_len: usize,
        wants_last_logits: bool,
    ) !?[]f32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        const decode_ctx = decode_runtime.makeDecodeContext(seq_len, query_seq_len);
        var work = PendingPrefillBatchWork{
            .allocator = self.allocator,
            .decode_state = decode_state,
            .token_ids = token_ids,
            .seq_len = seq_len,
            .query_seq_len = query_seq_len,
            .wants_last_logits = wants_last_logits,
        };
        defer if (work.logits) |logits| work.allocator.free(logits);
        var pending_options = pendingWorkOptionsFromState(decode_state, query_seq_len);
        if (self.execution_lock != null) {
            // CUDA row-batched prefill is not response-equivalent yet. Keep
            // prefill as a singleton; decode remains continuously batchable.
            pending_options.exclusive_step = true;
        }
        try scheduler.enqueuePrefillWork(lease.*, @ptrCast(&work), seq_len, query_seq_len, decode_ctx.kv_sequence_len, decode_ctx.kv_position_offset, pending_options);
        defer if (!work.ready.load(.acquire)) scheduler.cancelPrefillWork(@ptrCast(&work));

        var driver = PrefillStepDriver{
            .pipeline = self,
            .scheduler = scheduler,
            .work = &work,
            .decode_state = decode_state,
        };
        try runStepLoop(self.allocator, scheduler, lease, @ptrCast(&work), .prefill, io, null, &driver);

        if (work.failure) |err| return err;
        const logits = work.logits;
        work.logits = null;
        return logits;
    }

    /// Output of a successful step forward pass: the heap-allocated logits row
    /// buffer and the maximum query sequence length used to size each row.
    /// Caller owns the logits buffer.
    const StepForwardResult = struct {
        logits: ?[]f32,
        max_query_seq_len: usize,
    };

    fn scheduledStepNeedsLogits(claimed: []const runtime.scheduler.native_generate.StepItem) bool {
        for (claimed) |item| {
            switch (item.phase) {
                .decode => return true,
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    if (work.wants_last_logits) return true;
                },
                .waiting => return true,
            }
        }
        return false;
    }

    fn tryRunSingletonScheduledGemmaPrefillFrame(
        self: *NativeGenerationPipeline,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        decode_context: *const gpt_arch.DecodeContext,
    ) !bool {
        if (!singletonScheduledGemmaPrefillFrameEnabled()) return false;
        if (claimed.len != 1 or claimed[0].phase != .prefill) return false;
        const item = claimed[0];
        const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
        if (!shouldUseSingletonScheduledGemmaPrefillFrame(
            self.cb.kind(),
            &self.gpt_config,
            claimed.len,
            item.phase,
            item.query_sequence_len,
            work.wants_last_logits,
        )) return false;

        const Attempt = struct {
            pipeline: *NativeGenerationPipeline,
            work: *PendingPrefillBatchWork,
            decode_context: *const gpt_arch.DecodeContext,

            fn run(context: *anyopaque) !bool {
                const attempt: *@This() = @ptrCast(@alignCast(context));
                const pipeline = attempt.pipeline;
                const prepared = try pipeline.cb.decoderRuntimePrepareOrReuseFamily(
                    pipeline.allocator,
                    pipeline.gpt_config,
                    attempt.decode_context.kv_sequence_len,
                    pipeline.gpt_config.num_hidden_layers,
                );
                if (!prepared.prepared) return false;

                const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
                    &pipeline.cb,
                    pipeline.allocator,
                    pipeline.gpt_config,
                    pipeline.gpt_config.num_hidden_layers,
                    attempt.work.token_ids,
                    attempt.work.seq_len,
                    attempt.decode_context,
                    false,
                )) orelse return false;
                pipeline.cb.free(tail.final_hidden);
                return true;
            }
        };

        var attempt = Attempt{
            .pipeline = self,
            .work = work,
            .decode_context = decode_context,
        };
        return runWithDecoderRuntimeResetBoundary(&self.cb, @ptrCast(&attempt), Attempt.run);
    }

    /// Run a step's forward pass: allocate scratch, populate per-item context,
    /// reserve prefill state, build the mixed-batch decode context, and invoke
    /// the model. On any error before the forward pass returns successfully,
    /// any prefill reservations made so far are rolled back. Once the forward
    /// completes, the prefill state is committed and rollback is suppressed.
    fn forwardScheduledStep(
        self: *NativeGenerationPipeline,
        claimed: []const runtime.scheduler.native_generate.StepItem,
    ) !StepForwardResult {
        var items = try self.allocator.alloc(MixedBatchDecodeItem, claimed.len);
        defer self.allocator.free(items);

        var max_query_seq_len: usize = 0;
        for (claimed) |item| max_query_seq_len = @max(max_query_seq_len, item.query_sequence_len);

        const pad_id: i64 = self.tokenizer.specialTokens().pad_id;
        var input_ids = try self.allocator.alloc(i64, claimed.len * max_query_seq_len);
        defer self.allocator.free(input_ids);
        @memset(input_ids, pad_id);

        for (claimed, 0..) |item, idx| {
            switch (item.phase) {
                .decode => {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    items[idx] = .{
                        .state = work.decode_state,
                        .total_sequence_len = item.total_sequence_len,
                        .query_sequence_len = item.query_sequence_len,
                        .kv_sequence_len = item.kv_sequence_len,
                        .kv_position_offset = item.kv_position_offset,
                        .attention_mode = .paged_decode,
                    };
                    input_ids[idx * max_query_seq_len] = work.token_id;
                },
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    items[idx] = .{
                        .state = work.decode_state,
                        .total_sequence_len = item.total_sequence_len,
                        .query_sequence_len = item.query_sequence_len,
                        .kv_sequence_len = item.kv_sequence_len,
                        .kv_position_offset = item.kv_position_offset,
                        .attention_mode = .paged_prefill,
                    };
                    @memcpy(input_ids[idx * max_query_seq_len ..][0..work.query_seq_len], work.token_ids);
                },
                .waiting => return error.InvalidBatchDecodeState,
            }
        }

        const reserved = try self.allocator.alloc(usize, claimed.len);
        defer self.allocator.free(reserved);
        @memset(reserved, 0);

        // Track how many prefill items have been reserved so the errdefer
        // rollback only undoes reserves that were actually performed. The
        // counter is updated immediately after each successful reserve.
        var reserved_count: usize = 0;
        errdefer rollbackPrefillReservations(claimed, reserved, reserved_count);

        for (claimed, 0..) |item, idx| {
            if (item.phase != .prefill) continue;
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
            reserved[idx] = try reserveAndRefreshScheduledPrefill(&items[idx], work.seq_len);
            reserved_count = idx + 1;
        }

        var owned_ctx = try buildOwnedMixedBatchDecodeContext(self.allocator, items);
        defer owned_ctx.deinit();

        if (!scheduledStepNeedsLogits(claimed)) {
            if (try self.tryRunSingletonScheduledGemmaPrefillFrame(claimed, &owned_ctx.context)) {
                return .{ .logits = null, .max_query_seq_len = max_query_seq_len };
            }
            try gpt_arch.forwardHiddenOnly(&self.cb, self.allocator, self.gpt_config, input_ids, claimed.len, max_query_seq_len, &owned_ctx.context);
            return .{ .logits = null, .max_query_seq_len = max_query_seq_len };
        }

        const logits = try self.forwardAllLogits(input_ids, claimed.len, max_query_seq_len, &owned_ctx.context);
        // Past this point the forward has committed: the prefill state is
        // valid and rollback would corrupt it. Returning normally cancels the
        // errdefer above.
        return .{ .logits = logits, .max_query_seq_len = max_query_seq_len };
    }

    /// Drive a claimed step end-to-end. Always calls `scheduler.completeStep`
    /// before publishing `work.ready`, so a waiter can never observe ready
    /// work that the scheduler still owns. Per-item failures are surfaced via
    /// `work.failure`/`work.ready`; the function itself does not return errors
    /// to the caller.
    fn completeClaimedStepBeforePublish(
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        publisher: anytype,
    ) void {
        scheduler.completeStep(lease, claimed);
        publisher.publish(claimed);
    }

    const FailedStepPublisher = struct {
        err: anyerror,

        fn publish(self: @This(), claimed: []const runtime.scheduler.native_generate.StepItem) void {
            markStepFailed(claimed, self.err);
        }
    };

    const SuccessfulStepPublisher = struct {
        vocab_size: usize,
        logits: ?[]const f32,
        max_query_seq_len: usize,

        fn publish(self: @This(), claimed: []const runtime.scheduler.native_generate.StepItem) void {
            dispatchStepLogits(self.vocab_size, claimed, self.logits, self.max_query_seq_len);
        }
    };

    fn executeClaimedStep(
        self: *NativeGenerationPipeline,
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        claimed: []const runtime.scheduler.native_generate.StepItem,
    ) void {
        if (self.execution_lock) |mutex| {
            platform.sync.lockYielding(mutex);
        }
        defer if (self.execution_lock) |mutex| mutex.unlock();

        self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);

        const result = self.forwardScheduledStep(claimed) catch |err| {
            completeClaimedStepBeforePublish(scheduler, lease, claimed, FailedStepPublisher{ .err = err });
            return;
        };
        defer if (result.logits) |logits| self.allocator.free(logits);

        completeClaimedStepBeforePublish(scheduler, lease, claimed, SuccessfulStepPublisher{
            .vocab_size = self.gpt_config.vocab_size,
            .logits = result.logits,
            .max_query_seq_len = result.max_query_seq_len,
        });
    }

    fn reserveScheduledPrefillState(decode_state: *NativeDecodeState, target_total_seq_len: usize) !usize {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        return decode_runtime.reservePrefillTo(target_total_seq_len);
    }

    fn reserveAndRefreshScheduledPrefill(item: *MixedBatchDecodeItem, target_total_seq_len: usize) !usize {
        const reserved = try reserveScheduledPrefillState(item.state, target_total_seq_len);
        // The scheduler item captures KV geometry before this chunk is
        // reserved. Refresh it from the now-current state so paged KV writes
        // append the chunk instead of reusing the prior chunk's suffix range.
        refreshReservedPrefillGeometry(item);
        return reserved;
    }

    fn refreshReservedPrefillGeometry(item: *MixedBatchDecodeItem) void {
        var decode_runtime = BorrowedDecodeStateRuntime.init(item.state);
        const decode_ctx = decode_runtime.makeDecodeContext(item.total_sequence_len, item.query_sequence_len);
        item.kv_sequence_len = decode_ctx.kv_sequence_len;
        item.kv_position_offset = decode_ctx.kv_position_offset;
    }

    fn rollbackScheduledPrefillState(decode_state: *NativeDecodeState, reserved: usize) !void {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        try decode_runtime.rollbackReservedPrefill(reserved);
    }

    /// Roll back any prefill reservations that have been completed so far.
    /// `reserved_count` is the number of items at the front of `claimed` that
    /// were processed by the reservation loop; reservations beyond that index
    /// were never made and must not be undone. Errors during rollback are
    /// swallowed — the goal is to leave decode state in the most consistent
    /// shape we can on partial failure.
    fn rollbackPrefillReservations(
        claimed: []const runtime.scheduler.native_generate.StepItem,
        reserved: []const usize,
        reserved_count: usize,
    ) void {
        if (reserved_count == 0) return;
        const limit = @min(reserved_count, claimed.len);
        var idx: usize = 0;
        while (idx < limit) : (idx += 1) {
            if (claimed[idx].phase != .prefill) continue;
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(claimed[idx].work_ptr));
            rollbackScheduledPrefillState(work.decode_state, reserved[idx]) catch {};
        }
    }

    /// Surface a step-level failure as a per-item failure on every claimed
    /// work, marking each ready so its caller's wait loop unblocks. Used when
    /// the step forward pass cannot run at all (allocation failure, build
    /// context failure, forward kernel failure). Pairs with
    /// `scheduler.completeStep` to remove the pending entries.
    fn markStepFailed(
        claimed: []const runtime.scheduler.native_generate.StepItem,
        err: anyerror,
    ) void {
        if (platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_LAZY_PROFILE")) {
            for (claimed) |item| {
                std.log.err(
                    "scheduled_step_failed: phase={s} err={s} query_seq_len={d} total_seq_len={d} kv_seq_len={d} kv_pos={d}",
                    .{
                        @tagName(item.phase),
                        @errorName(err),
                        item.query_sequence_len,
                        item.total_sequence_len,
                        item.kv_sequence_len,
                        item.kv_position_offset,
                    },
                );
            }
        }
        for (claimed) |item| {
            switch (item.phase) {
                .decode => {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    work.failure = err;
                    work.ready.store(true, .release);
                },
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    work.failure = err;
                    work.ready.store(true, .release);
                },
                .waiting => {},
            }
        }
    }

    /// Slice a flat logits buffer into per-item rows and dupe each row onto
    /// the owning work. A per-work `dupe` failure is recorded as that work's
    /// `failure` only — peer works in the same step have already executed and
    /// must still complete cleanly.
    fn dispatchStepLogits(
        vocab_size: usize,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        logits: ?[]const f32,
        max_query_seq_len: usize,
    ) void {
        const logits_buf = logits orelse {
            for (claimed) |item| {
                switch (item.phase) {
                    .decode => {
                        const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                        work.failure = error.InvalidBatchDecodeState;
                        work.ready.store(true, .release);
                    },
                    .prefill => {
                        const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                        if (work.wants_last_logits) work.failure = error.InvalidBatchDecodeState;
                        work.ready.store(true, .release);
                    },
                    .waiting => {},
                }
            }
            return;
        };
        for (claimed, 0..) |item, idx| {
            const row_index = idx * max_query_seq_len + (item.query_sequence_len - 1);
            const start = row_index * vocab_size;
            const slice = logits_buf[start..][0..vocab_size];
            switch (item.phase) {
                .decode => {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    if (work.allocator.dupe(f32, slice)) |buf| {
                        work.logits = buf;
                    } else |dupe_err| {
                        work.failure = dupe_err;
                    }
                    work.ready.store(true, .release);
                },
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    if (work.wants_last_logits) {
                        if (work.allocator.dupe(f32, slice)) |buf| {
                            work.logits = buf;
                        } else |dupe_err| {
                            work.failure = dupe_err;
                        }
                    }
                    work.ready.store(true, .release);
                },
                .waiting => {},
            }
        }
    }
};

/// Drive a request's step loop to completion. Each iteration consults the
/// driver for a per-step KV-aware budget, asks the scheduler to claim a step,
/// dispatches the claim through the driver's executor, and yields when no
/// step is currently claimable.
///
/// Driver contract (duck-typed via `anytype`, must be a pointer):
/// - `fn isReady(self) bool`           — when to exit the loop
/// - `fn stepBudget(self) StepBudget`  — recomputed each iteration
/// - `fn execute(self, scheduler, lease, claimed)` — dispatch a claimed step
/// - `fn preStep(self) void`           — optional, runs before each claim
fn runStepLoop(
    allocator: std.mem.Allocator,
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    lease: *runtime.scheduler.native_generate.Lease,
    leader_work_ptr: *anyopaque,
    leader_phase: runtime.scheduler.native_generate.Phase,
    io: std.Io,
    initial_wait_error: ?anyerror,
    driver: anytype,
) !void {
    const DriverInfo = @typeInfo(@TypeOf(driver));
    const DriverChild = DriverInfo.pointer.child;

    var step = std.ArrayListUnmanaged(runtime.scheduler.native_generate.StepItem).empty;
    defer step.deinit(allocator);
    var wait_error = initial_wait_error;

    while (!driver.isReady()) {
        if (wait_error) |err| {
            // Cancellation cannot drop a stack-backed work pointer while a
            // peer may own it. Remove idle work immediately; otherwise wait
            // for the claimed step to publish ready before returning.
            if (scheduler.cancelPendingWorkIfIdle(leader_work_ptr, leader_phase)) return err;
            // ponytail: bounded polling while an in-flight forward drains;
            // replace with a scheduler event if canceled-waiter volume grows.
            platform.time.yieldBriefly();
            continue;
        }
        if (@hasDecl(DriverChild, "preStep")) driver.preStep();
        const budget = driver.stepBudget();
        const claimed = scheduler.claimStep(allocator, lease, leader_work_ptr, leader_phase, budget, &step) catch |err| {
            // A peer may already own this request's stack-backed work pointer.
            // Enter the same drain-or-cancel path as I/O cancellation instead
            // of unwinding it while a claimed step can still publish to it.
            wait_error = err;
            continue;
        };
        if (claimed) {
            driver.execute(scheduler, lease, step.items);
        } else {
            io.sleep(std.Io.Duration.fromMilliseconds(0), .awake) catch |err| {
                wait_error = err;
            };
        }
    }
    if (wait_error) |err| return err;
}

const DecodeStepDriver = struct {
    pipeline: *NativeGenerationPipeline,
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    work: *NativeGenerationPipeline.PendingDecodeBatchWork,
    decode_state: *NativeDecodeState,

    fn isReady(self: *const DecodeStepDriver) bool {
        return self.work.ready.load(.acquire);
    }

    fn stepBudget(self: *const DecodeStepDriver) runtime.scheduler.native_generate.StepBudget {
        return stepBudgetFromState(self.scheduler, self.decode_state);
    }

    fn preStep(self: *DecodeStepDriver) void {
        if (self.pipeline.execution_lock != null) return;
        self.pipeline.cb.drainPrefetchBudget(NativeGenerationPipeline.prefetch_drain_budget_per_step);
    }

    fn execute(
        self: *DecodeStepDriver,
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        claimed: []const runtime.scheduler.native_generate.StepItem,
    ) void {
        self.pipeline.executeClaimedStep(scheduler, lease, claimed);
    }
};

const PrefillStepDriver = struct {
    pipeline: *NativeGenerationPipeline,
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    work: *NativeGenerationPipeline.PendingPrefillBatchWork,
    decode_state: *NativeDecodeState,

    fn isReady(self: *const PrefillStepDriver) bool {
        return self.work.ready.load(.acquire);
    }

    fn stepBudget(self: *const PrefillStepDriver) runtime.scheduler.native_generate.StepBudget {
        return stepBudgetFromState(self.scheduler, self.decode_state);
    }

    fn execute(
        self: *PrefillStepDriver,
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        claimed: []const runtime.scheduler.native_generate.StepItem,
    ) void {
        self.pipeline.executeClaimedStep(scheduler, lease, claimed);
    }
};

/// Per-step admission budget reflecting the live KV pool headroom for the
/// requesting state. When the underlying pool has no soft cap configured,
/// `max_kv_blocks` stays unset and the scheduler treats the pool as
/// unbounded. When a cap is configured, we shave a single-block safety margin
/// so a step never admits the very last block under contention.
fn stepBudgetFromState(
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    decode_state: *NativeDecodeState,
) runtime.scheduler.native_generate.StepBudget {
    var budget = scheduler.defaultStepBudget();
    const lock = decode_state.lockPagedKv();
    defer if (lock) |mutex| mutex.unlock();
    const km = decode_state.kv_manager orelse return budget;
    const pool_id = decode_state.pool_id orelse return budget;
    const avail = km.poolAvailableBlocks(pool_id) orelse return budget;
    const safety: usize = 1;
    budget.max_kv_blocks = if (avail > safety) avail - safety else 0;
    return budget;
}

/// Tag the just-enqueued pending work with its KV-block cost so the scheduler
/// can apply per-step KV admission against pool headroom. A no-op when the
/// state has no kv_manager wired (non-paged backends).
fn notePendingKvBlocksFromState(
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    decode_state: *NativeDecodeState,
    work_ptr: *anyopaque,
    phase: runtime.scheduler.native_generate.Phase,
    additional_tokens: usize,
) void {
    const lock = decode_state.lockPagedKv();
    defer if (lock) |mutex| mutex.unlock();
    const km = decode_state.kv_manager orelse return;
    const seq_id = decode_state.sequence_id orelse return;
    const est = km.estimateBlocksFor(seq_id, additional_tokens) orelse return;
    scheduler.notePendingKvBlocks(work_ptr, phase, est);
}

fn pendingWorkOptionsFromState(
    decode_state: *NativeDecodeState,
    additional_tokens: usize,
) runtime.scheduler.native_generate.PendingWorkOptions {
    var options = runtime.scheduler.native_generate.PendingWorkOptions{
        .exclusive_step = decode_state.deepseek_v4_compressed_cache != null,
    };
    const lock = decode_state.lockPagedKv();
    defer if (lock) |mutex| mutex.unlock();
    const km = decode_state.kv_manager orelse return options;
    const seq_id = decode_state.sequence_id orelse return options;
    options.kv_blocks_estimate = km.estimateBlocksFor(seq_id, additional_tokens) orelse 0;
    return options;
}

fn notePendingExclusiveStepFromState(
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    decode_state: *const NativeDecodeState,
    work_ptr: *anyopaque,
    phase: runtime.scheduler.native_generate.Phase,
) void {
    if (decode_state.deepseek_v4_compressed_cache != null) {
        scheduler.notePendingExclusiveStep(work_ptr, phase, true);
    }
}

fn timestampDurationMillis(from: std.Io.Timestamp, to: std.Io.Timestamp) u64 {
    return @intCast(@divTrunc(std.Io.Timestamp.durationTo(from, to).nanoseconds, std.time.ns_per_ms));
}

pub fn maybePrependBos(encoded: *tokenizer_mod.EncodeResult, bos_token_id: i32, add_bos_token: bool) void {
    if (!add_bos_token) return;
    if (bos_token_id < 0) return;

    var actual_prompt_tokens: usize = 0;
    while (actual_prompt_tokens < encoded.attention_mask.len and encoded.attention_mask[actual_prompt_tokens] != 0) : (actual_prompt_tokens += 1) {}
    if (actual_prompt_tokens == 0) return;
    if (encoded.ids[0] == bos_token_id) return;

    const limit = encoded.ids.len;
    const copy_count = @min(actual_prompt_tokens, limit - 1);
    var i = copy_count;
    while (i > 0) : (i -= 1) {
        encoded.ids[i] = encoded.ids[i - 1];
        encoded.attention_mask[i] = encoded.attention_mask[i - 1];
    }
    encoded.ids[0] = bos_token_id;
    encoded.attention_mask[0] = 1;
}

pub fn sampleTokenFromLogits(
    allocator: std.mem.Allocator,
    logits: []const f32,
    config: GenerationConfig,
    token_history: []const i64,
) usize {
    var penalty_state = SamplingPenaltyState{};
    defer penalty_state.deinit(allocator);
    penalty_state.seedFromHistory(allocator, token_history) catch {};
    return sample(@constCast(logits), config, &penalty_state, allocator);
}

pub fn encodePromptForGeneration(
    tokenizer: tokenizer_mod.Tokenizer,
    allocator: std.mem.Allocator,
    prompt: []const u8,
    max_length: usize,
    add_bos_token: bool,
    bos_token: []const u8,
) !tokenizer_mod.EncodeResult {
    if (add_bos_token and bos_token.len > 0 and std.mem.startsWith(u8, prompt, bos_token)) {
        return tokenizer.encodeForGenerationConfigured(
            allocator,
            prompt,
            max_length,
            false,
        );
    }
    return tokenizer.encodeForGenerationConfigured(
        allocator,
        prompt,
        max_length,
        shouldAddBosToken(prompt, add_bos_token, bos_token),
    );
}

/// Maximum text tokens that can be admitted without exceeding either model's
/// context after generation and known multimodal expansion.
pub fn nativeGenerationPromptTokenLimit(
    target_config: gpt_mod.Config,
    draft_config: ?gpt_mod.Config,
    requested_output_tokens: usize,
    speculative_bonus_tokens: usize,
    media_token_allowance: usize,
) !usize {
    var context_tokens: usize = @intCast(target_config.max_position_embeddings);
    if (draft_config) |draft| {
        context_tokens = @min(context_tokens, @as(usize, @intCast(draft.max_position_embeddings)));
    }
    const output_tokens = std.math.add(
        usize,
        @max(requested_output_tokens, 1),
        speculative_bonus_tokens,
    ) catch return error.PromptTooLong;
    const reserved_tokens = std.math.add(
        usize,
        output_tokens,
        media_token_allowance,
    ) catch return error.PromptTooLong;
    if (context_tokens <= reserved_tokens) return error.PromptTooLong;
    return context_tokens - reserved_tokens;
}

pub fn validateNativeGenerationPromptTokenCount(
    prompt_tokens: usize,
    target_config: gpt_mod.Config,
    draft_config: ?gpt_mod.Config,
    requested_output_tokens: usize,
    speculative_bonus_tokens: usize,
) !void {
    const limit = try nativeGenerationPromptTokenLimit(
        target_config,
        draft_config,
        requested_output_tokens,
        speculative_bonus_tokens,
        0,
    );
    if (prompt_tokens > limit) return error.PromptTooLong;
}

/// Encode one native-generation prompt without silently dropping tokens at the
/// model boundary. The extra slot distinguishes an exact fit from overflow.
pub fn encodeNativeGenerationPrompt(
    tokenizer: tokenizer_mod.Tokenizer,
    allocator: std.mem.Allocator,
    prompt: []const u8,
    prompt_token_limit: usize,
    add_bos_token: bool,
    bos_token: []const u8,
) !tokenizer_mod.EncodeResult {
    if (prompt_token_limit == 0) return error.PromptTooLong;
    const detection_limit = std.math.add(usize, prompt_token_limit, 1) catch prompt_token_limit;
    var encoded = try encodePromptForGeneration(
        tokenizer,
        allocator,
        prompt,
        detection_limit,
        add_bos_token,
        bos_token,
    );
    errdefer encoded.deinit();

    var token_count: usize = 0;
    while (token_count < encoded.attention_mask.len and encoded.attention_mask[token_count] != 0) : (token_count += 1) {}
    if (token_count > prompt_token_limit) return error.PromptTooLong;
    return encoded;
}

fn shouldAddBosToken(prompt: []const u8, add_bos_token: bool, bos_token: []const u8) bool {
    if (!add_bos_token) return false;
    if (bos_token.len == 0) return true;
    return !std.mem.startsWith(u8, prompt, bos_token);
}

fn encodeQwenPromptWithImagePlaceholders(
    tokenizer: tokenizer_mod.Tokenizer,
    allocator: std.mem.Allocator,
    prompt: []const u8,
    max_length: usize,
    add_bos_token: bool,
    bos_token: []const u8,
    config: gpt_mod.Config,
) !tokenizer_mod.EncodeResult {
    const marker = "<start_of_image>";
    var ids = std.ArrayListUnmanaged(i32).empty;
    errdefer ids.deinit(allocator);

    var read_pos: usize = 0;
    var first_text = true;
    while (std.mem.indexOfPos(u8, prompt, read_pos, marker)) |idx| {
        try appendQwenPromptTextTokens(
            tokenizer,
            allocator,
            &ids,
            prompt[read_pos..idx],
            max_length,
            if (first_text) add_bos_token else false,
            if (first_text) bos_token else "",
        );
        first_text = false;
        try appendQwenVisualPlaceholderTokens(allocator, &ids, max_length, config);
        read_pos = idx + marker.len;
    }

    try appendQwenPromptTextTokens(
        tokenizer,
        allocator,
        &ids,
        prompt[read_pos..],
        max_length,
        if (first_text) add_bos_token else false,
        if (first_text) bos_token else "",
    );

    const mask = try allocator.alloc(i32, ids.items.len);
    @memset(mask, 1);
    return .{
        .ids = try ids.toOwnedSlice(allocator),
        .attention_mask = mask,
        .allocator = allocator,
    };
}

fn appendQwenPromptTextTokens(
    tokenizer: tokenizer_mod.Tokenizer,
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(i32),
    text: []const u8,
    max_length: usize,
    add_bos_token: bool,
    bos_token: []const u8,
) !void {
    if (text.len == 0 and !add_bos_token) return;
    if (out.items.len >= max_length) return error.PromptTooLong;
    const remaining = max_length - out.items.len;
    var encoded = try encodeNativeGenerationPrompt(tokenizer, allocator, text, remaining, add_bos_token, bos_token);
    defer encoded.deinit();
    var token_count: usize = 0;
    while (token_count < encoded.attention_mask.len and encoded.attention_mask[token_count] != 0) : (token_count += 1) {}
    try out.appendSlice(allocator, encoded.ids[0..token_count]);
}

fn appendQwenVisualPlaceholderTokens(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(i32),
    max_length: usize,
    config: gpt_mod.Config,
) !void {
    if (config.image_token_index < 0) return error.InvalidMultimodalConfig;
    const needed: usize = 1 +
        (if (config.boi_token_index >= 0) @as(usize, 1) else 0) +
        (if (config.eoi_token_index >= 0) @as(usize, 1) else 0);
    if (out.items.len + needed > max_length) return error.PromptTooLong;
    if (config.boi_token_index >= 0) try out.append(allocator, config.boi_token_index);
    try out.append(allocator, config.image_token_index);
    if (config.eoi_token_index >= 0) try out.append(allocator, config.eoi_token_index);
}

test "completed streaming fallback emits full text exactly once when callback stops" {
    const CallbackContext = struct {
        expected: []const u8,
        calls: usize = 0,
        matched: bool = false,

        fn onToken(raw_ctx: *anyopaque, text: []const u8) bool {
            const ctx: *@This() = @ptrCast(@alignCast(raw_ctx));
            ctx.calls += 1;
            ctx.matched = std.mem.eql(u8, ctx.expected, text);
            return false;
        }
    };

    var ctx = CallbackContext{ .expected = "complete multimodal response" };
    try std.testing.expect(!emitCompletedStreamingText(&ctx, CallbackContext.onToken, ctx.expected));
    try std.testing.expectEqual(@as(usize, 1), ctx.calls);
    try std.testing.expect(ctx.matched);
}

test "first token callback cancellation prevents speculative entry" {
    try std.testing.expect(shouldEnterSpeculativeDecodeLoop(true, false, false));
    try std.testing.expect(!shouldEnterSpeculativeDecodeLoop(false, false, false));
    try std.testing.expect(!shouldEnterSpeculativeDecodeLoop(true, true, false));
    try std.testing.expect(!shouldEnterSpeculativeDecodeLoop(true, false, true));
}

test "shouldAddBosToken skips duplicate literal bos prefix" {
    try std.testing.expect(!shouldAddBosToken("<bos><start_of_turn>user\nHello", true, "<bos>"));
}

test "native generation prompt limit reserves output media and draft context" {
    const target = gpt_mod.Config{ .max_position_embeddings = 8192 };
    const draft = gpt_mod.Config{ .max_position_embeddings = 4096 };
    try std.testing.expectEqual(
        @as(usize, 3775),
        try nativeGenerationPromptTokenLimit(target, draft, 64, 1, 256),
    );
    try std.testing.expectEqual(
        @as(usize, 8128),
        try nativeGenerationPromptTokenLimit(target, null, 64, 0, 0),
    );
    try std.testing.expectError(
        error.PromptTooLong,
        nativeGenerationPromptTokenLimit(.{ .max_position_embeddings = 64 }, null, 64, 0, 0),
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        try nativeGenerationPromptTokenLimit(.{ .max_position_embeddings = 65 }, null, 64, 0, 0),
    );
    try std.testing.expectError(
        error.PromptTooLong,
        nativeGenerationPromptTokenLimit(.{ .max_position_embeddings = 65 }, null, 64, 1, 0),
    );
    try std.testing.expectError(
        error.PromptTooLong,
        nativeGenerationPromptTokenLimit(target, null, std.math.maxInt(usize), 1, 0),
    );
    try validateNativeGenerationPromptTokenCount(4031, target, draft, 64, 1);
    try std.testing.expectError(
        error.PromptTooLong,
        validateNativeGenerationPromptTokenCount(4032, target, draft, 64, 1),
    );

    const images = [_][]const u8{ "a", "b" };
    const messages = [_]Message{.{ .role = "user", .content = "describe", .image_bytes = &images }};
    try std.testing.expectEqual(
        @as(usize, 514),
        nativeGenerationMediaTokenAllowance(&messages, .{ .mm_tokens_per_image = 256 }),
    );
    try std.testing.expectEqual(
        @as(usize, 0),
        nativeGenerationPreliminaryMediaTokenAllowance(&messages, .{
            .family = .qwen3_5,
            .mm_tokens_per_image = 256,
        }),
    );
    try std.testing.expectEqual(
        @as(usize, 514),
        nativeGenerationPreliminaryMediaTokenAllowance(&messages, .{
            .family = .gemma,
            .mm_tokens_per_image = 256,
        }),
    );
}

test "Qwen3-VL media admission uses exact image geometry before scheduling" {
    var png_header = [_]u8{0} ** 24;
    @memcpy(png_header[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, png_header[8..12], 13, .big);
    @memcpy(png_header[12..16], "IHDR");
    std.mem.writeInt(u32, png_header[16..20], 2048, .big);
    std.mem.writeInt(u32, png_header[20..24], 1416, .big);
    const images = [_][]const u8{&png_header};
    const messages = [_]Message{.{
        .role = "user",
        .content = "describe",
        .image_bytes = &images,
    }};
    const config = gpt_mod.Config{
        .family = .qwen3_vl,
        .hidden_size = 2048,
        .vision_hidden_size = 1024,
        .vision_intermediate_size = 4096,
        .vision_num_attention_heads = 16,
        .vision_patch_size = 16,
        .vision_spatial_merge_size = 2,
        .vision_deepstack_visual_indexes_len = 3,
    };
    const admission = try nativeGenerationMediaAdmission(
        std.testing.allocator,
        "unused-for-qwen3-vl",
        &messages,
        config,
    );
    try std.testing.expectEqual(@as(usize, 533), admission.token_allowance);
    try std.testing.expect(admission.host_scratch_bytes > 0);
    try std.testing.expect(admission.backend_scratch_bytes > 0);
    try std.testing.expectEqual(
        @as(usize, 0),
        nativeGenerationPreliminaryMediaTokenAllowance(&messages, config),
    );
}

test "native generation prefill ceiling only specializes Gemma4 Metal target" {
    const gemma4_target = gpt_mod.Config{
        .family = .gemma,
        .num_attention_heads = 8,
        .num_key_value_heads = 2,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .ple_hidden_size = 2560,
    };
    try std.testing.expectEqual(
        @as(usize, 512),
        nativeGenerationPrefillChunkCeiling(.metal, gemma4_target, 2048),
    );
    try std.testing.expectEqual(
        @as(usize, 64),
        nativeGenerationPrefillChunkCeiling(.metal, gemma4_target, 64),
    );
    try std.testing.expectEqual(
        @as(usize, 2048),
        nativeGenerationPrefillChunkCeiling(.cuda, gemma4_target, 2048),
    );
    const gemma4_e2b_cuda = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .intermediate_size = 6144,
        .shared_layer_intermediate_size = 12288,
        .num_kv_shared_layers = 20,
        .sliding_window = 512,
        .ple_hidden_size = 256,
    };
    try std.testing.expectEqual(
        @as(usize, 512),
        nativeGenerationPrefillChunkCeiling(.cuda, gemma4_e2b_cuda, 2048),
    );
    try std.testing.expectEqual(
        @as(usize, 256),
        nativeGenerationPrefillChunkCeiling(.cuda, gemma4_e2b_cuda, 256),
    );
    try std.testing.expectEqual(
        @as(usize, 2048),
        nativeGenerationPrefillChunkCeiling(.metal, .{ .family = .gemma }, 2048),
    );
    inline for (.{
        gpt_mod.Config{ .family = .gemma, .num_attention_heads = 7, .num_key_value_heads = 2, .attention_head_dim = 256, .global_head_dim = 512, .sliding_window = 512, .ple_hidden_size = 2560 },
        gpt_mod.Config{ .family = .gemma, .num_attention_heads = 8, .num_key_value_heads = 1, .attention_head_dim = 256, .global_head_dim = 512, .sliding_window = 512, .ple_hidden_size = 2560 },
        gpt_mod.Config{ .family = .gemma, .num_attention_heads = 8, .num_key_value_heads = 2, .attention_head_dim = 128, .global_head_dim = 512, .sliding_window = 512, .ple_hidden_size = 2560 },
        gpt_mod.Config{ .family = .gemma, .num_attention_heads = 8, .num_key_value_heads = 2, .attention_head_dim = 256, .global_head_dim = 256, .sliding_window = 512, .ple_hidden_size = 2560 },
        gpt_mod.Config{ .family = .gemma, .num_attention_heads = 8, .num_key_value_heads = 2, .attention_head_dim = 256, .global_head_dim = 512, .sliding_window = 1024, .ple_hidden_size = 2560 },
    }) |other_geometry| {
        try std.testing.expectEqual(
            @as(usize, 2048),
            nativeGenerationPrefillChunkCeiling(.metal, other_geometry, 2048),
        );
    }
    var assistant = gemma4_target;
    assistant.gemma4_mtp_assistant = true;
    try std.testing.expectEqual(
        @as(usize, 2048),
        nativeGenerationPrefillChunkCeiling(.metal, assistant, 2048),
    );
}

test "encodePromptForGeneration does not duplicate literal bos prefix" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"<bos>": 2, "Hello": 3},
        \\    "merges": []
        \\  },
        \\  "added_tokens": [
        \\    {"id": 2, "content": "<bos>", "special": true}
        \\  ]
        \\}
    ;

    var tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer tok.deinitSelf();

    var encoded = try encodePromptForGeneration(tok.tokenizer(), allocator, "<bos>Hello", 8, true, "<bos>");
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 2), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 3), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 0), encoded.attention_mask[2]);

    try std.testing.expectError(
        error.PromptTooLong,
        encodeNativeGenerationPrompt(tok.tokenizer(), allocator, "<bos>Hello", 1, true, "<bos>"),
    );
}

test "grammar prompt opens Gemma4 public final channel" {
    const allocator = std.testing.allocator;
    const thought_prompt =
        "<bos><|turn>user\nReturn JSON<turn|>\n<|turn>model\n" ++
        gemma4_thought_channel_prompt_suffix ++ "\n";
    const expected =
        "<bos><|turn>user\nReturn JSON<turn|>\n<|turn>model\n" ++
        gemma4_final_channel_prompt_suffix ++ "\n";
    const opened = try openGemma4FinalChannelForGrammar(allocator, thought_prompt);
    defer allocator.free(opened);
    try std.testing.expectEqualStrings(expected, opened);

    const already_open = try openGemma4FinalChannelForGrammar(allocator, expected);
    defer allocator.free(already_open);
    try std.testing.expectEqualStrings(expected, already_open);

    try std.testing.expectError(
        error.GrammarRequiresGemma4ChannelPrompt,
        openGemma4FinalChannelForGrammar(allocator, "<bos>raw prompt"),
    );
}

test "chat template enable_thinking false opens an explicit final channel" {
    const allocator = std.testing.allocator;
    const source =
        "{{ bos_token }}" ++
        "{%- if add_generation_prompt -%}" ++
        "{%- if enable_thinking is defined and not enable_thinking -%}" ++
        "{{ '<|channel>final\\n<channel|>' }}" ++
        "{%- else -%}" ++
        "{{ '<|channel>thought\\n<channel|>' }}" ++
        "{%- endif -%}" ++
        "{%- endif -%}";
    var template = try ChatTemplate.init(allocator, source, "<bos>", "", "", "");
    defer template.deinit();

    const messages = [_]Message{.{ .role = "user", .content = "hello" }};
    const default_prompt = try template.apply(allocator, &messages, true);
    defer allocator.free(default_prompt);
    try std.testing.expectEqualStrings(
        "<bos><|channel>thought\n<channel|>",
        default_prompt,
    );
    try std.testing.expect(!promptOpensGemma4FinalChannel(default_prompt));

    const final_prompt = try template.applyWithOptions(allocator, &messages, .{
        .enable_thinking = false,
    });
    defer allocator.free(final_prompt);
    try std.testing.expectEqualStrings(
        "<bos><|channel>final\n<channel|>",
        final_prompt,
    );
    try std.testing.expect(promptOpensGemma4FinalChannel(final_prompt));
    try std.testing.expect(promptOpensGemma4FinalChannel(
        "<bos><|channel>final\n<channel|>\n",
    ));
    try std.testing.expect(configPromptOpensGemma4FinalChannel(.{
        .family = .gemma,
        .gemma4_channel_protocol = true,
    }, final_prompt));
    try std.testing.expect(!configPromptOpensGemma4FinalChannel(.{
        .family = .gemma,
    }, final_prompt));
}

test "serving chat templates preserve ChatML Llama and Gemma prompt bytes" {
    const allocator = std.testing.allocator;
    const messages = [_]Message{
        .{ .role = "user", .content = "Hello" },
        .{ .role = "assistant", .content = "Hi" },
    };
    const cases = [_]struct {
        source: []const u8,
        bos: []const u8,
        expected: []const u8,
    }{
        .{
            .source = "{%- for message in messages %}\n{{ '<|im_start|>' + message['role'] + '\\n' + message['content'] + '<|im_end|>\\n' }}\n{%- endfor %}\n{%- if add_generation_prompt %}\n{{ '<|im_start|>assistant\\n' }}\n{%- endif %}",
            .bos = "",
            .expected = "<|im_start|>user\nHello<|im_end|>\n\n<|im_start|>assistant\nHi<|im_end|>\n<|im_start|>assistant\n",
        },
        .{
            .source = "{{ bos_token }}{%- for message in messages %}\n{{ '<|start_header_id|>' + message['role'] + '<|end_header_id|>\\n\\n' + message['content'] + '<|eot_id|>' }}\n{%- endfor %}\n{%- if add_generation_prompt %}{{ '<|start_header_id|>assistant<|end_header_id|>\\n\\n' }}{%- endif %}",
            .bos = "<|begin_of_text|>",
            .expected = "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\nHello<|eot_id|>\n<|start_header_id|>assistant<|end_header_id|>\n\nHi<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n",
        },
        .{
            .source = "{{ bos_token }}{%- for message in messages %}\n{{ '<start_of_turn>' + message['role'] + '\\n' + message['content'] + '<end_of_turn>\\n' }}\n{%- endfor %}\n{%- if add_generation_prompt %}{{ '<start_of_turn>assistant\\n' }}{%- endif %}",
            .bos = "<bos>",
            .expected = "<bos><start_of_turn>user\nHello<end_of_turn>\n\n<start_of_turn>assistant\nHi<end_of_turn>\n<start_of_turn>assistant\n",
        },
    };
    for (cases) |case| {
        var template = try ChatTemplate.init(allocator, case.source, case.bos, "", "", "");
        defer template.deinit();
        const prompt = try template.apply(allocator, &messages, true);
        defer allocator.free(prompt);
        try std.testing.expectEqualStrings(case.expected, prompt);
    }
}

test "final channel projection requires exact header and stops before trailing channels" {
    const header = [_]i32{ 102, 103, 104, 101 };
    const ids = [_]i64{ 7, 101, 9, 102, 103, 104, 101, 10, 106 };
    try std.testing.expectEqual(@as(?usize, 7), finalChannelContentStart(&ids, 101));
    try std.testing.expectEqualSlices(i64, &.{ 10, 106 }, finalChannelTokenSlice(&ids, 101));
    try std.testing.expectEqualSlices(i64, &ids, finalChannelTokenSlice(&ids, null));
    try std.testing.expectEqualSlices(i64, &ids, finalChannelTokenSlice(&ids, 999));
    const range = completeFinalChannelRange(&ids, &header, 101, 102, 106) orelse
        return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 7), range.start);
    try std.testing.expectEqual(@as(usize, 8), range.end);
    try std.testing.expectEqualSlices(
        i64,
        &.{10},
        finalResponseTokenSlice(&ids, true, false, 101, &header, 102, 106),
    );

    const malformed_trailing = [_]i64{
        7,   101, 9,   102, 103, 104, 101, 10,
        102, 103, 104, 101, 7,   106,
    };
    try std.testing.expectEqualSlices(
        i64,
        &.{10},
        finalResponseTokenSlice(&malformed_trailing, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{10},
        finalResponseTokenSlice(&.{ 102, 103, 104, 101, 10, 101, 7 }, true, false, 101, &header, 102, 106),
    );
    // Without any explicit header in the stream, the bare-close convention
    // applies: content after the first bare <channel|> is the public answer.
    try std.testing.expectEqualSlices(
        i64,
        &.{10},
        finalResponseTokenSlice(&.{ 7, 101, 10 }, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        finalResponseTokenSlice(&.{ 7, 8 }, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        finalResponseTokenSlice(&.{ 7, 8 }, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        finalResponseTokenSlice(&.{ 7, 8 }, true, false, null, &.{}, null, null),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8 },
        finalResponseTokenSlice(&.{ 7, 8 }, false, false, null, &.{}, null, null),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{ 10, 11 },
        finalResponseTokenSlice(&.{ 10, 11, 101, 7 }, true, true, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        finalResponseTokenSlice(&.{ 10, 11 }, true, true, null, &.{}, null, null),
    );
}

test "bare channel close projects public content when no final header exists" {
    const header = [_]i32{ 102, 103, 104, 101 };
    // ggml-org Gemma4 GGUF convention: thought tokens, bare <channel|>,
    // public answer, then <turn|> (possibly repeated as EOS padding).
    try std.testing.expectEqualSlices(
        i64,
        &.{ 20, 21, 22 },
        finalResponseTokenSlice(&.{ 7, 8, 9, 101, 20, 21, 22, 106, 106 }, true, false, 101, &header, 102, 106),
    );
    // Only the first bare close opens public content; a later boundary ends it.
    try std.testing.expectEqualSlices(
        i64,
        &.{20},
        finalResponseTokenSlice(&.{ 7, 101, 20, 101, 21 }, true, false, 101, &header, 102, 106),
    );
    // An explicit final header anywhere still wins over the bare-close rule.
    try std.testing.expectEqualSlices(
        i64,
        &.{30},
        finalResponseTokenSlice(&.{ 7, 101, 20, 102, 103, 104, 101, 30, 106 }, true, false, 101, &header, 102, 106),
    );
    // Content of an explicitly opened non-final channel stays private; content
    // after its bare close is public.
    try std.testing.expectEqualSlices(
        i64,
        &.{40},
        finalResponseTokenSlice(&.{ 102, 103, 105, 101, 7, 8, 101, 40, 106 }, true, false, 101, &header, 102, 106),
    );
    // A bare close with nothing after it fails closed.
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        finalResponseTokenSlice(&.{ 7, 101 }, true, false, 101, &header, 102, 106),
    );
    // An unterminated explicit header fails closed.
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        finalResponseTokenSlice(&.{ 102, 103, 7, 8 }, true, false, 101, &header, 102, 106),
    );
}

test "reasoning channel projection separates private thought from public content" {
    const header = [_]i32{ 102, 103, 104, 101 };
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8, 9 },
        reasoningResponseTokenSlice(&.{ 7, 8, 9, 101, 20, 21, 106 }, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8 },
        reasoningResponseTokenSlice(&.{ 7, 8, 102, 103, 104, 101, 20, 106 }, true, false, 101, &header, 102, 106),
    );
    // A late explicit final header must not retroactively classify the prior
    // bare-close marker or tentative public text as private reasoning.
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8 },
        reasoningResponseTokenSlice(&.{ 7, 8, 101, 20, 102, 103, 104, 101, 21, 106 }, true, false, 101, &header, 102, 106),
    );
    // The same malformed transition remains bounded when reasoning begins
    // with an explicit private-channel header.
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8 },
        reasoningResponseTokenSlice(&.{ 102, 103, 105, 101, 7, 8, 101, 20, 102, 103, 104, 101, 21, 106 }, true, false, 101, &header, 102, 106),
    );
    // An explicitly emitted private header is protocol, not reasoning text.
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8 },
        reasoningResponseTokenSlice(&.{ 102, 103, 105, 101, 7, 8, 101, 20, 106 }, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{ 7, 8 },
        reasoningResponseTokenSlice(&.{ 7, 8 }, true, false, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        reasoningResponseTokenSlice(&.{ 20, 21 }, true, true, 101, &header, 102, 106),
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{},
        reasoningResponseTokenSlice(&.{ 7, 8 }, false, false, null, &.{}, null, null),
    );
}

test "streaming completion emits bare-channel public content once" {
    const Capture = struct {
        text: std.ArrayListUnmanaged(u8) = .empty,

        fn callback(ctx: *anyopaque, delta: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.text.appendSlice(std.testing.allocator, delta) catch return false;
            return true;
        }
    };

    var capture = Capture{};
    defer capture.text.deinit(std.testing.allocator);
    const emitted = try std.testing.allocator.dupe(u8, "");
    defer std.testing.allocator.free(emitted);
    const state = StreamingTextState{ .emitted_text = emitted };

    try std.testing.expect(emitCompletedProjectionDelta(&state, &capture, Capture.callback, "public answer"));
    try std.testing.expectEqualStrings("public answer", capture.text.items);

    const already_emitted = StreamingTextState{ .emitted_text = capture.text.items };
    try std.testing.expect(emitCompletedProjectionDelta(&already_emitted, &capture, Capture.callback, "public answer"));
    try std.testing.expectEqualStrings("public answer", capture.text.items);
}

test "streaming emitted text replacement preserves ownership on OOM" {
    const allocator = std.testing.allocator;
    const original = try allocator.dupe(u8, "already emitted");
    var state = StreamingTextState{ .emitted_text = original };
    defer allocator.free(state.emitted_text);

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        replaceStreamingEmittedText(failing.allocator(), &state, "replacement"),
    );
    try std.testing.expect(state.emitted_text.ptr == original.ptr);
    try std.testing.expectEqualStrings("already emitted", state.emitted_text);
}

test "raw token diagnostics retain private generated ids" {
    const raw = [_]i64{ 7, 8 };
    const public = finalResponseTokenSlice(&raw, true, false, 101, &.{ 102, 103, 104, 101 }, 102, 106);
    try std.testing.expectEqual(@as(usize, 0), public.len);
    try std.testing.expectEqualSlices(i64, &raw, generationResultTokenSlice(&raw, public, true));
    try std.testing.expectEqualSlices(i64, public, generationResultTokenSlice(&raw, public, false));
}

test "streaming final channel projection ignores intermediate boundaries and streams final text" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "<unk>": 0,
        \\      "reasoning": 7,
        \\      "answer": 8,
        \\      " follows": 9,
        \\      "<channel|>": 101,
        \\      "<turn|>": 106
        \\    },
        \\    "merges": []
        \\  },
        \\  "added_tokens": [
        \\    {"id": 101, "content": "<channel|>", "special": true},
        \\    {"id": 106, "content": "<turn|>", "special": true}
        \\  ]
        \\}
    ;

    var tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer tok.deinitSelf();
    const tokenizer = tok.tokenizer();
    try std.testing.expectEqual(@as(?i32, 101), try resolveTokenId(tokenizer, allocator, "<channel|>"));
    try std.testing.expectEqual(@as(?i32, 106), try resolveTokenId(tokenizer, allocator, "<turn|>"));

    const Capture = struct {
        allocator: std.mem.Allocator,
        text: std.ArrayListUnmanaged(u8) = .empty,

        fn callback(ctx: *anyopaque, delta: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.text.appendSlice(self.allocator, delta) catch return false;
            return true;
        }
    };

    var capture = Capture{ .allocator = allocator };
    defer capture.text.deinit(allocator);
    var state = StreamingTextState{
        .emitted_text = try allocator.dupe(u8, ""),
        .final_channel_required = true,
        .final_channel_end_token_id = 101,
        .final_channel_header_token_ids = &.{ 102, 103, 104, 101 },
        .channel_start_token_id = 102,
        .turn_end_token_id = 106,
    };
    defer allocator.free(state.emitted_text);

    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{7},
        &state,
        Capture.callback,
        &capture,
    ));
    try std.testing.expectEqualStrings("", capture.text.items);
    try std.testing.expect(!state.saw_final_channel);

    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 7, 101, 8 },
        &state,
        Capture.callback,
        &capture,
    ));
    try std.testing.expectEqualStrings("", capture.text.items);
    try std.testing.expect(!state.saw_final_channel);

    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 7, 101, 8, 102, 103 },
        &state,
        Capture.callback,
        &capture,
    ));
    try std.testing.expectEqualStrings("", capture.text.items);
    try std.testing.expect(!state.saw_final_channel);

    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 7, 101, 8, 102, 103, 104, 101, 8 },
        &state,
        Capture.callback,
        &capture,
    ));
    try std.testing.expectEqualStrings("answer", capture.text.items);
    try std.testing.expect(state.saw_final_channel);

    // The normal autoregressive decoder consumes the turn token as EOS without
    // appending it, while speculative paths may retain it. Neither path should
    // delay or duplicate the final streamed text.
    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 7, 101, 8, 102, 103, 104, 101, 8, 9, 106 },
        &state,
        Capture.callback,
        &capture,
    ));
    try std.testing.expectEqualStrings("answer follows", capture.text.items);

    // A malformed continuation after the final channel must fail closed before
    // any subsequent channel name or private content reaches the callback.
    var trailing_capture = Capture{ .allocator = allocator };
    defer trailing_capture.text.deinit(allocator);
    var trailing_state = StreamingTextState{
        .emitted_text = try allocator.dupe(u8, ""),
        .final_channel_required = true,
        .final_channel_end_token_id = 101,
        .final_channel_header_token_ids = &.{ 102, 103, 104, 101 },
        .channel_start_token_id = 102,
        .turn_end_token_id = 106,
    };
    defer allocator.free(trailing_state.emitted_text);
    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 7, 101, 8, 102, 103, 104, 101, 8, 9, 101, 7 },
        &trailing_state,
        Capture.callback,
        &trailing_capture,
    ));
    try std.testing.expectEqualStrings("answer follows", trailing_capture.text.items);

    // When the prompt already opened the public final channel, stream content
    // immediately but retain the same fail-closed boundary enforcement.
    var preopened_capture = Capture{ .allocator = allocator };
    defer preopened_capture.text.deinit(allocator);
    var preopened_state = StreamingTextState{
        .emitted_text = try allocator.dupe(u8, ""),
        .final_channel_required = true,
        .final_channel_preopened = true,
        .final_channel_end_token_id = 101,
        .final_channel_header_token_ids = &.{ 102, 103, 104, 101 },
        .channel_start_token_id = 102,
        .turn_end_token_id = 106,
        .final_channel_content_start = 0,
        .saw_final_channel = true,
    };
    defer allocator.free(preopened_state.emitted_text);
    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{8},
        &preopened_state,
        Capture.callback,
        &preopened_capture,
    ));
    try std.testing.expectEqualStrings("answer", preopened_capture.text.items);
    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 8, 9, 101, 7 },
        &preopened_state,
        Capture.callback,
        &preopened_capture,
    ));
    try std.testing.expectEqualStrings("answer follows", preopened_capture.text.items);

    // Tokenizer metadata is part of the security boundary. A channel-aware
    // model with an incomplete protocol vocabulary must buffer indefinitely
    // rather than falling back to raw reasoning text.
    var incomplete_capture = Capture{ .allocator = allocator };
    defer incomplete_capture.text.deinit(allocator);
    var incomplete_state = StreamingTextState{
        .emitted_text = try allocator.dupe(u8, ""),
        .final_channel_required = true,
        .final_channel_end_token_id = 101,
        .final_channel_header_token_ids = &.{ 102, 103, 104, 101 },
        .channel_start_token_id = 102,
        .turn_end_token_id = null,
    };
    defer allocator.free(incomplete_state.emitted_text);
    try std.testing.expect(try emitDecodedDeltaForTokenizer(
        tokenizer,
        allocator,
        &.{ 7, 101, 8 },
        &incomplete_state,
        Capture.callback,
        &incomplete_capture,
    ));
    try std.testing.expectEqualStrings("", incomplete_capture.text.items);
}

test "shouldAddBosToken keeps bos when prompt lacks literal prefix" {
    try std.testing.expect(shouldAddBosToken("Hello", true, "<bos>"));
    try std.testing.expect(!shouldAddBosToken("Hello", false, "<bos>"));
}

test "markStepFailed marks every claimed work failed and ready" {
    const allocator = std.testing.allocator;
    var dummy_state = NativeDecodeState.initContiguous(allocator);
    defer dummy_state.kv_block_ids.deinit(allocator);
    defer dummy_state.moe_runtime.deinit();

    var dec_a = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_id = 7,
        .seq_len = 4,
    };
    var pre_a = NativeGenerationPipeline.PendingPrefillBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_ids = &.{},
        .seq_len = 4,
        .query_seq_len = 2,
        .wants_last_logits = true,
    };

    const items = [_]runtime.scheduler.native_generate.StepItem{
        .{ .work_ptr = @ptrCast(&dec_a), .phase = .decode, .query_sequence_len = 1, .total_sequence_len = 4, .kv_sequence_len = 4, .kv_position_offset = 0 },
        .{ .work_ptr = @ptrCast(&pre_a), .phase = .prefill, .query_sequence_len = 2, .total_sequence_len = 4, .kv_sequence_len = 2, .kv_position_offset = 0 },
    };

    NativeGenerationPipeline.markStepFailed(&items, error.OutOfMemory);

    try std.testing.expect(dec_a.ready.load(.acquire));
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), dec_a.failure);
    try std.testing.expect(dec_a.logits == null);

    try std.testing.expect(pre_a.ready.load(.acquire));
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), pre_a.failure);
    try std.testing.expect(pre_a.logits == null);
}

test "completeClaimedStepBeforePublish removes scheduler work before ready publication" {
    const allocator = std.testing.allocator;
    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease);
    var peer_lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(peer_lease);

    var dummy_state = NativeDecodeState.initContiguous(allocator);
    defer dummy_state.deinit();
    var work = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_id = 7,
        .seq_len = 4,
    };

    coordinator.beginDecode(&lease, 4);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work), 5, 5, 0, .{});
    var peer_work: u8 = 0;
    coordinator.beginDecode(&peer_lease, 4);
    try coordinator.enqueueDecodeWork(peer_lease, @ptrCast(&peer_work), 5, 5, 0, .{});

    var claimed = std.ArrayListUnmanaged(runtime.scheduler.native_generate.StepItem).empty;
    defer claimed.deinit(allocator);
    try std.testing.expect(try coordinator.claimStep(
        allocator,
        &lease,
        @ptrCast(&work),
        .decode,
        .{ .max_items = 1, .max_query_tokens = 1 },
        &claimed,
    ));
    try std.testing.expectEqual(@as(usize, 1), claimed.items.len);

    var pending_was_removed = false;
    var ownership_was_released = false;
    var ready_was_clear = false;
    const ProbePublisher = struct {
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        work: *NativeGenerationPipeline.PendingDecodeBatchWork,
        peer_lease: *runtime.scheduler.native_generate.Lease,
        pending_was_removed: *bool,
        ownership_was_released: *bool,
        ready_was_clear: *bool,

        fn publish(self: @This(), _: []const runtime.scheduler.native_generate.StepItem) void {
            self.pending_was_removed.* = self.scheduler.pendingKvBlocksEstimate(@ptrCast(self.work), .decode) == null;
            self.ownership_was_released.* = self.scheduler.tryAcquireTurn(self.peer_lease, .decode);
            if (self.ownership_was_released.*) self.scheduler.finishTurn(self.peer_lease, .decode);
            self.ready_was_clear.* = !self.work.ready.load(.acquire);
            self.work.ready.store(true, .release);
        }
    };

    NativeGenerationPipeline.completeClaimedStepBeforePublish(
        &coordinator,
        &lease,
        claimed.items,
        ProbePublisher{
            .scheduler = &coordinator,
            .work = &work,
            .peer_lease = &peer_lease,
            .pending_was_removed = &pending_was_removed,
            .ownership_was_released = &ownership_was_released,
            .ready_was_clear = &ready_was_clear,
        },
    );

    try std.testing.expect(pending_was_removed);
    try std.testing.expect(ownership_was_released);
    try std.testing.expect(ready_was_clear);
    try std.testing.expect(work.ready.load(.acquire));
}

test "dispatchStepLogits dupes per-item rows and respects wants_last_logits" {
    const allocator = std.testing.allocator;
    var dummy_state = NativeDecodeState.initContiguous(allocator);
    defer dummy_state.kv_block_ids.deinit(allocator);
    defer dummy_state.moe_runtime.deinit();

    var dec_w = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_id = 0,
        .seq_len = 4,
    };
    var pre_with_logits = NativeGenerationPipeline.PendingPrefillBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_ids = &.{},
        .seq_len = 4,
        .query_seq_len = 2,
        .wants_last_logits = true,
    };
    var pre_no_logits = NativeGenerationPipeline.PendingPrefillBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_ids = &.{},
        .seq_len = 4,
        .query_seq_len = 2,
        .wants_last_logits = false,
    };
    defer if (dec_w.logits) |buf| allocator.free(buf);
    defer if (pre_with_logits.logits) |buf| allocator.free(buf);

    // Vocab size 2, max_query_seq_len 2 → each item contributes 2 rows of 2
    // floats. The dispatch reads the (query_sequence_len - 1)th row.
    const vocab_size: usize = 2;
    const max_query: usize = 2;
    const logits = [_]f32{
        // Item 0 (decode, q=1) → row 0 starts at 0; (q-1) = 0
        10, 11, 0,  0,
        // Item 1 (prefill q=2, wants logits) → (q-1)=1 → row at idx*max_query + 1 = 3
        0,  0,  22, 23,
        // Item 2 (prefill q=2, no logits) → row 5
        0,  0,  33, 34,
    };

    const items = [_]runtime.scheduler.native_generate.StepItem{
        .{ .work_ptr = @ptrCast(&dec_w), .phase = .decode, .query_sequence_len = 1, .total_sequence_len = 4, .kv_sequence_len = 4, .kv_position_offset = 0 },
        .{ .work_ptr = @ptrCast(&pre_with_logits), .phase = .prefill, .query_sequence_len = 2, .total_sequence_len = 4, .kv_sequence_len = 2, .kv_position_offset = 0 },
        .{ .work_ptr = @ptrCast(&pre_no_logits), .phase = .prefill, .query_sequence_len = 2, .total_sequence_len = 4, .kv_sequence_len = 2, .kv_position_offset = 0 },
    };

    NativeGenerationPipeline.dispatchStepLogits(vocab_size, &items, &logits, max_query);

    try std.testing.expect(dec_w.ready.load(.acquire));
    try std.testing.expectEqualSlices(f32, &.{ 10, 11 }, dec_w.logits.?);
    try std.testing.expect(pre_with_logits.ready.load(.acquire));
    try std.testing.expectEqualSlices(f32, &.{ 22, 23 }, pre_with_logits.logits.?);
    try std.testing.expect(pre_no_logits.ready.load(.acquire));
    try std.testing.expect(pre_no_logits.logits == null);
}

test "dispatchStepLogits records dupe failure per-work without aborting peers" {
    const allocator = std.testing.allocator;
    var dummy_state = NativeDecodeState.initContiguous(allocator);
    defer dummy_state.kv_block_ids.deinit(allocator);
    defer dummy_state.moe_runtime.deinit();

    // Configure a failing allocator that refuses its very first allocation,
    // so the dupe for the middle work fails while peers still succeed.
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    const failing_alloc = failing.allocator();

    var dec_a = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_id = 0,
        .seq_len = 1,
    };
    var dec_b = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = failing_alloc,
        .decode_state = &dummy_state,
        .token_id = 0,
        .seq_len = 1,
    };
    var dec_c = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_id = 0,
        .seq_len = 1,
    };
    defer if (dec_a.logits) |buf| allocator.free(buf);
    defer if (dec_c.logits) |buf| allocator.free(buf);

    const vocab_size: usize = 2;
    const logits = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const items = [_]runtime.scheduler.native_generate.StepItem{
        .{ .work_ptr = @ptrCast(&dec_a), .phase = .decode, .query_sequence_len = 1, .total_sequence_len = 1, .kv_sequence_len = 1, .kv_position_offset = 0 },
        .{ .work_ptr = @ptrCast(&dec_b), .phase = .decode, .query_sequence_len = 1, .total_sequence_len = 1, .kv_sequence_len = 1, .kv_position_offset = 0 },
        .{ .work_ptr = @ptrCast(&dec_c), .phase = .decode, .query_sequence_len = 1, .total_sequence_len = 1, .kv_sequence_len = 1, .kv_position_offset = 0 },
    };

    NativeGenerationPipeline.dispatchStepLogits(vocab_size, &items, &logits, 1);

    try std.testing.expect(dec_a.ready.load(.acquire));
    try std.testing.expect(dec_a.failure == null);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, dec_a.logits.?);

    try std.testing.expect(dec_b.ready.load(.acquire));
    try std.testing.expect(dec_b.failure != null);
    try std.testing.expect(dec_b.logits == null);

    // The failing dupe must not stop dec_c's dispatch.
    try std.testing.expect(dec_c.ready.load(.acquire));
    try std.testing.expect(dec_c.failure == null);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6 }, dec_c.logits.?);
}

test "rollbackPrefillReservations is a no-op when reserved_count is 0" {
    const allocator = std.testing.allocator;
    var dummy_state = NativeDecodeState.initContiguous(allocator);
    defer dummy_state.kv_block_ids.deinit(allocator);
    defer dummy_state.moe_runtime.deinit();

    // Even though the prefill pending work has a non-zero reserved entry,
    // reserved_count = 0 means no rollback should be attempted. We can
    // exercise this without a real KvManager because the rollback function
    // returns early before touching decode_state.
    var pre = NativeGenerationPipeline.PendingPrefillBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_ids = &.{},
        .seq_len = 4,
        .query_seq_len = 2,
        .wants_last_logits = false,
    };
    const items = [_]runtime.scheduler.native_generate.StepItem{
        .{ .work_ptr = @ptrCast(&pre), .phase = .prefill, .query_sequence_len = 2, .total_sequence_len = 4, .kv_sequence_len = 2, .kv_position_offset = 0 },
    };
    const reserved = [_]usize{42};

    NativeGenerationPipeline.rollbackPrefillReservations(&items, &reserved, 0);
    // No assertion needed: success means we did not attempt a rollback that
    // would have touched the unconfigured dummy_state. If the function had
    // attempted to call truncateGeneratedTokens with reserved=42, the
    // unconfigured state would have crashed.
}

test "runStepLoop drives stub driver to completion and reports per-iteration budget" {
    const allocator = std.testing.allocator;
    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease);

    var work_byte: u8 = 1;
    coordinator.beginDecode(&lease, 4);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_byte), 5, 5, 0, .{});

    const StubDriver = struct {
        coordinator: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        ready: bool = false,
        budget_calls: usize = 0,
        execute_calls: usize = 0,
        last_claim_size: usize = 0,
        prestep_calls: usize = 0,

        fn isReady(self: *const @This()) bool {
            return self.ready;
        }

        fn stepBudget(self: *@This()) runtime.scheduler.native_generate.StepBudget {
            self.budget_calls += 1;
            return self.coordinator.defaultStepBudget();
        }

        fn preStep(self: *@This()) void {
            self.prestep_calls += 1;
        }

        fn execute(
            self: *@This(),
            scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
            lease_ptr: *runtime.scheduler.native_generate.Lease,
            claimed: []const runtime.scheduler.native_generate.StepItem,
        ) void {
            self.execute_calls += 1;
            self.last_claim_size = claimed.len;
            scheduler.completeStep(lease_ptr, claimed);
            self.ready = true;
        }
    };

    var driver = StubDriver{ .coordinator = &coordinator };
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();

    try runStepLoop(allocator, &coordinator, &lease, @ptrCast(&work_byte), .decode, io_impl.io(), null, &driver);

    try std.testing.expect(driver.ready);
    try std.testing.expectEqual(@as(usize, 1), driver.execute_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.last_claim_size);
    try std.testing.expectEqual(@as(usize, 1), driver.prestep_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.budget_calls);
    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_decode.items.len);
}

test "runStepLoop cancellation removes idle stack work without executing it" {
    const allocator = std.testing.allocator;
    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease);

    var work_byte: u8 = 1;
    coordinator.beginDecode(&lease, 4);
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_byte), 5, 5, 0, .{});

    const StubDriver = struct {
        fn isReady(_: *const @This()) bool {
            return false;
        }

        fn stepBudget(_: *@This()) runtime.scheduler.native_generate.StepBudget {
            unreachable;
        }

        fn execute(
            _: *@This(),
            _: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
            _: *runtime.scheduler.native_generate.Lease,
            _: []const runtime.scheduler.native_generate.StepItem,
        ) void {
            unreachable;
        }
    };

    var driver = StubDriver{};
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();

    try std.testing.expectError(
        error.Canceled,
        runStepLoop(
            allocator,
            &coordinator,
            &lease,
            @ptrCast(&work_byte),
            .decode,
            io_impl.io(),
            error.Canceled,
            &driver,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_decode.items.len);
    try std.testing.expectEqual(@as(?runtime.scheduler.native_generate.RequestId, null), coordinator.in_turn);
}

test "runStepLoop yields when no step is currently claimable" {
    const allocator = std.testing.allocator;
    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease);

    // Build a leader work pointer that is NOT enqueued — claimStep will fail.
    // After two yields, the driver reports ready to terminate the loop.
    var ghost_work: u8 = 0;

    const StubDriver = struct {
        ready_after: usize,
        cycles: usize = 0,

        fn isReady(self: *@This()) bool {
            self.cycles += 1;
            return self.cycles > self.ready_after;
        }

        fn stepBudget(_: *@This()) runtime.scheduler.native_generate.StepBudget {
            return .{ .max_items = 8, .max_query_tokens = 64 };
        }

        fn execute(
            _: *@This(),
            _: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
            _: *runtime.scheduler.native_generate.Lease,
            _: []const runtime.scheduler.native_generate.StepItem,
        ) void {
            // Should never run because the leader is not enqueued.
            unreachable;
        }
    };

    var driver = StubDriver{ .ready_after = 2 };
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();

    try runStepLoop(allocator, &coordinator, &lease, @ptrCast(&ghost_work), .decode, io_impl.io(), null, &driver);

    // The loop should have spun until isReady() returned true after ~3 calls
    // (the count begins at 1 inside isReady; ready_after=2 means: cycles 1,2
    // return false, cycle 3 returns true).
    try std.testing.expect(driver.cycles >= 3);
}

test "runStepLoop drains a multi-item step from a stub driver" {
    const allocator = std.testing.allocator;
    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    var lease_a = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease_a);
    var lease_b = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease_b);

    var work_a: u8 = 1;
    var work_b: u8 = 2;

    coordinator.beginDecode(&lease_a, 4);
    coordinator.beginDecode(&lease_b, 4);
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&work_a), 5, 5, 0, .{});
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&work_b), 5, 5, 0, .{});

    const StubDriver = struct {
        coordinator: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        leader_ptr: *anyopaque,
        ready: bool = false,
        observed_size: usize = 0,

        fn isReady(self: *const @This()) bool {
            return self.ready;
        }

        fn stepBudget(self: *const @This()) runtime.scheduler.native_generate.StepBudget {
            return self.coordinator.defaultStepBudget();
        }

        fn execute(
            self: *@This(),
            scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
            lease_ptr: *runtime.scheduler.native_generate.Lease,
            claimed: []const runtime.scheduler.native_generate.StepItem,
        ) void {
            self.observed_size = claimed.len;
            scheduler.completeStep(lease_ptr, claimed);
            self.ready = true;
        }
    };

    var driver = StubDriver{ .coordinator = &coordinator, .leader_ptr = @ptrCast(&work_a) };
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();

    try runStepLoop(allocator, &coordinator, &lease_a, @ptrCast(&work_a), .decode, io_impl.io(), null, &driver);

    try std.testing.expect(driver.ready);
    // Both pending decodes should have been packed into the single step.
    try std.testing.expectEqual(@as(usize, 2), driver.observed_size);
    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_decode.items.len);
}

test "stepBudgetFromState reflects KV pool headroom and applies safety margin" {
    const allocator = std.testing.allocator;

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();
    const pool_id = try kv_manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });

    var decode_state = NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, null);
    defer {
        decode_state.kv_block_ids.deinit(allocator);
        decode_state.moe_runtime.deinit();
    }
    try decode_state.ensureAttached();

    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    // No cap configured → no max_kv_blocks plumbed, scheduler treats unbounded.
    var budget = stepBudgetFromState(&coordinator, &decode_state);
    try std.testing.expect(budget.max_kv_blocks == null);

    kv_manager.setPoolTargetMaxBlocks(pool_id, 8);
    budget = stepBudgetFromState(&coordinator, &decode_state);
    // Pool empty: 8 available, minus 1 safety margin = 7.
    try std.testing.expectEqual(@as(?usize, 7), budget.max_kv_blocks);

    // Consume some pool blocks; the budget should shrink correspondingly.
    if (decode_state.sequence_id) |sid| {
        try kv_manager.appendTokens(sid, 12); // 12 / 4 page_size = 3 live blocks
    }
    budget = stepBudgetFromState(&coordinator, &decode_state);
    // 8 cap - 3 live = 5 grow_room + 0 free = 5; minus safety = 4.
    try std.testing.expectEqual(@as(?usize, 4), budget.max_kv_blocks);

    // Saturate the cap; budget should clamp to zero.
    kv_manager.setPoolTargetMaxBlocks(pool_id, 1);
    budget = stepBudgetFromState(&coordinator, &decode_state);
    // 1 cap - 3 live = 0 (clamped) + 0 free = 0; safety subtraction also clamps to 0.
    try std.testing.expectEqual(@as(?usize, 0), budget.max_kv_blocks);
}

test "notePendingKvBlocksFromState plumbs estimate to scheduler" {
    const allocator = std.testing.allocator;

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();
    const pool_id = try kv_manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 4,
    });

    var decode_state = NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, null);
    defer {
        decode_state.kv_block_ids.deinit(allocator);
        decode_state.moe_runtime.deinit();
    }
    try decode_state.ensureAttached();
    if (decode_state.sequence_id) |sid| {
        // Sequence holds 6 tokens — tail has 2 tokens of slack in a 4-page.
        try kv_manager.appendTokens(sid, 6);
    }

    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    const lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease);

    var work_a: u8 = 0;
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_a), 7, 7, 0, .{});

    notePendingKvBlocksFromState(&coordinator, &decode_state, @ptrCast(&work_a), .decode, 1);

    // 1 token fits in the existing tail slack → 0 new blocks.
    try std.testing.expectEqual(@as(?usize, 0), coordinator.pendingKvBlocksEstimate(@ptrCast(&work_a), .decode));

    // A larger overflow: 5 tokens vs 2-token slack → 1 new block.
    var work_b: u8 = 1;
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_b), 11, 11, 0, .{});
    notePendingKvBlocksFromState(&coordinator, &decode_state, @ptrCast(&work_b), .decode, 5);
    try std.testing.expectEqual(@as(?usize, 1), coordinator.pendingKvBlocksEstimate(@ptrCast(&work_b), .decode));
}

test "notePendingExclusiveStepFromState marks DeepSeek V4 compressed cache work" {
    const allocator = std.testing.allocator;
    var decode_state = NativeDecodeState.initContiguous(allocator);
    defer decode_state.deinit();

    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(allocator);
    defer coordinator.deinit();

    const lease = try coordinator.acquire(.{
        .requested_units = 1,
        .prompt_bytes = 64,
        .max_tokens = 8,
    });
    defer coordinator.release(lease);

    var normal_work: u8 = 0;
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&normal_work), 7, 7, 0, .{});
    notePendingExclusiveStepFromState(&coordinator, &decode_state, @ptrCast(&normal_work), .decode);
    try std.testing.expectEqual(@as(?bool, false), coordinator.pendingRequiresExclusiveStep(@ptrCast(&normal_work), .decode));

    decode_state.deepseek_v4_compressed_cache = try gpt_arch.DeepSeekV4CompressedCache.init(allocator, 1);

    var compressed_work: u8 = 1;
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&compressed_work), 8, 8, 0, .{});
    notePendingExclusiveStepFromState(&coordinator, &decode_state, @ptrCast(&compressed_work), .decode);
    try std.testing.expectEqual(@as(?bool, true), coordinator.pendingRequiresExclusiveStep(@ptrCast(&compressed_work), .decode));
}

test "rollbackPrefillReservations skips decode-phase entries within bounds" {
    const allocator = std.testing.allocator;
    var dummy_state = NativeDecodeState.initContiguous(allocator);
    defer dummy_state.kv_block_ids.deinit(allocator);
    defer dummy_state.moe_runtime.deinit();

    var dec = NativeGenerationPipeline.PendingDecodeBatchWork{
        .allocator = allocator,
        .decode_state = &dummy_state,
        .token_id = 0,
        .seq_len = 1,
    };
    const items = [_]runtime.scheduler.native_generate.StepItem{
        .{ .work_ptr = @ptrCast(&dec), .phase = .decode, .query_sequence_len = 1, .total_sequence_len = 1, .kv_sequence_len = 1, .kv_position_offset = 0 },
    };
    const reserved = [_]usize{99};

    // reserved_count = 1 means "the first item was processed"; since that
    // item is a decode (no reservation made), the rollback must not call
    // truncateGeneratedTokens. The fact that this returns cleanly without
    // touching the unconfigured state proves the skip works.
    NativeGenerationPipeline.rollbackPrefillReservations(&items, &reserved, 1);
}

test "scheduled prefill refreshes KV geometry after chunk reservation" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 16,
        .num_kv_heads = 1,
        .head_dim = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    var first_chunk = MixedBatchDecodeItem{
        .state = &state,
        .total_sequence_len = 128,
        .query_sequence_len = 128,
        .kv_sequence_len = 0,
        .kv_position_offset = 99,
        .attention_mode = .paged_prefill,
    };
    const first_reserved = try NativeGenerationPipeline.reserveAndRefreshScheduledPrefill(&first_chunk, 128);
    try std.testing.expectEqual(@as(usize, 128), first_reserved);
    try std.testing.expectEqual(@as(usize, 128), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 128), first_chunk.kv_sequence_len);
    try std.testing.expectEqual(@as(usize, 0), first_chunk.kv_position_offset);

    var second_chunk = MixedBatchDecodeItem{
        .state = &state,
        .total_sequence_len = 256,
        .query_sequence_len = 128,
        // This is the geometry captured while the first chunk was current.
        .kv_sequence_len = 128,
        .kv_position_offset = 99,
        .attention_mode = .paged_prefill,
    };

    const second_reserved = try NativeGenerationPipeline.reserveAndRefreshScheduledPrefill(&second_chunk, 256);

    try std.testing.expectEqual(@as(usize, 128), second_reserved);
    try std.testing.expectEqual(@as(usize, 256), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 256), second_chunk.kv_sequence_len);
    try std.testing.expectEqual(@as(usize, 0), second_chunk.kv_position_offset);
    try std.testing.expectEqual(@as(usize, 128), second_chunk.kv_sequence_len - second_chunk.query_sequence_len);

    var work = NativeGenerationPipeline.PendingPrefillBatchWork{
        .allocator = allocator,
        .decode_state = &state,
        .token_ids = &.{},
        .seq_len = 256,
        .query_seq_len = 128,
        .wants_last_logits = false,
    };
    const claimed = [_]runtime.scheduler.native_generate.StepItem{
        .{ .work_ptr = @ptrCast(&work), .phase = .prefill, .query_sequence_len = 128, .total_sequence_len = 256, .kv_sequence_len = 128, .kv_position_offset = 0 },
    };
    const reserved = [_]usize{second_reserved};

    NativeGenerationPipeline.rollbackPrefillReservations(&claimed, &reserved, 1);

    try std.testing.expectEqual(@as(usize, 128), state.total_tokens);
    const rolled_back = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 128), rolled_back.token_count);
    try std.testing.expectEqual(@as(usize, 8), rolled_back.logical_block_count);
    try std.testing.expectEqual(@as(usize, 8), rolled_back.logical_blocks.?.len);
    try std.testing.expectEqual(@as(u16, 16), rolled_back.tail_tokens);
    try std.testing.expectEqual(@as(usize, 0), rolled_back.position_offset);
}

test "native decode state paged kv grows in pages" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    try state.notePrefill(6);
    var view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 2), view.logical_block_count);
    try std.testing.expectEqual(@as(u16, 2), view.tail_tokens);

    try state.appendGeneratedToken();
    view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 2), view.logical_block_count);
    try std.testing.expectEqual(@as(u16, 3), view.tail_tokens);
}

test "native decode state batched append refreshes paged block table" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    try state.notePrefill(6);
    try state.appendGeneratedTokens(3);
    var view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 3), view.logical_block_count);
    try std.testing.expectEqual(@as(usize, 3), view.logical_blocks.?.len);
    try std.testing.expectEqual(@as(usize, 9), view.token_count);

    try state.truncateTokens(5);
    view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 1), view.logical_block_count);
    try std.testing.expectEqual(@as(usize, 1), view.logical_blocks.?.len);
    try std.testing.expectEqual(@as(usize, 4), view.token_count);
}

test "native decode state compacted batched append advances the full accepted count" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    // Model a six-token prompt compacted to two retained KV entries without
    // invoking the numerical compactor; this test targets view accounting.
    try state.notePrefill(2);
    state.total_tokens = 6;
    state.kv_compacted = true;
    (try manager.sequenceMut(state.sequence_id.?)).compacted = true;
    state.setPagedKvView(2, 4);

    try state.appendGeneratedTokens(3);
    const view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 5), view.token_count);
    try std.testing.expectEqual(@as(usize, 4), view.position_offset);
    try std.testing.expectEqual(@as(usize, 9), state.total_tokens);
}

test "native decode state compaction publishes the replacement block table" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    try state.notePrefill(6);
    const old_sequence_id = state.sequence_id.?;
    const old_blocks = state.kvView().?.logical_blocks.?;
    var old_block_ids: [2]runtime.kv.block.KvBlockId = undefined;
    @memcpy(&old_block_ids, old_blocks);

    const layer_kv = [_]f32{ 1, 0, 0, 1, 1, 1, 2, 1, 1, 2, 2, 2 };
    try manager.writeFullLayerKv(old_sequence_id, 0, 6, &layer_kv, &layer_kv);

    try std.testing.expectEqual(@as(usize, 6), try state.compactKvCache(.{ .target_ratio = 1.0 }));
    const view = state.kvView().?;
    try std.testing.expect(state.sequence_id.? != old_sequence_id);
    try std.testing.expectEqual(@as(usize, 6), view.token_count);
    try std.testing.expectEqual(@as(usize, 0), view.position_offset);
    try std.testing.expectEqual(@as(usize, 2), view.logical_blocks.?.len);
    try std.testing.expect(!std.mem.eql(runtime.kv.block.KvBlockId, &old_block_ids, view.logical_blocks.?));
}

test "native decode state sliding-window view stays block-aligned" {
    // Regression: the paged kernels index the front-trimmed block table with
    // view-relative positions, so the decode view offset must always equal
    // the block-aligned dropped-token count. A token-granular offset
    // (total - window) misaligns every KV read and write by offset % page.
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const page_size: usize = 4;
    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = page_size,
        .num_kv_heads = 8,
        .head_dim = 128,
        .sliding_window_size = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    try state.notePrefill(10);
    var step: usize = 0;
    while (step < 20) : (step += 1) {
        try state.appendGeneratedToken();
        const view = state.kvView().?;
        // Offset must be page-aligned so view-relative kernel indexing maps
        // exactly onto the compacted block table.
        try std.testing.expectEqual(@as(usize, 0), view.position_offset % page_size);
        // The view must cover exactly the retained suffix of the sequence.
        try std.testing.expectEqual(state.total_tokens, view.position_offset + view.token_count);
        try std.testing.expectEqual(manager.tokenCount(state.sequence_id.?).?, view.token_count);
        // Trimming keeps at least the window and less than window + one page.
        try std.testing.expect(view.token_count >= 8);
        try std.testing.expect(view.token_count < 8 + page_size);
    }
}

test "native decode state disables SWA ring when the KV pool trims history" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 8,
        .sliding_window_size = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    state.allow_swa_ring = true;
    state.configureKvMaxInflightTokens(4, true);
    try std.testing.expect(!state.allow_swa_ring);
    try std.testing.expectEqual(@as(usize, 0), state.kv_max_inflight_tokens);
}

test "native decode state clears an existing KV view ring policy" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    state.allow_swa_ring = true;
    state.configureKvMaxInflightTokens(4, true);
    try state.notePrefill(4);
    try std.testing.expect(state.kvView().?.allow_swa_ring);

    state.configureKvMaxInflightTokens(4, true);
    try std.testing.expect(!state.allow_swa_ring);
    try std.testing.expectEqual(@as(usize, 0), state.kv_max_inflight_tokens);
    try std.testing.expect(!state.kvView().?.allow_swa_ring);
    try std.testing.expectEqual(@as(usize, 0), state.kvView().?.max_inflight_tokens);
}

test "native decode state chunked prefill appends incrementally" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    try state.appendPrefillChunk(3);
    try std.testing.expectEqual(@as(usize, 3), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 3), manager.tokenCount(state.sequence_id.?).?);

    try state.appendPrefillChunk(2);
    try std.testing.expectEqual(@as(usize, 5), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 5), manager.tokenCount(state.sequence_id.?).?);
}

test "direct paged prefill OOM retry rolls back its exact reservation" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();
    var decode_runtime = BorrowedDecodeStateRuntime.init(&state);

    try decode_runtime.appendPrefillChunk(3);
    try decode_runtime.appendPrefillChunk(6);
    var current_chunk_size: usize = 8;

    try std.testing.expect(try retryDirectPrefillAfterMemoryBudgetExceeded(
        &decode_runtime,
        6,
        8,
        &current_chunk_size,
        true,
        error.MemoryBudgetExceeded,
    ));
    try std.testing.expectEqual(@as(usize, 4), current_chunk_size);
    try std.testing.expectEqual(@as(usize, 3), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 3), manager.tokenCount(state.sequence_id.?).?);
    try std.testing.expectEqual(@as(usize, 1), state.kvView().?.logical_block_count);
}

test "native decode state maps to gpt decode context" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 8,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();
    try state.notePrefill(10);

    const ctx = state.gptDecodeContext(10, 10);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_prefill, ctx.attention_mode);
    try std.testing.expect(ctx.kv_cache != null);
    try std.testing.expectEqual(@as(usize, 10), ctx.total_sequence_len);

    const decode_ctx = state.gptDecodeContext(10, 1);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_decode, decode_ctx.attention_mode);
    try std.testing.expectEqual(@as(usize, 1), decode_ctx.query_sequence_len);
}

test "native decode state attaches DeepSeek V4 compressed cache without disabling paged kv" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 8,
        .num_kv_heads = 1,
        .head_dim = 512,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    const config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_compressed_sparse_attention_layers = 1,
    };
    state.configureForGptConfig(config);
    try state.ensureDeepSeekV4CompressedCache(config);
    try std.testing.expect(!state.requiresFullRecompute());
    try std.testing.expect(state.isPaged());

    try state.notePrefill(10);
    try std.testing.expectEqual(@as(usize, 10), state.total_tokens);
    try std.testing.expect(state.kvView() != null);

    const decode_ctx = state.gptDecodeContext(10, 1);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_decode, decode_ctx.attention_mode);
    try std.testing.expectEqual(@as(usize, 1), decode_ctx.query_sequence_len);
    try std.testing.expectEqual(@as(usize, 10), decode_ctx.kv_sequence_len);
    try std.testing.expect(decode_ctx.kv_cache != null);
    try std.testing.expect(decode_ctx.deepseek_v4_compressed_cache != null);
}

test "native decode state truncation invalidates DeepSeek V4 compressed cache" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 8,
        .num_kv_heads = 1,
        .head_dim = 512,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    const config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_heavily_compressed_attention_layers = 1,
    };
    state.configureForGptConfig(config);
    try state.ensureDeepSeekV4CompressedCache(config);
    try state.notePrefill(10);
    try state.truncateTokens(2);

    try std.testing.expect(state.requiresFullRecompute());
    const decode_ctx = state.gptDecodeContext(8, 1);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.full_recompute, decode_ctx.attention_mode);
    try std.testing.expect(decode_ctx.deepseek_v4_compressed_cache != null);
}

test "native decode state derives DeepSeek V4 compressed cache requirement from attention schedule" {
    var scheduled = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 2,
    };
    scheduled.deepseek_v4_attention_schedule_len = 2;
    scheduled.deepseek_v4_attention_schedule[0] = .sliding_attention;
    scheduled.deepseek_v4_attention_schedule[1] = .heavily_compressed_attention;
    try std.testing.expect(NativeDecodeState.requiresDeepSeekV4CompressedCache(scheduled));

    var schedule_overrides_counters = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_compressed_sparse_attention_layers = 1,
    };
    schedule_overrides_counters.deepseek_v4_attention_schedule_len = 1;
    schedule_overrides_counters.deepseek_v4_attention_schedule[0] = .sliding_attention;
    try std.testing.expect(!NativeDecodeState.requiresDeepSeekV4CompressedCache(schedule_overrides_counters));

    const counter_fallback = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_compressed_sparse_attention_layers = 1,
    };
    try std.testing.expect(NativeDecodeState.requiresDeepSeekV4CompressedCache(counter_fallback));
}

test "native decode state clears DeepSeek V4 compressed cache when reconfigured" {
    const allocator = std.testing.allocator;
    var state = NativeDecodeState.initContiguous(allocator);
    defer state.deinit();

    const deepseek_config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_heavily_compressed_attention_layers = 1,
    };
    try state.ensureDeepSeekV4CompressedCache(deepseek_config);
    try std.testing.expect(state.deepseek_v4_compressed_cache != null);

    state.configureForGptConfig(.{ .family = .llama });
    try std.testing.expect(state.deepseek_v4_compressed_cache == null);
    try std.testing.expect(!state.requiresFullRecompute());
}

test "native decode state rejects KV compaction for DeepSeek V4 compressed cache" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 8,
        .num_kv_heads = 1,
        .head_dim = 512,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    const config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_heavily_compressed_attention_layers = 1,
    };
    try state.ensureDeepSeekV4CompressedCache(config);
    try state.notePrefill(10);

    try std.testing.expectError(
        error.DeepSeekV4CompressedKvCompactionNotSupported,
        state.compactKvCache(.{ .target_ratio = 0.5 }),
    );
}

test "native decode state rejects KV compaction when device storage is attached" {
    const allocator = std.testing.allocator;
    const pool_config = runtime.kv.pool.KvPoolConfig{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 8,
    };

    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(pool_config);
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, pool_config);
    defer storage.deinit();

    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    state.kv_storage = &storage;
    defer state.deinit();
    try state.notePrefill(4);

    try std.testing.expectError(
        error.KvStorageCompactionNotSupported,
        state.compactKvCache(.{ .target_ratio = 0.5 }),
    );
}

test "native generation pipeline rejects graph modes for DeepSeek V4 compressed cache" {
    const allocator = std.testing.allocator;
    const compressed_config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
        .deepseek_v4_heavily_compressed_attention_layers = 1,
    };
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = compressed_config,
        .tokenizer = undefined,
    };
    try pipeline.rejectUnsupportedDeepSeekV4GraphMode();

    var graph_cache: graph_mod.cache.GraphCache = undefined;
    pipeline.graph_cache = &graph_cache;
    try std.testing.expectError(error.DeepSeekV4CompressedGraphModeNotSupported, pipeline.rejectUnsupportedDeepSeekV4GraphMode());

    pipeline.graph_cache = null;
    pipeline.compiled_partition_backend = .metal;
    try std.testing.expectError(error.DeepSeekV4CompressedGraphModeNotSupported, pipeline.rejectUnsupportedDeepSeekV4GraphMode());

    pipeline.compiled_partition_backend = null;
    pipeline.gpt_config = .{ .family = .deepseek_v4, .num_hidden_layers = 1 };
    pipeline.graph_cache = &graph_cache;
    try pipeline.rejectUnsupportedDeepSeekV4GraphMode();
}

test "native generation pipeline rejects speculative decoding when draft requires DeepSeek V4 compressed cache" {
    const allocator = std.testing.allocator;
    const plain_config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
    };
    var draft_config = gpt_mod.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 1,
    };
    draft_config.deepseek_v4_attention_schedule_len = 1;
    draft_config.deepseek_v4_attention_schedule[0] = .compressed_sparse_attention;

    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = plain_config,
        .draft_gpt_config = draft_config,
        .tokenizer = undefined,
    };
    try std.testing.expect(pipeline.speculativeUsesDeepSeekV4CompressedCache());

    pipeline.draft_gpt_config = null;
    try std.testing.expect(!pipeline.speculativeUsesDeepSeekV4CompressedCache());
}

test "speculative draft pipeline does not inherit target execution attachments" {
    var target_graph_cache: graph_mod.cache.GraphCache = undefined;
    var pjrt_sentinel: u8 = 0;
    const draft_config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2048,
        .num_hidden_layers = 26,
    };
    const target = NativeGenerationPipeline{
        .allocator = std.testing.allocator,
        .cb = undefined,
        .gpt_config = .{
            .family = .gemma,
            .hidden_size = 2816,
            .num_hidden_layers = 30,
        },
        .tokenizer = undefined,
        .graph_cache = &target_graph_cache,
        .compiled_partition_backend = .metal,
        .compiled_attachment_target = .whole_model,
        .pjrt_client = @ptrCast(&pjrt_sentinel),
    };

    const draft = target.makeSpeculativeDraftPipeline(undefined, draft_config);
    try std.testing.expectEqual(@as(u32, 2048), draft.gpt_config.hidden_size);
    try std.testing.expect(draft.graph_cache == null);
    try std.testing.expect(draft.compiled_partition_backend == null);
    try std.testing.expectEqual(
        graph_mod.compiled_backend.AttachmentTarget.partitioned,
        draft.compiled_attachment_target,
    );
    try std.testing.expect(draft.pjrt_client == null);
}

test "speculative draft Metal Gemma uses contiguous state until KV storage is attached" {
    var config = gpt_mod.Config{ .family = .gemma };
    try std.testing.expect(ordinaryMetalGemmaDraftNeedsContiguousState(.metal, config, false));
    try std.testing.expect(!ordinaryMetalGemmaDraftNeedsContiguousState(.metal, config, true));
    try std.testing.expect(!ordinaryMetalGemmaDraftNeedsContiguousState(.cuda, config, false));

    config.gemma4_mtp_assistant = true;
    try std.testing.expect(!ordinaryMetalGemmaDraftNeedsContiguousState(.metal, config, false));
    config = .{ .family = .llama };
    try std.testing.expect(!ordinaryMetalGemmaDraftNeedsContiguousState(.metal, config, false));
}

test "qualified A4B Metal target disables speculative verification upfront" {
    const a4b_config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2816,
        .num_hidden_layers = 30,
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
        .num_shared_experts = 1,
        .expert_intermediate_size = 704,
    };
    try std.testing.expect(!supportsSpeculativeTargetVerification(.metal, a4b_config));
    try std.testing.expect(supportsSpeculativeTargetVerification(.cuda, a4b_config));
    try std.testing.expect(supportsSpeculativeTargetVerification(.metal, .{
        .family = .gemma,
        .hidden_size = 2048,
        .num_hidden_layers = 26,
    }));
}

test "contiguous speculative draft state keeps full-recompute query shape in sync" {
    var state = NativeDecodeState.initContiguous(std.testing.allocator);
    defer state.deinit();
    var draft_runtime = BorrowedDecodeStateRuntime.init(&state);

    try draft_runtime.notePrefill(2);
    const prefill_ctx = draft_runtime.makeDecodeContext(2, 2);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.full_recompute, prefill_ctx.attention_mode);
    try std.testing.expectEqual(@as(usize, 2), prefill_ctx.query_sequence_len);
    try std.testing.expect(prefill_ctx.kv_cache == null);

    _ = try draft_runtime.appendGeneratedToken();
    const next_ctx = draft_runtime.makeDecodeContext(3, 3);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.full_recompute, next_ctx.attention_mode);
    try std.testing.expectEqual(@as(usize, 3), next_ctx.query_sequence_len);
    try std.testing.expectEqual(@as(usize, 3), state.total_tokens);
}

test "speculative draft A4B multi-row verification fails before target state mutation" {
    const a4b_config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2816,
        .num_hidden_layers = 30,
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
        .num_shared_experts = 1,
        .expert_intermediate_size = 704,
    };
    try std.testing.expect(gemma4_runtime.isQualifiedA4bArchitecture(a4b_config));

    var blocked_state = NativeDecodeState.initContiguous(std.testing.allocator);
    defer blocked_state.deinit();
    try blocked_state.notePrefill(2);
    var blocked_runtime = BorrowedDecodeStateRuntime.init(&blocked_state);
    try std.testing.expectError(
        error.Gemma4A4bMetalSpeculativeVerificationNotQualified,
        reserveSpeculativeTargetVerification(&blocked_runtime, .metal, a4b_config, 5, 4),
    );
    try std.testing.expectEqual(@as(usize, 2), blocked_state.total_tokens);

    var allowed_state = NativeDecodeState.initContiguous(std.testing.allocator);
    defer allowed_state.deinit();
    try allowed_state.notePrefill(2);
    var allowed_runtime = BorrowedDecodeStateRuntime.init(&allowed_state);
    try reserveSpeculativeTargetVerification(&allowed_runtime, .cuda, a4b_config, 5, 4);
    try std.testing.expectEqual(@as(usize, 6), allowed_state.total_tokens);
}

extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;

const PromotedEnvGuard = struct {
    name: [:0]const u8,
    saved: ?[:0]u8,

    fn captureAndClear(allocator: std.mem.Allocator, name: [:0]const u8) !PromotedEnvGuard {
        const saved: ?[:0]u8 = if (platform.env.getenv(name.ptr)) |value|
            try allocator.dupeZ(u8, value)
        else
            null;
        _ = unsetenv(name.ptr);
        return .{ .name = name, .saved = saved };
    }

    fn restore(self: *PromotedEnvGuard, allocator: std.mem.Allocator) void {
        if (self.saved) |value| {
            _ = setenv(self.name.ptr, value.ptr, 1);
            allocator.free(value);
        } else {
            _ = unsetenv(self.name.ptr);
        }
    }
};

test "cuda prefill scheduling knobs promote tuned defaults with bidirectional env overrides" {
    const allocator = std.testing.allocator;
    const names = [_][:0]const u8{
        "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN",
        "ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS",
        "ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK",
    };
    var guards: [names.len]PromotedEnvGuard = undefined;
    var guarded: usize = 0;
    defer for (guards[0..guarded]) |*guard| guard.restore(allocator);
    for (names) |name| {
        guards[guarded] = try PromotedEnvGuard.captureAndClear(allocator, name);
        guarded += 1;
    }

    // Unset: promoted defaults.
    try std.testing.expect(enableCudaPrefillFirstToken());
    try std.testing.expectEqual(@as(usize, 2048), cudaPrefillFirstTokenCoalesceTokenLimit());
    try std.testing.expect(enableCudaGreedyPendingTokenReadback());

    // Explicit 0 disables.
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN", "0", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS", "0", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK", "0", 1));
    try std.testing.expect(!enableCudaPrefillFirstToken());
    try std.testing.expectEqual(@as(usize, 0), cudaPrefillFirstTokenCoalesceTokenLimit());
    try std.testing.expect(!enableCudaGreedyPendingTokenReadback());

    // Explicit values enable/override.
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN", "1", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS", "8", 1));
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK", "1", 1));
    try std.testing.expect(enableCudaPrefillFirstToken());
    try std.testing.expectEqual(@as(usize, 8), cudaPrefillFirstTokenCoalesceTokenLimit());
    try std.testing.expect(enableCudaGreedyPendingTokenReadback());
}

test "cuda prefill first token path supports multi-token pure greedy generation" {
    try std.testing.expect(shouldUsePrefillFirstTokenPath(.cuda, true, false, true));
    try std.testing.expect(!shouldUsePrefillFirstTokenPath(.cuda, false, false, true));
    try std.testing.expect(!shouldUsePrefillFirstTokenPath(.cuda, true, true, true));
    try std.testing.expect(!shouldUsePrefillFirstTokenPath(.native, true, false, true));
    try std.testing.expect(!shouldUsePrefillFirstTokenPath(.metal, true, false, true));
    try std.testing.expect(!shouldUsePrefillFirstTokenPath(.cuda, true, false, false));
}

test "cuda pending token readback owns the eligible prefill result" {
    try std.testing.expect(shouldUseCudaPendingTokenReadback(
        .cuda,
        true,
        true,
        false,
        false,
        false,
        false,
        300,
    ));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, false, true, false, false, false, false, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, true, false, false, false, false, false, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, true, true, true, false, false, false, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, true, true, false, true, false, false, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, true, true, false, false, true, false, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, true, true, false, false, false, true, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.metal, true, true, false, false, false, false, 300));
    try std.testing.expect(!shouldUseCudaPendingTokenReadback(.cuda, true, true, false, false, false, false, 1));
}

test "cuda pending token readback accepts complete streaming callbacks" {
    const Callback = struct {
        fn call(_: *anyopaque, _: []const u8) bool {
            return true;
        }
    };
    var callback_ctx: u8 = 0;
    var streaming_state = StreamingTextState{ .emitted_text = &.{} };

    try std.testing.expect(pendingTokenReadbackStreamingArgsValid(null, null, null));
    try std.testing.expect(pendingTokenReadbackStreamingArgsValid(
        &Callback.call,
        @ptrCast(&callback_ctx),
        &streaming_state,
    ));
    try std.testing.expect(!pendingTokenReadbackStreamingArgsValid(&Callback.call, null, null));
    try std.testing.expect(!pendingTokenReadbackStreamingArgsValid(null, @ptrCast(&callback_ctx), &streaming_state));
}

test "prefill output intent never materializes intermediate logits" {
    try std.testing.expectEqual(PrefillOutputIntent.kv_only, prefillOutputIntent(false, false, false));
    try std.testing.expectEqual(PrefillOutputIntent.kv_only, prefillOutputIntent(false, true, true));
    try std.testing.expectEqual(PrefillOutputIntent.last_logits, prefillOutputIntent(true, false, false));
    try std.testing.expectEqual(PrefillOutputIntent.greedy_token, prefillOutputIntent(true, true, false));
    try std.testing.expectEqual(PrefillOutputIntent.last_logits_and_hidden, prefillOutputIntent(true, false, true));
    try std.testing.expectEqual(PrefillOutputIntent.greedy_token_and_hidden, prefillOutputIntent(true, true, true));
}

test "required cuda decode graph blocks only eligible eager fallbacks" {
    try std.testing.expect(requiresCudaDecodeGraphBeforeEagerFallback(.cuda, true, true, 1, false, false));
    try std.testing.expect(!requiresCudaDecodeGraphBeforeEagerFallback(.cuda, false, true, 1, false, false));
    try std.testing.expect(!requiresCudaDecodeGraphBeforeEagerFallback(.metal, true, true, 1, false, false));
    try std.testing.expect(!requiresCudaDecodeGraphBeforeEagerFallback(.cuda, true, false, 1, false, false));
    try std.testing.expect(!requiresCudaDecodeGraphBeforeEagerFallback(.cuda, true, true, 2, false, false));
    try std.testing.expect(!requiresCudaDecodeGraphBeforeEagerFallback(.cuda, true, true, 1, true, false));
    try std.testing.expect(!requiresCudaDecodeGraphBeforeEagerFallback(.cuda, true, true, 1, false, true));
}

test "cuda prefill greedy token path is pure greedy only" {
    const greedy: GenerationConfig = .{ .max_tokens = 32, .temperature = 0 };
    try std.testing.expect(shouldUseCudaPrefillGreedyToken(.cuda, greedy, true, false, true));
    try std.testing.expect(!shouldUseCudaPrefillGreedyToken(.native, greedy, true, false, true));
    try std.testing.expect(!shouldUseCudaPrefillGreedyToken(.cuda, greedy, false, false, true));
    try std.testing.expect(shouldUseCudaPrefillGreedyToken(.cuda, greedy, true, true, true));
    try std.testing.expect(!shouldUseCudaPrefillGreedyToken(.cuda, greedy, true, false, false));

    var sampled = greedy;
    sampled.temperature = 0.7;
    try std.testing.expect(!shouldUseCudaPrefillGreedyToken(.cuda, sampled, true, false, true));

    var penalized = greedy;
    penalized.repetition_penalty = 1.1;
    try std.testing.expect(!shouldUseCudaPrefillGreedyToken(.cuda, penalized, true, false, true));

    var grammar = greedy;
    grammar.grammar = "json";
    try std.testing.expect(!shouldUseCudaPrefillGreedyToken(.cuda, grammar, true, false, true));
}

test "direct prepared prefill serializes multimodal CUDA and Metal execution" {
    var mutex: std.atomic.Mutex = .unlocked;
    try std.testing.expect(directPrefillExecutionMutex(true, false, false, &mutex) == &mutex);
    try std.testing.expect(directPrefillExecutionMutex(false, true, false, &mutex) == &mutex);
    try std.testing.expect(directPrefillExecutionMutex(false, false, true, &mutex) == &mutex);
    try std.testing.expect(directPrefillExecutionMutex(false, false, false, &mutex) == null);
    try std.testing.expect(directPrefillExecutionMutex(true, true, true, null) == null);
}

test "Metal Gemma target final-row logits path is narrowly gated" {
    var config = gpt_mod.Config{ .family = .gemma, .ple_hidden_size = 256 };
    try std.testing.expect(shouldUseMetalGemmaTargetLastLogits(.metal, &config, false));
    try std.testing.expect(!shouldUseMetalGemmaTargetLastLogits(.cuda, &config, false));
    try std.testing.expect(!shouldUseMetalGemmaTargetLastLogits(.metal, &config, true));
    config.gemma4_mtp_assistant = true;
    try std.testing.expect(!shouldUseMetalGemmaTargetLastLogits(.metal, &config, false));
    config.gemma4_mtp_assistant = false;
    config.ple_hidden_size = 0;
    try std.testing.expect(!shouldUseMetalGemmaTargetLastLogits(.metal, &config, false));
    config.ple_hidden_size = 256;
    config.family = .llama;
    try std.testing.expect(!shouldUseMetalGemmaTargetLastLogits(.metal, &config, false));
}

test "scheduled prefill sends final direct-only work through direct execution" {
    try std.testing.expect(!prefillFinalChunkNeedsDirectExecution(32, 64, .greedy_token_and_hidden, true, true));
    try std.testing.expect(!prefillFinalChunkNeedsDirectExecution(64, 64, .last_logits, false, false));
    try std.testing.expect(prefillFinalChunkNeedsDirectExecution(64, 64, .greedy_token, false, false));
    try std.testing.expect(prefillFinalChunkNeedsDirectExecution(64, 64, .last_logits_and_hidden, false, false));
    try std.testing.expect(prefillFinalChunkNeedsDirectExecution(64, 64, .last_logits, true, false));
    try std.testing.expect(prefillFinalChunkNeedsDirectExecution(64, 64, .last_logits, false, true));
}

test "scheduled Gemma prefill frame only owns singleton non-logit chunks" {
    var config = gpt_mod.Config{
        .family = .gemma,
        .ple_hidden_size = 256,
    };
    try std.testing.expect(shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 1, .prefill, 128, false));
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 2, .prefill, 128, false));
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 1, .prefill, 128, true));
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 1, .prefill, 1, false));
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 1, .decode, 128, false));
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.cuda, &config, 1, .prefill, 128, false));

    config.ple_hidden_size = 0;
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 1, .prefill, 128, false));
    config.ple_hidden_size = 256;
    config.num_local_experts = 2;
    config.num_experts_per_tok = 1;
    try std.testing.expect(!shouldUseSingletonScheduledGemmaPrefillFrame(.metal, &config, 1, .prefill, 128, false));
}

test "scheduled prefill frame resets decoder runtime after hit miss and error" {
    const Probe = struct {
        reset_calls: usize = 0,

        fn reset(context: *anyopaque) anyerror!void {
            const probe: *@This() = @ptrCast(@alignCast(context));
            probe.reset_calls += 1;
        }
    };
    const Attempt = struct {
        const Outcome = enum { hit, miss, fail };

        outcome: Outcome,
        run_calls: usize = 0,

        fn run(context: *anyopaque) anyerror!bool {
            const attempt: *@This() = @ptrCast(@alignCast(context));
            attempt.run_calls += 1;
            return switch (attempt.outcome) {
                .hit => true,
                .miss => false,
                .fail => error.ScheduledPrefillProbeFailure,
            };
        }
    };

    var probe = Probe{};
    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.decoderRuntimeResetState = Probe.reset;
    const cb = ComputeBackend{ .ptr = @ptrCast(&probe), .vtable = &vtable };

    var hit = Attempt{ .outcome = .hit };
    try std.testing.expect(try runWithDecoderRuntimeResetBoundary(&cb, @ptrCast(&hit), Attempt.run));
    try std.testing.expectEqual(@as(usize, 1), hit.run_calls);
    try std.testing.expectEqual(@as(usize, 2), probe.reset_calls);

    var miss = Attempt{ .outcome = .miss };
    try std.testing.expect(!try runWithDecoderRuntimeResetBoundary(&cb, @ptrCast(&miss), Attempt.run));
    try std.testing.expectEqual(@as(usize, 1), miss.run_calls);
    try std.testing.expectEqual(@as(usize, 4), probe.reset_calls);

    var failed = Attempt{ .outcome = .fail };
    try std.testing.expectError(
        error.ScheduledPrefillProbeFailure,
        runWithDecoderRuntimeResetBoundary(&cb, @ptrCast(&failed), Attempt.run),
    );
    try std.testing.expectEqual(@as(usize, 1), failed.run_calls);
    try std.testing.expectEqual(@as(usize, 6), probe.reset_calls);
}

test "metal final-hidden frame rejects retained prompt-cache prefixes" {
    try std.testing.expect(metalFinalHiddenFrameOwnsResidentPrefix(0));
    try std.testing.expect(!metalFinalHiddenFrameOwnsResidentPrefix(512));
}

test "metal prepared tail rejects chunked and cached-prefix prefill" {
    var ctx = gpt_arch.DecodeContext{
        .attention_mode = .paged_prefill,
        .total_sequence_len = 64,
        .query_sequence_len = 64,
        .kv_sequence_len = 64,
    };
    try std.testing.expect(NativeGenerationPipeline.metalPreparedTailOwnsWholePrefill(64, 64, &ctx));

    ctx.query_sequence_len = 32;
    try std.testing.expect(!NativeGenerationPipeline.metalPreparedTailOwnsWholePrefill(32, 64, &ctx));

    ctx.query_sequence_len = 64;
    ctx.kv_position_offset = 1;
    try std.testing.expect(!NativeGenerationPipeline.metalPreparedTailOwnsWholePrefill(64, 64, &ctx));
}

test "shared batch KV mutations use the model execution lock" {
    var execution_mutex: std.atomic.Mutex = .unlocked;
    var kv_mutex: std.atomic.Mutex = .unlocked;
    try std.testing.expect(batchKvMutationMutex(&execution_mutex, &kv_mutex) == &execution_mutex);
    try std.testing.expect(batchKvMutationMutex(&execution_mutex, null) == null);
    try std.testing.expect(batchKvMutationMutex(null, &kv_mutex) == null);
}

test "cuda prefill first token coalesces only short eligible legacy prompts" {
    try std.testing.expectEqual(@as(usize, 2), coalescedPrefillChunkSizeForFirstToken(.cuda, true, false, true, 2, 1, 8));
    try std.testing.expectEqual(@as(usize, 1), coalescedPrefillChunkSizeForFirstToken(.cuda, true, false, true, 16, 1, 8));
    try std.testing.expectEqual(@as(usize, 1), coalescedPrefillChunkSizeForFirstToken(.cuda, true, false, true, 2, 1, 0));
    try std.testing.expectEqual(@as(usize, 1), coalescedPrefillChunkSizeForFirstToken(.cuda, false, false, true, 2, 1, 8));
    try std.testing.expectEqual(@as(usize, 1), coalescedPrefillChunkSizeForFirstToken(.cuda, true, true, true, 2, 1, 8));
    try std.testing.expectEqual(@as(usize, 1), coalescedPrefillChunkSizeForFirstToken(.native, true, false, true, 2, 1, 8));
}

test "admitted prefill never enlarges scheduler geometry" {
    try std.testing.expectEqual(@as(usize, 128), schedulerChunkForPrefillIteration(128, 449, false));
    try std.testing.expectEqual(@as(usize, 449), schedulerChunkForPrefillIteration(128, 449, true));
    try std.testing.expectEqual(@as(usize, 449), schedulerChunkForPrefillIteration(0, 449, false));
}

test "whole-model prefill stays bounded and speculative width is validated" {
    try std.testing.expectEqual(@as(usize, 256), wholeModelPrefillChunkSize(0, 0, 2000));
    try std.testing.expectEqual(@as(usize, 128), wholeModelPrefillChunkSize(512, 128, 2000));
    try std.testing.expectEqual(@as(usize, 64), wholeModelPrefillChunkSize(256, 0, 64));
    try validateSpeculativeK(true, runtime.tier.memory.generation_max_speculative_k);
    try std.testing.expectError(
        error.InvalidSpeculativeK,
        validateSpeculativeK(true, runtime.tier.memory.generation_max_speculative_k + 1),
    );
    try validateSpeculativeK(false, runtime.tier.memory.generation_max_speculative_k + 1);
}

test "generation KV capacity policy is route and device-format aware" {
    const config = gpt_mod.Config{
        .family = .gemma,
        .num_hidden_layers = 6,
        .position_encoding = .rope,
        .sliding_window = 512,
        .sliding_window_pattern = 6,
        .ple_hidden_size = 256,
    };

    try std.testing.expectEqual(
        runtime.tier.memory.GenerationKvCapacityPolicy.split_swa_ring,
        generationKvCapacityPolicy(.metal_whole_model, true),
    );
    try std.testing.expectEqual(
        runtime.tier.memory.GenerationKvCapacityPolicy.full_history,
        generationKvCapacityPolicy(.standard, true),
    );

    try std.testing.expectEqual(
        runtime.tier.memory.GenerationKvCapacityPolicy.full_history,
        generationKvCapacityPolicyForRoute(.standard, config, .{}, .f16),
    );
    try std.testing.expectEqual(
        runtime.tier.memory.GenerationKvCapacityPolicy.full_history,
        generationKvCapacityPolicyForRoute(.metal_whole_model, config, .{}, .int4),
    );
    try std.testing.expectEqual(
        runtime.tier.memory.GenerationKvCapacityPolicy.full_history,
        generationKvCapacityPolicyForRoute(.metal_whole_model, config, .{
            .prompt_cache_enabled = true,
        }, .f16),
    );
}

test "native generation suppress token mask removes configured logits" {
    var logits = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    NativeGenerationPipeline.applySuppressTokenMask(&logits, &.{ 1, 3, 99, -1 });
    try std.testing.expectEqual(@as(f32, 1.0), logits[0]);
    try std.testing.expect(std.math.isInf(logits[1]) and logits[1] < 0);
    try std.testing.expectEqual(@as(f32, 3.0), logits[2]);
    try std.testing.expect(std.math.isInf(logits[3]) and logits[3] < 0);
    try std.testing.expect(NativeGenerationPipeline.isSuppressedToken(1, &.{ 1, 3, 99, -1 }));
    try std.testing.expect(!NativeGenerationPipeline.isSuppressedToken(2, &.{ 1, 3, 99, -1 }));
}

test "native decode state deinit releases paged sequence" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    try state.notePrefill(5);
    const sequence_id = state.sequence_id.?;
    try std.testing.expect(manager.tokenCount(sequence_id).? > 0);

    state.deinit();
    try std.testing.expectEqual(@as(?runtime.kv.manager.SequenceId, null), state.sequence_id);
    try std.testing.expectEqual(@as(?usize, null), manager.tokenCount(sequence_id));
}

test "native decode state reports retained kv window offsets after trim" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 2,
        .num_kv_heads = 8,
        .head_dim = 128,
        .sliding_window_size = 4,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();
    try state.notePrefill(4);

    try state.appendGeneratedToken();
    try state.appendGeneratedToken();

    const view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 4), view.token_count);
    try std.testing.expectEqual(@as(usize, 2), view.position_offset);

    const ctx = state.gptDecodeContext(6, 1);
    try std.testing.expectEqual(@as(usize, 6), ctx.total_sequence_len);
    try std.testing.expectEqual(@as(usize, 4), ctx.kv_sequence_len);
    try std.testing.expectEqual(@as(usize, 2), ctx.kv_position_offset);
    try std.testing.expectEqual(@as(usize, 2), ctx.kv_cache.?.position_offset);
}

test "native decode state can disable an initialized SWA ring policy" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .metal,
        .dtype = .f16,
        .page_size_tokens = runtime.tier.memory.generation_kv_page_size_tokens,
        .num_kv_heads = 4,
        .head_dim = 256,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    state.allow_swa_ring = true;
    state.configureKvMaxInflightTokens(512, true);
    try std.testing.expect(state.allow_swa_ring);

    state.configureKvMaxInflightTokens(0, false);
    try std.testing.expect(!state.allow_swa_ring);
    try std.testing.expectEqual(@as(usize, 0), state.kv_max_inflight_tokens);
}

test "owned batch decode context captures per-item kv bindings" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 8,
        .head_dim = 64,
    });
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 8,
        .head_dim = 64,
    });
    defer storage.deinit();

    var first = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    first.kv_storage = &storage;
    defer first.deinit();
    var second = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    second.kv_storage = &storage;
    defer second.deinit();

    first.allow_swa_ring = true;
    first.configureKvMaxInflightTokens(128, true);
    second.allow_swa_ring = true;
    second.configureKvMaxInflightTokens(512, true);
    try first.notePrefill(6);
    try second.notePrefill(6);
    try std.testing.expectEqual(@as(usize, 128), first.kvView().?.max_inflight_tokens);
    try std.testing.expectEqual(@as(usize, 512), second.kvView().?.max_inflight_tokens);
    try std.testing.expect(first.kvView().?.allow_swa_ring);
    try std.testing.expect(second.kvView().?.allow_swa_ring);

    var owned = try buildOwnedBatchDecodeContext(allocator, &.{ &first, &second }, 6, 1);
    defer owned.deinit();

    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_decode, owned.context.attention_mode);
    try std.testing.expectEqual(@as(usize, 2), owned.kv_batch.?.len);
    try std.testing.expect(owned.context.kv_batch != null);
    try std.testing.expectEqual(first.sequence_id.?, owned.kv_batch.?[0].kv_cache.sequence_id);
    try std.testing.expectEqual(second.sequence_id.?, owned.kv_batch.?[1].kv_cache.sequence_id);
    try std.testing.expectEqual(@as(usize, 128), owned.kv_batch.?[0].kv_cache.max_inflight_tokens);
    try std.testing.expectEqual(@as(usize, 512), owned.kv_batch.?[1].kv_cache.max_inflight_tokens);
    try std.testing.expect(owned.kv_batch.?[0].kv_cache.allow_swa_ring);
    try std.testing.expect(owned.kv_batch.?[1].kv_cache.allow_swa_ring);
    try std.testing.expectEqual(@as(?*runtime.kv.storage_runtime.KvStorageRuntime, &storage), owned.context.kv_storage);
    try std.testing.expectEqual(@as(?*runtime.kv.storage_runtime.KvStorageRuntime, &storage), owned.kv_batch.?[0].kv_storage);
    try std.testing.expectEqual(@as(?*runtime.kv.storage_runtime.KvStorageRuntime, &storage), owned.kv_batch.?[0].kv_cache.kv_storage);
}

test "mixed batch decode context captures per-item overrides" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 8,
        .head_dim = 64,
    });
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, .{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 8,
        .head_dim = 64,
    });
    defer storage.deinit();

    var prefill = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    prefill.kv_storage = &storage;
    defer prefill.deinit();
    var decode = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    decode.kv_storage = &storage;
    defer decode.deinit();

    try prefill.notePrefill(8);
    try decode.notePrefill(6);

    var owned = try buildOwnedMixedBatchDecodeContext(allocator, &.{
        .{
            .state = &decode,
            .total_sequence_len = 7,
            .query_sequence_len = 1,
            .kv_sequence_len = 6,
            .kv_position_offset = 0,
            .attention_mode = .paged_decode,
        },
        .{
            .state = &prefill,
            .total_sequence_len = 10,
            .query_sequence_len = 2,
            .kv_sequence_len = 8,
            .kv_position_offset = 0,
            .attention_mode = .paged_prefill,
        },
    });
    defer owned.deinit();

    try std.testing.expectEqual(@as(usize, 2), owned.kv_batch.?.len);
    try std.testing.expectEqual(@as(?usize, 1), owned.kv_batch.?[0].per_item_query_len);
    try std.testing.expectEqual(@as(?usize, 2), owned.kv_batch.?[1].per_item_query_len);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_prefill, owned.context.attention_mode);
    try std.testing.expectEqual(@as(?contracts.AttentionMode, .paged_decode), owned.kv_batch.?[0].per_item_mode);
    try std.testing.expectEqual(@as(?contracts.AttentionMode, .paged_prefill), owned.kv_batch.?[1].per_item_mode);
    try std.testing.expectEqual(@as(?*runtime.kv.storage_runtime.KvStorageRuntime, &storage), owned.context.kv_storage);
    try std.testing.expectEqual(@as(?*runtime.kv.storage_runtime.KvStorageRuntime, &storage), owned.kv_batch.?[0].kv_storage);
    try std.testing.expectEqual(@as(?*runtime.kv.storage_runtime.KvStorageRuntime, &storage), owned.kv_batch.?[1].kv_cache.kv_storage);
}

test "mixed batch decode context keeps single item on direct kv cache path" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 2,
        .num_kv_heads = 8,
        .head_dim = 64,
    });

    var decode = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer decode.deinit();

    try decode.notePrefill(6);

    var owned = try buildOwnedMixedBatchDecodeContext(allocator, &.{
        .{
            .state = &decode,
            .total_sequence_len = 7,
            .query_sequence_len = 1,
            .kv_sequence_len = 6,
            .kv_position_offset = 0,
            .attention_mode = .paged_decode,
        },
    });
    defer owned.deinit();

    try std.testing.expect(owned.kv_batch == null);
    try std.testing.expect(owned.context.kv_batch == null);
    try std.testing.expect(owned.context.kv_cache != null);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_decode, owned.context.attention_mode);
    try std.testing.expectEqual(@as(usize, 7), owned.context.total_sequence_len);
    try std.testing.expectEqual(@as(usize, 1), owned.context.query_sequence_len);
    try std.testing.expectEqual(@as(usize, 6), owned.context.kv_sequence_len);
}

test "gemma4 mtp assistant position mode computes target absolute positions" {
    try std.testing.expectEqual(Gemma4MtpPositionMode.target_constant, parseGemma4MtpPositionMode(null));
    try std.testing.expectEqual(Gemma4MtpPositionMode.target_constant, parseGemma4MtpPositionMode("unknown"));
    try std.testing.expectEqual(Gemma4MtpPositionMode.target_constant, parseGemma4MtpPositionMode("block_constant"));
    try std.testing.expectEqual(Gemma4MtpPositionMode.target_absolute, parseGemma4MtpPositionMode("target_absolute"));
    try std.testing.expectEqual(Gemma4MtpPositionMode.legacy_one, parseGemma4MtpPositionMode("legacy_one"));

    try std.testing.expectEqual(
        @as(usize, 21),
        gemma4MtpAssistantTotalSequenceLen(.target_absolute, 21, 0),
    );
    try std.testing.expectEqual(
        @as(usize, 23),
        gemma4MtpAssistantTotalSequenceLen(.target_absolute, 21, 2),
    );
    try std.testing.expectEqual(
        @as(usize, 21),
        gemma4MtpAssistantTotalSequenceLen(.target_constant, 21, 2),
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        gemma4MtpAssistantTotalSequenceLen(.legacy_one, 21, 2),
    );
}

test "gemma4 mtp target hidden source parser defaults safely" {
    try std.testing.expectEqual(Gemma4MtpTargetHiddenSource.final, parseGemma4MtpTargetHiddenSource(null));
    try std.testing.expectEqual(Gemma4MtpTargetHiddenSource.final, parseGemma4MtpTargetHiddenSource("final"));
    try std.testing.expectEqual(Gemma4MtpTargetHiddenSource.pre_norm, parseGemma4MtpTargetHiddenSource("pre_norm"));
    try std.testing.expectEqual(Gemma4MtpTargetHiddenSource.final, parseGemma4MtpTargetHiddenSource("unknown"));
}

test "workload regime tagging tolerates an unavailable optional profiler" {
    const Probe = struct {
        calls: usize = 0,
        failure: anyerror,

        fn set(ctx: *anyopaque, _: ops.WorkloadRegime) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return self.failure;
        }
    };

    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.workloadProfileSetRegime = Probe.set;

    var unavailable = Probe{ .failure = error.WorkloadProfileUnavailable };
    const unavailable_cb = ComputeBackend{ .ptr = &unavailable, .vtable = &vtable };
    try setWorkloadProfileRegime(&unavailable_cb, .decode);
    try std.testing.expectEqual(@as(usize, 1), unavailable.calls);

    var broken = Probe{ .failure = error.BackendBroken };
    const broken_cb = ComputeBackend{ .ptr = &broken, .vtable = &vtable };
    try std.testing.expectError(error.BackendBroken, setWorkloadProfileRegime(&broken_cb, .decode));
}

test "prepared tail MTP capture applies the final RMS norm contract" {
    const Probe = struct {
        called: bool = false,
        input: ?ops.CT = null,
        slot: usize = 0,
        hidden_size: usize = 0,
        eps: f32 = 0.0,
        output: ops.CT,

        fn applyRmsNorm(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormRequest) anyerror!?ops.CT {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.called = true;
            self.input = request.input;
            self.slot = request.slot;
            self.hidden_size = request.hidden_size;
            self.eps = request.eps;
            return self.output;
        }
    };

    var pre_norm_storage: u8 = 0;
    var post_norm_storage: u8 = 0;
    const pre_norm: ops.CT = @ptrCast(&pre_norm_storage);
    const post_norm: ops.CT = @ptrCast(&post_norm_storage);
    var probe = Probe{ .output = post_norm };
    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.decoderRuntimeApplyRmsNorm = Probe.applyRmsNorm;
    const cb = ComputeBackend{ .ptr = @ptrCast(&probe), .vtable = &vtable };

    try std.testing.expect((try NativeGenerationPipeline.capturePreparedTailPostNormHidden(
        &cb,
        false,
        pre_norm,
        7,
        1536,
        1e-6,
    )) == null);
    try std.testing.expect(!probe.called);

    const captured = (try NativeGenerationPipeline.capturePreparedTailPostNormHidden(
        &cb,
        true,
        pre_norm,
        7,
        1536,
        1e-6,
    )) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(post_norm, captured);
    try std.testing.expect(probe.called);
    try std.testing.expectEqual(pre_norm, probe.input.?);
    try std.testing.expectEqual(@as(usize, 7), probe.slot);
    try std.testing.expectEqual(@as(usize, 1536), probe.hidden_size);
    try std.testing.expectEqual(@as(f32, 1e-6), probe.eps);
}

test "speculation calibration parser is explicit" {
    try std.testing.expectEqual(SpeculationCalibration.none, parseSpeculationCalibration("none").?);
    try std.testing.expectEqual(SpeculationCalibration.none, parseSpeculationCalibration("off").?);
    try std.testing.expectEqual(SpeculationCalibration.probe, parseSpeculationCalibration("probe").?);
    try std.testing.expectEqual(SpeculationCalibration.probe, parseSpeculationCalibration("calibrate").?);
    try std.testing.expectEqual(SpeculationCalibration.positive, parseSpeculationCalibration("positive").?);
    try std.testing.expectEqual(SpeculationCalibration.positive, parseSpeculationCalibration("calibrated").?);
    try std.testing.expect(parseSpeculationCalibration("true") == null);
}

test "Gemma4 MTP bonus default remains conservative on Metal" {
    try std.testing.expectEqual(false, gemma4MtpAcceptBonusDefault(.metal));
    try std.testing.expectEqual(true, gemma4MtpAcceptBonusDefault(.cuda));
    try std.testing.expectEqual(true, gemma4MtpAcceptBonusDefault(.native));
}

test "Gemma4 MTP active materialization is default" {
    try std.testing.expectEqual(true, gemma4MtpActiveMaterializeEnabled());
}

test "Gemma4 MTP deferred materialization defaults off" {
    try std.testing.expectEqual(false, gemma4MtpDeferMaterializeEnabled(.metal));
    try std.testing.expectEqual(false, gemma4MtpDeferMaterializeTargetActivationEnabled(.metal));
    try std.testing.expectEqual(false, gemma4MtpDeferMaterializeEnabled(.cuda));
    try std.testing.expectEqual(false, gemma4MtpDeferMaterializeTargetActivationEnabled(.cuda));
}

test "Gemma4 MTP Metal auto defaults off" {
    try std.testing.expectEqual(false, gemma4MtpMetalAutoEnabled());
    try std.testing.expectEqual(false, gemma4MtpMetalPrefillHiddenCaptureEnabled());
    try std.testing.expectEqual(@as(usize, 2), gemma4MtpAutoMaxK());
    try std.testing.expectEqual(@as(usize, 16), gemma4MtpAutoCostProbeRounds());
    try std.testing.expectEqual(@as(usize, 2000), gemma4MtpAutoMinAcceptedPerRoundMilli());
}

test "gemma4 mtp pending token bookkeeping holds kv invariant across round shapes" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();
    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 4,
        .num_layers_packed = 1,
        .num_kv_heads = 2,
        .head_dim = 8,
    });
    var decode = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer decode.deinit();

    var mtp_activation = NativeGenerationPipeline.Gemma4MtpActivationState{
        .allocator = allocator,
        .draft_cb = undefined, // no device tensors touched in this test
    };
    defer mtp_activation.deinit();

    // Prefill + materialized first token: nothing pending, total == seq_len.
    try decode.notePrefill(8);
    var seq_len: usize = 8;
    try std.testing.expectEqual(@as(usize, 0), mtp_activation.pendingCount());
    try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());
    try mtp_activation.validateKvTokensInSync(&decode, seq_len);
    try std.testing.expectError(
        error.InvalidSpeculativeState,
        mtp_activation.validateKvTokensInSync(&decode, seq_len + 1),
    );

    // Round A: k=2, mismatch at draft index 1 -> correction committed, fold
    // leaves it pending instead of materializing.
    {
        const draft_count: usize = 2;
        const matched_drafts: usize = 1;
        try decode.appendGeneratedTokens(draft_count + mtp_activation.pendingCount());
        mtp_activation.notePendingKvWritten(); // verify forward succeeded
        try decode.truncateTokens(draft_count - matched_drafts);
        try mtp_activation.notePendingUnmaterialized();
        try std.testing.expectError(error.InvalidSpeculativeState, mtp_activation.notePendingUnmaterialized());
        seq_len += matched_drafts + 1;
        try std.testing.expectEqual(@as(usize, 1), mtp_activation.pendingCount());
        try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());
        try mtp_activation.validateKvTokensInSync(&decode, seq_len);
    }

    // Round B: enters pending; k=2 pure accept. The verify appends k+1 slots
    // (row 0 writes the pending token's KV) and rolls back only the tail.
    {
        const draft_count: usize = 2;
        const entry_pending = mtp_activation.pendingCount();
        try std.testing.expectEqual(@as(usize, 1), entry_pending);
        try decode.appendGeneratedTokens(draft_count + entry_pending);
        try std.testing.expectEqual(seq_len + draft_count, decode.total_tokens);
        mtp_activation.notePendingKvWritten();
        const matched_drafts: usize = 2;
        try decode.truncateTokens(draft_count - matched_drafts);
        seq_len += matched_drafts;
        try std.testing.expectEqual(@as(usize, 0), mtp_activation.pendingCount());
        try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());
    }

    // Round C: k=2 all matched + bonus -> bonus token pending.
    {
        const draft_count: usize = 2;
        try decode.appendGeneratedTokens(draft_count + mtp_activation.pendingCount());
        mtp_activation.notePendingKvWritten();
        try mtp_activation.notePendingUnmaterialized();
        seq_len += draft_count + 1;
        try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());
        try mtp_activation.validateKvTokensInSync(&decode, seq_len);
    }

    // Round D: verify forward fails while pending -> roll back the entire
    // reservation (drafts + pending slot); the pending token stays pending.
    {
        const draft_count: usize = 2;
        const entry_pending = mtp_activation.pendingCount();
        try std.testing.expectEqual(@as(usize, 1), entry_pending);
        try decode.appendGeneratedTokens(draft_count + entry_pending);
        try decode.truncateTokens(draft_count + mtp_activation.pendingCount());
        try std.testing.expectEqual(@as(usize, 1), mtp_activation.pendingCount());
        try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());
    }

    // Fallback flush: one-token materialize restores total == seq_len before
    // standardDecode runs.
    try decode.appendGeneratedTokens(1);
    mtp_activation.notePendingKvWritten();
    try std.testing.expectEqual(@as(usize, 0), mtp_activation.pendingCount());
    try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());

    // Final round ends with a correction (pending) and generation stops:
    // deinit with a pending token is a clean exit.
    try decode.appendGeneratedTokens(1 + mtp_activation.pendingCount());
    mtp_activation.notePendingKvWritten();
    try decode.truncateTokens(1);
    try mtp_activation.notePendingUnmaterialized();
    seq_len += 1;
    try std.testing.expectEqual(seq_len, decode.total_tokens + mtp_activation.pendingCount());
    try mtp_activation.validateKvTokensInSync(&decode, seq_len);
}

test "Gemma4 MTP cached first choice covers single draft" {
    try std.testing.expectEqual(false, gemma4MtpCachedFirstChoiceAllowed(0, true));
    try std.testing.expectEqual(false, gemma4MtpCachedFirstChoiceAllowed(1, false));
    try std.testing.expectEqual(true, gemma4MtpCachedFirstChoiceAllowed(1, true));
    try std.testing.expectEqual(true, gemma4MtpCachedFirstChoiceAllowed(2, true));
}

test "Gemma4 MTP deferred target activation maps cached verify rows" {
    try std.testing.expectEqual(@as(?usize, null), gemma4MtpDeferredTargetActivationRow(true, 0, 1));
    try std.testing.expectEqual(@as(?usize, 0), gemma4MtpDeferredTargetActivationRow(true, 0, 2));
    try std.testing.expectEqual(@as(?usize, 2), gemma4MtpDeferredTargetActivationRow(true, 0, 4));
    try std.testing.expectEqual(@as(?usize, 3), gemma4MtpDeferredTargetActivationRow(false, 1, 3));
}

test "Gemma4 Metal direct greedy is default" {
    try std.testing.expectEqual(true, gemma4MetalDirectGreedyDefault());
}

test "Metal pipelined greedy EOS policy honors ignore_eos" {
    const pipeline = NativeGenerationPipeline{
        .allocator = std.testing.allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16, .eos_token_id = 7 },
        .tokenizer = undefined,
    };

    try std.testing.expect(pipeline.shouldStopOnEos(.{}, 7));
    try std.testing.expect(!pipeline.shouldStopOnEos(.{ .ignore_eos = true }, 7));
    try std.testing.expect(!pipeline.shouldStopOnEos(.{}, 6));
}

test "Gemma4 pipelined Metal decode frames delegate to the shared policy" {
    // Policy precedence (disable > enable > device default) is owned and
    // tested by metal_runtime.pipelinedDecodeFrameEnabledForFlags.
    try std.testing.expect(!metal_runtime.pipelinedDecodeFrameEnabledForFlags(false, true, true));
}

test "gemma4 mtp adaptive k starts with probe and ramps on accepted windows" {
    var policy = MtpAdaptiveKPolicy.initFromValues(8, 750, 4, true, 1);

    try std.testing.expect(policy.enabled);
    try std.testing.expectEqual(@as(usize, 1), policy.nextK(16));
    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(1, 1));
    try std.testing.expectEqual(@as(usize, 1), policy.nextK(16));
    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(1, 1));
    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(1, 1));
    try std.testing.expectEqual(MtpAdaptiveKDecision.ramped, policy.observe(1, 0));
    try std.testing.expectEqual(@as(usize, 2), policy.nextK(16));

    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(2, 2));
    try std.testing.expectEqual(MtpAdaptiveKDecision.ramped, policy.observe(2, 2));
    try std.testing.expectEqual(@as(usize, 4), policy.nextK(16));
}

test "gemma4 mtp adaptive k falls back on low rolling acceptance" {
    var policy = MtpAdaptiveKPolicy.initFromValues(4, 750, 4, true, 1);

    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(1, 1));
    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(1, 0));
    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(1, 1));
    try std.testing.expectEqual(MtpAdaptiveKDecision.fallback, policy.observe(1, 0));
    try std.testing.expectEqual(@as(usize, 1), policy.nextK(16));
}

test "gemma4 mtp adaptive k can be disabled for fixed-k diagnostics" {
    var policy = MtpAdaptiveKPolicy.initFromValues(4, 750, 4, false, 1);

    try std.testing.expect(!policy.enabled);
    try std.testing.expectEqual(@as(usize, 4), policy.nextK(16));
    try std.testing.expectEqual(MtpAdaptiveKDecision.keep, policy.observe(4, 0));
    try std.testing.expectEqual(@as(usize, 4), policy.nextK(16));
}

test "gemma4 mtp adaptive k disables when acceptance probe window is zero" {
    var policy = MtpAdaptiveKPolicy.initFromValues(4, 750, 0, true, 1);

    try std.testing.expect(!policy.enabled);
    try std.testing.expectEqual(@as(usize, 4), policy.nextK(16));
}

test "gemma4 mtp adaptive zero-match fallback allows a second probe" {
    try std.testing.expectEqual(
        @as(usize, 2),
        gemma4MtpEffectiveZeroMatchFallbackRounds(1, true),
    );
    try std.testing.expectEqual(
        @as(usize, 3),
        gemma4MtpEffectiveZeroMatchFallbackRounds(3, true),
    );
    try std.testing.expectEqual(
        @as(usize, 0),
        gemma4MtpEffectiveZeroMatchFallbackRounds(0, true),
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        gemma4MtpEffectiveZeroMatchFallbackRounds(1, false),
    );
}

test "debug top k helper is stable and ranks tokens" {
    var logits = [_]f32{ 1.0, 5.0, 5.0, -1.0, 7.0 };
    var entries: [3]DebugTopKEntry = undefined;
    const count = fillDebugTopK(logits[0..], entries[0..]);

    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(usize, 4), entries[0].token);
    try std.testing.expectEqual(@as(usize, 1), entries[1].token);
    try std.testing.expectEqual(@as(usize, 2), entries[2].token);
    try std.testing.expectEqual(@as(?usize, 1), debugTopKRank(entries[0..count], 4));
    try std.testing.expectEqual(@as(?usize, 3), debugTopKRank(entries[0..count], 2));
    try std.testing.expectEqual(@as(?usize, null), debugTopKRank(entries[0..count], 0));
}

test "gemma4 mtp mismatch quality tracks near top-k format misses" {
    var assistant_logits = [_]f32{ 0.0, 3.0, 2.8, -1.0 };
    var target_logits = [_]f32{ 0.0, 1.8, 2.0, -1.0 };
    const assistant_rows = [_]?[]f32{assistant_logits[0..]};
    const assistant_totals = [_]usize{11};
    const source_tokens = [_]i64{42};
    const logit_sources = [_]gemma4_mtp.DraftLogitSource{.host_argmax};
    const trace = MtpParityTrace{
        .top_k = 4,
        .assistant_logits = assistant_rows[0..],
        .assistant_total_sequence_lens = assistant_totals[0..],
        .source_tokens = source_tokens[0..],
        .logit_sources = logit_sources[0..],
        .hidden_source = .final,
        .concat_order = .embedding_activation,
        .kv_donor_mode = .shared_type,
    };

    const stats = classifyGemma4MtpMismatch(trace, 0, target_logits[0..], 1, 2);

    try std.testing.expectEqual(@as(usize, 1), stats.mismatches);
    try std.testing.expectEqual(@as(usize, 1), stats.mismatches_with_assistant_logits);
    try std.testing.expectEqual(@as(usize, 1), stats.target_in_assistant_top2);
    try std.testing.expectEqual(@as(usize, 1), stats.target_in_assistant_top4);
    try std.testing.expectEqual(@as(usize, 1), stats.target_in_assistant_top8);
    try std.testing.expectEqual(@as(usize, 1), stats.draft_in_target_top2);
    try std.testing.expectEqual(@as(usize, 1), stats.format_or_control_misses);
    try std.testing.expectEqual(@as(usize, 1), stats.near_tie_misses);
    try std.testing.expectEqual(@as(usize, 0), stats.confident_misses);
    try std.testing.expectApproxEqAbs(@as(f32, 0.2), stats.averageAssistantTargetMargin(), 0.0001);
}

test "sampling penalties reuse incremental token counts" {
    const allocator = std.testing.allocator;
    var penalty_state = SamplingPenaltyState{};
    defer penalty_state.deinit(allocator);

    try penalty_state.seedFromHistory(allocator, &.{ 1, 2, 1 });

    var logits = [_]f32{ 0.0, 1.0, 1.0, 1.0 };
    applyRepetitionPenalties(logits[0..], &penalty_state, .{
        .repetition_penalty = 2.0,
        .frequency_penalty = 0.5,
        .presence_penalty = 0.25,
    });

    try std.testing.expectEqual(@as(f32, 0.0), logits[0]);
    try std.testing.expectApproxEqAbs(@as(f32, -0.75), logits[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, -0.25), logits[2], 0.0001);
    try std.testing.expectEqual(@as(f32, 1.0), logits[3]);
}

test "speculative verification applies grammar mask before accepting draft tokens" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "continuing_subword_prefix": "##",
        \\    "vocab": {
        \\      "[PAD]": 0,
        \\      "[UNK]": 1,
        \\      "[CLS]": 2,
        \\      "[SEP]": 3,
        \\      "hello": 4,
        \\      "world": 5
        \\    }
        \\  },
        \\  "added_tokens": [
        \\    {"id": 0, "content": "[PAD]", "special": true},
        \\    {"id": 1, "content": "[UNK]", "special": true},
        \\    {"id": 2, "content": "[CLS]", "special": true},
        \\    {"id": 3, "content": "[SEP]", "special": true}
        \\  ]
        \\}
    ;

    var tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer tok.deinitSelf();

    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{
            .vocab_size = @intCast(tok.tokenizer().vocabSize()),
        },
        .tokenizer = tok.tokenizer(),
    };

    var token_ids = [_]i64{ 2, 0, 0 };
    var penalties = SamplingPenaltyState{};
    defer penalties.deinit(allocator);
    var json_grammar: ?grammar_mod.JsonGrammar = null;

    var grammar = try grammar_mod.GbnfGrammar.parse(allocator, "root ::= \"hello\"");
    defer grammar.deinit();
    var token_table = try grammar_mod.TokenByteTable.init(allocator, pipeline.tokenizer, pipeline.gpt_config.vocab_size);
    defer token_table.deinit(allocator);

    var target_logits = [_]f32{
        -10.0, -10.0, -10.0, -10.0, 5.0, 9.0, // verification position: world would win without grammar
        -10.0, -10.0, -10.0, -10.0, 1.0, 2.0, // bonus position, should not be used
    };

    const result = try pipeline.acceptVerifiedDraftTokens(
        token_ids[0..],
        1,
        &.{5},
        target_logits[0..],
        2,
        .{ .temperature = 0 },
        &penalties,
        &token_table,
        &json_grammar,
        &grammar,
        true,
        null,
    );

    try std.testing.expectEqual(@as(usize, 0), result.matched_drafts);
    try std.testing.expectEqual(@as(usize, 1), result.accepted);
    try std.testing.expectEqual(true, result.correction_added);
    try std.testing.expectEqual(true, result.hit_grammar_stop);
    try std.testing.expectEqual(false, result.had_bonus);
    try std.testing.expectEqual(@as(i64, 4), token_ids[1]);
}

test "speculative bonus is disabled at generation budget boundary" {
    try std.testing.expectEqual(false, shouldAcceptSpeculativeBonus(false, 1, 2));
    try std.testing.expectEqual(true, shouldAcceptSpeculativeBonus(true, 1, 2));
    try std.testing.expectEqual(false, shouldAcceptSpeculativeBonus(true, 1, 1));
    try std.testing.expectEqual(false, shouldAcceptSpeculativeBonus(true, 2, 1));
}

test "speculative verification can skip target bonus token" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 6 },
        .tokenizer = undefined,
    };

    var token_ids = [_]i64{ 2, 0, 0 };
    var penalties = SamplingPenaltyState{};
    defer penalties.deinit(allocator);
    var json_grammar: ?grammar_mod.JsonGrammar = null;

    var target_logits = [_]f32{
        -10.0, -10.0, -10.0, -10.0, 9.0, 1.0, // draft token 4 matches
        -10.0, -10.0, -10.0, -10.0, 1.0, 9.0, // bonus token 5 would win
    };

    const result = try pipeline.acceptVerifiedDraftTokens(
        token_ids[0..],
        1,
        &.{4},
        target_logits[0..],
        2,
        .{ .temperature = 0 },
        &penalties,
        null,
        &json_grammar,
        null,
        false,
        null,
    );

    try std.testing.expectEqual(@as(usize, 1), result.matched_drafts);
    try std.testing.expectEqual(@as(usize, 1), result.accepted);
    try std.testing.expectEqual(false, result.correction_added);
    try std.testing.expectEqual(false, result.had_bonus);
    try std.testing.expectEqual(true, result.bonus_skipped);
    try std.testing.expectEqual(@as(i64, 0), token_ids[2]);
}

test "speculative greedy verification excludes EOS from accepted tokens" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 8, .eos_token_id = 7 },
        .tokenizer = undefined,
    };

    var correction_tokens = [_]i64{ 2, 0, 0 };
    var correction_penalties = SamplingPenaltyState{};
    defer correction_penalties.deinit(allocator);
    const correction = try pipeline.acceptVerifiedDraftTokenChoicesGreedy(
        correction_tokens[0..],
        1,
        &.{4},
        &.{ 7, 6 },
        &correction_penalties,
        true,
        .{},
    );
    try std.testing.expectEqual(@as(usize, 0), correction.accepted);
    try std.testing.expectEqual(@as(usize, 0), correction.matched_drafts);
    try std.testing.expect(!correction.correction_added);
    try std.testing.expect(correction.hit_eos);
    try std.testing.expectEqual(@as(i64, 0), correction_tokens[1]);

    var bonus_tokens = [_]i64{ 2, 4, 0 };
    var bonus_penalties = SamplingPenaltyState{};
    defer bonus_penalties.deinit(allocator);
    const bonus = try pipeline.acceptVerifiedDraftTokenChoicesGreedy(
        bonus_tokens[0..],
        1,
        &.{4},
        &.{ 4, 7 },
        &bonus_penalties,
        true,
        .{},
    );
    try std.testing.expectEqual(@as(usize, 1), bonus.accepted);
    try std.testing.expectEqual(@as(usize, 1), bonus.matched_drafts);
    try std.testing.expect(!bonus.had_bonus);
    try std.testing.expect(bonus.hit_eos);
    try std.testing.expectEqual(@as(i64, 0), bonus_tokens[2]);
}

test "gemma4 mtp cached first choice rejects mismatched draft" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16, .eos_token_id = 7 },
        .tokenizer = undefined,
    };

    var matching_tokens = [_]i64{ 2, 0, 0 };
    const matching = try pipeline.rejectMtpDraftFromCachedFirstChoice(
        matching_tokens[0..],
        1,
        &.{4},
        4,
        .{},
    );
    try std.testing.expect(matching == null);
    try std.testing.expectEqual(@as(i64, 0), matching_tokens[1]);

    var rejected_tokens = [_]i64{ 2, 0, 0 };
    const rejected = (try pipeline.rejectMtpDraftFromCachedFirstChoice(
        rejected_tokens[0..],
        1,
        &.{4},
        7,
        .{},
    )) orelse return error.TestExpectedNonNull;

    try std.testing.expectEqual(@as(usize, 0), rejected.matched_drafts);
    try std.testing.expectEqual(@as(usize, 0), rejected.accepted);
    try std.testing.expectEqual(false, rejected.correction_added);
    try std.testing.expectEqual(false, rejected.had_bonus);
    try std.testing.expectEqual(true, rejected.hit_eos);
    try std.testing.expectEqual(@as(i64, 0), rejected_tokens[1]);
    try std.testing.expectEqual(@as(usize, 1), rejected.mtp_quality.mismatches);

    var ignored_tokens = [_]i64{ 2, 0, 0 };
    const ignored = (try pipeline.rejectMtpDraftFromCachedFirstChoice(
        ignored_tokens[0..],
        1,
        &.{4},
        7,
        .{ .ignore_eos = true },
    )) orelse return error.TestExpectedNonNull;
    try std.testing.expectEqual(false, ignored.hit_eos);
    try std.testing.expectEqual(@as(usize, 1), ignored.accepted);
    try std.testing.expectEqual(true, ignored.correction_added);
    try std.testing.expectEqual(@as(i64, 7), ignored_tokens[1]);
}

test "gemma4 mtp verify-commit adapter handles correction metadata" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16 },
        .tokenizer = undefined,
    };

    var token_ids = [_]i64{ 2, 4, 0, 0 };
    var penalties = SamplingPenaltyState{};
    defer penalties.deinit(allocator);
    var target_choices = [_]u32{ 4, 7, 9 };
    var runtime_result = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = target_choices[0..],
        .matched_drafts = 1,
        .accepted = 2,
        .correction_added = true,
        .had_bonus = false,
        .bonus_skipped = false,
        .hit_eos = false,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };

    const result = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        token_ids[0..],
        1,
        &.{ 4, 5 },
        &runtime_result,
        &penalties,
    );

    try std.testing.expectEqual(@as(usize, 1), result.matched_drafts);
    try std.testing.expectEqual(@as(usize, 2), result.accepted);
    try std.testing.expectEqual(true, result.correction_added);
    try std.testing.expectEqual(false, result.had_bonus);
    try std.testing.expectEqual(@as(i64, 7), token_ids[2]);
}

test "gemma4 mtp verify-commit adapter excludes EOS metadata" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16, .eos_token_id = 7 },
        .tokenizer = undefined,
    };

    var correction_tokens = [_]i64{ 2, 4, 0, 0 };
    var correction_penalties = SamplingPenaltyState{};
    defer correction_penalties.deinit(allocator);
    var correction_choices = [_]u32{ 4, 7, 9 };
    var correction_runtime = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = correction_choices[0..],
        .matched_drafts = 1,
        .accepted = 2,
        .correction_added = true,
        .had_bonus = false,
        .bonus_skipped = false,
        .hit_eos = true,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };
    const correction = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        correction_tokens[0..],
        1,
        &.{ 4, 5 },
        &correction_runtime,
        &correction_penalties,
    );
    try std.testing.expectEqual(@as(usize, 1), correction.accepted);
    try std.testing.expectEqual(@as(usize, 1), correction.matched_drafts);
    try std.testing.expect(!correction.correction_added);
    try std.testing.expect(correction.hit_eos);
    try std.testing.expectEqual(@as(i64, 0), correction_tokens[2]);

    var matched_tokens = [_]i64{ 2, 4, 7, 0 };
    var matched_penalties = SamplingPenaltyState{};
    defer matched_penalties.deinit(allocator);
    var matched_choices = [_]u32{ 4, 7, 9 };
    var matched_runtime = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = matched_choices[0..],
        .matched_drafts = 2,
        .accepted = 2,
        .correction_added = false,
        .had_bonus = false,
        .bonus_skipped = false,
        .hit_eos = true,
        .commit_forward_required = false,
        .accepted_hidden_row = 1,
    };
    const matched = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        matched_tokens[0..],
        1,
        &.{ 4, 7 },
        &matched_runtime,
        &matched_penalties,
    );
    try std.testing.expectEqual(@as(usize, 1), matched.accepted);
    try std.testing.expectEqual(@as(usize, 1), matched.matched_drafts);
    try std.testing.expect(matched.hit_eos);

    var bonus_tokens = [_]i64{ 2, 4, 5, 0 };
    var bonus_penalties = SamplingPenaltyState{};
    defer bonus_penalties.deinit(allocator);
    var bonus_choices = [_]u32{ 4, 5, 7 };
    var bonus_runtime = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = bonus_choices[0..],
        .matched_drafts = 2,
        .accepted = 3,
        .correction_added = false,
        .had_bonus = true,
        .bonus_skipped = false,
        .hit_eos = true,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };
    const bonus = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        bonus_tokens[0..],
        1,
        &.{ 4, 5 },
        &bonus_runtime,
        &bonus_penalties,
    );
    try std.testing.expectEqual(@as(usize, 2), bonus.accepted);
    try std.testing.expectEqual(@as(usize, 2), bonus.matched_drafts);
    try std.testing.expect(!bonus.had_bonus);
    try std.testing.expect(bonus.hit_eos);
    try std.testing.expectEqual(@as(i64, 0), bonus_tokens[3]);
}

test "gemma4 mtp verify-commit adapter handles compact device metadata" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16 },
        .tokenizer = undefined,
    };

    var correction_tokens = [_]i64{ 2, 4, 0, 0 };
    var correction_penalties = SamplingPenaltyState{};
    defer correction_penalties.deinit(allocator);
    var correction_runtime = ops.Gemma4MtpVerifyCommitResult{
        .compact_device_result = true,
        .correction_token = 7,
        .matched_drafts = 1,
        .accepted = 2,
        .correction_added = true,
        .had_bonus = false,
        .bonus_skipped = false,
        .hit_eos = false,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };

    const correction_result = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        correction_tokens[0..],
        1,
        &.{ 4, 5 },
        &correction_runtime,
        &correction_penalties,
    );
    try std.testing.expectEqual(@as(usize, 2), correction_result.accepted);
    try std.testing.expectEqual(true, correction_result.correction_added);
    try std.testing.expectEqual(@as(i64, 7), correction_tokens[2]);

    var bonus_tokens = [_]i64{ 2, 4, 5, 0 };
    var bonus_penalties = SamplingPenaltyState{};
    defer bonus_penalties.deinit(allocator);
    var bonus_runtime = ops.Gemma4MtpVerifyCommitResult{
        .compact_device_result = true,
        .bonus_token = 6,
        .matched_drafts = 2,
        .accepted = 3,
        .correction_added = false,
        .had_bonus = true,
        .bonus_skipped = false,
        .hit_eos = false,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };

    const bonus_result = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        bonus_tokens[0..],
        1,
        &.{ 4, 5 },
        &bonus_runtime,
        &bonus_penalties,
    );
    try std.testing.expectEqual(@as(usize, 3), bonus_result.accepted);
    try std.testing.expectEqual(true, bonus_result.had_bonus);
    try std.testing.expectEqual(@as(i64, 6), bonus_tokens[3]);
}

test "gemma4 mtp verify-commit adapter handles bonus and skipped bonus metadata" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16 },
        .tokenizer = undefined,
    };

    var bonus_tokens = [_]i64{ 2, 4, 5, 0 };
    var bonus_penalties = SamplingPenaltyState{};
    defer bonus_penalties.deinit(allocator);
    var bonus_choices = [_]u32{ 4, 5, 6 };
    var bonus_runtime = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = bonus_choices[0..],
        .matched_drafts = 2,
        .accepted = 3,
        .correction_added = false,
        .had_bonus = true,
        .bonus_skipped = false,
        .hit_eos = false,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };

    const bonus_result = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        bonus_tokens[0..],
        1,
        &.{ 4, 5 },
        &bonus_runtime,
        &bonus_penalties,
    );

    try std.testing.expectEqual(@as(usize, 2), bonus_result.matched_drafts);
    try std.testing.expectEqual(@as(usize, 3), bonus_result.accepted);
    try std.testing.expectEqual(true, bonus_result.had_bonus);
    try std.testing.expectEqual(@as(i64, 6), bonus_tokens[3]);

    var skipped_tokens = [_]i64{ 2, 4, 5, 0 };
    var skipped_penalties = SamplingPenaltyState{};
    defer skipped_penalties.deinit(allocator);
    var skipped_choices = [_]u32{ 4, 5, 6 };
    var skipped_runtime = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = skipped_choices[0..],
        .matched_drafts = 2,
        .accepted = 2,
        .correction_added = false,
        .had_bonus = false,
        .bonus_skipped = true,
        .hit_eos = false,
        .commit_forward_required = false,
        .accepted_hidden_row = 1,
    };

    const skipped_result = try pipeline.acceptGemma4MtpVerifyCommitResultGreedy(
        skipped_tokens[0..],
        1,
        &.{ 4, 5 },
        &skipped_runtime,
        &skipped_penalties,
    );

    try std.testing.expectEqual(@as(usize, 2), skipped_result.accepted);
    try std.testing.expectEqual(false, skipped_result.had_bonus);
    try std.testing.expectEqual(true, skipped_result.bonus_skipped);
    try std.testing.expectEqual(@as(i64, 0), skipped_tokens[3]);
}

test "gemma4 mtp verify-commit adapter rejects inconsistent metadata" {
    const allocator = std.testing.allocator;
    var pipeline = NativeGenerationPipeline{
        .allocator = allocator,
        .cb = undefined,
        .gpt_config = .{ .vocab_size = 16 },
        .tokenizer = undefined,
    };

    var token_ids = [_]i64{ 2, 4, 0, 0 };
    var penalties = SamplingPenaltyState{};
    defer penalties.deinit(allocator);
    var target_choices = [_]u32{ 4, 7, 9 };
    var bad_accepted = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = target_choices[0..],
        .matched_drafts = 1,
        .accepted = 3,
        .correction_added = true,
        .had_bonus = false,
        .bonus_skipped = false,
        .hit_eos = false,
        .commit_forward_required = true,
        .accepted_hidden_row = null,
    };

    try std.testing.expectError(
        error.InvalidSpeculativeState,
        pipeline.acceptGemma4MtpVerifyCommitResultGreedy(token_ids[0..], 1, &.{ 4, 5 }, &bad_accepted, &penalties),
    );

    var mismatched_choices = [_]u32{ 9, 5, 6 };
    var bad_match = ops.Gemma4MtpVerifyCommitResult{
        .target_choices = mismatched_choices[0..],
        .matched_drafts = 1,
        .accepted = 1,
        .correction_added = false,
        .had_bonus = false,
        .bonus_skipped = false,
        .hit_eos = false,
        .commit_forward_required = false,
        .accepted_hidden_row = 0,
    };

    try std.testing.expectError(
        error.InvalidSpeculativeState,
        pipeline.acceptGemma4MtpVerifyCommitResultGreedy(token_ids[0..], 1, &.{ 4, 5 }, &bad_match, &penalties),
    );
}

/// Sample next token from logits using the full sampling pipeline.
/// Order (matching llama.cpp): repetition/frequency/presence penalty → temperature → top-k → top-p → min-p → sample.
fn sample(logits: []const f32, config: GenerationConfig, penalty_state: *const SamplingPenaltyState, allocator: std.mem.Allocator) usize {
    // Greedy (temperature=0 or default) with no penalties — fast path
    const has_penalties = hasSamplingPenalties(config);
    if (config.temperature <= 0 and !has_penalties) {
        return activations.argmax(logits);
    }

    const vocab_size = logits.len;
    const working = allocator.alloc(f32, vocab_size) catch return activations.argmax(logits);
    defer allocator.free(working);
    @memcpy(working, logits);

    // Step 1: Repetition / frequency / presence penalties (applied to raw logits before softmax)
    if (has_penalties and !penalty_state.isEmpty()) {
        applyRepetitionPenalties(working, penalty_state, config);
    }

    // Greedy after penalties
    if (config.temperature <= 0) {
        return activations.argmax(working);
    }

    // Step 2: Temperature scaling
    const inv_temp = 1.0 / config.temperature;
    for (working) |*v| v.* *= inv_temp;

    // Softmax
    activations.softmax(working, vocab_size);

    // Step 3: Top-k filtering
    if (config.top_k > 0 and @as(usize, @intCast(config.top_k)) < vocab_size) {
        activations.topK(working, @intCast(config.top_k), allocator);
    }

    // Step 4: Top-p (nucleus) filtering
    if (config.top_p > 0 and config.top_p < 1.0) {
        activations.topP(working, config.top_p, allocator);
    }

    // Step 5: Min-p filtering
    if (config.min_p > 0 and config.min_p < 1.0) {
        applyMinP(working, config.min_p);
    }

    // Step 6: Sample from the filtered distribution
    return activations.sampleFromProbs(working);
}

/// Apply repetition, frequency, and presence penalties to raw logits.
/// Repetition penalty: multiplicative scaling of logits for tokens in history.
/// Frequency penalty: additive penalty proportional to token count.
/// Presence penalty: additive penalty for any token that appeared.
fn applyRepetitionPenalties(logits: []f32, penalty_state: *const SamplingPenaltyState, config: GenerationConfig) void {
    var it = penalty_state.counts.iterator();
    while (it.next()) |entry| {
        const token_id = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (token_id >= logits.len) continue;

        // Repetition penalty (multiplicative, applied to raw logit)
        if (config.repetition_penalty != 1.0) {
            const logit = logits[token_id];
            // If logit > 0, divide by penalty; if logit <= 0, multiply by penalty
            // This matches llama.cpp / HuggingFace convention
            if (logit > 0) {
                logits[token_id] = logit / config.repetition_penalty;
            } else {
                logits[token_id] = logit * config.repetition_penalty;
            }
        }

        // Frequency penalty (additive, proportional to count)
        if (config.frequency_penalty != 0) {
            logits[token_id] -= config.frequency_penalty * @as(f32, @floatFromInt(count));
        }

        // Presence penalty (additive, binary — count > 0)
        if (config.presence_penalty != 0) {
            logits[token_id] -= config.presence_penalty;
        }
    }
}

/// Min-p filtering: zero out tokens where probability < min_p * max_probability.
fn applyMinP(probs: []f32, min_p: f32) void {
    // Find the maximum probability
    var max_prob: f32 = 0;
    for (probs) |p| {
        if (p > max_prob) max_prob = p;
    }

    const threshold = min_p * max_prob;
    for (probs) |*p| {
        if (p.* < threshold) p.* = 0;
    }
}

/// Format chat messages into a simple prompt string.
pub fn formatMessages(allocator: std.mem.Allocator, messages: []const Message) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    for (messages) |msg| {
        if (std.mem.eql(u8, msg.role, "system")) {
            try buf.appendSlice(allocator, "System: ");
        } else if (std.mem.eql(u8, msg.role, "user")) {
            try buf.appendSlice(allocator, "User: ");
        } else if (std.mem.eql(u8, msg.role, "assistant")) {
            try buf.appendSlice(allocator, "Assistant: ");
        }
        if (msg.content_parts) |parts| {
            for (parts) |part| {
                switch (part) {
                    .text => |text| try buf.appendSlice(allocator, text),
                    .image => try buf.appendSlice(allocator, "<start_of_image>"),
                    .audio => try buf.appendSlice(allocator, "<|audio|>"),
                }
            }
        } else {
            try buf.appendSlice(allocator, msg.content);
        }
        try buf.appendSlice(allocator, "\n\n");
    }
    try buf.appendSlice(allocator, "Assistant: ");

    return try allocator.dupe(u8, buf.items);
}

test "qwen image placeholders encode from config token ids" {
    const allocator = std.testing.allocator;
    const ByteTokenizer = struct {
        const Self = @This();

        fn tokenizer(self: *Self) tokenizer_mod.Tokenizer {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = tokenizer_mod.Tokenizer.VTable{
            .encode = encode,
            .encodeInto = encodeInto,
            .encodeForModel = encodeForModel,
            .encodeGeneration = encodeGeneration,
            .decode = decode,
            .specialTokens = specialTokens,
            .vocabSize = vocabSize,
            .deinit = deinit,
        };

        fn encode(_: *anyopaque, alloc: std.mem.Allocator, text: []const u8) ![]i32 {
            const ids = try alloc.alloc(i32, text.len);
            for (text, 0..) |ch, i| ids[i] = ch;
            return ids;
        }

        fn encodeInto(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
            const ids = try encode(ptr, alloc, text);
            defer alloc.free(ids);
            try out.appendSlice(alloc, ids);
        }

        fn encodeForModel(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8, max_length: usize) !tokenizer_mod.EncodeResult {
            return encodeGeneration(ptr, alloc, text, max_length, false);
        }

        fn encodeGeneration(ptr: *anyopaque, alloc: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) !tokenizer_mod.EncodeResult {
            const raw = try encode(ptr, alloc, text);
            defer alloc.free(raw);
            const prefix: usize = if (add_bos_token) 1 else 0;
            const total = @min(max_length, prefix + raw.len);
            const ids = try alloc.alloc(i32, max_length);
            const mask = try alloc.alloc(i32, max_length);
            var pos: usize = 0;
            if (add_bos_token and pos < total) {
                ids[pos] = 101;
                mask[pos] = 1;
                pos += 1;
            }
            for (raw) |id| {
                if (pos >= total) break;
                ids[pos] = id;
                mask[pos] = 1;
                pos += 1;
            }
            for (pos..max_length) |i| {
                ids[i] = 0;
                mask[i] = 0;
            }
            return .{ .ids = ids, .attention_mask = mask, .allocator = alloc };
        }

        fn decode(_: *anyopaque, alloc: std.mem.Allocator, ids: []const i32) ![]u8 {
            const text = try alloc.alloc(u8, ids.len);
            for (ids, 0..) |id, i| text[i] = @intCast(id);
            return text;
        }

        fn specialTokens(_: *anyopaque) tokenizer_mod.SpecialTokens {
            return .{ .cls_id = 101, .pad_id = 0 };
        }

        fn vocabSize(_: *anyopaque) usize {
            return 256;
        }

        fn deinit(_: *anyopaque) void {}
    };

    var byte_tokenizer = ByteTokenizer{};
    var encoded = try encodeQwenPromptWithImagePlaceholders(
        byte_tokenizer.tokenizer(),
        allocator,
        "User: <start_of_image>Read",
        128,
        true,
        "",
        .{
            .image_token_index = 248056,
            .boi_token_index = 248053,
            .eoi_token_index = 248054,
        },
    );
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 101), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 'U'), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 248053), encoded.ids[7]);
    try std.testing.expectEqual(@as(i32, 248056), encoded.ids[8]);
    try std.testing.expectEqual(@as(i32, 248054), encoded.ids[9]);
    try std.testing.expectEqual(@as(i32, 'R'), encoded.ids[10]);
}

fn collectImagesInPromptOrder(allocator: std.mem.Allocator, messages: []const Message) ![]const []const u8 {
    var images = std.ArrayListUnmanaged([]const u8).empty;
    errdefer images.deinit(allocator);

    for (messages) |msg| {
        if (msg.content_parts) |parts| {
            const msg_images = msg.image_bytes orelse &.{};
            for (parts) |part| {
                switch (part) {
                    .text => {},
                    .image => |image_idx| {
                        if (image_idx >= msg_images.len) return error.InvalidMessageImageIndex;
                        try images.append(allocator, msg_images[image_idx]);
                    },
                    .audio => {},
                }
            }
        } else if (msg.image_bytes) |msg_images| {
            for (msg_images) |image_bytes| try images.append(allocator, image_bytes);
        }
    }

    return try images.toOwnedSlice(allocator);
}

fn collectAudioInPromptOrder(allocator: std.mem.Allocator, messages: []const Message) ![]const []const u8 {
    var clips = std.ArrayListUnmanaged([]const u8).empty;
    errdefer clips.deinit(allocator);

    for (messages) |msg| {
        if (msg.content_parts) |parts| {
            const msg_audio = msg.audio_bytes orelse &.{};
            for (parts) |part| {
                switch (part) {
                    .text => {},
                    .image => {},
                    .audio => |audio_idx| {
                        if (audio_idx >= msg_audio.len) return error.InvalidMessageAudioIndex;
                        try clips.append(allocator, msg_audio[audio_idx]);
                    },
                }
            }
        } else if (msg.audio_bytes) |msg_audio| {
            for (msg_audio) |audio_bytes| try clips.append(allocator, audio_bytes);
        }
    }

    return try clips.toOwnedSlice(allocator);
}

// ── PJRT partition compilation ─────────────────────────────────────

/// Compile PJRT/HLO executors for eligible partitions and cache them
/// in the CacheEntry. On subsequent calls with the same entry, the
/// cached executors are reattached without recompilation.
fn attachPjrtExecutors(
    allocator: std.mem.Allocator,
    entry: *graph_mod.cache.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    dpp: *graph_mod.multi_executor.DevicePartitionPlan,
    cb: *const ops.ComputeBackend,
    pjrt_client: *anyopaque,
) !void {
    if (!build_options.enable_pjrt) return;

    const pjrt_lib = @import("pjrt");
    const client: *pjrt_lib.pjrt.Client = @ptrCast(@alignCast(pjrt_client));

    // First execution: compile PJRT executors for eligible partitions.
    if (entry.compiled_partitions == null) {
        var compiled = std.ArrayListUnmanaged(graph_mod.cache.CompiledPartition).empty;
        errdefer {
            for (compiled.items) |*cp| cp.executor.deinitExecutor();
            compiled.deinit(allocator);
        }

        for (dpp.base.partitions, 0..) |part, part_idx| {
            if (!isPartitionPjrtEligible(graph, part)) continue;

            const pjrt_exec = pjrt_executor_mod.createExecutor(
                allocator,
                graph,
                &dpp.base.partitions[part_idx],
                cb,
                cb, // host_backend = primary backend (has weights)
                client,
            ) catch |err| {
                // Fall back to per-node interpretation if compilation fails.
                std.log.warn("PJRT compilation failed for partition {d}: {s}", .{ part_idx, @errorName(err) });
                continue;
            };

            try compiled.append(allocator, .{
                .partition_idx = @intCast(part_idx),
                .executor = pjrt_exec.partitionExecutor().*,
            });
        }

        entry.compiled_partitions = if (compiled.items.len > 0)
            try compiled.toOwnedSlice(allocator)
        else
            null;
    }

    // Attach cached executors to the partition plan.
    if (entry.compiled_partitions) |cps| {
        for (cps) |*cp| {
            dpp.base.partitions[cp.partition_idx].executor = &cp.executor;
        }
        // Executors are owned by the cache, not the partition plan.
        dpp.base.owns_executors = false;
    }
}

const ml_graph = @import("ml").graph;
const TestGraph = ml_graph.Graph;
const TestBuilder = ml_graph.Builder;
const TestShape = ml_graph.Shape;
const TestNodeId = ml_graph.NodeId;

test "cached compiled_partitions attaches executors and sets owns_executors false" {
    // Simulates attaching cached partition executors:
    // given a CacheEntry with pre-populated compiled_partitions, verify
    // that partition executors are attached and owns_executors is false.
    const allocator = std.testing.allocator;

    var g = TestGraph.init(allocator);
    defer g.deinit();
    var b = TestBuilder.init(&g);
    const x = try b.parameter("x", TestShape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", TestShape.init(.f32, &.{4}));
    const normed = try b.rmsNorm(x, w, 4, 1e-5);
    const out = try b.gelu(normed);
    try g.markOutput(out);

    // Build a 2-partition plan: [0]=native, [1]=pjrt-eligible.
    const partition_mod = graph_mod.partition;
    const caps = [_]partition_mod.Capability{
        .{ .backend = .pjrt, .priority = 2, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 1, .supports = &partition_mod.supportsAll },
    };
    const plan = try partition_mod.partition(allocator, &g, &caps);

    // Wrap in DevicePartitionPlan.
    const dev_assign = try allocator.alloc(graph_mod.device_mesh.DeviceId, plan.partitions.len);
    @memset(dev_assign, 0);

    var dpp = graph_mod.multi_executor.DevicePartitionPlan{
        .base = plan,
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
    defer dpp.deinit();

    // Find a partition index with an eligible compiled backend.
    var compiled_pidx: ?u32 = null;
    for (dpp.base.partitions, 0..) |p, i| {
        if (isPartitionPjrtEligible(&g, p)) {
            compiled_pidx = @intCast(i);
            break;
        }
    }
    // There should be at least one eligible partition.
    try std.testing.expect(compiled_pidx != null);
    const pidx = compiled_pidx.?;

    // Simulate what executor attachment does on second call: pre-populate
    // the cache entry's compiled_partitions, then run the attach logic.
    var deinit_count: usize = 0;
    const MockCtx = struct {
        count: *usize,
        const vt = graph_mod.partition.PartitionExecutor.VTable{
            .execute = &noopExec,
            .deinit = &countDeinit,
        };
        fn noopExec(
            _: *anyopaque,
            _: []?ops.CT,
            _: []graph_mod.device_mesh.DeviceId,
            _: []const TestNodeId,
            _: graph_mod.device_mesh.DeviceId,
            _: graph_mod.partition.PartitionExecutor.ExecutionContext,
        ) anyerror!void {}
        fn countDeinit(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.count.* += 1;
        }
    };
    var mock = MockCtx{ .count = &deinit_count };

    const cps = try allocator.alloc(graph_mod.cache.CompiledPartition, 1);
    cps[0] = .{
        .partition_idx = pidx,
        .executor = .{ .ptr = @ptrCast(&mock), .vtable = &MockCtx.vt },
    };

    // Manually populate entry.compiled_partitions (simulating first-call cache).
    var entry = graph_mod.cache.CacheEntry{
        .key = .{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode },
        .graph = TestGraph.init(allocator), // dummy, not used by attach logic
        .last_used = 0,
        .compiled_partitions = cps,
    };
    defer entry.graph.deinit();

    // Before attach: no executor on the partition, owns_executors = true.
    try std.testing.expect(dpp.base.partitions[pidx].executor == null);
    try std.testing.expect(dpp.base.owns_executors);

    // Run the attach logic (same as the "Attach cached executors" block).
    if (entry.compiled_partitions) |cached_cps| {
        for (cached_cps) |*cp| {
            dpp.base.partitions[cp.partition_idx].executor = &cp.executor;
        }
        dpp.base.owns_executors = false;
    }

    // After attach: executor is set, owns_executors is false.
    try std.testing.expect(dpp.base.partitions[pidx].executor != null);
    try std.testing.expect(!dpp.base.owns_executors);

    // Cleanup: free compiled_partitions manually (mirrors freeCompiledPartitions).
    if (entry.compiled_partitions) |cached_cps| {
        for (cached_cps) |*cp| cp.executor.deinitExecutor();
        allocator.free(cached_cps);
        entry.compiled_partitions = null;
    }
    try std.testing.expectEqual(@as(usize, 1), deinit_count);
}

/// Check if all computation nodes in a partition are PJRT-eligible
/// (supported by supportsPjrt).
fn isPartitionPjrtEligible(
    graph: *const @import("ml").graph.Graph,
    part: graph_mod.partition.Partition,
) bool {
    const partition_mod = graph_mod.partition;
    for (part.node_ids) |nid| {
        const op = graph.node(nid).op;
        // Skip parameter/constant nodes — they're inputs, not compute ops.
        if (op == .parameter or op == .constant) continue;
        if (!partition_mod.supportsPjrt(op)) return false;
    }
    return part.node_ids.len > 0;
}
