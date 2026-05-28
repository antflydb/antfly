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
// 2. Native autoregressive decoding — for native/MLX backends using GPT arch forward pass
//
// The native path runs gpt_arch.forward() to get logits, samples the next token,
// and loops until EOS or max_tokens. Matches Go termite's TextGenerationPipeline.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ortgenai = if (build_options.enable_onnx) @import("../backends/ortgenai.zig") else struct {};
const decoder_bitnet_runtime = if (build_options.enable_mlx) @import("../backends/decoder_bitnet_runtime.zig") else struct {};
const tokenizer_mod = @import("termite_tokenizer");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const contracts = @import("../graph/backend_contracts.zig");
const ComputeBackend = ops.ComputeBackend;
const activations = @import("../backends/activations.zig");
const backends = @import("../backends/backends.zig");
const runtime = @import("../runtime/root.zig");
const jinja = @import("jinja");
const grammar_mod = @import("grammar.zig");
const gemma3_mm = @import("gemma3_multimodal.zig");
const gemma4_mm = @import("../architectures/gemma4_multimodal.zig");
const gemma4_mtp = @import("../architectures/gemma4_mtp.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const qwen2vl_mm = @import("qwen2vl_multimodal.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");
const hf_tokenizer = tokenizer_mod.hf;
const graph_mod = @import("../graph/root.zig");
const pjrt_executor_mod = if (build_options.enable_pjrt) @import("../graph/pjrt_executor.zig") else struct {};

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
    /// Grammar constraint mode. null = no constraint, "json" = JSON mode.
    grammar: ?[]const u8 = null,
    /// Path to a smaller draft model for speculative decoding. When set, the
    /// draft model generates `speculative_k` candidate tokens that are then
    /// verified by the target model in a single forward pass.
    draft_model: ?[]const u8 = null,
    /// Number of candidate tokens the draft model proposes per speculation
    /// round (default 4).
    speculative_k: u32 = 4,
    /// KV cache quantization format override. null = auto-select based on backend.
    cache_dtype: ?[]const u8 = null,
    /// KV cache compaction ratio after prefill. null = no compaction.
    /// 0.02 = 50x compression, 0.1 = 10x compression.
    cache_compaction_ratio: ?f32 = null,
};

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
            .template = try jinja.Template.init(allocator, source),
            .bos_token = bos_token,
            .eos_token = eos_token,
            .unk_token = unk_token,
            .pad_token = pad_token,
        };
    }

    pub fn apply(self: *const ChatTemplate, allocator: std.mem.Allocator, messages: []const Message, add_generation_prompt: bool) ![]u8 {
        // Convert Message into jinja.ChatMessage, preserving structured
        // image/text parts when available for multimodal chat templates.
        const chat_msgs = try allocator.alloc(jinja.ChatMessage, messages.len);
        defer allocator.free(chat_msgs);
        for (messages, 0..) |m, i| {
            var parts: ?[]const jinja.ChatContentPart = null;
            if (m.content_parts) |message_parts| {
                const chat_parts = try allocator.alloc(jinja.ChatContentPart, message_parts.len);
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
        defer for (chat_msgs) |msg| if (msg.parts) |parts| allocator.free(parts);

        var ctx = try jinja.chatTemplateContext(allocator, chat_msgs, .{
            .add_generation_prompt = add_generation_prompt,
            .bos_token = self.bos_token,
            .eos_token = self.eos_token,
            .unk_token = self.unk_token,
            .pad_token = self.pad_token,
        });

        const result = try self.template.render(allocator, &ctx);
        return try allocator.dupe(u8, result);
    }

    pub fn deinit(self: *ChatTemplate) void {
        self.template.deinit();
    }
};

pub const GenerationResult = struct {
    text: []const u8,
    token_ids: ?[]i32 = null,
    prompt_tokens: usize = 0,
    tokens_used: usize,
    finish_reason: []const u8,
    speculative: ?SpeculativeDecodeStats = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerationResult) void {
        self.allocator.free(self.text);
        if (self.token_ids) |ids| self.allocator.free(ids);
    }
};

pub const SpeculativeDecodeStats = struct {
    rounds: usize = 0,
    drafted_tokens: usize = 0,
    matched_draft_tokens: usize = 0,
    accepted_tokens: usize = 0,
    correction_tokens: usize = 0,
    bonus_tokens: usize = 0,

    pub fn rejectedDraftTokens(self: SpeculativeDecodeStats) usize {
        return self.drafted_tokens -| self.matched_draft_tokens;
    }
};

/// Streaming token callback. Called with each decoded text delta.
/// Return `true` to continue generation, `false` to stop early.
pub const TokenCallback = *const fn (ctx: *anyopaque, token_text: []const u8) bool;

pub const KvView = struct {
    sequence_id: runtime.kv.manager.SequenceId,
    pool_id: runtime.kv.block.KvPoolId,
    logical_block_count: usize,
    tail_tokens: u16,
    token_count: usize,
    position_offset: usize,
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

    fn deinit(self: *SamplingPenaltyState, allocator: std.mem.Allocator) void {
        self.counts.deinit(allocator);
        self.* = .{};
    }

    fn seedFromHistory(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_history: []const i64) !void {
        for (token_history) |token_id| try self.noteToken(allocator, token_id);
    }

    fn noteToken(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_id: i64) !void {
        if (token_id < 0) return;
        const entry = try self.counts.getOrPut(allocator, @intCast(token_id));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }

    fn noteTokens(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_ids: []const i64) !void {
        for (token_ids) |token_id| try self.noteToken(allocator, token_id);
    }

    fn clone(self: *const SamplingPenaltyState, allocator: std.mem.Allocator) !SamplingPenaltyState {
        var copy = SamplingPenaltyState{};
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

fn enableMlxGreedyDeviceDecodeDebug() bool {
    return getenvBool("TERMITE_MLX_GREEDY_DEVICE_DECODE");
}

fn enableMlxDeviceTokenHandoffDebug() bool {
    return getenvBool("TERMITE_MLX_DEVICE_TOKEN_HANDOFF");
}

fn enableMlxRawMetalWholeTokenDebug() bool {
    return getenvBool("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN");
}

fn enableGenerationStageDebug() bool {
    return getenvBool("TERMITE_GEN_STAGE_DEBUG");
}

fn enableGemma4MtpDebug() bool {
    return getenvBool("TERMITE_DEBUG_GEMMA4_MTP");
}

fn traceGraphExecutorOutputs() bool {
    return getenvBool("TERMITE_GRAPH_EXECUTOR_TRACE_OUTPUTS");
}

fn debugGenerationStage(comptime fmt: []const u8, args: anytype) void {
    if (!enableGenerationStageDebug()) return;
    std.debug.print("gen_debug: " ++ fmt ++ "\n", args);
}

fn debugGemma4Mtp(comptime fmt: []const u8, args: anytype) void {
    if (!enableGemma4MtpDebug()) return;
    std.debug.print("gemma4_mtp_debug: " ++ fmt ++ "\n", args);
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
}

fn isPureGreedyConfig(config: GenerationConfig) bool {
    return config.temperature <= 0 and !hasSamplingPenalties(config);
}

fn hasSamplingPenalties(config: GenerationConfig) bool {
    return config.repetition_penalty != 1.0 or
        config.frequency_penalty != 0 or
        config.presence_penalty != 0;
}

pub const DecoderRuntimeDebugStats = struct {
    forward_attempts: u64 = 0,
    flag_disabled: u64 = 0,
    backend_not_mlx: u64 = 0,
    scheduler_blocked: u64 = 0,
    graph_blocked: u64 = 0,
    first_token_blocked: u64 = 0,
    kv_missing: u64 = 0,
    non_greedy: u64 = 0,
    grammar_blocked: u64 = 0,
    prepare_attempts: u64 = 0,
    prepare_flag_disabled: u64 = 0,
    prepare_backend_not_mlx: u64 = 0,
    prepare_kv_missing: u64 = 0,
    prepare_scheduler_blocked: u64 = 0,
    prepare_graph_blocked: u64 = 0,
    prepare_arch_blocked: u64 = 0,
    prepare_model_blocked: u64 = 0,
    prepare_calls: u64 = 0,
    input_attempts: u64 = 0,
    input_flag_disabled: u64 = 0,
    input_backend_not_mlx: u64 = 0,
    input_kv_missing: u64 = 0,
    input_arch_blocked: u64 = 0,
    input_model_blocked: u64 = 0,
    input_seq_empty: u64 = 0,
    input_successes: u64 = 0,
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
    sequence_id: ?runtime.kv.manager.SequenceId = null,
    pool_id: ?runtime.kv.block.KvPoolId = null,
    total_tokens: usize = 0,
    kv_view: ?KvView = null,
    kv_compacted: bool = false,
    kv_block_ids: std.ArrayListUnmanaged(runtime.kv.block.KvBlockId) = .empty,
    moe_runtime: runtime.moe.runtime.MoeRuntime,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    qwen35_linear_cache: ?gpt_arch.Qwen35LinearCache = null,
    deepseek_v4_compressed_cache: ?gpt_arch.DeepSeekV4CompressedCache = null,
    force_full_recompute: bool = false,

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

    pub fn isPaged(self: *const NativeDecodeState) bool {
        return self.kv_manager != null and !self.force_full_recompute;
    }

    pub fn isKvCompacted(self: *const NativeDecodeState) bool {
        return self.kv_compacted;
    }

    pub fn configureForGptConfig(self: *NativeDecodeState, config: gpt_mod.Config) void {
        self.force_full_recompute = false;
        if (!requiresDeepSeekV4CompressedCache(config)) self.clearDeepSeekV4CompressedCache();
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

    pub fn ensureAttached(self: *NativeDecodeState) !void {
        if (!self.isPaged()) return;
        if (self.sequence_id != null) return;
        self.sequence_id = try self.kv_manager.?.attachSequence(self.pool_id orelse return error.InvalidPoolId);
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
            .logical_blocks = self.kv_block_ids.items,
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
        const table = manager.blockTable(sequence_id) orelse {
            self.kv_block_ids.clearRetainingCapacity();
            return;
        };
        try self.kv_block_ids.resize(self.allocator, table.blocks.items.len);
        @memcpy(self.kv_block_ids.items, table.blocks.items);
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
            const current_kv_tokens = if (self.kv_view) |view|
                @min(view.token_count + 1, self.total_tokens)
            else
                @min(@as(usize, 1), self.total_tokens);
            self.setPagedKvView(current_kv_tokens, self.total_tokens - current_kv_tokens);
            return;
        }
        const keep_tokens = self.kvSlidingWindowTokens() orelse self.total_tokens;
        const kv_tokens = @min(self.total_tokens, keep_tokens);
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
        if (self.kv_manager) |manager| {
            if (self.sequence_id) |sequence_id| {
                manager.releaseSequence(sequence_id) catch {};
            }
        }
        self.moe_runtime.deinit();
        if (self.qwen35_linear_cache) |*cache| cache.deinit();
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
            try self.ensureAttached();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(token_count));
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
            try self.ensureAttached();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(token_count));
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
            try self.ensureAttached();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, 1);
            _ = try self.kv_manager.?.trimSequenceToSlidingWindow(self.sequence_id.?);
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
            try self.ensureAttached();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(count));
            _ = try self.kv_manager.?.trimSequenceToSlidingWindow(self.sequence_id.?);
            if (count > 0) {
                const kv_tokens = if (self.kv_compacted)
                    @min(if (self.kv_view) |view| view.token_count + count else count, self.total_tokens)
                else blk: {
                    const keep_tokens = self.kvSlidingWindowTokens() orelse self.total_tokens;
                    break :blk @min(self.total_tokens, keep_tokens);
                };
                self.setPagedKvView(kv_tokens, self.total_tokens - kv_tokens);
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
            const manager = self.kv_manager.?;
            if (self.sequence_id) |seq_id| {
                const removed = try manager.truncateSequence(seq_id, count);
                const prior_kv_tokens = if (self.kv_view) |view| view.token_count else 0;
                const kv_tokens = prior_kv_tokens - @min(prior_kv_tokens, removed);
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
        if (self.deepseek_v4_compressed_cache != null) return error.DeepSeekV4CompressedKvCompactionNotSupported;
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

        // Swap: release old sequence, adopt new one.
        try manager.releaseSequence(seq_id);
        self.sequence_id = new_seq_id;

        // Mark compacted so sliding window trimming is skipped.
        const new_state = try manager.sequenceMut(new_seq_id);
        new_state.compacted = true;
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
        if (disablePagedKvDebug() or self.force_full_recompute) {
            return .{
                .attention_mode = .full_recompute,
                .total_sequence_len = seq_len,
                .query_sequence_len = seq_len,
                .kv_sequence_len = seq_len,
                .kv_position_offset = 0,
                .moe_runtime = &self.moe_runtime,
                .qwen35_linear_cache = if (self.qwen35_linear_cache) |*cache| cache else null,
                .deepseek_v4_compressed_cache = if (self.deepseek_v4_compressed_cache) |*cache| cache else null,
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
            .moe_runtime = &self.moe_runtime,
            .qwen35_linear_cache = if (self.qwen35_linear_cache) |*cache| cache else null,
            .deepseek_v4_compressed_cache = if (self.deepseek_v4_compressed_cache) |*cache| cache else null,
            .kv_cache = if (self.kvView()) |view|
                .{
                    .sequence_id = view.sequence_id,
                    .pool_id = view.pool_id,
                    .logical_block_count = view.logical_block_count,
                    .tail_tokens = view.tail_tokens,
                    .position_offset = view.position_offset,
                    .logical_blocks = view.logical_blocks,
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
                .logical_blocks = ctx.kv_cache.?.logical_blocks,
            },
            .kv_manager = ctx.kv_manager.?,
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
                .logical_blocks = ctx.kv_cache.?.logical_blocks,
            },
            .kv_manager = ctx.kv_manager.?,
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

        // Format messages into a prompt
        const prompt = if (self.prompt_override) |override|
            try self.allocator.dupe(u8, override)
        else if (self.chat_template) |ct|
            try ct.apply(self.allocator, messages, true)
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
                .prompt_tokens = 0,
                .tokens_used = result.tokens_used,
                .finish_reason = result.finish_reason,
                .allocator = result.allocator,
            };
        }

        const result = try ortgenai.generate(self.allocator, self.model, prompt, gen_opts);
        return .{
            .text = result.text,
            .token_ids = null,
            .prompt_tokens = 0,
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

        // Multimodal streaming not supported yet — fall back
        if (messagesHaveImages(messages)) {
            return self.generate(messages, config);
        }

        const prompt = if (self.prompt_override) |override|
            try self.allocator.dupe(u8, override)
        else if (self.chat_template) |ct|
            try ct.apply(self.allocator, messages, true)
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
            .prompt_tokens = 0,
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
    print_timing: bool = false,
    model_dir: ?[]const u8 = null,
    artifact_dir: ?[]const u8 = null,
    gguf_projector_path: ?[]const u8 = null,
    decode_state: ?*NativeDecodeState = null,
    scheduler: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = null,
    scheduler_lease: ?*runtime.scheduler.native_generate.Lease = null,
    /// Optional smaller draft model for speculative decoding. When set
    /// together with `GenerationConfig.draft_model`, the generate loop
    /// proposes K tokens with the draft and verifies them against the
    /// target (this pipeline) in a single forward pass.
    draft_cb: ?ComputeBackend = null,
    draft_gpt_config: ?gpt_mod.Config = null,
    draft_decode_state: ?*NativeDecodeState = null,
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

    const PendingDecodeBatchWork = struct {
        allocator: std.mem.Allocator,
        decode_state: *NativeDecodeState,
        token_id: i64,
        seq_len: usize,
        logits: ?[]f32 = null,
        failure: ?anyerror = null,
        ready: bool = false,
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
        ready: bool = false,
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

        // Format prompt
        const prompt = if (self.prompt_override) |override|
            try allocator.dupe(u8, override)
        else if (self.chat_template) |ct|
            try ct.apply(allocator, messages, true)
        else
            try formatMessages(allocator, messages);
        const formatted_prompt_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        defer allocator.free(prompt);

        // Tokenize
        var encoded = try encodePromptForGeneration(self.tokenizer, allocator, prompt, 2048, self.add_bos_token, self.bos_token);
        const encoded_prompt_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        defer encoded.deinit();

        var actual_prompt_tokens: usize = 0;
        while (actual_prompt_tokens < encoded.attention_mask.len and encoded.attention_mask[actual_prompt_tokens] != 0) : (actual_prompt_tokens += 1) {}
        if (actual_prompt_tokens == 0) return error.EmptyPrompt;
        debugGenerationStage(
            "encoded prompt chars={d} actual_prompt_tokens={d}",
            .{ prompt.len, actual_prompt_tokens },
        );

        const has_images = messagesHaveImages(messages);
        const has_audio = messagesHaveAudio(messages);

        // --- Speculative decoding path ---
        // Use the draft model to propose K tokens, then verify them against
        // the target. When grammar constraints are active, draft proposals
        // remain unconstrained but target-side verification still applies the
        // grammar at each accepted position.
        const use_speculative = self.draft_cb != null and
            self.draft_gpt_config != null;
        if ((has_images or has_audio) and use_speculative) return error.MultimodalSpeculativeDecodingNotSupported;
        if (use_speculative and self.speculativeUsesDeepSeekV4CompressedCache()) return error.DeepSeekV4CompressedSpeculativeDecodingNotSupported;

        var prepared_multimodal_prompt: ?gemma3_mm.PreparedPrompt = null;
        defer if (prepared_multimodal_prompt) |*prepared| prepared.deinit(&self.cb);

        const prompt_token_count = blk: {
            if (!has_images and !has_audio) break :blk actual_prompt_tokens;
            debugGenerationStage("multimodal prompt begin has_images={} has_audio={}", .{ has_images, has_audio });
            const images = try collectImagesInPromptOrder(allocator, messages);
            defer allocator.free(images);
            const audio_clips = try collectAudioInPromptOrder(allocator, messages);
            defer allocator.free(audio_clips);
            debugGenerationStage("multimodal collected images={d} audio={d}", .{ images.len, audio_clips.len });

            if (self.gguf_projector_path) |projector_path| {
                if (projector_format_mod.isTermite(try projector_format_mod.detectPath(allocator, projector_path))) {
                    if (has_audio) return error.NativeAudioGenerationNotImplemented;
                    if (!self.gpt_config.isMultimodal()) return error.InvalidModelForGeneration;
                    const model_dir = self.model_dir orelse return error.MissingModelDirForMultimodal;
                    const expanded_prompt = try gemma3_mm.expandPromptText(allocator, prompt, self.gpt_config, images.len);
                    defer allocator.free(expanded_prompt);
                    var expanded_encoded = try encodePromptForGeneration(self.tokenizer, allocator, expanded_prompt, 4096, self.add_bos_token, self.bos_token);
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
                    const max_expanded_tokens = @max(@as(usize, 4096), expanded_prompt.len);
                    var expanded_encoded = try encodePromptForGeneration(self.tokenizer, allocator, expanded_prompt, max_expanded_tokens, self.add_bos_token, self.bos_token);
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
                    const max_expanded_tokens = @max(@as(usize, 4096), prompt.len + images.len * 3);
                    debugGenerationStage("qwen3.5 multimodal encode prompt max_tokens={d}", .{max_expanded_tokens});
                    var qwen_encoded = try encodeQwenPromptWithImagePlaceholders(
                        self.tokenizer,
                        allocator,
                        prompt,
                        max_expanded_tokens,
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
                    var expanded_encoded = try encodePromptForGeneration(self.tokenizer, allocator, expanded_prompt, 4096, self.add_bos_token, self.bos_token);
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
            break :blk prepared_multimodal_prompt.?.token_ids.len;
        };

        // Build token sequence. Add slack only when speculative decoding is
        // active; the bonus token can write one position past max_tokens.
        const max_tokens: usize = @intCast(@max(config.max_tokens, 1));
        const spec_slack: usize = if (use_speculative and config.speculative_k > 0) 1 else 0;
        const max_seq = prompt_token_count + max_tokens + spec_slack;
        var token_ids = try allocator.alloc(i64, max_seq);
        defer allocator.free(token_ids);
        if (prepared_multimodal_prompt) |prepared| {
            @memcpy(token_ids[0..prepared.token_ids.len], prepared.token_ids);
        } else {
            for (0..actual_prompt_tokens) |i| token_ids[i] = @intCast(encoded.ids[i]);
        }
        var seq_len = prompt_token_count;
        debugGenerationStage(
            "starting prefill prompt_token_count={d} seq_len={d} multimodal={}",
            .{ prompt_token_count, seq_len, prepared_multimodal_prompt != null },
        );

        const runtime_prepare_started_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        const runtime_prepared = try self.prepareCompiledGenerationRuntime(prompt_token_count);
        const prefill_started_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        if (runtime_prepared) {
            debugGenerationStage("prepared compiled generation runtime prompt_token_count={d}", .{prompt_token_count});
        }

        // Prefill
        const prefill_output = if (prepared_multimodal_prompt) |*prepared|
            PrefillOutput{ .last_logits = try self.executePreparedMultimodalPrefill(prepared, seq_len, decode_state) }
        else
            try self.executePrefill(token_ids[0..seq_len], seq_len, decode_state, config, !use_speculative);
        var prefill_last_logits = prefill_output.last_logits;
        var prefill_greedy_token = prefill_output.greedy_token;
        const finished_prefill_at = if (self.io) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
        defer if (prefill_last_logits) |logits| allocator.free(logits);
        debugGenerationStage(
            "finished prefill seq_len={d} cached_logits={} greedy_token={}",
            .{ seq_len, prefill_last_logits != null, prefill_greedy_token != null },
        );

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
        const stream_enabled = on_token_fn != null and on_token_ctx != null;
        var emitted_text: []u8 = if (stream_enabled) try allocator.dupe(u8, "") else &.{};
        defer if (stream_enabled) allocator.free(emitted_text);
        var penalty_state = SamplingPenaltyState{};
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

        if (use_speculative) {
            const draft_cb = self.draft_cb.?;
            const draft_gpt_config = self.draft_gpt_config.?;
            const use_gemma4_mtp = draft_gpt_config.gemma4_mtp_assistant;
            var mtp_last_activation: ?[]f32 = null;
            defer if (mtp_last_activation) |activation| allocator.free(activation);

            // Create a temporary draft pipeline (borrows self's allocator/tokenizer)
            var draft_fallback_state = NativeDecodeState.initContiguous(allocator);
            defer draft_fallback_state.deinit();
            const draft_ds = self.draft_decode_state orelse &draft_fallback_state;
            var draft_runtime = BorrowedDecodeStateRuntime.init(draft_ds);
            var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);

            // Prefill draft model with the same prompt
            try draft_runtime.notePrefill(seq_len);

            var draft_pipeline = NativeGenerationPipeline{
                .allocator = allocator,
                .cb = draft_cb,
                .gpt_config = draft_gpt_config,
                .tokenizer = self.tokenizer,
                .artifact_dir = self.artifact_dir,
                .graph_cache = self.graph_cache,
                .compiled_partition_backend = self.compiled_partition_backend,
                .pjrt_client = self.pjrt_client,
            };

            // Prefill an ordinary draft model. Gemma 4 MTP assistants are not
            // standalone decoders: they are seeded from target activations and
            // target KV during each draft round.
            if (!use_gemma4_mtp) {
                const draft_ctx = draft_runtime.makeDecodeContext(seq_len, seq_len);
                const draft_prefill_logits = try draft_pipeline.forwardAllLogits(token_ids[0..seq_len], 1, seq_len, &draft_ctx);
                allocator.free(draft_prefill_logits);
            }

            // Also prefill the target if we haven't yet (non-chunked path)
            if (prefill_last_logits == null) {
                const target_ctx = decode_runtime.makeDecodeContext(seq_len, seq_len);
                if (use_gemma4_mtp) {
                    var target_prefill = try self.forwardAllLogitsAndHiddenHost(token_ids[0..seq_len], 1, seq_len, &target_ctx);
                    defer target_prefill.deinit();
                    prefill_last_logits = try allocator.dupe(
                        f32,
                        target_prefill.logits[(seq_len - 1) * vocab_size ..][0..vocab_size],
                    );
                    const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
                    mtp_last_activation = try allocator.dupe(
                        f32,
                        target_prefill.hidden[(seq_len - 1) * hidden_size ..][0..hidden_size],
                    );
                } else {
                    const target_prefill_logits = try self.forwardAllLogits(token_ids[0..seq_len], 1, seq_len, &target_ctx);
                    defer allocator.free(target_prefill_logits);
                    prefill_last_logits = try allocator.dupe(
                        f32,
                        target_prefill_logits[(seq_len - 1) * vocab_size ..][0..vocab_size],
                    );
                }
            } else if (use_gemma4_mtp and mtp_last_activation == null) {
                const target_ctx = decode_runtime.makeDecodeContext(seq_len, 1);
                var target_last = try self.forwardAllLogitsAndHiddenHost(token_ids[seq_len - 1 .. seq_len], 1, seq_len, &target_ctx);
                defer target_last.deinit();
                if (target_last.rows != 1 or target_last.hidden.len != self.gpt_config.hidden_size) return error.InvalidTensorShape;
                mtp_last_activation = try allocator.dupe(f32, target_last.hidden);
            }

            // Use prefill last logits for the first token
            const first_outcome = try self.sampleNextToken(
                prefill_last_logits.?,
                config,
                &penalty_state,
                if (token_table) |*tt| tt else null,
                &json_grammar,
                if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
            );
            const first_token = first_outcome.token;
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
                    &emitted_text,
                    on_token_fn.?,
                    on_token_ctx.?,
                );
                if (!keep_streaming) {
                    finish_reason = "stop";
                }
            }

            const first_is_eos = self.gpt_config.eos_token_id >= 0 and
                @as(i32, @intCast(first_token)) == self.gpt_config.eos_token_id;
            if (first_is_eos or first_outcome.grammar_complete) {
                finish_reason = "stop";
            }

            if (use_gemma4_mtp and !first_is_eos and !first_outcome.grammar_complete) {
                const materialized_hidden = try self.materializeAcceptedTokenKvAndReturnHidden(
                    token_ids,
                    seq_len,
                    decode_state,
                );
                if (mtp_last_activation) |old| allocator.free(old);
                mtp_last_activation = materialized_hidden;
            }

            // Speculative decode loop
            if (!first_is_eos and !first_outcome.grammar_complete) {
                while (tokens_generated < max_tokens) {
                    const remaining = max_tokens - tokens_generated;
                    const step_k = @min(config.speculative_k, @as(u32, @intCast(remaining)));

                    const result = if (use_gemma4_mtp)
                        try self.speculativeDecodeGemma4Mtp(
                            &draft_pipeline,
                            token_ids,
                            &seq_len,
                            decode_state,
                            config,
                            step_k,
                            &penalty_state,
                            if (token_table) |*tt| tt else null,
                            &json_grammar,
                            if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                            &mtp_last_activation,
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
                            &penalty_state,
                            if (token_table) |*tt| tt else null,
                            &json_grammar,
                            if (gbnf_grammar != null) &(gbnf_grammar.?) else null,
                        );

                    speculative_stats.rounds += 1;
                    speculative_stats.drafted_tokens += result.drafted;
                    speculative_stats.matched_draft_tokens += result.matched_drafts;
                    speculative_stats.accepted_tokens += result.accepted;
                    speculative_stats.correction_tokens += @intFromBool(result.correction_added);
                    speculative_stats.bonus_tokens += @intFromBool(result.had_bonus);

                    tokens_generated += result.accepted;
                    if (stream_enabled and result.accepted > 0) {
                        const keep_streaming = try self.emitDecodedDelta(
                            token_ids[prompt_token_count..seq_len],
                            &emitted_text,
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

                    if (result.accepted == 0) break; // safety valve
                }
            }
        }

        // Standard autoregressive loop (skipped when speculative decoding was used above)
        if (!use_speculative) {
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
                max_tokens,
                prompt_token_count,
                if (stream_enabled) on_token_fn else null,
                if (stream_enabled) on_token_ctx else null,
                if (stream_enabled) &emitted_text else null,
            );
            tokens_generated = decode_result.tokens_generated;
            finish_reason = decode_result.finish_reason;
            debugGenerationStage(
                "standard decode returned tokens_generated={d} finish_reason={s} seq_len={d}",
                .{ tokens_generated, finish_reason, seq_len },
            );
        }

        // Decode only the generated tokens
        const gen_start = prompt_token_count;
        const gen_ids = try allocator.alloc(i32, seq_len - gen_start);
        for (0..gen_ids.len) |i| gen_ids[i] = @intCast(token_ids[gen_start + i]);

        const text = try self.tokenizer.decode(allocator, gen_ids);
        if (self.print_timing and self.io != null) {
            const finished_generate_at = std.Io.Timestamp.now(self.io.?, .awake);
            std.debug.print(
                "generate_timing_ms: prompt_format={d} tokenize={d} runtime_prepare={d} prefill={d} decode={d} total={d}\n",
                .{
                    timestampDurationMillis(started_at, formatted_prompt_at),
                    timestampDurationMillis(formatted_prompt_at, encoded_prompt_at),
                    timestampDurationMillis(runtime_prepare_started_at, prefill_started_at),
                    timestampDurationMillis(prefill_started_at, finished_prefill_at),
                    timestampDurationMillis(finished_prefill_at, finished_generate_at),
                    timestampDurationMillis(started_at, finished_generate_at),
                },
            );
        }
        return .{
            .text = text,
            .token_ids = gen_ids,
            .prompt_tokens = prompt_token_count,
            .tokens_used = tokens_generated,
            .finish_reason = finish_reason,
            .speculative = if (use_speculative) speculative_stats else null,
            .allocator = allocator,
        };
    }

    const PrefillOutput = struct {
        last_logits: ?[]f32 = null,
        greedy_token: ?usize = null,
    };

    /// Run the prefill phase: process prompt tokens through the model,
    /// either chunked (for paged KV) or in one pass. Returns the logits
    /// for the last prompt position, or null if handled by the scheduler.
    fn executePrefill(
        self: *NativeGenerationPipeline,
        prompt_ids: []const i64,
        seq_len: usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        allow_resident_greedy_token: bool,
    ) !PrefillOutput {
        const allocator = self.allocator;
        var prefill_last_logits: ?[]f32 = null;
        debugGenerationStage(
            "executePrefill enter seq_len={d} paged={} scheduler={} compiled_whole_model={}",
            .{
                seq_len,
                decode_state.isPaged(),
                self.scheduler != null,
                self.compiled_partition_backend != null and self.compiled_attachment_target == .whole_model and self.graph_cache != null,
            },
        );
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (self.gpt_config.isQwen35() and decode_state.isPaged()) {
            try decode_state.ensureQwen35LinearCache(self.gpt_config);
            decode_state.resetQwen35LinearCache();
        }

        if (self.scheduler) |scheduler| {
            if (self.scheduler_lease) |lease| {
                scheduler.notePrefillProgress(lease, 0, seq_len);
            }
        }

        if (self.compiled_partition_backend != null and self.compiled_attachment_target == .whole_model and self.graph_cache != null) {
            debugGenerationStage("executePrefill whole-model fast path seq_len={d}", .{seq_len});
            const decode_context = try decode_runtime.preparePrefill(seq_len, seq_len);
            if (allow_resident_greedy_token) {
                if (try self.forwardGreedyCompiledModelToken(
                    prompt_ids,
                    1,
                    seq_len,
                    &decode_context,
                    config,
                    false,
                )) |token| {
                    if (self.scheduler) |scheduler| {
                        if (self.scheduler_lease) |lease| {
                            scheduler.notePrefillProgress(lease, seq_len, seq_len);
                            scheduler.finishTurn(lease, .prefill);
                        }
                    }
                    return .{ .greedy_token = token };
                }
            }
            prefill_last_logits = try self.forwardLastLogits(prompt_ids, 1, seq_len, &decode_context);
            if (self.scheduler) |scheduler| {
                if (self.scheduler_lease) |lease| {
                    scheduler.notePrefillProgress(lease, seq_len, seq_len);
                    scheduler.finishTurn(lease, .prefill);
                }
            }
            return .{ .last_logits = prefill_last_logits };
        }

        if (decode_state.isPaged() and seq_len > 1) {
            var current_chunk_size = blk: {
                const scheduler_chunk = if (self.scheduler_lease) |lease| lease.prefill_chunk_size else 0;
                if (config.prefill_chunk_size > 0 and scheduler_chunk > 0) {
                    break :blk @min(config.prefill_chunk_size, scheduler_chunk);
                }
                if (config.prefill_chunk_size > 0) break :blk config.prefill_chunk_size;
                if (scheduler_chunk > 0) break :blk scheduler_chunk;
                break :blk seq_len;
            };
            current_chunk_size = @max(@min(current_chunk_size, seq_len), 1);
            var processed: usize = 0;
            while (processed < seq_len) {
                const scheduler_chunk = if (self.scheduler_lease) |lease| lease.prefill_chunk_size else current_chunk_size;
                const chunk_size = @max(@min(current_chunk_size, scheduler_chunk), 1);
                const chunk_end = @min(seq_len, processed + chunk_size);
                const chunk = prompt_ids[processed..chunk_end];
                debugGenerationStage(
                    "executePrefill chunk start processed={d} chunk_len={d} chunk_end={d} current_chunk_size={d}",
                    .{ processed, chunk.len, chunk_end, current_chunk_size },
                );
                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        if (self.io) |io| {
                            prefill_last_logits = self.runScheduledPrefillBatch(scheduler, lease, io, decode_state, chunk, chunk_end, chunk.len, chunk_end == seq_len) catch |err| {
                                if (err == error.MemoryBudgetExceeded and chunk_size > 1) {
                                    current_chunk_size = @max(chunk_size / 2, 1);
                                    continue;
                                }
                                return err;
                            };
                            processed = chunk_end;
                            scheduler.notePrefillProgress(lease, processed, seq_len);
                            continue;
                        }
                    }
                }

                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        if (self.io) |io| scheduler.awaitTurn(lease, .prefill, io);
                    }
                }
                try decode_runtime.appendPrefillChunk(chunk.len);
                const decode_context = decode_runtime.makeDecodeContext(chunk_end, chunk.len);
                const logits = self.forwardAllLogits(chunk, 1, chunk_end, &decode_context) catch |err| {
                    if (err == error.MemoryBudgetExceeded and chunk_size > 1) {
                        current_chunk_size = @max(chunk_size / 2, 1);
                        continue;
                    }
                    return err;
                };
                defer allocator.free(logits);
                debugGenerationStage(
                    "executePrefill chunk complete processed={d} chunk_end={d} logits_len={d}",
                    .{ processed, chunk_end, logits.len },
                );
                if (chunk_end == seq_len) {
                    prefill_last_logits = try allocator.dupe(f32, logits[(chunk.len - 1) * self.gpt_config.vocab_size ..][0..self.gpt_config.vocab_size]);
                    debugGenerationStage(
                        "executePrefill captured last logits vocab_size={d}",
                        .{self.gpt_config.vocab_size},
                    );
                }
                processed = chunk_end;
                if (self.scheduler) |scheduler| {
                    if (self.scheduler_lease) |lease| {
                        scheduler.notePrefillProgress(lease, processed, seq_len);
                        scheduler.finishTurn(lease, .prefill);
                    }
                }
            }
        } else {
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
            "executePrefill exit cached_logits={}",
            .{prefill_last_logits != null},
        );
        return .{ .last_logits = prefill_last_logits };
    }

    fn executePreparedMultimodalPrefill(
        self: *NativeGenerationPipeline,
        prepared: *gemma3_mm.PreparedPrompt,
        seq_len: usize,
        decode_state: *NativeDecodeState,
    ) !?[]f32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
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
                    scheduler.notePrefillProgress(lease, seq_len, seq_len);
                    scheduler.finishTurn(lease, .prefill);
                } else {
                    scheduler.awaitTurn(lease, .prefill, self.io.?);
                    _ = try decode_runtime.preparePrefill(seq_len, seq_len);
                    scheduler.notePrefillProgress(lease, seq_len, seq_len);
                    scheduler.finishTurn(lease, .prefill);
                }
            } else {
                _ = try decode_runtime.preparePrefill(seq_len, seq_len);
            }
        } else {
            _ = try decode_runtime.preparePrefill(seq_len, seq_len);
        }

        const input_embeddings = prepared.input_embeddings orelse return error.InvalidPreparedPrompt;
        prepared.input_embeddings = null;
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
        return try self.allocator.dupe(f32, logits[(seq_len - 1) * self.gpt_config.vocab_size ..][0..self.gpt_config.vocab_size]);
    }

    const DecodeResult = struct {
        tokens_generated: usize,
        finish_reason: []const u8,
    };

    const DeviceDecodeOutcome = struct {
        token: usize,
        token_tensor: ?ops.CT = null,
    };

    fn emitDecodedDelta(
        self: *NativeGenerationPipeline,
        generated_token_ids: []const i64,
        emitted_text: *[]u8,
        on_token_fn: TokenCallback,
        on_token_ctx: *anyopaque,
    ) !bool {
        const allocator = self.allocator;
        const decoded_ids = try allocator.alloc(i32, generated_token_ids.len);
        defer allocator.free(decoded_ids);
        for (generated_token_ids, 0..) |token_id, idx| decoded_ids[idx] = @intCast(token_id);

        const decoded_text = try self.tokenizer.decode(allocator, decoded_ids);
        defer allocator.free(decoded_text);

        const prefix_len = std.mem.indexOfDiff(u8, emitted_text.*, decoded_text) orelse @min(emitted_text.*.len, decoded_text.len);
        const delta = decoded_text[prefix_len..];

        allocator.free(emitted_text.*);
        emitted_text.* = try allocator.dupe(u8, decoded_text);
        if (delta.len == 0) return true;
        return on_token_fn(on_token_ctx, delta);
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
        max_tokens: usize,
        prompt_token_count: usize,
        on_token_fn: ?TokenCallback,
        on_token_ctx: ?*anyopaque,
        emitted_text: ?*[]u8,
    ) !DecodeResult {
        const allocator = self.allocator;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var tokens_generated: usize = 0;
        var finish_reason: []const u8 = "length";
        var device_token_tensor: ?ops.CT = null;
        defer if (device_token_tensor) |tensor| self.cb.free(tensor);
        debugGenerationStage(
            "standardDecode enter seq_len={d} max_tokens={d} prefill_cached={}",
            .{ seq_len.*, max_tokens, prefill_last_logits.* != null },
        );

        while (tokens_generated < max_tokens) {
            var used_decode_microbatch = false;
            var next_device_token_tensor: ?ops.CT = null;
            errdefer if (next_device_token_tensor) |tensor| self.cb.free(tensor);
            debugGenerationStage(
                "standardDecode iter={d} seq_len={d} device_token_handoff={}",
                .{ tokens_generated, seq_len.*, device_token_tensor != null },
            );
            const outcome: SampleOutcome = blk: {
                if (tokens_generated == 0) {
                    if (prefill_greedy_token.*) |token| {
                        prefill_greedy_token.* = null;
                        break :blk .{ .token = token, .grammar_complete = false };
                    }
                }

                if (try self.forwardGreedyDeviceDecodeToken(
                    token_ids,
                    seq_len.*,
                    tokens_generated,
                    decode_state,
                    config,
                    token_table,
                    json_grammar,
                    gbnf_grammar,
                    device_token_tensor,
                )) |token| {
                    next_device_token_tensor = token.token_tensor;
                    break :blk .{ .token = token.token, .grammar_complete = false };
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
                                if (self.graph_cache == null and decode_runtime.kvView() != null and tokens_generated > 0) {
                                    used_decode_microbatch = true;
                                    owns_last_logits = true;
                                    break :logits_blk try self.runScheduledDecodeBatch(scheduler, lease, io, decode_state, token_ids[seq_len.* - 1], seq_len.*);
                                }
                                scheduler.awaitTurn(lease, .decode, io);
                            }
                        }
                    }

                    self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
                    const query_seq_len = if (decode_runtime.kvView() != null and tokens_generated > 0) 1 else seq_len.*;
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
            if (self.gpt_config.eos_token_id >= 0 and @as(i32, @intCast(next_token)) == self.gpt_config.eos_token_id) {
                if (next_device_token_tensor) |tensor| {
                    self.cb.free(tensor);
                    next_device_token_tensor = null;
                }
                finish_reason = "stop";
                break;
            }

            if (next_device_token_tensor == null and self.shouldSeedMlxDeviceTokenHandoff(tokens_generated, decode_state, config, token_table, json_grammar, gbnf_grammar)) {
                next_device_token_tensor = try self.makeDeviceTokenTensor(next_token);
            }
            if (device_token_tensor) |tensor| self.cb.free(tensor);
            device_token_tensor = next_device_token_tensor;
            next_device_token_tensor = null;

            token_ids[seq_len.*] = @intCast(next_token);
            seq_len.* += 1;
            tokens_generated += 1;
            _ = try decode_runtime.appendGeneratedToken();
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

    fn forwardGreedyDeviceDecodeToken(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        seq_len: usize,
        tokens_generated: usize,
        decode_state: *NativeDecodeState,
        config: GenerationConfig,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *const ?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*const grammar_mod.GbnfGrammar,
        input_token_tensor: ?ops.CT,
    ) !?DeviceDecodeOutcome {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        decoder_runtime_debug_stats.forward_attempts += 1;
        if (self.cb.kind() != .mlx) {
            decoder_runtime_debug_stats.backend_not_mlx += 1;
            return null;
        }
        if (!enableMlxGreedyDeviceDecodeDebug() and !enableMlxDeviceTokenHandoffDebug() and !enableMlxRawMetalWholeTokenDebug()) {
            decoder_runtime_debug_stats.flag_disabled += 1;
            return null;
        }
        if (self.scheduler != null) {
            decoder_runtime_debug_stats.scheduler_blocked += 1;
            return null;
        }
        if (self.graph_cache != null or self.compiled_partition_backend != null) {
            decoder_runtime_debug_stats.graph_blocked += 1;
            return null;
        }
        if (tokens_generated == 0) {
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

        try self.prepareMlxRawMetalWholeTokenDecode(decode_state);
        if (try self.forwardRawMetalWholeTokenInputSlice(
            token_ids,
            seq_len,
            decode_state,
        )) |token| {
            return .{ .token = token };
        }
        self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
        const decode_context = decode_runtime.makeDecodeContext(seq_len, 1);
        if (enableMlxDeviceTokenHandoffDebug()) {
            if (input_token_tensor) |token_tensor| {
                if (try gpt_arch.forwardGreedyLastTokenFromTokenTensor(
                    &self.cb,
                    self.allocator,
                    self.gpt_config,
                    token_tensor,
                    1,
                    seq_len,
                    &decode_context,
                )) |result| {
                    return .{
                        .token = result.token_id,
                        .token_tensor = result.token_tensor,
                    };
                }
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

    fn prepareMlxRawMetalWholeTokenDecode(
        self: *NativeGenerationPipeline,
        decode_state: *NativeDecodeState,
    ) !void {
        if (comptime !build_options.enable_mlx) return;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        decoder_runtime_debug_stats.prepare_attempts += 1;
        if (!enableMlxRawMetalWholeTokenDebug()) {
            decoder_runtime_debug_stats.prepare_flag_disabled += 1;
            return;
        }
        if (self.cb.kind() != .mlx) {
            decoder_runtime_debug_stats.prepare_backend_not_mlx += 1;
            return;
        }
        const kv_view = decode_runtime.kvView() orelse {
            decoder_runtime_debug_stats.prepare_kv_missing += 1;
            return;
        };
        if (self.scheduler != null) {
            decoder_runtime_debug_stats.prepare_scheduler_blocked += 1;
            return;
        }
        if (self.graph_cache != null or self.compiled_partition_backend != null) {
            decoder_runtime_debug_stats.prepare_graph_blocked += 1;
            return;
        }

        const cfg = self.gpt_config;
        if (cfg.family != .bitnet) {
            decoder_runtime_debug_stats.prepare_arch_blocked += 1;
            return;
        }
        if (cfg.usesMoe() or cfg.hasPle() or cfg.isMultimodal()) {
            decoder_runtime_debug_stats.prepare_model_blocked += 1;
            return;
        }
        if (cfg.sliding_window != 0) {
            decoder_runtime_debug_stats.prepare_model_blocked += 1;
            return;
        }

        if (!build_options.enable_mlx) {
            decoder_runtime_debug_stats.prepare_backend_not_mlx += 1;
            return;
        }

        decoder_runtime_debug_stats.prepare_calls += 1;
        _ = try self.cb.decoderRuntimePrepareGreedy(&.{
            .hidden_size = @intCast(cfg.hidden_size),
            .intermediate_size = @intCast(cfg.intermediate_size),
            .num_layers = @intCast(cfg.num_hidden_layers),
            .num_heads = @intCast(cfg.num_attention_heads),
            .num_kv_heads = @intCast(cfg.effectiveKVHeads()),
            .head_dim = @intCast(cfg.headDim()),
            .vocab_size = @intCast(cfg.vocab_size),
            .kv_tokens = kv_view.token_count,
        });

        if (!(try decoder_bitnet_runtime.prepareDecodeRuntime(
            &self.cb,
            self.allocator,
            cfg,
            kv_view.token_count,
            decoder_runtime_layer_count,
        ))) {
            decoder_runtime_debug_stats.prepare_model_blocked += 1;
        }
    }

    fn forwardRawMetalWholeTokenInputSlice(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        seq_len: usize,
        decode_state: *NativeDecodeState,
    ) !?usize {
        if (comptime !build_options.enable_mlx) return null;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        decoder_runtime_debug_stats.input_attempts += 1;
        if (!build_options.enable_mlx) {
            decoder_runtime_debug_stats.input_backend_not_mlx += 1;
            return null;
        }
        if (!enableMlxRawMetalWholeTokenDebug()) {
            decoder_runtime_debug_stats.input_flag_disabled += 1;
            return null;
        }
        if (self.cb.kind() != .mlx) {
            decoder_runtime_debug_stats.input_backend_not_mlx += 1;
            return null;
        }
        _ = decode_runtime.kvView() orelse {
            decoder_runtime_debug_stats.input_kv_missing += 1;
            return null;
        };

        const cfg = self.gpt_config;
        if (cfg.family != .bitnet) {
            decoder_runtime_debug_stats.input_arch_blocked += 1;
            return null;
        }
        if (cfg.usesMoe() or cfg.hasPle() or cfg.isMultimodal()) {
            decoder_runtime_debug_stats.input_model_blocked += 1;
            return null;
        }
        if (cfg.sliding_window != 0) {
            decoder_runtime_debug_stats.input_model_blocked += 1;
            return null;
        }
        if (seq_len == 0) {
            decoder_runtime_debug_stats.input_seq_empty += 1;
            return null;
        }

        self.cb.drainPrefetchBudget(prefetch_drain_budget_per_step);
        const decode_context = decode_runtime.makeDecodeContext(seq_len, 1);
        const token = (try decoder_bitnet_runtime.forwardGreedyToken(
            &self.cb,
            self.allocator,
            cfg,
            decoder_runtime_layer_count,
            token_ids[seq_len - 1],
            seq_len,
            &decode_context,
        )) orelse return null;
        decoder_runtime_debug_stats.input_successes += 1;
        return @intCast(token);
    }

    fn shouldSeedMlxDeviceTokenHandoff(
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
        if (self.cb.kind() != .mlx) return false;
        if (!enableMlxDeviceTokenHandoffDebug()) return false;
        if (self.scheduler != null) return false;
        if (self.graph_cache != null or self.compiled_partition_backend != null) return false;
        if (decode_runtime.kvView() == null) return false;
        if (!isPureGreedyConfig(config)) return false;
        if (token_table != null or json_grammar.* != null or gbnf_grammar != null) return false;
        if (self.gpt_config.hasPle()) return false;
        return true;
    }

    fn makeDeviceTokenTensor(self: *NativeGenerationPipeline, token_id: usize) !?ops.CT {
        const data = [_]i32{@intCast(token_id)};
        const shape = [_]i32{1};
        return try self.cb.fromInt32Shape(&data, &shape);
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
        return gpt_arch.forward(&self.cb, self.allocator, self.gpt_config, input_ids, batch, seq_len, decode_context);
    }

    const ForwardAllWithHiddenHost = struct {
        allocator: std.mem.Allocator,
        logits: []f32,
        hidden: []f32,
        rows: usize,

        fn deinit(self: *ForwardAllWithHiddenHost) void {
            self.allocator.free(self.logits);
            self.allocator.free(self.hidden);
            self.* = undefined;
        }
    };

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

        const lm_w = if (self.gpt_config.weight_tying)
            try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config)
        else
            self.cb.getWeight("lm_head.weight") catch try gpt_arch.getEmbeddingWeight(&self.cb, self.gpt_config);
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
        return .{
            .allocator = allocator,
            .logits = logits_host,
            .hidden = hidden_host,
            .rows = hidden_result.total_rows,
        };
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

        // --- Verify phase: run target model on all draft positions at once ---
        // We need logits for positions seq_len-1 .. seq_len+draft_count-1
        // (seq_len-1 gives us the logit that should predict token_ids[seq_len],
        //  and so on through seq_len+draft_count-1 which predicts the bonus token)
        const verify_len = draft_count + 1; // +1 so we get logits for the last draft token too
        const verify_seq = seq_len.* + draft_count;

        // Temporarily extend the target KV cache for verification
        _ = try decode_runtime.appendGeneratedTokens(draft_count);

        const target_query_len: usize = if (decode_runtime.kvView() != null) verify_len else verify_seq;
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
        penalty_state: *SamplingPenaltyState,
        token_table: ?*const grammar_mod.TokenByteTable,
        json_grammar: *?grammar_mod.JsonGrammar,
        gbnf_grammar: ?*grammar_mod.GbnfGrammar,
        last_activation: *?[]f32,
    ) !SpeculativeRoundResult {
        const allocator = self.allocator;
        const round_start = seq_len.*;
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        var round_penalties = try penalty_state.clone(allocator);
        defer round_penalties.deinit(allocator);

        var activation = last_activation.* orelse return error.MissingGemma4MtpActivation;
        var draft_tokens: [16]i64 = undefined;
        const actual_k = @min(k, 16);
        var draft_count: usize = 0;
        const draft_ctx = decode_state.gptDecodeContext(seq_len.*, 1);

        for (0..actual_k) |_| {
            const source_token = token_ids[seq_len.* + draft_count - 1];
            const draft_result = try gemma4_mtp.draftToken(.{
                .allocator = allocator,
                .target_cb = &self.cb,
                .draft_cb = &draft_pipeline.cb,
                .target_config = self.gpt_config,
                .draft_config = draft_pipeline.gpt_config,
                .token_id = source_token,
                .activation = activation,
                .decode_context = &draft_ctx,
            });
            if (activation.ptr != last_activation.*.?.ptr) allocator.free(activation);
            activation = draft_result.projected_activation;
            draft_tokens[draft_count] = @intCast(draft_result.token);
            token_ids[seq_len.* + draft_count] = @intCast(draft_result.token);
            draft_count += 1;
        }
        if (enableGemma4MtpDebug()) {
            std.debug.print("gemma4_mtp_debug: seq={d} source={d} drafted", .{
                seq_len.*,
                token_ids[seq_len.* - 1],
            });
            for (draft_tokens[0..draft_count]) |draft_token| {
                std.debug.print(" {d}", .{draft_token});
            }
            std.debug.print("\n", .{});
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

        const verify_len = draft_count + 1;
        const verify_seq = seq_len.* + draft_count;
        _ = try decode_runtime.appendGeneratedTokens(draft_count);
        const target_query_len: usize = if (decode_runtime.kvView() != null) verify_len else verify_seq;
        const target_ctx = decode_runtime.makeDecodeContext(verify_seq, target_query_len);
        const verify_start = if (target_query_len == verify_seq) 0 else verify_seq - target_query_len;

        var target_result = self.forwardAllLogitsAndHiddenHost(
            token_ids[verify_start..verify_seq],
            1,
            verify_seq,
            &target_ctx,
        ) catch |err| {
            decode_runtime.truncateGeneratedTokens(draft_count) catch {};
            return err;
        };
        defer target_result.deinit();

        const verify_result = try self.acceptVerifiedDraftTokens(
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
        );

        const matched_drafts = verify_result.matched_drafts;
        const accepted = verify_result.accepted;
        const rollback = draft_count - matched_drafts;
        if (rollback > 0) {
            try decode_runtime.truncateGeneratedTokens(rollback);
        }

        var next_activation: ?[]f32 = null;
        if (verify_result.accepted > 0) {
            const hidden_size: usize = @intCast(self.gpt_config.hidden_size);
            const row = verify_result.accepted - 1;
            if (row >= target_result.rows) return error.InvalidTensorShape;
            next_activation = try allocator.dupe(
                f32,
                target_result.hidden[row * hidden_size ..][0..hidden_size],
            );
        }
        if (verify_result.correction_added or verify_result.had_bonus) {
            const accepted_seq_len = seq_len.* + accepted;
            const materialized_hidden = try self.materializeAcceptedTokenKvAndReturnHidden(
                token_ids,
                accepted_seq_len,
                decode_state,
            );
            allocator.free(materialized_hidden);
        }

        if (last_activation.*) |old| {
            if (activation.ptr != old.ptr) allocator.free(activation);
            allocator.free(old);
        } else {
            allocator.free(activation);
        }
        last_activation.* = next_activation;
        seq_len.* += accepted;
        try penalty_state.noteTokens(allocator, token_ids[round_start..seq_len.*]);

        return .{
            .drafted = draft_count,
            .matched_drafts = matched_drafts,
            .accepted = accepted,
            .correction_added = verify_result.correction_added,
            .had_bonus = verify_result.had_bonus,
            .hit_eos = verify_result.hit_eos,
            .hit_grammar_stop = verify_result.hit_grammar_stop,
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
    };

    const SpeculativeVerificationResult = struct {
        matched_drafts: usize,
        accepted: usize,
        hit_eos: bool,
        hit_grammar_stop: bool,
        correction_added: bool,
        had_bonus: bool,
    };

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
        const working_logits = if (has_grammar)
            try self.allocator.dupe(f32, logits)
        else
            @constCast(logits);
        defer if (has_grammar) self.allocator.free(working_logits);

        if (has_grammar) {
            try self.applyGrammarMask(working_logits, token_table, json_grammar, gbnf_grammar);
        }

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
    ) !SpeculativeVerificationResult {
        const vocab_size = self.gpt_config.vocab_size;
        const verify_len = draft_tokens.len + 1;
        const logit_base_offset = (target_query_len - verify_len) * vocab_size;

        var matched_drafts: usize = 0;
        var accepted: usize = 0;
        var hit_eos = false;
        var hit_grammar_stop = false;
        var correction_added = false;

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
            debugGemma4Mtp("verify index={d} draft={d} target={d}", .{
                i,
                draft_tokens[i],
                target_choice,
            });

            if (target_choice == @as(usize, @intCast(draft_tokens[i]))) {
                matched_drafts += 1;
                accepted += 1;
                try round_penalties.noteToken(self.allocator, draft_tokens[i]);

                if (self.gpt_config.eos_token_id >= 0 and
                    @as(i32, @intCast(draft_tokens[i])) == self.gpt_config.eos_token_id)
                {
                    hit_eos = true;
                    break;
                }
                if (outcome.grammar_complete) {
                    hit_grammar_stop = true;
                    break;
                }
            } else {
                token_ids[seq_len + matched_drafts] = @intCast(target_choice);
                accepted = matched_drafts + 1;
                correction_added = true;

                if (self.gpt_config.eos_token_id >= 0 and
                    @as(i32, @intCast(target_choice)) == self.gpt_config.eos_token_id)
                {
                    hit_eos = true;
                }
                if (outcome.grammar_complete) {
                    hit_grammar_stop = true;
                }
                break;
            }
        }

        const had_bonus = matched_drafts == draft_tokens.len and !hit_eos and !hit_grammar_stop;
        if (had_bonus) {
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
            debugGemma4Mtp("bonus index={d} target={d}", .{
                draft_tokens.len,
                bonus_token,
            });

            token_ids[seq_len + accepted] = @intCast(bonus_token);
            accepted += 1;

            if (self.gpt_config.eos_token_id >= 0 and
                @as(i32, @intCast(bonus_token)) == self.gpt_config.eos_token_id)
            {
                hit_eos = true;
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
        // token, then advance the token counter. Both paged and non-paged paths
        // need this — the forward pass writes KV as a side effect, and without
        // it the next decode step reads uninitialized KV entries.
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        const logits = try self.forwardAllLogits(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        self.allocator.free(logits);
        _ = try decode_runtime.appendGeneratedToken();
    }

    fn materializeAcceptedTokenKvAndReturnHidden(
        self: *NativeGenerationPipeline,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
    ) ![]f32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) return error.MissingMaterializedHiddenState;
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        var result = try self.forwardAllLogitsAndHiddenHost(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        _ = try decode_runtime.appendGeneratedToken();
        if (result.rows != 1 or result.hidden.len != self.gpt_config.hidden_size) return error.InvalidTensorShape;
        return try self.allocator.dupe(f32, result.hidden);
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
        try scheduler.enqueueDecodeWork(lease.*, @ptrCast(&work), seq_len, decode_ctx.kv_sequence_len, decode_ctx.kv_position_offset);
        defer if (!work.ready) scheduler.cancelDecodeWork(@ptrCast(&work));
        notePendingKvBlocksFromState(scheduler, decode_state, @ptrCast(&work), .decode, 1);
        notePendingExclusiveStepFromState(scheduler, decode_state, @ptrCast(&work), .decode);

        var driver = DecodeStepDriver{
            .pipeline = self,
            .scheduler = scheduler,
            .work = &work,
            .decode_state = decode_state,
        };
        try runStepLoop(self.allocator, scheduler, lease, @ptrCast(&work), .decode, io, &driver);

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
        try scheduler.enqueuePrefillWork(lease.*, @ptrCast(&work), seq_len, query_seq_len, decode_ctx.kv_sequence_len, decode_ctx.kv_position_offset);
        defer if (!work.ready) scheduler.cancelPrefillWork(@ptrCast(&work));
        notePendingKvBlocksFromState(scheduler, decode_state, @ptrCast(&work), .prefill, query_seq_len);
        notePendingExclusiveStepFromState(scheduler, decode_state, @ptrCast(&work), .prefill);

        var driver = PrefillStepDriver{
            .pipeline = self,
            .scheduler = scheduler,
            .work = &work,
            .decode_state = decode_state,
        };
        try runStepLoop(self.allocator, scheduler, lease, @ptrCast(&work), .prefill, io, &driver);

        if (work.failure) |err| return err;
        const logits = work.logits;
        work.logits = null;
        return logits;
    }

    /// Output of a successful step forward pass: the heap-allocated logits row
    /// buffer and the maximum query sequence length used to size each row.
    /// Caller owns the logits buffer.
    const StepForwardResult = struct {
        logits: []f32,
        max_query_seq_len: usize,
    };

    /// Run a step's forward pass: allocate scratch, populate per-item context,
    /// reserve prefill state, build the mixed-batch decode context, and invoke
    /// the model. On any error before `forwardAllLogits` returns successfully,
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
            reserved[idx] = try reserveScheduledPrefillState(work.decode_state, work.seq_len);
            reserved_count = idx + 1;
        }

        var owned_ctx = try buildOwnedMixedBatchDecodeContext(self.allocator, items);
        defer owned_ctx.deinit();

        const logits = try self.forwardAllLogits(input_ids, claimed.len, max_query_seq_len, &owned_ctx.context);
        // Past this point the forward has committed: the prefill state is
        // valid and rollback would corrupt it. Returning normally cancels the
        // errdefer above.
        return .{ .logits = logits, .max_query_seq_len = max_query_seq_len };
    }

    /// Drive a claimed step end-to-end. Always calls `scheduler.completeStep`
    /// before returning so pending entries never leak. Per-item failures are
    /// surfaced via `work.failure`/`work.ready`; the function itself does not
    /// return errors to the caller.
    fn executeClaimedStep(
        self: *NativeGenerationPipeline,
        scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        lease: *runtime.scheduler.native_generate.Lease,
        claimed: []const runtime.scheduler.native_generate.StepItem,
    ) void {
        const result = self.forwardScheduledStep(claimed) catch |err| {
            markStepFailed(claimed, err);
            scheduler.completeStep(lease, claimed);
            return;
        };
        defer self.allocator.free(result.logits);

        dispatchStepLogits(self.gpt_config.vocab_size, claimed, result.logits, result.max_query_seq_len);
        scheduler.completeStep(lease, claimed);
    }

    fn reserveScheduledPrefillState(decode_state: *NativeDecodeState, target_total_seq_len: usize) !usize {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        return decode_runtime.reservePrefillTo(target_total_seq_len);
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
        for (claimed) |item| {
            switch (item.phase) {
                .decode => {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    work.failure = err;
                    work.ready = true;
                },
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    work.failure = err;
                    work.ready = true;
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
        logits: []const f32,
        max_query_seq_len: usize,
    ) void {
        for (claimed, 0..) |item, idx| {
            const row_index = idx * max_query_seq_len + (item.query_sequence_len - 1);
            const start = row_index * vocab_size;
            const slice = logits[start..][0..vocab_size];
            switch (item.phase) {
                .decode => {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    if (work.allocator.dupe(f32, slice)) |buf| {
                        work.logits = buf;
                    } else |dupe_err| {
                        work.failure = dupe_err;
                    }
                    work.ready = true;
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
                    work.ready = true;
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
    driver: anytype,
) !void {
    const DriverInfo = @typeInfo(@TypeOf(driver));
    const DriverChild = DriverInfo.pointer.child;

    var step = std.ArrayListUnmanaged(runtime.scheduler.native_generate.StepItem).empty;
    defer step.deinit(allocator);

    while (!driver.isReady()) {
        if (@hasDecl(DriverChild, "preStep")) driver.preStep();
        const budget = driver.stepBudget();
        if (try scheduler.claimStep(allocator, lease, leader_work_ptr, leader_phase, budget, &step)) {
            driver.execute(scheduler, lease, step.items);
        } else {
            io.sleep(std.Io.Duration.fromMilliseconds(0), .awake) catch break;
        }
    }
}

const DecodeStepDriver = struct {
    pipeline: *NativeGenerationPipeline,
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    work: *NativeGenerationPipeline.PendingDecodeBatchWork,
    decode_state: *NativeDecodeState,

    fn isReady(self: *const DecodeStepDriver) bool {
        return self.work.ready;
    }

    fn stepBudget(self: *const DecodeStepDriver) runtime.scheduler.native_generate.StepBudget {
        return stepBudgetFromState(self.scheduler, self.decode_state);
    }

    fn preStep(self: *DecodeStepDriver) void {
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
        return self.work.ready;
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
    const km = decode_state.kv_manager orelse return;
    const seq_id = decode_state.sequence_id orelse return;
    const est = km.estimateBlocksFor(seq_id, additional_tokens) orelse return;
    scheduler.notePendingKvBlocks(work_ptr, phase, est);
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
            true,
        );
    }
    return tokenizer.encodeForGenerationConfigured(
        allocator,
        prompt,
        max_length,
        shouldAddBosToken(prompt, add_bos_token, bos_token),
    );
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
    var encoded = try encodePromptForGeneration(tokenizer, allocator, text, remaining, add_bos_token, bos_token);
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

test "shouldAddBosToken skips duplicate literal bos prefix" {
    try std.testing.expect(!shouldAddBosToken("<bos><start_of_turn>user\nHello", true, "<bos>"));
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

    try std.testing.expect(dec_a.ready);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), dec_a.failure);
    try std.testing.expect(dec_a.logits == null);

    try std.testing.expect(pre_a.ready);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), pre_a.failure);
    try std.testing.expect(pre_a.logits == null);
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

    try std.testing.expect(dec_w.ready);
    try std.testing.expectEqualSlices(f32, &.{ 10, 11 }, dec_w.logits.?);
    try std.testing.expect(pre_with_logits.ready);
    try std.testing.expectEqualSlices(f32, &.{ 22, 23 }, pre_with_logits.logits.?);
    try std.testing.expect(pre_no_logits.ready);
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

    try std.testing.expect(dec_a.ready);
    try std.testing.expect(dec_a.failure == null);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, dec_a.logits.?);

    try std.testing.expect(dec_b.ready);
    try std.testing.expect(dec_b.failure != null);
    try std.testing.expect(dec_b.logits == null);

    // The failing dupe must not stop dec_c's dispatch.
    try std.testing.expect(dec_c.ready);
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
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_byte), 5, 5, 0);

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

    try runStepLoop(allocator, &coordinator, &lease, @ptrCast(&work_byte), .decode, io_impl.io(), &driver);

    try std.testing.expect(driver.ready);
    try std.testing.expectEqual(@as(usize, 1), driver.execute_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.last_claim_size);
    try std.testing.expectEqual(@as(usize, 1), driver.prestep_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.budget_calls);
    try std.testing.expectEqual(@as(usize, 0), coordinator.pending_decode.items.len);
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

    try runStepLoop(allocator, &coordinator, &lease, @ptrCast(&ghost_work), .decode, io_impl.io(), &driver);

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
    try coordinator.enqueueDecodeWork(lease_a, @ptrCast(&work_a), 5, 5, 0);
    try coordinator.enqueueDecodeWork(lease_b, @ptrCast(&work_b), 5, 5, 0);

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

    try runStepLoop(allocator, &coordinator, &lease_a, @ptrCast(&work_a), .decode, io_impl.io(), &driver);

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
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_a), 7, 7, 0);

    notePendingKvBlocksFromState(&coordinator, &decode_state, @ptrCast(&work_a), .decode, 1);

    // 1 token fits in the existing tail slack → 0 new blocks.
    try std.testing.expectEqual(@as(?usize, 0), coordinator.pendingKvBlocksEstimate(@ptrCast(&work_a), .decode));

    // A larger overflow: 5 tokens vs 2-token slack → 1 new block.
    var work_b: u8 = 1;
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&work_b), 11, 11, 0);
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
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&normal_work), 7, 7, 0);
    notePendingExclusiveStepFromState(&coordinator, &decode_state, @ptrCast(&normal_work), .decode);
    try std.testing.expectEqual(@as(?bool, false), coordinator.pendingRequiresExclusiveStep(@ptrCast(&normal_work), .decode));

    decode_state.deepseek_v4_compressed_cache = try gpt_arch.DeepSeekV4CompressedCache.init(allocator, 1);

    var compressed_work: u8 = 1;
    try coordinator.enqueueDecodeWork(lease, @ptrCast(&compressed_work), 8, 8, 0);
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

test "native decode state paged kv grows in pages" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .mlx,
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

test "native decode state chunked prefill appends incrementally" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .mlx,
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

test "native decode state maps to gpt decode context" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .mlx,
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

test "native decode state deinit releases paged sequence" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .mlx,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 8,
        .head_dim = 128,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    try state.notePrefill(5);
    try std.testing.expect(manager.tokenCount(state.sequence_id.?).? > 0);

    state.deinit();
    try std.testing.expectEqual(@as(?runtime.kv.manager.SequenceId, null), state.sequence_id);
    try std.testing.expectEqual(@as(usize, 0), manager.tokenCount(1).?);
}

test "native decode state reports retained kv window offsets after trim" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .mlx,
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

    var first = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer first.deinit();
    var second = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer second.deinit();

    try first.notePrefill(6);
    try second.notePrefill(6);

    var owned = try buildOwnedBatchDecodeContext(allocator, &.{ &first, &second }, 6, 1);
    defer owned.deinit();

    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.paged_decode, owned.context.attention_mode);
    try std.testing.expectEqual(@as(usize, 2), owned.kv_batch.?.len);
    try std.testing.expect(owned.context.kv_batch != null);
    try std.testing.expectEqual(first.sequence_id.?, owned.kv_batch.?[0].kv_cache.sequence_id);
    try std.testing.expectEqual(second.sequence_id.?, owned.kv_batch.?[1].kv_cache.sequence_id);
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

    var prefill = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer prefill.deinit();
    var decode = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
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
    );

    try std.testing.expectEqual(@as(usize, 0), result.matched_drafts);
    try std.testing.expectEqual(@as(usize, 1), result.accepted);
    try std.testing.expectEqual(true, result.correction_added);
    try std.testing.expectEqual(true, result.hit_grammar_stop);
    try std.testing.expectEqual(false, result.had_bonus);
    try std.testing.expectEqual(@as(i64, 4), token_ids[1]);
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
