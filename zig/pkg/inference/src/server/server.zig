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

// HTTP API server for Antfly inference.
// Uses generated types and server router from openapi-zig.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const api = @import("inference_api");
const generating_api = @import("antfly_generating_openapi");
const extraction_api = @import("antfly_extraction_openapi");
const readers_api = @import("antfly_readers");
const transcribing_api = @import("antfly_transcribing");
const extracting_api = @import("antfly_extracting");
const scraping = @import("antfly_scraping");
const jsonschema = @import("antfly_jsonschema");
const lib_chunker = @import("inference_chunker");
const hf_tokenizer_mod = @import("inference_hf_tokenizer");
const backends_mod = @import("../backends/backends.zig");
const session_factory = @import("../architectures/session_factory.zig");
const registry_mod = @import("../registry/registry.zig");
const extractors_mod = @import("../extractors/extractor.zig");
const cache_mod = @import("../cache/cache.zig");
const model_manager_mod = @import("model_manager.zig");
const model_caps = @import("../models/capabilities.zig");
const manifest_mod = @import("../models/manifest.zig");
const safetensors_mod = @import("../models/safetensors.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const model_compatibility = @import("../models/compatibility.zig");
const chunking_mod = @import("../pipelines/chunking.zig");
const embedding_mod = @import("../pipelines/embedding.zig");
const extraction_mod = @import("../pipelines/extraction.zig");
const sparse_embedding_mod = @import("../pipelines/sparse_embedding.zig");
const generation = @import("../pipelines/generation.zig");
const multimodal_reranker = @import("../pipelines/multimodal_reranker.zig");
const multimodal_qwen_adapter = @import("../pipelines/multimodal_qwen_adapter.zig");
const document_classification = @import("../pipelines/document_classification.zig");
const document_token_classification = @import("../pipelines/document_token_classification.zig");
const graph_mod = @import("../graph/root.zig");
const gliner_mod = @import("../pipelines/gliner.zig");
const grammar_mod = @import("../pipelines/grammar.zig");
const audio_mod = @import("../pipelines/audio.zig");
const readers_mod = @import("../readers/reader.zig");
const rebel_mod = @import("../pipelines/rebel.zig");
const resolver_mod = @import("../pipelines/resolver.zig");
const cleanup_pipeline_mod = @import("../pipelines/entity_cleanup.zig");
const cleanup_model_mod = @import("../finetune/entity_cleanup_model.zig");
const onnx_decoder_only_vlm = @import("../pipelines/onnx_decoder_only_vlm.zig");
const tool_parser_mod = @import("../pipelines/tool_parser.zig");
const ops = @import("../ops/ops.zig");
const runtime = @import("../runtime/root.zig");
const tabular_mod = @import("../tabular/root.zig");
const c_file = @import("../util/c_file.zig");
const native_backend_choice = @import("../native_backend_choice.zig");
const pjrt_lib = if (build_options.enable_pjrt) @import("pjrt") else struct {
    pub const pjrt = struct {
        pub const Client = struct {
            pub fn init(_: [:0]const u8) !@This() {
                return error.PjrtNotEnabled;
            }
            pub fn deinit(_: *@This()) void {}
        };
    };
};
pub const metrics_mod = @import("metrics.zig");
const request_queue_mod = @import("request_queue.zig");

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn shouldSkipAutoMtpDraftLoad(config: generation.GenerationConfig, draft_cfg: gpt_model_mod.Config) bool {
    if (config.speculation_policy != .auto) return false;
    if (!draft_cfg.gemma4_mtp_assistant) return false;
    if (config.speculation_calibration == .none) return true;
    if (!generation.gemma4MtpTargetReplayLikelyActive()) return true;
    const requested_max_tokens: usize = @intCast(@max(config.max_tokens, 1));
    return requested_max_tokens < generation.gemma4MtpAutoMinGenerationTokens();
}

pub const BudgetOverrides = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
    combined_limit_bytes: usize = 0,
    kv_limit_bytes: usize = 0,
    scratch_limit_bytes: usize = 0,

    pub fn apply(self: @This(), defaults: runtime.tier.memory.Limits) runtime.tier.memory.Limits {
        var limits = defaults;
        if (self.host_limit_bytes > 0) limits.host_limit_bytes = self.host_limit_bytes;
        if (self.backend_limit_bytes > 0) limits.backend_limit_bytes = self.backend_limit_bytes;
        if (self.combined_limit_bytes > 0) limits.combined_limit_bytes = self.combined_limit_bytes;
        if (self.kv_limit_bytes > 0) limits.kv_limit_bytes = self.kv_limit_bytes;
        if (self.scratch_limit_bytes > 0) limits.scratch_limit_bytes = self.scratch_limit_bytes;
        return limits;
    }
};

pub const PromptCacheConfig = struct {
    enabled: bool = false,
    mode: runtime.kv.prompt_cache.Mode = .block_hash,
    max_bytes_mb: usize = 512,
    min_tokens: usize = 64,
    ttl_ms: u64 = 300_000,

    /// max_bytes_mb is a node-wide accounting target. ModelManager divides it
    /// across active caches and reconfigures them together.
    pub fn runtimeConfig(
        self: @This(),
        resource_usage_observer: ?runtime.kv.prompt_cache.ResourceUsageObserver,
    ) runtime.kv.prompt_cache.Config {
        return .{
            .enabled = self.enabled,
            .mode = self.mode,
            .max_bytes = self.max_bytes_mb * 1024 * 1024,
            .min_tokens = self.min_tokens,
            .ttl_ms = self.ttl_ms,
            .resource_usage_observer = resource_usage_observer,
        };
    }
};

test "extraction resolution distinguishes absence resource pressure and internal failure" {
    try std.testing.expectEqual(
        ExtractionResolutionFailure.not_found,
        classifyExtractionResolutionFailure(error.ModelNotFound),
    );
    try std.testing.expectEqual(
        ExtractionResolutionFailure.resource_exhausted,
        classifyExtractionResolutionFailure(error.OutOfMemory),
    );
    try std.testing.expectEqual(
        ExtractionResolutionFailure.internal,
        classifyExtractionResolutionFailure(error.Unexpected),
    );
}

test "prompt cache config reports node target in bytes" {
    const cfg = PromptCacheConfig{ .enabled = true, .max_bytes_mb = 512 };
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), cfg.runtimeConfig(null).max_bytes);
}

test "model manager rebalances every active prompt cache" {
    const allocator = std.testing.allocator;
    var manager = model_manager_mod.ModelManager.init(allocator, backends_mod.SessionManager.init(allocator));
    defer manager.loaded.deinit(allocator);
    defer manager.loaded_aliases.deinit(allocator);

    var first: model_manager_mod.LoadedModel = undefined;
    first.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer first.prompt_prefix_cache.deinit();
    var second: model_manager_mod.LoadedModel = undefined;
    second.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer second.prompt_prefix_cache.deinit();
    try manager.loaded.put(allocator, "first", &first);
    try manager.loaded.put(allocator, "second", &second);

    first.prompt_prefix_cache.configure(.{
        .enabled = true,
        .mode = .simple,
        .min_tokens = 2,
        .max_bytes = 1 << 20,
    });
    const first_pool_id = (try first.prompt_prefix_cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const first_sequence_id = try first.prompt_prefix_cache.manager.attachSequence(first_pool_id);
    try first.prompt_prefix_cache.manager.appendTokens(first_sequence_id, 2);
    try first.prompt_prefix_cache.storeFromSequence("", &.{ 1, 2 }, first_sequence_id);
    try std.testing.expectEqual(@as(usize, 1), first.prompt_prefix_cache.stats().live_entries);

    manager.rebalancePromptCaches(&second, .{
        .enabled = true,
        .mode = .simple,
        .min_tokens = 2,
        .max_bytes = 2,
    });

    try std.testing.expectEqual(@as(usize, 1), first.prompt_prefix_cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 1), second.prompt_prefix_cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 0), first.prompt_prefix_cache.stats().live_entries);
}

test "concurrent first prompt cache activations share the node budget" {
    const allocator = std.testing.allocator;
    var manager = model_manager_mod.ModelManager.init(allocator, backends_mod.SessionManager.init(allocator));
    defer manager.loaded.deinit(allocator);
    defer manager.loaded_aliases.deinit(allocator);

    var first: model_manager_mod.LoadedModel = undefined;
    first.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer first.prompt_prefix_cache.deinit();
    var second: model_manager_mod.LoadedModel = undefined;
    second.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer second.prompt_prefix_cache.deinit();
    try manager.loaded.put(allocator, "first", &first);
    try manager.loaded.put(allocator, "second", &second);

    const node_config = runtime.kv.prompt_cache.Config{
        .enabled = true,
        .max_bytes = 1024,
    };
    // Reproduce the production interleaving: both handlers rebalance before
    // either has reached ensurePool().
    manager.rebalancePromptCaches(&first, node_config);
    manager.rebalancePromptCaches(&second, node_config);

    try std.testing.expectEqual(@as(usize, 512), first.prompt_prefix_cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 512), second.prompt_prefix_cache.config.max_bytes);
    try std.testing.expect(first.prompt_prefix_cache.isParticipating());
    try std.testing.expect(second.prompt_prefix_cache.isParticipating());

    manager.cancelPromptCacheActivation(&second, node_config);
    try std.testing.expectEqual(@as(usize, 1024), first.prompt_prefix_cache.config.max_bytes);
    try std.testing.expect(!second.prompt_prefix_cache.isParticipating());
}

pub const NodeConfig = struct {
    models_dir: []const u8 = "./models",
    ml_dir: []const u8 = "./ml",
    content_security: ?scraping.ContentSecurityConfig = null,
    s3_credentials: ?scraping.S3CredentialsConfig = null,
    preload: []const WarmModel = &.{},
    keep_alive_ms: u64 = 300_000,
    max_loaded_models: usize = 10,
    max_concurrent_requests: usize = 32,
    pool_size: usize = 2,
    generation_budget_overrides: BudgetOverrides = .{},
    prompt_cache: PromptCacheConfig = .{},
    prompt_cache_resource_usage_observer: ?runtime.kv.prompt_cache.ResourceUsageObserver = null,
    tokenizer_cache: hf_tokenizer_mod.HfTokenizer.BpeCacheConfig = .{},
    tokenizer_parallel_bpe: hf_tokenizer_mod.HfTokenizer.ParallelBpeConfig = .{},
    /// Permit artifacts whose compatibility cannot be proven by this build.
    /// Known incompatible or unsafe artifacts remain blocked.
    allow_unknown_models: bool = false,
};

pub const WarmModelKind = enum {
    generator,
    embedder,
    reranker,
    chunker,
    classifier,
    rewriter,
    reader,
    transcriber,
    extractor,
};

pub const WarmModel = struct {
    kind: WarmModelKind = .generator,
    name: []const u8,
    backend: ?backends_mod.BackendType = null,
    format: ?[]const u8 = null,
    quantization: ?[]const u8 = null,
};

pub const ai_api_prefix = "/ai/v1";
pub const public_api_prefix = "/ml/v1";
const max_generate_batch_items: usize = 128;
const max_read_batch_images: usize = 64;
const default_read_queue_max_tokens: usize = 256;
const max_read_tokens: usize = 1024;
const default_max_read_batch_bytes: usize = 256 * 1024 * 1024;

const GenerateBackendSelection = struct {
    native_choice: native_backend_choice.Choice = .auto,
    compiled_partition_backend: ?ops.BackendKind = null,
    compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = .partitioned,
    graph_mode_requested: bool = false,
    eager_mode_requested: bool = false,
};

fn parseGenerateBackendSelection(
    backend_value: ?api.ModelBackend,
    mode_value: ?[]const u8,
    compiled_target_value: ?[]const u8,
) !GenerateBackendSelection {
    const choice = if (backend_value) |value|
        modelBackendToNativeChoice(value)
    else
        native_backend_choice.Choice.auto;
    try native_backend_choice.validate(choice);

    var eager_mode_requested = false;
    const compiled_mode_requested = if (mode_value) |value| blk: {
        if (std.mem.eql(u8, value, "eager")) {
            eager_mode_requested = true;
            break :blk false;
        }
        if (std.mem.eql(u8, value, "compiled")) break :blk true;
        return error.InvalidGenerateMode;
    } else false;

    const explicit_partition_backend = native_backend_choice.compiledPartitionBackendForMode(
        choice,
        compiled_mode_requested,
    );
    const compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = if (compiled_target_value) |value| blk: {
        if (std.mem.eql(u8, value, "partitioned")) break :blk graph_mod.compiled_backend.AttachmentTarget.partitioned;
        if (std.mem.eql(u8, value, "whole-model")) break :blk graph_mod.compiled_backend.AttachmentTarget.whole_model;
        return error.InvalidCompiledTarget;
    } else blk: {
        if (compiled_mode_requested and explicit_partition_backend == .metal) break :blk graph_mod.compiled_backend.AttachmentTarget.whole_model;
        break :blk graph_mod.compiled_backend.AttachmentTarget.partitioned;
    };

    return .{
        .native_choice = choice,
        .compiled_partition_backend = explicit_partition_backend,
        .compiled_attachment_target = compiled_attachment_target,
        .graph_mode_requested = compiled_mode_requested,
        .eager_mode_requested = eager_mode_requested,
    };
}

fn shouldAutoUseMetalWholeModelGenerate(
    loaded_backend: backends_mod.BackendType,
    metal_executor_supported: bool,
    deepseek_compressed_cache: bool,
    selection: GenerateBackendSelection,
) bool {
    if (!build_options.enable_metal) return false;
    if (selection.eager_mode_requested) return false;
    if (selection.compiled_partition_backend != null) return false;
    if (selection.native_choice == .native) return false;
    return loaded_backend == .metal and metal_executor_supported and !deepseek_compressed_cache;
}

fn modelBackendToNativeChoice(value: api.ModelBackend) native_backend_choice.Choice {
    return switch (value) {
        .auto => .auto,
        .onnx => .onnx,
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .xla => .xla,
        .webgpu, .wasm => .webgpu,
    };
}

fn configureGenerateBackendPreference(
    session_manager: *backends_mod.SessionManager,
    selection: GenerateBackendSelection,
) void {
    native_backend_choice.configureSessionPreference(session_manager, selection.native_choice);
}

fn singleBackendPreference(backend: backends_mod.BackendType) []const backends_mod.BackendType {
    return switch (backend) {
        .native => &.{.native},
        .onnx => &.{.onnx},
        .metal => &.{.metal},
        .cuda => &.{.cuda},
        .pjrt => &.{.pjrt},
        .wasm => &.{.wasm},
    };
}

/// Global node pointer for operational handlers.
var active_node: ?*Node = null;
var active_models_dir: ?[]const u8 = null;

fn embedTimingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_EMBED_TIMING", false);
}

fn embedTimingNowNs() u128 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => 0,
    };
}

fn ensureDirectEmbeddingDeadline(deadline_ns: ?u64) !void {
    const deadline = deadline_ns orelse return;
    if (embedTimingNowNs() >= deadline) return error.Timeout;
}

fn freeDirectDenseVectors(allocator: std.mem.Allocator, vectors: [][]f32) void {
    for (vectors) |vector| allocator.free(vector);
    allocator.free(vectors);
}

fn embedTimingStart() u128 {
    if (!embedTimingEnabled()) return 0;
    return embedTimingNowNs();
}

fn logEmbedTiming(phase: []const u8, count: usize, start_ns: u128) void {
    if (start_ns == 0) return;
    const now = embedTimingNowNs();
    const elapsed_us = if (now > start_ns) @divTrunc(now - start_ns, 1000) else 0;
    std.log.info("antfly inference embed timing phase={s} count={d} elapsed_us={d}", .{ phase, count, elapsed_us });
}

fn serverGenerateTimingEnabled() bool {
    return platform.env.getenvBool("TERMITE_SERVER_GENERATE_TIMING");
}

fn elapsedMs(from_ns: u128, to_ns: u128) u64 {
    if (to_ns <= from_ns) return 0;
    return @intCast(@divTrunc(to_ns - from_ns, std.time.ns_per_ms));
}

fn allocCompletionId(allocator: std.mem.Allocator) ![]u8 {
    var bytes: [8]u8 = undefined;
    try fillRandomBytes(&bytes);
    const value = std.mem.readInt(u64, &bytes, .little);
    var scratch: [16]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&scratch, "{x}", .{value});
    var padded = [_]u8{'0'} ** 16;
    @memcpy(padded[padded.len - rendered.len ..], rendered);
    return std.fmt.allocPrint(allocator, "chatcmpl-{s}", .{padded[0..]});
}

fn fillRandomBytes(buffer: []u8) !void {
    if (buffer.len == 0) return;

    if (builtin.os.tag == .linux) {
        var offset: usize = 0;
        while (offset < buffer.len) {
            const remaining = buffer[offset..];
            const rc = std.os.linux.getrandom(remaining.ptr, remaining.len, 0);
            switch (std.posix.errno(rc)) {
                .SUCCESS => {
                    const n: usize = @intCast(rc);
                    if (n == 0) return error.EntropyUnavailable;
                    offset += n;
                },
                .INTR => continue,
                else => return error.EntropyUnavailable,
            }
        }
        return;
    }

    if (comptime @TypeOf(std.c.arc4random_buf) != void) {
        std.c.arc4random_buf(buffer.ptr, buffer.len);
        return;
    }

    return error.EntropyUnavailable;
}

fn completionCreatedTimestamp() i64 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => @intCast(ts.sec),
        else => 0,
    };
}

fn tokenUsage(prompt_tokens: usize, completion_tokens: usize) api.GenerateUsage {
    return .{
        .prompt_tokens = @intCast(prompt_tokens),
        .completion_tokens = @intCast(completion_tokens),
        .total_tokens = @intCast(prompt_tokens + completion_tokens),
    };
}

fn estimateTextTokens(text: []const u8) usize {
    var count: usize = 0;
    var in_token = false;
    for (text) |ch| {
        const ws = switch (ch) {
            ' ', '\n', '\r', '\t' => true,
            else => false,
        };
        if (ws) {
            in_token = false;
        } else if (!in_token) {
            count += 1;
            in_token = true;
        }
    }
    return count;
}

fn estimateTextsTokens(texts: []const []const u8) usize {
    var total: usize = 0;
    for (texts) |text| total += estimateTextTokens(text);
    return total;
}

// std.Io owns the actual worker pool, so this bounds queued tokenizer
// consumers without creating or oversubscribing OS threads. Sixteen gives the
// encoder enough pull-scheduled chunks for heterogeneous server CPUs while its
// 256 KiB threshold keeps normal request prompts on the serial path.
const tokenizer_parallel_max_tasks = 16;

fn countTokenizerTokens(
    allocator: std.mem.Allocator,
    io: ?std.Io,
    tokenizer: anytype,
    text: []const u8,
) !usize {
    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(allocator);
    if (io) |runtime_io| {
        try tokenizer.encodeIntoParallel(
            runtime_io,
            allocator,
            text,
            &ids,
            tokenizer_parallel_max_tasks,
        );
    } else {
        try tokenizer.encodeInto(allocator, text, &ids);
    }
    return ids.items.len;
}

fn countTokenizerTexts(
    allocator: std.mem.Allocator,
    io: ?std.Io,
    tokenizer: anytype,
    texts: []const []const u8,
) !usize {
    var total: usize = 0;
    for (texts) |text| total += try countTokenizerTokens(allocator, io, tokenizer, text);
    return total;
}

fn countParsedDenseEmbedTextTokens(
    allocator: std.mem.Allocator,
    io: ?std.Io,
    tokenizer: anytype,
    inputs: *const ParsedDenseEmbedInputs,
) usize {
    var total: usize = 0;
    for (inputs.texts.items) |item| {
        total += countTokenizerTokens(allocator, io, tokenizer, item.text) catch estimateTextTokens(item.text);
    }
    return total;
}

test "token counting uses the attached std.Io tokenizer path" {
    const ProbeTokenizer = struct {
        serial_calls: *usize,
        parallel_calls: *usize,

        pub fn encodeInto(
            self: @This(),
            allocator: std.mem.Allocator,
            _: []const u8,
            ids: *std.ArrayListUnmanaged(i32),
        ) !void {
            self.serial_calls.* += 1;
            try ids.append(allocator, 1);
        }

        pub fn encodeIntoParallel(
            self: @This(),
            _: std.Io,
            allocator: std.mem.Allocator,
            _: []const u8,
            ids: *std.ArrayListUnmanaged(i32),
            max_tasks: usize,
        ) !void {
            try std.testing.expectEqual(tokenizer_parallel_max_tasks, max_tasks);
            self.parallel_calls.* += 1;
            try ids.appendSlice(allocator, &.{ 1, 2 });
        }
    };

    var serial_calls: usize = 0;
    var parallel_calls: usize = 0;
    const tokenizer = ProbeTokenizer{
        .serial_calls = &serial_calls,
        .parallel_calls = &parallel_calls,
    };
    try std.testing.expectEqual(
        @as(usize, 2),
        try countTokenizerTokens(
            std.testing.allocator,
            std.testing.io,
            tokenizer,
            "parallel",
        ),
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        try countTokenizerTokens(
            std.testing.allocator,
            null,
            tokenizer,
            "serial",
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), parallel_calls);
    try std.testing.expectEqual(@as(usize, 1), serial_calls);
}

fn estimateParsedDenseEmbedPromptTokens(inputs: *const ParsedDenseEmbedInputs) usize {
    var total: usize = 0;
    for (inputs.texts.items) |item| total += estimateTextTokens(item.text);
    return total;
}

fn isOpenAiListTask(task: []const u8) bool {
    return std.mem.eql(u8, task, "generators") or std.mem.eql(u8, task, "embedders");
}

const CompatibilitySummary = model_manager_mod.CompatibilitySummary;
const CompatibilitySignature = [std.crypto.hash.sha2.Sha256.digest_length]u8;
const compatibility_sidecar_hash_limit: u64 = 4 * 1024 * 1024;
const compatibility_artifact_sample_bytes: usize = 64 * 1024;

const CachedCompatibility = struct {
    allocator: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    signature: CompatibilitySignature,
    summary: CompatibilitySummary,
    external_paths: [][]u8,
    external_paths_valid: bool,

    fn create(
        allocator: std.mem.Allocator,
        signature: CompatibilitySignature,
        summary: CompatibilitySummary,
        dependencies: *OnnxDependencies,
    ) !*CachedCompatibility {
        const self = try allocator.create(CachedCompatibility);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .signature = signature,
            .summary = summary,
            .external_paths = try dependencies.paths.toOwnedSlice(allocator),
            .external_paths_valid = dependencies.valid,
        };
        return self;
    }

    fn retain(self: *CachedCompatibility) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }

    fn release(self: *CachedCompatibility) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        for (self.external_paths) |path| self.allocator.free(path);
        self.allocator.free(self.external_paths);
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

const OnnxDependencies = struct {
    paths: std.ArrayListUnmanaged([]u8) = .empty,
    valid: bool = true,

    fn deinit(self: *OnnxDependencies, allocator: std.mem.Allocator) void {
        for (self.paths.items) |path| allocator.free(path);
        self.paths.deinit(allocator);
        self.* = .{};
    }

    fn append(self: *OnnxDependencies, allocator: std.mem.Allocator, path: []const u8) !void {
        for (self.paths.items) |existing| {
            if (std.mem.eql(u8, existing, path)) return;
        }
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);
        try self.paths.append(allocator, owned_path);
    }
};

fn updateCompatibilitySignatureSlice(
    signature: *std.crypto.hash.sha2.Sha256,
    value: []const u8,
) void {
    const len: u64 = @intCast(value.len);
    signature.update(std.mem.asBytes(&len));
    signature.update(value);
}

fn addArtifactIdentityToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    path: ?[]const u8,
    hash_small_contents: bool,
) !void {
    const artifact_path = path orelse return;
    updateCompatibilitySignatureSlice(signature, artifact_path);
    const stat = std.Io.Dir.cwd().statFile(io, artifact_path, .{}) catch {
        signature.update("missing");
        return;
    };
    signature.update(std.mem.asBytes(&stat.inode));
    signature.update(std.mem.asBytes(&stat.size));
    const mtime_ns = stat.mtime.toNanoseconds();
    const ctime_ns = stat.ctime.toNanoseconds();
    // std.Io.Timestamp uses i96, whose ABI representation can contain
    // indeterminate padding. Hash a fully represented integer instead.
    const canonical_mtime_ns: i128 = mtime_ns;
    const canonical_ctime_ns: i128 = ctime_ns;
    signature.update(std.mem.asBytes(&canonical_mtime_ns));
    signature.update(std.mem.asBytes(&canonical_ctime_ns));
    if (hash_small_contents and stat.size <= compatibility_sidecar_hash_limit) {
        const bytes = c_file.readFileMax(
            allocator,
            artifact_path,
            compatibility_sidecar_hash_limit,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            signature.update("content-unreadable");
            return;
        };
        defer allocator.free(bytes);
        updateCompatibilitySignatureSlice(signature, bytes);
    } else if (stat.size > 0) {
        // Stat identity alone misses atomic/in-place deployments that preserve
        // inode, length, and timestamp. Hash bounded samples so cache hits stay
        // O(1) in artifact size while still tracking large graph/weight swaps.
        const sample_len: usize = @intCast(@min(
            stat.size,
            compatibility_artifact_sample_bytes,
        ));
        const offsets = [_]u64{
            0,
            if (stat.size > sample_len) (stat.size - sample_len) / 2 else 0,
            if (stat.size > sample_len) stat.size - sample_len else 0,
        };
        for (offsets, 0..) |offset, index| {
            if (index > 0 and offset == offsets[index - 1]) continue;
            const bytes = c_file.readRegion(allocator, artifact_path, offset, sample_len) catch |err| {
                if (err == error.OutOfMemory) return err;
                signature.update("sample-unreadable");
                return;
            };
            defer allocator.free(bytes);
            signature.update(std.mem.asBytes(&offset));
            updateCompatibilitySignatureSlice(signature, bytes);
        }
    }

    const final_stat = std.Io.Dir.cwd().statFile(io, artifact_path, .{}) catch {
        signature.update("changed-during-read");
        return;
    };
    const final_mtime_ns = final_stat.mtime.toNanoseconds();
    const final_ctime_ns = final_stat.ctime.toNanoseconds();
    if (final_stat.inode != stat.inode or
        final_stat.size != stat.size or
        final_mtime_ns != mtime_ns or
        final_ctime_ns != ctime_ns)
        signature.update("changed-during-read");
}

fn addOnnxArtifactToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    maybe_path: ?[]const u8,
) !void {
    const path = maybe_path orelse return;
    try addArtifactIdentityToSignature(allocator, io, signature, path, false);
}

fn collectOnnxDependenciesForPath(
    allocator: std.mem.Allocator,
    dependencies: *OnnxDependencies,
    maybe_path: ?[]const u8,
) !void {
    const path = maybe_path orelse return;
    if (!std.mem.endsWith(u8, path, ".onnx")) return;
    var artifacts = backends_mod.imported_onnx_session.inspectArtifactSet(
        allocator,
        path,
    ) catch |err| {
        if (err == error.OutOfMemory) return err;
        dependencies.valid = false;
        return;
    };
    defer artifacts.deinit();
    for (artifacts.external_paths) |external_path| {
        try dependencies.append(allocator, external_path);
    }
}

fn collectCompatibilityOnnxDependencies(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
) !OnnxDependencies {
    var dependencies = OnnxDependencies{};
    errdefer dependencies.deinit(allocator);
    const paths = [_]?[]const u8{
        man.onnx_path,
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    };
    for (paths) |path| try collectOnnxDependenciesForPath(allocator, &dependencies, path);
    return dependencies;
}

fn addModelSidecarToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    model_path: []const u8,
    sidecar_name: []const u8,
) !void {
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const separator = if (std.mem.endsWith(u8, model_path, "/")) "" else "/";
    const path = std.fmt.bufPrint(
        &path_buffer,
        "{s}{s}{s}",
        .{ model_path, separator, sidecar_name },
    ) catch {
        // Keep overlong paths distinct and deterministically uncacheable from a
        // shorter valid sidecar path.
        signature.update(model_path);
        signature.update(sidecar_name);
        signature.update("sidecar-path-too-long");
        return;
    };
    try addArtifactIdentityToSignature(allocator, io, signature, path, true);
}

fn addShardedArtifactStatsToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    index_path: ?[]const u8,
) !void {
    const path = index_path orelse return;
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    var index = try safetensors_mod.ShardedIndex.load(allocator, bytes);
    defer index.deinit();
    const model_dir = std.fs.path.dirname(path) orelse return error.InvalidPath;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(allocator);
    var it = index.weight_map.iterator();
    while (it.next()) |entry| {
        const shard = entry.value_ptr.*;
        if (seen.contains(shard)) continue;
        try seen.put(allocator, shard, {});
        const shard_path = try std.fs.path.join(allocator, &.{ model_dir, shard });
        defer allocator.free(shard_path);
        try addArtifactIdentityToSignature(
            allocator,
            io,
            signature,
            shard_path,
            false,
        );
    }
}

fn addCompatibilityManifestFacts(
    signature: *std.crypto.hash.sha2.Sha256,
    man: *const manifest_mod.ModelManifest,
) void {
    const model_type: u8 = @intFromEnum(man.model_type);
    const native_arch_hint: u8 = @intFromEnum(man.native_arch_hint);
    signature.update(&.{ model_type, native_arch_hint });
    updateCompatibilitySignatureSlice(signature, man.config_model_arch);
    updateCompatibilitySignatureSlice(signature, man.gliner_model_type);
    updateCompatibilitySignatureSlice(signature, man.inference_bundle_family);
    signature.update(&.{
        @intFromBool(man.hasIncompleteGlinerBundle()),
        @intFromBool(man.hasIncompleteColqwenBundle()),
        @intFromBool(man.hasIncompleteClipclapGgufBundle()),
        @intFromBool(man.hasIncompleteFlorence2GgufBundle()),
        @intFromBool(man.isClipclapGgufBundle()),
    });
}

fn computeCompatibilitySignatureWithDependencies(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_path: []const u8,
    man: *const manifest_mod.ModelManifest,
    external_paths: []const []const u8,
    external_paths_valid: bool,
) !CompatibilitySignature {
    var signature = std.crypto.hash.sha2.Sha256.init(.{});
    addCompatibilityManifestFacts(&signature, man);

    // Hash every small metadata input parsed by loadListingFromDir. Content
    // hashing avoids stale policy decisions when deployment tools preserve
    // size and timestamps during an in-place replacement.
    for (manifest_mod.listing_compatibility_sidecars) |sidecar_name| {
        try addModelSidecarToSignature(
            allocator,
            io,
            &signature,
            model_path,
            sidecar_name,
        );
    }
    try addArtifactIdentityToSignature(allocator, io, &signature, man.model_manifest_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.config_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gguf_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gguf_projector_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.safetensors_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.safetensors_index_path, true);
    addShardedArtifactStatsToSignature(
        allocator,
        io,
        &signature,
        man.safetensors_index_path,
    ) catch |err| {
        if (err == error.OutOfMemory) return err;
        signature.update("invalid-sharded-index");
    };
    try addOnnxArtifactToSignature(allocator, io, &signature, man.onnx_path);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gliner_head_gguf_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gliner_head_safetensors_path, false);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.visual_model_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.audio_model_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.text_projection_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.visual_projection_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.audio_projection_path);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.tokenizer_json_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.tokenizer_config_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.preprocessor_config_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.processor_config_path, true);
    if (!external_paths_valid) signature.update("invalid-onnx-artifact-set");
    for (external_paths) |external_path| {
        try addArtifactIdentityToSignature(
            allocator,
            io,
            &signature,
            external_path,
            false,
        );
    }

    var digest: CompatibilitySignature = undefined;
    signature.final(&digest);
    return digest;
}

fn computeCompatibilitySignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_path: []const u8,
    man: *const manifest_mod.ModelManifest,
) !CompatibilitySignature {
    var dependencies = try collectCompatibilityOnnxDependencies(allocator, man);
    defer dependencies.deinit(allocator);
    return computeCompatibilitySignatureWithDependencies(
        allocator,
        io,
        model_path,
        man,
        dependencies.paths.items,
        dependencies.valid,
    );
}

test "compatibility signature tracks implicit GLiNER classification sidecar" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..] },
    );
    defer allocator.free(model_dir);

    var missing = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &missing,
        model_dir,
        "special_tokens_map.json",
    );

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "special_tokens_map.json",
        .data = "{\"additional_special_tokens\":[\"[P]\",\"[C]\",\"[E]\",\"[R]\",\"[SEP_TEXT]\"]}",
    });
    var present = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &present,
        model_dir,
        "special_tokens_map.json",
    );
    var missing_digest: CompatibilitySignature = undefined;
    var present_digest: CompatibilitySignature = undefined;
    missing.final(&missing_digest);
    present.final(&present_digest);
    try std.testing.expect(!std.mem.eql(
        u8,
        missing_digest[0..],
        present_digest[0..],
    ));
}

test "compatibility signature hashes same-size sidecar replacements" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..] },
    );
    defer allocator.free(model_dir);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"bert\"}",
    });
    var first = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &first,
        model_dir,
        "config.json",
    );
    var first_digest: CompatibilitySignature = undefined;
    first.final(&first_digest);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"bart\"}",
    });
    var second = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &second,
        model_dir,
        "config.json",
    );
    var second_digest: CompatibilitySignature = undefined;
    second.final(&second_digest);
    try std.testing.expect(!std.mem.eql(
        u8,
        first_digest[0..],
        second_digest[0..],
    ));
}

test "cached compatibility signature tracks known ONNX external dependencies without reparsing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..] },
    );
    defer allocator.free(model_dir);
    const external_path = try std.fs.path.join(
        allocator,
        &.{ model_dir, "weights.bin" },
    );
    defer allocator.free(external_path);
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "weights.bin",
        .data = "first-external-generation",
    });

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();
    const first = try computeCompatibilitySignatureWithDependencies(
        allocator,
        std.testing.io,
        model_dir,
        &man,
        &.{external_path},
        true,
    );
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "weights.bin",
        .data = "second-external-generat",
    });
    const second = try computeCompatibilitySignatureWithDependencies(
        allocator,
        std.testing.io,
        model_dir,
        &man,
        &.{external_path},
        true,
    );
    try std.testing.expect(!std.mem.eql(u8, first[0..], second[0..]));
}

/// Apply the serving policy before model loading. `ModelManager` repeats this check as a
/// safety backstop so a new endpoint or direct Node helper cannot bypass it.
fn rejectDisallowedModel(
    self: *Node,
    ctx: *httpx.Context,
    model_path: []const u8,
) !?httpx.Response {
    const summary = self.compatibilitySummaryForDir(ctx.allocator, model_path) catch {
        if (self.config.allow_unknown_models) return null;
        return try ctx.status(400).json(.{
            .@"error" = "UNKNOWN_MODEL_COMPATIBILITY",
            .message = "model compatibility could not be determined; restart with --allow-unknown-models to opt in",
        });
    };
    switch (summary.level) {
        .compatible => return null,
        .unknown => {
            if (self.config.allow_unknown_models) return null;
            return try ctx.status(400).json(.{
                .@"error" = "UNKNOWN_MODEL_COMPATIBILITY",
                .message = summary.message,
            });
        },
        .incompatible => return try ctx.status(400).json(.{
            .@"error" = "INCOMPATIBLE_MODEL",
            .message = summary.message,
        }),
    }
}

const transient_capacity_retry_after_seconds = "1";
const transient_capacity_retry_after_ms = 1000;

fn transientCapacityFailureResponse(
    ctx: *httpx.Context,
    code: []const u8,
    message: []const u8,
    reason: []const u8,
) !httpx.Response {
    // Retry-After is expressed in seconds by HTTP. The millisecond field keeps
    // SDK clients from having to parse headers and makes retryability explicit.
    try ctx.setHeader("Retry-After", transient_capacity_retry_after_seconds);
    return ctx.status(503).json(.{
        .@"error" = code,
        .message = message,
        .reason = reason,
        .retryable = true,
        .retry_after_ms = transient_capacity_retry_after_ms,
    });
}

fn modelResourceBusyResponse(ctx: *httpx.Context) !httpx.Response {
    return transientCapacityFailureResponse(
        ctx,
        "MODEL_RESOURCE_BUSY",
        "insufficient inference capacity is currently available",
        "inference_capacity",
    );
}

fn modelArtifactsChangingResponse(ctx: *httpx.Context) !httpx.Response {
    return transientCapacityFailureResponse(
        ctx,
        "MODEL_ARTIFACTS_CHANGING",
        "model artifacts are being published; retry with a consistent generation",
        "model_publication",
    );
}

fn modelLoadFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.UnknownModelCompatibility => ctx.status(400).json(.{
            .@"error" = "UNKNOWN_MODEL_COMPATIBILITY",
            .message = "model compatibility is unknown; restart with --allow-unknown-models to opt in",
        }),
        error.IncompatibleModel => ctx.status(400).json(.{
            .@"error" = "INCOMPATIBLE_MODEL",
            .message = "model artifact is incompatible with the available runtime",
        }),
        error.ResourceLimitExceeded => ctx.status(400).json(.{
            .@"error" = "MODEL_RESOURCE_LIMIT",
            .message = "model resource plan exceeds the configured inference budget",
        }),
        error.ResourceTemporarilyUnavailable => modelResourceBusyResponse(ctx),
        error.ModelArtifactsChanging => modelArtifactsChangingResponse(ctx),
        else => ctx.status(500).json(.{
            .@"error" = "MODEL_LOAD_FAILED",
            .message = @errorName(err),
        }),
    };
}

fn inferenceFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.ResourceLimitExceeded => ctx.status(400).json(.{
            .@"error" = "MODEL_RESOURCE_LIMIT",
            .message = "request resource plan exceeds the configured inference budget",
        }),
        error.ResourceTemporarilyUnavailable => modelResourceBusyResponse(ctx),
        else => ctx.status(500).json(.{
            .@"error" = "INFERENCE_FAILED",
            .message = @errorName(err),
        }),
    };
}

fn rejectExplicitBackendIncompatibility(
    ctx: *httpx.Context,
    model_path: []const u8,
    choice: native_backend_choice.Choice,
    allow_unknown: bool,
) !?httpx.Response {
    const backend: backends_mod.BackendType = switch (choice) {
        .auto => return null,
        .onnx => .onnx,
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .xla, .webgpu => return null,
    };
    var man = try manifest_mod.loadListingFromDir(ctx.allocator, model_path);
    defer man.deinit();
    const summary = try model_manager_mod.compatibilitySummaryForBackend(
        ctx.allocator,
        model_path,
        &man,
        backend,
    ) orelse {
        return try ctx.status(400).json(.{
            .@"error" = "INCOMPATIBLE_MODEL",
            .message = "the selected backend has no compatible artifact in this model bundle",
        });
    };
    if (summary.level == .incompatible or
        (summary.level == .unknown and !allow_unknown))
    {
        return try ctx.status(400).json(.{
            .@"error" = if (summary.level == .unknown)
                "UNKNOWN_MODEL_COMPATIBILITY"
            else
                "INCOMPATIBLE_MODEL",
            .message = summary.message,
        });
    }
    return null;
}

fn appendOpenAiModelEntry(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    model_id: []const u8,
    created: i64,
    compatibility_level: []const u8,
) !void {
    try buf.appendSlice(allocator, "{\"id\":");
    try jsonEncodeString(buf, allocator, model_id);
    const metadata = try std.fmt.allocPrint(
        allocator,
        ",\"object\":\"model\",\"created\":{d},\"owned_by\":\"antfly\",\"compatibility\":",
        .{created},
    );
    defer allocator.free(metadata);
    try buf.appendSlice(allocator, metadata);
    try jsonEncodeString(buf, allocator, compatibility_level);
    try buf.append(allocator, '}');
}

test "OpenAI model entries expose artifact compatibility" {
    const allocator = std.testing.allocator;
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try appendOpenAiModelEntry(
        &buf,
        allocator,
        "owner/model",
        42,
        "unknown",
    );
    try std.testing.expectEqualStrings(
        "{\"id\":\"owner/model\",\"object\":\"model\",\"created\":42,\"owned_by\":\"antfly\",\"compatibility\":\"unknown\"}",
        buf.items,
    );
}

fn appendUniqueOpenAiModelEntry(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    seen: *std.StringHashMapUnmanaged(void),
    count: *usize,
    model_id: []const u8,
    created: i64,
    compatibility_level: []const u8,
) !void {
    const entry = try seen.getOrPut(allocator, model_id);
    if (entry.found_existing) return;
    if (count.* > 0) try buf.append(allocator, ',');
    try appendOpenAiModelEntry(
        buf,
        allocator,
        model_id,
        created,
        compatibility_level,
    );
    count.* += 1;
}

const DiscoveredModelListing = struct {
    entry_index: usize,
    manifest: manifest_mod.ModelManifest,
    reader_supported: bool,
    /// `@tagName(manifest.model_type)`, not the path-derived registry kind.
    kind: []const u8,
    /// Compatibility derived from the artifact and available runtime paths.
    compatibility_level: []const u8,

    fn deinit(self: *@This()) void {
        self.manifest.deinit();
        self.* = undefined;
    }
};

const ModelCounts = struct {
    embedders: usize = 0,
    rerankers: usize = 0,
    chunkers: usize = 0,
    generators: usize = 0,
    classifiers: usize = 0,
    rewriters: usize = 0,
    readers: usize = 0,
    transcribers: usize = 0,
    extractors: usize = 0,

    fn total(self: @This()) usize {
        return self.embedders +
            self.rerankers +
            self.chunkers +
            self.generators +
            self.classifiers +
            self.rewriters +
            self.readers +
            self.transcribers +
            self.extractors;
    }
};

fn incrementModelCount(counts: *ModelCounts, task: []const u8) void {
    if (std.mem.eql(u8, task, "embedders")) counts.embedders += 1 else if (std.mem.eql(u8, task, "rerankers")) counts.rerankers += 1 else if (std.mem.eql(u8, task, "chunkers")) counts.chunkers += 1 else if (std.mem.eql(u8, task, "generators")) counts.generators += 1 else if (std.mem.eql(u8, task, "classifiers")) counts.classifiers += 1 else if (std.mem.eql(u8, task, "rewriters")) counts.rewriters += 1 else if (std.mem.eql(u8, task, "readers")) counts.readers += 1 else if (std.mem.eql(u8, task, "transcribers")) counts.transcribers += 1 else if (std.mem.eql(u8, task, "extractors")) counts.extractors += 1;
}

fn collectModelCounts(node: *Node, allocator: std.mem.Allocator, io: std.Io) ModelCounts {
    const task_names = [_][]const u8{
        "embedders",  "rerankers",   "chunkers",
        "generators", "classifiers", "extractors",
        "rewriters",  "readers",     "transcribers",
    };
    var counts = ModelCounts{};

    const ra = node.registry.allocator;
    const discovered = node.registry.discover(io) catch &[_]registry_mod.ModelEntry{};
    defer {
        for (discovered) |entry| {
            ra.free(entry.name);
            ra.free(entry.path);
        }
        if (discovered.len > 0) ra.free(discovered);
    }

    for (discovered) |entry| {
        if (!model_manager_mod.isModelDirPotentiallyLoadableInCurrentBuild(allocator, entry.path)) continue;

        var maybe_manifest: ?manifest_mod.ModelManifest = manifest_mod.loadFromDir(allocator, entry.path) catch null;
        defer if (maybe_manifest) |*man| man.deinit();

        const tasks = if (maybe_manifest) |*man| man.tasks else &.{};
        const capabilities = if (maybe_manifest) |*man| man.capabilities else &.{};
        const gliner_model_type = if (maybe_manifest) |*man| man.gliner_model_type else "";

        for (task_names) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (std.mem.eql(u8, task, "readers") and !readers_mod.isSupportedModelDir(allocator, entry.path)) continue;
            if (taskMatchesModelListing(task, @tagName(entry.kind), gliner_model_type, tasks, capabilities)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    node.model_manager.lockLoadedModels();
    defer node.model_manager.unlockLoadedModels();
    var it = node.model_manager.loaded.iterator();
    while (it.next()) |entry| {
        var already_listed = false;
        for (discovered) |d| {
            if (std.mem.eql(u8, d.path, entry.value_ptr.*.model_dir)) {
                already_listed = true;
                break;
            }
        }
        if (already_listed) continue;

        const model = entry.value_ptr.*;
        const model_task = @tagName(model.manifest.model_type);
        for (task_names) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (taskMatchesModelListing(task, model_task, model.manifest.gliner_model_type, model.manifest.tasks, model.manifest.capabilities)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    return counts;
}

fn collectDiscoveredModelCounts(models_dir: []const u8, allocator: std.mem.Allocator, io: std.Io) ModelCounts {
    const task_names = [_][]const u8{
        "embedders",  "rerankers",   "chunkers",
        "generators", "classifiers", "extractors",
        "rewriters",  "readers",     "transcribers",
    };
    var counts = ModelCounts{};

    var registry = registry_mod.ModelRegistry.init(allocator, models_dir);
    defer registry.deinit();
    const discovered = registry.discover(io) catch &[_]registry_mod.ModelEntry{};
    defer {
        for (discovered) |entry| {
            allocator.free(entry.name);
            allocator.free(entry.path);
        }
        if (discovered.len > 0) allocator.free(discovered);
    }

    for (discovered) |entry| {
        if (!model_manager_mod.isModelDirPotentiallyLoadableInCurrentBuild(allocator, entry.path)) continue;

        var maybe_manifest: ?manifest_mod.ModelManifest = manifest_mod.loadFromDir(allocator, entry.path) catch null;
        defer if (maybe_manifest) |*man| man.deinit();

        const tasks = if (maybe_manifest) |*man| man.tasks else &.{};
        const capabilities = if (maybe_manifest) |*man| man.capabilities else &.{};
        const gliner_model_type = if (maybe_manifest) |*man| man.gliner_model_type else "";

        for (task_names) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (std.mem.eql(u8, task, "readers") and !readers_mod.isSupportedModelDir(allocator, entry.path)) continue;
            if (taskMatchesModelListing(task, @tagName(entry.kind), gliner_model_type, tasks, capabilities)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    return counts;
}

const ExtractionResolutionFailure = enum {
    not_found,
    resource_exhausted,
    internal,
};

fn classifyExtractionResolutionFailure(err: anyerror) ExtractionResolutionFailure {
    return switch (err) {
        error.ModelNotFound => .not_found,
        error.OutOfMemory => .resource_exhausted,
        else => .internal,
    };
}

pub const Node = struct {
    config: NodeConfig,
    allocator: std.mem.Allocator,
    session_manager: backends_mod.SessionManager,
    model_manager: model_manager_mod.ModelManager,
    registry: registry_mod.ModelRegistry,
    extraction_reader_resolver: extractors_mod.ReaderResolver,
    tabular_registry: tabular_mod.registry.Registry,
    embed_cache: cache_mod.ResultCache([]const f32),
    metrics: metrics_mod.Metrics,
    request_queue: request_queue_mod.RequestQueue,
    compatibility_cache: std.StringHashMapUnmanaged(*CachedCompatibility) = .empty,
    compatibility_cache_lock: std.atomic.Mutex = .unlocked,

    pub const DirectSparseEmbedding = sparse_embedding_mod.SparseVector;

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Node {
        var node: Node = .{
            .config = config,
            .allocator = allocator,
            .session_manager = backends_mod.SessionManager.init(allocator),
            .model_manager = model_manager_mod.ModelManager.init(allocator, backends_mod.SessionManager.init(allocator)),
            .registry = registry_mod.ModelRegistry.init(allocator, config.models_dir),
            .extraction_reader_resolver = extractors_mod.ReaderResolver.init(allocator),
            .tabular_registry = tabular_mod.registry.Registry.init(allocator),
            .embed_cache = cache_mod.ResultCache([]const f32).init(allocator, 120_000),
            .metrics = metrics_mod.Metrics.default,
            .request_queue = request_queue_mod.RequestQueue.init(config.max_concurrent_requests),
            .compatibility_cache = .empty,
        };
        node.model_manager.configureServingPolicy(.{
            .allow_unknown = config.allow_unknown_models,
        });
        node.model_manager.configureModelCache(
            config.keep_alive_ms,
            config.max_loaded_models,
        );
        node.model_manager.configureAdmissionLimits(.{
            .host_limit_bytes = config.generation_budget_overrides.host_limit_bytes,
            .backend_limit_bytes = config.generation_budget_overrides.backend_limit_bytes,
            .combined_limit_bytes = config.generation_budget_overrides.combined_limit_bytes,
            .kv_limit_bytes = config.generation_budget_overrides.kv_limit_bytes,
            .scratch_limit_bytes = config.generation_budget_overrides.scratch_limit_bytes,
        });
        node.model_manager.tokenizer_cache_config = config.tokenizer_cache;
        node.model_manager.tokenizer_parallel_bpe_config =
            config.tokenizer_parallel_bpe;
        node.updateQueueMetrics();
        return node;
    }

    /// Attach process-wide tokenizer-cache admission before loading models.
    pub fn configureTokenizerCaches(
        self: *Node,
        config: hf_tokenizer_mod.HfTokenizer.BpeCacheConfig,
    ) !void {
        try self.model_manager.configureTokenizerCaches(config);
        self.config.tokenizer_cache = config;
    }

    pub fn configureAdmissionResourceBudget(
        self: *Node,
        resource_budget: ?runtime.tier.memory.AdmissionResourceBudget,
    ) void {
        self.model_manager.configureAdmissionResourceBudget(resource_budget);
    }

    /// Configure the std.Io tokenizer scheduler and optional consumer-local
    /// tables before model load. Table memory is admitted by the cache
    /// resource budget configured above.
    pub fn configureTokenizerParallelBpe(
        self: *Node,
        config: hf_tokenizer_mod.HfTokenizer.ParallelBpeConfig,
    ) !void {
        try self.model_manager.configureTokenizerParallelBpe(config);
        self.config.tokenizer_parallel_bpe = config;
    }

    pub fn deinit(self: *Node) void {
        self.model_manager.deinit();
        self.registry.deinit();
        self.extraction_reader_resolver.deinit();
        self.tabular_registry.deinit();
        self.embed_cache.deinit();
        var compatibility_it = self.compatibility_cache.iterator();
        while (compatibility_it.next()) |entry| {
            entry.value_ptr.*.release();
            self.allocator.free(entry.key_ptr.*);
        }
        self.compatibility_cache.deinit(self.allocator);
    }

    /// Derive artifact compatibility once per immutable artifact signature. Discovery
    /// calls this repeatedly, so caching prevents GGUF/ONNX metadata inspection from
    /// becoming request-path work while still invalidating when a sidecar is replaced.
    fn compatibilitySummaryForDir(
        self: *Node,
        allocator: std.mem.Allocator,
        model_path: []const u8,
    ) !CompatibilitySummary {
        var io_impl: ?std.Io.Threaded = null;
        defer if (io_impl) |*threaded| threaded.deinit();
        const io = self.session_manager.io orelse blk: {
            io_impl = std.Io.Threaded.init(allocator, .{});
            break :blk io_impl.?.io();
        };

        // A deployment can replace several sidecars and artifacts without an
        // atomic directory snapshot. Derive and validate the manifest twice so
        // the cached assessment is published only when both parsed facts and
        // artifact identities describe the same stable generation.
        const max_snapshot_attempts = 3;
        var attempt: usize = 0;
        while (attempt < max_snapshot_attempts) : (attempt += 1) {
            var man = try manifest_mod.loadListingFromDir(allocator, model_path);
            defer man.deinit();

            spinLock(&self.compatibility_cache_lock);
            const cached = self.compatibility_cache.get(model_path);
            if (cached) |entry| entry.retain();
            self.compatibility_cache_lock.unlock();
            if (cached) |entry| {
                const cached_signature = computeCompatibilitySignatureWithDependencies(
                    allocator,
                    io,
                    model_path,
                    &man,
                    entry.external_paths,
                    entry.external_paths_valid,
                ) catch |err| {
                    entry.release();
                    return err;
                };
                const cache_hit = std.mem.eql(
                    u8,
                    entry.signature[0..],
                    cached_signature[0..],
                );
                const cached_summary = entry.summary;
                entry.release();
                if (cache_hit) return cached_summary;
            }

            var dependencies = try collectCompatibilityOnnxDependencies(allocator, &man);
            defer dependencies.deinit(allocator);
            const artifact_signature = try computeCompatibilitySignatureWithDependencies(
                allocator,
                io,
                model_path,
                &man,
                dependencies.paths.items,
                dependencies.valid,
            );

            const summary = try model_manager_mod.compatibilitySummaryForBackends(
                allocator,
                model_path,
                &man,
                self.session_manager.preferred_backends,
            );

            var verification_manifest = try manifest_mod.loadListingFromDir(
                allocator,
                model_path,
            );
            defer verification_manifest.deinit();
            var verification_dependencies = try collectCompatibilityOnnxDependencies(
                allocator,
                &verification_manifest,
            );
            defer verification_dependencies.deinit(allocator);
            const verification_signature = try computeCompatibilitySignatureWithDependencies(
                allocator,
                io,
                model_path,
                &verification_manifest,
                verification_dependencies.paths.items,
                verification_dependencies.valid,
            );
            if (!std.mem.eql(
                u8,
                artifact_signature[0..],
                verification_signature[0..],
            )) continue;

            const new_cached = try CachedCompatibility.create(
                self.allocator,
                artifact_signature,
                summary,
                &verification_dependencies,
            );
            var replaced: ?*CachedCompatibility = null;
            spinLock(&self.compatibility_cache_lock);
            if (self.compatibility_cache.getPtr(model_path)) |cached_ptr| {
                replaced = cached_ptr.*;
                cached_ptr.* = new_cached;
            } else {
                const owned_path = self.allocator.dupe(u8, model_path) catch |err| {
                    self.compatibility_cache_lock.unlock();
                    new_cached.release();
                    return err;
                };
                self.compatibility_cache.put(
                    self.allocator,
                    owned_path,
                    new_cached,
                ) catch |err| {
                    self.allocator.free(owned_path);
                    self.compatibility_cache_lock.unlock();
                    new_cached.release();
                    return err;
                };
            }
            self.compatibility_cache_lock.unlock();
            if (replaced) |old| old.release();
            return summary;
        }
        return error.ModelArtifactsChanging;
    }

    pub fn attachIo(self: *Node, io: std.Io) void {
        self.session_manager.io = io;
        self.model_manager.attachIo(io);
    }

    pub fn embedDenseTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![][]f32 {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        return try self.embedDenseTextsDirectWithContext(allocator, io_impl.io(), null, model_name, texts);
    }

    pub fn embedDenseTextsDirectWithContext(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        deadline_ns: ?u64,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![][]f32 {
        if (texts.len == 0) return try allocator.alloc([]f32, 0);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        try self.request_queue.acquire();
        self.updateQueueMetrics();
        defer self.releaseSlot();
        self.metrics.incRequest("embed.local");
        defer self.metrics.decActive();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "embedders");
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        try ensureDirectEmbeddingDeadline(deadline_ns);
        if (model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
        var asset_lease = model.acquireEmbeddingAssetLease(false);
        defer asset_lease.release();
        try model.ensureEmbeddingAssets(true, false, false);
        try ensureDirectEmbeddingDeadline(deadline_ns);

        var pipeline = model.embeddingPipeline(allocator);
        const vectors = try pipeline.embed(texts);
        asset_lease.release();
        errdefer freeDirectDenseVectors(allocator, vectors);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        return vectors;
    }

    pub fn embedSparseTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![]DirectSparseEmbedding {
        if (texts.len == 0) return try allocator.alloc(DirectSparseEmbedding, 0);
        try self.request_queue.acquire();
        self.updateQueueMetrics();
        defer self.releaseSlot();
        self.metrics.incRequest("embed_sparse.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "embedders");
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        if (!model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
        var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
            .allocator = allocator,
            .session = model.session,
            .tok = model.getTokenizer(),
            .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
            .execution_lock = model.embeddingExecutionLock(),
        };
        return try pipeline.embed(texts);
    }

    pub fn rerankTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        query: []const u8,
        documents: []const []const u8,
    ) ![]f32 {
        if (documents.len == 0) return try allocator.alloc(f32, 0);
        try self.request_queue.acquire();
        self.updateQueueMetrics();
        defer self.releaseSlot();
        self.metrics.incRequest("rerank.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "rerankers");
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        var pipeline = model.rerankingPipeline(allocator);
        return try pipeline.rerank(query, documents);
    }

    pub fn generateTextDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        roles: []const []const u8,
        contents: []const []const u8,
    ) ![]u8 {
        if (roles.len != contents.len) return error.InvalidGenerationRequest;
        if (roles.len == 0) return error.InvalidGenerationRequest;

        var messages = try allocator.alloc(generation.Message, roles.len);
        defer allocator.free(messages);
        for (roles, contents, 0..) |role, content, i| {
            messages[i] = .{
                .role = role,
                .content = content,
            };
        }

        return self.generateMessagesDirectMaxTokens(allocator, model_name, messages, 256, null, null, false);
    }

    pub fn generateMessagesDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        messages: []const generation.Message,
    ) ![]u8 {
        return self.generateMessagesDirectMaxTokens(allocator, model_name, messages, 256, null, null, false);
    }

    const DirectGenerateTiming = struct {
        resolve_ms: u64 = 0,
        load_ms: u64 = 0,
        setup_ms: u64 = 0,
        generate_ms: u64 = 0,
        total_ms: u64 = 0,
    };

    fn countPromptTokens(
        allocator: std.mem.Allocator,
        model: *model_manager_mod.LoadedModel,
        messages: []const generation.Message,
    ) !usize {
        const prompt = if (model.chat_tmpl) |ct|
            try ct.apply(allocator, messages, true)
        else
            try generation.formatMessages(allocator, messages);
        defer allocator.free(prompt);

        var encoded = try generation.encodePromptForGeneration(
            model.getTokenizer(),
            allocator,
            prompt,
            2048,
            model.manifest.add_bos_token,
            model.manifest.bos_token,
        );
        defer encoded.deinit();

        var prompt_tokens: usize = 0;
        while (prompt_tokens < encoded.attention_mask.len and encoded.attention_mask[prompt_tokens] != 0) : (prompt_tokens += 1) {}
        if (prompt_tokens == 0) return error.EmptyPrompt;
        return prompt_tokens;
    }

    fn generateMessagesDirectMaxTokens(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        messages: []const generation.Message,
        max_tokens: i32,
        preferred_backends: ?[]const backends_mod.BackendType,
        timing: ?*DirectGenerateTiming,
        pin_after_success: bool,
    ) ![]u8 {
        if (messages.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();

        const queue_units = self.estimateGenerateQueueUnits(messages, max_tokens);
        try self.request_queue.acquireUnits(queue_units);
        self.updateQueueMetrics();
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("generate.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "generators");
        const resolved_at_ns = embedTimingNowNs();
        var model_handle = if (preferred_backends) |backends|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, backends, false)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        const loaded_at_ns = embedTimingNowNs();
        model.lockNativeGeneration();
        defer model.unlockNativeGeneration();
        const gpt_config = session_factory.getGptConfig(model.session) orelse return error.UnsupportedGeneratorProvider;
        const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            .pjrt, .onnx, .wasm => return error.UnsupportedGeneratorProvider,
        };
        const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
        const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
            .native => .cpu,
            .metal, .cuda => .gpu,
        };
        const budget_limits = self.config.generation_budget_overrides.apply(session_factory.widenBudgetLimitsForSession(
            model.session,
            runtime.tier.memory.defaultLimitsForBackend(budget_backend_class),
        ));
        var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
        const prompt_tokens = try countPromptTokens(allocator, model, messages);
        const resource_estimate = try runtime.tier.memory.estimateGptGeneration(
            backend_kind,
            kv_dtype,
            gpt_config,
            prompt_tokens,
            @intCast(@max(max_tokens, 1)),
            256,
        );
        run_budget.reserveEstimate(resource_estimate) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                var buf: [512]u8 = undefined;
                std.log.warn("{s}", .{
                    session_factory.memoryBudgetExceededDetail(model.session, &run_budget, &buf) catch
                        "request exceeds native generation memory budget",
                });
            }
            return err;
        };
        var admission_lease = try self.model_manager.acquireRunResources(
            budget_backend_class,
            budget_limits,
            resource_estimate,
        );
        defer admission_lease.release();

        var kv_manager = runtime.kv.manager.KvManager.init(allocator);
        defer kv_manager.deinit();
        var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                var buf: [512]u8 = undefined;
                std.log.warn("{s}", .{
                    session_factory.memoryBudgetExceededDetail(model.session, &run_budget, &buf) catch
                        "request exceeds native generation memory budget",
                });
            }
            return err;
        };
        defer cb.deinit();

        const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
            null
        else if (gpt_config.sliding_window > 0)
            gpt_config.sliding_window
        else if (gpt_config.max_position_embeddings > 0)
            gpt_config.max_position_embeddings
        else
            null;
        const pool_id = try kv_manager.addPool(.{
            .backend = backend_kind,
            .dtype = kv_dtype,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
            .num_kv_heads = gpt_config.maxKvHeads(),
            .head_dim = gpt_config.maxHeadDim(),
            .sliding_window_size = sliding_window_size,
        });
        var kv_storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, .{
            .backend = backend_kind,
            .dtype = kv_dtype,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
            .num_kv_heads = gpt_config.maxKvHeads(),
            .head_dim = gpt_config.maxHeadDim(),
            .sliding_window_size = sliding_window_size,
        });
        defer kv_storage.deinit();
        try cb.provisionKvDeviceWriteHook(&kv_storage);
        var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
        decode_state.kv_storage = &kv_storage;
        defer decode_state.deinit();

        const use_metal_whole_model = build_options.enable_metal and
            model.session.backend() == .metal and
            graph_mod.metal_executor.supportsSession(model.session) and
            !generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config);

        var pipeline = generation.NativeGenerationPipeline{
            .allocator = allocator,
            .io = io,
            .cb = cb,
            .session = model.session,
            .gpt_config = gpt_config,
            .kv_dtype = kv_dtype,
            .tokenizer = model.getTokenizer(),
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .chat_template = model.chat_tmpl,
            .print_timing = timing != null,
            .model_dir = model_path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
            .graph_cache = if (use_metal_whole_model) &model.native_generation_graph_cache else null,
            .compiled_partition_backend = if (use_metal_whole_model) .metal else null,
            .compiled_attachment_target = if (use_metal_whole_model) .whole_model else .partitioned,
        };
        const debug_metal_timing = timing != null and use_metal_whole_model and platform.env.getenvBool("TERMITE_DEBUG_METAL_TIMING");
        if (debug_metal_timing) graph_mod.metal_executor.resetTimingStats();
        const setup_at_ns = embedTimingNowNs();
        var result = try pipeline.generate(messages, .{ .max_tokens = max_tokens, .prefill_chunk_size = 256 });
        const generated_at_ns = embedTimingNowNs();
        defer result.deinit();
        if (debug_metal_timing) {
            if (model.native_generation_graph_cache.getSessionCompiledModelRuntime(.metal, .whole_model)) |runtime_model| {
                runtime_model.printDebugTiming();
            }
        }
        if (timing) |t| {
            t.* = .{
                .resolve_ms = elapsedMs(started_at_ns, resolved_at_ns),
                .load_ms = elapsedMs(resolved_at_ns, loaded_at_ns),
                .setup_ms = elapsedMs(loaded_at_ns, setup_at_ns),
                .generate_ms = elapsedMs(setup_at_ns, generated_at_ns),
                .total_ms = elapsedMs(started_at_ns, generated_at_ns),
            };
        }
        const text = try allocator.dupe(u8, result.text);
        if (pin_after_success) model_handle.pin();
        return text;
    }

    pub fn warmConfiguredModels(self: *Node, allocator: std.mem.Allocator) !void {
        for (self.config.preload) |model| {
            self.warmModel(allocator, model) catch |err| {
                if (model.backend) |backend| {
                    std.log.err(
                        "failed to warm configured inference model kind={s} model={s} backend={s} err={}",
                        .{ @tagName(model.kind), model.name, @tagName(backend), err },
                    );
                } else {
                    std.log.err(
                        "failed to warm configured inference model kind={s} model={s} backend=auto err={}",
                        .{ @tagName(model.kind), model.name, err },
                    );
                }
                return err;
            };
        }
    }

    pub fn warmConfiguredGenerators(self: *Node, allocator: std.mem.Allocator) !void {
        try self.warmConfiguredModels(allocator);
    }

    pub fn warmModel(self: *Node, allocator: std.mem.Allocator, model: WarmModel) !void {
        switch (model.kind) {
            .generator => try self.warmGeneratorWithBackend(allocator, model.name, model.backend),
            .embedder => try self.warmEmbedder(allocator, model.name, model.backend),
            .reranker => try self.warmReranker(allocator, model.name, model.backend),
            .chunker, .classifier, .rewriter, .reader, .transcriber, .extractor => try self.warmLoadOnlyModel(allocator, model),
        }
    }

    pub fn warmGenerator(self: *Node, allocator: std.mem.Allocator, model_name: []const u8) !void {
        try self.warmGeneratorWithBackend(allocator, model_name, null);
    }

    fn warmGeneratorWithBackend(self: *Node, allocator: std.mem.Allocator, model_name: []const u8, backend: ?backends_mod.BackendType) !void {
        if (model_name.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();
        std.log.info("warming inference generator model={s}", .{model_name});
        const preferred_backends = if (backend) |value| singleBackendPreference(value) else null;
        const messages = [_]generation.Message{.{
            .role = "user",
            .content = "ping",
        }};
        var timing = DirectGenerateTiming{};
        const text = try self.generateMessagesDirectMaxTokens(allocator, model_name, &messages, 1, preferred_backends, &timing, true);
        defer allocator.free(text);
        const elapsed_ms = elapsedMs(started_at_ns, embedTimingNowNs());
        std.log.info(
            "warmed inference generator model={s} elapsed_ms={d} resolve_ms={d} load_ms={d} setup_ms={d} generate_ms={d}",
            .{
                model_name,
                elapsed_ms,
                timing.resolve_ms,
                timing.load_ms,
                timing.setup_ms,
                timing.generate_ms,
            },
        );
    }

    fn warmEmbedder(self: *Node, allocator: std.mem.Allocator, model_name: []const u8, backend: ?backends_mod.BackendType) !void {
        if (model_name.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();
        std.log.info("warming inference embedder model={s}", .{model_name});
        const texts = [_][]const u8{"ping"};

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model_name, "embedders");
        var model_handle = if (backend) |value|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, singleBackendPreference(value), false)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();

        if (model.manifest.hasCapability("sparse")) {
            var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
                .allocator = allocator,
                .session = model.session,
                .tok = model.getTokenizer(),
                .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
                .execution_lock = model.embeddingExecutionLock(),
            };
            const sparse = try pipeline.embed(&texts);
            defer {
                for (sparse) |*item| item.deinit(allocator);
                allocator.free(sparse);
            }
        } else {
            var asset_lease = model.acquireEmbeddingAssetLease(false);
            defer asset_lease.release();
            try model.ensureEmbeddingAssets(true, false, false);
            var pipeline = model.embeddingPipeline(allocator);
            const embeddings = try pipeline.embed(&texts);
            defer {
                for (embeddings) |embedding| allocator.free(embedding);
                allocator.free(embeddings);
            }
        }
        model_handle.pin();
        std.log.info("warmed inference embedder model={s} elapsed_ms={d}", .{ model_name, elapsedMs(started_at_ns, embedTimingNowNs()) });
    }

    fn warmReranker(self: *Node, allocator: std.mem.Allocator, model_name: []const u8, backend: ?backends_mod.BackendType) !void {
        if (model_name.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();
        std.log.info("warming inference reranker model={s}", .{model_name});
        const documents = [_][]const u8{"pong"};
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model_name, "rerankers");
        var model_handle = if (backend) |value|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, singleBackendPreference(value), false)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        var pipeline = model.rerankingPipeline(allocator);
        const scores = try pipeline.rerank("ping", &documents);
        defer allocator.free(scores);
        model_handle.pin();
        std.log.info("warmed inference reranker model={s} elapsed_ms={d}", .{ model_name, elapsedMs(started_at_ns, embedTimingNowNs()) });
    }

    fn warmLoadOnlyModel(self: *Node, allocator: std.mem.Allocator, model: WarmModel) !void {
        if (model.name.len == 0) return error.InvalidGenerationRequest;
        const task_dir = switch (model.kind) {
            .chunker => "chunkers",
            .classifier => "classifiers",
            .rewriter => "rewriters",
            .reader => "readers",
            .transcriber => "transcribers",
            .extractor => "extractors",
            else => return error.UnsupportedWarmModelKind,
        };
        const started_at_ns = embedTimingNowNs();
        std.log.info("loading inference {s} model={s}", .{ @tagName(model.kind), model.name });
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model.name, task_dir);
        var model_handle = if (model.backend) |backend|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, singleBackendPreference(backend), false)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        model_handle.pin();
        std.log.info("loaded inference {s} model={s} elapsed_ms={d}", .{ @tagName(model.kind), model.name, elapsedMs(started_at_ns, embedTimingNowNs()) });
    }

    pub fn embedDenseJsonInputDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        input: std.json.Value,
    ) ![][]f32 {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        return try self.embedDenseJsonInputDirectWithContext(allocator, io_impl.io(), null, model_name, input);
    }

    pub fn embedDenseJsonInputDirectWithContext(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        deadline_ns: ?u64,
        model_name: []const u8,
        input: std.json.Value,
    ) ![][]f32 {
        try ensureDirectEmbeddingDeadline(deadline_ns);
        try self.request_queue.acquire();
        self.updateQueueMetrics();
        defer self.releaseSlot();
        self.metrics.incRequest("embed.local");
        defer self.metrics.decActive();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "embedders");
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        try ensureDirectEmbeddingDeadline(deadline_ns);
        if (model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;

        var parsed = try parseDenseEmbedInputs(self, allocator, &model.manifest, input);
        defer parsed.deinit(allocator);
        if (parsed.total_count == 0) return try allocator.alloc([]f32, 0);

        var asset_lease = model.acquireEmbeddingAssetLease(parsed.audio.items.len > 0);
        defer asset_lease.release();
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var pipeline = try prepareInitialDenseEmbeddingPipeline(model, allocator, &parsed);
        var audio_asset_guard = AudioEmbeddingAssetGuard.init(
            model,
            parsed.audio.items.len > 0,
        );
        defer audio_asset_guard.deinit();
        const vectors = try embedDenseInputs(
            allocator,
            &pipeline,
            &parsed,
            model,
            &audio_asset_guard,
            &asset_lease,
        );
        asset_lease.release();
        errdefer freeDirectDenseVectors(allocator, vectors);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        return vectors;
    }

    pub fn readImagesDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: readers_api.Request,
    ) ![]readers_api.Result {
        if (request.images.len == 0) return try allocator.alloc(readers_api.Result, 0);
        if (request.images.len > max_read_batch_images) return error.ReadBatchTooLarge;
        const max_tokens = try validateReadMaxTokens(request.max_tokens);
        const queue_units = estimateReadQueueUnits(request.images.len, max_tokens);
        try self.request_queue.acquireUnits(queue_units);
        self.updateQueueMetrics();
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("read.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "readers");
        var reader = try readers_mod.LoadedReader.loadFromDir(allocator, model_path, &self.session_manager, &self.model_manager);
        defer reader.deinit();

        const out = try allocator.alloc(readers_api.Result, request.images.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*result| readers_api.deinitResult(allocator, result);
            allocator.free(out);
        }

        const downloaded = try allocator.alloc(scraping.DownloadedContent, request.images.len);
        var downloaded_count: usize = 0;
        defer {
            for (downloaded[0..downloaded_count]) |*item| item.deinit(allocator);
            allocator.free(downloaded);
        }
        const image_datas = try allocator.alloc([]const u8, request.images.len);
        defer allocator.free(image_datas);

        const batch_byte_cap = readBatchMaxBytes();
        var batch_bytes: usize = 0;
        for (request.images, 0..) |image_url, i| {
            var item = try downloadReadBatchContent(self, allocator, image_url, batch_byte_cap, batch_bytes);
            errdefer item.deinit(allocator);
            batch_bytes = try addReadBatchDownloadedBytes(batch_bytes, item, batch_byte_cap);
            downloaded[i] = item;
            downloaded_count += 1;
            image_datas[i] = downloaded[i].data;
        }

        const results = try reader.readBatch(image_datas, .{
            .prompt = normalizeReadPrompt(request.prompt),
            .max_tokens = max_tokens,
        });
        defer {
            for (results) |result| {
                var tmp = result;
                tmp.deinit();
            }
            allocator.free(results);
        }
        if (results.len != request.images.len) return error.InvalidReadResultCount;

        for (results, 0..) |result, i| {
            var item: readers_api.Result = .{
                .text = try allocator.dupe(u8, result.text),
            };
            errdefer readers_api.deinitResult(allocator, &item);
            item.fields_json = try readerFieldsJsonAlloc(allocator, result.fields);
            item.regions_json = try readerRegionsJsonAlloc(allocator, result.regions);
            out[i] = item;
            initialized += 1;
        }
        return out;
    }

    pub fn transcribeAudioDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: transcribing_api.Request,
    ) !transcribing_api.Response {
        try self.request_queue.acquire();
        self.updateQueueMetrics();
        defer self.releaseSlot();
        self.metrics.incRequest("transcribe.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "transcribers");
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        const whisper_config = session_factory.getWhisperConfig(model.session) orelse return error.UnsupportedTranscriberProvider;

        const transcription = @import("../pipelines/transcription.zig");
        const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
        const prompt_cache = if (model.whisper_prompt_cache) |*cache|
            cache
        else
            return error.InvalidWhisperDecoderConfig;
        var prompt_scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
        const forced_ids = try prompt_cache.resolve(&prompt_scratch, request.language);
        var pipeline = transcription.TranscriptionPipeline.init(
            allocator,
            model.session,
            model.session,
            model.getTokenizer(),
            .{
                .max_length = @intCast(whisper_config.max_target_positions),
                .decoder_start_token_id = whisper_config.decoder_start_token_id,
                .eos_token_id = whisper_config.eos_token_id,
                .language = request.language,
                .forced_decoder_ids = forced_ids,
                .language_tokens = prompt_cache.language_tokens,
            },
        );

        var downloaded = try downloadRemoteContent(self, allocator, request.url);
        defer downloaded.deinit(allocator);
        const decode_options = audio_mod.DecodeOptions{ .mime_hint = downloaded.content_type };
        if (!audio_mod.canDecodeWithOptions(downloaded.data, decode_options)) return error.UnsupportedAudioInput;

        var result = try pipeline.transcribeWithOptions(downloaded.data, decode_options);
        defer result.deinit();
        return .{
            .text = try allocator.dupe(u8, result.text),
            .language = if (result.language) |language| try allocator.dupe(u8, language) else null,
        };
    }

    pub fn extractDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: extracting_api.Request,
    ) !extracting_api.Response {
        try self.request_queue.acquire();
        self.updateQueueMetrics();
        var queue_units: usize = 1;
        defer if (queue_units > 0) self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("extract.local");
        defer self.metrics.decActive();

        var schema_parsed = try std.json.parseFromSlice(extraction_api.ExtractionSchema, allocator, request.schema_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer schema_parsed.deinit();
        const schemas = try parseCanonicalStructureSchemas(allocator, schema_parsed.value);
        defer {
            for (schemas) |*schema| schema.deinit(allocator);
            allocator.free(schemas);
        }

        var options = try parseExtractionOptionsJson(allocator, request.options_json);
        defer options.deinit();
        const config = extraction_mod.ExtractionConfig{
            .threshold = options.threshold orelse 0.3,
            .flat_ner = options.flat_ner orelse true,
            .include_confidence = options.include_confidence orelse false,
            .include_spans = options.include_spans orelse false,
        };

        var parsed_inputs = try parseDirectExtractionInputs(self, allocator, request.inputs, options.prompt, options.max_tokens);
        defer parsed_inputs.deinit();
        try validateExtractionInputKinds(parsed_inputs.texts.items.len, parsed_inputs.images.items.len);
        const required_units = if (parsed_inputs.images.items.len > 0) blk: {
            if (parsed_inputs.images.items.len > max_read_batch_images) return error.ReadBatchTooLarge;
            if (parsed_inputs.max_tokens) |max_tokens| {
                if (max_tokens == 0 or max_tokens > max_read_tokens) return error.InvalidMaxTokens;
            }
            break :blk estimateReadQueueUnits(parsed_inputs.images.items.len, parsed_inputs.max_tokens);
        } else 1;
        if (required_units > queue_units) {
            self.releaseSlotUnits(queue_units);
            queue_units = 0;
            try self.request_queue.acquireUnits(required_units);
            queue_units = required_units;
            self.updateQueueMetrics();
        }

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const extractor_ctx = extractors_mod.Context{
            .allocator = allocator,
            .io = io_impl.io(),
            .models_dir = self.config.models_dir,
            .session_manager = &self.session_manager,
            .model_manager = &self.model_manager,
            .reader_resolver = &self.extraction_reader_resolver,
        };
        var extractor = try extractors_mod.resolve(extractor_ctx, model_name, parsed_inputs.images.items.len > 0);
        defer extractor.deinit(allocator);

        const results = if (parsed_inputs.images.items.len > 0)
            try extractor.extractImages(extractor_ctx, schemas, config, parsed_inputs.images.items, .{
                .prompt = parsed_inputs.prompt,
                .max_tokens = parsed_inputs.max_tokens,
            })
        else
            try extractor.extractText(extractor_ctx, schemas, config, parsed_inputs.texts.items);
        defer {
            for (results) |*result| result.deinit(allocator);
            allocator.free(results);
        }

        return .{
            .allocator = allocator,
            .json = try extractionResponseJsonAlloc(allocator, model_name, request.inputs, results),
        };
    }

    /// Resolve a model name to a directory path.
    /// Supports: absolute path, "hf:owner/name:variant", "owner/name", variant resolution.
    /// Matches Go inference's resolveModel: exact match → re-scan → variant resolution.
    /// When task_type is provided (e.g. "embedders"), also searches models_dir/task_type/.
    pub fn resolveModelPath(self: *Node, io: std.Io, name: ?[]const u8, task_type: ?[]const u8) ![]const u8 {
        if (name) |raw| {
            // Strip "hf:" prefix if present
            const n = if (std.mem.startsWith(u8, raw, "hf:")) raw[3..] else raw;

            // Strip ":variant" suffix for path resolution (variant is for pulling, not path lookup)
            const name_without_variant = if (std.mem.indexOfScalar(u8, n, ':')) |colon| n[0..colon] else n;

            // Absolute path
            if (std.mem.startsWith(u8, name_without_variant, "/")) return name_without_variant;

            // Try exact match: models_dir/name
            const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, name_without_variant });
            if (dirContainsModel(path)) {
                return path;
            } else {
                self.allocator.free(path);
            }

            // Try task-type subdirectory: models_dir/task_type/name
            if (task_type) |tt| {
                const task_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}/{s}", .{ self.config.models_dir, tt, name_without_variant });
                if (dirContainsModel(task_path)) {
                    return task_path;
                } else {
                    self.allocator.free(task_path);
                }

                // Variant resolution within task-type dir
                const task_dir = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, tt });
                defer self.allocator.free(task_dir);
                if (try registry_mod.resolveVariant(self.allocator, io, task_dir, name_without_variant)) |variant_path| {
                    return variant_path;
                }
            }

            // Variant resolution: look for "name-{suffix}" with shortest suffix wins
            if (try registry_mod.resolveVariant(self.allocator, io, self.config.models_dir, name_without_variant)) |variant_path| {
                return variant_path;
            }

            // If name has "owner/model" format, try just the model part (flat layout)
            if (std.mem.indexOfScalar(u8, name_without_variant, '/')) |slash| {
                const model_only = name_without_variant[slash + 1 ..];
                const flat_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, model_only });
                if (dirContainsModel(flat_path)) {
                    return flat_path;
                } else {
                    self.allocator.free(flat_path);
                }

                // Variant resolution on model-only name
                if (try registry_mod.resolveVariant(self.allocator, io, self.config.models_dir, model_only)) |variant_path| {
                    return variant_path;
                }
            }

            return error.ModelNotFound;
        }

        // No model specified — use models_dir itself if it contains model files,
        // otherwise scan for the first subdirectory.
        if (dirContainsModel(self.config.models_dir)) {
            return self.config.models_dir;
        }
        // Scan models_dir for subdirectories that contain model files
        // If task_type is provided, prefer scanning within that subdirectory
        if (task_type) |tt| {
            const task_dir = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, tt });
            if (dirContainsModel(task_dir)) {
                return task_dir;
            }
            if (self.findFirstModelInDir(task_dir)) |p| {
                self.allocator.free(task_dir);
                return p;
            }
            self.allocator.free(task_dir);
        }
        return self.findFirstModelDir() orelse error.ModelNotSpecified;
    }

    fn findFirstModelInDir(self: *Node, dir_path: []const u8) ?[]const u8 {
        if (!build_options.link_libc) {
            var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, dir_path, .{ .iterate = true }) catch return null;
            defer dir.close(std.Options.debug_io);
            var iter = dir.iterate();
            while (iter.next(std.Options.debug_io) catch null) |entry| {
                const ename_slice = entry.name;
                if (ename_slice.len == 0 or ename_slice[0] == '.') continue;

                const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ dir_path, ename_slice }) catch continue;
                if (dirContainsModel(sub_path)) {
                    return sub_path;
                }
                if (self.findFirstModelInDir(sub_path)) |p| {
                    self.allocator.free(sub_path);
                    return p;
                }
                self.allocator.free(sub_path);
            }
            return null;
        }

        const dir_z = self.allocator.dupeZ(u8, dir_path) catch return null;
        defer self.allocator.free(dir_z);

        const cc = c_file.c;
        const dir = cc.opendir(dir_z.ptr);
        if (dir == null) return null;
        defer _ = cc.closedir(dir);

        while (cc.readdir(dir)) |entry| {
            const ename: [*:0]const u8 = @ptrCast(&entry.*.d_name);
            const ename_slice = std.mem.span(ename);
            if (ename_slice.len == 0 or ename_slice[0] == '.') continue;

            const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ dir_path, ename_slice }) catch continue;
            if (dirContainsModel(sub_path)) {
                return sub_path;
            }
            // Recurse one level for owner/model layout
            if (self.findFirstModelInDir(sub_path)) |p| {
                self.allocator.free(sub_path);
                return p;
            }
            self.allocator.free(sub_path);
        }
        return null;
    }

    fn findFirstModelDir(self: *Node) ?[]const u8 {
        if (!build_options.link_libc) {
            var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, self.config.models_dir, .{ .iterate = true }) catch return null;
            defer dir.close(std.Options.debug_io);
            var iter = dir.iterate();
            while (iter.next(std.Options.debug_io) catch null) |entry| {
                const name_slice = entry.name;
                if (name_slice.len == 0 or name_slice[0] == '.') continue;

                const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, name_slice }) catch continue;
                if (dirContainsModel(sub_path)) {
                    return sub_path;
                }
                self.allocator.free(sub_path);
            }
            return null;
        }

        const dir_z = self.allocator.dupeZ(u8, self.config.models_dir) catch return null;
        defer self.allocator.free(dir_z);

        const cc = c_file.c;
        const dir = cc.opendir(dir_z.ptr);
        if (dir == null) return null;
        defer _ = cc.closedir(dir);

        while (cc.readdir(dir)) |entry| {
            const name: [*:0]const u8 = @ptrCast(&entry.*.d_name);
            const name_slice = std.mem.span(name);
            if (name_slice.len == 0 or name_slice[0] == '.') continue;

            const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, name_slice }) catch continue;
            if (dirContainsModel(sub_path)) {
                return sub_path;
            }
            self.allocator.free(sub_path);
        }
        return null;
    }

    // --- ServerRouter handler methods ---
    // These implement the interface required by api.ServerRouter(Node).

    /// Acquire a request slot; returns 503 if queue is full.
    fn acquireSlot(self: *Node, ctx: *httpx.Context) !?httpx.Response {
        return self.acquireSlotUnits(ctx, 1);
    }

    fn acquireSlotUnits(self: *Node, ctx: *httpx.Context, units: usize) !?httpx.Response {
        const requested_units = self.request_queue.capacityUnits(units);
        self.request_queue.acquireUnits(units) catch {
            self.metrics.incError();
            self.metrics.recordQueueRejection(requested_units);
            self.updateQueueMetrics();
            const resp = try transientCapacityFailureResponse(
                ctx,
                "SERVICE_UNAVAILABLE",
                "server at capacity, try again later",
                "request_queue",
            );
            return resp;
        };
        self.updateQueueMetrics();
        return null;
    }

    fn releaseSlot(self: *Node) void {
        self.releaseSlotUnits(1);
    }

    fn releaseSlotUnits(self: *Node, units: usize) void {
        self.request_queue.releaseUnits(units);
        self.updateQueueMetrics();
    }

    fn updateQueueMetrics(self: *Node) void {
        self.metrics.setQueueState(
            self.request_queue.depth(),
            self.request_queue.max_concurrent,
            self.request_queue.requests(),
        );
    }

    fn estimateHttpRequestQueueUnits(self: *Node, ctx: *httpx.Context) usize {
        _ = self;
        const body_len = if (ctx.request.body) |body| body.len else 0;
        const bytes_per_unit: usize = 1024 * 1024;
        return 1 + (body_len / bytes_per_unit);
    }

    fn estimateGenerateQueueUnits(self: *Node, messages: []const generation.Message, max_tokens: i32) usize {
        _ = self;
        var text_bytes: usize = 0;
        var image_count: usize = 0;
        for (messages) |msg| {
            text_bytes += msg.content.len;
            if (msg.image_bytes) |images| image_count += images.len;
        }

        const prompt_units = 1 + (text_bytes / 2048);
        const decode_units: usize = @intCast(@max(@divTrunc(max_tokens, 256), 0));
        const image_units = image_count * 2;
        return 1 + prompt_units + decode_units + image_units;
    }

    fn estimateGenerateBatchQueueUnits(
        self: *Node,
        requests: []const api.GenerateBatchRequestItem,
        owned_messages: []const OwnedGenerateMessages,
        pending: []const bool,
    ) usize {
        var total: usize = 1;
        for (requests, pending, 0..) |item, is_pending, idx| {
            if (!is_pending) continue;
            const max_tokens: i32 = if (item.body.max_tokens) |mt| @intCast(mt) else 256;
            const item_units = self.estimateGenerateQueueUnits(owned_messages[idx].messages, max_tokens);
            total = std.math.add(usize, total, item_units) catch std.math.maxInt(usize);
        }
        return total;
    }

    fn estimateGeneratePromptBytes(self: *Node, messages: []const generation.Message) usize {
        _ = self;
        var text_bytes: usize = 0;
        for (messages) |msg| {
            text_bytes += msg.content.len;
        }
        return text_bytes;
    }

    fn estimateNativePromptTokens(
        self: *Node,
        allocator: std.mem.Allocator,
        model: *model_manager_mod.LoadedModel,
        messages: []const generation.Message,
    ) !usize {
        _ = self;
        const prompt = if (model.chat_tmpl) |ct|
            try ct.apply(allocator, messages, true)
        else
            try generation.formatMessages(allocator, messages);
        defer allocator.free(prompt);
        var encoded = try generation.encodePromptForGeneration(
            model.getTokenizer(),
            allocator,
            prompt,
            2048,
            model.manifest.add_bos_token,
            model.manifest.bos_token,
        );
        defer encoded.deinit();
        var count: usize = 0;
        while (count < encoded.attention_mask.len and encoded.attention_mask[count] != 0) : (count += 1) {}
        return count;
    }

    fn memoryBudgetExceededMessage(
        allocator: std.mem.Allocator,
        session: backends_mod.Session,
        run_budget: *const runtime.tier.memory.RunBudget,
    ) []const u8 {
        var buf: [512]u8 = undefined;
        const msg = session_factory.memoryBudgetExceededDetail(session, run_budget, &buf) catch {
            return "request exceeds native generation memory budget";
        };
        return allocator.dupe(u8, msg) catch "request exceeds native generation memory budget";
    }

    pub fn generateEmbeddings(self: *Node, ctx: *httpx.Context) !httpx.Response {
        return self.createEmbedding(ctx);
    }

    pub fn createEmbedding(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(std.json.Value)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const request = parseEmbedRequest(parsed.value) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedRequestParseErrorMessage(err),
            });
        };

        validateEmbeddingEncodingFormat(request.encoding_format) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "encoding_format must be \"float\"",
            });
        };
        const requested_dimensions = parseRequestedEmbeddingDimensions(request.dimensions) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "dimensions must be a positive integer",
            });
        };

        const queue_units = self.estimateHttpRequestQueueUnits(ctx);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("embed");
        defer self.metrics.decActive();

        // Resolve and load model.
        const model_name: ?[]const u8 = if (request.model.len > 0) request.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "embedders") catch
            return ctx.status(404).json(.{
                .@"error" = "MODEL_NOT_FOUND",
                .message = "model not found; specify 'model' as a path or owner/name",
            });

        if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        if (model.manifest.hasCapability("sparse")) {
            const sparse_texts = parseSparseEmbedInputs(ctx.allocator, request.input) catch {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "sparse models only support text input (string or array of strings)",
                });
            };
            defer ctx.allocator.free(sparse_texts);

            if (sparse_texts.len == 0) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "input is empty" });
            }
            if (requested_dimensions != null) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "dimensions is not supported for sparse embedding models",
                });
            }
            var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
                .allocator = ctx.allocator,
                .session = model.session,
                .tok = model.getTokenizer(),
                .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
                .execution_lock = model.embeddingExecutionLock(),
            };
            const sparse_vecs = pipeline.embed(sparse_texts) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer {
                for (sparse_vecs) |*sv| @constCast(sv).deinit(ctx.allocator);
                ctx.allocator.free(sparse_vecs);
            }

            var arena = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena.deinit();
            const prompt_tokens = countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), sparse_texts) catch estimateTextsTokens(sparse_texts);
            const response = try buildEmbedSparseResponse(arena.allocator(), request.model, sparse_vecs, prompt_tokens);
            return ctx.json(response);
        }

        var inputs = switch (request.error_policy) {
            .fail_fast => parseDenseEmbedInputs(self, ctx.allocator, &model.manifest, request.input),
            .per_item => parseDenseEmbedInputsPerItem(self, ctx.allocator, &model.manifest, request.input),
        } catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedInputParseErrorMessage(err),
            });
        };
        defer inputs.deinit(ctx.allocator);

        if (inputs.total_count == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "input is empty" });
        }

        var asset_lease = model.acquireEmbeddingAssetLease(inputs.audio.items.len > 0);
        defer asset_lease.release();
        var pipeline = prepareInitialDenseEmbeddingPipeline(model, ctx.allocator, &inputs) catch |err|
            return inferenceFailureResponse(ctx, err);
        var audio_asset_guard = AudioEmbeddingAssetGuard.init(
            model,
            inputs.audio.items.len > 0,
        );
        defer audio_asset_guard.deinit();
        applyDenseEmbeddingRequestOptions(&pipeline, &model.manifest, request) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedRequestOptionErrorMessage(err),
            });
        };
        const pipeline_start = embedTimingStart();
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const response_build_start = embedTimingStart();
        const prompt_tokens = if (inputs.texts.items.len > 0)
            countParsedDenseEmbedTextTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), &inputs)
        else
            estimateParsedDenseEmbedPromptTokens(&inputs);

        switch (request.error_policy) {
            .fail_fast => {
                const embeddings = embedDenseInputs(
                    ctx.allocator,
                    &pipeline,
                    &inputs,
                    model,
                    &audio_asset_guard,
                    &asset_lease,
                ) catch |err| return embedDenseInputFailureResponse(ctx, err);
                asset_lease.release();
                logEmbedTiming("embed.pipeline", inputs.total_count, pipeline_start);
                defer {
                    for (embeddings) |e| ctx.allocator.free(e);
                    ctx.allocator.free(embeddings);
                }
                const response = buildEmbedDenseResponse(arena.allocator(), request.model, embeddings, requested_dimensions, prompt_tokens) catch |err| switch (err) {
                    error.InvalidEmbeddingDimensions => {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "dimensions exceeds the model embedding size",
                        });
                    },
                    else => return err,
                };
                logEmbedTiming("embed.response_build", inputs.total_count, response_build_start);
                const response_json_start = embedTimingStart();
                const http_response = try ctx.json(response);
                logEmbedTiming("embed.response_json", inputs.total_count, response_json_start);
                return http_response;
            },
            .per_item => {
                var partial = try embedDenseInputsPartial(
                    ctx.allocator,
                    &pipeline,
                    &inputs,
                    model,
                    &audio_asset_guard,
                    &asset_lease,
                );
                asset_lease.release();
                logEmbedTiming("embed.pipeline", inputs.total_count, pipeline_start);
                defer partial.deinit(ctx.allocator);
                const response = buildEmbedDensePartialResponse(arena.allocator(), request.model, &partial, requested_dimensions, prompt_tokens) catch |err| switch (err) {
                    error.InvalidEmbeddingDimensions => {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "dimensions exceeds the model embedding size",
                        });
                    },
                    else => return err,
                };
                logEmbedTiming("embed.response_build", inputs.total_count, response_build_start);
                const response_json_start = embedTimingStart();
                const http_response = try ctx.json(response);
                logEmbedTiming("embed.response_json", inputs.total_count, response_json_start);
                return http_response;
            },
        }
    }

    pub fn chunkText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (ctx.parseJson(api.ChunkRequest) catch |err|
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = chunkRequestParseErrorMessage(err) })) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        const queue_units = self.estimateHttpRequestQueueUnits(ctx);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("chunk");
        defer self.metrics.decActive();

        const input = parseChunkRequestInput(ctx.allocator, body.input) catch |err|
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = chunkInputParseErrorMessage(err) });
        defer deinitChunkRequestInput(ctx.allocator, input);

        var config = lib_chunker.FixedChunkConfig{};
        if (body.config) |cfg| {
            if (cfg.model) |model| config.model = model;
            if (cfg.max_chunks) |max_chunks| config.max_chunks = @intCast(max_chunks);
            config.threshold = cfg.threshold;
            if (cfg.text) |text_cfg| {
                if (text_cfg.target_tokens) |tt| config.text.target_tokens = @intCast(tt);
                if (text_cfg.overlap_tokens) |ot| config.text.overlap_tokens = @intCast(ot);
                if (text_cfg.separator) |separator| config.text.separator = separator;
            }
            if (cfg.audio) |audio_cfg| {
                if (audio_cfg.window_duration_ms) |window| config.audio.window_duration_ms = @intCast(window);
                if (audio_cfg.overlap_duration_ms) |overlap| config.audio.overlap_duration_ms = @intCast(overlap);
            }
        }

        const chunks = lib_chunker.fixed_multimodal.chunkInput(ctx.allocator, input, config) catch |err|
            return ctx.status(500).json(.{ .@"error" = "CHUNKING_FAILED", .message = @errorName(err) });
        defer lib_chunker.types.freeChunks(ctx.allocator, chunks);

        const api_chunks = try ctx.allocator.alloc(api.ChunkObject, chunks.len);
        defer ctx.allocator.free(api_chunks);

        // Base64-encoded copies of binary chunks; kept alive until ctx.json serializes.
        var encoded_datas = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (encoded_datas.items) |e| ctx.allocator.free(e);
            encoded_datas.deinit(ctx.allocator);
        }

        for (chunks, 0..) |chunk, i| {
            var encoded_data: ?[]const u8 = null;
            if (chunk.data) |data| {
                const encoded_len = std.base64.standard.Encoder.calcSize(data.len);
                const encoded = try ctx.allocator.alloc(u8, encoded_len);
                errdefer ctx.allocator.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, data);
                try encoded_datas.append(ctx.allocator, encoded);
                encoded_data = encoded;
            }

            api_chunks[i] = .{
                .object = "chunk",
                .index = @intCast(i),
                .id = @intCast(chunk.id),
                .mime_type = chunk.mime_type,
                .text = chunk.text,
                .start_char = if (chunk.start_char) |v| @intCast(v) else null,
                .end_char = if (chunk.end_char) |v| @intCast(v) else null,
                .data = encoded_data,
                .start_time_ms = chunk.start_time_ms,
                .end_time_ms = chunk.end_time_ms,
                .frame_index = if (chunk.frame_index) |v| @intCast(v) else null,
                .frame_delay_ms = if (chunk.frame_delay_ms) |v| @intCast(v) else null,
            };
        }

        const prompt_tokens = switch (input) {
            .text => |text| estimateTextTokens(text),
            .binary => 0,
        };

        return ctx.json(api.ChunkResponse{
            .object = "list",
            .data = api_chunks,
            .model = if (config.model.len > 0) config.model else "fixed-bert-tokenizer",
            .usage = tokenUsage(prompt_tokens, 0),
            .cache_hit = false,
        });
    }

    pub fn rerankPrompts(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.RerankRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("rerank");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "rerankers") catch
            return ctx.status(404).json(.{
                .@"error" = "MODEL_NOT_FOUND",
                .message = "model not found; specify 'model' as a path or owner/name",
            });

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        var pipeline = model.rerankingPipeline(ctx.allocator);
        const scores = pipeline.rerank(body.query, body.prompts) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer ctx.allocator.free(scores);

        const prompt_tokens =
            (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * body.prompts.len +
            (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.prompts) catch estimateTextsTokens(body.prompts));
        return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
    }

    pub fn rerankMultimodalPrompts(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed_body = (try ctx.parseJson(api.RerankMultimodalRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed_body.deinit();
        const body = parsed_body.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("rerank");
        defer self.metrics.decActive();

        if (body.documents.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "documents must not be empty" });
        }

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "rerankers") catch
            return ctx.status(404).json(.{
                .@"error" = "MODEL_NOT_FOUND",
                .message = "model not found; specify 'model' as a path or owner/name",
            });

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        var parsed_docs = std.ArrayListUnmanaged(ParsedMultimodalRerankDocument).empty;
        defer {
            for (parsed_docs.items) |*doc| doc.deinit();
            parsed_docs.deinit(ctx.allocator);
        }

        var has_images = false;
        for (body.documents) |doc| {
            const parsed = parseChatMessageContentToTextAndImages(self, ctx.allocator, doc.content) catch |err| switch (err) {
                error.InvalidImageDataUri => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid image data URI" }),
                error.ImageDownloadFailed => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "image download failed" }),
                error.UnsupportedContentPartType => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "multimodal rerank documents only support text and image content parts" }),
                else => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = @errorName(err) }),
            };
            if (parsed.images.len > 0) has_images = true;
            try parsed_docs.append(ctx.allocator, parsed);
        }

        if (!has_images) {
            const flat_texts = try ctx.allocator.alloc([]const u8, parsed_docs.items.len);
            defer ctx.allocator.free(flat_texts);
            for (parsed_docs.items, 0..) |doc, idx| flat_texts[idx] = doc.text;

            var pipeline = model.rerankingPipeline(ctx.allocator);
            const scores = pipeline.rerank(body.query, flat_texts) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer ctx.allocator.free(scores);
            const prompt_tokens =
                (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * flat_texts.len +
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), flat_texts) catch estimateTextsTokens(flat_texts));
            return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
        }

        if (!(model.manifest.hasCapability("colqwen") or model.manifest.hasCapability("multimodal_late_interaction"))) {
            return ctx.status(400).json(.{
                .@"error" = "MODEL_NOT_SUPPORTED",
                .message = "model does not advertise multimodal late-interaction reranking capability",
            });
        }

        model.ensureVisionSession() catch |err|
            return inferenceFailureResponse(ctx, err);
        const vision_session = model.vision_session;
        const gpt_cfg = session_factory.getGptConfig(model.session) orelse
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "multimodal late-interaction reranking currently requires a native qwen/gpt text session" });
        if (vision_session == null and !gpt_cfg.supportsNativeQwen2VlVision()) {
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "model lacks both visual_model.onnx and native qwen2-vl vision config" });
        }
        const prep_cfg = multimodal_qwen_adapter.loadPreprocessorConfig(ctx.allocator, model_path) catch |err|
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) });

        var cb = session_factory.getComputeBackend(model.session, ctx.allocator) catch |err|
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) });
        defer cb.deinit();

        var mm_pipeline = multimodal_reranker.Pipeline.init(
            ctx.allocator,
            &cb,
            vision_session,
            model.getTokenizer(),
            gpt_cfg,
            prep_cfg,
            model.manifest.max_position_embeddings,
            model.manifest.add_bos_token,
            .{ .distributed = runtime.distributed.configFromEnv() },
        );

        var query_encoded = mm_pipeline.encodeQueryText(body.query) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer query_encoded.deinit();

        const scores = try ctx.allocator.alloc(f32, parsed_docs.items.len);
        defer ctx.allocator.free(scores);

        for (parsed_docs.items, 0..) |doc, idx| {
            if (doc.images.len == 0) {
                var text_pipeline = model.rerankingPipeline(ctx.allocator);
                const text_scores = text_pipeline.rerank(body.query, &.{doc.text}) catch |err|
                    return inferenceFailureResponse(ctx, err);
                defer ctx.allocator.free(text_scores);
                scores[idx] = text_scores[0];
                continue;
            }

            scores[idx] = mm_pipeline.scoreDocumentText(
                query_encoded,
                doc.text,
                doc.images,
            ) catch |err| switch (err) {
                error.InvalidImageDataUri, error.UnsupportedContentPartType => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = @errorName(err) }),
                error.ImageTokenLengthMismatch, error.ImageProjectionSizeMismatch, error.UnexpectedOutputShape => return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) }),
                else => return inferenceFailureResponse(ctx, err),
            };
        }

        var doc_texts = try ctx.allocator.alloc([]const u8, parsed_docs.items.len);
        defer ctx.allocator.free(doc_texts);
        for (parsed_docs.items, 0..) |doc, idx| doc_texts[idx] = doc.text;
        const prompt_tokens =
            (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * doc_texts.len +
            (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), doc_texts) catch estimateTextsTokens(doc_texts));
        return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
    }

    pub fn generateContent(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.GenerateRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.prompt_cache_key) |key| {
            if (key.len > runtime.kv.prompt_cache.max_namespace_bytes) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "prompt_cache_key is too long",
                });
            }
        }
        self.metrics.incRequest("generate");
        defer self.metrics.decActive();

        // Resolve model
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "generators") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;

        // Extract messages from request body
        var messages = std.ArrayListUnmanaged(generation.Message).empty;
        defer messages.deinit(ctx.allocator);

        // Track decoded image bytes for cleanup
        var decoded_images = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (decoded_images.items) |img| ctx.allocator.free(img);
            decoded_images.deinit(ctx.allocator);
        }
        // Track per-message image slices for cleanup
        var image_slices = std.ArrayListUnmanaged([]const []const u8).empty;
        defer {
            for (image_slices.items) |s| ctx.allocator.free(s);
            image_slices.deinit(ctx.allocator);
        }

        for (body.messages) |msg| {
            const role: []const u8 = switch (msg.role) {
                .system => "system",
                .user => "user",
                .assistant => "assistant",
                .tool => "tool",
            };

            var text_buf = std.ArrayListUnmanaged(u8).empty;
            defer text_buf.deinit(ctx.allocator);
            var msg_images = std.ArrayListUnmanaged([]const u8).empty;
            defer msg_images.deinit(ctx.allocator);
            var msg_parts = std.ArrayListUnmanaged(generation.Message.ContentPart).empty;
            defer msg_parts.deinit(ctx.allocator);

            if (msg.content) |cv| {
                switch (cv) {
                    .string => |s| {
                        try text_buf.appendSlice(ctx.allocator, s);
                    },
                    .array => |arr| {
                        // OpenAI-style content parts array
                        for (arr.items) |part| {
                            if (part != .object) continue;
                            const obj = part.object;
                            const type_val = obj.get("type") orelse continue;
                            if (type_val != .string) continue;
                            const ptype = type_val.string;

                            if (std.mem.eql(u8, ptype, "text")) {
                                if (obj.get("text")) |tv| {
                                    if (tv == .string) {
                                        try text_buf.appendSlice(ctx.allocator, tv.string);
                                        try msg_parts.append(ctx.allocator, .{ .text = tv.string });
                                    }
                                }
                            } else if (std.mem.eql(u8, ptype, "image_url")) {
                                // Extract URL from image_url object or string
                                const url_str = blk: {
                                    const iu = obj.get("image_url") orelse {
                                        return ctx.status(400).json(.{
                                            .@"error" = "INVALID_REQUEST",
                                            .message = "image_url content part missing 'image_url' field",
                                        });
                                    };
                                    if (iu == .object) {
                                        if (iu.object.get("url")) |u| {
                                            if (u == .string) break :blk u.string;
                                        }
                                    } else if (iu == .string) break :blk iu.string;
                                    return ctx.status(400).json(.{
                                        .@"error" = "INVALID_REQUEST",
                                        .message = "image_url must contain a 'url' string",
                                    });
                                };
                                const downloaded = downloadRemoteContent(self, ctx.allocator, url_str) catch {
                                    return ctx.status(400).json(.{
                                        .@"error" = "INVALID_REQUEST",
                                        .message = "failed to download image_url content",
                                    });
                                };
                                defer ctx.allocator.free(downloaded.content_type);
                                try decoded_images.append(ctx.allocator, downloaded.data);
                                try msg_images.append(ctx.allocator, downloaded.data);
                                try msg_parts.append(ctx.allocator, .{ .image = msg_images.items.len - 1 });
                            }
                        }
                    },
                    else => {},
                }
            }

            const content = try ctx.allocator.dupe(u8, text_buf.items);
            const msg_img_slice: ?[]const []const u8 = if (msg_images.items.len > 0)
                try ctx.allocator.dupe([]const u8, msg_images.items)
            else
                null;
            if (msg_img_slice) |s| try image_slices.append(ctx.allocator, s);
            const msg_part_slice: ?[]const generation.Message.ContentPart = if (msg_parts.items.len > 0)
                try ctx.allocator.dupe(generation.Message.ContentPart, msg_parts.items)
            else
                null;

            try messages.append(ctx.allocator, .{
                .role = role,
                .content = content,
                .image_bytes = msg_img_slice,
                .content_parts = msg_part_slice,
            });
        }

        if (messages.items.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'messages' must not be empty" });
        }

        const parsed_tool_choice = tool_parser_mod.parseToolChoice(body.tool_choice) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "invalid tool_choice",
            });
        };
        if (body.tool_choice != null and (body.tools == null or body.tools.?.len == 0)) {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "tools are required when tool_choice is set",
            });
        }

        var tool_parser: ?tool_parser_mod.Parser = null;
        defer if (tool_parser) |*parser| parser.deinit();

        if (body.tools) |tools| {
            if (tools.len > 0 and tool_parser_mod.toolCallsEnabled(parsed_tool_choice)) {
                tool_parser = tool_parser_mod.loadParser(ctx.allocator, model_path) catch |err| switch (err) {
                    error.UnknownToolCallFormat => return ctx.status(400).json(.{
                        .@"error" = "INVALID_MODEL",
                        .message = "model has an unsupported tool_call_format",
                    }),
                    else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) }),
                };
                if (tool_parser == null) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_MODEL",
                        .message = "model does not support tool calling",
                    });
                }

                var selected_tools = std.ArrayListUnmanaged(tool_parser_mod.ToolDefinition).empty;
                defer selected_tools.deinit(ctx.allocator);

                const forced_function = tool_parser_mod.forcedFunctionName(parsed_tool_choice);
                for (tools) |tool| {
                    if (!std.mem.eql(u8, tool.type, "function")) {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "only function tools are supported",
                        });
                    }
                    if (forced_function) |forced| {
                        if (!std.mem.eql(u8, tool.function.name, forced)) continue;
                    }
                    try selected_tools.append(ctx.allocator, .{
                        .type = tool.type,
                        .function = .{
                            .name = tool.function.name,
                            .description = tool.function.description orelse "",
                            .parameters = tool.function.parameters,
                            .strict = tool.function.strict orelse false,
                        },
                    });
                }

                if (forced_function != null and selected_tools.items.len == 0) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "forced function was not found in tools",
                    });
                }

                const tools_prompt = try tool_parser.?.formatToolsPrompt(ctx.allocator, selected_tools.items);
                defer ctx.allocator.free(tools_prompt);

                const prompt = if (forced_function) |forced|
                    try std.fmt.allocPrint(ctx.allocator, "{s}\nYou MUST call the {s} function. Do not respond with text, only call the function.\n", .{
                        tools_prompt,
                        forced,
                    })
                else
                    try ctx.allocator.dupe(u8, tools_prompt);
                defer ctx.allocator.free(prompt);

                try prependSystemPrompt(ctx.allocator, &messages, prompt);
            }
        }

        const same_named_draft_model = if (body.draft_model) |draft_model_name|
            body.model.len > 0 and std.mem.eql(u8, draft_model_name, body.model)
        else
            false;
        const effective_draft_model_name: ?[]const u8 = if (same_named_draft_model) null else body.draft_model;

        const want_stream = body.stream orelse false;
        // Caching requires an explicit non-empty key: keyless requests would all
        // share one per-model namespace, leaking prompt presence across callers.
        const prompt_cache_key: ?[]const u8 = if (body.prompt_cache_key) |key|
            (if (key.len > 0) key else null)
        else
            null;
        const configured_max_tokens: i32 = if (body.max_tokens) |mt| @intCast(mt) else 256;
        const queue_units = self.estimateGenerateQueueUnits(messages.items, configured_max_tokens);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);

        var config = generation.GenerationConfig{
            .max_tokens = configured_max_tokens,
            .temperature = body.temperature orelse 0,
            .top_p = body.top_p orelse 0,
            .top_k = if (body.top_k) |tk| @intCast(tk) else 0,
            .min_p = body.min_p orelse 0,
            .repetition_penalty = body.repetition_penalty orelse 1.0,
            .frequency_penalty = body.frequency_penalty orelse 0,
            .presence_penalty = body.presence_penalty orelse 0,
            .speculative_k = if (effective_draft_model_name != null)
                if (body.speculative_k) |k| @intCast(@max(k, 1)) else 4
            else
                4,
            .speculation_requested = effective_draft_model_name != null,
            .prefill_chunk_size = 256,
            .cache_dtype = body.cache_dtype,
            .cache_compaction_ratio = body.cache_compaction_ratio,
            .prompt_cache_enabled = self.config.prompt_cache.enabled and (body.prompt_cache orelse true) and prompt_cache_key != null and !want_stream,
            .prompt_cache_key = prompt_cache_key,
        };
        const backend_selection = parseGenerateBackendSelection(body.backend, body.mode, body.compiled_target) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = switch (err) {
                    error.InvalidGenerateMode => "unsupported generation mode",
                    error.InvalidCompiledTarget => "unsupported compiled_target",
                    else => "unsupported backend",
                },
            });
        };
        if (try rejectExplicitBackendIncompatibility(
            ctx,
            model_path,
            backend_selection.native_choice,
            self.config.allow_unknown_models,
        )) |response|
            return response;
        const allow_onnx = effective_draft_model_name == null and
            !backend_selection.graph_mode_requested and
            (body.backend == null or backend_selection.native_choice == .onnx);

        if (body.response_format) |rf| {
            if (std.mem.eql(u8, rf.type, "json_object")) {
                config.grammar = "json";
            } else if (std.mem.eql(u8, rf.type, "json_schema")) {
                const schema_cfg = rf.json_schema orelse {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "response_format.json_schema is required for type=json_schema",
                    });
                };
                const schema = schema_cfg.schema orelse {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "response_format.json_schema.schema is required for type=json_schema",
                    });
                };
                config.grammar = grammar_mod.buildJsonSchemaGrammar(ctx.allocator, schema) catch |err| {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = @errorName(err),
                    });
                };
            } else if (!std.mem.eql(u8, rf.type, "text")) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "unsupported response_format.type",
                });
            }
        }

        if (body.grammar) |grammar| {
            if (grammar.len == 0) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "grammar must not be empty",
                });
            }
            if (!std.mem.eql(u8, grammar, "json")) {
                var compiled = grammar_mod.GbnfGrammar.parse(ctx.allocator, grammar) catch |err| {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = @errorName(err),
                    });
                };
                compiled.deinit();
            }
            config.grammar = grammar;
        }

        // Try plain HF ONNX decoder-only VLM packages before Ort GenAI overlays.
        if (allow_onnx and build_options.enable_onnx and
            !c_file.fileExistsInDir(ctx.allocator, model_path, "genai_config.json") and
            onnx_decoder_only_vlm.isSupportedModelDir(ctx.allocator, model_path))
        {
            var prompt_override: ?[]u8 = null;
            defer if (prompt_override) |prompt| ctx.allocator.free(prompt);
            if (tool_parser) |*parser| {
                if (std.mem.eql(u8, parser.name(), "functiongemma")) {
                    prompt_override = try buildFunctionGemmaPrompt(
                        ctx.allocator,
                        "",
                        messages.items,
                    );
                }
            }

            var pipeline = onnx_decoder_only_vlm.Pipeline.load(ctx.allocator, model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer pipeline.deinit();
            pipeline.prompt_override = if (prompt_override) |prompt| prompt else null;

            if (config.grammar != null) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "grammar-constrained decoding is native-backend only; ONNX generation remains unconstrained-only",
                });
            }

            if (want_stream) {
                return self.streamGenerate(ctx, body.model, &pipeline, messages.items, config, if (tool_parser) |*parser| parser else null);
            }

            var result = generateMaybeStopOnTool(&pipeline, messages.items, config, if (tool_parser) |*parser| parser else null) catch |err|
                return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
            defer result.deinit();

            var response_text = result.text;
            var tool_response_text: ?[]u8 = null;
            defer if (tool_response_text) |text| ctx.allocator.free(text);
            const parsed_tool_calls = if (tool_parser) |*parser| blk: {
                parser.reset();
                _ = parser.feed(result.text) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                tool_response_text = parser.finishText(ctx.allocator) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                response_text = tool_response_text.?;
                if (response_text.len == 0) response_text = result.text;
                const calls = parser.toolCalls();
                break :blk if (calls.len > 0) calls else null;
            } else null;

            if (parsed_tool_calls == null and tool_parser != null and response_text.len == 0) {
                response_text = "No tool call was emitted.";
            }

            var formatted_response_text: ?[]u8 = null;
            defer if (formatted_response_text) |text| ctx.allocator.free(text);
            if (parsed_tool_calls == null) {
                formatted_response_text = coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text) catch |err|
                    return ctx.status(500).json(.{
                        .@"error" = "STRUCTURED_OUTPUT_INVALID",
                        .message = @errorName(err),
                    });
                if (formatted_response_text) |text| response_text = text;
            }

            return self.buildGenerateResponse(
                ctx,
                body.model,
                response_text,
                if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
                result.prompt_tokens,
                result.tokens_used,
                0,
                parsed_tool_calls,
            );
        }

        // Try ortgenai first (models with genai_config.json)
        if (allow_onnx and build_options.enable_onnx) {
            const ortgenai = backends_mod.ortgenai;
            const ort_model_dir = ortgenai.prepareGenerativeModelPackage(ctx.allocator, model_path) catch null;
            defer if (ort_model_dir) |prepared| ctx.allocator.free(prepared);
            if (ort_model_dir) |prepared_model_dir| {
                var ort_manifest = manifest_mod.loadFromDir(ctx.allocator, prepared_model_dir) catch |err|
                    return modelLoadFailureResponse(ctx, err);
                defer ort_manifest.deinit();

                const use_functiongemma_prompt_override = if (tool_parser) |*parser|
                    std.mem.eql(u8, parser.name(), "functiongemma")
                else
                    false;

                var ort_chat_template_storage: ?generation.ChatTemplate = null;
                defer if (ort_chat_template_storage) |*ct| ct.deinit();
                if (!use_functiongemma_prompt_override) {
                    if (ort_manifest.chat_template) |ct_source| {
                        ort_chat_template_storage = generation.ChatTemplate.init(
                            ctx.allocator,
                            ct_source,
                            ort_manifest.bos_token,
                            ort_manifest.eos_token,
                            ort_manifest.unk_token,
                            ort_manifest.pad_token,
                        ) catch |err| blk: {
                            std.log.warn("chat template init failed for {s}: {s}", .{ model_path, @errorName(err) });
                            break :blk null;
                        };
                    }
                }

                var prompt_override: ?[]u8 = null;
                defer if (prompt_override) |prompt| ctx.allocator.free(prompt);
                if (use_functiongemma_prompt_override) {
                    prompt_override = try buildFunctionGemmaPrompt(
                        ctx.allocator,
                        ort_manifest.bos_token,
                        messages.items,
                    );
                }

                var gen_model = ortgenai.GenAiModel.load(ctx.allocator, prepared_model_dir) catch |err|
                    return modelLoadFailureResponse(ctx, err);
                defer gen_model.deinit();

                var pipeline = generation.GenerationPipeline{
                    .allocator = ctx.allocator,
                    .model = &gen_model,
                    .chat_template = if (ort_chat_template_storage) |*ct| ct else null,
                    .prompt_override = if (prompt_override) |prompt| prompt else null,
                };

                if (config.grammar != null) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "grammar-constrained decoding is native-backend only; ONNX generation remains unconstrained-only",
                    });
                }

                if (want_stream) {
                    return self.streamGenerate(ctx, body.model, &pipeline, messages.items, config, if (tool_parser) |*parser| parser else null);
                }

                var result = generateMaybeStopOnTool(&pipeline, messages.items, config, if (tool_parser) |*parser| parser else null) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                defer result.deinit();

                var response_text = result.text;
                var tool_response_text: ?[]u8 = null;
                defer if (tool_response_text) |text| ctx.allocator.free(text);
                const parsed_tool_calls = if (tool_parser) |*parser| blk: {
                    parser.reset();
                    _ = parser.feed(result.text) catch |err|
                        return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                    tool_response_text = parser.finishText(ctx.allocator) catch |err|
                        return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                    response_text = tool_response_text.?;
                    if (response_text.len == 0) response_text = result.text;
                    const calls = parser.toolCalls();
                    break :blk if (calls.len > 0) calls else null;
                } else null;

                if (parsed_tool_calls == null and tool_parser != null and response_text.len == 0) {
                    response_text = "No tool call was emitted.";
                }

                var formatted_response_text: ?[]u8 = null;
                defer if (formatted_response_text) |text| ctx.allocator.free(text);
                if (parsed_tool_calls == null) {
                    formatted_response_text = coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text) catch |err|
                        return ctx.status(500).json(.{
                            .@"error" = "STRUCTURED_OUTPUT_INVALID",
                            .message = @errorName(err),
                        });
                    if (formatted_response_text) |text| response_text = text;
                }

                return self.buildGenerateResponse(
                    ctx,
                    body.model,
                    response_text,
                    if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
                    result.prompt_tokens,
                    result.tokens_used,
                    0,
                    parsed_tool_calls,
                );
            }
        }

        // Fall back to native generation (CPU/GPU GPT arch forward pass).
        var model_handle = if (backend_selection.native_choice != .auto) blk: {
            var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
            configureGenerateBackendPreference(&request_session_manager, backend_selection);
            break :blk self.model_manager.acquireFromDirWithPreferredBackends(model_path, request_session_manager.preferred_backends, false) catch |err|
                return modelLoadFailureResponse(ctx, err);
        } else self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();
        model.lockNativeGeneration();
        defer model.unlockNativeGeneration();
        const prompt_bytes = self.estimateGeneratePromptBytes(messages.items);
        const prompt_tokens = self.estimateNativePromptTokens(ctx.allocator, model, messages.items) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZE_FAILED", .message = @errorName(err) });
        var native_generate_lease: ?runtime.scheduler.native_generate.Lease = null;
        defer if (native_generate_lease) |lease| {
            if (model.native_generate_coordinator) |coordinator| coordinator.release(lease);
        };
        if (model.native_generate_coordinator) |coordinator| {
            native_generate_lease = try coordinator.acquire(.{
                .requested_units = queue_units,
                .prompt_bytes = prompt_bytes,
                .max_tokens = configured_max_tokens,
            });
            config.prefill_chunk_size = native_generate_lease.?.prefill_chunk_size;
        }

        const gpt_config = session_factory.getGptConfig(model.session) orelse {
            // The session is not a decoder at all, which in practice means the
            // architecture was never recognized and the model fell through to the
            // default encoder path. Name it, so the caller can tell "unsupported model"
            // apart from "Antfly is broken".
            var inspection: model_compatibility.Inspection = model_compatibility.inspectAlloc(ctx.allocator, &model.manifest) catch .{
                .architecture = try ctx.allocator.dupe(u8, "unknown"),
            };
            defer inspection.deinit(ctx.allocator);
            return ctx.status(400).json(.{
                .@"error" = "INCOMPATIBLE_MODEL",
                .message = try std.fmt.allocPrint(
                    ctx.allocator,
                    "model architecture \"{s}\" does not provide a generation runtime",
                    .{inspection.architecture},
                ),
            });
        };
        const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            .pjrt => return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = "unexpected PJRT backend in native generation path" }),
            .onnx => return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = "unexpected ONNX backend in native generation path" }),
            .wasm => return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = "unexpected WASM backend in server generation path" }),
        };
        const kv_dtype = if (config.cache_dtype) |name|
            runtime.kv.pool.parseKvDType(name) orelse
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid cache_dtype value" })
        else
            session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
        const admission_prefill_chunk = if (config.prefill_chunk_size > 0) config.prefill_chunk_size else 256;
        const resource_estimate = runtime.tier.memory.estimateGptGeneration(
            backend_kind,
            kv_dtype,
            gpt_config,
            prompt_tokens,
            @intCast(@max(config.max_tokens, 1)),
            admission_prefill_chunk,
        ) catch |err| switch (err) {
            error.InvalidModelConfig => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model contains invalid generation dimensions",
            }),
            error.ResourceLimitExceeded => return ctx.status(400).json(.{
                .@"error" = "MODEL_RESOURCE_LIMIT",
                .message = "request resource estimate exceeds addressable memory",
            }),
        };

        const tok = model.getTokenizer();
        var draft_cb: ?ops.ComputeBackend = null;
        defer if (draft_cb) |*cb_value| cb_value.deinit();
        var draft_gpt_config: ?@import("../models/gpt.zig").Config = null;
        var draft_model_handle: ?model_manager_mod.ModelHandle = null;
        defer if (draft_model_handle) |*handle| handle.release();
        var draft_model_for_generation: ?*model_manager_mod.LoadedModel = null;
        var draft_backend_kind: ?runtime.kv.pool.BackendKind = null;
        var draft_kv_dtype: ?runtime.kv.pool.KvDType = null;
        var draft_resource_estimate: ?runtime.tier.memory.Estimate = null;
        var pjrt_client: ?pjrt_lib.pjrt.Client = null;
        defer if (pjrt_client) |*client| client.deinit();
        var pjrt_plugin_path: ?[:0]u8 = null;
        defer if (pjrt_plugin_path) |path| ctx.allocator.free(path);
        if (backend_selection.compiled_partition_backend == .pjrt) {
            pjrt_plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(ctx.allocator);
            const plugin_path = pjrt_plugin_path orelse
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "xla backend requires TERMITE_XLA_PLUGIN or TERMITE_PJRT_PLUGIN",
                });
            pjrt_client = pjrt_lib.pjrt.Client.init(plugin_path) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        }

        if (effective_draft_model_name) |draft_model_name| {
            const draft_model_path = self.resolveModelPath(ctx.io, draft_model_name, "generators") catch
                return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "draft model not found" });
            if (!std.mem.eql(u8, draft_model_path, model_path)) {
                config.draft_model = draft_model_path;
                var load_draft_backend = true;
                if (config.speculation_policy == .auto) {
                    var draft_manifest = manifest_mod.loadFromDir(ctx.allocator, draft_model_path) catch |err|
                        return modelLoadFailureResponse(ctx, err);
                    defer draft_manifest.deinit();
                    const draft_cfg = session_factory.loadGptConfigFromModelDir(ctx.allocator, draft_model_path, draft_manifest) catch |err|
                        return ctx.status(400).json(.{ .@"error" = "INVALID_MODEL", .message = @errorName(err) });
                    if (shouldSkipAutoMtpDraftLoad(config, draft_cfg)) {
                        draft_gpt_config = draft_cfg;
                        load_draft_backend = false;
                    }
                }
                if (load_draft_backend) {
                    draft_model_handle = if (backend_selection.native_choice != .auto) blk: {
                        var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
                        configureGenerateBackendPreference(&request_session_manager, backend_selection);
                        break :blk self.model_manager.acquireFromDirWithPreferredBackends(draft_model_path, request_session_manager.preferred_backends, false) catch |err|
                            return modelLoadFailureResponse(ctx, err);
                    } else self.model_manager.acquireFromDir(draft_model_path) catch |err|
                        return modelLoadFailureResponse(ctx, err);
                    const draft_model = draft_model_handle.?.get();
                    const draft_cfg = session_factory.getGptConfig(draft_model.session) orelse
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_MODEL",
                            .message = "draft_model does not support generation",
                        });
                    const draft_tok = draft_model.getTokenizer();
                    const target_special = tok.specialTokens();
                    const draft_special = draft_tok.specialTokens();
                    if (draft_tok.vocabSize() != tok.vocabSize() or
                        draft_cfg.vocab_size != gpt_config.vocab_size or
                        draft_special.cls_id != target_special.cls_id or
                        draft_special.sep_id != target_special.sep_id or
                        draft_special.pad_id != target_special.pad_id or
                        draft_special.unk_id != target_special.unk_id)
                    {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "draft_model tokenizer is incompatible with target model",
                        });
                    }

                    const actual_draft_backend: runtime.kv.pool.BackendKind = switch (draft_model.session.backend()) {
                        .native => .native,
                        .metal => .metal,
                        .cuda => .cuda,
                        .pjrt, .onnx, .wasm => return ctx.status(400).json(.{
                            .@"error" = "INVALID_MODEL",
                            .message = "draft_model does not use a supported generation backend",
                        }),
                    };
                    const actual_draft_kv_dtype = session_factory.recommendedKvDTypeForSession(
                        draft_model.session,
                        actual_draft_backend,
                    );
                    draft_resource_estimate = runtime.tier.memory.estimateGptGeneration(
                        actual_draft_backend,
                        actual_draft_kv_dtype,
                        draft_cfg,
                        prompt_tokens,
                        @intCast(@max(config.max_tokens, 1)),
                        admission_prefill_chunk,
                    ) catch |err| switch (err) {
                        error.InvalidModelConfig => return ctx.status(400).json(.{
                            .@"error" = "INVALID_MODEL",
                            .message = "draft_model contains invalid generation dimensions",
                        }),
                        error.ResourceLimitExceeded => return ctx.status(400).json(.{
                            .@"error" = "MODEL_RESOURCE_LIMIT",
                            .message = "draft_model resource estimate exceeds addressable memory",
                        }),
                    };
                    draft_model_for_generation = draft_model;
                    draft_backend_kind = actual_draft_backend;
                    draft_kv_dtype = actual_draft_kv_dtype;
                    draft_gpt_config = draft_cfg;
                }
            }
        }

        const budget_backend_class: runtime.tier.memory.BackendClass =
            if (backend_kind != .native or (draft_backend_kind != null and draft_backend_kind.? != .native))
                .gpu
            else
                .cpu;
        var budget_limits = runtime.tier.memory.defaultLimitsForBackend(budget_backend_class);
        budget_limits = session_factory.widenBudgetLimitsForSession(model.session, budget_limits);
        if (draft_model_for_generation) |draft_model| {
            budget_limits = session_factory.widenBudgetLimitsForSession(draft_model.session, budget_limits);
        }
        budget_limits = self.config.generation_budget_overrides.apply(budget_limits);
        var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
        run_budget.reserveEstimate(resource_estimate) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                return ctx.status(400).json(.{
                    .@"error" = "MODEL_RESOURCE_LIMIT",
                    .message = memoryBudgetExceededMessage(ctx.allocator, model.session, &run_budget),
                });
            }
            return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        };
        if (draft_resource_estimate) |estimate| {
            run_budget.reserveEstimate(estimate) catch |err| {
                if (err == error.MemoryBudgetExceeded) {
                    return ctx.status(400).json(.{
                        .@"error" = "MODEL_RESOURCE_LIMIT",
                        .message = memoryBudgetExceededMessage(
                            ctx.allocator,
                            draft_model_for_generation.?.session,
                            &run_budget,
                        ),
                    });
                }
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
            };
        }
        const target_backend_class: runtime.tier.memory.BackendClass =
            if (backend_kind == .native) .cpu else .gpu;
        const target_admission_limits = self.config.generation_budget_overrides.apply(
            session_factory.widenBudgetLimitsForSession(
                model.session,
                runtime.tier.memory.defaultLimitsForBackend(target_backend_class),
            ),
        );
        var admission_requests: [2]runtime.tier.memory.AdmissionRequest = undefined;
        admission_requests[0] = .{
            .backend_class = target_backend_class,
            .limits = target_admission_limits,
            .amounts = .fromEstimate(resource_estimate),
        };
        const admission_request_count: usize = if (draft_resource_estimate) |estimate| blk: {
            const draft_backend_class: runtime.tier.memory.BackendClass =
                if (draft_backend_kind.? == .native) .cpu else .gpu;
            admission_requests[1] = .{
                .backend_class = draft_backend_class,
                .limits = self.config.generation_budget_overrides.apply(
                    session_factory.widenBudgetLimitsForSession(
                        draft_model_for_generation.?.session,
                        runtime.tier.memory.defaultLimitsForBackend(draft_backend_class),
                    ),
                ),
                .amounts = .fromEstimate(estimate),
            };
            break :blk 2;
        } else 1;
        var admission_lease = self.model_manager.acquireRunResourceEstimates(
            admission_requests[0..admission_request_count],
        ) catch |err| switch (err) {
            error.ResourceLimitExceeded => return ctx.status(400).json(.{
                .@"error" = "MODEL_RESOURCE_LIMIT",
                .message = "request exceeds the configured inference resource budget",
            }),
            error.ResourceTemporarilyUnavailable => return modelResourceBusyResponse(ctx),
        };
        defer admission_lease.release();

        if (draft_model_for_generation) |draft_model| {
            draft_cb = session_factory.getComputeBackendWithBudget(draft_model.session, ctx.allocator, &run_budget) catch |err| {
                if (err == error.MemoryBudgetExceeded) {
                    return ctx.status(400).json(.{
                        .@"error" = "MODEL_RESOURCE_LIMIT",
                        .message = memoryBudgetExceededMessage(ctx.allocator, draft_model.session, &run_budget),
                    });
                }
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
            };
        }

        var kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
        defer kv_manager.deinit();
        var draft_kv_manager: ?runtime.kv.manager.KvManager = null;
        defer if (draft_kv_manager) |*manager| manager.deinit();

        var cb = session_factory.getComputeBackendWithBudget(model.session, ctx.allocator, &run_budget) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                return ctx.status(400).json(.{
                    .@"error" = "MODEL_RESOURCE_LIMIT",
                    .message = memoryBudgetExceededMessage(ctx.allocator, model.session, &run_budget),
                });
            }
            return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        };
        defer cb.deinit();
        const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
            null
        else if (gpt_config.sliding_window > 0)
            gpt_config.sliding_window
        else if (gpt_config.max_position_embeddings > 0)
            gpt_config.max_position_embeddings
        else
            null;
        const pool_config: runtime.kv.pool.KvPoolConfig = .{
            .backend = backend_kind,
            .dtype = kv_dtype,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
            .num_kv_heads = gpt_config.maxKvHeads(),
            .head_dim = gpt_config.maxHeadDim(),
            .sliding_window_size = sliding_window_size,
        };

        var prompt_cache: ?*runtime.kv.prompt_cache.PromptPrefixCache = null;
        var active_kv_manager: *runtime.kv.manager.KvManager = &kv_manager;
        var active_kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null;
        var pool_id: runtime.kv.block.KvPoolId = undefined;
        if (config.prompt_cache_enabled and (backend_kind == .native or backend_kind == .metal or backend_kind == .cuda) and
            backend_selection.compiled_partition_backend == null and
            effective_draft_model_name == null and
            config.cache_compaction_ratio == null)
        {
            const prompt_cache_config = self.config.prompt_cache.runtimeConfig(
                self.config.prompt_cache_resource_usage_observer,
            );
            self.model_manager.rebalancePromptCaches(
                model,
                prompt_cache_config,
            );
            const cache_ready = if (backend_kind == .metal or backend_kind == .cuda) blk: {
                const ensured = model.prompt_prefix_cache.ensureStorage(pool_config) catch |err| {
                    self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                };
                const storage = if (ensured) |result| result.storage else break :blk false;
                if (storage.device_write_hook == null) {
                    cb.provisionKvDeviceWriteHook(storage) catch |err|
                        return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                }
                if (storage.device_write_hook == null) break :blk false;
                active_kv_storage = storage;
                break :blk true;
            } else blk: {
                const maybe_cache_pool_id = model.prompt_prefix_cache.ensurePool(pool_config) catch |err| {
                    self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                };
                break :blk maybe_cache_pool_id != null;
            };

            if (cache_ready) {
                active_kv_manager = model.prompt_prefix_cache.managerPtr();
                pool_id = model.prompt_prefix_cache.pool_id.?;
                prompt_cache = &model.prompt_prefix_cache;
            } else {
                pool_id = kv_manager.addPool(pool_config) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                config.prompt_cache_enabled = false;
                self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
            }
        } else {
            config.prompt_cache_enabled = false;
            pool_id = kv_manager.addPool(pool_config) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        }

        var kv_storage: ?runtime.kv.storage_runtime.KvStorageRuntime = if (active_kv_storage == null)
            runtime.kv.storage_runtime.KvStorageRuntime.init(ctx.allocator, pool_config) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) })
        else
            null;
        defer if (kv_storage) |*storage| storage.deinit();
        if (kv_storage) |*storage| {
            cb.provisionKvDeviceWriteHook(storage) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        }
        var decode_state = generation.NativeDecodeState.initPaged(ctx.allocator, active_kv_manager, pool_id, model.shared_moe_cache);
        if (active_kv_storage) |storage| {
            decode_state.kv_storage = storage;
        } else if (kv_storage) |*storage| {
            decode_state.kv_storage = storage;
        }
        defer decode_state.deinit();

        var draft_decode_state: ?generation.NativeDecodeState = null;
        defer if (draft_decode_state) |*state| state.deinit();

        if (draft_cb != null) {
            if (draft_gpt_config) |draft_cfg| {
                draft_kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
                const draft_sliding_window_size: ?u32 = if (draft_cfg.position_encoding == .absolute)
                    null
                else if (draft_cfg.sliding_window > 0)
                    @intCast(draft_cfg.sliding_window)
                else
                    null;
                const draft_pool_id = draft_kv_manager.?.addPool(.{
                    .backend = draft_backend_kind.?,
                    .dtype = draft_kv_dtype.?,
                    .page_size_tokens = 16,
                    .num_layers_packed = @intCast(draft_cfg.num_hidden_layers),
                    .num_kv_heads = draft_cfg.maxKvHeads(),
                    .head_dim = draft_cfg.maxHeadDim(),
                    .sliding_window_size = draft_sliding_window_size,
                }) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                draft_decode_state = generation.NativeDecodeState.initPaged(ctx.allocator, &draft_kv_manager.?, draft_pool_id, null);
            }
        }

        const auto_metal_whole_model = shouldAutoUseMetalWholeModelGenerate(
            model.session.backend(),
            graph_mod.metal_executor.supportsSession(model.session),
            generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config),
            backend_selection,
        );
        const effective_compiled_partition_backend: ?ops.BackendKind = if (auto_metal_whole_model)
            .metal
        else
            backend_selection.compiled_partition_backend;
        const effective_compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = if (auto_metal_whole_model)
            .whole_model
        else
            backend_selection.compiled_attachment_target;

        const graph_mode = backend_selection.graph_mode_requested or
            effective_compiled_partition_backend != null or
            graphModeEnabled();
        const use_scheduler = !graph_mode;
        const use_model_graph_cache = graph_mode and
            build_options.enable_metal and
            model.session.backend() == .metal and
            effective_compiled_partition_backend == .metal and
            effective_compiled_attachment_target == .whole_model and
            graph_mod.metal_executor.supportsSession(model.session) and
            !generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config);
        var request_graph_cache: ?graph_mod.cache.GraphCache = if (graph_mode and !use_model_graph_cache)
            graph_mod.cache.GraphCache.init(ctx.allocator)
        else
            null;
        defer if (request_graph_cache) |*cache| cache.deinit();
        const graph_cache = if (!graph_mode)
            null
        else if (use_model_graph_cache)
            &model.native_generation_graph_cache
        else
            &request_graph_cache.?;
        const request_generate_timing = serverGenerateTimingEnabled();
        const debug_metal_timing = request_generate_timing and
            use_model_graph_cache and
            platform.env.getenvBool("TERMITE_DEBUG_METAL_TIMING");
        if (debug_metal_timing) graph_mod.metal_executor.resetTimingStats();

        var pipeline = generation.NativeGenerationPipeline{
            .allocator = ctx.allocator,
            .io = ctx.io,
            .cb = cb,
            .session = model.session,
            .gpt_config = gpt_config,
            .kv_dtype = kv_dtype,
            .shared_moe_cache = model.shared_moe_cache,
            .tokenizer = tok,
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .chat_template = model.chat_tmpl,
            .print_timing = request_generate_timing,
            .model_dir = model_path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
            .scheduler = if (use_scheduler) model.native_generate_coordinator else null,
            .scheduler_lease = if (use_scheduler) if (native_generate_lease) |*lease| lease else null else null,
            .draft_cb = if (draft_cb) |cb_value| cb_value else null,
            .draft_gpt_config = draft_gpt_config,
            .draft_decode_state = if (draft_decode_state) |*state| state else null,
            .prompt_cache = prompt_cache,
            .graph_cache = graph_cache,
            .compiled_partition_backend = effective_compiled_partition_backend,
            .compiled_attachment_target = effective_compiled_attachment_target,
            .pjrt_client = if (pjrt_client) |*client| client else null,
        };

        if (want_stream) {
            return self.streamGenerate(ctx, body.model, &pipeline, messages.items, config, if (tool_parser) |*parser| parser else null);
        }

        var result = generateMaybeStopOnTool(&pipeline, messages.items, config, if (tool_parser) |*parser| parser else null) catch |err|
            return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
        defer result.deinit();
        if (debug_metal_timing) {
            if (model.native_generation_graph_cache.getSessionCompiledModelRuntime(.metal, .whole_model)) |runtime_model| {
                runtime_model.printDebugTiming();
            }
        }

        var response_text = result.text;
        var tool_response_text: ?[]u8 = null;
        defer if (tool_response_text) |text| ctx.allocator.free(text);
        const parsed_tool_calls = if (tool_parser) |*parser| blk: {
            parser.reset();
            _ = parser.feed(result.text) catch |err|
                return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
            tool_response_text = parser.finishText(ctx.allocator) catch |err|
                return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
            response_text = tool_response_text.?;
            if (response_text.len == 0) response_text = result.text;
            const calls = parser.toolCalls();
            break :blk if (calls.len > 0) calls else null;
        } else null;

        if (parsed_tool_calls == null and tool_parser != null and response_text.len == 0) {
            response_text = "No tool call was emitted.";
        }

        var formatted_response_text: ?[]u8 = null;
        defer if (formatted_response_text) |text| ctx.allocator.free(text);
        if (parsed_tool_calls == null) {
            formatted_response_text = coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text) catch |err|
                return ctx.status(500).json(.{
                    .@"error" = "STRUCTURED_OUTPUT_INVALID",
                    .message = @errorName(err),
                });
            if (formatted_response_text) |text| response_text = text;
        }

        return self.buildGenerateResponse(
            ctx,
            body.model,
            response_text,
            if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
            result.prompt_tokens,
            result.tokens_used,
            result.cached_prompt_tokens,
            parsed_tool_calls,
        );
    }

    const OwnedGenerateMessages = struct {
        allocator: std.mem.Allocator,
        messages: []generation.Message = &.{},
        decoded_images: [][]u8 = &.{},
        image_slices: [][]const []const u8 = &.{},
        content_parts: [][]const generation.Message.ContentPart = &.{},

        fn deinit(self: *OwnedGenerateMessages) void {
            for (self.messages) |msg| self.allocator.free(msg.content);
            self.allocator.free(self.messages);
            for (self.decoded_images) |img| self.allocator.free(img);
            self.allocator.free(self.decoded_images);
            for (self.image_slices) |slice| self.allocator.free(slice);
            self.allocator.free(self.image_slices);
            for (self.content_parts) |parts| self.allocator.free(parts);
            self.allocator.free(self.content_parts);
            self.* = .{ .allocator = self.allocator };
        }
    };

    fn parseGenerateMessages(self: *Node, allocator: std.mem.Allocator, body: api.GenerateRequest) !OwnedGenerateMessages {
        var messages = std.ArrayListUnmanaged(generation.Message).empty;
        errdefer {
            for (messages.items) |msg| allocator.free(msg.content);
            messages.deinit(allocator);
        }
        var decoded_images = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (decoded_images.items) |img| allocator.free(img);
            decoded_images.deinit(allocator);
        }
        var image_slices = std.ArrayListUnmanaged([]const []const u8).empty;
        errdefer {
            for (image_slices.items) |slice| allocator.free(slice);
            image_slices.deinit(allocator);
        }
        var content_parts = std.ArrayListUnmanaged([]const generation.Message.ContentPart).empty;
        errdefer {
            for (content_parts.items) |parts| allocator.free(parts);
            content_parts.deinit(allocator);
        }

        for (body.messages) |msg| {
            const role: []const u8 = switch (msg.role) {
                .system => "system",
                .user => "user",
                .assistant => "assistant",
                .tool => "tool",
            };

            var text_buf = std.ArrayListUnmanaged(u8).empty;
            defer text_buf.deinit(allocator);
            var msg_images = std.ArrayListUnmanaged([]const u8).empty;
            defer msg_images.deinit(allocator);
            var msg_parts = std.ArrayListUnmanaged(generation.Message.ContentPart).empty;
            defer msg_parts.deinit(allocator);

            if (msg.content) |cv| {
                switch (cv) {
                    .string => |s| try text_buf.appendSlice(allocator, s),
                    .array => |arr| {
                        for (arr.items) |part| {
                            if (part != .object) continue;
                            const obj = part.object;
                            const type_val = obj.get("type") orelse continue;
                            if (type_val != .string) continue;
                            const ptype = type_val.string;

                            if (std.mem.eql(u8, ptype, "text")) {
                                if (obj.get("text")) |tv| {
                                    if (tv == .string) {
                                        try text_buf.appendSlice(allocator, tv.string);
                                        try msg_parts.append(allocator, .{ .text = tv.string });
                                    }
                                }
                            } else if (std.mem.eql(u8, ptype, "image_url")) {
                                const url_str = blk: {
                                    const iu = obj.get("image_url") orelse return error.InvalidImageUrl;
                                    if (iu == .object) {
                                        if (iu.object.get("url")) |u| {
                                            if (u == .string) break :blk u.string;
                                        }
                                    } else if (iu == .string) break :blk iu.string;
                                    return error.InvalidImageUrl;
                                };
                                const downloaded = try downloadRemoteContent(self, allocator, url_str);
                                defer allocator.free(downloaded.content_type);
                                try decoded_images.append(allocator, downloaded.data);
                                try msg_images.append(allocator, downloaded.data);
                                try msg_parts.append(allocator, .{ .image = msg_images.items.len - 1 });
                            }
                        }
                    },
                    else => {},
                }
            }

            const content = try allocator.dupe(u8, text_buf.items);
            const msg_img_slice: ?[]const []const u8 = if (msg_images.items.len > 0)
                try allocator.dupe([]const u8, msg_images.items)
            else
                null;
            if (msg_img_slice) |slice| try image_slices.append(allocator, slice);
            const msg_part_slice: ?[]const generation.Message.ContentPart = if (msg_parts.items.len > 0)
                try allocator.dupe(generation.Message.ContentPart, msg_parts.items)
            else
                null;
            if (msg_part_slice) |parts| try content_parts.append(allocator, parts);

            try messages.append(allocator, .{
                .role = role,
                .content = content,
                .image_bytes = msg_img_slice,
                .content_parts = msg_part_slice,
            });
        }

        return .{
            .allocator = allocator,
            .messages = try messages.toOwnedSlice(allocator),
            .decoded_images = try decoded_images.toOwnedSlice(allocator),
            .image_slices = try image_slices.toOwnedSlice(allocator),
            .content_parts = try content_parts.toOwnedSlice(allocator),
        };
    }

    fn generateBatchUnsupportedReasonPreflight(body: api.GenerateRequest) ?api.GenerateBatchError {
        if (body.stream orelse false) return .{ .code = "UNSUPPORTED_STREAM", .message = "batch generation does not support stream=true", .retryable = false };
        if (body.tools != null or body.tool_choice != null) return .{ .code = "UNSUPPORTED_TOOLS", .message = "batch generation does not support tools yet", .retryable = false };
        if (body.draft_model != null) return .{ .code = "UNSUPPORTED_DRAFT_MODEL", .message = "batch generation does not support draft_model yet", .retryable = false };
        if (body.mode) |mode| {
            if (!std.mem.eql(u8, mode, "eager")) return .{ .code = "UNSUPPORTED_MODE", .message = "batch generation requires eager native mode", .retryable = false };
        }
        if (body.compiled_target != null) return .{ .code = "UNSUPPORTED_COMPILED_TARGET", .message = "batch generation does not support compiled_target yet", .retryable = false };
        if (body.backend) |backend| switch (backend) {
            .auto, .native, .metal, .cuda => {},
            .onnx, .xla, .webgpu, .wasm => return .{ .code = "UNSUPPORTED_BACKEND", .message = "batch generation requires a native backend", .retryable = false },
        };
        if (generateRequestHasNonTextContentParts(body)) {
            return .{ .code = "UNSUPPORTED_MULTIMODAL", .message = "batch generation currently supports text-only native requests", .retryable = false };
        }
        return null;
    }

    fn generateRequestHasNonTextContentParts(body: api.GenerateRequest) bool {
        for (body.messages) |msg| {
            const content = msg.content orelse continue;
            switch (content) {
                .array => |parts| {
                    for (parts.items) |part| {
                        if (part != .object) continue;
                        const type_value = part.object.get("type") orelse continue;
                        if (type_value != .string) continue;
                        if (!std.mem.eql(u8, type_value.string, "text")) return true;
                    }
                },
                .object => return true,
                else => {},
            }
        }
        return false;
    }

    fn generateBatchUnsupportedReason(body: api.GenerateRequest, messages: []const generation.Message) ?api.GenerateBatchError {
        if (generateBatchUnsupportedReasonPreflight(body)) |reason| return reason;
        if (generation.messagesHaveImages(messages) or generation.messagesHaveAudio(messages)) {
            return .{ .code = "UNSUPPORTED_MULTIMODAL", .message = "batch generation currently supports text-only native requests", .retryable = false };
        }
        return null;
    }

    fn generateConfigFromBody(allocator: std.mem.Allocator, body: api.GenerateRequest) !generation.GenerationConfig {
        var config = generation.GenerationConfig{
            .max_tokens = if (body.max_tokens) |mt| @intCast(mt) else 256,
            .temperature = body.temperature orelse 0,
            .top_p = body.top_p orelse 0,
            .top_k = if (body.top_k) |tk| @intCast(tk) else 0,
            .min_p = body.min_p orelse 0,
            .repetition_penalty = body.repetition_penalty orelse 1.0,
            .frequency_penalty = body.frequency_penalty orelse 0,
            .presence_penalty = body.presence_penalty orelse 0,
            .speculative_k = 4,
            .speculation_requested = false,
            .prefill_chunk_size = 256,
            .cache_dtype = body.cache_dtype,
            .cache_compaction_ratio = body.cache_compaction_ratio,
        };
        if (body.response_format) |rf| {
            if (std.mem.eql(u8, rf.type, "json_object")) {
                config.grammar = "json";
            } else if (std.mem.eql(u8, rf.type, "json_schema")) {
                const schema_cfg = rf.json_schema orelse return error.MissingJsonSchema;
                const schema = schema_cfg.schema orelse return error.MissingJsonSchema;
                config.grammar = try grammar_mod.buildJsonSchemaGrammar(allocator, schema);
            } else if (!std.mem.eql(u8, rf.type, "text")) {
                return error.UnsupportedResponseFormat;
            }
        }
        if (body.grammar) |grammar| {
            if (grammar.len == 0) return error.EmptyGrammar;
            if (!std.mem.eql(u8, grammar, "json")) {
                var compiled = try grammar_mod.GbnfGrammar.parse(allocator, grammar);
                compiled.deinit();
            }
            config.grammar = grammar;
        }
        return config;
    }

    fn buildGenerateResponseValue(
        allocator: std.mem.Allocator,
        model_name: []const u8,
        response_text: []const u8,
        finish_reason: []const u8,
        prompt_tokens: usize,
        completion_tokens: usize,
    ) !api.GenerateResponse {
        const completion_id = try allocCompletionId(allocator);
        const created = completionCreatedTimestamp();
        const content = try allocator.dupe(u8, response_text);
        const choices = try allocator.alloc(api.GenerateChoice, 1);
        choices[0] = .{
            .index = 0,
            .message = .{ .role = .assistant, .content = content },
            .finish_reason = parseFinishReason(finish_reason),
        };
        return .{
            .id = completion_id,
            .object = "chat.completion",
            .created = created,
            .model = model_name,
            .choices = choices,
            .usage = tokenUsage(prompt_tokens, completion_tokens),
        };
    }

    const BatchGenerateTaskResult = struct {
        text: ?[]const u8 = null,
        finish_reason: []const u8 = "length",
        prompt_tokens: usize = 0,
        completion_tokens: usize = 0,
        @"error": ?api.GenerateBatchError = null,
    };

    fn applyBatchGenerateTaskResult(
        allocator: std.mem.Allocator,
        model_name: []const u8,
        task_result: BatchGenerateTaskResult,
        result: *api.GenerateBatchResultItem,
    ) !void {
        if (task_result.@"error") |batch_err| {
            result.@"error" = batch_err;
        } else if (task_result.text) |text| {
            result.response = try buildGenerateResponseValue(
                allocator,
                model_name,
                text,
                task_result.finish_reason,
                task_result.prompt_tokens,
                task_result.completion_tokens,
            );
        } else {
            result.@"error" = .{
                .code = "GENERATION_FAILED",
                .message = "missing batch generation result",
                .retryable = true,
            };
        }
    }

    fn batchModelLoadError(err: anyerror) api.GenerateBatchError {
        return switch (err) {
            error.UnknownModelCompatibility => .{
                .code = "UNKNOWN_MODEL_COMPATIBILITY",
                .message = "model compatibility is unknown",
                .retryable = false,
            },
            error.IncompatibleModel => .{
                .code = "INCOMPATIBLE_MODEL",
                .message = "model artifact is incompatible with the selected runtime",
                .retryable = false,
            },
            error.ResourceLimitExceeded => .{
                .code = "MODEL_RESOURCE_LIMIT",
                .message = "model resource plan exceeds the configured inference budget",
                .retryable = false,
            },
            error.ResourceTemporarilyUnavailable => .{
                .code = "MODEL_RESOURCE_BUSY",
                .message = "insufficient inference capacity is currently available",
                .retryable = true,
                .retry_after_ms = transient_capacity_retry_after_ms,
            },
            else => .{
                .code = "MODEL_LOAD_FAILED",
                .message = @errorName(err),
                .retryable = true,
            },
        };
    }

    fn batchAdmissionError(err: anyerror) api.GenerateBatchError {
        const retryable = err == error.ResourceTemporarilyUnavailable;
        return .{
            .code = if (retryable) "MODEL_RESOURCE_BUSY" else "MODEL_RESOURCE_LIMIT",
            .message = if (retryable)
                "insufficient inference capacity is currently available"
            else
                "request exceeds the configured inference resource budget",
            .retryable = retryable,
            .retry_after_ms = if (retryable) transient_capacity_retry_after_ms else null,
        };
    }

    const BatchGenerateTask = struct {
        allocator: std.mem.Allocator,
        pipeline: generation.NativeGenerationPipeline,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
        response_format: ?api.GenerateResponseFormat,
        out: *BatchGenerateTaskResult,

        fn run(self: *@This()) std.Io.Cancelable!void {
            self.runInner() catch |err| {
                self.out.@"error" = switch (err) {
                    error.InvalidStructuredOutput => .{
                        .code = "STRUCTURED_OUTPUT_INVALID",
                        .message = @errorName(err),
                        .retryable = true,
                    },
                    else => .{
                        .code = "GENERATION_FAILED",
                        .message = @errorName(err),
                        .retryable = true,
                    },
                };
            };
        }

        fn runInner(self: *@This()) !void {
            var result = try self.pipeline.generate(self.messages, self.config);
            defer result.deinit();
            var response_text = result.text;
            var formatted_response_text: ?[]u8 = null;
            defer if (formatted_response_text) |text| self.allocator.free(text);
            formatted_response_text = try coerceGenerateResponseFormat(self.allocator, self.response_format, response_text);
            if (formatted_response_text) |text| response_text = text;
            self.out.text = try self.allocator.dupe(u8, response_text);
            self.out.finish_reason = result.finish_reason;
            self.out.prompt_tokens = result.prompt_tokens;
            self.out.completion_tokens = result.tokens_used;
        }
    };

    const BatchExecutionMode = enum {
        /// NativeCompute is cheap request state over a shared, internally
        /// synchronized weight store. Give each item its own instance so task
        /// allocators and RunBudget accounting remain independent.
        isolated_parallel,
        /// Metal/CUDA sessions own stateful command/runtime objects. Reusing
        /// those objects serially avoids racing streams, scratch buffers, and
        /// graph-capture state while still amortizing backend construction.
        shared_serial,
    };

    fn batchExecutionMode(backend: runtime.kv.pool.BackendKind) BatchExecutionMode {
        return switch (backend) {
            .native => .isolated_parallel,
            .metal, .cuda => .shared_serial,
        };
    }

    const BatchAdmission = struct {
        lease: runtime.tier.memory.AdmissionLease,
        estimate: runtime.tier.memory.Estimate,
    };

    fn acquireBatchAdmission(
        self: *Node,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        run_budget: *runtime.tier.memory.RunBudget,
        estimate: runtime.tier.memory.Estimate,
    ) !BatchAdmission {
        try run_budget.reserveEstimate(estimate);
        errdefer run_budget.releaseEstimate(estimate);
        return .{
            .lease = try self.model_manager.acquireRunResources(
                backend_class,
                limits,
                estimate,
            ),
            .estimate = estimate,
        };
    }

    fn releaseBatchAdmission(
        run_budget: *runtime.tier.memory.RunBudget,
        admission: *?BatchAdmission,
    ) void {
        if (admission.*) |*owned| {
            owned.lease.release();
            run_budget.releaseEstimate(owned.estimate);
            admission.* = null;
        }
    }

    pub fn generateBatchContent(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.GenerateBatchRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.mode) |mode| {
            if (mode != .sync) return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "only mode=sync is supported" });
        }
        if (body.requests.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'requests' must not be empty" });
        }
        if (body.requests.len > max_generate_batch_items) {
            return ctx.status(413).json(.{
                .@"error" = "BATCH_TOO_LARGE",
                .message = try std.fmt.allocPrint(ctx.allocator, "'requests' must contain at most {d} items", .{max_generate_batch_items}),
            });
        }

        var response_arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer response_arena.deinit();
        const response_alloc = response_arena.allocator();

        const results = try response_alloc.alloc(api.GenerateBatchResultItem, body.requests.len);
        var owned_messages = try ctx.allocator.alloc(OwnedGenerateMessages, body.requests.len);
        defer {
            for (owned_messages) |*owned| owned.deinit();
            ctx.allocator.free(owned_messages);
        }
        var pending = try ctx.allocator.alloc(bool, body.requests.len);
        defer ctx.allocator.free(pending);

        for (body.requests, 0..) |item, idx| {
            results[idx] = .{
                .custom_id = item.custom_id,
                .index = @intCast(idx),
            };
            if (generateBatchUnsupportedReasonPreflight(item.body)) |batch_err| {
                results[idx].@"error" = batch_err;
                owned_messages[idx] = .{ .allocator = ctx.allocator };
                pending[idx] = false;
                continue;
            }
            owned_messages[idx] = parseGenerateMessages(self, ctx.allocator, item.body) catch |err| blk: {
                results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = @errorName(err), .retryable = false };
                break :blk .{ .allocator = ctx.allocator };
            };
            if (results[idx].@"error" == null and owned_messages[idx].messages.len == 0) {
                results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "'messages' must not be empty", .retryable = false };
            }
            if (results[idx].@"error" == null) {
                if (generateBatchUnsupportedReason(item.body, owned_messages[idx].messages)) |batch_err| results[idx].@"error" = batch_err;
            }
            pending[idx] = results[idx].@"error" == null;
        }

        const queue_units = self.estimateGenerateBatchQueueUnits(body.requests, owned_messages, pending);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("generate_batch");
        defer self.metrics.decActive();

        while (true) {
            const first_idx = blk: {
                for (pending, 0..) |is_pending, idx| {
                    if (is_pending) break :blk idx;
                }
                break :blk null;
            } orelse break;
            const first_body = body.requests[first_idx].body;
            const model_path = self.resolveModelPath(ctx.io, first_body.model, "generators") catch {
                results[first_idx].@"error" = .{ .code = "MODEL_NOT_FOUND", .message = "model not found", .retryable = false };
                pending[first_idx] = false;
                continue;
            };
            const selection = parseGenerateBackendSelection(first_body.backend, first_body.mode, first_body.compiled_target) catch {
                results[first_idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "unsupported backend", .retryable = false };
                pending[first_idx] = false;
                continue;
            };

            var group_indices = std.ArrayListUnmanaged(usize).empty;
            defer group_indices.deinit(ctx.allocator);
            for (pending, 0..) |is_pending, idx| {
                if (!is_pending) continue;
                const candidate = body.requests[idx].body;
                if (!std.mem.eql(u8, candidate.model, first_body.model)) continue;
                if (candidate.backend != first_body.backend) continue;
                if (!std.mem.eql(u8, candidate.mode orelse "", first_body.mode orelse "")) continue;
                if (!std.mem.eql(u8, candidate.compiled_target orelse "", first_body.compiled_target orelse "")) continue;
                if (!std.mem.eql(u8, candidate.cache_dtype orelse "", first_body.cache_dtype orelse "")) continue;
                try group_indices.append(ctx.allocator, idx);
            }

            const compatibility_summary = self.compatibilitySummaryForDir(ctx.allocator, model_path) catch CompatibilitySummary{
                .level = .unknown,
                .code = .artifact_unreadable,
                .message = "model compatibility could not be determined",
            };
            const compatibility_blocked = compatibility_summary.level == .incompatible or
                (compatibility_summary.level == .unknown and !self.config.allow_unknown_models);
            if (compatibility_blocked) {
                const code = if (compatibility_summary.level == .incompatible)
                    "INCOMPATIBLE_MODEL"
                else
                    "UNKNOWN_MODEL_COMPATIBILITY";
                for (group_indices.items) |idx| {
                    results[idx].@"error" = .{
                        .code = code,
                        .message = compatibility_summary.message,
                        .retryable = false,
                    };
                    pending[idx] = false;
                }
                continue;
            }

            var model_handle = if (selection.native_choice != .auto) blk: {
                var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
                configureGenerateBackendPreference(&request_session_manager, selection);
                break :blk self.model_manager.acquireFromDirWithPreferredBackends(model_path, request_session_manager.preferred_backends, false) catch |err| {
                    for (group_indices.items) |idx| {
                        results[idx].@"error" = batchModelLoadError(err);
                        pending[idx] = false;
                    }
                    continue;
                };
            } else self.model_manager.acquireFromDir(model_path) catch |err| {
                for (group_indices.items) |idx| {
                    results[idx].@"error" = batchModelLoadError(err);
                    pending[idx] = false;
                }
                continue;
            };
            const model = model_handle.get();

            model.lockNativeGeneration();
            {
                defer model_handle.release();
                defer model.unlockNativeGeneration();

                const gpt_config = session_factory.getGptConfig(model.session) orelse {
                    for (group_indices.items) |idx| {
                        results[idx].@"error" = .{ .code = "INVALID_MODEL", .message = "model does not support native generation", .retryable = false };
                        pending[idx] = false;
                    }
                    continue;
                };
                const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
                    .native => .native,
                    .metal => .metal,
                    .cuda => .cuda,
                    else => {
                        for (group_indices.items) |idx| {
                            results[idx].@"error" = .{ .code = "UNSUPPORTED_BACKEND", .message = "batch generation requires a native backend", .retryable = false };
                            pending[idx] = false;
                        }
                        continue;
                    },
                };

                var configs = try ctx.allocator.alloc(generation.GenerationConfig, group_indices.items.len);
                defer ctx.allocator.free(configs);
                var prompt_tokens = try ctx.allocator.alloc(usize, group_indices.items.len);
                defer ctx.allocator.free(prompt_tokens);
                var prompt_bytes = try ctx.allocator.alloc(usize, group_indices.items.len);
                defer ctx.allocator.free(prompt_bytes);
                var valid_count: usize = 0;
                for (group_indices.items, 0..) |idx, pos| {
                    configs[pos] = generateConfigFromBody(ctx.allocator, body.requests[idx].body) catch |err| {
                        results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = @errorName(err), .retryable = false };
                        pending[idx] = false;
                        continue;
                    };
                    prompt_tokens[pos] = self.estimateNativePromptTokens(ctx.allocator, model, owned_messages[idx].messages) catch |err| {
                        results[idx].@"error" = .{ .code = "TOKENIZE_FAILED", .message = @errorName(err), .retryable = false };
                        pending[idx] = false;
                        continue;
                    };
                    prompt_bytes[pos] = self.estimateGeneratePromptBytes(owned_messages[idx].messages);
                    valid_count += 1;
                }
                if (valid_count == 0) continue;

                const kv_dtype = if (first_body.cache_dtype) |name|
                    runtime.kv.pool.parseKvDType(name) orelse {
                        for (group_indices.items) |idx| {
                            results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "invalid cache_dtype value", .retryable = false };
                            pending[idx] = false;
                        }
                        continue;
                    }
                else
                    session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
                const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
                    .native => .cpu,
                    .metal, .cuda => .gpu,
                };
                const budget_limits = self.config.generation_budget_overrides.apply(session_factory.widenBudgetLimitsForSession(
                    model.session,
                    runtime.tier.memory.defaultLimitsForBackend(budget_backend_class),
                ));
                const execution_mode = batchExecutionMode(backend_kind);
                var shared_run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
                const task_run_budgets = try ctx.allocator.alloc(
                    runtime.tier.memory.RunBudget,
                    group_indices.items.len,
                );
                defer ctx.allocator.free(task_run_budgets);
                for (task_run_budgets) |*budget| {
                    budget.* = runtime.tier.memory.RunBudget.init(budget_limits);
                }
                const resource_estimates = try ctx.allocator.alloc(
                    ?runtime.tier.memory.Estimate,
                    group_indices.items.len,
                );
                @memset(resource_estimates, null);
                defer ctx.allocator.free(resource_estimates);
                // Allocate ownership slots before acquiring capacity. Once a
                // process-wide lease is granted, publishing it here cannot fail.
                const admissions = try ctx.allocator.alloc(?BatchAdmission, group_indices.items.len);
                @memset(admissions, null);
                defer {
                    for (admissions, 0..) |*maybe_admission, pos| {
                        const item_run_budget = if (execution_mode == .isolated_parallel)
                            &task_run_budgets[pos]
                        else
                            &shared_run_budget;
                        releaseBatchAdmission(item_run_budget, maybe_admission);
                    }
                    ctx.allocator.free(admissions);
                }
                var runnable_count: usize = 0;
                for (group_indices.items, 0..) |idx, pos| {
                    if (!pending[idx]) continue;
                    const admission_prefill_chunk = if (configs[pos].prefill_chunk_size > 0) configs[pos].prefill_chunk_size else 256;
                    const resource_estimate = runtime.tier.memory.estimateGptGeneration(
                        backend_kind,
                        kv_dtype,
                        gpt_config,
                        prompt_tokens[pos],
                        @intCast(@max(configs[pos].max_tokens, 1)),
                        admission_prefill_chunk,
                    ) catch |err| {
                        results[idx].@"error" = .{
                            .code = if (err == error.InvalidModelConfig) "INVALID_MODEL" else "MODEL_RESOURCE_LIMIT",
                            .message = @errorName(err),
                            .retryable = false,
                        };
                        pending[idx] = false;
                        continue;
                    };
                    resource_estimates[pos] = resource_estimate;
                    if (execution_mode == .shared_serial) {
                        runnable_count += 1;
                        continue;
                    }
                    admissions[pos] = self.acquireBatchAdmission(
                        budget_backend_class,
                        budget_limits,
                        &task_run_budgets[pos],
                        resource_estimate,
                    ) catch |err| {
                        results[idx].@"error" = batchAdmissionError(err);
                        pending[idx] = false;
                        continue;
                    };
                    runnable_count += 1;
                }
                if (runnable_count == 0) continue;

                var shared_cb: ?ops.ComputeBackend = null;
                if (execution_mode == .shared_serial) {
                    var provision_admitted = false;
                    for (group_indices.items, 0..) |idx, pos| {
                        if (!pending[idx]) continue;
                        admissions[pos] = self.acquireBatchAdmission(
                            budget_backend_class,
                            budget_limits,
                            &shared_run_budget,
                            resource_estimates[pos].?,
                        ) catch |err| {
                            results[idx].@"error" = batchAdmissionError(err);
                            pending[idx] = false;
                            continue;
                        };
                        provision_admitted = true;
                        break;
                    }
                    if (!provision_admitted) continue;
                    shared_cb = session_factory.getComputeBackendWithBudget(
                        model.session,
                        ctx.allocator,
                        &shared_run_budget,
                    ) catch |err| {
                        for (group_indices.items) |idx| {
                            if (!pending[idx]) continue;
                            results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                        }
                        continue;
                    };
                }
                defer if (shared_cb) |*cb| cb.deinit();

                var kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
                defer kv_manager.deinit();
                const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
                    null
                else if (gpt_config.sliding_window > 0)
                    gpt_config.sliding_window
                else if (gpt_config.max_position_embeddings > 0)
                    gpt_config.max_position_embeddings
                else
                    null;
                const pool_id = kv_manager.addPool(.{
                    .backend = backend_kind,
                    .dtype = kv_dtype,
                    .page_size_tokens = 16,
                    .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
                    .num_kv_heads = gpt_config.maxKvHeads(),
                    .head_dim = gpt_config.maxHeadDim(),
                    .sliding_window_size = sliding_window_size,
                }) catch |err| {
                    for (group_indices.items) |idx| {
                        if (!pending[idx]) continue;
                        results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                        pending[idx] = false;
                    }
                    continue;
                };
                var kv_storage = runtime.kv.storage_runtime.KvStorageRuntime.init(ctx.allocator, .{
                    .backend = backend_kind,
                    .dtype = kv_dtype,
                    .page_size_tokens = 16,
                    .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
                    .num_kv_heads = gpt_config.maxKvHeads(),
                    .head_dim = gpt_config.maxHeadDim(),
                    .sliding_window_size = sliding_window_size,
                }) catch |err| {
                    for (group_indices.items) |idx| {
                        if (!pending[idx]) continue;
                        results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                        pending[idx] = false;
                    }
                    continue;
                };
                defer kv_storage.deinit();
                if (shared_cb) |*cb| {
                    cb.provisionKvDeviceWriteHook(&kv_storage) catch |err| {
                        for (group_indices.items) |idx| {
                            if (!pending[idx]) continue;
                            results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                        }
                        continue;
                    };
                }

                var kv_mutex: std.atomic.Mutex = .unlocked;
                var task_arenas = try ctx.allocator.alloc(std.heap.ArenaAllocator, group_indices.items.len);
                for (task_arenas) |*arena| arena.* = std.heap.ArenaAllocator.init(ctx.allocator);
                defer {
                    for (task_arenas) |*arena| arena.deinit();
                    ctx.allocator.free(task_arenas);
                }
                const task_cbs = try ctx.allocator.alloc(?ops.ComputeBackend, group_indices.items.len);
                @memset(task_cbs, null);
                defer {
                    for (task_cbs) |*maybe_cb| {
                        if (maybe_cb.*) |*cb| cb.deinit();
                    }
                    ctx.allocator.free(task_cbs);
                }
                const decode_states = try ctx.allocator.alloc(generation.NativeDecodeState, group_indices.items.len);
                for (decode_states) |*state| state.* = generation.NativeDecodeState.initContiguous(ctx.allocator);
                defer {
                    for (decode_states) |*state| state.deinit();
                    ctx.allocator.free(decode_states);
                }
                var leases = try ctx.allocator.alloc(runtime.scheduler.native_generate.Lease, group_indices.items.len);
                for (leases) |*lease| lease.* = .{ .request_id = 0, .reserved_units = 0, .prompt_bytes = 0, .max_tokens = 0, .prefill_chunk_size = 0, .active_requests_snapshot = 0 };
                defer {
                    if (model.native_generate_coordinator) |coordinator| {
                        for (leases) |*lease| {
                            if (lease.request_id != 0) {
                                coordinator.release(lease.*);
                                lease.request_id = 0;
                            }
                        }
                    }
                    ctx.allocator.free(leases);
                }
                var task_results = try ctx.allocator.alloc(BatchGenerateTaskResult, group_indices.items.len);
                for (task_results) |*task_result| task_result.* = .{};
                defer ctx.allocator.free(task_results);
                var task_ran = try ctx.allocator.alloc(bool, group_indices.items.len);
                @memset(task_ran, false);
                defer ctx.allocator.free(task_ran);
                var tasks = try ctx.allocator.alloc(BatchGenerateTask, group_indices.items.len);
                defer ctx.allocator.free(tasks);

                var spawned_any = false;
                var group = std.Io.Group.init;
                for (group_indices.items, 0..) |idx, pos| {
                    if (!pending[idx]) continue;
                    const task_alloc = task_arenas[pos].allocator();
                    if (execution_mode == .shared_serial and admissions[pos] == null) {
                        const resource_estimate = resource_estimates[pos].?;
                        admissions[pos] = self.acquireBatchAdmission(
                            budget_backend_class,
                            budget_limits,
                            &shared_run_budget,
                            resource_estimate,
                        ) catch |err| {
                            results[idx].@"error" = batchAdmissionError(err);
                            pending[idx] = false;
                            continue;
                        };
                    }
                    if (execution_mode == .isolated_parallel) {
                        task_cbs[pos] = session_factory.getComputeBackendWithBudget(
                            model.session,
                            task_alloc,
                            &task_run_budgets[pos],
                        ) catch |err| {
                            releaseBatchAdmission(&task_run_budgets[pos], &admissions[pos]);
                            results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                            continue;
                        };
                    }
                    const task_cb = if (execution_mode == .isolated_parallel)
                        task_cbs[pos].?
                    else
                        shared_cb.?;
                    const queue_item_units = self.estimateGenerateQueueUnits(owned_messages[idx].messages, configs[pos].max_tokens);
                    decode_states[pos] = generation.NativeDecodeState.initPaged(task_alloc, &kv_manager, pool_id, model.shared_moe_cache);
                    decode_states[pos].kv_lock = &kv_mutex;
                    decode_states[pos].kv_storage = &kv_storage;
                    leases[pos] = if (model.native_generate_coordinator) |coordinator| blk: {
                        const lease = coordinator.acquire(.{
                            .requested_units = queue_item_units,
                            .prompt_bytes = prompt_bytes[pos],
                            .max_tokens = configs[pos].max_tokens,
                        }) catch |err| {
                            const item_run_budget = if (execution_mode == .isolated_parallel)
                                &task_run_budgets[pos]
                            else
                                &shared_run_budget;
                            releaseBatchAdmission(item_run_budget, &admissions[pos]);
                            results[idx].@"error" = .{ .code = "QUEUE_FULL", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                            continue;
                        };
                        configs[pos].prefill_chunk_size = lease.prefill_chunk_size;
                        break :blk lease;
                    } else .{ .request_id = 0, .reserved_units = 0, .prompt_bytes = 0, .max_tokens = 0, .prefill_chunk_size = 256, .active_requests_snapshot = 0 };
                    tasks[pos] = .{
                        .allocator = task_alloc,
                        .pipeline = .{
                            .allocator = task_alloc,
                            .io = ctx.io,
                            .cb = task_cb,
                            .session = model.session,
                            .gpt_config = gpt_config,
                            .kv_dtype = kv_dtype,
                            .shared_moe_cache = model.shared_moe_cache,
                            .tokenizer = model.getTokenizer(),
                            .add_bos_token = model.manifest.add_bos_token,
                            .bos_token = model.manifest.bos_token,
                            .chat_template = model.chat_tmpl,
                            .print_timing = serverGenerateTimingEnabled(),
                            .model_dir = model_path,
                            .gguf_projector_path = model.manifest.gguf_projector_path,
                            .decode_state = &decode_states[pos],
                            .scheduler = model.native_generate_coordinator,
                            .scheduler_lease = if (model.native_generate_coordinator != null) &leases[pos] else null,
                        },
                        .messages = owned_messages[idx].messages,
                        .config = configs[pos],
                        .response_format = body.requests[idx].body.response_format,
                        .out = &task_results[pos],
                    };
                    task_ran[pos] = true;
                    if (execution_mode == .shared_serial) {
                        tasks[pos].run() catch {};
                        if (model.native_generate_coordinator) |coordinator| {
                            if (leases[pos].request_id != 0) {
                                coordinator.release(leases[pos]);
                                leases[pos].request_id = 0;
                            }
                        }
                        releaseBatchAdmission(&shared_run_budget, &admissions[pos]);
                        decode_states[pos].deinit();
                        decode_states[pos] = generation.NativeDecodeState.initContiguous(ctx.allocator);
                        try applyBatchGenerateTaskResult(
                            response_alloc,
                            body.requests[idx].body.model,
                            task_results[pos],
                            &results[idx],
                        );
                        pending[idx] = false;
                        task_arenas[pos].deinit();
                        task_arenas[pos] = std.heap.ArenaAllocator.init(ctx.allocator);
                        continue;
                    }
                    group.concurrent(ctx.io, BatchGenerateTask.run, .{&tasks[pos]}) catch {
                        tasks[pos].run() catch {};
                        continue;
                    };
                    spawned_any = true;
                }
                if (spawned_any) group.await(ctx.io) catch {};
                for (group_indices.items, 0..) |idx, pos| {
                    if (!pending[idx]) continue;
                    if (!task_ran[pos]) {
                        pending[idx] = false;
                        continue;
                    }
                    try applyBatchGenerateTaskResult(
                        response_alloc,
                        body.requests[idx].body.model,
                        task_results[pos],
                        &results[idx],
                    );
                    pending[idx] = false;
                }
            }
        }

        var succeeded: i64 = 0;
        for (results) |item| {
            if (item.response != null and item.@"error" == null) succeeded += 1;
        }
        const total: i64 = @intCast(results.len);
        return ctx.json(api.GenerateBatchResponse{
            .object = "generate.batch",
            .data = results,
            .summary = .{ .total = total, .succeeded = succeeded, .failed = total - succeeded },
        });
    }

    pub fn chatCompletions(self: *Node, ctx: *httpx.Context) !httpx.Response {
        return self.generateContent(ctx);
    }

    const ToolStopCtx = struct {
        parser: *tool_parser_mod.Parser,
        errored: ?anyerror = null,

        fn onToken(raw_ctx: *anyopaque, token_text: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            _ = self.parser.feed(token_text) catch |err| {
                self.errored = err;
                return false;
            };
            return self.parser.toolCalls().len == 0;
        }
    };

    fn generateMaybeStopOnTool(
        pipeline: anytype,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
        tool_parser: ?*tool_parser_mod.Parser,
    ) !generation.GenerationResult {
        var tool_stop_ctx: ?ToolStopCtx = if (tool_parser) |parser| blk: {
            parser.reset();
            break :blk .{ .parser = parser };
        } else null;

        var result = if (tool_stop_ctx) |*stop_ctx|
            try pipeline.generateStreaming(messages, config, @ptrCast(stop_ctx), ToolStopCtx.onToken)
        else
            try pipeline.generate(messages, config);
        errdefer result.deinit();

        if (tool_stop_ctx) |stop_ctx| {
            if (stop_ctx.errored) |err| return err;
        }

        return result;
    }

    /// Send a completed GenerationResult as a single SSE stream.
    fn sendResultAsSSE(
        _: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        result: *@import("../pipelines/generation.zig").GenerationResult,
        tool_parser: ?*tool_parser_mod.Parser,
    ) !httpx.Response {
        var writer = ctx.streamResponse(200) catch |err| {
            std.debug.print("streamResponse failed: {}\n", .{err});
            return ctx.status(500).json(.{ .@"error" = "STREAM_INIT_FAILED", .message = @errorName(err) });
        };
        const stream_id = try allocCompletionId(ctx.allocator);
        defer ctx.allocator.free(stream_id);
        const stream_created = completionCreatedTimestamp();

        emitRoleDelta(&writer, ctx.allocator, stream_id, stream_created, model_name) catch |err| {
            writer.writeEvent("error", @errorName(err)) catch {};
            writer.close() catch {};
            return ctx.response.build();
        };

        writeStreamCompletion(
            ctx.allocator,
            &writer,
            stream_id,
            stream_created,
            model_name,
            result.text,
            result.finish_reason,
            tool_parser,
        ) catch |err| {
            writer.writeEvent("error", @errorName(err)) catch {};
            writer.close() catch {};
            return ctx.response.build();
        };
        writer.writeEvent(null, "[DONE]") catch {};
        writer.close() catch {};
        return ctx.response.build();
    }

    const ParsedMultimodalRerankDocument = struct {
        allocator: std.mem.Allocator,
        text: []u8,
        images: [][]const u8,

        fn deinit(self: *ParsedMultimodalRerankDocument) void {
            self.allocator.free(self.text);
            for (self.images) |img| self.allocator.free(img);
            self.allocator.free(self.images);
        }
    };

    fn writeRerankScoresResponse(
        ctx: *httpx.Context,
        model_name: []const u8,
        scores: []const f32,
        prompt_tokens: usize,
    ) !httpx.Response {
        const data = try ctx.allocator.alloc(api.RerankObject, scores.len);
        defer ctx.allocator.free(data);
        for (scores, 0..) |score, i| {
            data[i] = .{
                .object = "rerank.score",
                .index = @intCast(i),
                .score = score,
            };
        }
        return ctx.json(api.RerankResponse{
            .object = "list",
            .data = data,
            .model = model_name,
            .usage = tokenUsage(prompt_tokens, 0),
        });
    }

    fn parseChatMessageContentToTextAndImages(self: *Node, allocator: std.mem.Allocator, content: api.ChatMessageContent) !ParsedMultimodalRerankDocument {
        var text_buf = std.ArrayListUnmanaged(u8).empty;
        errdefer text_buf.deinit(allocator);
        var images = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (images.items) |img| allocator.free(img);
            images.deinit(allocator);
        }

        switch (content) {
            .string => |s| try text_buf.appendSlice(allocator, s),
            .array => |arr| {
                for (arr.items) |part| {
                    if (part != .object) return error.UnsupportedContentPartType;
                    const obj = part.object;
                    const type_val = obj.get("type") orelse return error.UnsupportedContentPartType;
                    if (type_val != .string) return error.UnsupportedContentPartType;
                    const ptype = type_val.string;

                    if (std.mem.eql(u8, ptype, "text")) {
                        const text_val = obj.get("text") orelse return error.UnsupportedContentPartType;
                        if (text_val != .string) return error.UnsupportedContentPartType;
                        try text_buf.appendSlice(allocator, text_val.string);
                    } else if (std.mem.eql(u8, ptype, "image_url")) {
                        const iu = obj.get("image_url") orelse return error.UnsupportedContentPartType;
                        const url_str = if (iu == .object)
                            if (iu.object.get("url")) |u| (if (u == .string) u.string else null) else null
                        else if (iu == .string)
                            iu.string
                        else
                            null;
                        const url = url_str orelse return error.UnsupportedContentPartType;
                        if (std.mem.startsWith(u8, url, "data:")) {
                            const decoded = decodeDataUri(allocator, url) catch return error.InvalidImageDataUri;
                            try images.append(allocator, decoded.data);
                        } else {
                            var downloaded = downloadRemoteContent(self, allocator, url) catch return error.ImageDownloadFailed;
                            defer downloaded.deinit(allocator);
                            try images.append(allocator, try allocator.dupe(u8, downloaded.data));
                        }
                    } else if (std.mem.eql(u8, ptype, "media")) {
                        const data_val = obj.get("data") orelse return error.UnsupportedContentPartType;
                        const mime_val = obj.get("mime_type") orelse return error.UnsupportedContentPartType;
                        if (data_val != .string or mime_val != .string) return error.UnsupportedContentPartType;
                        if (!std.mem.startsWith(u8, mime_val.string, "image/")) return error.UnsupportedContentPartType;
                        const decoded_payload = decodeMediaData(allocator, data_val.string) catch return error.UnsupportedContentPartType;
                        const decoded = decoded_payload.data;
                        errdefer allocator.free(decoded);
                        if (!mediaMimeMatches(mime_val.string, decoded_payload.mime_type)) return error.UnsupportedContentPartType;
                        try images.append(allocator, decoded);
                    } else {
                        return error.UnsupportedContentPartType;
                    }
                }
            },
            else => return error.UnsupportedContentPartType,
        }

        return .{
            .allocator = allocator,
            .text = try text_buf.toOwnedSlice(allocator),
            .images = try images.toOwnedSlice(allocator),
        };
    }

    fn buildGenerateResponse(
        _: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        response_text: []const u8,
        finish_reason: []const u8,
        prompt_tokens: usize,
        completion_tokens: usize,
        cached_prompt_tokens: usize,
        tool_calls: ?[]const tool_parser_mod.ToolCall,
    ) !httpx.Response {
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const completion_id = try allocCompletionId(alloc);
        const created = completionCreatedTimestamp();

        var message: api.GenerateMessage = .{ .role = .assistant };
        if (tool_calls) |calls| {
            if (calls.len == 0) {
                message.content = response_text;
            } else {
                const api_calls = try alloc.alloc(generating_api.ToolCall, calls.len);
                for (calls, 0..) |call, i| {
                    api_calls[i] = .{
                        .id = call.id,
                        .type = call.type,
                        .function = .{ .name = call.function.name, .arguments = call.function.arguments },
                    };
                }
                message.tool_calls = api_calls;
            }
        } else {
            message.content = response_text;
        }

        const choices = [_]api.GenerateChoice{.{
            .index = 0,
            .message = message,
            .finish_reason = parseFinishReason(finish_reason),
        }};
        return ctx.json(api.GenerateResponse{
            .id = completion_id,
            .object = "chat.completion",
            .created = created,
            .model = model_name,
            .choices = &choices,
            .usage = .{
                .prompt_tokens = @intCast(prompt_tokens),
                .completion_tokens = @intCast(completion_tokens),
                .total_tokens = @intCast(prompt_tokens + completion_tokens),
                .cached_prompt_tokens = if (cached_prompt_tokens == 0) null else @intCast(cached_prompt_tokens),
            },
        });
    }

    fn prependSystemPrompt(
        allocator: std.mem.Allocator,
        messages: *std.ArrayListUnmanaged(generation.Message),
        prompt: []const u8,
    ) !void {
        if (messages.items.len > 0 and std.mem.eql(u8, messages.items[0].role, "system")) {
            const merged = try std.fmt.allocPrint(allocator, "{s}\n\n{s}", .{ prompt, messages.items[0].content });
            allocator.free(messages.items[0].content);
            messages.items[0].content = merged;
            return;
        }

        try messages.insert(allocator, 0, .{
            .role = "system",
            .content = try allocator.dupe(u8, prompt),
            .image_bytes = null,
        });
    }

    fn buildFunctionGemmaPrompt(
        allocator: std.mem.Allocator,
        bos_token: []const u8,
        messages: []const generation.Message,
    ) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(allocator);

        if (bos_token.len > 0) try buf.appendSlice(allocator, bos_token);
        for (messages) |message| {
            const role = if (std.mem.eql(u8, message.role, "assistant"))
                "model"
            else if (std.mem.eql(u8, message.role, "system"))
                "developer"
            else
                message.role;
            try buf.appendSlice(allocator, "<start_of_turn>");
            try buf.appendSlice(allocator, role);
            try buf.append(allocator, '\n');
            try buf.appendSlice(allocator, message.content);
            try buf.appendSlice(allocator, "<end_of_turn>\n");
        }
        try buf.appendSlice(allocator, "<start_of_turn>model\n");
        return try buf.toOwnedSlice(allocator);
    }

    /// SSE streaming generation: sends token-by-token events via chunked transfer encoding.
    /// OpenAI-compatible format: data: {"choices":[{"delta":{"content":"token"}}]}\n\n
    fn streamGenerate(
        _: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        pipeline: anytype,
        messages: []const @import("../pipelines/generation.zig").Message,
        config: @import("../pipelines/generation.zig").GenerationConfig,
        tool_parser: ?*tool_parser_mod.Parser,
    ) !httpx.Response {
        var writer = ctx.streamResponse(200) catch |err| {
            std.debug.print("streamResponse failed: {}\n", .{err});
            return ctx.status(500).json(.{ .@"error" = "STREAM_INIT_FAILED", .message = @errorName(err) });
        };
        const stream_id = try allocCompletionId(ctx.allocator);
        defer ctx.allocator.free(stream_id);
        const stream_created = completionCreatedTimestamp();

        // Context for the token callback — carries the writer and model name for building SSE events
        const StreamCtx = struct {
            writer: *httpx.Context.StreamWriter,
            stream_id: []const u8,
            stream_created: i64,
            model_name: []const u8,
            allocator: std.mem.Allocator,
            parser: ?*tool_parser_mod.Parser,
            errored: bool = false,

            fn onToken(raw_ctx: *anyopaque, token_text: []const u8) bool {
                const self: *@This() = @ptrCast(@alignCast(raw_ctx));
                if (self.parser) |parser| {
                    const update = parser.feed(token_text) catch {
                        self.errored = true;
                        return false;
                    };
                    if (update.ready_text.len > 0) {
                        emitContentDelta(self.writer, self.allocator, self.stream_id, self.stream_created, self.model_name, update.ready_text) catch {
                            self.errored = true;
                            return false;
                        };
                    }
                    if (!parser.streamsIncrementalToolDeltas() and update.new_calls.len > 0) {
                        for (update.new_calls, 0..) |call, idx| {
                            emitToolCallDelta(self.writer, self.allocator, self.stream_id, self.stream_created, self.model_name, update.call_start_index + idx, call) catch {
                                self.errored = true;
                                return false;
                            };
                        }
                    }
                    if (update.active_tool_delta) |delta| {
                        emitToolCallDeltaUpdate(self.writer, self.allocator, self.stream_id, self.stream_created, self.model_name, delta) catch {
                            self.errored = true;
                            return false;
                        };
                    }
                    return true;
                }
                // Build OpenAI-compatible SSE chunk
                emitContentDelta(self.writer, self.allocator, self.stream_id, self.stream_created, self.model_name, token_text) catch {
                    self.errored = true;
                    return false;
                };
                return true;
            }
        };

        var stream_ctx = StreamCtx{
            .writer = &writer,
            .stream_id = stream_id,
            .stream_created = stream_created,
            .model_name = model_name,
            .allocator = ctx.allocator,
            .parser = tool_parser,
        };

        emitRoleDelta(&writer, ctx.allocator, stream_id, stream_created, model_name) catch |err| {
            writer.writeEvent("error", @errorName(err)) catch {};
            writer.close() catch {};
            return ctx.response.build();
        };

        var result = pipeline.generateStreaming(
            messages,
            config,
            @ptrCast(&stream_ctx),
            StreamCtx.onToken,
        ) catch |err| {
            // Try to send an error event before closing
            writer.writeEvent("error", @errorName(err)) catch {};
            writer.close() catch {};
            return ctx.response.build();
        };
        defer result.deinit();

        if (stream_ctx.errored) {
            writer.writeEvent("error", "STREAM_WRITE_FAILED") catch {};
            writer.close() catch {};
            return ctx.response.build();
        }

        if (tool_parser != null) {
            flushStreamParserState(ctx.allocator, &writer, stream_id, stream_created, model_name, result.finish_reason, tool_parser.?) catch |err| {
                writer.writeEvent("error", @errorName(err)) catch {};
                writer.close() catch {};
                return ctx.response.build();
            };
        } else {
            emitFinishDelta(&writer, ctx.allocator, stream_id, stream_created, model_name, result.finish_reason) catch {};
        }

        // Send the final [DONE] event (OpenAI convention)
        writer.writeEvent(null, "[DONE]") catch {};
        writer.close() catch {};

        return ctx.response.build();
    }

    fn writeStreamCompletion(
        allocator: std.mem.Allocator,
        writer: *httpx.Context.StreamWriter,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        full_text: []const u8,
        default_finish_reason: []const u8,
        tool_parser: ?*tool_parser_mod.Parser,
    ) !void {
        if (tool_parser) |parser| {
            parser.reset();
            _ = try parser.feed(full_text);
            const remaining = try parser.finishText(allocator);
            defer allocator.free(remaining);
            const calls = parser.toolCalls();
            if (remaining.len > 0) try emitContentDelta(writer, allocator, stream_id, stream_created, model_name, remaining);
            if (calls.len > 0) {
                for (calls, 0..) |call, idx| try emitToolCallDeltaUpdate(writer, allocator, stream_id, stream_created, model_name, .{
                    .index = idx,
                    .id = call.id,
                    .type = call.type,
                    .name = call.function.name,
                    .arguments = call.function.arguments,
                });
                try emitFinishDelta(writer, allocator, stream_id, stream_created, model_name, "tool_calls");
                return;
            }
            if (remaining.len == 0 and full_text.len > 0) try emitContentDelta(writer, allocator, stream_id, stream_created, model_name, full_text);
            try emitFinishDelta(writer, allocator, stream_id, stream_created, model_name, default_finish_reason);
            return;
        }

        if (full_text.len > 0) try emitContentDelta(writer, allocator, stream_id, stream_created, model_name, full_text);
        try emitFinishDelta(writer, allocator, stream_id, stream_created, model_name, default_finish_reason);
    }

    fn flushStreamParserState(
        allocator: std.mem.Allocator,
        writer: *httpx.Context.StreamWriter,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        default_finish_reason: []const u8,
        parser: *tool_parser_mod.Parser,
    ) !void {
        const remaining = try parser.finishRemainingText(allocator);
        defer allocator.free(remaining);
        if (remaining.len > 0) try emitContentDelta(writer, allocator, stream_id, stream_created, model_name, remaining);
        const finish_reason = if (parser.toolCalls().len > 0) "tool_calls" else default_finish_reason;
        try emitFinishDelta(writer, allocator, stream_id, stream_created, model_name, finish_reason);
    }

    fn parseFinishReason(s: []const u8) api.FinishReason {
        const map = std.StaticStringMap(api.FinishReason).initComptime(.{
            .{ "stop", .stop },
            .{ "length", .length },
            .{ "tool_calls", .tool_calls },
            .{ "content_filter", .content_filter },
            .{ "function_call", .function_call },
        });
        return map.get(s) orelse .stop;
    }

    fn writeGenerateChunkEvent(
        writer: *httpx.Context.StreamWriter,
        allocator: std.mem.Allocator,
        chunk: api.GenerateChunk,
    ) !void {
        const payload = try std.json.Stringify.valueAlloc(allocator, chunk, .{});
        defer allocator.free(payload);
        try writer.writeEvent(null, payload);
    }

    fn emitRoleDelta(
        writer: *httpx.Context.StreamWriter,
        allocator: std.mem.Allocator,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .role = .assistant },
        }};
        try writeGenerateChunkEvent(writer, allocator, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitContentDelta(
        writer: *httpx.Context.StreamWriter,
        allocator: std.mem.Allocator,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        token_text: []const u8,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .content = token_text },
        }};
        try writeGenerateChunkEvent(writer, allocator, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitToolCallDeltaUpdate(
        writer: *httpx.Context.StreamWriter,
        allocator: std.mem.Allocator,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        delta: tool_parser_mod.ToolCallDeltaUpdate,
    ) !void {
        const function_delta: ?api.ToolCallFunctionDelta = if (delta.name != null or delta.arguments != null)
            .{ .name = delta.name, .arguments = delta.arguments }
        else
            null;
        const tool_calls = [_]api.ToolCallDelta{.{
            .index = @intCast(delta.index),
            .id = delta.id,
            .type = delta.type,
            .function = function_delta,
        }};
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .tool_calls = &tool_calls },
        }};
        try writeGenerateChunkEvent(writer, allocator, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitToolCallDelta(
        writer: *httpx.Context.StreamWriter,
        allocator: std.mem.Allocator,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        index: usize,
        call: tool_parser_mod.ToolCall,
    ) !void {
        try emitToolCallDeltaUpdate(writer, allocator, stream_id, stream_created, model_name, .{
            .index = index,
            .id = call.id,
            .type = call.type,
            .name = call.function.name,
            .arguments = call.function.arguments,
        });
    }

    fn emitFinishDelta(
        writer: *httpx.Context.StreamWriter,
        allocator: std.mem.Allocator,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        finish_reason: []const u8,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{},
            .finish_reason = parseFinishReason(finish_reason),
        }};
        try writeGenerateChunkEvent(writer, allocator, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    const EntityExtractionRequest = struct {
        model: []const u8,
        labels: ?[]const []const u8 = null,
        relation_labels: ?[]const []const u8 = null,
        resolver: ?extraction_api.ExtractionResolverOptions = null,
        threshold: ?f32 = null,
        flat_ner: ?bool = null,
        include_confidence: bool = false,
        include_spans: bool = false,
        input_ids: []const ?[]const u8,
    };

    fn extractEntitiesAndRelations(
        self: *Node,
        ctx: *httpx.Context,
        body: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
    ) !httpx.Response {
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "extractors") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        if (rebel_mod.isRebelModel(ctx.allocator, model_path)) {
            return self.extractRebel(ctx, model_path, body, texts, want_relations);
        }

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        // Use GLiNER pipeline for GLiNER models, standard NER for BIO models
        if (model.isGlinerModel()) {
            return self.extractGliner(ctx, model, body, texts, want_relations);
        }

        if (want_relations) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model does not support relation extraction" });
        }

        var pipeline = model.nerPipeline(ctx.allocator);
        pipeline.config.threshold = body.threshold orelse pipeline.config.threshold;
        const all_entities = pipeline.recognizeBatch(texts) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer {
            for (all_entities) |entities| {
                for (entities) |e| ctx.allocator.free(e.text);
                ctx.allocator.free(entities);
            }
            ctx.allocator.free(all_entities);
        }

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, try model.getCleanupHead(), texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveExtractedOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
        }

        return self.buildEntityExtractionResponse(ctx, body, entities_for_response, null, texts);
    }

    fn extractRebel(
        self: *Node,
        ctx: *httpx.Context,
        model_path: []const u8,
        body: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
    ) !httpx.Response {
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
        const hf_tokenizer = @import("inference_hf_tokenizer");

        const paths = enc_dec_mod.findEncoderDecoderPaths(ctx.allocator, model_path) catch |err|
            return ctx.status(400).json(.{ .@"error" = "INVALID_MODEL", .message = @errorName(err) });
        defer ctx.allocator.free(paths.encoder);
        defer ctx.allocator.free(paths.decoder);

        const tok_path = std.fmt.allocPrint(ctx.allocator, "{s}/tokenizer.json", .{model_path}) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
        defer ctx.allocator.free(tok_path);

        const tok_bytes = c_file.readFile(ctx.allocator, tok_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
        defer ctx.allocator.free(tok_bytes);

        var hf_tok = hf_tokenizer.HfTokenizer.loadFromBytes(ctx.allocator, tok_bytes) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
        defer hf_tok.deinitSelf();

        var config = rebel_mod.loadConfig(ctx.allocator, model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);

        const dec_config = enc_dec_mod.loadDecoderConfig(ctx.allocator, model_path) catch enc_dec_mod.DecoderConfig{};
        if (dec_config.max_length > 0) config.max_length = dec_config.max_length;

        var component_loader = self.model_manager.componentLoaderForPaths(
            model_path,
            self.session_manager.preferred_backends,
            &.{ paths.encoder, paths.decoder },
        ) catch |err| return modelLoadFailureResponse(ctx, err);
        var encoder_managed = component_loader.load(paths.encoder) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer encoder_managed.deinit();
        var strict_loader = component_loader.restrictToBackend(encoder_managed.session.backend()) catch |err|
            return modelLoadFailureResponse(ctx, err);
        var decoder_managed = strict_loader.load(paths.decoder) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer decoder_managed.deinit();
        const encoder_session = encoder_managed.disownSession();
        const decoder_session = decoder_managed.disownSession();

        var pipeline = rebel_mod.RebelPipeline{
            .allocator = ctx.allocator,
            .enc_dec = .{
                .allocator = ctx.allocator,
                .encoder = encoder_session,
                .decoder = decoder_session,
                .config = dec_config,
            },
            .tokenizer = hf_tok.tokenizer(),
            .config = config,
        };
        defer pipeline.deinit();

        if (want_relations) {
            const relation_labels = body.relation_labels.?;
            const extracted = pipeline.extractRelationsBatch(texts, body.labels, relation_labels) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer {
                for (extracted.entities) |entities| {
                    for (entities) |entity| ctx.allocator.free(entity.text);
                    ctx.allocator.free(entities);
                }
                ctx.allocator.free(extracted.entities);

                for (extracted.relations) |relations| {
                    for (relations) |*relation| relation.deinit(ctx.allocator);
                    ctx.allocator.free(relations);
                }
                ctx.allocator.free(extracted.relations);
            }

            if (body.resolver) |resolver_cfg| {
                var resolved = try resolveExtractedOutput(ctx.allocator, extracted.entities, extracted.relations, resolver_cfg);
                defer resolved.deinit(ctx.allocator);
                return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
            }

            return self.buildEntityExtractionResponse(ctx, body, extracted.entities, extracted.relations, texts);
        }

        const all_entities = pipeline.recognizeBatch(texts) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer {
            for (all_entities) |entities| {
                for (entities) |entity| ctx.allocator.free(entity.text);
                ctx.allocator.free(entities);
            }
            ctx.allocator.free(all_entities);
        }

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, null, texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveExtractedOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
        }

        return self.buildEntityExtractionResponse(ctx, body, entities_for_response, null, texts);
    }

    fn extractGliner(
        self: *Node,
        ctx: *httpx.Context,
        model: *model_manager_mod.LoadedModel,
        body: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
    ) !httpx.Response {
        var pipeline = model.glinerPipeline(ctx.allocator);
        pipeline.config.threshold = body.threshold orelse pipeline.config.threshold;
        pipeline.config.flat_ner = body.flat_ner orelse pipeline.config.flat_ner;

        // Parse labels from request body (or use defaults)
        const labels: ?[]const []const u8 = if (body.labels) |lbls| blk: {
            if (lbls.len > 0) {
                break :blk lbls;
            }
            break :blk null;
        } else null;

        if (want_relations) {
            if (!model.supportsRelationExtraction() or !pipeline.supportsRelationExtraction()) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model does not support relation extraction" });
            }

            const relation_labels = body.relation_labels.?;
            const extracted = pipeline.extractRelationsBatch(texts, labels, relation_labels) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer {
                for (extracted.entities) |entities| {
                    for (entities) |e| ctx.allocator.free(e.text);
                    ctx.allocator.free(entities);
                }
                ctx.allocator.free(extracted.entities);

                for (extracted.relations) |relations| {
                    for (relations) |*relation| relation.deinit(ctx.allocator);
                    ctx.allocator.free(relations);
                }
                ctx.allocator.free(extracted.relations);
            }

            if (body.resolver) |resolver_cfg| {
                var resolved = try resolveExtractedOutput(ctx.allocator, extracted.entities, extracted.relations, resolver_cfg);
                defer resolved.deinit(ctx.allocator);
                return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
            }

            return self.buildEntityExtractionResponse(ctx, body, extracted.entities, extracted.relations, texts);
        }

        const all_entities = pipeline.recognizeBatch(texts, labels) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer {
            for (all_entities) |entities| {
                for (entities) |e| ctx.allocator.free(e.text);
                ctx.allocator.free(entities);
            }
            ctx.allocator.free(all_entities);
        }

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, try model.getCleanupHead(), texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveExtractedOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
        }

        return self.buildEntityExtractionResponse(ctx, body, entities_for_response, null, texts);
    }

    fn buildEntityExtractionResponse(
        _: *Node,
        ctx: *httpx.Context,
        request: EntityExtractionRequest,
        all_entities: []const []const @import("../pipelines/ner.zig").Entity,
        all_relations: ?[]const []const gliner_mod.Relation,
        input_texts: []const []const u8,
    ) !httpx.Response {
        if (all_entities.len != input_texts.len or (all_relations != null and all_relations.?.len != input_texts.len)) {
            return ctx.status(500).json(.{
                .@"error" = "INVALID_EXTRACTION_RESULT",
                .message = "model returned a different number of result batches than inputs",
            });
        }
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const data = try alloc.alloc(extraction_api.ExtractionObject, all_entities.len);
        for (all_entities, 0..) |entities, ti| {
            const entity_objects = try alloc.alloc(extraction_api.ExtractionEntity, entities.len);
            for (entities, 0..) |e, ei| entity_objects[ei] = toExtractionApiEntity(e, request);

            const relations_inner: ?[]const extraction_api.ExtractionRelation = if (all_relations) |rels_by_text| blk: {
                const relations = if (ti < rels_by_text.len) rels_by_text[ti] else &.{};
                const relation_objects = try alloc.alloc(extraction_api.ExtractionRelation, relations.len);
                for (relations, 0..) |r, ri| {
                    const source_index = findEntityIndex(entities, r.head) orelse return ctx.status(500).json(.{
                        .@"error" = "INVALID_EXTRACTION_RESULT",
                        .message = "relation source is missing from extracted entities",
                    });
                    const target_index = findEntityIndex(entities, r.tail) orelse return ctx.status(500).json(.{
                        .@"error" = "INVALID_EXTRACTION_RESULT",
                        .message = "relation target is missing from extracted entities",
                    });
                    relation_objects[ri] = .{
                        .type = r.label,
                        .source = .{ .entity_index = @intCast(source_index) },
                        .target = .{ .entity_index = @intCast(target_index) },
                        .score = if (request.include_confidence) r.score else null,
                    };
                }
                break :blk relation_objects;
            } else null;

            data[ti] = .{
                .id = if (ti < request.input_ids.len) request.input_ids[ti] else null,
                .entities = entity_objects,
                .relations = relations_inner,
            };
        }

        const prompt_tokens = estimateTextsTokens(input_texts);
        var usage: std.json.ObjectMap = .empty;
        try usage.ensureTotalCapacity(alloc, 3);
        usage.putAssumeCapacity("prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
        usage.putAssumeCapacity("completion_tokens", .{ .integer = 0 });
        usage.putAssumeCapacity("total_tokens", .{ .integer = @intCast(prompt_tokens) });
        return ctx.json(extraction_api.ExtractionResponse{
            .object = "extraction",
            .data = data,
            .model = request.model,
            .usage = .{ .object = usage },
        });
    }

    fn toExtractionApiEntity(entity: @import("../pipelines/ner.zig").Entity, request: EntityExtractionRequest) extraction_api.ExtractionEntity {
        return .{
            .text = entity.text,
            .label = entity.label,
            .start = if (request.include_spans) @intCast(entity.start) else null,
            .end = if (request.include_spans) @intCast(entity.end) else null,
            .score = if (request.include_confidence) entity.score else null,
        };
    }

    fn findEntityIndex(entities: []const @import("../pipelines/ner.zig").Entity, needle: @import("../pipelines/ner.zig").Entity) ?usize {
        for (entities, 0..) |entity, index| {
            if (entity.start == needle.start and entity.end == needle.end and
                std.mem.eql(u8, entity.label, needle.label) and std.mem.eql(u8, entity.text, needle.text)) return index;
        }
        return null;
    }

    pub fn classifyText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.ClassifyRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        const queue_units = self.estimateHttpRequestQueueUnits(ctx);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("classify");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        if (self.resolveModelPath(ctx.io, model_name, "classifiers")) |model_path| {
            var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer model_handle.release();
            const model = model_handle.get();

            // Detect entailment index from id2label (varies by NLI model)
            const entailment_idx: ?usize = if (model.manifest.id2label) |labels| blk: {
                for (labels, 0..) |label, i| {
                    if (std.mem.eql(u8, label, "entailment") or std.mem.eql(u8, label, "ENTAILMENT")) {
                        break :blk i;
                    }
                }
                break :blk null;
            } else null;

            const config = @import("../pipelines/classification.zig").ClassificationConfig{
                .max_length = model.manifest.max_position_embeddings,
                .hypothesis_template = body.hypothesis_template orelse "This example is {}.",
                .multi_label = body.multi_label orelse false,
                .entailment_index = entailment_idx,
            };
            var pipeline = model.classificationPipeline(ctx.allocator, config);

            const all_results = pipeline.classifyBatch(body.texts, body.labels) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer {
                for (all_results) |r| ctx.allocator.free(r);
                ctx.allocator.free(all_results);
            }

            const prompt_tokens =
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.texts) catch estimateTextsTokens(body.texts)) +
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.labels) catch estimateTextsTokens(body.labels));
            return buildClassificationResponse(ctx, body.model, all_results, prompt_tokens);
        } else |_| {}

        if (self.resolveModelPath(ctx.io, model_name, "extractors")) |model_path| {
            var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer model_handle.release();
            const model = model_handle.get();
            if (!model.isGlinerModel() or !model.supportsClassification()) {
                return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
            }

            var pipeline = model.glinerPipeline(ctx.allocator);
            const all_results = pipeline.classifyBatch(body.texts, body.labels, .{
                .threshold = 0.0,
                .multi_label = body.multi_label orelse false,
            }) catch |err| switch (err) {
                error.MissingSpecialTokenIds => return ctx.status(500).json(.{ .@"error" = "MODEL_CONFIG_INVALID", .message = @errorName(err) }),
                else => return inferenceFailureResponse(ctx, err),
            };
            defer {
                for (all_results) |r| ctx.allocator.free(r);
                ctx.allocator.free(all_results);
            }

            const prompt_tokens =
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.texts) catch estimateTextsTokens(body.texts)) +
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.labels) catch estimateTextsTokens(body.labels));
            return buildClassificationResponse(ctx, body.model, all_results, prompt_tokens);
        } else |_| {}

        return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
    }

    pub fn classifyDocument(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.DocumentClassificationRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("classify");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "classifiers") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        const prefix = body.prefix orelse document_classification.default_prefix;
        const checkpoint_path = document_classification.resolveCheckpointPath(ctx.allocator, model_path) catch |err| switch (err) {
            error.CheckpointNotFound => return ctx.status(404).json(.{
                .@"error" = "CHECKPOINT_NOT_FOUND",
                .message = "layoutdoc_sequence_head.safetensors not found",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) }),
        };
        defer ctx.allocator.free(checkpoint_path);

        var head = document_classification.Head.load(ctx.allocator, checkpoint_path, prefix) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer head.deinit();

        const num_tokens: usize = std.math.cast(usize, body.num_tokens) orelse
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "num_tokens out of range" });

        const input = document_classification.ExampleInput{
            .image_path = body.image_path,
            .num_tokens = num_tokens,
        };

        const features = document_classification.extractFeatures(ctx.allocator, input) catch |err| switch (err) {
            error.FileNotFound => return ctx.status(404).json(.{ .@"error" = "IMAGE_NOT_FOUND", .message = "image not found" }),
            else => return inferenceFailureResponse(ctx, err),
        };

        const results = document_classification.classifyWithHead(ctx.allocator, &head, body.labels, input) catch |err| switch (err) {
            error.LabelCountMismatch => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "label count does not match checkpoint output width",
            }),
            error.FileNotFound => return ctx.status(404).json(.{ .@"error" = "IMAGE_NOT_FOUND", .message = "image not found" }),
            else => return inferenceFailureResponse(ctx, err),
        };
        defer ctx.allocator.free(results);

        var input_obj: std.json.ObjectMap = .empty;
        defer input_obj.deinit(ctx.allocator);
        try input_obj.put(ctx.allocator, "image_path", .{ .string = body.image_path });
        try input_obj.put(ctx.allocator, "num_tokens", .{ .integer = @intCast(num_tokens) });

        var best_obj: std.json.ObjectMap = .empty;
        defer best_obj.deinit(ctx.allocator);
        const best_value: ?std.json.Value = if (results.len > 0) blk: {
            try best_obj.put(ctx.allocator, "label", .{ .string = results[0].label });
            try best_obj.put(ctx.allocator, "score", .{ .float = results[0].score });
            break :blk .{ .object = best_obj };
        } else null;

        const api_scores = try ctx.allocator.alloc(api.DocumentClassificationResult, results.len);
        defer ctx.allocator.free(api_scores);
        for (results, 0..) |result, idx| {
            api_scores[idx] = .{ .label = result.label, .score = result.score };
        }

        const data = [_]api.DocumentClassificationObject{.{
            .object = "document.classification",
            .index = 0,
            .checkpoint_path = checkpoint_path,
            .prefix = prefix,
            .input = .{ .object = input_obj },
            .features = .{
                .num_tokens = @intCast(features.num_tokens),
                .image_width = @intCast(features.image_width),
                .image_height = @intCast(features.image_height),
                .image_components = @intCast(features.image_components),
                .mean_darkness = features.mean_darkness,
                .std_darkness = features.std_darkness,
                .top_darkness = features.top_darkness,
                .bottom_darkness = features.bottom_darkness,
                .left_darkness = features.left_darkness,
                .right_darkness = features.right_darkness,
                .center_darkness = features.center_darkness,
            },
            .best = best_value,
            .scores = api_scores,
        }};

        return ctx.json(api.DocumentClassificationResponse{
            .object = "list",
            .data = &data,
            .model = body.model,
            .usage = tokenUsage(@intCast(num_tokens), 0),
        });
    }

    pub fn classifyDocumentTokens(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.DocumentTokenClassificationRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("classify");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "classifiers") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        const prefix = body.prefix orelse document_token_classification.default_prefix;
        const checkpoint_path = document_token_classification.resolveCheckpointPath(ctx.allocator, model_path) catch |err| switch (err) {
            error.CheckpointNotFound => return ctx.status(404).json(.{
                .@"error" = "CHECKPOINT_NOT_FOUND",
                .message = "layoutdoc_token_head.safetensors not found",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) }),
        };
        defer ctx.allocator.free(checkpoint_path);

        var head = document_token_classification.Head.load(ctx.allocator, checkpoint_path, prefix) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer head.deinit();

        const tokens = try ctx.allocator.alloc(document_token_classification.TokenBox, body.tokens.len);
        defer ctx.allocator.free(tokens);
        for (body.tokens, 0..) |tok, idx| {
            if (tok.bbox.len != 4) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "each token bbox must contain 4 integers" });
            }
            tokens[idx] = .{
                .text = tok.text,
                .bbox = .{
                    @intCast(tok.bbox[0]),
                    @intCast(tok.bbox[1]),
                    @intCast(tok.bbox[2]),
                    @intCast(tok.bbox[3]),
                },
            };
        }

        const predictions = document_token_classification.classifyWithHead(ctx.allocator, &head, body.labels, tokens) catch |err| switch (err) {
            error.LabelCountMismatch => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "label count does not match checkpoint output width",
            }),
            else => return inferenceFailureResponse(ctx, err),
        };
        defer {
            for (predictions) |pred| ctx.allocator.free(pred.scores);
            ctx.allocator.free(predictions);
        }

        const api_predictions = try ctx.allocator.alloc(api.DocumentTokenClassificationPrediction, predictions.len);
        defer ctx.allocator.free(api_predictions);

        var bbox_bufs = std.ArrayListUnmanaged([]i64).empty;
        defer {
            for (bbox_bufs.items) |b| ctx.allocator.free(b);
            bbox_bufs.deinit(ctx.allocator);
        }
        var feat_bbox_bufs = std.ArrayListUnmanaged([]i64).empty;
        defer {
            for (feat_bbox_bufs.items) |b| ctx.allocator.free(b);
            feat_bbox_bufs.deinit(ctx.allocator);
        }
        var score_bufs = std.ArrayListUnmanaged([]api.DocumentTokenClassificationResult).empty;
        defer {
            for (score_bufs.items) |s| ctx.allocator.free(s);
            score_bufs.deinit(ctx.allocator);
        }
        var best_objs = std.ArrayListUnmanaged(*std.json.ObjectMap).empty;
        defer {
            for (best_objs.items) |o| {
                o.deinit(ctx.allocator);
                ctx.allocator.destroy(o);
            }
            best_objs.deinit(ctx.allocator);
        }

        for (predictions, 0..) |pred, pred_idx| {
            const bbox_slice = try ctx.allocator.alloc(i64, pred.bbox.len);
            for (pred.bbox, 0..) |coord, ci| bbox_slice[ci] = coord;
            try bbox_bufs.append(ctx.allocator, bbox_slice);

            const feat_bbox_slice = try ctx.allocator.alloc(i64, pred.features.bbox.len);
            for (pred.features.bbox, 0..) |coord, ci| feat_bbox_slice[ci] = coord;
            try feat_bbox_bufs.append(ctx.allocator, feat_bbox_slice);

            const api_scores = try ctx.allocator.alloc(api.DocumentTokenClassificationResult, pred.scores.len);
            for (pred.scores, 0..) |score, si| {
                api_scores[si] = .{ .label = score.label, .score = score.score };
            }
            try score_bufs.append(ctx.allocator, api_scores);

            const best_value: ?std.json.Value = if (pred.best) |best| blk: {
                const obj = try ctx.allocator.create(std.json.ObjectMap);
                obj.* = .empty;
                try best_objs.append(ctx.allocator, obj);
                try obj.put(ctx.allocator, "label", .{ .string = best.label });
                try obj.put(ctx.allocator, "score", .{ .float = best.score });
                break :blk .{ .object = obj.* };
            } else null;

            api_predictions[pred_idx] = .{
                .token_index = @intCast(pred.token_index),
                .text = pred.text,
                .bbox = bbox_slice,
                .features = .{
                    .text_length = @intCast(pred.features.text_length),
                    .bbox = feat_bbox_slice,
                    .width = pred.features.width,
                    .height = pred.features.height,
                    .relative_position = pred.features.relative_position,
                    .bbox_phase_sin = pred.features.bbox_phase_sin,
                },
                .best = best_value,
                .scores = api_scores,
            };
        }

        const data = [_]api.DocumentTokenClassificationObject{.{
            .object = "document.token_classification",
            .index = 0,
            .checkpoint_path = checkpoint_path,
            .prefix = prefix,
            .num_tokens = @intCast(predictions.len),
            .predictions = api_predictions,
        }};

        return ctx.json(api.DocumentTokenClassificationResponse{
            .object = "list",
            .data = &data,
            .model = body.model,
            .usage = tokenUsage(body.tokens.len, 0),
        });
    }

    pub fn rewriteText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.RewriteRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("rewrite");
        defer self.metrics.decActive();

        // Resolve model
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "rewriters") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        // Check if this is an encoder-decoder model and find ONNX file paths
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
        const paths = enc_dec_mod.findEncoderDecoderPaths(ctx.allocator, model_path) catch
            return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support rewriting (missing encoder/decoder model files)",
            });
        defer ctx.allocator.free(paths.encoder);
        defer ctx.allocator.free(paths.decoder);

        var component_loader = self.model_manager.componentLoaderForPaths(
            model_path,
            self.session_manager.preferred_backends,
            &.{ paths.encoder, paths.decoder },
        ) catch |err| return modelLoadFailureResponse(ctx, err);
        var encoder_managed = component_loader.load(paths.encoder) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer encoder_managed.deinit();
        var strict_loader = component_loader.restrictToBackend(encoder_managed.session.backend()) catch |err|
            return modelLoadFailureResponse(ctx, err);
        var decoder_managed = strict_loader.load(paths.decoder) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer decoder_managed.deinit();
        const encoder_session = encoder_managed.session;
        const decoder_session = decoder_managed.session;

        // Parse decoder config
        const dec_config = enc_dec_mod.loadDecoderConfig(ctx.allocator, model_path) catch enc_dec_mod.DecoderConfig{};

        // Load tokenizer
        const hf_tokenizer = @import("inference_hf_tokenizer");
        const tok_path = std.fmt.allocPrint(ctx.allocator, "{s}/tokenizer.json", .{model_path}) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
        defer ctx.allocator.free(tok_path);

        const tok_bytes = c_file.readFile(ctx.allocator, tok_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
        defer ctx.allocator.free(tok_bytes);

        var hf_tok = hf_tokenizer.HfTokenizer.loadFromBytes(ctx.allocator, tok_bytes) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
        defer hf_tok.deinitSelf();

        const rewriting = @import("../pipelines/rewriting.zig");
        var pipeline = rewriting.RewritingPipeline{
            .allocator = ctx.allocator,
            .enc_dec = .{
                .allocator = ctx.allocator,
                .encoder = encoder_session,
                .decoder = decoder_session,
                .config = dec_config,
            },
            .tokenizer = hf_tok.tokenizer(),
            .config = .{
                .max_length = dec_config.max_length,
            },
        };

        const data = try ctx.allocator.alloc(api.RewriteObject, body.inputs.len);
        var filled: usize = 0;
        defer {
            for (data[0..filled]) |item| {
                for (item.texts) |s| ctx.allocator.free(s);
                ctx.allocator.free(item.texts);
            }
            ctx.allocator.free(data);
        }

        var completion_tokens: usize = 0;
        for (body.inputs, 0..) |input_text, i| {
            var result = pipeline.rewrite(input_text) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer result.deinit();

            const inner = try ctx.allocator.alloc([]const u8, 1);
            errdefer ctx.allocator.free(inner);
            inner[0] = try ctx.allocator.dupe(u8, result.text);
            completion_tokens += countTokenizerTokens(ctx.allocator, self.session_manager.io, hf_tok.tokenizer(), result.text) catch estimateTextTokens(result.text);
            data[i] = .{
                .object = "rewrite",
                .index = @intCast(i),
                .texts = inner,
            };
            filled = i + 1;
        }

        const prompt_tokens = countTokenizerTexts(ctx.allocator, self.session_manager.io, hf_tok.tokenizer(), body.inputs) catch estimateTextsTokens(body.inputs);
        return ctx.json(api.RewriteResponse{
            .object = "list",
            .data = data,
            .model = body.model,
            .usage = tokenUsage(prompt_tokens, completion_tokens),
        });
    }

    pub fn readImages(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.ReadRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.images.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'images' must not be empty" });
        }
        if (body.images.len > max_read_batch_images) {
            return ctx.status(413).json(.{
                .@"error" = "BATCH_TOO_LARGE",
                .message = try std.fmt.allocPrint(ctx.allocator, "'images' must contain at most {d} items", .{max_read_batch_images}),
            });
        }
        const max_tokens = validateReadMaxTokens(body.max_tokens) catch
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "'max_tokens' must be between 1 and 1024",
            });
        const queue_units = estimateReadQueueUnits(body.images.len, max_tokens);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("read");
        defer self.metrics.decActive();

        // Resolve model
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "readers") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        var reader = readers_mod.LoadedReader.loadFromDir(
            ctx.allocator,
            model_path,
            &self.session_manager,
            &self.model_manager,
        ) catch |err| switch (err) {
            error.InvalidModelForReading => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support reading (missing encoder/decoder model files)",
            }),
            error.NativePix2StructNotYetSupported => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "native Pix2Struct reader checkpoints are not supported yet",
            }),
            error.MultiStageReaderNotYetSupported => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "multi-stage OCR model uses unsupported stages or configuration",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) }),
        };
        defer reader.deinit();

        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const results_out = try alloc.alloc(api.ReadObject, body.images.len);
        var filled: usize = 0;
        defer {
            for (0..filled) |i| {
                var tmp = results_out[i];
                if (tmp.fields) |*f| f.deinit(alloc);
            }
        }

        const downloaded = try ctx.allocator.alloc(scraping.DownloadedContent, body.images.len);
        var downloaded_count: usize = 0;
        defer {
            for (downloaded[0..downloaded_count]) |*item| item.deinit(ctx.allocator);
            ctx.allocator.free(downloaded);
        }
        const image_datas = try ctx.allocator.alloc([]const u8, body.images.len);
        defer ctx.allocator.free(image_datas);

        const batch_byte_cap = readBatchMaxBytes();
        var batch_bytes: usize = 0;
        for (body.images, 0..) |img_url, i| {
            var item = downloadReadBatchContent(self, ctx.allocator, img_url.url, batch_byte_cap, batch_bytes) catch |err| switch (err) {
                error.ReadBatchTooLarge,
                error.StreamTooLong,
                => return ctx.status(413).json(.{
                    .@"error" = "BATCH_TOO_LARGE",
                    .message = try std.fmt.allocPrint(ctx.allocator, "total downloaded image bytes must be at most {d}", .{batch_byte_cap}),
                }),
                else => return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "failed to download image content",
                }),
            };
            errdefer item.deinit(ctx.allocator);
            batch_bytes = addReadBatchDownloadedBytes(batch_bytes, item, batch_byte_cap) catch |err| switch (err) {
                error.ReadBatchTooLarge => return ctx.status(413).json(.{
                    .@"error" = "BATCH_TOO_LARGE",
                    .message = try std.fmt.allocPrint(ctx.allocator, "total downloaded image bytes must be at most {d}", .{batch_byte_cap}),
                }),
            };
            downloaded[i] = item;
            downloaded_count += 1;
            image_datas[i] = downloaded[i].data;
        }

        const results = reader.readBatch(image_datas, .{
            .prompt = normalizeReadPrompt(body.prompt),
            .max_tokens = max_tokens,
        }) catch |err| switch (err) {
            error.InvalidMaxTokens => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "'max_tokens' exceeds the selected model's context limit",
            }),
            else => return inferenceFailureResponse(ctx, err),
        };
        defer {
            for (results) |result| {
                var tmp = result;
                tmp.deinit();
            }
            ctx.allocator.free(results);
        }
        if (results.len != body.images.len)
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = "InvalidReadResultCount" });

        var completion_tokens: usize = 0;
        for (results, 0..) |result, i| {
            completion_tokens += estimateTextTokens(result.text);
            results_out[i] = try toApiReadObject(alloc, result, i);
            filled = i + 1;
        }

        const prompt_tokens = if (normalizeReadPrompt(body.prompt)) |prompt| estimateTextTokens(prompt) * body.images.len else 0;
        return ctx.json(api.ReadResponse{
            .object = "list",
            .data = results_out,
            .model = body.model,
            .usage = tokenUsage(prompt_tokens, completion_tokens),
        });
    }

    fn toApiReadObject(alloc: std.mem.Allocator, result: readers_mod.Result, index: usize) !api.ReadObject {
        const text_copy = try alloc.dupe(u8, result.text);

        var fields_map: ?std.json.ArrayHashMap([]const u8) = null;
        if (result.fields.len > 0) {
            var m: std.json.ArrayHashMap([]const u8) = .{};
            try m.map.ensureTotalCapacity(alloc, result.fields.len);
            for (result.fields) |field| {
                const name = try alloc.dupe(u8, field.name);
                const value = try alloc.dupe(u8, field.value);
                m.map.putAssumeCapacity(name, value);
            }
            fields_map = m;
        }

        var regions: ?[]const api.TextRegion = null;
        if (result.regions.len > 0) {
            const out = try alloc.alloc(api.TextRegion, result.regions.len);
            for (result.regions, 0..) |region, i| {
                const bbox = try alloc.alloc(f64, region.bbox.len);
                for (region.bbox, 0..) |c, j| bbox[j] = c;
                out[i] = .{
                    .text = try alloc.dupe(u8, region.text),
                    .bbox = bbox,
                    .confidence = if (region.confidence) |c| @floatCast(c) else null,
                    .label = if (region.label) |l| try alloc.dupe(u8, l) else null,
                };
            }
            regions = out;
        }

        return .{
            .text = text_copy,
            .fields = fields_map,
            .regions = regions,
            .object = "read",
            .index = @intCast(index),
        };
    }

    pub fn transcribeAudio(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.TranscribeRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("transcribe");
        defer self.metrics.decActive();

        // Resolve model
        const transcribe_model_name: ?[]const u8 = if (body.model) |m| (if (m.len > 0) m else null) else null;
        const model_path = self.resolveModelPath(ctx.io, transcribe_model_name, "transcribers") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        // Find encoder/decoder sessions
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
        const tokenizer_mod = @import("inference_tokenizer");
        const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
        var encoder_session: backends_mod.Session = undefined;
        var decoder_session: backends_mod.Session = undefined;
        var encoder_managed: ?model_manager_mod.ManagedSession = null;
        defer if (encoder_managed) |*managed| managed.deinit();
        var decoder_managed: ?model_manager_mod.ManagedSession = null;
        defer if (decoder_managed) |*managed| managed.deinit();
        var tokenizer: tokenizer_mod.Tokenizer = undefined;
        var decoder_config: enc_dec_mod.DecoderConfig = undefined;
        var loaded_model_handle: ?model_manager_mod.ModelHandle = null;
        defer if (loaded_model_handle) |*handle| handle.release();
        var whisper_assets_handle: ?model_manager_mod.WhisperAssetsHandle = null;
        defer if (whisper_assets_handle) |*handle| handle.release();
        var prompt_cache: ?*const whisper_prompt.PromptCache = null;

        if (enc_dec_mod.findEncoderDecoderPaths(ctx.allocator, model_path)) |paths| {
            defer ctx.allocator.free(paths.encoder);
            defer ctx.allocator.free(paths.decoder);

            whisper_assets_handle = self.model_manager.acquireWhisperCompositeAssets(
                model_path,
                &.{ paths.encoder, paths.decoder },
            ) catch |err| return modelLoadFailureResponse(ctx, err);
            var component_loader = self.model_manager.componentLoaderForPaths(
                model_path,
                self.session_manager.preferred_backends,
                &.{ paths.encoder, paths.decoder },
            ) catch |err| return modelLoadFailureResponse(ctx, err);
            encoder_managed = component_loader.load(paths.encoder) catch |err|
                return modelLoadFailureResponse(ctx, err);
            encoder_session = encoder_managed.?.session;
            var strict_loader = component_loader.restrictToBackend(encoder_session.backend()) catch |err|
                return modelLoadFailureResponse(ctx, err);
            decoder_managed = strict_loader.load(paths.decoder) catch |err|
                return modelLoadFailureResponse(ctx, err);
            decoder_session = decoder_managed.?.session;
            self.model_manager.validateWhisperAssetsCurrent(
                &whisper_assets_handle.?,
                model_path,
                &.{ paths.encoder, paths.decoder },
            ) catch |err| return modelLoadFailureResponse(ctx, err);
            const assets = whisper_assets_handle.?.get();
            tokenizer = assets.tokenizer();
            decoder_config = assets.decoder_config;
            prompt_cache = &assets.prompt_cache;
        } else |_| {
            loaded_model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            const model = loaded_model_handle.?.get();
            const whisper_config = session_factory.getWhisperConfig(model.session) orelse {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_MODEL",
                    .message = "model does not support transcription",
                });
            };
            encoder_session = model.session;
            decoder_session = model.session;
            tokenizer = model.getTokenizer();
            prompt_cache = if (model.whisper_prompt_cache) |*cache| cache else null;
            decoder_config = .{
                .max_length = @intCast(whisper_config.max_target_positions),
                .decoder_start_token_id = whisper_config.decoder_start_token_id,
                .eos_token_id = whisper_config.eos_token_id,
                .pad_token_id = whisper_config.pad_token_id,
                .vocab_size = @intCast(whisper_config.vocab_size),
            };
        }

        const effective_prompt_cache = prompt_cache orelse
            return ctx.status(500).json(.{ .@"error" = "INVALID_MODEL", .message = "WhisperPromptCacheUnavailable" });
        var prompt_scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
        const forced_ids = effective_prompt_cache.resolve(&prompt_scratch, body.language) catch
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "language is not supported by this Whisper model",
            });
        const transcription = @import("../pipelines/transcription.zig");
        var pipeline = transcription.TranscriptionPipeline.init(
            ctx.allocator,
            encoder_session,
            decoder_session,
            tokenizer,
            .{
                .max_length = decoder_config.max_length,
                .decoder_start_token_id = decoder_config.decoder_start_token_id,
                .eos_token_id = decoder_config.eos_token_id,
                .language = body.language,
                .forced_decoder_ids = forced_ids,
                .language_tokens = effective_prompt_cache.language_tokens,
            },
        );

        // Decode audio data — accept both raw base64 and data URI format
        const decoded_audio = if (std.mem.startsWith(u8, body.audio, "data:"))
            decodeDataUri(ctx.allocator, body.audio) catch
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid audio data URI" })
        else blk: {
            const audio_bytes = std.base64.standard.Decoder.calcSizeForSlice(body.audio) catch
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid base64 audio data" });
            const buf2 = ctx.allocator.alloc(u8, audio_bytes) catch |err|
                return ctx.status(500).json(.{ .@"error" = "ALLOC_FAILED", .message = @errorName(err) });
            std.base64.standard.Decoder.decode(buf2, body.audio) catch {
                ctx.allocator.free(buf2);
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid base64 audio data" });
            };
            break :blk DecodedDataUri{
                .mime_type = null,
                .data = buf2,
            };
        };
        defer decoded_audio.deinit(ctx.allocator);

        const decode_options = audio_mod.DecodeOptions{
            .mime_hint = decoded_audio.mime_type,
        };
        if (!audio_mod.canDecodeWithOptions(decoded_audio.data, decode_options)) {
            return unsupportedAudioResponse(ctx, "unsupported audio input");
        }

        var result = pipeline.transcribeWithOptions(decoded_audio.data, decode_options) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer result.deinit();

        const model_str = body.model orelse "default";
        const data = [_]api.TranscribeObject{.{
            .object = "transcription",
            .index = 0,
            .text = result.text,
            .language = result.language,
        }};
        return ctx.json(api.TranscribeResponse{
            .object = "list",
            .data = &data,
            .model = model_str,
            .usage = tokenUsage(0, countTokenizerTokens(ctx.allocator, self.session_manager.io, tokenizer, result.text) catch estimateTextTokens(result.text)),
        });
    }

    pub fn extractJSON(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(extraction_api.ExtractionRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.model.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model is required" });
        }
        if (body.inputs.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "inputs are required" });
        }
        const has_entities = if (body.schema.entities) |entities| entities.len > 0 else false;
        const has_relations = if (body.schema.relations) |relations| relations.len > 0 else false;
        const has_classifications = if (body.schema.classifications) |classifications| classifications.len > 0 else false;
        const has_structures = if (body.schema.structures) |structures| structures.map.count() > 0 else false;
        const operation_count: usize = @intFromBool(has_entities or has_relations) + @intFromBool(has_classifications) + @intFromBool(has_structures);
        if (operation_count == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "schema must request at least one extraction operation" });
        }
        if (operation_count > 1) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "mixed extraction operations are not supported by this model runtime" });
        }
        if (body.schema.entities) |labels| {
            for (labels) |label| if (!isCanonicalLabelSegment(label)) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "entity labels must be non-empty and cannot contain '::'" });
            };
        }

        if (has_structures) {
            const inputs = try canonicalExtractingInputs(ctx.allocator, body.inputs);
            defer freeCanonicalExtractingInputs(ctx.allocator, inputs);
            const schema_json = try std.json.Stringify.valueAlloc(ctx.allocator, body.schema, .{});
            defer ctx.allocator.free(schema_json);
            const options_json = if (body.options) |options|
                try std.json.Stringify.valueAlloc(ctx.allocator, options, .{})
            else
                try ctx.allocator.dupe(u8, "{}");
            defer ctx.allocator.free(options_json);
            var response = self.extractDirect(ctx.allocator, body.model, .{
                .inputs = inputs,
                .schema_json = schema_json,
                .options_json = options_json,
            }) catch |err| return extractionDirectFailureResponse(ctx, err);
            defer response.deinit();
            try ctx.setHeader("content-type", "application/json");
            _ = ctx.response.body(response.json);
            return ctx.response.build();
        }

        if (has_classifications) {
            var classification_inputs = parseCanonicalTextInputs(self, ctx.allocator, body.inputs) catch
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "classification extraction requires text inputs" });
            defer classification_inputs.deinit();
            return self.extractClassifications(ctx, body, classification_inputs.texts.items);
        }

        var direct_inputs = parseCanonicalTextInputs(self, ctx.allocator, body.inputs) catch
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "entity and relation extraction requires text inputs" });
        defer direct_inputs.deinit();
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("extract");
        defer self.metrics.decActive();

        const relation_labels = canonicalRelationLabels(ctx.allocator, body.schema.relations orelse &.{}) catch
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid relation schema" });
        defer freeStringSlice(ctx.allocator, relation_labels);
        const options = body.options orelse extraction_api.ExtractionOptions{};
        const entity_request = EntityExtractionRequest{
            .model = body.model,
            .labels = body.schema.entities,
            .relation_labels = if (relation_labels.len > 0) relation_labels else null,
            .resolver = options.resolver,
            .threshold = options.threshold,
            .flat_ner = options.flat_ner,
            .include_confidence = options.include_confidence orelse false,
            .include_spans = options.include_spans orelse false,
            .input_ids = try canonicalInputIds(ctx.allocator, body.inputs),
        };
        defer ctx.allocator.free(entity_request.input_ids);
        return self.extractEntitiesAndRelations(ctx, entity_request, direct_inputs.texts.items, has_relations);
    }

    fn extractClassifications(
        self: *Node,
        ctx: *httpx.Context,
        body: extraction_api.ExtractionRequest,
        texts: []const []const u8,
    ) !httpx.Response {
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("extract");
        defer self.metrics.decActive();

        const model_path = self.resolveModelPath(ctx.io, body.model, "extractors") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();
        if (!model.isGlinerModel() or !model.supportsClassification()) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_MODEL", .message = "model does not support classification extraction" });
        }

        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();
        const lists = try alloc.alloc(std.ArrayListUnmanaged(extraction_api.ExtractionClassification), texts.len);
        for (lists) |*list| list.* = .empty;

        var pipeline = model.glinerPipeline(ctx.allocator);
        const schemas = body.schema.classifications orelse &.{};
        const options = body.options orelse extraction_api.ExtractionOptions{};
        for (schemas) |schema| {
            if (schema.name.len == 0 or schema.labels.len == 0) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "classification name and labels are required" });
            }
            const results = pipeline.classifyBatch(texts, schema.labels, .{
                .threshold = options.threshold orelse 0.0,
                .multi_label = schema.multi_label orelse false,
                .top_k = if (schema.multi_label orelse false) 0 else 1,
            }) catch |err| return inferenceFailureResponse(ctx, err);
            defer {
                for (results) |items| ctx.allocator.free(items);
                ctx.allocator.free(results);
            }
            for (results, 0..) |items, input_index| {
                for (items) |item| try lists[input_index].append(alloc, .{
                    .name = schema.name,
                    .label = item.label,
                    .score = if (options.include_confidence orelse false) item.score else null,
                });
            }
        }

        const data = try alloc.alloc(extraction_api.ExtractionObject, texts.len);
        for (data, 0..) |*item, index| item.* = .{
            .id = body.inputs[index].id,
            .classifications = lists[index].items,
        };
        return ctx.json(extraction_api.ExtractionResponse{
            .object = "extraction",
            .model = body.model,
            .data = data,
        });
    }

    pub fn extract(self: *Node, ctx: *httpx.Context) !httpx.Response {
        return self.extractJSON(ctx);
    }

    fn downloadImagesForExtraction(self: *Node, ctx: *httpx.Context, images: []const api.ImageURL) ![][]const u8 {
        const image_datas = try ctx.allocator.alloc([]const u8, images.len);
        var initialized: usize = 0;
        errdefer {
            for (image_datas[0..initialized]) |image_data| ctx.allocator.free(image_data);
            ctx.allocator.free(image_datas);
        }

        const batch_byte_cap = readBatchMaxBytes();
        var batch_bytes: usize = 0;
        for (images, 0..) |img_url, i| {
            var downloaded = downloadReadBatchContent(self, ctx.allocator, img_url.url, batch_byte_cap, batch_bytes) catch |err| switch (err) {
                error.ReadBatchTooLarge,
                error.StreamTooLong,
                => return error.ReadBatchTooLarge,
                else => return error.ImageDownloadFailed,
            };
            defer downloaded.deinit(ctx.allocator);

            batch_bytes = addReadBatchDownloadedBytes(batch_bytes, downloaded, batch_byte_cap) catch
                return error.ReadBatchTooLarge;
            image_datas[i] = try ctx.allocator.dupe(u8, downloaded.data);
            initialized += 1;
        }

        return image_datas;
    }

    pub fn listModels(self: *Node, ctx: *httpx.Context) !httpx.Response {
        const json_body = try self.listModelsJsonAlloc(ctx.allocator, ctx.io);
        defer ctx.allocator.free(json_body);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(json_body);
        return ctx.response.build();
    }

    /// Build the /ai/v1/models response body. Exposed so an embedding host
    /// (e.g. the antfly server's /connections endpoint) can list the embedded
    /// node's models without going through HTTP. Caller owns the result.
    pub fn listModelsJsonAlloc(self: *Node, a: std.mem.Allocator, io: std.Io) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        defer buf.deinit(a);
        var body = std.ArrayListUnmanaged(u8).empty;
        defer body.deinit(a);
        var openai_data = std.ArrayListUnmanaged(u8).empty;
        defer openai_data.deinit(a);
        var openai_models = std.StringHashMapUnmanaged(void).empty;
        defer openai_models.deinit(a);
        var openai_data_count: usize = 0;
        const list_created = completionCreatedTimestamp();

        // Discover models from filesystem registry
        const ra = self.registry.allocator;
        const discovered = self.registry.discoverShallow(io) catch &[_]registry_mod.ModelEntry{};
        defer {
            for (discovered) |entry| {
                ra.free(entry.name);
                ra.free(entry.path);
            }
            if (discovered.len > 0) ra.free(discovered);
        }

        var discovered_listings = std.ArrayListUnmanaged(DiscoveredModelListing).empty;
        defer {
            for (discovered_listings.items) |*listing| listing.deinit();
            discovered_listings.deinit(a);
        }
        try discovered_listings.ensureTotalCapacity(a, discovered.len);
        for (discovered, 0..) |entry, entry_index| {
            var manifest = manifest_mod.loadFromDir(a, entry.path) catch continue;
            if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) {
                manifest.deinit();
                continue;
            }
            const model_kind = @tagName(manifest.model_type);
            const reader_candidate = taskMatchesModelListing(
                "readers",
                model_kind,
                manifest.gliner_model_type,
                manifest.tasks,
                manifest.capabilities,
            );
            const compatibility_summary = self.compatibilitySummaryForDir(a, entry.path) catch CompatibilitySummary{
                .level = .unknown,
                .code = .artifact_unreadable,
                .message = "model compatibility could not be determined",
            };
            discovered_listings.appendAssumeCapacity(.{
                .entry_index = entry_index,
                .manifest = manifest,
                .reader_supported = reader_candidate and readers_mod.isSupportedModelDir(a, entry.path),
                .kind = model_kind,
                .compatibility_level = @tagName(compatibility_summary.level),
            });
        }

        const task_names = [_][]const u8{
            "embedders",  "rerankers", "chunkers", "generators",   "classifiers",
            "extractors", "rewriters", "readers",  "transcribers",
        };

        for (task_names, 0..) |task, task_idx| {
            if (task_idx > 0) try body.append(a, ',');
            try body.append(a, '"');
            try body.appendSlice(a, task);
            try body.appendSlice(a, "\":{");

            var model_count: usize = 0;

            // Add built-in chunkers
            if (std.mem.eql(u8, task, "chunkers")) {
                try body.appendSlice(a, "\"fixed_bert\":{\"inputs\":[\"text\"]},\"fixed_bpe\":{\"inputs\":[\"text\"]}");
                model_count += 2;
            }

            // Add discovered models matching this task
            for (discovered_listings.items) |*listing| {
                const entry = discovered[listing.entry_index];
                if (std.mem.eql(u8, task, "readers") and !listing.reader_supported) continue;

                const tasks = listing.manifest.tasks;
                const capabilities = listing.manifest.capabilities;
                const gliner_model_type = listing.manifest.gliner_model_type;
                const inputs = listing.manifest.inputs;
                const has_visual = listing.manifest.visual_model_path != null or listing.manifest.visual_projection_path != null;
                const has_audio = listing.manifest.audio_model_path != null or listing.manifest.audio_projection_path != null;
                if (!taskMatchesModelListing(task, listing.kind, gliner_model_type, tasks, capabilities)) continue;

                if (model_count > 0) try body.append(a, ',');
                try jsonEncodeString(&body, a, entry.name);
                try body.append(a, ':');
                // Discovery does not parse chat templates (that is what made this handler
                // slow), so only models already loaded can report a template failure.
                const chat_template_failed =
                    self.model_manager.loadedChatTemplateFailed(entry.path) orelse false;
                try appendModelInfo(
                    &body,
                    a,
                    listing.kind,
                    gliner_model_type,
                    capabilities,
                    inputs,
                    has_visual,
                    has_audio,
                    chat_template_failed,
                    listing.compatibility_level,
                );
                if (isOpenAiListTask(task)) {
                    const enabled = listing.compatibility_level.len > 0 and
                        (!std.mem.eql(u8, listing.compatibility_level, "unknown") or self.config.allow_unknown_models) and
                        !std.mem.eql(u8, listing.compatibility_level, "incompatible");
                    if (enabled) {
                        try appendUniqueOpenAiModelEntry(
                            &openai_data,
                            a,
                            &openai_models,
                            &openai_data_count,
                            entry.name,
                            list_created,
                            listing.compatibility_level,
                        );
                    }
                }
                model_count += 1;
            }

            // Add loaded models not yet listed (loaded by path, not discovered by name)
            {
                self.model_manager.lockLoadedModels();
                defer self.model_manager.unlockLoadedModels();
                var loaded_paths_seen = std.ArrayListUnmanaged([]const u8).empty;
                defer loaded_paths_seen.deinit(a);
                var it = self.model_manager.loaded.iterator();
                while (it.next()) |entry| {
                    const model = entry.value_ptr.*;
                    const model_task = @tagName(model.manifest.model_type);
                    if (!taskMatchesModelListing(task, model_task, model.manifest.gliner_model_type, model.manifest.tasks, model.manifest.capabilities)) continue;

                    // Skip if already listed from discovery
                    var already_listed = false;
                    for (discovered) |d| {
                        if (std.mem.eql(u8, d.path, model.model_dir)) {
                            already_listed = true;
                            break;
                        }
                    }
                    if (!already_listed) {
                        for (loaded_paths_seen.items) |path| {
                            if (std.mem.eql(u8, path, model.model_dir)) {
                                already_listed = true;
                                break;
                            }
                        }
                    }
                    if (!already_listed) {
                        try loaded_paths_seen.append(a, model.model_dir);
                        if (model_count > 0) try body.append(a, ',');
                        try jsonEncodeString(&body, a, model.model_dir);
                        try body.append(a, ':');
                        const loaded_compatibility = self.compatibilitySummaryForDir(a, model.model_dir) catch CompatibilitySummary{
                            .level = .unknown,
                            .code = .artifact_unreadable,
                            .message = "model compatibility could not be determined",
                        };
                        try appendModelInfo(
                            &body,
                            a,
                            model_task,
                            model.manifest.gliner_model_type,
                            model.manifest.capabilities,
                            model.manifest.inputs,
                            model.manifest.visual_model_path != null or model.manifest.visual_projection_path != null,
                            model.manifest.audio_model_path != null or model.manifest.audio_projection_path != null,
                            model.chat_template_failed,
                            @tagName(loaded_compatibility.level),
                        );
                        if (isOpenAiListTask(task)) {
                            const enabled = loaded_compatibility.level == .compatible or
                                (loaded_compatibility.level == .unknown and self.config.allow_unknown_models);
                            if (enabled) {
                                try appendUniqueOpenAiModelEntry(
                                    &openai_data,
                                    a,
                                    &openai_models,
                                    &openai_data_count,
                                    model.model_dir,
                                    list_created,
                                    @tagName(loaded_compatibility.level),
                                );
                            }
                        }
                        model_count += 1;
                    }
                }
            }

            try body.append(a, '}');
        }
        try buf.appendSlice(a, "{\"object\":\"list\",\"data\":[");
        try buf.appendSlice(a, openai_data.items);
        try buf.appendSlice(a, "],");
        try buf.appendSlice(a, body.items);
        try buf.append(a, '}');

        return try buf.toOwnedSlice(a);
    }

    pub fn listPredictors(self: *Node, ctx: *httpx.Context) !httpx.Response {
        try self.discoverPredictors(ctx);
        const infos = self.tabular_registry.list(ctx.allocator) catch
            return ctx.status(500).json(.{ .@"error" = "INTERNAL_ERROR", .message = "failed to list predictors" });
        defer ctx.allocator.free(infos);

        var predictors: std.json.ArrayHashMap(api.PredictorInfo) = .{};
        for (infos) |info| {
            try predictors.map.put(ctx.allocator, info.name, .{
                .task = predictorTaskFromTabular(info.task),
                .num_features = @intCast(info.num_features),
                .num_outputs = @intCast(info.num_outputs),
                .feature_names = if (info.feature_names.len > 0) info.feature_names else null,
                .source_framework = if (info.source_framework.len > 0) info.source_framework else null,
            });
        }

        return ctx.json(api.PredictorsResponse{
            .object = "list",
            .predictors = predictors,
        });
    }

    pub fn predict(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.PredictRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;

        try self.discoverPredictors(ctx);
        const result = tabular_mod.http.predict(ctx.io, ctx.allocator, &self.tabular_registry, .{
            .model = body.model,
            .input = body.input,
        }) catch |err| switch (err) {
            error.ModelNotFound => return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "predictor not found" }),
            error.BatchTooLarge => return ctx.status(413).json(.{ .@"error" = "BATCH_TOO_LARGE", .message = "batch too large" }),
            error.FeatureMismatch => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "feature vector length mismatch" }),
            error.LoadFailed => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = "failed to load predictor" }),
            else => return ctx.status(500).json(.{ .@"error" = "INTERNAL_ERROR", .message = @errorName(err) }),
        };

        return ctx.json(api.PredictResponse{
            .model = result.model,
            .task = predictorTaskFromTabular(result.task),
            .predictions = result.predictions,
        });
    }

    fn discoverPredictors(self: *Node, ctx: *httpx.Context) !void {
        _ = tabular_mod.discovery.seedBuiltins(ctx.io, self.config.ml_dir) catch {};
        _ = tabular_mod.discovery.discover(ctx.io, ctx.allocator, &self.tabular_registry, self.config.ml_dir) catch {};
    }

    pub fn getVersion(_: *Node, ctx: *httpx.Context) !httpx.Response {
        return ctx.json(.{
            .version = build_options.inference_version,
            .git_commit = build_options.git_commit,
            .build_time = build_options.build_time,
            .go_version = build_options.go_version,
            .allow_downloads = build_options.allow_downloads,
            .runtime = "antfly-inference",
            .backends = .{
                .native = build_options.enable_native,
                .onnx = true,
                .onnx_runtime = build_options.enable_onnx,
                .wasm = build_options.enable_wasm,
            },
        });
    }

    /// Register inference API routes on an external server with a compile-time prefix.
    /// Used by standalone mode to register on the unified httpx.Server.
    pub fn registerRoutesOn(self: *Node, comptime prefix: []const u8, server: anytype) !void {
        const router = api.ServerRouter(Node).init(self);
        var prefixed = PrefixedServer(prefix, @TypeOf(server.*)){ .inner = server };
        if (comptime std.mem.eql(u8, prefix, public_api_prefix)) {
            try server.get(prefix ++ "/models", mlModelsHandler);
        }
        try router.register(&prefixed);
        try server.get(prefix ++ "/metrics", metricsHandler);
        active_node = self;
        active_models_dir = self.config.models_dir;
    }

    /// Register AI routes without the Traditional ML predictor endpoints.
    pub fn registerAiRoutesOn(self: *Node, comptime prefix: []const u8, server: anytype) !void {
        const router = api.ServerRouter(Node).init(self);
        var prefixed = AiPrefixedServer(prefix, @TypeOf(server.*)){ .inner = server };
        try router.register(&prefixed);
        // Keep the retired endpoint deterministic when inference routes share
        // the standalone public server. Without an explicit tombstone, httpx
        // reports 405 because nearby generated routes match the path shape,
        // while the dedicated inference server reports 404.
        try server.post(prefix ++ "/recognize", retiredRecognizeHandler);
    }

    fn retiredRecognizeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        return ctx.status(404).json(.{
            .@"error" = "NOT_FOUND",
            .message = "resource not found",
        });
    }

    fn registerRootOperationalRoutes(server: anytype) !void {
        try server.get("/healthz", healthzHandler);
        try server.get("/readyz", readyzHandler);
    }

    pub fn serve(self: *Node, allocator: std.mem.Allocator, io: std.Io, host: []const u8, port: u16) !void {
        self.attachIo(io);
        var server = httpx.Server.initWithConfig(allocator, io, .{
            .host = host,
            .port = port,
            // Generation can legitimately take longer than the generic 30s HTTP
            // default during cold model startup or first-token GPU execution.
            .request_timeout_ms = 300_000,
            .keep_alive_timeout_ms = 300_000,
        });
        defer server.deinit();

        try self.registerRoutesOn(public_api_prefix, &server);
        try self.registerAiRoutesOn(ai_api_prefix, &server);
        try registerRootOperationalRoutes(&server);
        defer {
            active_node = null;
            active_models_dir = null;
        }

        try server.listen();
    }

    fn mlModelsHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        const node = active_node orelse return ctx.status(503).json(.{ .@"error" = "not_initialized", .message = "server not initialized" });
        return node.listPredictors(ctx);
    }

    fn metricsHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        const node = active_node orelse return ctx.status(503).text("service unavailable");

        var writer: std.Io.Writer.Allocating = .init(ctx.allocator);
        defer writer.deinit();

        // Core metrics via prometheus lib
        try @constCast(&node.metrics).render(&writer.writer);

        // Scheduler metrics (computed on-the-fly from loaded models)
        node.model_manager.lockLoadedModels();
        defer node.model_manager.unlockLoadedModels();
        const aggregate = runtime.scheduler.native_generate.aggregateStats(node.model_manager.loaded);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_waiting_requests", "gauge", "Waiting native scheduler requests across loaded models", aggregate.snapshot.waiting_requests);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_prefill_requests", "gauge", "Prefill-phase native scheduler requests across loaded models", aggregate.snapshot.prefill_requests);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_decode_requests", "gauge", "Decode-phase native scheduler requests across loaded models", aggregate.snapshot.decode_requests);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_active_units", "gauge", "Active native scheduler units across loaded models", aggregate.snapshot.active_units);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_batches_total", "counter", "Total unified scheduler steps (one fused forward pass per step)", aggregate.stats.step_batches_total);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_prefill_items_total", "counter", "Total prefill items packed into unified scheduler steps", aggregate.stats.step_prefill_items_total);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_decode_items_total", "counter", "Total decode items packed into unified scheduler steps", aggregate.stats.step_decode_items_total);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_query_tokens_total", "counter", "Total query tokens fused across unified scheduler steps", aggregate.stats.step_query_tokens_total);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_singleton_batches_total", "counter", "Total unified scheduler steps that contained only the leader item", aggregate.stats.step_singleton_batches_total);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_kv_block_skips_total", "counter", "Total pending items skipped due to per-step KV-block budget", aggregate.stats.step_kv_block_skips_total);
        try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_turn_yields_total", "counter", "Total cooperative scheduler yields while waiting for turns", aggregate.stats.turn_yields_total);
        try appendResidentProjectionMetrics(&writer.writer, aggregateResidentProjectionStats(node.model_manager.loaded));
        try appendGraphExecutorMetrics(&writer.writer, graph_mod.executor_stats.snapshot());
        try appendPromptCacheMetrics(&writer.writer, node.model_manager.loaded);

        try ctx.setHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        return ctx.text(writer.writer.buffered());
    }

    fn healthzHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        return ctx.json(.{ .status = "ok" });
    }

    fn readyzHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        const models_dir = active_models_dir orelse return ctx.status(503).json(.{
            .status = "not_ready",
            .models = .{
                .embedders = 0,
                .rerankers = 0,
                .chunkers = 0,
                .generators = 0,
                .classifiers = 0,
                .rewriters = 0,
                .readers = 0,
                .transcribers = 0,
                .extractors = 0,
            },
        });
        const counts = collectDiscoveredModelCounts(models_dir, ctx.allocator, ctx.io);
        const status_text = if (counts.total() > 0) "ready" else "not_ready";
        const status_code: u16 = if (counts.total() > 0) 200 else 503;
        return ctx.status(status_code).json(.{
            .status = status_text,
            .models = .{
                .embedders = counts.embedders,
                .rerankers = counts.rerankers,
                .chunkers = counts.chunkers,
                .generators = counts.generators,
                .classifiers = counts.classifiers,
                .rewriters = counts.rewriters,
                .readers = counts.readers,
                .transcribers = counts.transcribers,
                .extractors = counts.extractors,
            },
        });
    }
};

fn predictorTaskFromTabular(task: @import("ml_tabular").ir.TaskType) api.PredictorTask {
    return switch (task) {
        .regression => .regression,
        .binary_classification => .binary_classification,
        .multiclass => .multiclass,
        .ranking => .ranking,
    };
}

fn buildClassificationResponse(
    ctx: *httpx.Context,
    model_name: []const u8,
    all_results: anytype,
    prompt_tokens: usize,
) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const data = try alloc.alloc(api.ClassifyObject, all_results.len);
    for (all_results, 0..) |results, ti| {
        const inner = try alloc.alloc(api.ClassifyResult, results.len);
        for (results, 0..) |r, ri| inner[ri] = .{ .label = r.label, .score = r.score };
        data[ti] = .{
            .object = "classification",
            .index = @intCast(ti),
            .classifications = inner,
        };
    }

    return ctx.json(api.ClassifyResponse{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = tokenUsage(prompt_tokens, 0),
    });
}

fn extractionResponseJsonAlloc(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    inputs: []const extracting_api.Input,
    all_results: []const extraction_mod.ExtractionResult,
) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const data = try alloc.alloc(extraction_api.ExtractionObject, all_results.len);
    for (all_results, 0..) |result, result_index| {
        var structures_map: std.json.ObjectMap = .empty;
        try structures_map.ensureTotalCapacity(alloc, result.structures.len);
        for (result.structures) |structure| {
            var instances: std.json.Array = .init(alloc);
            try instances.ensureTotalCapacity(structure.instances.len);
            for (structure.instances, 0..) |instance, instance_index| {
                var instance_obj: std.json.ObjectMap = .empty;
                try instance_obj.ensureTotalCapacity(alloc, instance.fields.len);
                for (instance.fields) |field| {
                    try instance_obj.put(alloc, field.name, try extractedFieldToValue(alloc, field.value));
                }
                _ = instance_index;
                instances.appendAssumeCapacity(.{ .object = instance_obj });
            }
            structures_map.putAssumeCapacity(structure.name, .{ .array = instances });
        }
        data[result_index] = .{
            .id = if (result_index < inputs.len) inputs[result_index].id else null,
            .structures = .{ .object = structures_map },
        };
    }

    return try std.json.Stringify.valueAlloc(allocator, extraction_api.ExtractionResponse{
        .object = "extraction",
        .data = data,
        .model = model_name,
        .usage = null,
    }, .{});
}

fn readerFieldsJsonAlloc(allocator: std.mem.Allocator, fields: []const readers_mod.Field) !?[]const u8 {
    if (fields.len == 0) return null;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var obj: std.json.ObjectMap = .empty;
    try obj.ensureTotalCapacity(alloc, fields.len);
    for (fields) |field| {
        try obj.put(alloc, field.name, .{ .string = field.value });
    }
    return try std.json.Stringify.valueAlloc(allocator, std.json.Value{ .object = obj }, .{});
}

fn readerRegionsJsonAlloc(allocator: std.mem.Allocator, regions: []const readers_mod.Region) !?[]const u8 {
    if (regions.len == 0) return null;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var arr = std.json.Array.init(alloc);
    try arr.ensureTotalCapacity(regions.len);
    for (regions) |region| {
        var obj: std.json.ObjectMap = .empty;
        try obj.ensureTotalCapacity(alloc, 4);
        try obj.put(alloc, "text", .{ .string = region.text });
        var bbox = std.json.Array.init(alloc);
        try bbox.ensureTotalCapacity(region.bbox.len);
        for (region.bbox) |coord| bbox.appendAssumeCapacity(.{ .float = coord });
        try obj.put(alloc, "bbox", .{ .array = bbox });
        if (region.confidence) |confidence| try obj.put(alloc, "confidence", .{ .float = confidence });
        if (region.label) |label| try obj.put(alloc, "label", .{ .string = label });
        arr.appendAssumeCapacity(.{ .object = obj });
    }
    return try std.json.Stringify.valueAlloc(allocator, std.json.Value{ .array = arr }, .{});
}

const DirectExtractionOptions = struct {
    allocator: std.mem.Allocator,
    threshold: ?f32 = null,
    flat_ner: ?bool = null,
    include_confidence: ?bool = null,
    include_spans: ?bool = null,
    prompt: ?[]u8 = null,
    max_tokens: ?usize = null,

    fn deinit(self: *@This()) void {
        if (self.prompt) |prompt| self.allocator.free(prompt);
        self.* = undefined;
    }
};

fn parseExtractionOptionsJson(allocator: std.mem.Allocator, raw: []const u8) !DirectExtractionOptions {
    var out = DirectExtractionOptions{ .allocator = allocator };
    errdefer out.deinit();
    if (raw.len == 0) return out;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return out;
    const obj = parsed.value.object;
    out.threshold = jsonFloatField(obj, "threshold");
    out.flat_ner = jsonBoolField(obj, "flat_ner");
    out.include_confidence = jsonBoolField(obj, "include_confidence");
    out.include_spans = jsonBoolField(obj, "include_spans");
    if (jsonStringField(obj, "prompt")) |prompt| out.prompt = try allocator.dupe(u8, prompt);
    out.max_tokens = jsonUsizeField(obj, "max_tokens");
    return out;
}

const DirectExtractionInputs = struct {
    allocator: std.mem.Allocator,
    texts: std.ArrayListUnmanaged([]const u8) = .empty,
    images: std.ArrayListUnmanaged([]const u8) = .empty,
    prompt: ?[]u8 = null,
    max_tokens: ?usize = null,

    fn deinit(self: *@This()) void {
        for (self.texts.items) |text| self.allocator.free(@constCast(text));
        self.texts.deinit(self.allocator);
        for (self.images.items) |image| self.allocator.free(@constCast(image));
        self.images.deinit(self.allocator);
        if (self.prompt) |prompt| self.allocator.free(prompt);
        self.* = undefined;
    }
};

fn canonicalExtractingInputs(allocator: std.mem.Allocator, inputs: []const extraction_api.ExtractionInput) ![]extracting_api.Input {
    const out = try allocator.alloc(extracting_api.Input, inputs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |input| allocator.free(input.content_json);
        allocator.free(out);
    }
    for (inputs, 0..) |input, index| {
        out[index] = .{
            .id = input.id,
            .content_json = try std.json.Stringify.valueAlloc(allocator, input.content, .{}),
        };
        initialized += 1;
    }
    return out;
}

fn validateExtractionInputKinds(text_count: usize, image_count: usize) !void {
    if (text_count == 0 and image_count == 0) return error.UnsupportedInput;
    if (text_count > 0 and image_count > 0) return error.UnsupportedInput;
}

fn parseCanonicalStructureSchemas(
    allocator: std.mem.Allocator,
    schema: extraction_api.ExtractionSchema,
) ![]extraction_mod.ExtractionSchema {
    const structures = schema.structures orelse return error.MissingStructureSchema;
    if (structures.map.count() == 0) return error.MissingStructureSchema;
    const out = try allocator.alloc(extraction_mod.ExtractionSchema, structures.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(allocator);
        allocator.free(out);
    }
    var iterator = structures.map.iterator();
    while (iterator.next()) |entry| {
        if (entry.key_ptr.*.len == 0) return error.InvalidStructureField;
        const fields_map = entry.value_ptr.fields;
        if (fields_map.map.count() == 0) return error.MissingStructureFields;
        const fields = try allocator.alloc(extraction_mod.SchemaField, fields_map.map.count());
        var fields_initialized: usize = 0;
        errdefer {
            for (fields[0..fields_initialized]) |*field| field.deinit(allocator);
            allocator.free(fields);
        }
        var fields_iterator = fields_map.map.iterator();
        while (fields_iterator.next()) |field_entry| {
            if (field_entry.key_ptr.*.len == 0) return error.InvalidStructureField;
            const descriptor = field_entry.value_ptr.*;
            var field_type: extraction_mod.FieldType = .str;
            var choices = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (choices.items) |choice| allocator.free(choice);
                choices.deinit(allocator);
            }
            switch (descriptor) {
                .string => |kind| {
                    if (std.ascii.eqlIgnoreCase(kind, "list") or std.ascii.eqlIgnoreCase(kind, "array")) {
                        field_type = .list;
                    } else if (!std.ascii.eqlIgnoreCase(kind, "str") and !std.ascii.eqlIgnoreCase(kind, "string")) {
                        return error.InvalidStructureField;
                    }
                },
                .object => |object| {
                    if (jsonStringField(object, "type")) |kind| {
                        if (std.ascii.eqlIgnoreCase(kind, "list") or std.ascii.eqlIgnoreCase(kind, "array")) {
                            field_type = .list;
                        } else if (!std.ascii.eqlIgnoreCase(kind, "str") and !std.ascii.eqlIgnoreCase(kind, "string")) {
                            return error.InvalidStructureField;
                        }
                    }
                    if (object.get("enum")) |enum_value| if (enum_value == .array) {
                        if (enum_value.array.items.len < 2) return error.InvalidStructureField;
                        for (enum_value.array.items) |choice| {
                            if (choice != .string or choice.string.len == 0) return error.InvalidStructureField;
                            try choices.append(allocator, try allocator.dupe(u8, choice.string));
                        }
                    } else return error.InvalidStructureField;
                },
                else => return error.InvalidStructureField,
            }
            const owned_name = try allocator.dupe(u8, field_entry.key_ptr.*);
            errdefer allocator.free(owned_name);
            const owned_choices = try choices.toOwnedSlice(allocator);
            errdefer {
                for (owned_choices) |choice| allocator.free(choice);
                allocator.free(owned_choices);
            }
            fields[fields_initialized] = .{
                .name = owned_name,
                .field_type = field_type,
                .choices = owned_choices,
            };
            fields_initialized += 1;
        }
        out[initialized] = .{
            .name = try allocator.dupe(u8, entry.key_ptr.*),
            .fields = fields,
        };
        initialized += 1;
    }
    return out;
}

fn freeCanonicalExtractingInputs(allocator: std.mem.Allocator, inputs: []extracting_api.Input) void {
    for (inputs) |input| allocator.free(input.content_json);
    allocator.free(inputs);
}

fn parseCanonicalTextInputs(
    node: *Node,
    allocator: std.mem.Allocator,
    inputs: []const extraction_api.ExtractionInput,
) !DirectExtractionInputs {
    const direct = try canonicalExtractingInputs(allocator, inputs);
    defer freeCanonicalExtractingInputs(allocator, direct);
    var parsed = try parseDirectExtractionInputs(node, allocator, direct, null, null);
    errdefer parsed.deinit();
    if (parsed.images.items.len > 0 or parsed.texts.items.len != inputs.len) return error.UnsupportedInput;
    return parsed;
}

fn canonicalInputIds(allocator: std.mem.Allocator, inputs: []const extraction_api.ExtractionInput) ![]?[]const u8 {
    const ids = try allocator.alloc(?[]const u8, inputs.len);
    for (inputs, 0..) |input, index| ids[index] = input.id;
    return ids;
}

fn canonicalRelationLabels(
    allocator: std.mem.Allocator,
    schemas: []const extraction_api.ExtractionRelationSchema,
) ![][]const u8 {
    const labels = try allocator.alloc([]const u8, schemas.len);
    var initialized: usize = 0;
    errdefer {
        for (labels[0..initialized]) |label| allocator.free(label);
        allocator.free(labels);
    }
    for (schemas, 0..) |schema, index| {
        if (!isCanonicalLabelSegment(schema.type)) return error.InvalidRelationSchema;
        if (schema.target != null and schema.source == null) return error.InvalidRelationSchema;
        if (schema.source) |source| if (!isCanonicalLabelSegment(source)) return error.InvalidRelationSchema;
        if (schema.target) |target| if (!isCanonicalLabelSegment(target)) return error.InvalidRelationSchema;
        labels[index] = if (schema.source) |source|
            if (schema.target) |target|
                try std.fmt.allocPrint(allocator, "{s}::{s}::{s}", .{ source, schema.type, target })
            else
                try std.fmt.allocPrint(allocator, "{s}::{s}", .{ source, schema.type })
        else
            try allocator.dupe(u8, schema.type);
        initialized += 1;
    }
    return labels;
}

fn isCanonicalLabelSegment(value: []const u8) bool {
    return value.len > 0 and std.mem.indexOf(u8, value, "::") == null;
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn extractionDirectFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.InvalidExtractionConfig,
        error.InvalidStructureField,
        error.MissingStructureFields,
        error.MissingStructureSchema,
        error.UnsupportedInput,
        error.InvalidMaxTokens,
        => ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = @errorName(err) }),
        error.ReadBatchTooLarge,
        error.StreamTooLong,
        => ctx.status(413).json(.{ .@"error" = "BATCH_TOO_LARGE", .message = @errorName(err) }),
        error.ModelNotFound => ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" }),
        else => inferenceFailureResponse(ctx, err),
    };
}

fn parseDirectExtractionInputs(
    node: *Node,
    allocator: std.mem.Allocator,
    inputs: []const extracting_api.Input,
    prompt: ?[]const u8,
    max_tokens: ?usize,
) !DirectExtractionInputs {
    var out = DirectExtractionInputs{
        .allocator = allocator,
        .prompt = if (prompt) |value| try allocator.dupe(u8, value) else null,
        .max_tokens = max_tokens,
    };
    errdefer out.deinit();

    for (inputs) |input| {
        try appendDirectExtractionContent(node, allocator, &out, input.content_json);
    }
    if (out.texts.items.len > 0 and out.images.items.len > 0) return error.UnsupportedInput;
    if (out.texts.items.len == 0 and out.images.items.len == 0) return error.UnsupportedInput;
    return out;
}

fn appendDirectExtractionContent(
    node: *Node,
    allocator: std.mem.Allocator,
    out: *DirectExtractionInputs,
    content_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content_json, .{});
    defer parsed.deinit();

    switch (parsed.value) {
        .string => |text| try out.texts.append(allocator, try allocator.dupe(u8, text)),
        .array => |parts| {
            var text_buf = std.ArrayListUnmanaged(u8).empty;
            defer text_buf.deinit(allocator);
            var saw_media = false;
            for (parts.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "text")) {
                    const text_value = part.object.get("text") orelse continue;
                    if (text_value != .string) continue;
                    if (text_buf.items.len > 0) try text_buf.append(allocator, '\n');
                    try text_buf.appendSlice(allocator, text_value.string);
                } else if (std.mem.eql(u8, type_value.string, "image_url")) {
                    const image_url = part.object.get("image_url") orelse continue;
                    const url = if (image_url == .object)
                        if (image_url.object.get("url")) |url_value| (if (url_value == .string) url_value.string else null) else null
                    else if (image_url == .string)
                        image_url.string
                    else
                        null;
                    if (url) |value| {
                        try appendDownloadedExtractionImage(node, allocator, out, value);
                        saw_media = true;
                    }
                } else if (std.mem.eql(u8, type_value.string, "media")) {
                    if (part.object.get("url")) |url_value| {
                        if (url_value == .string) {
                            try appendDownloadedExtractionImage(node, allocator, out, url_value.string);
                            saw_media = true;
                        }
                    } else if (part.object.get("data")) |data_value| {
                        if (data_value == .string) {
                            const decoded = try decodeMediaData(allocator, data_value.string);
                            var owns_decoded = true;
                            errdefer if (owns_decoded) allocator.free(decoded.data);
                            if (decoded.mime_type) |mime| {
                                if (!std.mem.startsWith(u8, mime, "image/")) return error.UnsupportedInput;
                            }
                            try out.images.append(allocator, decoded.data);
                            owns_decoded = false;
                            saw_media = true;
                        }
                    }
                }
            }
            if (saw_media) {
                if (out.prompt == null and text_buf.items.len > 0) out.prompt = try text_buf.toOwnedSlice(allocator);
            } else if (text_buf.items.len > 0) {
                try out.texts.append(allocator, try text_buf.toOwnedSlice(allocator));
            }
        },
        else => {
            const text = try std.json.Stringify.valueAlloc(allocator, parsed.value, .{});
            try out.texts.append(allocator, text);
        },
    }
}

fn appendDownloadedExtractionImage(
    node: *Node,
    allocator: std.mem.Allocator,
    out: *DirectExtractionInputs,
    url: []const u8,
) !void {
    var downloaded = try downloadRemoteContent(node, allocator, url);
    defer downloaded.deinit(allocator);
    if (!std.mem.startsWith(u8, downloaded.content_type, "image/")) return error.UnsupportedInput;
    try out.images.append(allocator, try allocator.dupe(u8, downloaded.data));
}

fn jsonStringField(obj: std.json.ObjectMap, name: []const u8) ?[]const u8 {
    const value = obj.get(name) orelse return null;
    return if (value == .string) value.string else null;
}

fn jsonBoolField(obj: std.json.ObjectMap, name: []const u8) ?bool {
    const value = obj.get(name) orelse return null;
    return if (value == .bool) value.bool else null;
}

fn jsonFloatField(obj: std.json.ObjectMap, name: []const u8) ?f32 {
    const value = obj.get(name) orelse return null;
    return switch (value) {
        .float => |number| @floatCast(number),
        .integer => |number| @floatFromInt(number),
        .number_string => |raw| std.fmt.parseFloat(f32, raw) catch null,
        else => null,
    };
}

fn jsonUsizeField(obj: std.json.ObjectMap, name: []const u8) ?usize {
    const value = obj.get(name) orelse return null;
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else null,
        .number_string => |raw| std.fmt.parseInt(usize, raw, 10) catch null,
        else => null,
    };
}

fn extractedFieldToValue(
    alloc: std.mem.Allocator,
    field: extraction_mod.ExtractedField,
) !std.json.Value {
    switch (field) {
        .single => |value| return try extractedFieldValueToValue(alloc, value),
        .list => |values| {
            var arr: std.json.Array = .init(alloc);
            try arr.ensureTotalCapacity(values.len);
            for (values) |value| {
                arr.appendAssumeCapacity(try extractedFieldValueToValue(alloc, value));
            }
            return .{ .array = arr };
        },
    }
}

fn extractedFieldValueToValue(
    alloc: std.mem.Allocator,
    value: extraction_mod.ExtractedFieldValue,
) !std.json.Value {
    var obj: std.json.ObjectMap = .empty;
    try obj.ensureTotalCapacity(alloc, 4);
    obj.putAssumeCapacity("value", .{ .string = value.value });
    if (value.score) |score| obj.putAssumeCapacity("score", .{ .float = score });
    if (value.start) |start| obj.putAssumeCapacity("start", .{ .integer = @intCast(start) });
    if (value.end) |end| obj.putAssumeCapacity("end", .{ .integer = @intCast(end) });
    return .{ .object = obj };
}

const ResolvedExtractionOutput = struct {
    entities: [][]@import("../pipelines/ner.zig").Entity,
    relations: ?[][]gliner_mod.Relation,

    fn deinit(self: *ResolvedExtractionOutput, allocator: std.mem.Allocator) void {
        for (self.entities) |entities| {
            for (entities) |entity| {
                allocator.free(entity.text);
                allocator.free(entity.label);
            }
            allocator.free(entities);
        }
        allocator.free(self.entities);

        if (self.relations) |relations_by_text| {
            for (relations_by_text) |relations| {
                for (relations) |*relation| {
                    allocator.free(relation.head.label);
                    allocator.free(relation.tail.label);
                    relation.deinit(allocator);
                }
                allocator.free(relations);
            }
            allocator.free(relations_by_text);
        }
    }
};

fn resolveExtractedOutput(
    allocator: std.mem.Allocator,
    all_entities: []const []const @import("../pipelines/ner.zig").Entity,
    all_relations: ?[]const []const gliner_mod.Relation,
    resolver_cfg: extraction_api.ExtractionResolverOptions,
) !ResolvedExtractionOutput {
    const cfg = resolver_mod.ResolverConfig{
        .similarity_threshold = if ((resolver_cfg.similarity_threshold orelse 0.0) == 0.0) 0.85 else resolver_cfg.similarity_threshold.?,
        .type_must_match = resolver_cfg.type_must_match orelse true,
        .min_entity_confidence = resolver_cfg.min_entity_confidence orelse 0.0,
        .min_relation_confidence = resolver_cfg.min_relation_confidence orelse 0.0,
        .deduplicate_relations = resolver_cfg.deduplicate_relations orelse true,
        .track_provenance = resolver_cfg.track_provenance orelse true,
    };

    var kg = try resolver_mod.buildKnowledgeGraph(allocator, all_entities, all_relations, cfg);
    defer kg.deinit(allocator);

    const input_count = all_entities.len;
    const entity_batches = try allocator.alloc([]@import("../pipelines/ner.zig").Entity, input_count);
    var initialized_entity_batches: usize = 0;
    errdefer {
        freeEntityBatches(allocator, entity_batches[0..initialized_entity_batches]);
        allocator.free(entity_batches);
    }

    for (0..input_count) |text_index| {
        var resolved_for_text = std.ArrayListUnmanaged(@import("../pipelines/ner.zig").Entity).empty;
        var owned = false;
        defer if (!owned) {
            for (resolved_for_text.items) |entity| {
                allocator.free(entity.text);
                allocator.free(entity.label);
            }
            resolved_for_text.deinit(allocator);
        };

        for (kg.entities) |entity| {
            if (!resolvedHasTextIndex(entity.text_indices, text_index)) continue;
            try resolved_for_text.append(allocator, try cloneResolvedEntity(allocator, entity, text_index));
        }

        entity_batches[text_index] = try resolved_for_text.toOwnedSlice(allocator);
        owned = true;
        initialized_entity_batches += 1;
    }

    var relation_batches: ?[][]gliner_mod.Relation = null;
    if (kg.relations.len > 0) {
        const batches = try allocator.alloc([]gliner_mod.Relation, input_count);
        var initialized_relation_batches: usize = 0;
        errdefer {
            freeRelationBatches(allocator, batches[0..initialized_relation_batches]);
            allocator.free(batches);
        }

        for (0..input_count) |text_index| {
            var resolved_for_text = std.ArrayListUnmanaged(gliner_mod.Relation).empty;
            var owned = false;
            defer if (!owned) {
                for (resolved_for_text.items) |*relation| {
                    allocator.free(relation.head.label);
                    allocator.free(relation.tail.label);
                    relation.deinit(allocator);
                }
                resolved_for_text.deinit(allocator);
            };

            for (kg.relations) |relation| {
                if (!resolvedHasTextIndex(relation.text_indices, text_index)) continue;
                try resolved_for_text.append(allocator, try cloneResolvedRelation(allocator, kg.entities, relation, text_index));
            }

            batches[text_index] = try resolved_for_text.toOwnedSlice(allocator);
            owned = true;
            initialized_relation_batches += 1;
        }

        relation_batches = batches;
    }

    return .{
        .entities = entity_batches,
        .relations = relation_batches,
    };
}

fn resolvedHasTextIndex(indices: ?[]const usize, text_index: usize) bool {
    const values = indices orelse return text_index == 0;
    for (values) |value| {
        if (value == text_index) return true;
    }
    return false;
}

fn cloneResolvedEntity(
    allocator: std.mem.Allocator,
    entity: resolver_mod.ResolvedEntity,
    text_index: usize,
) !@import("../pipelines/ner.zig").Entity {
    const occurrence = resolvedOccurrenceForInput(entity, text_index) orelse return error.InvalidResolvedEntity;
    const text = try allocator.dupe(u8, occurrence.text);
    errdefer allocator.free(text);
    const label = try allocator.dupe(u8, entity.label);
    errdefer allocator.free(label);

    return .{
        .text = text,
        .label = label,
        .start = occurrence.start,
        .end = occurrence.end,
        .score = occurrence.score,
    };
}

fn cloneResolvedRelation(
    allocator: std.mem.Allocator,
    entities: []const resolver_mod.ResolvedEntity,
    relation: resolver_mod.ResolvedRelation,
    text_index: usize,
) !gliner_mod.Relation {
    const head_entity = findResolvedEntityById(entities, relation.head_id) orelse return error.InvalidResolvedRelation;
    const tail_entity = findResolvedEntityById(entities, relation.tail_id) orelse return error.InvalidResolvedRelation;
    const head_occurrence = resolvedOccurrenceForInput(head_entity, text_index) orelse return error.InvalidResolvedRelation;
    const tail_occurrence = resolvedOccurrenceForInput(tail_entity, text_index) orelse return error.InvalidResolvedRelation;

    const head_text = try allocator.dupe(u8, head_occurrence.text);
    errdefer allocator.free(head_text);
    const head_label = try allocator.dupe(u8, head_entity.label);
    errdefer allocator.free(head_label);
    const tail_text = try allocator.dupe(u8, tail_occurrence.text);
    errdefer allocator.free(tail_text);
    const tail_label = try allocator.dupe(u8, tail_entity.label);
    errdefer allocator.free(tail_label);
    const label = try allocator.dupe(u8, relation.label);
    errdefer allocator.free(label);

    return .{
        .head = .{
            .text = head_text,
            .label = head_label,
            .start = head_occurrence.start,
            .end = head_occurrence.end,
            .score = head_occurrence.score,
        },
        .tail = .{
            .text = tail_text,
            .label = tail_label,
            .start = tail_occurrence.start,
            .end = tail_occurrence.end,
            .score = tail_occurrence.score,
        },
        .label = label,
        .score = relation.score,
    };
}

fn resolvedOccurrenceForInput(entity: resolver_mod.ResolvedEntity, text_index: usize) ?resolver_mod.ResolvedMention {
    var best: ?resolver_mod.ResolvedMention = null;
    for (entity.occurrences) |occurrence| {
        if (occurrence.text_index != text_index) continue;
        if (best == null or occurrence.score > best.?.score or
            (occurrence.score == best.?.score and occurrence.text.len > best.?.text.len))
        {
            best = occurrence;
        }
    }
    return best;
}

fn freeRelationBatches(allocator: std.mem.Allocator, batches: [][]gliner_mod.Relation) void {
    for (batches) |relations| {
        for (relations) |*relation| {
            allocator.free(relation.head.label);
            allocator.free(relation.tail.label);
            relation.deinit(allocator);
        }
        allocator.free(relations);
    }
}

fn findResolvedEntityById(
    entities: []const resolver_mod.ResolvedEntity,
    id: []const u8,
) ?resolver_mod.ResolvedEntity {
    for (entities) |entity| {
        if (std.mem.eql(u8, entity.id, id)) return entity;
    }
    return null;
}

fn applyLearnedCleanupIfPresent(
    allocator: std.mem.Allocator,
    cleanup_head: ?*const cleanup_model_mod.CleanupHead,
    texts: []const []const u8,
    entities_by_text: []const []const @import("../pipelines/ner.zig").Entity,
) !?[][]@import("../pipelines/ner.zig").Entity {
    if (texts.len != entities_by_text.len) return error.ShapeMismatch;

    const head = cleanup_head orelse return null;

    const out = try allocator.alloc([]@import("../pipelines/ner.zig").Entity, texts.len);
    var built: usize = 0;
    errdefer {
        freeEntityBatches(allocator, out[0..built]);
        allocator.free(out);
    }

    for (texts, entities_by_text, 0..) |text, entities, idx| {
        var cleanup_entities = try allocator.alloc(cleanup_pipeline_mod.Entity, entities.len);
        defer allocator.free(cleanup_entities);
        for (entities, 0..) |entity, entity_idx| {
            cleanup_entities[entity_idx] = .{
                .text = entity.text,
                .label = entity.label,
                .start = entity.start,
                .end = entity.end,
                .score = entity.score,
            };
        }

        const scored = try cleanup_model_mod.scoreEntities(allocator, head, text, cleanup_entities);
        defer {
            for (scored) |*mention| mention.deinit(allocator);
            allocator.free(scored);
        }

        var cleaned = try cleanup_pipeline_mod.cleanupMentions(allocator, scored, .{
            .min_validity_score = head.min_validity_score,
            .dedup_similarity_threshold = head.dedup_similarity_threshold,
        });
        defer cleaned.deinit(allocator);

        out[idx] = try allocator.alloc(@import("../pipelines/ner.zig").Entity, cleaned.resolved_entities.len);
        for (cleaned.resolved_entities, 0..) |resolved_entity, entity_idx| {
            out[idx][entity_idx] = .{
                .text = try allocator.dupe(u8, resolved_entity.text),
                .label = try allocator.dupe(u8, resolved_entity.label),
                .start = resolved_entity.start,
                .end = resolved_entity.end,
                .score = resolved_entity.detect_score * resolved_entity.validity_score,
            };
        }
        built += 1;
    }

    return out;
}

fn freeEntityBatches(allocator: std.mem.Allocator, all_entities: []const []@import("../pipelines/ner.zig").Entity) void {
    for (all_entities) |entities| {
        for (entities) |entity| {
            allocator.free(entity.text);
            allocator.free(entity.label);
        }
        allocator.free(entities);
    }
    allocator.free(all_entities);
}

fn appendPromMetric(writer: *std.Io.Writer, name: []const u8, metric_type: []const u8, help: []const u8, value: u64) !void {
    try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n{s} {d}\n", .{ name, help, name, metric_type, name, value });
}

fn appendResidentProjectionMetrics(writer: *std.Io.Writer, stats: embedding_mod.ResidentProjectionStats) !void {
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_text_success_total", "counter", "Total successful text resident projection attempts", stats.text_success);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_text_fallback_total", "counter", "Total text resident projection fallbacks", stats.text_fallback);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_image_success_total", "counter", "Total successful image resident projection attempts", stats.image_success);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_image_fallback_total", "counter", "Total image resident projection fallbacks", stats.image_fallback);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_audio_success_total", "counter", "Total successful audio resident projection attempts", stats.audio_success);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_audio_fallback_total", "counter", "Total audio resident projection fallbacks", stats.audio_fallback);
}

fn appendGraphExecutorMetrics(writer: *std.Io.Writer, stats: graph_mod.executor_stats.ExecutionStats) !void {
    try appendPromMetric(writer, "inference_graph_executor_partitions_total", "counter", "Total graph executor partitions executed", stats.partitions_executed);
    try appendPromMetric(writer, "inference_graph_executor_cross_device_transfers_total", "counter", "Total graph executor cross-device transfers", stats.cross_device_transfers);
    try appendPromMetric(writer, "inference_graph_executor_runtime_input_transfers_total", "counter", "Total graph executor runtime input transfers", stats.runtime_input_transfers);
    try appendPromMetric(writer, "inference_graph_executor_device_resident_transfers_total", "counter", "Total graph executor device-resident transfers", stats.device_resident_transfers);
    try appendPromMetric(writer, "inference_graph_executor_backend_command_dispatches_total", "counter", "Total graph executor backend command dispatches", stats.backend_command_dispatches);
    try appendPromMetric(writer, "inference_graph_executor_planned_operator_dispatches_total", "counter", "Total graph executor planned operator dispatches", stats.planned_operator_dispatches);
    try appendPromMetric(writer, "inference_graph_executor_interpreter_fallbacks_total", "counter", "Total graph executor interpreter fallback partitions", stats.interpreter_fallbacks);
    try appendPromMetric(writer, "inference_graph_executor_device_resident_outputs_total", "counter", "Total graph executor device-resident outputs", stats.device_resident_outputs);
    try appendPromMetric(writer, "inference_graph_executor_device_resident_parameter_outputs_total", "counter", "Total graph executor device-resident parameter outputs", stats.device_resident_parameter_outputs);
    try appendPromMetric(writer, "inference_graph_executor_host_materialized_outputs_total", "counter", "Total graph executor host-materialized outputs", stats.host_materialized_outputs);
    try appendPromMetric(writer, "inference_graph_executor_boundary_output_materializations_total", "counter", "Total graph executor boundary output materializations", stats.boundary_output_materializations);
    try appendPromMetric(writer, "inference_graph_executor_graph_plan_slots_reserved_total", "counter", "Total graph executor planned buffer slots reserved", stats.graph_plan_slots_reserved);
    try appendPromMetric(writer, "inference_graph_executor_graph_plan_bytes_reserved_total", "counter", "Total graph executor planned buffer bytes reserved", stats.graph_plan_bytes_reserved);
}

fn appendPromptCacheMetrics(writer: *std.Io.Writer, models: anytype) !void {
    var hits: u64 = 0;
    var misses: u64 = 0;
    var evictions: u64 = 0;
    var cached_tokens: u64 = 0;
    var live_entries: u64 = 0;
    var live_bytes: u64 = 0;
    var block_hash_hits: u64 = 0;
    var block_hash_misses: u64 = 0;
    var block_hash_evictions: u64 = 0;
    var block_hash_cached_blocks: u64 = 0;
    var block_hash_collision_guards: u64 = 0;
    var it = models.iterator();
    while (it.next()) |entry| {
        const stats = entry.value_ptr.*.prompt_prefix_cache.stats();
        hits += stats.hits;
        misses += stats.misses;
        evictions += stats.evictions;
        cached_tokens += stats.cached_tokens;
        live_entries += @intCast(stats.live_entries);
        live_bytes += @intCast(stats.live_bytes);
        block_hash_hits += stats.block_hash_hits;
        block_hash_misses += stats.block_hash_misses;
        block_hash_evictions += stats.block_hash_evictions;
        block_hash_cached_blocks += @intCast(stats.block_hash_cached_blocks);
        block_hash_collision_guards += stats.block_hash_collision_guards;
    }
    try appendPromMetric(writer, "antfly_inference_prompt_cache_hits_total", "counter", "Total prompt prefix cache hits", hits);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_misses_total", "counter", "Total prompt prefix cache misses", misses);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_evictions_total", "counter", "Total prompt prefix cache evictions", evictions);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_cached_tokens", "gauge", "Prompt prefix cache retained prompt tokens", cached_tokens);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_live_entries", "gauge", "Prompt prefix cache live entries", live_entries);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_live_bytes", "gauge", "Prompt prefix cache estimated logical bytes", live_bytes);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_hits_total", "counter", "Total block-hash prompt cache hits", block_hash_hits);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_misses_total", "counter", "Total block-hash prompt cache misses", block_hash_misses);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_evictions_total", "counter", "Total block-hash prompt cache evictions", block_hash_evictions);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_cached_blocks", "gauge", "Block-hash prompt cache retained KV blocks", block_hash_cached_blocks);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_collision_guards_total", "counter", "Total block-hash prompt cache collisions detected", block_hash_collision_guards);
}

fn aggregateResidentProjectionStats(models: anytype) embedding_mod.ResidentProjectionStats {
    var aggregate = embedding_mod.ResidentProjectionStats{};
    var it = models.iterator();
    while (it.next()) |entry| {
        aggregate.add(entry.value_ptr.*.resident_projection_stats.snapshot());
    }
    return aggregate;
}

test "resident projection metrics render counters" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();

    try appendResidentProjectionMetrics(&writer.writer, .{
        .text_success = 1,
        .text_fallback = 2,
        .image_success = 3,
        .image_fallback = 4,
        .audio_success = 5,
        .audio_fallback = 6,
    });
    const output = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_embed_resident_projection_text_success_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_embed_resident_projection_audio_fallback_total 6\n") != null);
}

test "graph executor metrics render counters" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();

    try appendGraphExecutorMetrics(&writer.writer, .{
        .partitions_executed = 1,
        .interpreter_fallbacks = 2,
        .host_materialized_outputs = 3,
    });
    const output = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_graph_executor_partitions_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_graph_executor_interpreter_fallbacks_total 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_graph_executor_host_materialized_outputs_total 3\n") != null);
}

test "OpenAI model listing deduplicates multi-task models" {
    var output = std.ArrayListUnmanaged(u8).empty;
    defer output.deinit(std.testing.allocator);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(std.testing.allocator);
    var count: usize = 0;

    try appendUniqueOpenAiModelEntry(
        &output,
        std.testing.allocator,
        &seen,
        &count,
        "antflydb/clipclap",
        17,
        "compatible",
    );
    try appendUniqueOpenAiModelEntry(
        &output,
        std.testing.allocator,
        &seen,
        &count,
        "antflydb/clipclap",
        17,
        "compatible",
    );

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(
        @as(usize, 1),
        std.mem.count(u8, output.items, "\"id\":\"antflydb/clipclap\""),
    );
}

fn taskMatchesModelListing(task: []const u8, model_kind: []const u8, gliner_model_type: []const u8, tasks: []const []const u8, capabilities: []const []const u8) bool {
    if (tasks.len > 0) {
        const singular_task: ?[]const u8 = if (std.mem.eql(u8, task, "embedders"))
            "embed"
        else if (std.mem.eql(u8, task, "rerankers"))
            "rerank"
        else if (std.mem.eql(u8, task, "chunkers"))
            "chunk"
        else if (std.mem.eql(u8, task, "generators"))
            "generate"
        else if (std.mem.eql(u8, task, "classifiers"))
            "classify"
        else if (std.mem.eql(u8, task, "rewriters"))
            "rewrite"
        else if (std.mem.eql(u8, task, "readers"))
            "read"
        else if (std.mem.eql(u8, task, "transcribers"))
            "transcribe"
        else if (std.mem.eql(u8, task, "extractors"))
            "extract"
        else
            null;

        if (singular_task) |expected| {
            for (tasks) |candidate| {
                if (std.mem.eql(u8, candidate, expected)) return true;
            }
        }
        return false;
    }
    if (task.len > 0 and std.mem.eql(u8, task[0 .. task.len - 1], model_kind)) return true;
    return std.mem.eql(u8, task, "extractors") and
        model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "extraction");
}

fn appendModelInfo(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    model_kind: []const u8,
    gliner_model_type: []const u8,
    capabilities: []const []const u8,
    inputs: []const []const u8,
    has_visual: bool,
    has_audio: bool,
    /// Set when the model shipped a chat template we could not parse. Without this the
    /// degradation to raw prompting is invisible to API clients.
    chat_template_failed: bool,
    /// Artifact compatibility derived for the current build.
    compatibility_level: []const u8,
) !void {
    const inferred_classification = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "classification") and !model_caps.hasCapability(capabilities, "classification");
    const inferred_relations = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "relations") and !model_caps.hasCapability(capabilities, "relations");
    const inferred_extraction = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "extraction") and !model_caps.hasCapability(capabilities, "extraction");
    const has_known_inputs = model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "text") or
        model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "image") or
        model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "audio");

    if (capabilities.len == 0 and !inferred_classification and !inferred_relations and !inferred_extraction and !has_known_inputs) {
        if (!chat_template_failed and compatibility_level.len == 0) {
            try buf.appendSlice(allocator, "{}");
            return;
        }
        try buf.append(allocator, '{');
        if (chat_template_failed) try buf.appendSlice(allocator, "\"chat_template\":false");
        if (compatibility_level.len > 0) {
            if (chat_template_failed) try buf.append(allocator, ',');
            try buf.appendSlice(allocator, "\"compatibility\":");
            try jsonEncodeString(buf, allocator, compatibility_level);
        }
        try buf.append(allocator, '}');
        return;
    }

    try buf.appendSlice(allocator, "{\"capabilities\":[");
    var cap_index: usize = 0;
    for (capabilities) |cap| {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, cap);
        cap_index += 1;
    }
    if (inferred_classification) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "classification");
        cap_index += 1;
    }
    if (inferred_relations) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "relations");
        cap_index += 1;
    }
    if (inferred_extraction) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "extraction");
        cap_index += 1;
    }
    try buf.appendSlice(allocator, "],\"inputs\":[");
    var input_index: usize = 0;
    for ([_][]const u8{ "text", "image", "audio" }) |input| {
        if (!model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, input)) continue;
        if (input_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, input);
        input_index += 1;
    }
    try buf.append(allocator, ']');
    if (chat_template_failed) try buf.appendSlice(allocator, ",\"chat_template\":false");
    if (compatibility_level.len > 0) {
        try buf.appendSlice(allocator, ",\"compatibility\":");
        try jsonEncodeString(buf, allocator, compatibility_level);
    }
    try buf.append(allocator, '}');
}

/// Wrapper that prepends a path prefix to route registrations.
/// This bridges the generated router (which emits paths like "/embed")
/// to the actual server (which serves under a configured prefix such as "/ml/v1/embed").
fn PrefixedServer(comptime prefix: []const u8, comptime Inner: type) type {
    return struct {
        inner: *Inner,

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.post(prefix ++ path, handler);
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.get(prefix ++ path, handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.put(prefix ++ path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.delete(prefix ++ path, handler);
        }
    };
}

fn isMlOnlyRoute(comptime path: []const u8) bool {
    return std.mem.eql(u8, path, "/predict") or std.mem.eql(u8, path, "/predictors");
}

fn AiPrefixedServer(comptime prefix: []const u8, comptime Inner: type) type {
    return struct {
        inner: *Inner,

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            try self.inner.post(prefix ++ path, handler);
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            try self.inner.get(prefix ++ path, handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            try self.inner.put(prefix ++ path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            try self.inner.delete(prefix ++ path, handler);
        }
    };
}

const RecordingRouteMethod = enum {
    get,
    post,
    put,
    delete,
};

const RecordingRoute = struct {
    method: RecordingRouteMethod,
    path: []u8,
};

const RecordingServer = struct {
    allocator: std.mem.Allocator,
    routes: std.ArrayListUnmanaged(RecordingRoute) = .empty,

    fn deinit(self: *@This()) void {
        for (self.routes.items) |route| self.allocator.free(route.path);
        self.routes.deinit(self.allocator);
    }

    fn append(self: *@This(), method: RecordingRouteMethod, comptime path: []const u8) !void {
        try self.routes.append(self.allocator, .{
            .method = method,
            .path = try self.allocator.dupe(u8, path),
        });
    }

    pub fn get(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.get, path);
    }

    pub fn post(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.post, path);
    }

    pub fn put(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.put, path);
    }

    pub fn delete(self: *@This(), comptime path: []const u8, _: httpx.Handler) !void {
        try self.append(.delete, path);
    }

    fn hasRoute(self: *const @This(), method: RecordingRouteMethod, path: []const u8) bool {
        for (self.routes.items) |route| {
            if (route.method == method and std.mem.eql(u8, route.path, path)) return true;
        }
        return false;
    }
};

/// Check if an entity label matches a schema field name.
/// Case-insensitive comparison; also matches if label contains the schema name.
fn labelMatchesSchema(label: []const u8, schema_name: []const u8) bool {
    if (label.len == 0 or schema_name.len == 0) return false;
    // Exact match (case-insensitive)
    if (std.ascii.eqlIgnoreCase(label, schema_name)) return true;
    // Label contains schema name (e.g. "B-PERSON" matches "person")
    if (label.len > schema_name.len) {
        // Check suffix after "B-", "I-", etc.
        if (label.len >= 2 and label[1] == '-') {
            if (std.ascii.eqlIgnoreCase(label[2..], schema_name)) return true;
        }
    }
    return false;
}

test "node config accepts shared scraping config" {
    const cfg = NodeConfig{
        .content_security = .{ .block_private_ips = true },
        .s3_credentials = .{ .endpoint = @constCast("s3.amazonaws.com") },
    };
    try std.testing.expectEqual(@as(?bool, true), cfg.content_security.?.block_private_ips);
    try std.testing.expectEqualStrings("s3.amazonaws.com", cfg.s3_credentials.?.endpoint.?);
}

test "generate batch preflight rejects image content without parsing media" {
    const request_json =
        \\{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image.png"}}]}]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const reason = Node.generateBatchUnsupportedReasonPreflight(parsed.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("UNSUPPORTED_MULTIMODAL", reason.code);
}

test "generate batch isolates native execution and serializes stateful GPU backends" {
    try std.testing.expectEqual(
        Node.BatchExecutionMode.isolated_parallel,
        Node.batchExecutionMode(.native),
    );
    try std.testing.expectEqual(
        Node.BatchExecutionMode.shared_serial,
        Node.batchExecutionMode(.metal),
    );
    try std.testing.expectEqual(
        Node.BatchExecutionMode.shared_serial,
        Node.batchExecutionMode(.cuda),
    );
}

test "generate batch queue units sum pending generation work" {
    const alloc = std.testing.allocator;
    var node = try Node.init(alloc, .{});
    defer node.deinit();

    const request_json =
        \\{"mode":"sync","requests":[
        \\{"custom_id":"a","body":{"model":"m","max_tokens":512,"messages":[{"role":"user","content":"short"}]}},
        \\{"custom_id":"b","body":{"model":"m","max_tokens":256,"messages":[{"role":"user","content":"this prompt is deliberately longer than one prompt queue block so the estimate is weighted"}]}},
        \\{"custom_id":"c","body":{"model":"m","max_tokens":256,"messages":[{"role":"user","content":"already rejected"}]}}
        \\]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateBatchRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var owned_messages = try alloc.alloc(Node.OwnedGenerateMessages, parsed.value.requests.len);
    defer {
        for (owned_messages) |*owned| owned.deinit();
        alloc.free(owned_messages);
    }
    for (parsed.value.requests, 0..) |item, idx| {
        owned_messages[idx] = try node.parseGenerateMessages(alloc, item.body);
    }
    const pending = [_]bool{ true, true, false };

    const expected =
        @as(usize, 1) +
        node.estimateGenerateQueueUnits(owned_messages[0].messages, 512) +
        node.estimateGenerateQueueUnits(owned_messages[1].messages, 256);
    try std.testing.expectEqual(expected, node.estimateGenerateBatchQueueUnits(parsed.value.requests, owned_messages, pending[0..]));
}

test "generate batch capacity errors include actionable retry metadata" {
    const busy_load = Node.batchModelLoadError(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy_load.code);
    try std.testing.expectEqual(true, busy_load.retryable);
    try std.testing.expectEqual(
        @as(?i64, transient_capacity_retry_after_ms),
        busy_load.retry_after_ms,
    );

    const busy_admission = Node.batchAdmissionError(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy_admission.code);
    try std.testing.expectEqual(true, busy_admission.retryable);
    try std.testing.expectEqual(
        @as(?i64, transient_capacity_retry_after_ms),
        busy_admission.retry_after_ms,
    );

    const permanent = Node.batchAdmissionError(error.ResourceLimitExceeded);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_LIMIT", permanent.code);
    try std.testing.expectEqual(false, permanent.retryable);
    try std.testing.expectEqual(@as(?i64, null), permanent.retry_after_ms);
}

test "read batch downloaded byte accounting enforces aggregate cap" {
    const item = scraping.DownloadedContent{
        .content_type = @constCast("image/png"),
        .data = @constCast("12345"),
    };
    try std.testing.expectEqual(@as(usize, 14), try addReadBatchDownloadedBytes(0, item, 14));
    try std.testing.expectError(error.ReadBatchTooLarge, addReadBatchDownloadedBytes(10, item, 14));
}

test "read max tokens preserves omission and rejects unsafe signed values" {
    try std.testing.expectEqual(@as(?usize, null), try validateReadMaxTokens(null));
    try std.testing.expectEqual(@as(?usize, 1), try validateReadMaxTokens(1));
    try std.testing.expectEqual(@as(?usize, max_read_tokens), try validateReadMaxTokens(@intCast(max_read_tokens)));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(-1));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(0));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(@intCast(max_read_tokens + 1)));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(std.math.maxInt(i64)));
}

test "read prompt treats empty public values as an omitted OCR prompt" {
    try std.testing.expectEqual(@as(?[]const u8, null), normalizeReadPrompt(null));
    try std.testing.expectEqual(@as(?[]const u8, null), normalizeReadPrompt(""));
    try std.testing.expectEqual(@as(?[]const u8, null), normalizeReadPrompt(" \t\n"));
    try std.testing.expectEqualStrings("<OCR>", normalizeReadPrompt("<OCR>").?);
}

test "read queue units scale with image batch and decode length" {
    try std.testing.expectEqual(@as(usize, 1), estimateReadQueueUnits(1, null));
    try std.testing.expectEqual(@as(usize, 4), estimateReadQueueUnits(4, null));
    try std.testing.expectEqual(@as(usize, 8), estimateReadQueueUnits(4, default_read_queue_max_tokens + 1));
    try std.testing.expectEqual(@as(usize, 16), estimateReadQueueUnits(4, max_read_tokens));
}

test "fail-fast embedding capacity response exposes retry contract" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .GET, "/ai/v1/embeddings");
    defer request.deinit();

    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try embedDenseInputFailureResponse(
        &ctx,
        error.ResourceTemporarilyUnavailable,
    );
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings(
        transient_capacity_retry_after_seconds,
        response.header("Retry-After").?,
    );

    const RetryBody = struct {
        @"error": []const u8,
        message: []const u8,
        reason: []const u8,
        retryable: bool,
        retry_after_ms: i64,
    };
    var parsed = try std.json.parseFromSlice(
        RetryBody,
        allocator,
        response.body.?,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", parsed.value.@"error");
    try std.testing.expectEqualStrings(
        "insufficient inference capacity is currently available",
        parsed.value.message,
    );
    try std.testing.expectEqualStrings("inference_capacity", parsed.value.reason);
    try std.testing.expect(parsed.value.retryable);
    try std.testing.expectEqual(
        @as(i64, transient_capacity_retry_after_ms),
        parsed.value.retry_after_ms,
    );
}

test "changing model publication returns an explicit retry contract" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/transcribe");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try modelLoadFailureResponse(&ctx, error.ModelArtifactsChanging);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings(
        transient_capacity_retry_after_seconds,
        response.header("Retry-After").?,
    );
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "MODEL_ARTIFACTS_CHANGING") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"retryable\":true") != null);
}

test "registerRoutesOn prefixes embed aliases and metrics route" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();
    defer {
        active_node = null;
        active_models_dir = null;
    }

    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerRoutesOn(public_api_prefix, &server);

    try std.testing.expect(server.hasRoute(.post, public_api_prefix ++ "/embed"));
    try std.testing.expect(server.hasRoute(.post, public_api_prefix ++ "/embeddings"));
    try std.testing.expect(server.hasRoute(.post, public_api_prefix ++ "/generate/batch"));
    try std.testing.expect(server.hasRoute(.get, public_api_prefix ++ "/models"));
    try std.testing.expect(server.hasRoute(.get, public_api_prefix ++ "/metrics"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/healthz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/readyz"));
}

test "node attachIo wires model session manager" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();

    node.attachIo(std.testing.io);

    try std.testing.expect(node.session_manager.io != null);
    try std.testing.expect(node.model_manager.session_manager.io != null);
}

test "canonical relation schemas preserve endpoint label constraints" {
    const schemas = [_]extraction_api.ExtractionRelationSchema{
        .{ .type = "works_at", .source = "person", .target = "organization" },
        .{ .type = "located_in", .source = "organization" },
        .{ .type = "knows" },
    };
    const labels = try canonicalRelationLabels(std.testing.allocator, &schemas);
    defer freeStringSlice(std.testing.allocator, labels);

    try std.testing.expectEqual(@as(usize, 3), labels.len);
    try std.testing.expectEqualStrings("person::works_at::organization", labels[0]);
    try std.testing.expectEqualStrings("organization::located_in", labels[1]);
    try std.testing.expectEqualStrings("knows", labels[2]);

    try std.testing.expectError(
        error.InvalidRelationSchema,
        canonicalRelationLabels(std.testing.allocator, &.{.{ .type = "works_at", .target = "organization" }}),
    );
    try std.testing.expectError(
        error.InvalidRelationSchema,
        canonicalRelationLabels(std.testing.allocator, &.{.{ .type = "person::works_at" }}),
    );
}

test "canonical structure schemas preserve field types and choices" {
    var parsed = try std.json.parseFromSlice(
        extraction_api.ExtractionSchema,
        std.testing.allocator,
        \\{"structures":{"person":{"fields":{"name":"string","tags":{"type":"array"},"role":{"type":"str","enum":["engineer","manager"]}}}}}
    ,
        .{ .allocate = .alloc_always },
    );
    defer parsed.deinit();
    const schemas = try parseCanonicalStructureSchemas(std.testing.allocator, parsed.value);
    defer {
        for (schemas) |*schema| schema.deinit(std.testing.allocator);
        std.testing.allocator.free(schemas);
    }

    try std.testing.expectEqual(@as(usize, 1), schemas.len);
    try std.testing.expectEqualStrings("person", schemas[0].name);
    try std.testing.expectEqual(@as(usize, 3), schemas[0].fields.len);
    for (schemas[0].fields) |field| {
        if (std.mem.eql(u8, field.name, "name")) {
            try std.testing.expectEqual(extraction_mod.FieldType.str, field.field_type);
        } else if (std.mem.eql(u8, field.name, "tags")) {
            try std.testing.expectEqual(extraction_mod.FieldType.list, field.field_type);
        } else if (std.mem.eql(u8, field.name, "role")) {
            try std.testing.expectEqual(@as(usize, 2), field.choices.len);
            try std.testing.expectEqualStrings("engineer", field.choices[0]);
            try std.testing.expectEqualStrings("manager", field.choices[1]);
        } else return error.UnexpectedField;
    }
}

test "canonical structure schemas reject invalid enum descriptors without leaking" {
    var parsed = try std.json.parseFromSlice(
        extraction_api.ExtractionSchema,
        std.testing.allocator,
        \\{"structures":{"person":{"fields":{"role":{"enum":["only-one"]}}}}}
    ,
        .{ .allocate = .alloc_always },
    );
    defer parsed.deinit();
    try std.testing.expectError(
        error.InvalidStructureField,
        parseCanonicalStructureSchemas(std.testing.allocator, parsed.value),
    );
}

test "canonical structured extraction rejects empty and mixed input batches" {
    try std.testing.expectError(error.UnsupportedInput, validateExtractionInputKinds(0, 0));
    try std.testing.expectError(error.UnsupportedInput, validateExtractionInputKinds(1, 1));
    try validateExtractionInputKinds(2, 0);
    try validateExtractionInputKinds(0, 2);
}

test "component session pool adapter retains explicit resource-managed backend policy" {
    const session_manager = backends_mod.SessionManager.init(std.testing.allocator);
    var manager = model_manager_mod.ModelManager.init(std.testing.allocator, session_manager);
    defer manager.deinit();

    var loader = model_manager_mod.ModelManager.ComponentLoader{ .manager = &manager };
    loader.allowed_backends[0] = .native;
    loader.allowed_backends[1] = .metal;
    loader.allowed_backend_count = 2;
    std.crypto.hash.sha2.Sha256.hash(
        "/models/component.onnx",
        &loader.component_path_digests[0],
        .{},
    );
    loader.component_path_count = 1;
    const strict = try loader.restrictToBackend(.metal);
    try std.testing.expectEqualSlices(
        backends_mod.BackendType,
        &.{.metal},
        strict.preferredBackends(),
    );
    var pool_loader = try loader.sessionPoolLoader();
    defer pool_loader.deinit();
    loader = undefined;
    try std.testing.expectError(
        error.UnvalidatedModelComponent,
        pool_loader.load("/models/substituted.onnx"),
    );
}

test "registerAiRoutesOn excludes Traditional ML predictor routes" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();

    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerAiRoutesOn(ai_api_prefix, &server);

    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/embed"));
    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/embeddings"));
    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/generate/batch"));
    try std.testing.expect(server.hasRoute(.get, ai_api_prefix ++ "/models"));
    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/recognize"));
    try std.testing.expect(!server.hasRoute(.post, ai_api_prefix ++ "/predict"));
    try std.testing.expect(!server.hasRoute(.get, ai_api_prefix ++ "/predictors"));
}

test "root operational routes stay outside inference API prefix" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try Node.registerRootOperationalRoutes(&server);

    try std.testing.expect(server.hasRoute(.get, "/healthz"));
    try std.testing.expect(server.hasRoute(.get, "/readyz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/healthz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/readyz"));
}

test "registerRoutesOn supports alternate prefixes through the shared router" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();
    defer {
        active_node = null;
        active_models_dir = null;
    }

    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerRoutesOn("/custom/v9", &server);

    try std.testing.expect(server.hasRoute(.post, "/custom/v9/embed"));
    try std.testing.expect(server.hasRoute(.post, "/custom/v9/embeddings"));
    try std.testing.expect(server.hasRoute(.get, "/custom/v9/models"));
    try std.testing.expect(server.hasRoute(.get, "/custom/v9/metrics"));
}

test "budget overrides apply selectively" {
    const defaults: runtime.tier.memory.Limits = .{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 200,
        .combined_limit_bytes = 300,
        .kv_limit_bytes = 400,
        .scratch_limit_bytes = 500,
    };
    const applied = (BudgetOverrides{
        .host_limit_bytes = 150,
        .combined_limit_bytes = 350,
        .scratch_limit_bytes = 600,
    }).apply(defaults);
    try std.testing.expectEqual(@as(usize, 150), applied.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 200), applied.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 350), applied.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 400), applied.kv_limit_bytes);
    try std.testing.expectEqual(@as(usize, 600), applied.scratch_limit_bytes);
}

test "taskMatchesModelListing exposes extraction-capable models only as extractors" {
    try std.testing.expect(taskMatchesModelListing("extractors", "extractor", "", &.{}, &.{}));
    try std.testing.expect(taskMatchesModelListing("extractors", "recognizer", "", &.{}, &.{"extraction"}));
    try std.testing.expect(taskMatchesModelListing("extractors", "reader", "", &.{}, &.{"extraction"}));
    try std.testing.expect(taskMatchesModelListing("extractors", "recognizer", "gliner2", &.{}, &.{"labels"}));
    try std.testing.expect(!taskMatchesModelListing("classifiers", "recognizer", "gliner2", &.{}, &.{"classification"}));
}

test "taskMatchesModelListing prefers explicit tasks when present" {
    try std.testing.expect(taskMatchesModelListing("generators", "generator", "", &.{"generate"}, &.{}));
    try std.testing.expect(taskMatchesModelListing("extractors", "generator", "", &.{"extract"}, &.{}));
    try std.testing.expect(!taskMatchesModelListing("extractors", "generator", "", &.{"generate"}, &.{"extraction"}));
}

test "generate backend selection keeps compiled mode explicit" {
    const eager_webgpu = parseGenerateBackendSelection(.webgpu, null, null);
    if (build_options.enable_wasm and build_options.enable_webgpu) {
        const eager = try eager_webgpu;
        try std.testing.expectEqual(native_backend_choice.Choice.webgpu, eager.native_choice);
        try std.testing.expectEqual(@as(?ops.BackendKind, null), eager.compiled_partition_backend);
        try std.testing.expect(!eager.graph_mode_requested);

        const compiled = try parseGenerateBackendSelection(.webgpu, "compiled", null);
        try std.testing.expectEqual(native_backend_choice.Choice.webgpu, compiled.native_choice);
        try std.testing.expectEqual(@as(?ops.BackendKind, .webgpu), compiled.compiled_partition_backend);
        try std.testing.expectEqual(graph_mod.compiled_backend.AttachmentTarget.partitioned, compiled.compiled_attachment_target);
        try std.testing.expect(compiled.graph_mode_requested);
    } else {
        try std.testing.expectError(error.BackendUnavailable, eager_webgpu);
    }

    const auto_compiled = try parseGenerateBackendSelection(null, "compiled", null);
    try std.testing.expectEqual(native_backend_choice.Choice.auto, auto_compiled.native_choice);
    try std.testing.expectEqual(@as(?ops.BackendKind, null), auto_compiled.compiled_partition_backend);
    try std.testing.expect(auto_compiled.graph_mode_requested);

    const auto_default = try parseGenerateBackendSelection(null, null, null);
    try std.testing.expectEqual(build_options.enable_metal, shouldAutoUseMetalWholeModelGenerate(.metal, true, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.native, true, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, false, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, true, true, auto_default));

    if (build_options.enable_metal) {
        const metal_eager = try parseGenerateBackendSelection(.metal, "eager", null);
        try std.testing.expect(metal_eager.eager_mode_requested);
        try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, true, false, metal_eager));
    } else {
        try std.testing.expectError(
            error.BackendUnavailable,
            parseGenerateBackendSelection(.metal, "eager", null),
        );
    }

    try std.testing.expectError(error.InvalidGenerateMode, parseGenerateBackendSelection(null, "graph", null));
    try std.testing.expectError(error.InvalidCompiledTarget, parseGenerateBackendSelection(null, "compiled", "full"));
}

test "singleBackendPreference is strict" {
    try std.testing.expectEqualSlices(backends_mod.BackendType, &.{.metal}, singleBackendPreference(.metal));
}

test "download remote content accepts data uri" {
    const alloc = std.testing.allocator;
    const node = Node{
        .config = .{},
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
        .extraction_reader_resolver = undefined,
        .tabular_registry = undefined,
        .embed_cache = undefined,
        .metrics = undefined,
        .request_queue = undefined,
    };
    var downloaded = try downloadRemoteContent(&node, alloc, "data:text/plain;base64,aGVsbG8=");
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain", downloaded.content_type);
    try std.testing.expectEqualStrings("hello", downloaded.data);
}

test "download remote content blocks private ip urls when configured" {
    const alloc = std.testing.allocator;
    const node = Node{
        .config = .{ .content_security = .{ .block_private_ips = true } },
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
        .extraction_reader_resolver = undefined,
        .tabular_registry = undefined,
        .embed_cache = undefined,
        .metrics = undefined,
        .request_queue = undefined,
    };
    try std.testing.expectError(error.PrivateIpBlocked, downloadRemoteContent(&node, alloc, "http://127.0.0.1/test.png"));
}

test "download remote content blocks hosts outside allowlist" {
    const alloc = std.testing.allocator;
    const allowed_hosts = [_][]u8{@constCast("cdn.example.com")};
    const node = Node{
        .config = .{ .content_security = .{ .allowed_hosts = &allowed_hosts } },
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
        .extraction_reader_resolver = undefined,
        .tabular_registry = undefined,
        .embed_cache = undefined,
        .metrics = undefined,
        .request_queue = undefined,
    };
    try std.testing.expectError(error.HostNotAllowed, downloadRemoteContent(&node, alloc, "https://example.com/a.png"));
}

test "chunk request requires input in generated schema" {
    try std.testing.expectError(
        error.MissingField,
        std.json.parseFromSlice(api.ChunkRequest, std.testing.allocator, "{}", .{ .ignore_unknown_fields = true }),
    );
}

test "chunk request input parser rejects invalid content parts" {
    const cases = [_]struct {
        name: []const u8,
        input_json: []const u8,
        expected_error: anyerror,
        expected_message: []const u8,
    }{
        .{
            .name = "empty text input",
            .input_json = "\"\"",
            .expected_error = error.ChunkInputRequired,
            .expected_message = "missing 'input' field",
        },
        .{
            .name = "missing content part type",
            .input_json = "{\"text\":\"hello\"}",
            .expected_error = error.UnsupportedChunkInputContentPartType,
            .expected_message = "input content part type must be 'text' or 'media'",
        },
        .{
            .name = "unsupported content part type",
            .input_json = "{\"type\":\"image_url\",\"image_url\":{\"url\":\"https://example.test/a.png\"}}",
            .expected_error = error.UnsupportedChunkInputContentPartType,
            .expected_message = "input content part type must be 'text' or 'media'",
        },
        .{
            .name = "missing text",
            .input_json = "{\"type\":\"text\"}",
            .expected_error = error.ChunkTextContentPartMissingText,
            .expected_message = "text content part missing 'text' field",
        },
        .{
            .name = "empty text",
            .input_json = "{\"type\":\"text\",\"text\":\"\"}",
            .expected_error = error.ChunkTextContentPartMissingText,
            .expected_message = "text content part missing 'text' field",
        },
        .{
            .name = "missing media data",
            .input_json = "{\"type\":\"media\",\"mime_type\":\"audio/wav\"}",
            .expected_error = error.ChunkMediaContentPartMissingData,
            .expected_message = "media content part missing 'data' field",
        },
        .{
            .name = "empty media data",
            .input_json = "{\"type\":\"media\",\"data\":\"\",\"mime_type\":\"audio/wav\"}",
            .expected_error = error.ChunkMediaContentPartMissingData,
            .expected_message = "media content part missing 'data' field",
        },
        .{
            .name = "empty media data uri payload",
            .input_json = "{\"type\":\"media\",\"data\":\"data:audio/wav;base64,\",\"mime_type\":\"audio/wav\"}",
            .expected_error = error.ChunkMediaContentPartMissingData,
            .expected_message = "media content part missing 'data' field",
        },
        .{
            .name = "missing media mime",
            .input_json = "{\"type\":\"media\",\"data\":\"AA==\"}",
            .expected_error = error.ChunkMediaContentPartMissingMimeType,
            .expected_message = "media content part missing 'mime_type' field",
        },
        .{
            .name = "blank media mime",
            .input_json = "{\"type\":\"media\",\"data\":\"AA==\",\"mime_type\":\"  \"}",
            .expected_error = error.ChunkMediaContentPartMissingMimeType,
            .expected_message = "media content part missing 'mime_type' field",
        },
    };

    for (cases) |case| {
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, case.input_json, .{});
        defer parsed.deinit();

        try std.testing.expectError(case.expected_error, parseChunkRequestInput(std.testing.allocator, parsed.value));
        try std.testing.expectEqualStrings(case.expected_message, chunkInputParseErrorMessage(case.expected_error));
    }
}

test "chunk request input parser accepts valid text and media" {
    var text_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"type\":\"text\",\"text\":\"hello\"}", .{});
    defer text_parsed.deinit();
    const text_input = try parseChunkRequestInput(std.testing.allocator, text_parsed.value);
    defer deinitChunkRequestInput(std.testing.allocator, text_input);
    try std.testing.expectEqualStrings("hello", text_input.text);

    var media_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"type\":\"media\",\"data\":\"aGVsbG8=\",\"mime_type\":\"audio/wav\"}", .{});
    defer media_parsed.deinit();
    const media_input = try parseChunkRequestInput(std.testing.allocator, media_parsed.value);
    defer deinitChunkRequestInput(std.testing.allocator, media_input);
    try std.testing.expectEqualStrings("audio/wav", media_input.binary.mime_type);
    try std.testing.expectEqualStrings("hello", media_input.binary.data);
}

fn dirContainsModel(path: []const u8) bool {
    var buf: [4096]u8 = undefined;
    inline for ([_][]const u8{ "/tokenizer.json", "/config.json", "/genai_config.json", "/model.onnx", "/model_i8.onnx", "/onnx/model.onnx" }) |suffix| {
        if (path.len + suffix.len < buf.len) {
            @memcpy(buf[0..path.len], path);
            @memcpy(buf[path.len .. path.len + suffix.len], suffix);
            buf[path.len + suffix.len] = 0;
            if (c_file.fileExistsZ(buf[0 .. path.len + suffix.len :0])) return true;
        }
    }

    if (!build_options.link_libc) {
        var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, path, .{ .iterate = true }) catch return false;
        defer dir.close(std.Options.debug_io);
        var iter = dir.iterate();
        while (iter.next(std.Options.debug_io) catch null) |entry| {
            const name = entry.name;
            if (name.len > 5 and std.mem.endsWith(u8, name, ".gguf")) return true;
        }
        return false;
    }

    const path_z = std.heap.page_allocator.dupeZ(u8, path) catch return false;
    defer std.heap.page_allocator.free(path_z);
    const dir = c_file.c.opendir(path_z.ptr);
    if (dir == null) return false;
    defer _ = c_file.c.closedir(dir);

    while (c_file.c.readdir(dir)) |entry| {
        const name_z: [*:0]const u8 = @ptrCast(&entry.*.d_name);
        const name = std.mem.span(name_z);
        if (name.len > 5 and std.mem.endsWith(u8, name, ".gguf")) return true;
    }

    return false;
}

fn dirExists(path: []const u8) bool {
    var buf: [4096]u8 = undefined;
    if (path.len >= buf.len) return false;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    return c_file.fileExistsZ(buf[0..path.len :0]);
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn jsonBytesResponse(ctx: *httpx.Context, body: []const u8) !httpx.Response {
    try ctx.setHeader("Content-Type", "application/json");
    _ = ctx.response.body(body);
    return ctx.response.build();
}

fn parseChunkRequestInput(allocator: std.mem.Allocator, input: std.json.Value) !lib_chunker.Input {
    return switch (input) {
        .string => |s| blk: {
            if (s.len == 0) return error.ChunkInputRequired;
            break :blk .{ .text = s };
        },
        .object => |obj| blk: {
            const type_val = obj.get("type") orelse return error.UnsupportedChunkInputContentPartType;
            if (type_val != .string) return error.ChunkContentPartTypeMustBeString;
            if (std.mem.eql(u8, type_val.string, "text")) {
                const text_val = obj.get("text") orelse return error.ChunkTextContentPartMissingText;
                if (text_val != .string or text_val.string.len == 0) return error.ChunkTextContentPartMissingText;
                break :blk .{ .text = text_val.string };
            }
            if (!std.mem.eql(u8, type_val.string, "media")) return error.UnsupportedChunkInputContentPartType;

            const data_val = obj.get("data") orelse return error.ChunkMediaContentPartMissingData;
            if (data_val != .string) return error.ChunkMediaDataMustBeBase64String;
            if (data_val.string.len == 0) return error.ChunkMediaContentPartMissingData;
            const mime_val = obj.get("mime_type") orelse return error.ChunkMediaContentPartMissingMimeType;
            if (mime_val != .string) return error.ChunkMediaMimeTypeMustBeString;
            if (std.mem.trim(u8, mime_val.string, &std.ascii.whitespace).len == 0) return error.ChunkMediaContentPartMissingMimeType;

            const decoded_payload = decodeMediaData(allocator, data_val.string) catch return error.ChunkInvalidBase64Data;
            const decoded = decoded_payload.data;
            errdefer allocator.free(decoded);
            if (decoded.len == 0) return error.ChunkMediaContentPartMissingData;
            if (!mediaMimeMatches(mime_val.string, decoded_payload.mime_type)) return error.ChunkMediaDataMimeTypeMismatch;
            break :blk .{ .binary = .{
                .mime_type = mime_val.string,
                .data = decoded,
            } };
        },
        else => error.ChunkInputMustBeStringOrContentPartObject,
    };
}

fn deinitChunkRequestInput(allocator: std.mem.Allocator, input: lib_chunker.Input) void {
    switch (input) {
        .binary => |binary| allocator.free(binary.data),
        .text => {},
    }
}

fn chunkInputParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.ChunkInputRequired => "missing 'input' field",
        error.UnsupportedChunkInputContentPartType => "input content part type must be 'text' or 'media'",
        error.ChunkContentPartTypeMustBeString => "content part 'type' must be a string",
        error.ChunkTextContentPartMissingText => "text content part missing 'text' field",
        error.ChunkMediaContentPartMissingData => "media content part missing 'data' field",
        error.ChunkMediaDataMustBeBase64String => "media 'data' must be a base64 string",
        error.ChunkMediaContentPartMissingMimeType => "media content part missing 'mime_type' field",
        error.ChunkMediaMimeTypeMustBeString => "media 'mime_type' must be a string",
        error.ChunkInvalidBase64Data => "invalid base64 data",
        error.ChunkMediaDataMimeTypeMismatch => "media data URI mime_type does not match content part mime_type",
        error.ChunkInputMustBeStringOrContentPartObject => "'input' must be a string or content part object",
        else => "invalid chunk input",
    };
}

fn chunkRequestParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.MissingField => "missing required 'input' field",
        error.SyntaxError => "request body must be valid JSON",
        error.UnexpectedToken => "request body does not match chunk request schema",
        else => "invalid chunk request",
    };
}

fn validateEmbeddingEncodingFormat(encoding_format: ?[]const u8) !void {
    const value = encoding_format orelse "float";
    if (!std.mem.eql(u8, value, "float")) return error.UnsupportedEncodingFormat;
}

fn parseRequestedEmbeddingDimensions(dimensions: ?i64) !?usize {
    const value = dimensions orelse return null;
    if (value <= 0) return error.InvalidEmbeddingDimensions;
    return @intCast(value);
}

const ParsedEmbedRequest = struct {
    model: []const u8,
    input: std.json.Value,
    encoding_format: ?[]const u8,
    dimensions: ?i64,
    task_type: ?EmbeddingTaskType,
    error_policy: EmbedErrorPolicy = .fail_fast,
};

const EmbedErrorPolicy = enum {
    fail_fast,
    per_item,
};

fn parseEmbedErrorPolicy(value: []const u8) ?EmbedErrorPolicy {
    if (std.mem.eql(u8, value, "fail_fast")) return .fail_fast;
    if (std.mem.eql(u8, value, "per_item")) return .per_item;
    return null;
}

const EmbeddingTaskType = enum {
    RETRIEVAL_QUERY,
    RETRIEVAL_DOCUMENT,
    QUESTION_ANSWERING,
    FACT_VERIFICATION,
    CODE_RETRIEVAL_QUERY,
    CLASSIFICATION,
    CLUSTERING,
    SEMANTIC_SIMILARITY,

    fn usesQueryPrefix(self: EmbeddingTaskType) bool {
        return switch (self) {
            .RETRIEVAL_QUERY,
            .QUESTION_ANSWERING,
            .FACT_VERIFICATION,
            .CODE_RETRIEVAL_QUERY,
            => true,
            else => false,
        };
    }

    fn usesDocumentPrefix(self: EmbeddingTaskType) bool {
        return switch (self) {
            .RETRIEVAL_DOCUMENT => true,
            else => false,
        };
    }
};

fn parseEmbeddingTaskType(value: []const u8) ?EmbeddingTaskType {
    if (std.mem.eql(u8, value, "RETRIEVAL_QUERY")) return .RETRIEVAL_QUERY;
    if (std.mem.eql(u8, value, "RETRIEVAL_DOCUMENT")) return .RETRIEVAL_DOCUMENT;
    if (std.mem.eql(u8, value, "QUESTION_ANSWERING")) return .QUESTION_ANSWERING;
    if (std.mem.eql(u8, value, "FACT_VERIFICATION")) return .FACT_VERIFICATION;
    if (std.mem.eql(u8, value, "CODE_RETRIEVAL_QUERY")) return .CODE_RETRIEVAL_QUERY;
    if (std.mem.eql(u8, value, "CLASSIFICATION")) return .CLASSIFICATION;
    if (std.mem.eql(u8, value, "CLUSTERING")) return .CLUSTERING;
    if (std.mem.eql(u8, value, "SEMANTIC_SIMILARITY")) return .SEMANTIC_SIMILARITY;
    return null;
}

fn parseLegacyEmbeddingInputType(value: []const u8) ?EmbeddingTaskType {
    if (std.mem.eql(u8, value, "search_query") or std.mem.eql(u8, value, "query")) return .RETRIEVAL_QUERY;
    if (std.mem.eql(u8, value, "search_document") or std.mem.eql(u8, value, "document")) return .RETRIEVAL_DOCUMENT;
    if (std.mem.eql(u8, value, "classification")) return .CLASSIFICATION;
    if (std.mem.eql(u8, value, "clustering")) return .CLUSTERING;
    return null;
}

const ParsedTextEmbedInput = struct {
    index: usize,
    text: []const u8,
};

const ParsedBinaryEmbedInput = struct {
    index: usize,
    bytes: []u8,
    mime_type: ?[]const u8 = null,
};

const ParsedDenseEmbedInputs = struct {
    texts: std.ArrayListUnmanaged(ParsedTextEmbedInput) = .empty,
    images: std.ArrayListUnmanaged(ParsedBinaryEmbedInput) = .empty,
    audio: std.ArrayListUnmanaged(ParsedBinaryEmbedInput) = .empty,
    parse_errors: std.ArrayListUnmanaged(EmbedItemError) = .empty,
    total_count: usize = 0,

    fn deinit(self: *ParsedDenseEmbedInputs, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        for (self.images.items) |item| allocator.free(item.bytes);
        self.images.deinit(allocator);
        for (self.audio.items) |item| allocator.free(item.bytes);
        self.audio.deinit(allocator);
        self.parse_errors.deinit(allocator);
    }
};

fn parseEmbedRequest(body: std.json.Value) !ParsedEmbedRequest {
    if (body != .object) return error.RequestBodyMustBeObject;
    const obj = body.object;

    const model_value = obj.get("model") orelse return error.ModelRequired;
    if (model_value != .string or model_value.string.len == 0) return error.ModelRequired;

    const input_value = obj.get("input") orelse return error.InputRequired;

    const encoding_format: ?[]const u8 = if (obj.get("encoding_format")) |value| blk: {
        if (value != .string) return error.EncodingFormatMustBeString;
        break :blk value.string;
    } else null;

    const dimensions: ?i64 = if (obj.get("dimensions")) |value| blk: {
        if (value != .integer) return error.DimensionsMustBeInteger;
        break :blk value.integer;
    } else null;

    const task_type: ?EmbeddingTaskType = if (obj.get("task_type")) |value| blk: {
        if (value != .string) return error.TaskTypeMustBeString;
        break :blk parseEmbeddingTaskType(value.string) orelse return error.UnsupportedEmbeddingTaskType;
    } else null;

    const legacy_task_type: ?EmbeddingTaskType = if (obj.get("input_type")) |value| blk: {
        if (value != .string) return error.InputTypeMustBeString;
        break :blk parseLegacyEmbeddingInputType(value.string) orelse return error.UnsupportedEmbeddingInputType;
    } else null;

    if (task_type != null and legacy_task_type != null and task_type.? != legacy_task_type.?) {
        return error.ConflictingEmbeddingTaskTypes;
    }

    const error_policy: EmbedErrorPolicy = if (obj.get("error_policy")) |value| blk: {
        if (value != .string) return error.ErrorPolicyMustBeString;
        break :blk parseEmbedErrorPolicy(value.string) orelse return error.UnsupportedEmbeddingErrorPolicy;
    } else .fail_fast;

    return .{
        .model = model_value.string,
        .input = input_value,
        .encoding_format = encoding_format,
        .dimensions = dimensions,
        .task_type = task_type orelse legacy_task_type,
        .error_policy = error_policy,
    };
}

fn embedRequestParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.RequestBodyMustBeObject => "request body must be a JSON object",
        error.ModelRequired => "model is required",
        error.InputRequired => "input is required",
        error.EncodingFormatMustBeString => "encoding_format must be a string",
        error.DimensionsMustBeInteger => "dimensions must be an integer",
        error.TaskTypeMustBeString => "task_type must be a string",
        error.UnsupportedEmbeddingTaskType => "task_type must be one of RETRIEVAL_QUERY, RETRIEVAL_DOCUMENT, QUESTION_ANSWERING, FACT_VERIFICATION, CODE_RETRIEVAL_QUERY, CLASSIFICATION, CLUSTERING, or SEMANTIC_SIMILARITY",
        error.InputTypeMustBeString => "input_type must be a string",
        error.UnsupportedEmbeddingInputType => "input_type must be a legacy alias for a supported embedding task type",
        error.ConflictingEmbeddingTaskTypes => "task_type and input_type specify different embedding task types",
        error.ErrorPolicyMustBeString => "error_policy must be a string",
        error.UnsupportedEmbeddingErrorPolicy => "error_policy must be one of fail_fast or per_item",
        else => "invalid embedding request",
    };
}

fn isJinaV5EmbeddingManifest(manifest: *const manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "jina_embeddings_v5") or
        (manifest.pooling == .last and
            std.mem.eql(u8, manifest.embedding_text_prefix, "Document: "));
}

fn applyDenseEmbeddingRequestOptions(
    pipeline: *embedding_mod.EmbeddingPipeline,
    manifest: *const manifest_mod.ModelManifest,
    request: ParsedEmbedRequest,
) !void {
    if (!isJinaV5EmbeddingManifest(manifest)) return;

    const task_type = request.task_type orelse EmbeddingTaskType.RETRIEVAL_DOCUMENT;
    if (task_type.usesQueryPrefix()) {
        pipeline.config.text_prefix = "Query: ";
    } else if (task_type.usesDocumentPrefix()) {
        pipeline.config.text_prefix = "Document: ";
    } else {
        return error.UnsupportedEmbeddingTaskType;
    }
}

fn embedRequestOptionErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.UnsupportedEmbeddingTaskType => "task_type must be a query/document retrieval task for this embedding model",
        else => "invalid embedding options",
    };
}

fn parseSparseEmbedInputs(
    allocator: std.mem.Allocator,
    input: std.json.Value,
) ![]const []const u8 {
    var texts = std.ArrayListUnmanaged([]const u8).empty;
    errdefer texts.deinit(allocator);

    switch (input) {
        .string => |value| try texts.append(allocator, value),
        .array => |arr| {
            for (arr.items) |item| {
                if (item != .string) return error.SparseModelsRequireTextInput;
                try texts.append(allocator, item.string);
            }
        },
        else => return error.SparseModelsRequireTextInput,
    }

    return try texts.toOwnedSlice(allocator);
}

fn parseDenseEmbedInputs(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
) !ParsedDenseEmbedInputs {
    var parsed: ParsedDenseEmbedInputs = .{};
    errdefer parsed.deinit(allocator);

    switch (input) {
        .string => |value| {
            if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
            try parsed.texts.append(allocator, .{ .index = 0, .text = value });
            parsed.total_count = 1;
        },
        .array => |arr| {
            if (arr.items.len == 0) return parsed;

            for (arr.items, 0..) |item, index| {
                try appendDenseEmbedInput(self, allocator, manifest, &parsed, item, index);
            }

            parsed.total_count = arr.items.len;
        },
        else => return error.InputMustBeStringOrArrayOfStringsOrContentParts,
    }

    return parsed;
}

fn parseDenseEmbedInputsPerItem(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
) !ParsedDenseEmbedInputs {
    var parsed: ParsedDenseEmbedInputs = .{};
    errdefer parsed.deinit(allocator);

    switch (input) {
        .string => |value| {
            parsed.total_count = 1;
            appendDenseEmbedInput(self, allocator, manifest, &parsed, .{ .string = value }, 0) catch |err| {
                try parsed.parse_errors.append(allocator, embedInputItemFailure(0, err));
            };
        },
        .array => |arr| {
            parsed.total_count = arr.items.len;
            for (arr.items, 0..) |item, index| {
                appendDenseEmbedInput(self, allocator, manifest, &parsed, item, index) catch |err| {
                    try parsed.parse_errors.append(allocator, embedInputItemFailure(index, err));
                };
            }
        },
        else => return error.InputMustBeStringOrArrayOfStringsOrContentParts,
    }

    return parsed;
}

fn appendDenseEmbedInput(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parsed: *ParsedDenseEmbedInputs,
    item: std.json.Value,
    index: usize,
) !void {
    if (item == .string) {
        if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
        try parsed.texts.append(allocator, .{ .index = index, .text = item.string });
        return;
    }

    if (item != .object) return error.InputMustBeStringOrArrayOfStringsOrContentParts;

    const obj = item.object;
    const type_value = obj.get("type") orelse return error.ContentPartTypeRequired;
    if (type_value != .string) return error.ContentPartTypeMustBeString;
    const part_type = type_value.string;

    if (std.mem.eql(u8, part_type, "text")) {
        if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
        const text_value = obj.get("text") orelse return error.TextContentPartMissingText;
        if (text_value != .string) return error.TextContentPartMissingText;
        try parsed.texts.append(allocator, .{ .index = index, .text = text_value.string });
        return;
    }

    if (std.mem.eql(u8, part_type, "image_url")) {
        if (!model_caps.modelAcceptsInput(manifest, "image")) return error.ModelDoesNotSupportImageInput;
        const image_url = obj.get("image_url") orelse return error.ImageUrlContentPartMissingImageUrl;
        const url = switch (image_url) {
            .string => image_url.string,
            .object => blk: {
                const url_value = image_url.object.get("url") orelse return error.ImageUrlContentPartMissingUrl;
                if (url_value != .string) return error.ImageUrlContentPartMissingUrl;
                break :blk url_value.string;
            },
            else => return error.ImageUrlContentPartMissingUrl,
        };

        const downloaded = downloadRemoteContent(self, allocator, url) catch return error.ImageUrlDownloadFailed;
        errdefer allocator.free(downloaded.data);
        defer allocator.free(downloaded.content_type);

        if (!std.mem.startsWith(u8, downloaded.content_type, "image/")) return error.ImageUrlMustResolveToImage;
        try parsed.images.append(allocator, .{
            .index = index,
            .bytes = downloaded.data,
            .mime_type = null,
        });
        return;
    }

    if (std.mem.eql(u8, part_type, "media")) {
        const data_value = obj.get("data") orelse return error.MediaContentPartMissingData;
        if (data_value != .string) return error.MediaContentPartMissingData;
        const mime_value = obj.get("mime_type") orelse return error.MediaContentPartMissingMimeType;
        if (mime_value != .string) return error.MediaContentPartMissingMimeType;

        const decoded_payload = decodeMediaData(allocator, data_value.string) catch return error.InvalidMediaBase64;
        const decoded = decoded_payload.data;
        errdefer allocator.free(decoded);
        if (!mediaMimeMatches(mime_value.string, decoded_payload.mime_type)) return error.MediaDataMimeTypeMismatch;

        if (std.mem.startsWith(u8, mime_value.string, "image/")) {
            if (!model_caps.modelAcceptsInput(manifest, "image")) return error.ModelDoesNotSupportImageInput;
            try parsed.images.append(allocator, .{
                .index = index,
                .bytes = decoded,
                .mime_type = mime_value.string,
            });
            return;
        }

        if (std.mem.startsWith(u8, mime_value.string, "audio/")) {
            if (!model_caps.modelAcceptsInput(manifest, "audio")) return error.ModelDoesNotSupportAudioInput;
            try parsed.audio.append(allocator, .{
                .index = index,
                .bytes = decoded,
                .mime_type = mime_value.string,
            });
            return;
        }

        return error.UnsupportedMediaMimeType;
    }

    return error.UnknownContentPartType;
}

fn embedInputParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.InputMustBeStringOrArrayOfStringsOrContentParts => "input must be a string, array of strings, or array of content parts",
        error.ContentPartTypeRequired => "content part missing 'type' field",
        error.ContentPartTypeMustBeString => "content part 'type' must be a string",
        error.TextContentPartMissingText => "text content part missing 'text' field",
        error.ImageUrlContentPartMissingImageUrl => "image_url content part missing 'image_url' field",
        error.ImageUrlContentPartMissingUrl => "image_url must contain a 'url' string",
        error.ImageUrlDownloadFailed => "failed to download image_url content",
        error.ImageUrlMustResolveToImage => "image_url content must resolve to an image",
        error.MediaContentPartMissingData => "media content part missing 'data' field",
        error.MediaContentPartMissingMimeType => "media content part missing 'mime_type' field",
        error.InvalidMediaBase64 => "invalid base64 media data",
        error.MediaDataMimeTypeMismatch => "media data URI mime_type does not match content part mime_type",
        error.UnsupportedMediaMimeType => "media content part must have an image/* or audio/* mime_type",
        error.ModelDoesNotSupportTextInput => "model does not support text input",
        error.ModelDoesNotSupportImageInput => "model does not support image input",
        error.ModelDoesNotSupportAudioInput => "model does not support audio input",
        error.UnknownContentPartType => "unsupported content part type",
        else => "invalid embedding input",
    };
}

const EmbedDenseInputFailure = struct {
    status: u16,
    code: []const u8,
    message: []const u8,
};

fn embedDenseInputFailure(err: anyerror) EmbedDenseInputFailure {
    return switch (err) {
        error.ImageDecodeFailed => .{
            .status = 400,
            .code = "INVALID_IMAGE",
            .message = "unsupported or corrupt image input",
        },
        error.ResourceLimitExceeded => .{
            .status = 400,
            .code = "MODEL_RESOURCE_LIMIT",
            .message = "request resource plan exceeds the configured inference budget",
        },
        error.ResourceTemporarilyUnavailable => .{
            .status = 503,
            .code = "MODEL_RESOURCE_BUSY",
            .message = "insufficient inference capacity is currently available",
        },
        else => .{
            .status = 500,
            .code = "INFERENCE_FAILED",
            .message = @errorName(err),
        },
    };
}

fn embedDenseInputFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    if (err == error.ResourceTemporarilyUnavailable) {
        return modelResourceBusyResponse(ctx);
    }
    const failure = embedDenseInputFailure(err);
    return ctx.status(failure.status).json(.{
        .@"error" = failure.code,
        .message = failure.message,
    });
}

const EmbedItemError = struct {
    index: i64,
    code: []const u8,
    message: []const u8,
    stage: []const u8,
    retryable: bool,
    status: u16,
    retry_after_ms: ?i64 = null,
};

const EmbedPartialSummary = struct {
    total: i64,
    succeeded: i64,
    failed: i64,
};

const EmbedResponseStrict = struct {
    object: []const u8,
    data: []const api.EmbeddingObject,
    model: []const u8,
    usage: api.EmbeddingUsage,
};

const EmbedDensePartialResponse = struct {
    object: []const u8,
    data: []const api.EmbeddingObject,
    model: []const u8,
    errors: []const EmbedItemError,
    summary: EmbedPartialSummary,
    usage: api.EmbeddingUsage,
};

const DenseEmbedPartialResult = struct {
    embeddings: []?[]f32,
    errors: []EmbedItemError,

    fn deinit(self: *DenseEmbedPartialResult, allocator: std.mem.Allocator) void {
        for (self.embeddings) |maybe_embedding| {
            if (maybe_embedding) |embedding| allocator.free(embedding);
        }
        allocator.free(self.embeddings);
        allocator.free(self.errors);
        self.* = undefined;
    }

    fn successCount(self: *const DenseEmbedPartialResult) usize {
        var count: usize = 0;
        for (self.embeddings) |maybe_embedding| {
            if (maybe_embedding != null) count += 1;
        }
        return count;
    }
};

fn embedItemFailure(index: usize, err: anyerror, stage: []const u8) EmbedItemError {
    return switch (err) {
        error.ImageDecodeFailed => .{
            .index = @intCast(index),
            .code = "INVALID_IMAGE",
            .message = "unsupported or corrupt image input",
            .stage = "image_decode",
            .retryable = false,
            .status = 400,
        },
        error.ResourceLimitExceeded => .{
            .index = @intCast(index),
            .code = "MODEL_RESOURCE_LIMIT",
            .message = "request resource plan exceeds the configured inference budget",
            .stage = stage,
            .retryable = false,
            .status = 400,
        },
        error.ResourceTemporarilyUnavailable => .{
            .index = @intCast(index),
            .code = "MODEL_RESOURCE_BUSY",
            .message = "insufficient inference capacity is currently available",
            .stage = stage,
            .retryable = true,
            .status = 503,
            .retry_after_ms = transient_capacity_retry_after_ms,
        },
        else => .{
            .index = @intCast(index),
            .code = "INFERENCE_FAILED",
            .message = @errorName(err),
            .stage = stage,
            .retryable = true,
            .status = 500,
        },
    };
}

fn embedInputItemFailure(index: usize, err: anyerror) EmbedItemError {
    return switch (err) {
        error.ImageUrlDownloadFailed => .{
            .index = @intCast(index),
            .code = "IMAGE_FETCH_FAILED",
            .message = "failed to download image_url content",
            .stage = "fetch",
            .retryable = true,
            .status = 502,
        },
        error.ImageUrlMustResolveToImage => .{
            .index = @intCast(index),
            .code = "INVALID_IMAGE_URL",
            .message = "image_url content must resolve to an image",
            .stage = "fetch",
            .retryable = false,
            .status = 400,
        },
        error.InvalidMediaBase64,
        error.MediaDataMimeTypeMismatch,
        => .{
            .index = @intCast(index),
            .code = "INVALID_MEDIA",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        error.UnsupportedMediaMimeType => .{
            .index = @intCast(index),
            .code = "UNSUPPORTED_MEDIA",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        error.ModelDoesNotSupportTextInput,
        error.ModelDoesNotSupportImageInput,
        error.ModelDoesNotSupportAudioInput,
        => .{
            .index = @intCast(index),
            .code = "UNSUPPORTED_INPUT_MODALITY",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        error.InputMustBeStringOrArrayOfStringsOrContentParts,
        error.ContentPartTypeRequired,
        error.ContentPartTypeMustBeString,
        error.TextContentPartMissingText,
        error.ImageUrlContentPartMissingImageUrl,
        error.ImageUrlContentPartMissingUrl,
        error.MediaContentPartMissingData,
        error.MediaContentPartMissingMimeType,
        error.UnknownContentPartType,
        => .{
            .index = @intCast(index),
            .code = "INVALID_INPUT",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        else => .{
            .index = @intCast(index),
            .code = "INPUT_PREP_FAILED",
            .message = @errorName(err),
            .stage = "parse",
            .retryable = true,
            .status = 500,
        },
    };
}

fn prepareInitialDenseEmbeddingPipeline(
    model: *model_manager_mod.LoadedModel,
    allocator: std.mem.Allocator,
    inputs: *const ParsedDenseEmbedInputs,
) !embedding_mod.EmbeddingPipeline {
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    if (inputs.audio.items.len > 0) {
        // Audio is deliberately the only optional phase admitted up front.
        // Text/image assets are admitted after the audio outputs are copied.
        try model.ensureAudioEmbeddingAssetsLocked();
    } else {
        try model.ensurePrimaryEmbeddingAssetsLocked(
            inputs.texts.items.len > 0,
            inputs.images.items.len > 0,
        );
    }
    return model.embeddingPipelineLocked(allocator);
}

fn admitPrimaryDenseEmbeddingAssetsAfterAudio(
    model: *model_manager_mod.LoadedModel,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
) !void {
    if (inputs.texts.items.len == 0 and inputs.images.items.len == 0) return;
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    try model.ensurePrimaryEmbeddingAssetsLocked(
        inputs.texts.items.len > 0,
        inputs.images.items.len > 0,
    );
    model.bindEmbeddingPipelineAssetsLocked(pipeline);
}

const PartialPrimaryDenseEmbeddingAdmission = struct {
    text_error: ?anyerror = null,
    image_error: ?anyerror = null,
};

fn admitPrimaryDenseEmbeddingAssetsAfterAudioPartial(
    model: *model_manager_mod.LoadedModel,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
) PartialPrimaryDenseEmbeddingAdmission {
    var admission = PartialPrimaryDenseEmbeddingAdmission{};
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();

    if (inputs.texts.items.len > 0) {
        model.ensurePrimaryEmbeddingAssetsLocked(true, false) catch |err| {
            admission.text_error = err;
        };
    }
    if (inputs.images.items.len > 0) {
        model.ensurePrimaryEmbeddingAssetsLocked(false, true) catch |err| {
            admission.image_error = err;
        };
    }
    model.bindEmbeddingPipelineAssetsLocked(pipeline);
    return admission;
}

fn releaseAudioDenseEmbeddingAssets(model: *model_manager_mod.LoadedModel) void {
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    model.releaseAudioEmbeddingAssetsLocked();
}

/// Owns the ephemeral audio sidecars from successful admission until the
/// audio phase has either materialized its outputs or unwound. Keeping this
/// ownership explicit closes the sessions on every error path before the
/// caller releases its exclusive embedding-asset lease.
const AudioEmbeddingAssetGuard = struct {
    model: *model_manager_mod.LoadedModel,
    held: bool,

    fn init(model: *model_manager_mod.LoadedModel, held: bool) AudioEmbeddingAssetGuard {
        return .{ .model = model, .held = held };
    }

    fn release(self: *AudioEmbeddingAssetGuard) void {
        if (!self.held) return;
        releaseAudioDenseEmbeddingAssets(self.model);
        self.held = false;
    }

    fn deinit(self: *AudioEmbeddingAssetGuard) void {
        self.release();
    }
};

fn embedDenseInputs(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
    model: *model_manager_mod.LoadedModel,
    audio_asset_guard: *AudioEmbeddingAssetGuard,
    asset_lease: *model_manager_mod.EmbeddingAssetLease,
) ![][]f32 {
    const embeddings = try allocator.alloc([]f32, inputs.total_count);
    errdefer allocator.free(embeddings);

    var filled = try allocator.alloc(bool, inputs.total_count);
    defer allocator.free(filled);
    @memset(filled, false);
    errdefer {
        for (embeddings, 0..) |embedding, i| {
            if (filled[i]) allocator.free(embedding);
        }
    }

    // Materialize the audio outputs while its ephemeral sidecars are resident,
    // release them, and only then admit reusable text/image sidecars.
    if (inputs.audio.items.len > 0) {
        {
            defer audio_asset_guard.release();
            const audio_inputs = try allocator.alloc(embedding_mod.EncodedAudioClip, inputs.audio.items.len);
            defer allocator.free(audio_inputs);
            for (inputs.audio.items, 0..) |item, i| {
                audio_inputs[i] = .{
                    .bytes = item.bytes,
                    .decode_options = .{
                        .mime_hint = item.mime_type,
                    },
                };
            }

            const audio_embeddings = try pipeline.embedEncodedAudio(audio_inputs);
            defer allocator.free(audio_embeddings);
            for (inputs.audio.items, 0..) |item, i| {
                embeddings[item.index] = audio_embeddings[i];
                filled[item.index] = true;
            }
        }
        if (inputs.texts.items.len > 0 or inputs.images.items.len > 0) {
            asset_lease.downgradeExclusiveToShared();
            try admitPrimaryDenseEmbeddingAssetsAfterAudio(model, pipeline, inputs);
        } else {
            asset_lease.release();
        }
    }

    if (inputs.texts.items.len > 0) {
        const texts = try allocator.alloc([]const u8, inputs.texts.items.len);
        defer allocator.free(texts);
        for (inputs.texts.items, 0..) |item, i| texts[i] = item.text;

        const text_embeddings = try pipeline.embed(texts);
        defer allocator.free(text_embeddings);
        for (inputs.texts.items, 0..) |item, i| {
            embeddings[item.index] = text_embeddings[i];
            filled[item.index] = true;
        }
    }

    if (inputs.images.items.len > 0) {
        const images = try allocator.alloc([]const u8, inputs.images.items.len);
        defer allocator.free(images);
        for (inputs.images.items, 0..) |item, i| images[i] = item.bytes;

        const image_embeddings = try pipeline.embedImages(images);
        defer allocator.free(image_embeddings);
        for (inputs.images.items, 0..) |item, i| {
            embeddings[item.index] = image_embeddings[i];
            filled[item.index] = true;
        }
    }

    for (filled) |was_filled| {
        if (!was_filled) return error.MissingEmbeddingResult;
    }

    return embeddings;
}

fn embedDenseInputsPartial(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
    model: *model_manager_mod.LoadedModel,
    audio_asset_guard: *AudioEmbeddingAssetGuard,
    asset_lease: *model_manager_mod.EmbeddingAssetLease,
) !DenseEmbedPartialResult {
    var result = try initDenseEmbedPartialResult(allocator, inputs.total_count);
    errdefer result.deinit(allocator);

    var errors = std.ArrayListUnmanaged(EmbedItemError).empty;
    errdefer errors.deinit(allocator);
    try errors.appendSlice(allocator, inputs.parse_errors.items);

    var primary_admission = PartialPrimaryDenseEmbeddingAdmission{};
    if (inputs.audio.items.len > 0) {
        {
            defer audio_asset_guard.release();
            const audio_inputs = try allocator.alloc(embedding_mod.EncodedAudioClip, inputs.audio.items.len);
            defer allocator.free(audio_inputs);
            for (inputs.audio.items, 0..) |item, i| {
                audio_inputs[i] = .{
                    .bytes = item.bytes,
                    .decode_options = .{ .mime_hint = item.mime_type },
                };
            }

            if (pipeline.embedEncodedAudio(audio_inputs)) |embeddings| {
                defer allocator.free(embeddings);
                for (inputs.audio.items, 0..) |item, i| {
                    result.embeddings[item.index] = embeddings[i];
                }
            } else |_| {
                try embedAudioInputsIndividually(allocator, pipeline, inputs.audio.items, audio_inputs, &errors, &result);
            }
        }
        if (inputs.texts.items.len > 0 or inputs.images.items.len > 0) {
            asset_lease.downgradeExclusiveToShared();
            primary_admission = admitPrimaryDenseEmbeddingAssetsAfterAudioPartial(model, pipeline, inputs);
        } else {
            asset_lease.release();
        }
        if (primary_admission.text_error) |err| {
            try appendDenseEmbeddingItemErrors(allocator, &errors, inputs.texts.items, err, "model_admission");
        }
        if (primary_admission.image_error) |err| {
            try appendDenseEmbeddingItemErrors(allocator, &errors, inputs.images.items, err, "model_admission");
        }
    }

    if (primary_admission.text_error == null and inputs.texts.items.len > 0) {
        const texts = try allocator.alloc([]const u8, inputs.texts.items.len);
        defer allocator.free(texts);
        for (inputs.texts.items, 0..) |item, i| texts[i] = item.text;

        if (pipeline.embed(texts)) |embeddings| {
            defer allocator.free(embeddings);
            for (inputs.texts.items, 0..) |item, i| {
                result.embeddings[item.index] = embeddings[i];
            }
        } else |_| {
            try embedTextInputsIndividually(allocator, pipeline, inputs.texts.items, texts, &errors, &result);
        }
    }

    if (primary_admission.image_error == null and inputs.images.items.len > 0) {
        const images = try allocator.alloc([]const u8, inputs.images.items.len);
        defer allocator.free(images);
        for (inputs.images.items, 0..) |item, i| images[i] = item.bytes;

        if (pipeline.embedImages(images)) |embeddings| {
            defer allocator.free(embeddings);
            for (inputs.images.items, 0..) |item, i| {
                result.embeddings[item.index] = embeddings[i];
            }
        } else |_| {
            try embedImageInputsIndividually(allocator, pipeline, inputs.images.items, images, &errors, &result);
        }
    }

    for (result.embeddings, 0..) |maybe_embedding, index| {
        if (maybe_embedding == null) {
            var already_reported = false;
            for (errors.items) |item_error| {
                if (item_error.index == @as(i64, @intCast(index))) {
                    already_reported = true;
                    break;
                }
            }
            if (!already_reported) {
                try errors.append(allocator, embedItemFailure(index, error.MissingEmbeddingResult, "inference"));
            }
        }
    }

    const owned_errors = try errors.toOwnedSlice(allocator);
    allocator.free(result.errors);
    result.errors = owned_errors;
    return result;
}

fn initDenseEmbedPartialResult(allocator: std.mem.Allocator, total_count: usize) !DenseEmbedPartialResult {
    const embeddings = try allocator.alloc(?[]f32, total_count);
    errdefer allocator.free(embeddings);
    const errors = try allocator.alloc(EmbedItemError, 0);
    @memset(embeddings, null);
    return .{
        .embeddings = embeddings,
        .errors = errors,
    };
}

fn appendDenseEmbeddingItemErrors(
    allocator: std.mem.Allocator,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    items: anytype,
    err: anyerror,
    stage: []const u8,
) !void {
    for (items) |item| {
        try errors.append(allocator, embedItemFailure(item.index, err, stage));
    }
}

fn embedTextInputsIndividually(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    items: []const ParsedTextEmbedInput,
    texts: []const []const u8,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    result: *DenseEmbedPartialResult,
) !void {
    for (items, 0..) |item, i| {
        const single = pipeline.embed(texts[i .. i + 1]) catch |single_err| {
            try errors.append(allocator, embedItemFailure(item.index, single_err, "text_inference"));
            continue;
        };
        defer allocator.free(single);
        if (single.len != 1) {
            for (single) |embedding| allocator.free(embedding);
            try errors.append(allocator, embedItemFailure(item.index, error.MissingEmbeddingResult, "text_inference"));
            continue;
        }
        result.embeddings[item.index] = single[0];
    }
}

fn embedImageInputsIndividually(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    items: []const ParsedBinaryEmbedInput,
    images: []const []const u8,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    result: *DenseEmbedPartialResult,
) !void {
    for (items, 0..) |item, i| {
        const single = pipeline.embedImages(images[i .. i + 1]) catch |single_err| {
            try errors.append(allocator, embedItemFailure(item.index, single_err, "image_inference"));
            continue;
        };
        defer allocator.free(single);
        if (single.len != 1) {
            for (single) |embedding| allocator.free(embedding);
            try errors.append(allocator, embedItemFailure(item.index, error.MissingEmbeddingResult, "image_inference"));
            continue;
        }
        result.embeddings[item.index] = single[0];
    }
}

fn embedAudioInputsIndividually(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    items: []const ParsedBinaryEmbedInput,
    audio_inputs: []const embedding_mod.EncodedAudioClip,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    result: *DenseEmbedPartialResult,
) !void {
    for (items, 0..) |item, i| {
        const single = pipeline.embedEncodedAudio(audio_inputs[i .. i + 1]) catch |single_err| {
            try errors.append(allocator, embedItemFailure(item.index, single_err, "audio_inference"));
            continue;
        };
        defer allocator.free(single);
        if (single.len != 1) {
            for (single) |embedding| allocator.free(embedding);
            try errors.append(allocator, embedItemFailure(item.index, error.MissingEmbeddingResult, "audio_inference"));
            continue;
        }
        result.embeddings[item.index] = single[0];
    }
}

fn buildEmbedDenseResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    embeddings: []const []const f32,
    requested_dimensions: ?usize,
    prompt_tokens: usize,
) !EmbedResponseStrict {
    const data = try arena.alloc(api.EmbeddingObject, embeddings.len);
    for (embeddings, 0..) |emb, i| {
        const dimensions = requested_dimensions orelse emb.len;
        if (dimensions > emb.len) return error.InvalidEmbeddingDimensions;
        var arr: std.json.Array = .init(arena);
        try arr.ensureTotalCapacity(dimensions);
        for (emb[0..dimensions]) |val| arr.appendAssumeCapacity(.{ .float = val });
        data[i] = .{
            .object = "embedding",
            .index = @intCast(i),
            .embedding = .{ .array = arr },
        };
    }
    return .{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = .{
            .prompt_tokens = @intCast(prompt_tokens),
            .total_tokens = @intCast(prompt_tokens),
        },
    };
}

fn buildEmbedDensePartialResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    result: *const DenseEmbedPartialResult,
    requested_dimensions: ?usize,
    prompt_tokens: usize,
) !EmbedDensePartialResponse {
    const succeeded = result.successCount();
    const data = try arena.alloc(api.EmbeddingObject, succeeded);
    var out_index: usize = 0;
    for (result.embeddings, 0..) |maybe_embedding, input_index| {
        const emb = maybe_embedding orelse continue;
        const dimensions = requested_dimensions orelse emb.len;
        if (dimensions > emb.len) return error.InvalidEmbeddingDimensions;
        var arr: std.json.Array = .init(arena);
        try arr.ensureTotalCapacity(dimensions);
        for (emb[0..dimensions]) |val| arr.appendAssumeCapacity(.{ .float = val });
        data[out_index] = .{
            .object = "embedding",
            .index = @intCast(input_index),
            .embedding = .{ .array = arr },
        };
        out_index += 1;
    }

    const errors = try arena.dupe(EmbedItemError, result.errors);
    return .{
        .object = "list",
        .data = data,
        .model = model_name,
        .errors = errors,
        .summary = .{
            .total = @intCast(result.embeddings.len),
            .succeeded = @intCast(succeeded),
            .failed = @intCast(result.errors.len),
        },
        .usage = .{
            .prompt_tokens = @intCast(prompt_tokens),
            .total_tokens = @intCast(prompt_tokens),
        },
    };
}

fn buildEmbedSparseResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    sparse_vecs: []const sparse_embedding_mod.SparseVector,
    prompt_tokens: usize,
) !EmbedResponseStrict {
    const data = try arena.alloc(api.EmbeddingObject, sparse_vecs.len);
    for (sparse_vecs, 0..) |sv, i| {
        var indices: std.json.Array = .init(arena);
        try indices.ensureTotalCapacity(sv.indices.len);
        for (sv.indices) |idx| indices.appendAssumeCapacity(.{ .integer = @intCast(idx) });

        var values: std.json.Array = .init(arena);
        try values.ensureTotalCapacity(sv.values.len);
        for (sv.values) |val| values.appendAssumeCapacity(.{ .float = val });

        var obj: std.json.ObjectMap = .empty;
        try obj.put(arena, "indices", .{ .array = indices });
        try obj.put(arena, "values", .{ .array = values });

        data[i] = .{
            .object = "embedding",
            .index = @intCast(i),
            .embedding = .{ .object = obj },
        };
    }
    return .{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = .{
            .prompt_tokens = @intCast(prompt_tokens),
            .total_tokens = @intCast(prompt_tokens),
        },
    };
}

test "Antfly inference embeddings validates encoding format and dimensions" {
    try validateEmbeddingEncodingFormat(null);
    try validateEmbeddingEncodingFormat("float");
    try std.testing.expectError(error.UnsupportedEncodingFormat, validateEmbeddingEncodingFormat("base64"));
    try std.testing.expectEqual(@as(?usize, null), try parseRequestedEmbeddingDimensions(null));
    try std.testing.expectEqual(@as(?usize, 128), try parseRequestedEmbeddingDimensions(128));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseRequestedEmbeddingDimensions(0));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseRequestedEmbeddingDimensions(-1));
}

test "jina embedding request options switch query and document prefixes" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .pooling = .last,
    };
    defer manifest.deinit();
    manifest.embedding_text_prefix = try allocator.dupe(u8, "Document: ");
    manifest.tasks = try allocator.alloc([]const u8, 1);
    manifest.tasks[0] = try allocator.dupe(u8, "retrieval");

    var pipeline = embedding_mod.EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const query_request = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_QUERY,
    };
    try applyDenseEmbeddingRequestOptions(&pipeline, &manifest, query_request);
    try std.testing.expectEqualStrings("Query: ", pipeline.config.text_prefix);

    const qa_request = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .QUESTION_ANSWERING,
    };
    try applyDenseEmbeddingRequestOptions(&pipeline, &manifest, qa_request);
    try std.testing.expectEqualStrings("Query: ", pipeline.config.text_prefix);

    const document_request = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_DOCUMENT,
    };
    try applyDenseEmbeddingRequestOptions(&pipeline, &manifest, document_request);
    try std.testing.expectEqualStrings("Document: ", pipeline.config.text_prefix);

    const bad_task_type = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .CLASSIFICATION,
    };
    try std.testing.expectError(error.UnsupportedEmbeddingTaskType, applyDenseEmbeddingRequestOptions(&pipeline, &manifest, bad_task_type));
}

test "jina embedding request options support legacy input_type aliases" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .pooling = .last,
    };
    defer manifest.deinit();
    manifest.embedding_text_prefix = try allocator.dupe(u8, "Document: ");

    var pipeline = embedding_mod.EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const body =
        \\{
        \\  "model": "jina-merged",
        \\  "input": "hello",
        \\  "input_type": "query"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    try std.testing.expectEqual(EmbeddingTaskType.RETRIEVAL_QUERY, request.task_type.?);
    try applyDenseEmbeddingRequestOptions(&pipeline, &manifest, request);
    try std.testing.expectEqualStrings("Query: ", pipeline.config.text_prefix);
}

test "embedding request parser uses Google task_type as canonical field" {
    const allocator = std.testing.allocator;
    const body =
        \\{
        \\  "model": "jina",
        \\  "input": "hello",
        \\  "task_type": "RETRIEVAL_DOCUMENT"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    try std.testing.expectEqual(EmbeddingTaskType.RETRIEVAL_DOCUMENT, request.task_type.?);
}

test "embedding request parser rejects conflicting task aliases" {
    const allocator = std.testing.allocator;
    const body =
        \\{
        \\  "model": "jina",
        \\  "input": "hello",
        \\  "task_type": "RETRIEVAL_QUERY",
        \\  "input_type": "search_document"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.ConflictingEmbeddingTaskTypes, parseEmbedRequest(parsed.value));
}

test "Antfly inference embed parser accepts per-item error policy" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": ["hello"],
        \\  "error_policy": "per_item"
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    try std.testing.expectEqual(EmbedErrorPolicy.per_item, request.error_policy);
}

test "Antfly inference embed parser rejects unknown error policy" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": ["hello"],
        \\  "error_policy": "best_effort"
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedEmbeddingErrorPolicy, parseEmbedRequest(parsed.value));
}

test "Antfly inference per-item dense parser records invalid media and keeps siblings" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "error_policy": "per_item",
        \\  "input": [
        \\    "text that can still be embedded",
        \\    {"type":"media","mime_type":"image/webp","data":"not-valid-base64-or-image"}
        \\  ]
        \\}
    ;

    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed_json.deinit();

    const request = try parseEmbedRequest(parsed_json.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };

    var inputs = try parseDenseEmbedInputsPerItem(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 0), inputs.images.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.parse_errors.items.len);
    try std.testing.expectEqual(@as(i64, 1), inputs.parse_errors.items[0].index);
    try std.testing.expectEqualStrings("INVALID_MEDIA", inputs.parse_errors.items[0].code);
    try std.testing.expectEqualStrings("parse", inputs.parse_errors.items[0].stage);
    try std.testing.expectEqual(false, inputs.parse_errors.items[0].retryable);
}

test "dense partial result cleanup owns every allocation exactly once" {
    const Probe = struct {
        fn failAfterPopulating(allocator: std.mem.Allocator) !void {
            var result = try initDenseEmbedPartialResult(allocator, 2);
            errdefer result.deinit(allocator);
            result.embeddings[0] = try allocator.dupe(f32, &.{ 1.0, 2.0 });
            return error.InjectedFailure;
        }
    };

    try std.testing.expectError(error.InjectedFailure, Probe.failAfterPopulating(std.testing.allocator));
}

test "audio embedding asset guard releases admission on early failure" {
    var model: model_manager_mod.LoadedModel = undefined;
    model.embedding_session_lock = .unlocked;
    model.audio_session = null;
    model.audio_projection = null;
    model.audio_resource_lease = null;
    model.audio_projection_resource_lease = null;

    var guard = AudioEmbeddingAssetGuard.init(&model, true);
    const Probe = struct {
        fn fail(owned: *AudioEmbeddingAssetGuard) !void {
            defer owned.deinit();
            return error.InjectedFailure;
        }
    };

    try std.testing.expectError(error.InjectedFailure, Probe.fail(&guard));
    try std.testing.expect(!guard.held);
    guard.deinit();
}

test "primary embedding admission failures are recorded per affected item" {
    const allocator = std.testing.allocator;
    const texts = [_]ParsedTextEmbedInput{
        .{ .index = 1, .text = "first" },
        .{ .index = 3, .text = "second" },
    };
    var image_bytes = [_]u8{ 1, 2, 3 };
    const images = [_]ParsedBinaryEmbedInput{
        .{ .index = 4, .bytes = &image_bytes, .mime_type = "image/png" },
    };
    var errors = std.ArrayListUnmanaged(EmbedItemError).empty;
    defer errors.deinit(allocator);

    try appendDenseEmbeddingItemErrors(
        allocator,
        &errors,
        &texts,
        error.ResourceTemporarilyUnavailable,
        "model_admission",
    );
    try appendDenseEmbeddingItemErrors(
        allocator,
        &errors,
        &images,
        error.ResourceLimitExceeded,
        "model_admission",
    );

    try std.testing.expectEqual(@as(usize, 3), errors.items.len);
    try std.testing.expectEqual(@as(i64, 1), errors.items[0].index);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", errors.items[0].code);
    try std.testing.expectEqual(@as(u16, 503), errors.items[0].status);
    try std.testing.expect(errors.items[0].retryable);
    try std.testing.expectEqual(
        @as(?i64, transient_capacity_retry_after_ms),
        errors.items[0].retry_after_ms,
    );
    try std.testing.expectEqual(@as(i64, 3), errors.items[1].index);
    try std.testing.expectEqualStrings("model_admission", errors.items[1].stage);
    try std.testing.expectEqual(@as(i64, 4), errors.items[2].index);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_LIMIT", errors.items[2].code);
    try std.testing.expectEqual(@as(u16, 400), errors.items[2].status);
    try std.testing.expect(!errors.items[2].retryable);
    try std.testing.expectEqual(@as(?i64, null), errors.items[2].retry_after_ms);
}

fn expectJsonNumber(expected: f64, value: std.json.Value) !void {
    const actual: f64 = switch (value) {
        .float => |float| float,
        .integer => |integer| @floatFromInt(integer),
        else => return error.ExpectedJsonNumber,
    };
    try std.testing.expectEqual(expected, actual);
}

test "Antfly inference embeddings dense response supports truncation" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const embedding = [_]f32{ 1.0, 2.0, 3.0 };
    const embeddings = [_][]const f32{embedding[0..]};
    const response = try buildEmbedDenseResponse(arena.allocator(), "dense-model", &embeddings, 2, 7);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), data.len);
    const embedding_json = data[0].object.get("embedding").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), embedding_json.len);
    try expectJsonNumber(1.0, embedding_json[0]);
    try expectJsonNumber(2.0, embedding_json[1]);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("usage").?.object.get("total_tokens").?.integer);
}

test "Antfly inference embeddings per-item response includes successes and indexed errors" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const first = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(first);
    const third = try alloc.dupe(f32, &[_]f32{ 4.0, 5.0, 6.0 });
    defer alloc.free(third);
    var errors = [_]EmbedItemError{
        embedItemFailure(1, error.ImageDecodeFailed, "image_inference"),
    };
    var embeddings = [_]?[]f32{ first, null, third };
    const partial = DenseEmbedPartialResult{
        .embeddings = embeddings[0..],
        .errors = errors[0..],
    };

    const response = try buildEmbedDensePartialResponse(arena.allocator(), "dense-model", &partial, 2, 7);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), data.len);
    try std.testing.expectEqual(@as(i64, 0), data[0].object.get("index").?.integer);
    try std.testing.expectEqual(@as(i64, 2), data[1].object.get("index").?.integer);
    try std.testing.expectEqual(@as(usize, 2), data[0].object.get("embedding").?.array.items.len);

    const response_errors = parsed.value.object.get("errors").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), response_errors.len);
    try std.testing.expectEqual(@as(i64, 1), response_errors[0].object.get("index").?.integer);
    try std.testing.expectEqualStrings("INVALID_IMAGE", response_errors[0].object.get("code").?.string);
    try std.testing.expectEqual(false, response_errors[0].object.get("retryable").?.bool);

    const summary = parsed.value.object.get("summary").?.object;
    try std.testing.expectEqual(@as(i64, 3), summary.get("total").?.integer);
    try std.testing.expectEqual(@as(i64, 2), summary.get("succeeded").?.integer);
    try std.testing.expectEqual(@as(i64, 1), summary.get("failed").?.integer);
}

test "Antfly inference embeddings sparse response uses the shared embedding field" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const indices = try alloc.dupe(u32, &[_]u32{ 7, 42 });
    defer alloc.free(indices);
    const values = try alloc.dupe(f32, &[_]f32{ 1.5, 0.5 });
    defer alloc.free(values);
    const sparse = [_]sparse_embedding_mod.SparseVector{
        .{
            .indices = indices,
            .values = values,
        },
    };

    const response = try buildEmbedSparseResponse(arena.allocator(), "sparse-model", &sparse, 11);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), data.len);
    const sparse_embedding = data[0].object.get("embedding").?.object;
    try std.testing.expectEqual(@as(usize, 2), sparse_embedding.get("indices").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 2), sparse_embedding.get("values").?.array.items.len);
    try std.testing.expectEqual(@as(i64, 11), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 11), parsed.value.object.get("usage").?.object.get("total_tokens").?.integer);
}

test "Antfly inference embed request parser accepts multimodal content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"text","text":"hello world"},
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\    {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.audio.items.len);
    try std.testing.expectEqualStrings("hello world", inputs.texts.items[0].text);
}

test "Antfly inference embed request parser accepts mixed strings and content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    "hello world",
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\    {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.audio.items.len);
    try std.testing.expectEqual(@as(usize, 0), inputs.texts.items[0].index);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items[0].index);
    try std.testing.expectEqual(@as(usize, 2), inputs.audio.items[0].index);
}

test "Antfly inference embed media-only usage does not require text tokens" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\    {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 0), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 0), estimateParsedDenseEmbedPromptTokens(&inputs));
}

test "embedding image decode failures are permanent client input errors" {
    const failure = embedDenseInputFailure(error.ImageDecodeFailed);
    try std.testing.expectEqual(@as(u16, 400), failure.status);
    try std.testing.expectEqualStrings("INVALID_IMAGE", failure.code);
    try std.testing.expectEqualStrings("unsupported or corrupt image input", failure.message);

    const runtime_failure = embedDenseInputFailure(error.OutOfMemory);
    try std.testing.expectEqual(@as(u16, 500), runtime_failure.status);
    try std.testing.expectEqualStrings("INFERENCE_FAILED", runtime_failure.code);

    const limit_failure = embedDenseInputFailure(error.ResourceLimitExceeded);
    try std.testing.expectEqual(@as(u16, 400), limit_failure.status);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_LIMIT", limit_failure.code);

    const busy_failure = embedDenseInputFailure(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqual(@as(u16, 503), busy_failure.status);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy_failure.code);
}

test "Antfly inference embed parser accepts data uri media payloads" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"media","mime_type":"image/png","data":"data:image/png;base64,AQI="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items.len);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, inputs.images.items[0].bytes);
}

test "Antfly inference embed parser rejects mismatched data uri media mime type" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"media","mime_type":"audio/wav","data":"data:image/png;base64,AQI="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    try std.testing.expectError(
        error.MediaDataMimeTypeMismatch,
        parseDenseEmbedInputs(&node, alloc, &manifest, request.input),
    );
}

test "Antfly inference sparse embed parser rejects multimodal content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\[
        \\  {"type":"text","text":"hello world"}
        \\]
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.SparseModelsRequireTextInput, parseSparseEmbedInputs(alloc, parsed.value));
}

test "multimodal rerank parser accepts colqwen-style text and image content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice total due date",
        \\  "documents": [
        \\    {
        \\      "content": [
        \\        {"type":"text","text":"invoice page"},
        \\        {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\        {"type":"media","mime_type":"image/png","data":"AQ=="},
        \\        {"type":"text","text":" appendix"}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();

    var node: Node = undefined;
    node.config = .{};

    var doc = try node.parseChatMessageContentToTextAndImages(alloc, parsed.value.documents[0].content);
    defer doc.deinit();

    try std.testing.expectEqualStrings("invoice page appendix", doc.text);
    try std.testing.expectEqual(@as(usize, 2), doc.images.len);
    try std.testing.expectEqual(@as(usize, 1), doc.images[0].len);
    try std.testing.expectEqual(@as(usize, 1), doc.images[1].len);
    try std.testing.expectEqual(@as(u8, 0), doc.images[0][0]);
    try std.testing.expectEqual(@as(u8, 1), doc.images[1][0]);
}

test "multimodal rerank parser rejects non-image media content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice total due date",
        \\  "documents": [
        \\    {
        \\      "content": [
        \\        {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();

    var node: Node = undefined;
    node.config = .{};

    try std.testing.expectError(
        error.UnsupportedContentPartType,
        node.parseChatMessageContentToTextAndImages(alloc, parsed.value.documents[0].content),
    );
}

test "multimodal rerank parser rejects invalid image data uris" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice total due date",
        \\  "documents": [
        \\    {
        \\      "content": [
        \\        {"type":"image_url","image_url":{"url":"data:image/png;base64,%%%"}}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();

    var node: Node = undefined;
    node.config = .{};

    try std.testing.expectError(
        error.InvalidImageDataUri,
        node.parseChatMessageContentToTextAndImages(alloc, parsed.value.documents[0].content),
    );
}

/// Decode a data URI (data:mime/type;base64,...) to raw bytes.
fn downloadRemoteContent(self: *const Node, alloc: std.mem.Allocator, url: []const u8) !scraping.DownloadedContent {
    const security = if (self.config.content_security) |*cfg| cfg else null;
    const s3_credentials = if (self.config.s3_credentials) |*cfg| cfg else null;
    return try scraping.downloadContentAlloc(alloc, url, security, s3_credentials);
}

fn downloadReadBatchContent(
    self: *const Node,
    alloc: std.mem.Allocator,
    url: []const u8,
    max_bytes: usize,
    current_bytes: usize,
) !scraping.DownloadedContent {
    if (current_bytes >= max_bytes) return error.ReadBatchTooLarge;
    const remaining = max_bytes - current_bytes;
    var bounded_security = if (self.config.content_security) |cfg| cfg else scraping.ContentSecurityConfig{};
    const remaining_u64: u64 = @intCast(remaining);
    bounded_security.max_download_size_bytes = if (bounded_security.max_download_size_bytes) |configured|
        @min(configured, remaining_u64)
    else
        remaining_u64;
    const s3_credentials = if (self.config.s3_credentials) |*cfg| cfg else null;
    return try scraping.downloadContentAlloc(alloc, url, &bounded_security, s3_credentials);
}

fn readBatchMaxBytes() usize {
    return @max(@as(usize, 1), platform.env.getenvUsize("ANTFLY_INFERENCE_READ_BATCH_BYTES") orelse default_max_read_batch_bytes);
}

fn validateReadMaxTokens(value: ?i64) !?usize {
    const requested = value orelse return null;
    if (requested < 1 or requested > @as(i64, @intCast(max_read_tokens))) return error.InvalidMaxTokens;
    return @intCast(requested);
}

fn normalizeReadPrompt(prompt: ?[]const u8) ?[]const u8 {
    const value = prompt orelse return null;
    return if (std.mem.trim(u8, value, " \t\r\n").len == 0) null else value;
}

fn estimateReadQueueUnits(image_count: usize, max_tokens: ?usize) usize {
    const estimated_max_tokens = max_tokens orelse default_read_queue_max_tokens;
    const token_units = 1 + ((@max(estimated_max_tokens, 1) - 1) / default_read_queue_max_tokens);
    return std.math.mul(usize, @max(image_count, 1), token_units) catch std.math.maxInt(usize);
}

fn addReadBatchDownloadedBytes(current: usize, item: scraping.DownloadedContent, max_bytes: usize) !usize {
    const with_data = std.math.add(usize, current, item.data.len) catch return error.ReadBatchTooLarge;
    const total = std.math.add(usize, with_data, item.content_type.len) catch return error.ReadBatchTooLarge;
    if (total > max_bytes) return error.ReadBatchTooLarge;
    return total;
}

const DecodedDataUri = struct {
    mime_type: ?[]const u8,
    data: []u8,

    fn deinit(self: DecodedDataUri, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

fn decodeDataUri(allocator: std.mem.Allocator, uri: []const u8) !DecodedDataUri {
    // Expect: data:<mime>;base64,<data>
    const prefix = "data:";
    if (!std.mem.startsWith(u8, uri, prefix)) return error.InvalidDataUri;

    const b64_marker = ";base64,";
    const marker_pos = std.mem.indexOf(u8, uri, b64_marker) orelse return error.InvalidDataUri;
    const mime_start = prefix.len;
    const mime_raw = uri[mime_start..marker_pos];
    const b64_start = marker_pos + b64_marker.len;
    const b64_data = uri[b64_start..];

    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(b64_data) catch return error.InvalidBase64;
    const decoded = try allocator.alloc(u8, decoded_len);
    errdefer allocator.free(decoded);

    std.base64.standard.Decoder.decode(decoded, b64_data) catch return error.InvalidBase64;
    return .{
        .mime_type = if (mime_raw.len == 0) null else mime_raw,
        .data = decoded,
    };
}

fn decodeMediaData(allocator: std.mem.Allocator, data: []const u8) !DecodedDataUri {
    if (std.mem.startsWith(u8, data, "data:")) return try decodeDataUri(allocator, data);

    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(data) catch return error.InvalidBase64;
    const decoded = try allocator.alloc(u8, decoded_len);
    errdefer allocator.free(decoded);
    std.base64.standard.Decoder.decode(decoded, data) catch return error.InvalidBase64;
    return .{
        .mime_type = null,
        .data = decoded,
    };
}

fn mediaMimeMatches(declared: ?[]const u8, embedded: ?[]const u8) bool {
    const embedded_mime = embedded orelse return true;
    const declared_mime = declared orelse return true;
    return std.ascii.eqlIgnoreCase(trimMimeParametersLocal(declared_mime), trimMimeParametersLocal(embedded_mime));
}

fn trimMimeParametersLocal(value: []const u8) []const u8 {
    const semi = std.mem.indexOfScalar(u8, value, ';') orelse return std.mem.trim(u8, value, &std.ascii.whitespace);
    return std.mem.trim(u8, value[0..semi], &std.ascii.whitespace);
}

fn unsupportedAudioResponse(ctx: *httpx.Context, message: []const u8) !httpx.Response {
    return ctx.status(400).json(.{
        .@"error" = "UNSUPPORTED",
        .message = message,
    });
}

fn coerceGenerateResponseFormat(
    allocator: std.mem.Allocator,
    response_format: ?api.GenerateResponseFormat,
    json_text: []const u8,
) !?[]u8 {
    const rf = response_format orelse return null;
    if (std.mem.eql(u8, rf.type, "json_object")) {
        var parsed = std.json.parseFromSlice(std.json.Value, allocator, json_text, .{}) catch
            return error.InvalidStructuredOutput;
        defer parsed.deinit();
        if (parsed.value == .object) return null;
        return error.InvalidStructuredOutput;
    }
    if (!std.mem.eql(u8, rf.type, "json_schema")) return null;

    const schema_cfg = rf.json_schema orelse return error.MissingJsonSchema;
    validateGeneratedJsonSchema(allocator, json_text, schema_cfg) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidStructuredOutput;
    };
    return null;
}

fn validateGeneratedJsonSchema(
    allocator: std.mem.Allocator,
    json_text: []const u8,
    schema_cfg: api.GenerateJsonSchemaConfig,
) !void {
    const schema = schema_cfg.schema orelse return error.MissingJsonSchema;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_text, .{});
    defer parsed.deinit();
    try jsonschema.validateJsonSchemaValue(allocator, schema, parsed.value);
}

test "structured output validation fails closed instead of fabricating JSON" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidStructuredOutput,
        coerceGenerateResponseFormat(
            allocator,
            .{ .type = "json_object" },
            "not json",
        ),
    );
    try std.testing.expectError(
        error.InvalidStructuredOutput,
        coerceGenerateResponseFormat(
            allocator,
            .{ .type = "json_object" },
            "[]",
        ),
    );

    var parsed_schema = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        "{\"type\":\"object\",\"required\":[\"answer\"],\"properties\":{\"answer\":{\"type\":\"string\"}}}",
        .{},
    );
    defer parsed_schema.deinit();
    try std.testing.expectError(
        error.InvalidStructuredOutput,
        coerceGenerateResponseFormat(
            allocator,
            .{
                .type = "json_schema",
                .json_schema = .{ .schema = parsed_schema.value },
            },
            "{}",
        ),
    );
    try std.testing.expectEqual(
        @as(?[]u8, null),
        try coerceGenerateResponseFormat(
            allocator,
            .{
                .type = "json_schema",
                .json_schema = .{ .schema = parsed_schema.value },
            },
            "{\"answer\":\"ok\"}",
        ),
    );
}

test "shared json schema validator: additionalProperties schema object" {
    const allocator = std.testing.allocator;
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "object",
        \\  "properties": { "name": { "type": "string" } },
        \\  "additionalProperties": { "type": "integer", "minimum": 1 }
        \\}
    , .{});
    defer schema_parsed.deinit();

    var value_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"name":"ok","score":2}
    , .{});
    defer value_parsed.deinit();

    try jsonschema.validateJsonSchemaValue(allocator, schema_parsed.value, value_parsed.value);
}

test "shared json schema validator: combinators and bounds" {
    const allocator = std.testing.allocator;
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "allOf": [{ "type": "integer", "minimum": 2 }],
        \\  "anyOf": [{ "const": 3 }, { "const": 4 }],
        \\  "oneOf": [{ "const": 3 }, { "const": 5 }]
        \\}
    , .{});
    defer schema_parsed.deinit();

    try jsonschema.validateJsonSchemaValue(allocator, schema_parsed.value, .{ .integer = 3 });
}

test "shared json schema validator: array bounds" {
    const allocator = std.testing.allocator;
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "array",
        \\  "items": { "type": "integer" },
        \\  "minItems": 1,
        \\  "maxItems": 2
        \\}
    , .{});
    defer schema_parsed.deinit();

    var value_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\[1,2]
    , .{});
    defer value_parsed.deinit();

    try jsonschema.validateJsonSchemaValue(allocator, schema_parsed.value, value_parsed.value);
}

fn appendIntJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: anytype) !void {
    const num = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(num);
    try buf.appendSlice(allocator, num);
}

fn appendBase64Json(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    const encoded_len = std.base64.standard.Encoder.calcSize(value.len);
    const encoded = try allocator.alloc(u8, encoded_len);
    defer allocator.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, value);
    try jsonEncodeString(buf, allocator, encoded);
}

fn graphModeEnabled() bool {
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    return platform.env.getenvBool("TERMITE_GRAPH_MODE");
}
