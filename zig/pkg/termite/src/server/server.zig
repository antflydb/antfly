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

// HTTP API server for Termite, matching the Go Termite OpenAPI spec.
// Uses generated types and server router from openapi-zig.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const api = @import("termite_api");
const scraping = @import("antfly_scraping");
const jsonschema = @import("antfly_jsonschema");
const lib_chunker = @import("termite_chunker");
const backends_mod = @import("../backends/backends.zig");
const session_factory = @import("../architectures/session_factory.zig");
const registry_mod = @import("../registry/registry.zig");
const extractors_mod = @import("../extractors/extractor.zig");
const cache_mod = @import("../cache/cache.zig");
const model_manager_mod = @import("model_manager.zig");
const model_caps = @import("../models/capabilities.zig");
const manifest_mod = @import("../models/manifest.zig");
const gpt_model_mod = @import("../models/gpt.zig");
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

pub const NodeConfig = struct {
    models_dir: []const u8 = "./models",
    content_security: ?scraping.ContentSecurityConfig = null,
    s3_credentials: ?scraping.S3CredentialsConfig = null,
    keep_alive_ms: u64 = 300_000,
    max_loaded_models: usize = 10,
    max_concurrent_requests: usize = 32,
    pool_size: usize = 2,
    generation_budget_overrides: BudgetOverrides = .{},
};

pub const public_api_prefix = "/ml/v1";

const GenerateBackendSelection = struct {
    native_choice: native_backend_choice.Choice = .auto,
    compiled_partition_backend: ?ops.BackendKind = null,
    compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = .partitioned,
    graph_mode_requested: bool = false,
};

fn parseGenerateBackendSelection(
    backend_value: ?[]const u8,
    mode_value: ?[]const u8,
    compiled_target_value: ?[]const u8,
) !GenerateBackendSelection {
    const choice = if (backend_value) |value|
        native_backend_choice.parse(value) orelse return error.InvalidBackend
    else
        native_backend_choice.Choice.auto;
    try native_backend_choice.validate(choice);

    const compiled_mode_requested = if (mode_value) |value| blk: {
        if (std.mem.eql(u8, value, "eager")) break :blk false;
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
    };
}

fn configureGenerateBackendPreference(
    session_manager: *backends_mod.SessionManager,
    selection: GenerateBackendSelection,
) void {
    native_backend_choice.configureSessionPreference(session_manager, selection.native_choice);
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

fn embedTimingStart() u128 {
    if (!embedTimingEnabled()) return 0;
    return embedTimingNowNs();
}

fn logEmbedTiming(phase: []const u8, count: usize, start_ns: u128) void {
    if (start_ns == 0) return;
    const now = embedTimingNowNs();
    const elapsed_us = if (now > start_ns) @divTrunc(now - start_ns, 1000) else 0;
    std.log.info("termite embed timing phase={s} count={d} elapsed_us={d}", .{ phase, count, elapsed_us });
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

fn countTokenizerTokens(allocator: std.mem.Allocator, tokenizer: anytype, text: []const u8) !usize {
    const ids = try tokenizer.encode(allocator, text);
    defer allocator.free(ids);
    return ids.len;
}

fn countTokenizerTexts(allocator: std.mem.Allocator, tokenizer: anytype, texts: []const []const u8) !usize {
    var total: usize = 0;
    for (texts) |text| total += try countTokenizerTokens(allocator, tokenizer, text);
    return total;
}

fn countParsedDenseEmbedTextTokens(
    allocator: std.mem.Allocator,
    tokenizer: anytype,
    inputs: *const ParsedDenseEmbedInputs,
) usize {
    var total: usize = 0;
    for (inputs.texts.items) |item| {
        total += countTokenizerTokens(allocator, tokenizer, item.text) catch estimateTextTokens(item.text);
    }
    return total;
}

fn estimateParsedDenseEmbedPromptTokens(inputs: *const ParsedDenseEmbedInputs) usize {
    var total: usize = 0;
    for (inputs.texts.items) |item| total += estimateTextTokens(item.text);
    return total;
}

fn isOpenAiListTask(task: []const u8) bool {
    return std.mem.eql(u8, task, "generators") or std.mem.eql(u8, task, "embedders");
}

fn appendOpenAiModelEntry(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    model_id: []const u8,
    created: i64,
) !void {
    try buf.appendSlice(allocator, "{\"id\":");
    try jsonEncodeString(buf, allocator, model_id);
    const metadata = try std.fmt.allocPrint(
        allocator,
        ",\"object\":\"model\",\"created\":{d},\"owned_by\":\"termite\"}}",
        .{created},
    );
    defer allocator.free(metadata);
    try buf.appendSlice(allocator, metadata);
}

const model_listing_tasks = [_][]const u8{
    "embedders",  "rerankers",   "chunkers",
    "generators", "recognizers", "classifiers",
    "rewriters",  "readers",     "transcribers",
    "extractors",
};

const DiscoveredModelListing = struct {
    entry: registry_mod.ModelEntry,
    manifest: manifest_mod.ModelManifest,
    reader_supported: bool,

    fn deinit(self: *@This()) void {
        self.manifest.deinit();
    }

    fn listingKindName(self: @This()) []const u8 {
        if (self.entry.kind == .extractor) return @tagName(self.entry.kind);
        return @tagName(self.manifest.model_type);
    }
};

fn buildDiscoveredModelListings(
    allocator: std.mem.Allocator,
    discovered: []const registry_mod.ModelEntry,
) ![]DiscoveredModelListing {
    var listings = std.ArrayListUnmanaged(DiscoveredModelListing).empty;
    errdefer {
        for (listings.items) |*listing| listing.deinit();
        listings.deinit(allocator);
    }

    for (discovered) |entry| {
        var manifest = manifest_mod.loadListingFromDir(allocator, entry.path) catch continue;

        if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) {
            manifest.deinit();
            continue;
        }

        const kind_name = if (entry.kind == .extractor) @tagName(entry.kind) else @tagName(manifest.model_type);
        const reader_candidate = taskMatchesModelListing(
            "readers",
            kind_name,
            manifest.gliner_model_type,
            manifest.tasks,
            manifest.capabilities,
        );
        const reader_supported = !reader_candidate or readers_mod.isSupportedModelDir(allocator, entry.path);

        listings.append(allocator, .{
            .entry = entry,
            .manifest = manifest,
            .reader_supported = reader_supported,
        }) catch |err| {
            manifest.deinit();
            return err;
        };
    }

    return try listings.toOwnedSlice(allocator);
}

fn deinitDiscoveredModelListings(allocator: std.mem.Allocator, listings: []DiscoveredModelListing) void {
    for (listings) |*listing| listing.deinit();
    allocator.free(listings);
}

const ModelCounts = struct {
    embedders: usize = 0,
    rerankers: usize = 0,
    chunkers: usize = 0,
    generators: usize = 0,
    recognizers: usize = 0,
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
            self.recognizers +
            self.classifiers +
            self.rewriters +
            self.readers +
            self.transcribers +
            self.extractors;
    }
};

fn incrementModelCount(counts: *ModelCounts, task: []const u8) void {
    if (std.mem.eql(u8, task, "embedders")) counts.embedders += 1 else if (std.mem.eql(u8, task, "rerankers")) counts.rerankers += 1 else if (std.mem.eql(u8, task, "chunkers")) counts.chunkers += 1 else if (std.mem.eql(u8, task, "generators")) counts.generators += 1 else if (std.mem.eql(u8, task, "recognizers")) counts.recognizers += 1 else if (std.mem.eql(u8, task, "classifiers")) counts.classifiers += 1 else if (std.mem.eql(u8, task, "rewriters")) counts.rewriters += 1 else if (std.mem.eql(u8, task, "readers")) counts.readers += 1 else if (std.mem.eql(u8, task, "transcribers")) counts.transcribers += 1 else if (std.mem.eql(u8, task, "extractors")) counts.extractors += 1;
}

fn collectModelCounts(node: *Node, allocator: std.mem.Allocator, io: std.Io) ModelCounts {
    var counts = ModelCounts{};

    const ra = node.registry.allocator;
    const discovered = node.registry.discoverShallow(io) catch &[_]registry_mod.ModelEntry{};
    defer {
        for (discovered) |entry| {
            ra.free(entry.name);
            ra.free(entry.path);
        }
        if (discovered.len > 0) ra.free(discovered);
    }

    const listings = buildDiscoveredModelListings(allocator, discovered) catch return counts;
    defer deinitDiscoveredModelListings(allocator, listings);

    for (listings) |listing| {
        for (model_listing_tasks) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (std.mem.eql(u8, task, "readers") and !listing.reader_supported) continue;
            if (taskMatchesModelListing(task, listing.listingKindName(), listing.manifest.gliner_model_type, listing.manifest.tasks, listing.manifest.capabilities)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    var it = node.model_manager.loaded.iterator();
    while (it.next()) |entry| {
        var already_listed = false;
        for (discovered) |d| {
            if (std.mem.eql(u8, d.path, entry.key_ptr.*)) {
                already_listed = true;
                break;
            }
        }
        if (already_listed) continue;

        const model = entry.value_ptr.*;
        const model_task = @tagName(model.manifest.model_type);
        for (model_listing_tasks) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (taskMatchesModelListing(task, model_task, model.manifest.gliner_model_type, model.manifest.tasks, model.manifest.capabilities)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    return counts;
}

fn collectDiscoveredModelCounts(models_dir: []const u8, allocator: std.mem.Allocator, io: std.Io) ModelCounts {
    var counts = ModelCounts{};

    var registry = registry_mod.ModelRegistry.init(allocator, models_dir);
    defer registry.deinit();
    const discovered = registry.discoverShallow(io) catch &[_]registry_mod.ModelEntry{};
    defer {
        for (discovered) |entry| {
            allocator.free(entry.name);
            allocator.free(entry.path);
        }
        if (discovered.len > 0) allocator.free(discovered);
    }

    const listings = buildDiscoveredModelListings(allocator, discovered) catch return counts;
    defer deinitDiscoveredModelListings(allocator, listings);

    for (listings) |listing| {
        for (model_listing_tasks) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (std.mem.eql(u8, task, "readers") and !listing.reader_supported) continue;
            if (taskMatchesModelListing(task, listing.listingKindName(), listing.manifest.gliner_model_type, listing.manifest.tasks, listing.manifest.capabilities)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    return counts;
}

pub const Node = struct {
    config: NodeConfig,
    allocator: std.mem.Allocator,
    session_manager: backends_mod.SessionManager,
    model_manager: model_manager_mod.ModelManager,
    registry: registry_mod.ModelRegistry,
    embed_cache: cache_mod.ResultCache([]const f32),
    metrics: metrics_mod.Metrics,
    request_queue: request_queue_mod.RequestQueue,

    pub const DirectSparseEmbedding = sparse_embedding_mod.SparseVector;

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Node {
        return .{
            .config = config,
            .allocator = allocator,
            .session_manager = backends_mod.SessionManager.init(allocator),
            .model_manager = model_manager_mod.ModelManager.init(allocator, backends_mod.SessionManager.init(allocator)),
            .registry = registry_mod.ModelRegistry.init(allocator, config.models_dir),
            .embed_cache = cache_mod.ResultCache([]const f32).init(allocator, 120_000),
            .metrics = metrics_mod.Metrics.default,
            .request_queue = request_queue_mod.RequestQueue.init(config.max_concurrent_requests),
        };
    }

    pub fn deinit(self: *Node) void {
        self.model_manager.deinit();
        self.registry.deinit();
        self.embed_cache.deinit();
    }

    pub fn embedDenseTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![][]f32 {
        if (texts.len == 0) return try allocator.alloc([]f32, 0);
        try self.request_queue.acquire();
        defer self.releaseSlot();
        self.metrics.incRequest("embed.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "embedders");
        const model = try self.model_manager.loadFromDir(model_path);
        if (model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
        try model.ensureEmbeddingAssets(true, false, false);

        var pipeline = model.embeddingPipeline(allocator);
        return try pipeline.embed(texts);
    }

    pub fn embedSparseTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![]DirectSparseEmbedding {
        if (texts.len == 0) return try allocator.alloc(DirectSparseEmbedding, 0);
        try self.request_queue.acquire();
        defer self.releaseSlot();
        self.metrics.incRequest("embed_sparse.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "embedders");
        const model = try self.model_manager.loadFromDir(model_path);
        if (!model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
        var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
            .allocator = allocator,
            .session = model.session,
            .tok = model.getTokenizer(),
            .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
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
        defer self.releaseSlot();
        self.metrics.incRequest("rerank.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "rerankers");
        const model = try self.model_manager.loadFromDir(model_path);
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

        const configured_max_tokens: i32 = 256;
        const queue_units = self.estimateGenerateQueueUnits(messages, configured_max_tokens);
        try self.request_queue.acquireUnits(queue_units);
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("generate.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "generators");
        const model = try self.model_manager.loadFromDir(model_path);
        const gpt_config = session_factory.getGptConfig(model.session) orelse return error.UnsupportedGeneratorProvider;
        const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .mlx => .mlx,
            .cuda => .cuda,
            .pjrt, .onnx, .wasm => return error.UnsupportedGeneratorProvider,
        };
        const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);

        var kv_manager = runtime.kv.manager.KvManager.init(allocator);
        defer kv_manager.deinit();
        var cb = try session_factory.getComputeBackend(model.session, allocator);
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
        var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
        defer decode_state.deinit();

        var pipeline = generation.NativeGenerationPipeline{
            .allocator = allocator,
            .io = io,
            .cb = cb,
            .gpt_config = gpt_config,
            .tokenizer = model.getTokenizer(),
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .chat_template = model.chat_tmpl,
            .model_dir = model_path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
        };
        var result = try pipeline.generate(messages, .{ .max_tokens = configured_max_tokens, .prefill_chunk_size = 256 });
        defer result.deinit();
        return try allocator.dupe(u8, result.text);
    }

    /// Resolve a model name to a directory path.
    /// Supports: absolute path, "hf:owner/name:variant", "owner/name", variant resolution.
    /// Matches Go termite's resolveModel: exact match → re-scan → variant resolution.
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
                if (registry_mod.resolveVariant(self.allocator, io, task_dir, name_without_variant)) |variant_path| {
                    return variant_path;
                }
            }

            // Variant resolution: look for "name-{suffix}" with shortest suffix wins
            if (registry_mod.resolveVariant(self.allocator, io, self.config.models_dir, name_without_variant)) |variant_path| {
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
                if (registry_mod.resolveVariant(self.allocator, io, self.config.models_dir, model_only)) |variant_path| {
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
        self.request_queue.acquireUnits(units) catch {
            self.metrics.incError();
            const resp = try ctx.status(503).json(.{
                .@"error" = "SERVICE_UNAVAILABLE",
                .message = "server at capacity, try again later",
            });
            return resp;
        };
        self.metrics.setQueueDepth(self.request_queue.depth());
        return null;
    }

    fn releaseSlot(self: *Node) void {
        self.releaseSlotUnits(1);
    }

    fn releaseSlotUnits(self: *Node, units: usize) void {
        self.request_queue.releaseUnits(units);
        self.metrics.setQueueDepth(self.request_queue.depth());
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

        // Extract inputs from the polymorphic input field, separated by modality.
        var texts = std.ArrayListUnmanaged([]const u8).empty;
        defer texts.deinit(ctx.allocator);
        var images = std.ArrayListUnmanaged([]const u8).empty; // raw decoded bytes
        defer {
            for (images.items) |img| ctx.allocator.free(img);
            images.deinit(ctx.allocator);
        }
        var audio_clips = std.ArrayListUnmanaged(embedding_mod.EncodedAudioClip).empty;
        defer {
            for (audio_clips.items) |clip| ctx.allocator.free(clip.bytes);
            audio_clips.deinit(ctx.allocator);
        }

        switch (request.input) {
            .array => |arr| {
                for (arr.items) |item| {
                    switch (item) {
                        .string => |s| try texts.append(ctx.allocator, s),
                        .object => |obj| {
                            // ContentPart: discriminate by "type" field
                            const type_val = obj.get("type") orelse return ctx.status(400).json(.{
                                .@"error" = "INVALID_REQUEST",
                                .message = "content part missing 'type' field",
                            });
                            if (type_val != .string) return ctx.status(400).json(.{
                                .@"error" = "INVALID_REQUEST",
                                .message = "content part 'type' must be a string",
                            });
                            const ctype = type_val.string;
                            if (std.mem.eql(u8, ctype, "text")) {
                                const text_val = obj.get("text") orelse return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "text content part missing 'text' field",
                                });
                                if (text_val != .string) return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "text content part 'text' must be a string",
                                });
                                try texts.append(ctx.allocator, text_val.string);
                            } else if (std.mem.eql(u8, ctype, "image_url")) {
                                const url_obj = obj.get("image_url") orelse return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "image_url content part missing 'image_url' field",
                                });
                                const url_str = if (url_obj == .object)
                                    if (url_obj.object.get("url")) |u| (if (u == .string) u.string else null) else null
                                else if (url_obj == .string) url_obj.string else null;

                                const url = url_str orelse return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "image_url must contain a 'url' string",
                                });
                                const downloaded = downloadRemoteContent(self, ctx.allocator, url) catch
                                    return ctx.status(400).json(.{
                                        .@"error" = "INVALID_REQUEST",
                                        .message = "failed to download image_url content",
                                    });
                                defer ctx.allocator.free(downloaded.content_type);
                                try images.append(ctx.allocator, downloaded.data);
                            } else if (std.mem.eql(u8, ctype, "media")) {
                                const data_val = obj.get("data") orelse return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "media content part missing 'data' field",
                                });
                                if (data_val != .string) return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "media 'data' must be a base64 string",
                                });
                                const mime_val = obj.get("mime_type");
                                const is_audio = if (mime_val) |mv|
                                    (if (mv == .string) std.mem.startsWith(u8, mv.string, "audio/") else false)
                                else
                                    false;
                                const is_image = if (mime_val) |mv|
                                    (if (mv == .string) std.mem.startsWith(u8, mv.string, "image/") else false)
                                else
                                    false;

                                const decoded_payload = decodeMediaData(ctx.allocator, data_val.string) catch
                                    return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid base64 data" });
                                const decoded = decoded_payload.data;
                                errdefer ctx.allocator.free(decoded);
                                if (!mediaMimeMatches(if (mime_val) |mv| if (mv == .string) mv.string else null else null, decoded_payload.mime_type)) {
                                    ctx.allocator.free(decoded);
                                    return ctx.status(400).json(.{
                                        .@"error" = "INVALID_REQUEST",
                                        .message = "media data URI mime_type does not match content part mime_type",
                                    });
                                }

                                if (is_audio) {
                                    const mime_str = if (mime_val) |mv| if (mv == .string) mv.string else null else null;
                                    const decode_options = audio_mod.DecodeOptions{
                                        .mime_hint = mime_str,
                                    };
                                    if (!audio_mod.canDecodeWithOptions(decoded, decode_options)) {
                                        ctx.allocator.free(decoded);
                                        return unsupportedAudioResponse(ctx, "unsupported audio media content");
                                    }
                                    try audio_clips.append(ctx.allocator, .{
                                        .bytes = decoded,
                                        .decode_options = decode_options,
                                    });
                                } else if (is_image) {
                                    try images.append(ctx.allocator, decoded);
                                } else {
                                    ctx.allocator.free(decoded);
                                    return ctx.status(400).json(.{
                                        .@"error" = "INVALID_REQUEST",
                                        .message = "media content part must have mime_type starting with 'audio/' or 'image/'",
                                    });
                                }
                            } else {
                                return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "unknown content part type",
                                });
                            }
                        },
                        else => return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "input array must contain strings or content part objects",
                        }),
                    }
                }
            },
            .string => |s| try texts.append(ctx.allocator, s),
            else => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "'input' must be a string or array of strings/content parts",
            }),
        }

        const total_inputs = texts.items.len + images.items.len + audio_clips.items.len;
        if (total_inputs == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "input is empty" });
        }

        // Resolve and load model.
        const model_name: ?[]const u8 = if (request.model.len > 0) request.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "embedders") catch
            return ctx.status(404).json(.{
                .@"error" = "MODEL_NOT_FOUND",
                .message = "model not found; specify 'model' as a path or owner/name",
            });

        const model = self.model_manager.loadFromDir(model_path) catch |err|
            return ctx.status(500).json(.{
                .@"error" = "MODEL_LOAD_FAILED",
                .message = @errorName(err),
            });

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
            };
            const sparse_vecs = pipeline.embed(sparse_texts) catch |err|
                return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
            defer {
                for (sparse_vecs) |*sv| @constCast(sv).deinit(ctx.allocator);
                ctx.allocator.free(sparse_vecs);
            }

            var arena = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena.deinit();
            const prompt_tokens = countTokenizerTexts(ctx.allocator, model.getTokenizer(), sparse_texts) catch estimateTextsTokens(sparse_texts);
            const response = try buildEmbedSparseResponse(arena.allocator(), request.model, sparse_vecs, prompt_tokens);
            return ctx.json(response);
        }

        var inputs = parseDenseEmbedInputs(self, ctx.allocator, &model.manifest, request.input) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedInputParseErrorMessage(err),
            });
        };
        defer inputs.deinit(ctx.allocator);

        if (inputs.total_count == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "input is empty" });
        }

        model.ensureEmbeddingAssets(
            inputs.texts.items.len > 0,
            inputs.images.items.len > 0,
            inputs.audio.items.len > 0,
        ) catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });

        var pipeline = model.embeddingPipeline(ctx.allocator);
        applyDenseEmbeddingRequestOptions(&pipeline, &model.manifest, request) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedRequestOptionErrorMessage(err),
            });
        };
        const pipeline_start = embedTimingStart();
        const embeddings = embedDenseInputs(ctx.allocator, &pipeline, &inputs) catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
        logEmbedTiming("embed.pipeline", inputs.total_count, pipeline_start);
        defer {
            for (embeddings) |e| ctx.allocator.free(e);
            ctx.allocator.free(embeddings);
        }
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const response_build_start = embedTimingStart();
        const prompt_tokens = if (inputs.texts.items.len > 0)
            countParsedDenseEmbedTextTokens(ctx.allocator, model.getTokenizer(), &inputs)
        else
            estimateParsedDenseEmbedPromptTokens(&inputs);
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
    }

    pub fn chunkText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.ChunkRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        const queue_units = self.estimateHttpRequestQueueUnits(ctx);
        if (try self.acquireSlotUnits(ctx, queue_units)) |resp| return resp;
        defer self.releaseSlotUnits(queue_units);
        self.metrics.incRequest("chunk");
        defer self.metrics.decActive();

        const input: lib_chunker.Input = blk: {
            if (body.input) |input_val| {
                switch (input_val) {
                    .string => |s| break :blk .{ .text = s },
                    .object => |obj| {
                        if (obj.get("type")) |type_val| {
                            if (type_val != .string) return ctx.status(400).json(.{
                                .@"error" = "INVALID_REQUEST",
                                .message = "content part 'type' must be a string",
                            });
                            if (std.mem.eql(u8, type_val.string, "text")) {
                                const text_val = obj.get("text") orelse return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "text content part missing 'text' field",
                                });
                                if (text_val != .string) return ctx.status(400).json(.{
                                    .@"error" = "INVALID_REQUEST",
                                    .message = "text content part 'text' must be a string",
                                });
                                break :blk .{ .text = text_val.string };
                            }
                        }

                        const data_val = obj.get("data") orelse return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "media content part missing 'data' field",
                        });
                        if (data_val != .string) return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "media 'data' must be a base64 string",
                        });
                        const mime_val = obj.get("mime_type") orelse return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "media content part missing 'mime_type' field",
                        });
                        if (mime_val != .string) return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "media 'mime_type' must be a string",
                        });
                        const decoded_payload = decodeMediaData(ctx.allocator, data_val.string) catch
                            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid base64 data" });
                        const decoded = decoded_payload.data;
                        errdefer ctx.allocator.free(decoded);
                        if (!mediaMimeMatches(mime_val.string, decoded_payload.mime_type)) {
                            ctx.allocator.free(decoded);
                            return ctx.status(400).json(.{
                                .@"error" = "INVALID_REQUEST",
                                .message = "media data URI mime_type does not match content part mime_type",
                            });
                        }
                        break :blk .{ .binary = .{
                            .mime_type = mime_val.string,
                            .data = decoded,
                        } };
                    },
                    else => return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "'input' must be a string or content part object",
                    }),
                }
            }
            // Fall back to deprecated 'text' field
            if (body.text) |t| break :blk .{ .text = t };
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "missing 'input' or 'text' field" });
        };

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

        const model = self.model_manager.loadFromDir(model_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });

        var pipeline = model.rerankingPipeline(ctx.allocator);
        const scores = pipeline.rerank(body.query, body.prompts) catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
        defer ctx.allocator.free(scores);

        const prompt_tokens =
            (countTokenizerTokens(ctx.allocator, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * body.prompts.len +
            (countTokenizerTexts(ctx.allocator, model.getTokenizer(), body.prompts) catch estimateTextsTokens(body.prompts));
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

        const model = self.model_manager.loadFromDir(model_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });

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
                return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
            defer ctx.allocator.free(scores);
            const prompt_tokens =
                (countTokenizerTokens(ctx.allocator, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * flat_texts.len +
                (countTokenizerTexts(ctx.allocator, model.getTokenizer(), flat_texts) catch estimateTextsTokens(flat_texts));
            return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
        }

        if (!(model.manifest.hasCapability("colqwen") or model.manifest.hasCapability("multimodal_late_interaction"))) {
            return ctx.status(400).json(.{
                .@"error" = "MODEL_NOT_SUPPORTED",
                .message = "model does not advertise multimodal late-interaction reranking capability",
            });
        }

        model.ensureVisionSession() catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
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
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
        defer query_encoded.deinit();

        const scores = try ctx.allocator.alloc(f32, parsed_docs.items.len);
        defer ctx.allocator.free(scores);

        for (parsed_docs.items, 0..) |doc, idx| {
            if (doc.images.len == 0) {
                var text_pipeline = model.rerankingPipeline(ctx.allocator);
                const text_scores = text_pipeline.rerank(body.query, &.{doc.text}) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
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
                else => return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) }),
            };
        }

        var doc_texts = try ctx.allocator.alloc([]const u8, parsed_docs.items.len);
        defer ctx.allocator.free(doc_texts);
        for (parsed_docs.items, 0..) |doc, idx| doc_texts[idx] = doc.text;
        const prompt_tokens =
            (countTokenizerTokens(ctx.allocator, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * doc_texts.len +
            (countTokenizerTexts(ctx.allocator, model.getTokenizer(), doc_texts) catch estimateTextsTokens(doc_texts));
        return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
    }

    pub fn generateContent(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.GenerateRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        self.metrics.incRequest("generate");
        defer self.metrics.decActive();

        // Resolve model
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "generators") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

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
            .prefill_chunk_size = 256,
            .cache_dtype = body.cache_dtype,
            .cache_compaction_ratio = body.cache_compaction_ratio,
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
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
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
                formatted_response_text = try coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text);
                if (formatted_response_text) |text| response_text = text;
            }

            return self.buildGenerateResponse(
                ctx,
                body.model,
                response_text,
                if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
                result.prompt_tokens,
                result.tokens_used,
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
                    return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
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
                    return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
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
                    formatted_response_text = try coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text);
                    if (formatted_response_text) |text| response_text = text;
                }

                return self.buildGenerateResponse(
                    ctx,
                    body.model,
                    response_text,
                    if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
                    result.prompt_tokens,
                    result.tokens_used,
                    parsed_tool_calls,
                );
            }
        }

        // Fall back to native generation (native/MLX with GPT arch forward pass)
        const model = if (backend_selection.native_choice != .auto) blk: {
            var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
            configureGenerateBackendPreference(&request_session_manager, backend_selection);
            break :blk self.model_manager.loadFromDirWithPreferredBackends(model_path, request_session_manager.preferred_backends, false) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
        } else self.model_manager.loadFromDir(model_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
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

        const gpt_config = session_factory.getGptConfig(model.session) orelse
            return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support generation (not a GPT-family model)",
            });
        const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .mlx => .mlx,
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
        const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
            .native => .cpu,
            .metal, .mlx, .cuda => .gpu,
        };
        const budget_limits = self.config.generation_budget_overrides.apply(session_factory.widenBudgetLimitsForSession(
            model.session,
            runtime.tier.memory.defaultLimitsForBackend(budget_backend_class),
        ));
        var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
        const admission_prefill_chunk = if (config.prefill_chunk_size > 0) config.prefill_chunk_size else 256;
        run_budget.reserveEstimate(runtime.tier.memory.estimateGptGeneration(
            backend_kind,
            kv_dtype,
            gpt_config,
            prompt_tokens,
            @intCast(@max(config.max_tokens, 1)),
            admission_prefill_chunk,
        )) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                return ctx.status(507).json(.{
                    .@"error" = "MEMORY_BUDGET_EXCEEDED",
                    .message = memoryBudgetExceededMessage(ctx.allocator, model.session, &run_budget),
                });
            }
            return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        };

        const tok = model.getTokenizer();
        var draft_cb: ?ops.ComputeBackend = null;
        defer if (draft_cb) |*cb_value| cb_value.deinit();
        var draft_gpt_config: ?@import("../models/gpt.zig").Config = null;
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
                const draft_model = if (backend_selection.native_choice != .auto) blk: {
                    var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
                    configureGenerateBackendPreference(&request_session_manager, backend_selection);
                    break :blk self.model_manager.loadFromDirWithPreferredBackends(draft_model_path, request_session_manager.preferred_backends, false) catch |err|
                        return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
                } else self.model_manager.loadFromDir(draft_model_path) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
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

                draft_cb = session_factory.getComputeBackendWithBudget(draft_model.session, ctx.allocator, &run_budget) catch |err| {
                    if (err == error.MemoryBudgetExceeded) {
                        return ctx.status(507).json(.{
                            .@"error" = "MEMORY_BUDGET_EXCEEDED",
                            .message = memoryBudgetExceededMessage(ctx.allocator, draft_model.session, &run_budget),
                        });
                    }
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                };
                draft_gpt_config = draft_cfg;
                config.draft_model = draft_model_path;
            }
        }

        var kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
        defer kv_manager.deinit();
        var draft_kv_manager: ?runtime.kv.manager.KvManager = null;
        defer if (draft_kv_manager) |*manager| manager.deinit();

        var cb = session_factory.getComputeBackendWithBudget(model.session, ctx.allocator, &run_budget) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                return ctx.status(507).json(.{
                    .@"error" = "MEMORY_BUDGET_EXCEEDED",
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
        const pool_id = kv_manager.addPool(.{
            .backend = backend_kind,
            .dtype = kv_dtype,
            .page_size_tokens = 16,
            .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
            .num_kv_heads = gpt_config.maxKvHeads(),
            .head_dim = gpt_config.maxHeadDim(),
            .sliding_window_size = sliding_window_size,
        }) catch |err|
            return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        var decode_state = generation.NativeDecodeState.initPaged(ctx.allocator, &kv_manager, pool_id, model.shared_moe_cache);
        defer decode_state.deinit();
        var draft_decode_state: ?generation.NativeDecodeState = null;
        defer if (draft_decode_state) |*state| state.deinit();

        if (draft_gpt_config) |draft_cfg| {
            // Draft model uses the same backend kind as the target — they run
            // on the same machine so the available backends are identical.
            const draft_backend_kind = backend_kind;
            const draft_kv_dtype: runtime.kv.pool.KvDType = switch (draft_backend_kind) {
                .native => .f32,
                .cuda => .f16,
                .metal, .mlx => if (draft_cfg.family == .gemma) .f32 else .f16,
            };
            draft_kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
            const draft_sliding_window_size: ?u32 = if (draft_cfg.position_encoding == .absolute)
                null
            else if (draft_cfg.sliding_window > 0)
                @intCast(draft_cfg.sliding_window)
            else
                null;
            const draft_pool_id = draft_kv_manager.?.addPool(.{
                .backend = draft_backend_kind,
                .dtype = draft_kv_dtype,
                .page_size_tokens = 16,
                .num_layers_packed = @intCast(draft_cfg.num_hidden_layers),
                .num_kv_heads = draft_cfg.num_key_value_heads,
                .head_dim = draft_cfg.hidden_size / draft_cfg.num_attention_heads,
                .sliding_window_size = draft_sliding_window_size,
            }) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
            draft_decode_state = generation.NativeDecodeState.initPaged(ctx.allocator, &draft_kv_manager.?, draft_pool_id, null);
        }

        const graph_mode = backend_selection.graph_mode_requested or
            backend_selection.compiled_partition_backend != null or
            graphModeEnabled();
        const use_scheduler = !graph_mode;
        var graph_cache = graph_mod.cache.GraphCache.init(ctx.allocator);
        defer graph_cache.deinit();

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
            .model_dir = model_path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
            .scheduler = if (use_scheduler) model.native_generate_coordinator else null,
            .scheduler_lease = if (use_scheduler) if (native_generate_lease) |*lease| lease else null else null,
            .draft_cb = if (draft_cb) |cb_value| cb_value else null,
            .draft_gpt_config = draft_gpt_config,
            .draft_decode_state = if (draft_decode_state) |*state| state else null,
            .graph_cache = if (graph_mode) &graph_cache else null,
            .compiled_partition_backend = backend_selection.compiled_partition_backend,
            .compiled_attachment_target = backend_selection.compiled_attachment_target,
            .pjrt_client = if (pjrt_client) |*client| client else null,
        };

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
            formatted_response_text = try coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text);
            if (formatted_response_text) |text| response_text = text;
        }

        return self.buildGenerateResponse(
            ctx,
            body.model,
            response_text,
            if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
            result.prompt_tokens,
            result.tokens_used,
            parsed_tool_calls,
        );
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
                const api_calls = try alloc.alloc(api.ToolCall, calls.len);
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

    pub fn recognizeEntities(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.RecognizeRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("recognize");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveModelPath(ctx.io, model_name, "recognizers") catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });

        if (rebel_mod.isRebelModel(ctx.allocator, model_path)) {
            return self.recognizeRebel(ctx, model_path, body);
        }

        const model = self.model_manager.loadFromDir(model_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });

        // Use GLiNER pipeline for GLiNER models, standard NER for BIO models
        if (model.isGlinerModel()) {
            return self.recognizeGliner(ctx, model, body);
        }

        if (body.relation_labels) |relation_labels| {
            if (relation_labels.len > 0) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model does not support relation extraction" });
            }
        }

        var pipeline = model.nerPipeline(ctx.allocator);
        const all_entities = pipeline.recognizeBatch(body.texts) catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
        defer {
            for (all_entities) |entities| {
                for (entities) |e| ctx.allocator.free(e.text);
                ctx.allocator.free(entities);
            }
            ctx.allocator.free(all_entities);
        }

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, try model.getCleanupHead(), body.texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveRecognizeOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildRecognizeResponse(ctx, body.model, resolved.entities, resolved.relations, body.texts);
        }

        return self.buildRecognizeResponse(ctx, body.model, entities_for_response, null, body.texts);
    }

    fn recognizeRebel(self: *Node, ctx: *httpx.Context, model_path: []const u8, body: api.RecognizeRequest) !httpx.Response {
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
        const hf_tokenizer = @import("termite_hf_tokenizer");

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
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });

        const dec_config = enc_dec_mod.loadDecoderConfig(ctx.allocator, model_path) catch enc_dec_mod.DecoderConfig{};
        if (dec_config.max_length > 0) config.max_length = dec_config.max_length;

        const sessions = blk: {
            var encoder_session = self.session_manager.loadModel(paths.encoder) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
            errdefer encoder_session.close();

            const decoder_session = self.session_manager.loadModel(paths.decoder) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });

            break :blk .{
                .encoder = encoder_session,
                .decoder = decoder_session,
            };
        };

        var pipeline = rebel_mod.RebelPipeline{
            .allocator = ctx.allocator,
            .enc_dec = .{
                .allocator = ctx.allocator,
                .encoder = sessions.encoder,
                .decoder = sessions.decoder,
                .config = dec_config,
            },
            .tokenizer = hf_tok.tokenizer(),
            .config = config,
        };
        defer pipeline.deinit();

        if (body.relation_labels) |relation_labels| {
            if (relation_labels.len > 0) {
                const extracted = pipeline.extractRelationsBatch(body.texts, body.labels, relation_labels) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
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
                    var resolved = try resolveRecognizeOutput(ctx.allocator, extracted.entities, extracted.relations, resolver_cfg);
                    defer resolved.deinit(ctx.allocator);
                    return self.buildRecognizeResponse(ctx, body.model, resolved.entities, resolved.relations, body.texts);
                }

                return self.buildRecognizeResponse(ctx, body.model, extracted.entities, extracted.relations, body.texts);
            }
        }

        const all_entities = pipeline.recognizeBatch(body.texts) catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
        defer {
            for (all_entities) |entities| {
                for (entities) |entity| ctx.allocator.free(entity.text);
                ctx.allocator.free(entities);
            }
            ctx.allocator.free(all_entities);
        }

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, null, body.texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveRecognizeOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildRecognizeResponse(ctx, body.model, resolved.entities, resolved.relations, body.texts);
        }

        return self.buildRecognizeResponse(ctx, body.model, entities_for_response, null, body.texts);
    }

    fn recognizeGliner(self: *Node, ctx: *httpx.Context, model: *model_manager_mod.LoadedModel, body: api.RecognizeRequest) !httpx.Response {
        var pipeline = model.glinerPipeline(ctx.allocator);

        // Parse labels from request body (or use defaults)
        const labels: ?[]const []const u8 = if (body.labels) |lbls| blk: {
            if (lbls.len > 0) {
                break :blk lbls;
            }
            break :blk null;
        } else null;

        const want_relations = if (body.relation_labels) |relation_labels| relation_labels.len > 0 else false;
        if (want_relations) {
            if (!model.supportsRelationExtraction() or !pipeline.supportsRelationExtraction()) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model does not support relation extraction" });
            }

            const relation_labels = body.relation_labels.?;
            const extracted = pipeline.extractRelationsBatch(body.texts, labels, relation_labels) catch |err|
                return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
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
                var resolved = try resolveRecognizeOutput(ctx.allocator, extracted.entities, extracted.relations, resolver_cfg);
                defer resolved.deinit(ctx.allocator);
                return self.buildRecognizeResponse(ctx, body.model, resolved.entities, resolved.relations, body.texts);
            }

            return self.buildRecognizeResponse(ctx, body.model, extracted.entities, extracted.relations, body.texts);
        }

        const all_entities = pipeline.recognizeBatch(body.texts, labels) catch |err|
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
        defer {
            for (all_entities) |entities| {
                for (entities) |e| ctx.allocator.free(e.text);
                ctx.allocator.free(entities);
            }
            ctx.allocator.free(all_entities);
        }

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, try model.getCleanupHead(), body.texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveRecognizeOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildRecognizeResponse(ctx, body.model, resolved.entities, resolved.relations, body.texts);
        }

        return self.buildRecognizeResponse(ctx, body.model, entities_for_response, null, body.texts);
    }

    fn buildRecognizeResponse(
        _: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        all_entities: []const []const @import("../pipelines/ner.zig").Entity,
        all_relations: ?[]const []const gliner_mod.Relation,
        input_texts: []const []const u8,
    ) !httpx.Response {
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const data = try alloc.alloc(api.RecognizeObject, all_entities.len);
        for (all_entities, 0..) |entities, ti| {
            const entity_objects = try alloc.alloc(api.RecognizeEntity, entities.len);
            for (entities, 0..) |e, ei| entity_objects[ei] = toApiEntity(e);

            const relations_inner: ?[]const api.Relation = if (all_relations) |rels_by_text| blk: {
                const relations = if (ti < rels_by_text.len) rels_by_text[ti] else &.{};
                const relation_objects = try alloc.alloc(api.Relation, relations.len);
                for (relations, 0..) |r, ri| relation_objects[ri] = .{
                    .head = toApiEntity(r.head),
                    .tail = toApiEntity(r.tail),
                    .label = r.label,
                    .score = r.score,
                };
                break :blk relation_objects;
            } else null;

            data[ti] = .{
                .object = "recognition",
                .index = @intCast(ti),
                .entities = entity_objects,
                .relations = relations_inner,
            };
        }

        return ctx.json(api.RecognizeResponse{
            .object = "list",
            .data = data,
            .model = model_name,
            .usage = tokenUsage(estimateTextsTokens(input_texts), 0),
        });
    }

    fn toApiEntity(entity: @import("../pipelines/ner.zig").Entity) api.RecognizeEntity {
        return .{
            .text = entity.text,
            .label = entity.label,
            .start = @intCast(entity.start),
            .end = @intCast(entity.end),
            .score = entity.score,
        };
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
            const model = self.model_manager.loadFromDir(model_path) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });

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
                return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
            defer {
                for (all_results) |r| ctx.allocator.free(r);
                ctx.allocator.free(all_results);
            }

            const prompt_tokens =
                (countTokenizerTexts(ctx.allocator, model.getTokenizer(), body.texts) catch estimateTextsTokens(body.texts)) +
                (countTokenizerTexts(ctx.allocator, model.getTokenizer(), body.labels) catch estimateTextsTokens(body.labels));
            return buildClassificationResponse(ctx, body.model, all_results, prompt_tokens);
        } else |_| {}

        if (self.resolveModelPath(ctx.io, model_name, "recognizers")) |model_path| {
            const model = self.model_manager.loadFromDir(model_path) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
            if (!model.isGlinerModel() or !model.supportsClassification()) {
                return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
            }

            var pipeline = model.glinerPipeline(ctx.allocator);
            const all_results = pipeline.classifyBatch(body.texts, body.labels, .{
                .threshold = 0.0,
                .multi_label = body.multi_label orelse false,
            }) catch |err| switch (err) {
                error.MissingSpecialTokenIds => return ctx.status(500).json(.{ .@"error" = "MODEL_CONFIG_INVALID", .message = @errorName(err) }),
                else => return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) }),
            };
            defer {
                for (all_results) |r| ctx.allocator.free(r);
                ctx.allocator.free(all_results);
            }

            const prompt_tokens =
                (countTokenizerTexts(ctx.allocator, model.getTokenizer(), body.texts) catch estimateTextsTokens(body.texts)) +
                (countTokenizerTexts(ctx.allocator, model.getTokenizer(), body.labels) catch estimateTextsTokens(body.labels));
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
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
        defer head.deinit();

        const num_tokens: usize = std.math.cast(usize, body.num_tokens) orelse
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "num_tokens out of range" });

        const input = document_classification.ExampleInput{
            .image_path = body.image_path,
            .num_tokens = num_tokens,
        };

        const features = document_classification.extractFeatures(ctx.allocator, input) catch |err| switch (err) {
            error.FileNotFound => return ctx.status(404).json(.{ .@"error" = "IMAGE_NOT_FOUND", .message = "image not found" }),
            else => return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) }),
        };

        const results = document_classification.classifyWithHead(ctx.allocator, &head, body.labels, input) catch |err| switch (err) {
            error.LabelCountMismatch => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "label count does not match checkpoint output width",
            }),
            error.FileNotFound => return ctx.status(404).json(.{ .@"error" = "IMAGE_NOT_FOUND", .message = "image not found" }),
            else => return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) }),
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
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
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
            else => return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) }),
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

        // Load encoder and decoder sessions via the session manager.
        // Sessions are owned by this handler: a close flag guards all exit
        // paths (both error returns and ctx.status non-error returns), and
        // the enclosing pipeline is kept as a plain value (no deinit) so
        // closes never run twice.
        var encoder_session: backends_mod.Session = undefined;
        var close_encoder = false;
        defer if (close_encoder) encoder_session.close();
        var decoder_session: backends_mod.Session = undefined;
        var close_decoder = false;
        defer if (close_decoder) decoder_session.close();

        encoder_session = self.session_manager.loadModel(paths.encoder) catch |err|
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
        close_encoder = true;

        decoder_session = self.session_manager.loadModel(paths.decoder) catch |err|
            return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
        close_decoder = true;

        // Parse decoder config
        const dec_config = enc_dec_mod.loadDecoderConfig(ctx.allocator, model_path) catch enc_dec_mod.DecoderConfig{};

        // Load tokenizer
        const hf_tokenizer = @import("termite_hf_tokenizer");
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
                return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
            defer result.deinit();

            const inner = try ctx.allocator.alloc([]const u8, 1);
            errdefer ctx.allocator.free(inner);
            inner[0] = try ctx.allocator.dupe(u8, result.text);
            completion_tokens += countTokenizerTokens(ctx.allocator, hf_tok.tokenizer(), result.text) catch estimateTextTokens(result.text);
            data[i] = .{
                .object = "rewrite",
                .index = @intCast(i),
                .texts = inner,
            };
            filled = i + 1;
        }

        const prompt_tokens = countTokenizerTexts(ctx.allocator, hf_tok.tokenizer(), body.inputs) catch estimateTextsTokens(body.inputs);
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
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("read");
        defer self.metrics.decActive();

        if (body.images.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'images' must not be empty" });
        }

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

        var completion_tokens: usize = 0;
        for (body.images, 0..) |img_url, i| {
            var downloaded = downloadRemoteContent(self, ctx.allocator, img_url.url) catch
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "failed to download image content",
                });
            defer downloaded.deinit(ctx.allocator);

            var result = reader.read(downloaded.data, .{
                .prompt = body.prompt,
                .max_tokens = if (body.max_tokens) |mt| @intCast(mt) else null,
            }) catch |err|
                return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
            defer result.deinit();

            completion_tokens += estimateTextTokens(result.text);
            results_out[i] = try toApiReadObject(alloc, result, i);
            filled = i + 1;
        }

        const prompt_tokens = if (body.prompt) |prompt| estimateTextTokens(prompt) * body.images.len else 0;
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
        const tokenizer_mod = @import("termite_tokenizer");
        const hf_tokenizer = @import("termite_hf_tokenizer");
        var encoder_session: backends_mod.Session = undefined;
        var decoder_session: backends_mod.Session = undefined;
        var close_encoder = false;
        defer if (close_encoder) encoder_session.close();
        var close_decoder = false;
        defer if (close_decoder) decoder_session.close();
        var tokenizer: tokenizer_mod.Tokenizer = undefined;
        var hf_tok_owned: ?*hf_tokenizer.HfTokenizer = null;
        defer if (hf_tok_owned) |hf_tok| hf_tok.deinitSelf();

        if (enc_dec_mod.findEncoderDecoderPaths(ctx.allocator, model_path)) |paths| {
            defer ctx.allocator.free(paths.encoder);
            defer ctx.allocator.free(paths.decoder);

            encoder_session = self.session_manager.loadModel(paths.encoder) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
            close_encoder = true;

            decoder_session = self.session_manager.loadModel(paths.decoder) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
            close_decoder = true;

            const tok_path = std.fmt.allocPrint(ctx.allocator, "{s}/tokenizer.json", .{model_path}) catch |err|
                return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
            defer ctx.allocator.free(tok_path);

            const tok_bytes = c_file.readFile(ctx.allocator, tok_path) catch |err|
                return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
            defer ctx.allocator.free(tok_bytes);

            hf_tok_owned = hf_tokenizer.HfTokenizer.loadFromBytes(ctx.allocator, tok_bytes) catch |err|
                return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = @errorName(err) });
            if (hf_tok_owned) |hf_tok| {
                tokenizer = hf_tok.tokenizer();
            }
        } else |_| {
            const model = self.model_manager.loadFromDir(model_path) catch |err|
                return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) });
            if (session_factory.getWhisperConfig(model.session) == null) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_MODEL",
                    .message = "model does not support transcription",
                });
            }
            encoder_session = model.session;
            decoder_session = model.session;
            tokenizer = model.getTokenizer();
        }

        // Parse decoder config for Whisper token IDs
        const dec_config = enc_dec_mod.loadDecoderConfig(ctx.allocator, model_path) catch enc_dec_mod.DecoderConfig{};

        // Parse forced_decoder_ids from generation_config.json
        const forced_ids = loadForcedDecoderIds(ctx.allocator, model_path);
        defer if (forced_ids) |f| ctx.allocator.free(f);

        const transcription = @import("../pipelines/transcription.zig");
        var pipeline = transcription.TranscriptionPipeline.init(
            ctx.allocator,
            encoder_session,
            decoder_session,
            tokenizer,
            .{
                .max_length = dec_config.max_length,
                .decoder_start_token_id = dec_config.decoder_start_token_id,
                .eos_token_id = dec_config.eos_token_id,
                .language = body.language,
                .forced_decoder_ids = forced_ids,
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
            return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = @errorName(err) });
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
            .usage = tokenUsage(0, countTokenizerTokens(ctx.allocator, tokenizer, result.text) catch estimateTextTokens(result.text)),
        });
    }

    pub fn extractJSON(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.ExtractRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("extract");
        defer self.metrics.decActive();

        if (body.model.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model is required" });
        }
        if (body.schema.map.count() == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "schema is required" });
        }

        const texts = body.texts orelse &.{};
        const images = body.images orelse &.{};
        const has_texts = texts.len > 0;
        const has_images = images.len > 0;
        if (has_texts == has_images) {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "exactly one of texts or images must be provided",
            });
        }
        const schemas = extraction_mod.parseSchemas(ctx.allocator, &body.schema) catch |err| {
            const message = try std.fmt.allocPrint(ctx.allocator, "invalid schema: {s}", .{@errorName(err)});
            defer ctx.allocator.free(message);
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = message });
        };
        defer {
            for (schemas) |*schema| schema.deinit(ctx.allocator);
            ctx.allocator.free(schemas);
        }

        const config = extraction_mod.ExtractionConfig{
            .threshold = body.threshold orelse 0.3,
            .flat_ner = body.flat_ner orelse true,
            .include_confidence = body.include_confidence orelse false,
            .include_spans = body.include_spans orelse false,
        };

        const extractor_ctx = extractors_mod.Context{
            .allocator = ctx.allocator,
            .io = ctx.io,
            .models_dir = self.config.models_dir,
            .session_manager = &self.session_manager,
            .model_manager = &self.model_manager,
        };
        var extractor = extractors_mod.resolve(extractor_ctx, body.model, has_images) catch
            return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
        defer extractor.deinit(ctx.allocator);

        const results = (if (has_texts)
            extractor.extractText(extractor_ctx, schemas, config, texts)
        else blk: {
            const image_datas = try self.downloadImagesForExtraction(ctx, images);
            defer {
                for (image_datas) |image_data| ctx.allocator.free(image_data);
                ctx.allocator.free(image_datas);
            }
            break :blk extractor.extractImages(extractor_ctx, schemas, config, image_datas, .{
                .prompt = body.prompt,
                .max_tokens = if (body.max_tokens) |mt| @intCast(mt) else null,
            });
        }) catch |err| switch (err) {
            error.UnsupportedInput => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = if (has_images)
                    "model does not support image extraction"
                else
                    "model does not support text extraction",
            }),
            error.ImageDownloadFailed => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "failed to download image content",
            }),
            error.InvalidModelForExtraction => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support extraction",
            }),
            error.NoReaderModelAvailable => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "no compatible reader model is available for image extraction",
            }),
            error.MultiStageReaderNotYetSupported => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "selected reader model uses unsupported OCR stages or configuration",
            }),
            error.ReadStageInferenceFailed => return ctx.status(500).json(.{
                .@"error" = "INFERENCE_FAILED",
                .message = "reader stage failed during extraction",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) }),
        };
        defer {
            for (results) |*result| result.deinit(ctx.allocator);
            ctx.allocator.free(results);
        }

        const prompt_tokens = if (has_texts)
            estimateTextsTokens(texts)
        else if (body.prompt) |prompt|
            estimateTextTokens(prompt) * images.len
        else
            0;
        return buildExtractionResponse(ctx, body.model, results, prompt_tokens);
    }

    fn downloadImagesForExtraction(self: *Node, ctx: *httpx.Context, images: []const api.ImageURL) ![][]const u8 {
        const image_datas = try ctx.allocator.alloc([]const u8, images.len);
        var initialized: usize = 0;
        errdefer {
            for (image_datas[0..initialized]) |image_data| ctx.allocator.free(image_data);
            ctx.allocator.free(image_datas);
        }

        for (images, 0..) |img_url, i| {
            var downloaded = downloadRemoteContent(self, ctx.allocator, img_url.url) catch
                return error.ImageDownloadFailed;
            defer downloaded.deinit(ctx.allocator);

            image_datas[i] = try ctx.allocator.dupe(u8, downloaded.data);
            initialized += 1;
        }

        return image_datas;
    }

    pub fn listModels(self: *Node, ctx: *httpx.Context) !httpx.Response {
        const a = ctx.allocator;
        var buf = std.ArrayListUnmanaged(u8).empty;
        defer buf.deinit(a);
        var body = std.ArrayListUnmanaged(u8).empty;
        defer body.deinit(a);
        var openai_data = std.ArrayListUnmanaged(u8).empty;
        defer openai_data.deinit(a);
        var openai_data_count: usize = 0;
        const list_created = completionCreatedTimestamp();

        // Discover models from filesystem registry
        const ra = self.registry.allocator;
        const discovered = self.registry.discoverShallow(ctx.io) catch &[_]registry_mod.ModelEntry{};
        defer {
            for (discovered) |entry| {
                ra.free(entry.name);
                ra.free(entry.path);
            }
            if (discovered.len > 0) ra.free(discovered);
        }

        const listings = try buildDiscoveredModelListings(a, discovered);
        defer deinitDiscoveredModelListings(a, listings);

        for (model_listing_tasks, 0..) |task, task_idx| {
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
            for (listings) |listing| {
                if (std.mem.eql(u8, task, "readers") and !listing.reader_supported) continue;
                if (!taskMatchesModelListing(task, listing.listingKindName(), listing.manifest.gliner_model_type, listing.manifest.tasks, listing.manifest.capabilities)) continue;

                if (model_count > 0) try body.append(a, ',');
                try jsonEncodeString(&body, a, listing.entry.name);
                try body.append(a, ':');
                try appendModelInfo(
                    &body,
                    a,
                    listing.listingKindName(),
                    listing.manifest.gliner_model_type,
                    listing.manifest.capabilities,
                    listing.manifest.inputs,
                    listing.manifest.visual_model_path != null or listing.manifest.visual_projection_path != null,
                    listing.manifest.audio_model_path != null or listing.manifest.audio_projection_path != null,
                );
                if (isOpenAiListTask(task)) {
                    if (openai_data_count > 0) try openai_data.append(a, ',');
                    try appendOpenAiModelEntry(&openai_data, a, listing.entry.name, list_created);
                    openai_data_count += 1;
                }
                model_count += 1;
            }

            // Add loaded models not yet listed (loaded by path, not discovered by name)
            var it = self.model_manager.loaded.iterator();
            while (it.next()) |entry| {
                const model = entry.value_ptr.*;
                const model_task = @tagName(model.manifest.model_type);
                if (!taskMatchesModelListing(task, model_task, model.manifest.gliner_model_type, model.manifest.tasks, model.manifest.capabilities)) continue;

                // Skip if already listed from discovery
                var already_listed = false;
                for (discovered) |d| {
                    if (std.mem.eql(u8, d.path, entry.key_ptr.*)) {
                        already_listed = true;
                        break;
                    }
                }
                if (!already_listed) {
                    if (model_count > 0) try body.append(a, ',');
                    try jsonEncodeString(&body, a, entry.key_ptr.*);
                    try body.append(a, ':');
                    try appendModelInfo(
                        &body,
                        a,
                        model_task,
                        model.manifest.gliner_model_type,
                        model.manifest.capabilities,
                        model.manifest.inputs,
                        model.manifest.visual_model_path != null or model.manifest.visual_projection_path != null,
                        model.manifest.audio_model_path != null or model.manifest.audio_projection_path != null,
                    );
                    if (isOpenAiListTask(task)) {
                        if (openai_data_count > 0) try openai_data.append(a, ',');
                        try appendOpenAiModelEntry(&openai_data, a, entry.key_ptr.*, list_created);
                        openai_data_count += 1;
                    }
                    model_count += 1;
                }
            }

            try body.append(a, '}');
        }
        try buf.appendSlice(a, "{\"object\":\"list\",\"data\":[");
        try buf.appendSlice(a, openai_data.items);
        try buf.appendSlice(a, "],");
        try buf.appendSlice(a, body.items);
        try buf.append(a, '}');

        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(buf.items);
        return ctx.response.build();
    }

    pub fn getVersion(_: *Node, ctx: *httpx.Context) !httpx.Response {
        return ctx.json(.{
            .version = build_options.termite_version,
            .git_commit = build_options.git_commit,
            .build_time = build_options.build_time,
            .go_version = build_options.go_version,
            .allow_downloads = build_options.allow_downloads,
            .runtime = "termite-zig",
            .backends = .{
                .native = build_options.enable_native,
                .onnx = true,
                .onnx_runtime = build_options.enable_onnx,
                .mlx = build_options.enable_mlx,
                .wasm = build_options.enable_wasm,
            },
        });
    }

    /// Register termite API routes on an external server with a compile-time prefix.
    /// Used by swarm mode to register on the unified httpx.Server.
    pub fn registerRoutesOn(self: *Node, comptime prefix: []const u8, server: anytype) !void {
        const router = api.ServerRouter(Node).init(self);
        var prefixed = PrefixedServer(prefix, @TypeOf(server.*)){ .inner = server };
        try router.register(&prefixed);
        try server.get(prefix ++ "/metrics", metricsHandler);
        active_node = self;
        active_models_dir = self.config.models_dir;
    }

    fn registerRootOperationalRoutes(server: anytype) !void {
        try server.get("/healthz", healthzHandler);
        try server.get("/readyz", readyzHandler);
    }

    pub fn serve(self: *Node, allocator: std.mem.Allocator, io: std.Io, host: []const u8, port: u16) !void {
        var server = httpx.Server.initWithConfig(allocator, io, .{
            .host = host,
            .port = port,
            // Generation can legitimately take longer than the generic 30s HTTP
            // default during cold model startup or first-token MLX execution.
            .request_timeout_ms = 300_000,
            .keep_alive_timeout_ms = 300_000,
        });
        defer server.deinit();

        try self.registerRoutesOn(public_api_prefix, &server);
        try registerRootOperationalRoutes(&server);
        defer {
            active_node = null;
            active_models_dir = null;
        }

        try server.listen();
    }

    fn metricsHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        const node = active_node orelse return ctx.status(503).text("service unavailable");

        var writer: std.Io.Writer.Allocating = .init(ctx.allocator);
        defer writer.deinit();

        // Core metrics via prometheus lib
        try @constCast(&node.metrics).render(&writer.writer);

        // Scheduler metrics (computed on-the-fly from loaded models)
        const aggregate = runtime.scheduler.native_generate.aggregateStats(node.model_manager.loaded);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_waiting_requests", "gauge", "Waiting native scheduler requests across loaded models", aggregate.snapshot.waiting_requests);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_prefill_requests", "gauge", "Prefill-phase native scheduler requests across loaded models", aggregate.snapshot.prefill_requests);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_decode_requests", "gauge", "Decode-phase native scheduler requests across loaded models", aggregate.snapshot.decode_requests);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_active_units", "gauge", "Active native scheduler units across loaded models", aggregate.snapshot.active_units);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_step_batches_total", "counter", "Total unified scheduler steps (one fused forward pass per step)", aggregate.stats.step_batches_total);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_step_prefill_items_total", "counter", "Total prefill items packed into unified scheduler steps", aggregate.stats.step_prefill_items_total);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_step_decode_items_total", "counter", "Total decode items packed into unified scheduler steps", aggregate.stats.step_decode_items_total);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_step_query_tokens_total", "counter", "Total query tokens fused across unified scheduler steps", aggregate.stats.step_query_tokens_total);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_step_singleton_batches_total", "counter", "Total unified scheduler steps that contained only the leader item", aggregate.stats.step_singleton_batches_total);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_step_kv_block_skips_total", "counter", "Total pending items skipped due to per-step KV-block budget", aggregate.stats.step_kv_block_skips_total);
        try appendPromMetric(&writer.writer, "termite_native_scheduler_turn_yields_total", "counter", "Total cooperative scheduler yields while waiting for turns", aggregate.stats.turn_yields_total);
        try appendResidentProjectionMetrics(&writer.writer, aggregateResidentProjectionStats(node.model_manager.loaded));
        try appendGraphExecutorMetrics(&writer.writer, graph_mod.executor_stats.snapshot());

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
                .recognizers = 0,
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
                .recognizers = counts.recognizers,
                .classifiers = counts.classifiers,
                .rewriters = counts.rewriters,
                .readers = counts.readers,
                .transcribers = counts.transcribers,
                .extractors = counts.extractors,
            },
        });
    }
};

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

fn buildExtractionResponse(
    ctx: *httpx.Context,
    model_name: []const u8,
    all_results: []const extraction_mod.ExtractionResult,
    prompt_tokens: usize,
) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const data = try alloc.alloc(api.ExtractObject, all_results.len);
    for (all_results, 0..) |result, result_index| {
        var structures_map: std.json.ArrayHashMap([]const std.json.Value) = .{};
        try structures_map.map.ensureTotalCapacity(alloc, result.structures.len);
        for (result.structures) |structure| {
            const instances = try alloc.alloc(std.json.Value, structure.instances.len);
            for (structure.instances, 0..) |instance, instance_index| {
                var instance_obj: std.json.ObjectMap = .empty;
                try instance_obj.ensureTotalCapacity(alloc, instance.fields.len);
                for (instance.fields) |field| {
                    try instance_obj.put(alloc, field.name, try extractedFieldToValue(alloc, field.value));
                }
                instances[instance_index] = .{ .object = instance_obj };
            }
            structures_map.map.putAssumeCapacity(structure.name, instances);
        }
        data[result_index] = .{
            .object = "extraction",
            .index = @intCast(result_index),
            .results = structures_map,
        };
    }

    return ctx.json(api.ExtractResponse{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = tokenUsage(prompt_tokens, 0),
    });
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

const ResolvedRecognizeOutput = struct {
    entities: [][]@import("../pipelines/ner.zig").Entity,
    relations: ?[][]gliner_mod.Relation,

    fn deinit(self: *ResolvedRecognizeOutput, allocator: std.mem.Allocator) void {
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

fn resolveRecognizeOutput(
    allocator: std.mem.Allocator,
    all_entities: []const []const @import("../pipelines/ner.zig").Entity,
    all_relations: ?[]const []const gliner_mod.Relation,
    resolver_cfg: api.ResolverConfig,
) !ResolvedRecognizeOutput {
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
            try resolved_for_text.append(allocator, try cloneResolvedEntity(allocator, entity));
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
                try resolved_for_text.append(allocator, try cloneResolvedRelation(allocator, kg.entities, relation));
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
) !@import("../pipelines/ner.zig").Entity {
    const text = try allocator.dupe(u8, entity.canonical_name);
    errdefer allocator.free(text);
    const label = try allocator.dupe(u8, entity.label);
    errdefer allocator.free(label);

    return .{
        .text = text,
        .label = label,
        .start = 0,
        .end = 0,
        .score = entity.score,
    };
}

fn cloneResolvedRelation(
    allocator: std.mem.Allocator,
    entities: []const resolver_mod.ResolvedEntity,
    relation: resolver_mod.ResolvedRelation,
) !gliner_mod.Relation {
    const head_entity = findResolvedEntityById(entities, relation.head_id) orelse return error.InvalidResolvedRelation;
    const tail_entity = findResolvedEntityById(entities, relation.tail_id) orelse return error.InvalidResolvedRelation;

    const head_text = try allocator.dupe(u8, head_entity.canonical_name);
    errdefer allocator.free(head_text);
    const head_label = try allocator.dupe(u8, head_entity.label);
    errdefer allocator.free(head_label);
    const tail_text = try allocator.dupe(u8, tail_entity.canonical_name);
    errdefer allocator.free(tail_text);
    const tail_label = try allocator.dupe(u8, tail_entity.label);
    errdefer allocator.free(tail_label);
    const label = try allocator.dupe(u8, relation.label);
    errdefer allocator.free(label);

    return .{
        .head = .{
            .text = head_text,
            .label = head_label,
            .start = 0,
            .end = 0,
            .score = head_entity.score,
        },
        .tail = .{
            .text = tail_text,
            .label = tail_label,
            .start = 0,
            .end = 0,
            .score = tail_entity.score,
        },
        .label = label,
        .score = relation.score,
    };
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
    try appendPromMetric(writer, "termite_embed_resident_projection_text_success_total", "counter", "Total successful text resident projection attempts", stats.text_success);
    try appendPromMetric(writer, "termite_embed_resident_projection_text_fallback_total", "counter", "Total text resident projection fallbacks", stats.text_fallback);
    try appendPromMetric(writer, "termite_embed_resident_projection_image_success_total", "counter", "Total successful image resident projection attempts", stats.image_success);
    try appendPromMetric(writer, "termite_embed_resident_projection_image_fallback_total", "counter", "Total image resident projection fallbacks", stats.image_fallback);
    try appendPromMetric(writer, "termite_embed_resident_projection_audio_success_total", "counter", "Total successful audio resident projection attempts", stats.audio_success);
    try appendPromMetric(writer, "termite_embed_resident_projection_audio_fallback_total", "counter", "Total audio resident projection fallbacks", stats.audio_fallback);
}

fn appendGraphExecutorMetrics(writer: *std.Io.Writer, stats: graph_mod.executor_stats.ExecutionStats) !void {
    try appendPromMetric(writer, "termite_graph_executor_partitions_total", "counter", "Total graph executor partitions executed", stats.partitions_executed);
    try appendPromMetric(writer, "termite_graph_executor_cross_device_transfers_total", "counter", "Total graph executor cross-device transfers", stats.cross_device_transfers);
    try appendPromMetric(writer, "termite_graph_executor_runtime_input_transfers_total", "counter", "Total graph executor runtime input transfers", stats.runtime_input_transfers);
    try appendPromMetric(writer, "termite_graph_executor_device_resident_transfers_total", "counter", "Total graph executor device-resident transfers", stats.device_resident_transfers);
    try appendPromMetric(writer, "termite_graph_executor_backend_command_dispatches_total", "counter", "Total graph executor backend command dispatches", stats.backend_command_dispatches);
    try appendPromMetric(writer, "termite_graph_executor_planned_operator_dispatches_total", "counter", "Total graph executor planned operator dispatches", stats.planned_operator_dispatches);
    try appendPromMetric(writer, "termite_graph_executor_interpreter_fallbacks_total", "counter", "Total graph executor interpreter fallback partitions", stats.interpreter_fallbacks);
    try appendPromMetric(writer, "termite_graph_executor_device_resident_outputs_total", "counter", "Total graph executor device-resident outputs", stats.device_resident_outputs);
    try appendPromMetric(writer, "termite_graph_executor_host_materialized_outputs_total", "counter", "Total graph executor host-materialized outputs", stats.host_materialized_outputs);
    try appendPromMetric(writer, "termite_graph_executor_boundary_output_materializations_total", "counter", "Total graph executor boundary output materializations", stats.boundary_output_materializations);
    try appendPromMetric(writer, "termite_graph_executor_graph_plan_slots_reserved_total", "counter", "Total graph executor planned buffer slots reserved", stats.graph_plan_slots_reserved);
    try appendPromMetric(writer, "termite_graph_executor_graph_plan_bytes_reserved_total", "counter", "Total graph executor planned buffer bytes reserved", stats.graph_plan_bytes_reserved);
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
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_embed_resident_projection_text_success_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_embed_resident_projection_audio_fallback_total 6\n") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_graph_executor_partitions_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_graph_executor_interpreter_fallbacks_total 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_graph_executor_host_materialized_outputs_total 3\n") != null);
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
        else if (std.mem.eql(u8, task, "recognizers"))
            "recognize"
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
) !void {
    const inferred_classification = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "classification") and !model_caps.hasCapability(capabilities, "classification");
    const inferred_relations = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "relations") and !model_caps.hasCapability(capabilities, "relations");
    const inferred_extraction = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "extraction") and !model_caps.hasCapability(capabilities, "extraction");
    const has_known_inputs = model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "text") or
        model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "image") or
        model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "audio");

    if (capabilities.len == 0 and !inferred_classification and !inferred_relations and !inferred_extraction and !has_known_inputs) {
        try buf.appendSlice(allocator, "{}");
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
    try buf.appendSlice(allocator, "]}");
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
    try std.testing.expect(server.hasRoute(.get, public_api_prefix ++ "/models"));
    try std.testing.expect(server.hasRoute(.get, public_api_prefix ++ "/metrics"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/healthz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/readyz"));
}

test "root operational routes stay outside termite API prefix" {
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

test "taskMatchesModelListing derives extractors from recognizer capabilities" {
    try std.testing.expect(taskMatchesModelListing("recognizers", "recognizer", "", &.{}, &.{}));
    try std.testing.expect(taskMatchesModelListing("extractors", "extractor", "", &.{}, &.{}));
    try std.testing.expect(taskMatchesModelListing("extractors", "recognizer", "", &.{}, &.{"extraction"}));
    try std.testing.expect(taskMatchesModelListing("extractors", "reader", "", &.{}, &.{"extraction"}));
    try std.testing.expect(taskMatchesModelListing("extractors", "recognizer", "gliner2", &.{}, &.{"labels"}));
    try std.testing.expect(!taskMatchesModelListing("classifiers", "recognizer", "gliner2", &.{}, &.{"classification"}));
}

test "taskMatchesModelListing prefers explicit tasks when present" {
    try std.testing.expect(taskMatchesModelListing("generators", "generator", "", &.{"generate"}, &.{}));
    try std.testing.expect(taskMatchesModelListing("extractors", "generator", "", &.{"extract"}, &.{}));
    try std.testing.expect(!taskMatchesModelListing("recognizers", "generator", "", &.{"generate"}, &.{"extraction"}));
}

test "buildDiscoveredModelListings parses reusable model listing metadata" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/owner/embedder/onnx");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/embedder/model_manifest.json",
        .data =
        \\{"type":"embedder","tasks":["embed"],"capabilities":["sparse"],"inputs":["text"]}
        ,
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/embedder/onnx/model.onnx",
        .data = "",
    });
    try tmp.dir.createDirPath(io, "models/owner/not-loadable");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/not-loadable/model_manifest.json",
        .data =
        \\{"type":"generator","tasks":["generate"]}
        ,
    });

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models" });
    defer allocator.free(models_dir);
    const embedder_path = try std.fs.path.join(allocator, &.{ models_dir, "owner", "embedder" });
    defer allocator.free(embedder_path);
    const not_loadable_path = try std.fs.path.join(allocator, &.{ models_dir, "owner", "not-loadable" });
    defer allocator.free(not_loadable_path);

    const discovered = [_]registry_mod.ModelEntry{
        .{ .name = "owner/embedder", .kind = .embedder, .path = embedder_path, .variant = "f32" },
        .{ .name = "owner/not-loadable", .kind = .generator, .path = not_loadable_path, .variant = "f32" },
    };

    const listings = try buildDiscoveredModelListings(allocator, &discovered);
    defer deinitDiscoveredModelListings(allocator, listings);

    try std.testing.expectEqual(@as(usize, 1), listings.len);
    try std.testing.expectEqualStrings("owner/embedder", listings[0].entry.name);
    try std.testing.expect(listings[0].manifest.hasTask("embed"));
    try std.testing.expect(listings[0].manifest.hasCapability("sparse"));
    try std.testing.expect(listings[0].manifest.hasInput("text"));
    try std.testing.expect(taskMatchesModelListing(
        "embedders",
        listings[0].listingKindName(),
        listings[0].manifest.gliner_model_type,
        listings[0].manifest.tasks,
        listings[0].manifest.capabilities,
    ));
}

test "modelSupportsCapability infers gliner2 extraction and classification" {
    try std.testing.expect(model_caps.modelSupportsCapability("recognizer", "gliner2", &.{"labels"}, "classification"));
    try std.testing.expect(model_caps.modelSupportsCapability("recognizer", "gliner2", &.{"labels"}, "relations"));
    try std.testing.expect(model_caps.modelSupportsCapability("recognizer", "gliner2", &.{"labels"}, "extraction"));
    try std.testing.expect(!model_caps.modelSupportsCapability("recognizer", "", &.{"labels"}, "extraction"));
}

test "modelKindAcceptsInput infers text and image modalities" {
    try std.testing.expect(model_caps.modelKindAcceptsInput("recognizer", "gliner2", &.{}, false, false, "text"));
    try std.testing.expect(!model_caps.modelKindAcceptsInput("recognizer", "gliner2", &.{}, false, false, "image"));
    try std.testing.expect(model_caps.modelKindAcceptsInput("reader", "", &.{}, false, false, "image"));
    try std.testing.expect(!model_caps.modelKindAcceptsInput("reader", "", &.{}, false, false, "text"));
    try std.testing.expect(model_caps.modelKindAcceptsInput("embedder", "", &.{}, true, false, "image"));
    try std.testing.expect(model_caps.modelKindAcceptsInput("transcriber", "", &.{}, false, false, "audio"));
    try std.testing.expect(model_caps.modelKindAcceptsInput("recognizer", "", &.{"image"}, false, false, "image"));
    try std.testing.expect(!model_caps.modelKindAcceptsInput("recognizer", "", &.{"image"}, false, false, "text"));
}

test "generate backend selection keeps compiled mode explicit" {
    const eager_webgpu = parseGenerateBackendSelection("webgpu", null, null);
    if (build_options.enable_wasm and build_options.enable_webgpu) {
        const eager = try eager_webgpu;
        try std.testing.expectEqual(native_backend_choice.Choice.webgpu, eager.native_choice);
        try std.testing.expectEqual(@as(?ops.BackendKind, null), eager.compiled_partition_backend);
        try std.testing.expect(!eager.graph_mode_requested);

        const compiled = try parseGenerateBackendSelection("webgpu", "compiled", null);
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
    try std.testing.expectError(error.InvalidGenerateMode, parseGenerateBackendSelection(null, "graph", null));
    try std.testing.expectError(error.InvalidCompiledTarget, parseGenerateBackendSelection(null, "compiled", "full"));
}

test "download remote content accepts data uri" {
    const alloc = std.testing.allocator;
    const node = Node{
        .config = .{},
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
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
        .embed_cache = undefined,
        .metrics = undefined,
        .request_queue = undefined,
    };
    try std.testing.expectError(error.HostNotAllowed, downloadRemoteContent(&node, alloc, "https://example.com/a.png"));
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
};

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
    total_count: usize = 0,

    fn deinit(self: *ParsedDenseEmbedInputs, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        for (self.images.items) |item| allocator.free(item.bytes);
        self.images.deinit(allocator);
        for (self.audio.items) |item| allocator.free(item.bytes);
        self.audio.deinit(allocator);
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

    return .{
        .model = model_value.string,
        .input = input_value,
        .encoding_format = encoding_format,
        .dimensions = dimensions,
        .task_type = task_type orelse legacy_task_type,
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
                if (item == .string) {
                    if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
                    try parsed.texts.append(allocator, .{ .index = index, .text = item.string });
                    continue;
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
                    continue;
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
                    continue;
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
                        continue;
                    }

                    if (std.mem.startsWith(u8, mime_value.string, "audio/")) {
                        if (!model_caps.modelAcceptsInput(manifest, "audio")) return error.ModelDoesNotSupportAudioInput;
                        try parsed.audio.append(allocator, .{
                            .index = index,
                            .bytes = decoded,
                            .mime_type = mime_value.string,
                        });
                        continue;
                    }

                    return error.UnsupportedMediaMimeType;
                }

                return error.UnknownContentPartType;
            }

            parsed.total_count = arr.items.len;
        },
        else => return error.InputMustBeStringOrArrayOfStringsOrContentParts,
    }

    return parsed;
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

fn embedDenseInputs(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
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

    if (inputs.audio.items.len > 0) {
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

    for (filled) |was_filled| {
        if (!was_filled) return error.MissingEmbeddingResult;
    }

    return embeddings;
}

fn buildEmbedDenseResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    embeddings: []const []const f32,
    requested_dimensions: ?usize,
    prompt_tokens: usize,
) !api.EmbedResponse {
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

fn buildEmbedSparseResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    sparse_vecs: []const sparse_embedding_mod.SparseVector,
    prompt_tokens: usize,
) !api.EmbedResponse {
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

test "termite embeddings validates encoding format and dimensions" {
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

fn expectJsonNumber(expected: f64, value: std.json.Value) !void {
    const actual: f64 = switch (value) {
        .float => |float| float,
        .integer => |integer| @floatFromInt(integer),
        else => return error.ExpectedJsonNumber,
    };
    try std.testing.expectEqual(expected, actual);
}

test "termite embeddings dense response supports truncation" {
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

test "termite embeddings sparse response uses the shared embedding field" {
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

test "termite embed request parser accepts multimodal content parts" {
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

test "termite embed request parser accepts mixed strings and content parts" {
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

test "termite embed media-only usage does not require text tokens" {
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

test "termite embed parser accepts data uri media payloads" {
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

test "termite embed parser rejects mismatched data uri media mime type" {
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

test "termite sparse embed parser rejects multimodal content parts" {
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
            return try allocator.dupe(u8, "{}");
        defer parsed.deinit();
        if (parsed.value == .object) return null;
        return try allocator.dupe(u8, "{}");
    }
    if (!std.mem.eql(u8, rf.type, "json_schema")) return null;

    const schema_cfg = rf.json_schema orelse return error.MissingJsonSchema;
    validateGeneratedJsonSchema(allocator, json_text, schema_cfg) catch {
        const schema = schema_cfg.schema orelse return error.MissingJsonSchema;
        const fallback = try minimalJsonForSchema(allocator, schema);
        errdefer allocator.free(fallback);
        try validateGeneratedJsonSchema(allocator, fallback, schema_cfg);
        return fallback;
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

fn minimalJsonForSchema(allocator: std.mem.Allocator, schema: std.json.Value) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(allocator);
    try appendMinimalJsonForSchema(&buf, allocator, schema);
    return try buf.toOwnedSlice(allocator);
}

fn appendMinimalJsonForSchema(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    schema: std.json.Value,
) anyerror!void {
    if (schema != .object) {
        try buf.appendSlice(allocator, "null");
        return;
    }

    const obj = schema.object;
    if (obj.get("const")) |value| {
        const rendered = try std.json.Stringify.valueAlloc(allocator, value, .{});
        defer allocator.free(rendered);
        try buf.appendSlice(allocator, rendered);
        return;
    }
    if (obj.get("enum")) |values| {
        if (values == .array and values.array.items.len > 0) {
            const rendered = try std.json.Stringify.valueAlloc(allocator, values.array.items[0], .{});
            defer allocator.free(rendered);
            try buf.appendSlice(allocator, rendered);
            return;
        }
    }

    const type_name = if (obj.get("type")) |type_value|
        if (type_value == .string) type_value.string else null
    else
        null;

    if (type_name) |name| {
        if (std.mem.eql(u8, name, "object")) {
            try appendMinimalObjectForSchema(buf, allocator, obj);
            return;
        }
        if (std.mem.eql(u8, name, "array")) {
            try buf.appendSlice(allocator, "[]");
            return;
        }
        if (std.mem.eql(u8, name, "string")) {
            try buf.appendSlice(allocator, "\"\"");
            return;
        }
        if (std.mem.eql(u8, name, "integer") or std.mem.eql(u8, name, "number")) {
            try buf.append(allocator, '0');
            return;
        }
        if (std.mem.eql(u8, name, "boolean")) {
            try buf.appendSlice(allocator, "false");
            return;
        }
        if (std.mem.eql(u8, name, "null")) {
            try buf.appendSlice(allocator, "null");
            return;
        }
    }

    if (obj.get("properties") != null or obj.get("required") != null) {
        try appendMinimalObjectForSchema(buf, allocator, obj);
        return;
    }

    try buf.appendSlice(allocator, "null");
}

fn appendMinimalObjectForSchema(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    schema_obj: std.json.ObjectMap,
) anyerror!void {
    try buf.append(allocator, '{');
    var first = true;
    const properties = schema_obj.get("properties");
    if (schema_obj.get("required")) |required| {
        if (required == .array) {
            for (required.array.items) |name_value| {
                if (name_value != .string) continue;
                if (!first) try buf.append(allocator, ',');
                first = false;
                try jsonEncodeString(buf, allocator, name_value.string);
                try buf.append(allocator, ':');
                const property_schema = if (properties != null and properties.? == .object)
                    properties.?.object.get(name_value.string) orelse .null
                else
                    .null;
                try appendMinimalJsonForSchema(buf, allocator, property_schema);
            }
        }
    }
    try buf.append(allocator, '}');
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

/// Parse forced_decoder_ids from generation_config.json.
/// Returns null if file not found or no forced_decoder_ids field.
fn loadForcedDecoderIds(allocator: std.mem.Allocator, model_dir: []const u8) ?[]const [2]i32 {
    const path = std.fmt.allocPrint(allocator, "{s}/generation_config.json", .{model_dir}) catch return null;
    defer allocator.free(path);

    const data = c_file.readFile(allocator, path) catch return null;
    defer allocator.free(data);

    // Simple JSON extraction: find "forced_decoder_ids": [[1, 50362], ...]
    const key = "\"forced_decoder_ids\"";
    const key_pos = std.mem.indexOf(u8, data, key) orelse return null;
    const after_key = data[key_pos + key.len ..];

    // Skip whitespace and colon
    var pos: usize = 0;
    while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ':' or after_key[pos] == '\n' or after_key[pos] == '\r' or after_key[pos] == '\t')) pos += 1;

    if (pos >= after_key.len or after_key[pos] == 'n') return null; // null value

    if (after_key[pos] != '[') return null;
    pos += 1; // skip outer [

    var result = std.ArrayListUnmanaged([2]i32).empty;

    while (pos < after_key.len) {
        // Skip whitespace
        while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ',' or after_key[pos] == '\n' or after_key[pos] == '\r' or after_key[pos] == '\t')) pos += 1;
        if (pos >= after_key.len or after_key[pos] == ']') break;

        if (after_key[pos] != '[') break;
        pos += 1; // skip inner [

        // Parse first int
        while (pos < after_key.len and after_key[pos] == ' ') pos += 1;
        const first = parseJsonInt(after_key[pos..]) orelse break;
        pos += first.len;

        // Skip comma
        while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ',')) pos += 1;

        // Parse second int
        const second = parseJsonInt(after_key[pos..]) orelse break;
        pos += second.len;

        // Skip to closing ]
        while (pos < after_key.len and after_key[pos] != ']') pos += 1;
        if (pos < after_key.len) pos += 1; // skip ]

        result.append(allocator, .{ @intCast(first.value), @intCast(second.value) }) catch return null;
    }

    if (result.items.len == 0) {
        result.deinit(allocator);
        return null;
    }
    return result.toOwnedSlice(allocator) catch null;
}

const ParsedInt = struct { value: i64, len: usize };

fn parseJsonInt(s: []const u8) ?ParsedInt {
    if (s.len == 0) return null;
    var i: usize = 0;
    var neg = false;
    if (s[0] == '-') {
        neg = true;
        i = 1;
    }
    if (i >= s.len or s[i] < '0' or s[i] > '9') return null;
    var val: i64 = 0;
    while (i < s.len and s[i] >= '0' and s[i] <= '9') {
        val = val * 10 + @as(i64, s[i] - '0');
        i += 1;
    }
    return .{ .value = if (neg) -val else val, .len = i };
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
