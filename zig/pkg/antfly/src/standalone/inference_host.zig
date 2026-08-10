// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Embedded inference lifecycle and provider adapters. This module is compiled
//! once in the inference archive and used directly only by non-linked tests.

const std = @import("std");
const httpx = @import("httpx");
const antfly = @import("inference_host_root.zig");
const inference = @import("inference_server");
const inference_bridge = @import("inference_bridge.zig");

pub const LinkedInferenceState = struct {
    alloc: std.mem.Allocator,
    node: inference.server.Node,
    warm_models: ResolvedWarmModels,
};

const ResolvedWarmModels = struct {
    items: []const inference.server.WarmModel,

    fn deinit(self: *ResolvedWarmModels, alloc: std.mem.Allocator) void {
        if (self.items.len != 0) alloc.free(self.items);
        self.* = undefined;
    }
};

fn parseWarmModelKind(value: []const u8) ?inference.server.WarmModelKind {
    inline for (std.meta.fields(inference.server.WarmModelKind)) |field| {
        if (std.mem.eql(u8, value, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn convertWarmModels(
    alloc: std.mem.Allocator,
    context: *const inference_bridge.CreateContext,
    loaded_config: ?*const antfly.common.config.Config,
) !ResolvedWarmModels {
    if (context.preload_ptr) |preload_ptr| {
        const specs = preload_ptr[0..context.preload_len];
        if (specs.len != 0) {
            const out = try alloc.alloc(inference.server.WarmModel, specs.len);
            errdefer alloc.free(out);
            for (specs, 0..) |model, i| {
                out[i] = .{
                    .kind = parseWarmModelKind(model.kind.slice()) orelse return error.InvalidArguments,
                    .name = model.name.slice(),
                    .backend = antfly.inference_runtime.parseOptionalBackendType(model.backend.slice()) catch
                        return error.InvalidArguments,
                    .format = model.format.slice(),
                    .quantization = model.quantization.slice(),
                };
            }
            return .{ .items = out };
        }
    }

    const loaded = loaded_config orelse return .{ .items = &.{} };
    if (loaded.inference.preload.len == 0) return .{ .items = &.{} };
    const out = try alloc.alloc(inference.server.WarmModel, loaded.inference.preload.len);
    errdefer alloc.free(out);
    for (loaded.inference.preload, 0..) |model, i| {
        out[i] = .{
            .kind = parseWarmModelKind(model.kind) orelse return error.InvalidConfig,
            .name = model.name,
            .backend = antfly.inference_runtime.parseOptionalBackendType(model.backend) catch
                return error.InvalidConfig,
            .format = model.format,
            .quantization = model.quantization,
        };
    }
    return .{ .items = out };
}

pub fn parseKeepAliveMs(raw: []const u8) !u64 {
    if (std.mem.eql(u8, raw, "0")) return 0;
    if (raw.len == 0) return error.InvalidInferenceModelCacheConfig;
    var i: usize = 0;
    var total_ns: u64 = 0;
    while (i < raw.len) {
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        if (i == start) return error.InvalidInferenceModelCacheConfig;
        const value = std.fmt.parseUnsigned(u64, raw[start..i], 10) catch
            return error.InvalidInferenceModelCacheConfig;
        const unit_ns: u64 = if (std.mem.startsWith(u8, raw[i..], "ms")) blk: {
            i += 2;
            break :blk std.time.ns_per_ms;
        } else if (i < raw.len and raw[i] == 's') blk: {
            i += 1;
            break :blk std.time.ns_per_s;
        } else if (i < raw.len and raw[i] == 'm') blk: {
            i += 1;
            break :blk std.time.ns_per_min;
        } else if (i < raw.len and raw[i] == 'h') blk: {
            i += 1;
            break :blk std.time.ns_per_hour;
        } else return error.InvalidInferenceModelCacheConfig;
        const part_ns = std.math.mul(u64, value, unit_ns) catch
            return error.InvalidInferenceModelCacheConfig;
        total_ns = std.math.add(u64, total_ns, part_ns) catch
            return error.InvalidInferenceModelCacheConfig;
    }
    if (total_ns == 0) return 0;
    return @max(@as(u64, 1), total_ns / std.time.ns_per_ms);
}

test "standalone inference keep alive parses compound durations and zero" {
    try std.testing.expectEqual(@as(u64, 0), try parseKeepAliveMs("0"));
    try std.testing.expectEqual(@as(u64, 0), try parseKeepAliveMs("0s"));
    try std.testing.expectEqual(@as(u64, 90_000), try parseKeepAliveMs("1m30s"));
    try std.testing.expectError(
        error.InvalidInferenceModelCacheConfig,
        parseKeepAliveMs("forever"),
    );
}

/// Creates the standalone inference implementation inside its focused codegen
/// unit. The caller passes only ABI-safe launch settings, never CliConfig.
pub fn linkedInferenceCreate(context: *const inference_bridge.CreateContext) !*anyopaque {
    const init: *const std.process.Init = @ptrCast(@alignCast(context.init));
    const loaded_config: ?*const antfly.common.config.Config = if (context.loaded_config) |cfg|
        @ptrCast(@alignCast(cfg))
    else
        null;
    const data_dir = context.data_dir_ptr[0..context.data_dir_len];
    const alloc = init.gpa;

    const state = try alloc.create(LinkedInferenceState);
    errdefer alloc.destroy(state);
    var warm_models = try convertWarmModels(alloc, context, loaded_config);
    errdefer warm_models.deinit(alloc);

    var node_config = inference.server.NodeConfig{
        .models_dir = context.models_dir.slice() orelse
            if (loaded_config) |cfg| cfg.inference.models_dir orelse
                antfly.inference_runtime.defaultModelsDirForDataDir(alloc, data_dir) else antfly.inference_runtime.defaultModelsDirForDataDir(alloc, data_dir),
        .ml_dir = context.ml_dir.slice() orelse
            if (loaded_config) |cfg| cfg.inference.ml_dir orelse
                antfly.inference_runtime.defaultMlDirForDataDir(alloc, data_dir) else antfly.inference_runtime.defaultMlDirForDataDir(alloc, data_dir),
        .generation_budget_overrides = .{
            .host_limit_bytes = context.host_limit_bytes,
            .backend_limit_bytes = context.backend_limit_bytes,
            .combined_limit_bytes = context.combined_limit_bytes,
            .kv_limit_bytes = context.kv_limit_bytes,
            .scratch_limit_bytes = context.scratch_limit_bytes,
        },
        .preload = warm_models.items,
    };
    if (loaded_config) |cfg| {
        if (cfg.effectiveAntflyContentSecurity()) |security| node_config.content_security = security.*;
        if (cfg.inference.s3_credentials) |creds| node_config.s3_credentials = creds;
        if (cfg.inference.keep_alive) |value|
            node_config.keep_alive_ms = try parseKeepAliveMs(value);
        if (cfg.inference.max_loaded_models) |value|
            node_config.max_loaded_models =
                std.math.cast(usize, value) orelse return error.InvalidInferenceModelCacheConfig;
    }

    state.* = .{
        .alloc = alloc,
        .node = try inference.server.Node.init(alloc, node_config),
        .warm_models = warm_models,
    };
    return state;
}

pub fn linkedInferenceConfigure(context: *const inference_bridge.ConfigureContext) !void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context.handle));
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context.resource_manager));
    state.node.configureAdmissionResourceBudget(inferenceAdmissionResourceBudget(manager));
    state.node.config.prompt_cache_resource_usage_observer = promptCacheResourceUsageObserver(manager);
    try state.node.configureTokenizerCaches(.{
        .bulk_slots_per_shard = 16 * 1024,
        .resource_budget = tokenizerCacheResourceBudget(manager),
    });
    if (context.io) |io_ptr| {
        const io: *const std.Io = @ptrCast(@alignCast(io_ptr));
        state.node.attachIo(io.*);
    }
    state.node.warmConfiguredModels(state.alloc) catch |err| {
        std.log.err("standalone startup failed step=warm_inference_models err={}", .{err});
        return err;
    };
}

pub fn linkedInferenceProvider(context: *const inference_bridge.ProviderContext) void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context.handle));
    const provider: *antfly.inference.managed_embedder.AntflyProvider = @ptrCast(@alignCast(context.out_provider));
    provider.* = localAntflyProvider(&state.node);
}

pub fn linkedInferenceRegisterRoutes(context: *const inference_bridge.RoutesContext) !void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context.handle));
    const server: *httpx.Server = @ptrCast(@alignCast(context.server));
    try state.node.registerRoutesOn(inference.server.public_api_prefix, server);
    try state.node.registerAiRoutesOn(inference.server.ai_api_prefix, server);
}

pub fn linkedInferenceDestroy(handle: *anyopaque) void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(handle));
    const alloc = state.alloc;
    state.node.deinit();
    state.warm_models.deinit(alloc);
    alloc.destroy(state);
}

fn localAntflyProvider(node: *inference.server.Node) antfly.inference.managed_embedder.AntflyProvider {
    return .{
        .ptr = node,
        .embed_dense_texts = localAntflyEmbedDenseTexts,
        .embed_dense_texts_with_context = localAntflyEmbedDenseTextsWithContext,
        .embed_sparse_texts = localAntflyEmbedSparseTexts,
        .embed_dense_parts = localAntflyEmbedDenseParts,
        .embed_dense_parts_with_context = localAntflyEmbedDensePartsWithContext,
        .rerank_texts = localAntflyRerankTexts,
        .generate_text = localAntflyGenerateText,
        .generate_messages = localAntflyGenerateMessages,
        .read_images = localAntflyReadImages,
        .transcribe_audio = localAntflyTranscribeAudio,
        .extract = localAntflyExtract,
        .list_models_json = localAntflyListModelsJson,
    };
}

fn promptCacheResourceUsageObserver(manager: *antfly.resource_manager.ResourceManager) inference.runtime.kv.prompt_cache.ResourceUsageObserver {
    return .{
        .context = manager,
        .update = observePromptCacheResourceUsage,
    };
}

fn inferenceAdmissionResourceBudget(
    manager: *antfly.resource_manager.ResourceManager,
) inference.runtime.tier.memory.AdmissionResourceBudget {
    return .{
        .context = manager,
        .try_reserve = reserveInferenceAdmissionResources,
        .release = releaseInferenceAdmissionResources,
    };
}

fn inferenceAdmissionSliceAmounts(
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) ![3]antfly.resource_manager.SliceAmount {
    const model_residency = try std.math.add(
        usize,
        amounts.host_weight_bytes,
        amounts.backend_weight_bytes,
    );
    const kv_working_set = try std.math.add(
        usize,
        amounts.host_kv_bytes,
        amounts.backend_kv_bytes,
    );
    const scratch_working_set = try std.math.add(
        usize,
        amounts.host_scratch_bytes,
        amounts.backend_scratch_bytes,
    );
    return .{
        .{ .slice = .inference_model_residency, .bytes = @intCast(model_residency) },
        .{ .slice = .inference_kv_working_set, .bytes = @intCast(kv_working_set) },
        .{ .slice = .inference_scratch_working_set, .bytes = @intCast(scratch_working_set) },
    };
}

fn reserveInferenceAdmissionResources(
    context: *anyopaque,
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) inference.runtime.tier.memory.AdmissionResourceError!void {
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context));
    const slices = inferenceAdmissionSliceAmounts(amounts) catch
        return error.ResourceLimitExceeded;
    manager.reserveBatchClassified(&slices) catch |err| switch (err) {
        error.ResourceRequestTooLarge => return error.ResourceLimitExceeded,
        error.ResourceTemporarilyUnavailable => return error.ResourceTemporarilyUnavailable,
        // Duplicate slices are impossible in the fixed bridge plan.
        error.DuplicateResourceSlice => unreachable,
    };
}

fn releaseInferenceAdmissionResources(
    context: *anyopaque,
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) void {
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context));
    const slices = inferenceAdmissionSliceAmounts(amounts) catch unreachable;
    manager.releaseBatch(&slices);
}

test "inference admission bridge charges combined native residency to resource manager" {
    var budgets = antfly.resource_manager.Options.defaultBudgets();
    budgets[@intFromEnum(antfly.resource_manager.Slice.inference_model_residency)] =
        .{ .hard_limit_bytes = 100 };
    var manager = antfly.resource_manager.ResourceManager.init(.{ .budgets = budgets });
    const budget = inferenceAdmissionResourceBudget(&manager);

    try std.testing.expectError(
        error.ResourceLimitExceeded,
        budget.try_reserve(budget.context, .{
            .host_weight_bytes = 80,
            .backend_weight_bytes = 30,
        }),
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );

    const admitted = inference.runtime.tier.memory.AdmissionAmounts{
        .host_weight_bytes = 60,
        .backend_weight_bytes = 30,
    };
    try budget.try_reserve(budget.context, admitted);
    try std.testing.expectEqual(
        @as(u64, 90),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        budget.try_reserve(budget.context, .{
            .host_weight_bytes = 11,
        }),
    );
    budget.release(budget.context, admitted);
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
}

fn observePromptCacheResourceUsage(context: *anyopaque, current: *u64, next: u64) void {
    const manager: *antfly.resource_manager.ResourceManager = @ptrCast(@alignCast(context));
    manager.observeUsage(.inference_prompt_cache, current, next);
}

fn tokenizerCacheResourceBudget(
    manager: *antfly.resource_manager.ResourceManager,
) inference.hf_tokenizer.HfTokenizer.BpeCacheResourceBudget {
    return .{
        .context = manager,
        .try_reserve = reserveTokenizerCacheBytes,
        .release = releaseTokenizerCacheBytes,
    };
}

fn reserveTokenizerCacheBytes(context: *anyopaque, bytes: usize) bool {
    const manager: *antfly.resource_manager.ResourceManager =
        @ptrCast(@alignCast(context));
    // Cache growth is optional: honor the slice's shrink policy at the soft
    // boundary by declining new entries/workspace retention. reserve() below
    // remains the atomic hard guard if another producer wins the race.
    if (manager.admissionDecision(
        .inference_tokenizer_cache,
        @intCast(bytes),
    ).action == .shrink_cache) return false;
    var reservation = manager.reserve(
        .inference_tokenizer_cache,
        @intCast(bytes),
    ) catch return false;
    // The tokenizer owns the reservation until its entry/cache is released.
    reservation.released = true;
    return true;
}

fn releaseTokenizerCacheBytes(context: *anyopaque, bytes: usize) void {
    const manager: *antfly.resource_manager.ResourceManager =
        @ptrCast(@alignCast(context));
    manager.releaseBytes(.inference_tokenizer_cache, @intCast(bytes));
}

fn localAntflyListModelsJson(ptr: *anyopaque, alloc: std.mem.Allocator) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try node.listModelsJsonAlloc(alloc, io_impl.io());
}

fn localAntflyEmbedDenseTexts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.embedDenseTextsDirect(alloc, model, texts);
}

fn localAntflyEmbedDenseTextsWithContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: antfly.inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.embedDenseTextsDirectWithContext(alloc, context.io, context.deadline_ns, model, texts);
}

fn localAntflyEmbedDenseParts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
) anyerror![][]f32 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try localAntflyEmbedDensePartsWithExecutionContext(ptr, alloc, model, parts, io_impl.io(), null);
}

fn localAntflyEmbedDensePartsWithExecutionContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
    io: std.Io,
    deadline_ns: ?u64,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    var values = std.json.Array.init(alloc);
    defer values.deinit();
    var encoded_buffers = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (encoded_buffers.items) |buf| alloc.free(buf);
        encoded_buffers.deinit(alloc);
    }

    for (parts) |part| {
        switch (part) {
            .text => |text| {
                var obj = std.json.ObjectMap.empty;
                errdefer obj.deinit(alloc);
                try obj.put(alloc, "type", .{ .string = "text" });
                try obj.put(alloc, "text", .{ .string = text });
                try values.append(.{ .object = obj });
            },
            .media_url => |url| {
                var image_url = std.json.ObjectMap.empty;
                errdefer image_url.deinit(alloc);
                try image_url.put(alloc, "url", .{ .string = url });

                var obj = std.json.ObjectMap.empty;
                errdefer obj.deinit(alloc);
                try obj.put(alloc, "type", .{ .string = "image_url" });
                try obj.put(alloc, "image_url", .{ .object = image_url });
                try values.append(.{ .object = obj });
            },
            .binary => |binary_part| {
                const encoded_len = std.base64.standard.Encoder.calcSize(binary_part.data.len);
                const encoded = try alloc.alloc(u8, encoded_len);
                errdefer alloc.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, binary_part.data);
                try encoded_buffers.append(alloc, encoded);

                var obj = std.json.ObjectMap.empty;
                errdefer {
                    obj.deinit(alloc);
                    _ = encoded_buffers.pop();
                    alloc.free(encoded);
                }
                try obj.put(alloc, "type", .{ .string = "media" });
                try obj.put(alloc, "data", .{ .string = encoded });
                try obj.put(alloc, "mime_type", .{ .string = binary_part.mime_type });
                try values.append(.{ .object = obj });
            },
        }
    }

    return try node.embedDenseJsonInputDirectWithContext(alloc, io, deadline_ns, model, .{ .array = values });
}

fn localAntflyEmbedDensePartsWithContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
    context: antfly.inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    return try localAntflyEmbedDensePartsWithExecutionContext(ptr, alloc, model, parts, context.io, context.deadline_ns);
}

fn localAntflyEmbedSparseTexts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![]antfly.db.embedder.SparseEmbedding {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    const sparse = try node.embedSparseTextsDirect(alloc, model, texts);
    errdefer {
        for (sparse) |*item| item.deinit(alloc);
        alloc.free(sparse);
    }
    const out = try alloc.alloc(antfly.db.embedder.SparseEmbedding, sparse.len);
    errdefer alloc.free(out);
    for (sparse, 0..) |item, i| {
        out[i] = .{
            .indices = item.indices,
            .values = item.values,
        };
    }
    alloc.free(sparse);
    return out;
}

fn localAntflyRerankTexts(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
) anyerror![]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.rerankTextsDirect(alloc, model, query, documents);
}

fn localAntflyGenerateText(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.generateTextDirect(alloc, model, roles, contents);
}

fn localAntflyGenerateMessages(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    var converted = try convertLocalGenerateMessages(alloc, messages);
    defer converted.deinit(alloc);
    return try node.generateMessagesDirect(alloc, model, converted.messages);
}

fn localAntflyReadImages(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.readers.Request,
) anyerror![]antfly.readers.Result {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.readImagesDirect(alloc, model, request);
}

fn localAntflyTranscribeAudio(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.transcribing.Request,
) anyerror!antfly.transcribing.Response {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.transcribeAudioDirect(alloc, model, request);
}

fn localAntflyExtract(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.extracting.Request,
) anyerror!antfly.extracting.Response {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    return try node.extractDirect(alloc, model, request);
}

const LocalGenerateMessages = struct {
    messages: []inference.pipelines.GenerationMessage,
    owned_texts: std.ArrayListUnmanaged([]u8) = .empty,
    owned_media: std.ArrayListUnmanaged([]u8) = .empty,
    owned_slices: std.ArrayListUnmanaged([]const []const u8) = .empty,
    owned_parts: std.ArrayListUnmanaged([]inference.pipelines.GenerationMessage.ContentPart) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.owned_texts.items) |text| alloc.free(text);
        self.owned_texts.deinit(alloc);
        for (self.owned_media.items) |media| alloc.free(media);
        self.owned_media.deinit(alloc);
        for (self.owned_slices.items) |slice| alloc.free(slice);
        self.owned_slices.deinit(alloc);
        for (self.owned_parts.items) |parts| alloc.free(parts);
        self.owned_parts.deinit(alloc);
        alloc.free(self.messages);
        self.* = undefined;
    }
};

pub fn convertLocalGenerateMessages(
    alloc: std.mem.Allocator,
    messages: []const antfly.inference.ChatMessage,
) !LocalGenerateMessages {
    var out = LocalGenerateMessages{
        .messages = try alloc.alloc(inference.pipelines.GenerationMessage, messages.len),
    };
    errdefer out.deinit(alloc);

    for (messages, 0..) |message, i| {
        out.messages[i] = try convertLocalGenerateMessage(alloc, &out, message);
    }
    return out;
}

fn convertLocalGenerateMessage(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    message: antfly.inference.ChatMessage,
) !inference.pipelines.GenerationMessage {
    const role = message.role.toSlice();
    const content = message.content orelse {
        const text = try alloc.dupe(u8, "");
        var text_owned = true;
        errdefer if (text_owned) alloc.free(text);
        try owner.owned_texts.append(alloc, text);
        text_owned = false;
        return .{ .role = role, .content = text };
    };

    return switch (content) {
        .text => |text_value| blk: {
            const text = try alloc.dupe(u8, text_value);
            var text_owned = true;
            errdefer if (text_owned) alloc.free(text);
            try owner.owned_texts.append(alloc, text);
            text_owned = false;
            break :blk .{ .role = role, .content = text };
        },
        .parts => |parts| try convertLocalGenerateParts(alloc, owner, role, parts),
    };
}

fn convertLocalGenerateParts(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    role: []const u8,
    parts: []const antfly.inference.ContentPart,
) !inference.pipelines.GenerationMessage {
    var text_buf = std.ArrayListUnmanaged(u8).empty;
    errdefer text_buf.deinit(alloc);
    var images = std.ArrayListUnmanaged([]const u8).empty;
    errdefer images.deinit(alloc);
    var audio = std.ArrayListUnmanaged([]const u8).empty;
    errdefer audio.deinit(alloc);
    var out_parts = std.ArrayListUnmanaged(inference.pipelines.GenerationMessage.ContentPart).empty;
    errdefer out_parts.deinit(alloc);

    for (parts) |part| {
        switch (part) {
            .text => |text| {
                const start = text_buf.items.len;
                try text_buf.appendSlice(alloc, text);
                _ = start;
                try out_parts.append(alloc, .{ .text = text });
            },
            .image_url => |image_url| {
                const decoded = try decodeLocalGenerateDataUri(alloc, image_url.url, null);
                var decoded_owned = true;
                errdefer if (decoded_owned) alloc.free(decoded.data);
                if (!std.mem.startsWith(u8, decoded.mime_type, "image/")) {
                    return error.UnsupportedGeneratorProvider;
                }
                try images.append(alloc, decoded.data);
                try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                try owner.owned_media.append(alloc, decoded.data);
                decoded_owned = false;
            },
            .media => |media| {
                const raw = media.url orelse media.data;
                const decoded = try decodeLocalGenerateDataUri(alloc, raw, media.mime_type);
                var decoded_owned = true;
                errdefer if (decoded_owned) alloc.free(decoded.data);
                if (std.mem.startsWith(u8, decoded.mime_type, "image/")) {
                    try images.append(alloc, decoded.data);
                    try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                    try owner.owned_media.append(alloc, decoded.data);
                    decoded_owned = false;
                } else if (std.mem.startsWith(u8, decoded.mime_type, "audio/")) {
                    try audio.append(alloc, decoded.data);
                    try out_parts.append(alloc, .{ .audio = audio.items.len - 1 });
                    try owner.owned_media.append(alloc, decoded.data);
                    decoded_owned = false;
                } else {
                    return error.UnsupportedGeneratorProvider;
                }
            },
        }
    }

    const text = try text_buf.toOwnedSlice(alloc);
    var text_owned = true;
    errdefer if (text_owned) alloc.free(text);
    try owner.owned_texts.append(alloc, text);
    text_owned = false;
    const image_slice = if (images.items.len > 0) blk: {
        const slice = try images.toOwnedSlice(alloc);
        var slice_owned = true;
        errdefer if (slice_owned) alloc.free(slice);
        try owner.owned_slices.append(alloc, slice);
        slice_owned = false;
        break :blk slice;
    } else null;
    const audio_slice = if (audio.items.len > 0) blk: {
        const slice = try audio.toOwnedSlice(alloc);
        var slice_owned = true;
        errdefer if (slice_owned) alloc.free(slice);
        try owner.owned_slices.append(alloc, slice);
        slice_owned = false;
        break :blk slice;
    } else null;
    const content_parts = if (out_parts.items.len > 0) blk: {
        const slice = try out_parts.toOwnedSlice(alloc);
        var slice_owned = true;
        errdefer if (slice_owned) alloc.free(slice);
        try owner.owned_parts.append(alloc, slice);
        slice_owned = false;
        break :blk slice;
    } else null;

    return .{
        .role = role,
        .content = text,
        .image_bytes = image_slice,
        .audio_bytes = audio_slice,
        .content_parts = content_parts,
    };
}

const DecodedLocalMedia = struct {
    data: []u8,
    mime_type: []const u8,
};

fn decodeLocalGenerateDataUri(
    alloc: std.mem.Allocator,
    raw: []const u8,
    declared_mime_type: ?[]const u8,
) !DecodedLocalMedia {
    var mime_type = declared_mime_type orelse "application/octet-stream";
    var payload = raw;
    if (std.mem.startsWith(u8, raw, "data:")) {
        const comma = std.mem.indexOfScalar(u8, raw, ',') orelse return error.UnsupportedGeneratorProvider;
        const meta = raw["data:".len..comma];
        if (!std.mem.endsWith(u8, meta, ";base64")) return error.UnsupportedGeneratorProvider;
        const embedded_mime = meta[0 .. meta.len - ";base64".len];
        if (embedded_mime.len > 0) {
            if (declared_mime_type) |declared| {
                if (!std.mem.eql(u8, declared, embedded_mime)) return error.UnsupportedGeneratorProvider;
            }
            mime_type = embedded_mime;
        }
        payload = raw[comma + 1 ..];
    }

    const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(payload);
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, payload);
    return .{ .data = decoded, .mime_type = mime_type };
}

// ---------------------------------------------------------------
