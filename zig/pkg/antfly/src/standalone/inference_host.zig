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
const http_abi = @import("../runtime_http_abi.zig");
const platform_sync = @import("antfly_platform").sync;
const runtime_http_bridge = @import("../runtime_http_bridge.zig");
const inference_api = @import("inference_api");

pub const LinkedInferenceState = struct {
    alloc: std.mem.Allocator,
    /// Host-owned interface protected by standalone's inference-lane lease.
    io: std.Io,
    node: inference.server.Node,
    warm_models: ResolvedWarmModels,
    content_security: ?std.json.Parsed(antfly.common.config.Config.ContentSecurityConfig),
    s3_credentials: ?std.json.Parsed(antfly.common.config.Config.S3CredentialsConfig),
    runtime_config: std.json.Parsed(InferenceRuntimeConfig),
    owned_models_dir: ?[]u8,
    owned_ml_dir: ?[]u8,
    resource_budget_context: ?*LinkedResourceBudgetContext = null,
    routes: std.ArrayListUnmanaged(*RouteState) = .empty,
    route_manifest: std.ArrayListUnmanaged(inference_bridge.RouteManifestEntry) = .empty,
    route_validator: httpx.Router,
    route_manifest_mutex: std.atomic.Mutex = .unlocked,
    route_manifest_ready: bool = false,
};

/// Stable, ref-counted copy of the standalone resource-owner capability. The
/// inference Node and any tokenizer resource domain may outlive the configure
/// call and LinkedInferenceState fields, but never this context.
const LinkedResourceBudgetContext = struct {
    alloc: std.mem.Allocator,
    budget: inference_bridge.ResourceBudget,
    references: std.atomic.Value(usize) = .init(1),

    fn create(
        alloc: std.mem.Allocator,
        budget: inference_bridge.ResourceBudget,
    ) !*@This() {
        if (budget.retain_context(budget.context) == 0)
            return error.ResourceOwnerShuttingDown;
        errdefer budget.release_context(budget.context);
        const self = try alloc.create(@This());
        self.* = .{ .alloc = alloc, .budget = budget };
        return self;
    }

    fn retain(self: *@This()) bool {
        var current = self.references.load(.acquire);
        while (current != 0 and current != std.math.maxInt(usize)) {
            if (self.references.cmpxchgWeak(
                current,
                current + 1,
                .acq_rel,
                .acquire,
            )) |observed| {
                current = observed;
            } else return true;
        }
        return false;
    }

    fn release(self: *@This()) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous == 1) {
            self.budget.release_context(self.budget.context);
            self.alloc.destroy(self);
        }
    }
};

const InferenceRuntimeConfig = struct {
    max_concurrent_requests: ?usize = null,
    kernel_jit: inference.graph.kernel_jit.Config = .{},
    prompt_cache: inference.server.PromptCacheConfig = .{},
};

const RouteState = struct {
    owner: *LinkedInferenceState,
    handler: httpx.Handler,
};

const HttpResponseState = struct {
    alloc: std.mem.Allocator,
    response: httpx.Response,
    header_views: []http_abi.HeaderView,
};

const ProviderResponseState = struct {
    alloc: std.mem.Allocator,
    json: []u8,
};

test "standalone linked inference ABI validates the supported function-table prefix" {
    try std.testing.expect(inference_bridge.validContext(
        inference_bridge.RouteManifestContext,
        inference_bridge.abi_version,
        @sizeOf(inference_bridge.RouteManifestContext),
    ));
    try std.testing.expect(!inference_bridge.validContext(
        inference_bridge.RouteManifestContext,
        inference_bridge.abi_version - 1,
        @sizeOf(inference_bridge.RouteManifestContext),
    ));

    var table: inference_bridge.FunctionTable = undefined;
    table.abi_version = inference_bridge.abi_version;
    table.struct_size = @sizeOf(inference_bridge.FunctionTable);
    table.capabilities = inference_bridge.Capability.provider;
    try std.testing.expect(inference_bridge.validFunctionTable(&table, inference_bridge.Capability.provider));
    try std.testing.expect(!inference_bridge.validFunctionTable(&table, inference_bridge.Capability.route_manifest));
    table.struct_size = inference_bridge.requiredFunctionTableSize(inference_bridge.Capability.provider).? - 1;
    try std.testing.expect(!inference_bridge.validFunctionTable(&table, inference_bridge.Capability.provider));
    table.struct_size = inference_bridge.requiredFunctionTableSize(inference_bridge.Capability.provider).?;
    try std.testing.expect(inference_bridge.validFunctionTable(&table, inference_bridge.Capability.provider));
}

const ModelTextsRequest = struct {
    model: []const u8,
    texts: []const []const u8,
};

const ModelPartsRequest = struct {
    model: []const u8,
    parts: []const antfly.template.ContentPart,
};

const RerankTextsRequest = struct {
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
};

const GenerateTextRequest = struct {
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
};

const GenerateMessagesRequest = struct {
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
};

const ReadImagesRequest = struct {
    model: []const u8,
    request: antfly.readers.Request,
};

const TranscribeAudioRequest = struct {
    model: []const u8,
    request: antfly.transcribing.Request,
};

const ExtractRequest = struct {
    model: []const u8,
    request: antfly.extracting.Request,
};

const ResolvedWarmModels = struct {
    items: []const inference.server.WarmModel,

    fn deinit(self: *ResolvedWarmModels, alloc: std.mem.Allocator) void {
        if (self.items.len != 0) alloc.free(self.items);
        self.* = undefined;
    }
};

fn parseWarmModelKind(value: []const u8) ?inference.server.WarmModelKind {
    const info = @typeInfo(inference.server.WarmModelKind).@"enum";
    inline for (info.field_names, info.field_values) |field_name, field_value| {
        if (std.mem.eql(u8, value, field_name)) return @fromBackingInt(@intCast(field_value));
    }
    return null;
}

fn convertWarmModels(
    alloc: std.mem.Allocator,
    context: *const inference_bridge.CreateContext,
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
                    .residency_mode = switch (model.residency_mode) {
                        .auto => .auto,
                        .resident => .resident,
                        .streamed => .streamed,
                    },
                    .memory_budget_mb = model.memory_budget_mb,
                };
            }
            return .{ .items = out };
        }
    }

    return .{ .items = &.{} };
}

test "standalone preload bridge preserves A4B residency controls" {
    const wire_model = inference_bridge.WarmModel{
        .kind = inference_bridge.String.init("generator"),
        .name = inference_bridge.String.init("gemma4-a4b"),
        .backend = inference_bridge.OptionalString.init("metal"),
        .residency_mode = .streamed,
        .memory_budget_mb = 4096,
    };
    var context: inference_bridge.CreateContext = undefined;
    context.preload_ptr = @ptrCast(&wire_model);
    context.preload_len = 1;

    var resolved = try convertWarmModels(std.testing.allocator, &context);
    defer resolved.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), resolved.items.len);
    try std.testing.expectEqual(inference.ops.A4bResidencyMode.streamed, resolved.items[0].residency_mode.?);
    try std.testing.expectEqual(@as(?u32, 4096), resolved.items[0].memory_budget_mb);
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

test "standalone data directory does not change the default models directory" {
    const first = try antfly.inference_runtime.defaultModelsDirForDataDirAlloc(std.testing.allocator, "/tmp/antfly-data-a");
    defer std.testing.allocator.free(first);
    const second = try antfly.inference_runtime.defaultModelsDirForDataDirAlloc(std.testing.allocator, "/tmp/antfly-data-b");
    defer std.testing.allocator.free(second);

    try std.testing.expectEqualStrings(first, second);
    try std.testing.expect(!std.mem.startsWith(u8, first, "/tmp/antfly-data-"));
}

/// Creates the standalone inference implementation inside its focused codegen
/// unit. The caller passes only ABI-safe launch settings, never CliConfig.
pub fn linkedInferenceCreate(context: *const inference_bridge.CreateContext) !*anyopaque {
    const data_dir = context.data_dir_ptr[0..context.data_dir_len];
    const alloc = std.heap.c_allocator;
    const io = try context.executor.get();

    var content_security = if (context.content_security_json.slice()) |json|
        try std.json.parseFromSlice(antfly.common.config.Config.ContentSecurityConfig, alloc, json, .{ .ignore_unknown_fields = true })
    else
        null;
    errdefer if (content_security) |*parsed| parsed.deinit();
    var s3_credentials = if (context.s3_credentials_json.slice()) |json|
        try std.json.parseFromSlice(antfly.common.config.Config.S3CredentialsConfig, alloc, json, .{ .ignore_unknown_fields = true })
    else
        null;
    errdefer if (s3_credentials) |*parsed| parsed.deinit();
    var runtime_config = try std.json.parseFromSlice(
        InferenceRuntimeConfig,
        alloc,
        context.runtime_config_json.slice(),
        .{ .ignore_unknown_fields = false },
    );
    errdefer runtime_config.deinit();
    try runtime_config.value.kernel_jit.validate();
    try runtime_config.value.prompt_cache.validate();

    const state = try alloc.create(LinkedInferenceState);
    errdefer alloc.destroy(state);
    var warm_models = try convertWarmModels(alloc, context);
    errdefer warm_models.deinit(alloc);
    const owned_models_dir = if (context.models_dir.slice() == null)
        try antfly.inference_runtime.defaultModelsDirForDataDirAlloc(alloc, data_dir)
    else
        null;
    errdefer if (owned_models_dir) |path| alloc.free(path);
    const owned_ml_dir = if (context.ml_dir.slice() == null)
        try antfly.inference_runtime.defaultMlDirForDataDirAlloc(alloc, data_dir)
    else
        null;
    errdefer if (owned_ml_dir) |path| alloc.free(path);

    var node_config = inference.server.NodeConfig{
        .models_dir = context.models_dir.slice() orelse owned_models_dir.?,
        .ml_dir = context.ml_dir.slice() orelse owned_ml_dir.?,
        .generation_budget_overrides = .{
            .host_limit_bytes = context.host_limit_bytes,
            .backend_limit_bytes = context.backend_limit_bytes,
            .combined_limit_bytes = context.combined_limit_bytes,
            .kv_limit_bytes = context.kv_limit_bytes,
            .scratch_limit_bytes = context.scratch_limit_bytes,
        },
        .preload = warm_models.items,
        .process_memory_limit_bytes = context.process_memory_limit_bytes,
        .process_memory_limit_provenance = switch (context.process_memory_limit_provenance) {
            .automatic => .automatic,
            .explicit => .explicit,
            .cgroup_v2 => .cgroup_v2,
            .cgroup_v1 => .cgroup_v1,
            .host => .host,
            .unavailable => .unavailable,
        },
        .resource_ownership = .external_required,
        .tokenizer_cache = .{
            .bulk_slots_per_shard = 16 * 1024,
        },
        .kernel_jit = runtime_config.value.kernel_jit,
        .prompt_cache = runtime_config.value.prompt_cache,
    };
    std.log.info("standalone inference paths models_dir={s} ml_dir={s}", .{
        node_config.models_dir,
        node_config.ml_dir,
    });
    if (content_security) |*parsed| node_config.content_security = parsed.value;
    if (s3_credentials) |*parsed| node_config.s3_credentials = parsed.value;
    if (context.keep_alive.slice()) |value| node_config.keep_alive_ms = try parseKeepAliveMs(value);
    if (context.has_max_loaded_models != 0)
        node_config.max_loaded_models = std.math.cast(usize, context.max_loaded_models) orelse
            return error.InvalidInferenceModelCacheConfig;
    if (runtime_config.value.max_concurrent_requests) |limit|
        node_config.max_concurrent_requests = limit;

    state.* = .{
        .alloc = alloc,
        .io = io,
        .node = undefined,
        .warm_models = warm_models,
        .content_security = content_security,
        .s3_credentials = s3_credentials,
        .runtime_config = runtime_config,
        .owned_models_dir = owned_models_dir,
        .owned_ml_dir = owned_ml_dir,
        .route_validator = httpx.Router.init(alloc),
    };
    errdefer state.route_validator.deinit();
    state.node = try inference.server.Node.init(alloc, node_config);
    state.node.attachIo(state.io);
    return state;
}

pub fn linkedInferenceConfigure(context: *const inference_bridge.ConfigureContext) !void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context.handle));
    if (!inference_bridge.validContext(
        inference_bridge.ResourceBudget,
        context.resource_budget.abi_version,
        context.resource_budget.struct_size,
    ))
        return error.UnsupportedVersion;
    if (state.resource_budget_context != null)
        return error.ExternalResourceBudgetsAlreadyConfigured;
    const resource_context = try LinkedResourceBudgetContext.create(
        state.alloc,
        context.resource_budget.*,
    );
    state.resource_budget_context = resource_context;
    state.node.config.prompt_cache_resource_usage_observer = promptCacheResourceUsageObserver(state);
    state.node.configureExternalResourceBudgets(
        inferenceAdmissionResourceBudget(resource_context),
        tokenizerCacheResourceBudget(resource_context),
    ) catch |err| {
        state.resource_budget_context = null;
        resource_context.release();
        return err;
    };
    state.node.warmConfiguredModelsBeforeServing(state.alloc) catch |err| {
        std.log.err("standalone startup failed step=warm_inference_models err={}", .{err});
        return err;
    };
}

pub fn linkedInferenceInvokeProvider(context: *const inference_bridge.ProviderInvokeContext) !void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context.handle));
    const operation = std.enums.fromInt(inference_bridge.ProviderOperation, context.operation) orelse
        return error.UnsupportedOperation;
    const request_json = context.request_json.slice();
    const deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null;
    const alloc = state.alloc;

    const response_json = switch (operation) {
        .embed_dense_texts, .embed_dense_texts_with_context => blk: {
            var parsed = try std.json.parseFromSlice(ModelTextsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = if (operation == .embed_dense_texts_with_context)
                try state.node.embedDenseTextsDirectWithContext(state.alloc, state.io, deadline_ns, parsed.value.model, parsed.value.texts)
            else
                try state.node.embedDenseTextsDirect(state.alloc, parsed.value.model, parsed.value.texts);
            defer {
                for (result) |values| alloc.free(values);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .embed_sparse_texts => blk: {
            var parsed = try std.json.parseFromSlice(ModelTextsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try localAntflyEmbedSparseTexts(&state.node, alloc, parsed.value.model, parsed.value.texts);
            defer {
                for (result) |*item| item.deinit(alloc);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .embed_dense_parts, .embed_dense_parts_with_context => blk: {
            var parsed = try std.json.parseFromSlice(ModelPartsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try localAntflyEmbedDensePartsWithExecutionContext(
                &state.node,
                alloc,
                parsed.value.model,
                parsed.value.parts,
                state.io,
                if (operation == .embed_dense_parts_with_context) deadline_ns else null,
            );
            defer {
                for (result) |values| alloc.free(values);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .rerank_texts => blk: {
            var parsed = try std.json.parseFromSlice(RerankTextsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try state.node.rerankTextsDirect(alloc, parsed.value.model, parsed.value.query, parsed.value.documents);
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .generate_text => blk: {
            var parsed = try std.json.parseFromSlice(GenerateTextRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try state.node.generateTextDirect(alloc, parsed.value.model, parsed.value.roles, parsed.value.contents);
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .generate_messages => blk: {
            var parsed = try std.json.parseFromSlice(GenerateMessagesRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try localAntflyGenerateMessages(
                &state.node,
                alloc,
                parsed.value.model,
                parsed.value.messages,
            );
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .read_images => blk: {
            var parsed = try std.json.parseFromSlice(ReadImagesRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try state.node.readImagesDirect(alloc, parsed.value.model, parsed.value.request);
            defer {
                for (result) |*item| antfly.readers.deinitResult(alloc, item);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .transcribe_audio => blk: {
            var parsed = try std.json.parseFromSlice(TranscribeAudioRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            var result = try state.node.transcribeAudioDirect(alloc, parsed.value.model, parsed.value.request);
            defer antfly.transcribing.deinitResponse(alloc, &result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .extract => blk: {
            var parsed = try std.json.parseFromSlice(ExtractRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            var result = try state.node.extractDirect(alloc, parsed.value.model, parsed.value.request);
            defer result.deinit();
            break :blk try std.json.Stringify.valueAlloc(alloc, result.json, .{});
        },
        .list_models_json => blk: {
            const result = try state.node.listModelsJsonAlloc(alloc, state.io);
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
    };
    errdefer alloc.free(response_json);
    const response = try alloc.create(ProviderResponseState);
    response.* = .{ .alloc = alloc, .json = response_json };
    context.out_response_handle.* = response;
    context.out_response_json.* = inference_bridge.String.init(response_json);
}

pub fn linkedInferenceDestroyProviderResponse(handle: *anyopaque) void {
    const response: *ProviderResponseState = @ptrCast(@alignCast(handle));
    const alloc = response.alloc;
    alloc.free(response.json);
    alloc.destroy(response);
}

pub fn linkedInferenceRegisterRoutesOn(handle: *anyopaque, server: *httpx.Server) !void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(handle));
    var registrar = DirectServer{ .owner = state, .server = server };
    try state.node.registerRoutesOn(inference.server.public_api_prefix, &registrar);
    try state.node.registerAiRoutesOn(inference.server.ai_api_prefix, &registrar);
}

pub fn linkedInferenceRouteManifest(context: *const inference_bridge.RouteManifestContext) !void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context.handle));
    platform_sync.lockYielding(&state.route_manifest_mutex);
    defer state.route_manifest_mutex.unlock();
    if (!state.route_manifest_ready) {
        const routes_start = state.routes.items.len;
        const manifest_start = state.route_manifest.items.len;
        var manifest = ManifestServer{ .owner = state };
        state.node.registerRoutesOn(inference.server.public_api_prefix, &manifest) catch |err| {
            rollbackRouteManifest(state, routes_start, manifest_start);
            return err;
        };
        state.node.registerAiRoutesOn(inference.server.ai_api_prefix, &manifest) catch |err| {
            rollbackRouteManifest(state, routes_start, manifest_start);
            return err;
        };
        state.route_manifest_ready = true;
    }
    context.out_entries.* = if (state.route_manifest.items.len == 0) null else state.route_manifest.items.ptr;
    context.out_len.* = state.route_manifest.items.len;
}

fn rollbackRouteManifest(state: *LinkedInferenceState, routes_start: usize, manifest_start: usize) void {
    for (state.routes.items[routes_start..]) |route| state.alloc.destroy(route);
    state.routes.shrinkRetainingCapacity(routes_start);
    state.route_manifest.shrinkRetainingCapacity(manifest_start);
    state.route_validator.deinit();
    state.route_validator = httpx.Router.init(state.alloc);
}

const ManifestServer = struct {
    owner: *LinkedInferenceState,

    fn register(self: *const ManifestServer, method: http_abi.HttpMethod, comptime path: []const u8, handler: httpx.Handler) !void {
        const metadata = routeMetadata(method, path);
        self.owner.route_validator.add(switch (method) {
            .get => .GET,
            .post => .POST,
            .put => .PUT,
            .delete => .DELETE,
        }, path, handler) catch |err| {
            std.log.err("linked inference route manifest rejected method={s} path={s} err={}", .{
                @tagName(method),
                path,
                err,
            });
            return err;
        };
        const route = try self.owner.alloc.create(RouteState);
        errdefer self.owner.alloc.destroy(route);
        route.* = .{ .owner = self.owner, .handler = handler };
        try self.owner.routes.append(self.owner.alloc, route);
        errdefer _ = self.owner.routes.pop();
        try self.owner.route_manifest.append(self.owner.alloc, .{
            .route_handle = route,
            .method = method,
            .path = http_abi.Bytes.init(path),
            .request_body = metadata.request_body,
            .streaming_response = @intFromBool(metadata.streaming_response),
        });
    }

    pub fn get(self: *const ManifestServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.get, path, handler);
    }

    pub fn post(self: *const ManifestServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.post, path, handler);
    }

    pub fn put(self: *const ManifestServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.put, path, handler);
    }

    pub fn delete(self: *const ManifestServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.delete, path, handler);
    }
};

const RouteMetadata = struct {
    request_body: http_abi.RequestBodyMode,
    streaming_response: bool,
};

fn routeMetadata(method: http_abi.HttpMethod, path: []const u8) RouteMetadata {
    const method_name = switch (method) {
        .get => "GET",
        .post => "POST",
        .put => "PUT",
        .delete => "DELETE",
    };
    const relative_path = if (std.mem.startsWith(u8, path, inference.server.public_api_prefix))
        path[inference.server.public_api_prefix.len..]
    else if (std.mem.startsWith(u8, path, inference.server.ai_api_prefix))
        path[inference.server.ai_api_prefix.len..]
    else
        path;
    for (inference_api.server.routes) |route| {
        if (std.mem.eql(u8, route.method, method_name) and std.mem.eql(u8, route.path, relative_path)) {
            return .{
                .request_body = switch (route.request_body) {
                    .none => .none,
                    .buffered => .buffered,
                },
                .streaming_response = route.streaming_response,
            };
        }
    }
    return .{
        .request_body = if (method == .get) .none else .buffered,
        .streaming_response = false,
    };
}

const DirectServer = struct {
    owner: *LinkedInferenceState,
    server: *httpx.Server,

    fn register(self: *const DirectServer, method: http_abi.HttpMethod, comptime path: []const u8, handler: httpx.Handler) !void {
        const route = try self.owner.alloc.create(RouteState);
        errdefer self.owner.alloc.destroy(route);
        route.* = .{ .owner = self.owner, .handler = handler };
        try self.owner.routes.append(self.owner.alloc, route);
        errdefer _ = self.owner.routes.pop();
        try self.server.routeWithData(switch (method) {
            .get => .GET,
            .post => .POST,
            .put => .PUT,
            .delete => .DELETE,
        }, path, localInferenceHttpHandler, route);
    }

    pub fn get(self: *const DirectServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.get, path, handler);
    }

    pub fn post(self: *const DirectServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.post, path, handler);
    }

    pub fn put(self: *const DirectServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.put, path, handler);
    }

    pub fn delete(self: *const DirectServer, comptime path: []const u8, handler: httpx.Handler) !void {
        try self.register(.delete, path, handler);
    }
};

fn localInferenceHttpHandler(context: *httpx.Context) anyerror!httpx.Response {
    const route: *RouteState = @ptrCast(@alignCast(context.route_data orelse return error.InferenceRouteUnavailable));
    return route.handler.invoke(context);
}

pub fn linkedInferenceHandleHttp(context: *const inference_bridge.HttpHandleContext) !void {
    const route: *RouteState = @ptrCast(@alignCast(context.route_handle));
    const state = route.owner;
    const alloc = state.alloc;
    const request = context.request;
    const query = request.query.slice();
    const target = if (query) |value|
        try std.fmt.allocPrint(alloc, "{s}?{s}", .{ request.path.slice(), value })
    else
        try alloc.dupe(u8, request.path.slice());
    defer alloc.free(target);

    var http_request = try httpx.Request.init(alloc, switch (request.method) {
        .get => .GET,
        .post => .POST,
        .put => .PUT,
        .delete => .DELETE,
    }, target);
    defer http_request.deinit();
    const input_headers = if (request.headers_ptr) |ptr| ptr[0..request.headers_len] else &.{};
    for (input_headers) |header| try http_request.headers.append(header.name.slice(), header.value.slice());
    http_request.body = request.body.slice();

    const input_params = if (request.params_ptr) |ptr| ptr[0..request.params_len] else &.{};
    const params = try alloc.alloc(httpx.RouteParam, input_params.len);
    defer alloc.free(params);
    for (input_params, 0..) |param, i| {
        params[i] = .{ .name = param.name.slice(), .value = param.value.slice() };
    }

    var http_context = httpx.Context.init(alloc, state.io, &http_request);
    defer http_context.deinit();
    http_context.params = params;
    runtime_http_bridge.installInbound(&http_context, &context.cancellation, &context.body_source, &context.stream);
    var response = try route.handler.invoke(&http_context);
    errdefer response.deinit();

    const response_state = try alloc.create(HttpResponseState);
    errdefer alloc.destroy(response_state);
    const response_headers = response.headers.iterator();
    const header_views = try alloc.alloc(http_abi.HeaderView, response_headers.len);
    errdefer alloc.free(header_views);
    for (response_headers, 0..) |header, i| {
        header_views[i] = .{
            .name = http_abi.Bytes.init(header.name),
            .value = http_abi.Bytes.init(header.value),
        };
    }
    response_state.* = .{ .alloc = alloc, .response = response, .header_views = header_views };
    context.out_response_handle.* = response_state;
    context.out_response.* = .{
        .status = response.status.code,
        .content_type = http_abi.OptionalBytes.init(response.contentType()),
        .headers_ptr = if (header_views.len == 0) null else header_views.ptr,
        .headers_len = header_views.len,
        .body = http_abi.Bytes.init(response.body orelse ""),
    };
}

pub fn linkedInferenceDestroyHttpResponse(handle: *anyopaque) void {
    const state: *HttpResponseState = @ptrCast(@alignCast(handle));
    const alloc = state.alloc;
    state.response.deinit();
    alloc.free(state.header_views);
    alloc.destroy(state);
}

pub fn linkedInferenceTryAcquireRequest(handle: *anyopaque) bool {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(handle));
    return state.node.tryAcquireRequestSlot();
}

pub fn linkedInferenceReleaseRequest(handle: *anyopaque) void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(handle));
    state.node.releaseRequestSlot();
}

pub fn linkedInferenceRequestAdmissionStats(handle: *anyopaque) inference_bridge.RequestAdmissionStats {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(handle));
    const stats = state.node.inference_admission.stats();
    return .{
        .capacity = stats.capacity_requests,
        .in_flight = stats.in_flight_requests,
        .peak_in_flight = stats.peak_in_flight_requests,
        .rejected_total = stats.rejected_requests_total,
    };
}

pub fn linkedInferenceDestroy(handle: *anyopaque) void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(handle));
    const alloc = state.alloc;
    state.node.detachPromptCacheResourceUsageObserver();
    state.node.deinit();
    if (state.resource_budget_context) |context| context.release();
    state.resource_budget_context = null;
    for (state.routes.items) |route| alloc.destroy(route);
    state.routes.deinit(alloc);
    state.route_manifest.deinit(alloc);
    state.route_validator.deinit();
    state.warm_models.deinit(alloc);
    if (state.content_security) |*parsed| parsed.deinit();
    if (state.s3_credentials) |*parsed| parsed.deinit();
    state.runtime_config.deinit();
    if (state.owned_models_dir) |path| alloc.free(path);
    if (state.owned_ml_dir) |path| alloc.free(path);
    alloc.destroy(state);
}

fn promptCacheResourceUsageObserver(state: *LinkedInferenceState) inference.runtime.kv.prompt_cache.ResourceUsageObserver {
    return .{
        .context = state,
        .update = observePromptCacheResourceUsage,
    };
}

fn inferenceAdmissionResourceBudget(
    context: *LinkedResourceBudgetContext,
) inference.runtime.tier.memory.AdmissionResourceBudget {
    return .{
        .context = context,
        .retain_context = retainLinkedResourceBudgetContext,
        .release_context = releaseLinkedResourceBudgetContext,
        .try_reserve = reserveInferenceAdmissionResources,
        .retain = retainInferenceAdmissionResources,
        .release = releaseInferenceAdmissionResources,
    };
}

fn retainLinkedResourceBudgetContext(context: *anyopaque) bool {
    const resource_context: *LinkedResourceBudgetContext = @ptrCast(@alignCast(context));
    return resource_context.retain();
}

fn releaseLinkedResourceBudgetContext(context: *anyopaque) void {
    const resource_context: *LinkedResourceBudgetContext = @ptrCast(@alignCast(context));
    resource_context.release();
}

fn bridgeAdmissionAmounts(
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) inference_bridge.AdmissionAmounts {
    return .{
        .host_weight_bytes = amounts.host_weight_bytes,
        .backend_weight_bytes = amounts.backend_weight_bytes,
        .host_kv_bytes = amounts.host_kv_bytes,
        .backend_kv_bytes = amounts.backend_kv_bytes,
        .host_scratch_bytes = amounts.host_scratch_bytes,
        .backend_scratch_bytes = amounts.backend_scratch_bytes,
    };
}

fn reserveInferenceAdmissionResources(
    context: *anyopaque,
    amounts: inference.runtime.tier.memory.AdmissionAmounts,
) inference.runtime.tier.memory.AdmissionResourceError!usize {
    const resource_context: *LinkedResourceBudgetContext = @ptrCast(@alignCast(context));
    const budget = &resource_context.budget;
    const bridged = bridgeAdmissionAmounts(amounts);
    var lease: usize = 0;
    const status = budget.reserve_admission(budget.context, &bridged, &lease);
    if (status.isOk()) {
        if (lease == 0) return error.ResourceLimitExceeded;
        return lease;
    }
    const err = inference_bridge.errorFromStatus(status);
    if (err == error.ResourceTemporarilyUnavailable) return error.ResourceTemporarilyUnavailable;
    return error.ResourceLimitExceeded;
}

fn retainInferenceAdmissionResources(
    context: *anyopaque,
    lease: usize,
    retained: inference.runtime.tier.memory.AdmissionAmounts,
) inference.runtime.tier.memory.AdmissionResourceError!void {
    const resource_context: *LinkedResourceBudgetContext = @ptrCast(@alignCast(context));
    const budget = &resource_context.budget;
    const bridged = bridgeAdmissionAmounts(retained);
    const status = budget.retain_admission(budget.context, lease, &bridged);
    if (status.isOk()) return;
    const err = inference_bridge.errorFromStatus(status);
    if (err == error.ResourceTemporarilyUnavailable) return error.ResourceTemporarilyUnavailable;
    return error.ResourceLimitExceeded;
}

fn releaseInferenceAdmissionResources(
    context: *anyopaque,
    lease: usize,
) void {
    const resource_context: *LinkedResourceBudgetContext = @ptrCast(@alignCast(context));
    const budget = &resource_context.budget;
    budget.release_admission(budget.context, lease);
}

fn observePromptCacheResourceUsage(context: *anyopaque, current: *u64, next: u64) void {
    const state: *LinkedInferenceState = @ptrCast(@alignCast(context));
    const resource_context = state.resource_budget_context orelse return;
    const budget = &resource_context.budget;
    if (budget.observe_prompt_cache(
        budget.context,
        @intFromPtr(current),
        current.*,
        next,
    ) != 0)
        current.* = next;
}

test "standalone prompt cache detaches resource observer before owner teardown" {
    const Observer = struct {
        alive: bool = true,
        callbacks_after_teardown: usize = 0,

        fn update(context: *anyopaque, current: *u64, next: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (!self.alive) self.callbacks_after_teardown += 1;
            current.* = next;
        }
    };

    var observer = Observer{};
    var cache = inference.runtime.kv.prompt_cache.PromptPrefixCache.init(std.testing.allocator);
    cache.configure(.{
        .enabled = true,
        .mode = .simple,
        .min_tokens = 2,
        .max_bytes = 1 << 20,
        .resource_usage_observer = .{
            .context = &observer,
            .update = Observer.update,
        },
    });
    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const sequence_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(sequence_id, 2);
    try cache.storeFromSequence("shutdown", &.{ 1, 2 }, sequence_id);

    cache.detachResourceUsageObserver();
    observer.alive = false;
    cache.deinit();
    try std.testing.expectEqual(@as(usize, 0), observer.callbacks_after_teardown);
}

fn tokenizerCacheResourceBudget(
    context: *LinkedResourceBudgetContext,
) inference.hf_tokenizer.HfTokenizer.BpeCacheResourceBudget {
    return .{
        .context = context,
        .retain_context = retainLinkedResourceBudgetContext,
        .release_context = releaseLinkedResourceBudgetContext,
        .observe = observeTokenizerCacheBytes,
    };
}

fn observeTokenizerCacheBytes(
    context: *anyopaque,
    observer_id: usize,
    previous: usize,
    next: usize,
) bool {
    const resource_context: *LinkedResourceBudgetContext = @ptrCast(@alignCast(context));
    const budget = &resource_context.budget;
    return budget.observe_tokenizer_cache(
        budget.context,
        observer_id,
        @intCast(previous),
        @intCast(next),
    ) != 0;
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

fn localAntflyEmbedDensePartsWithExecutionContext(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
    io: std.Io,
    deadline_ns: ?u64,
) anyerror![][]f32 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    const direct_parts = try localAntflyDirectDenseParts(alloc, parts);
    defer alloc.free(direct_parts);
    return try node.embedDensePartsDirectWithContext(alloc, io, deadline_ns, model, direct_parts);
}

pub fn localAntflyDirectDenseParts(
    alloc: std.mem.Allocator,
    parts: []const antfly.template.ContentPart,
) ![]inference.server.Node.DirectDenseEmbedPart {
    const out = try alloc.alloc(inference.server.Node.DirectDenseEmbedPart, parts.len);
    for (parts, out) |part, *direct| direct.* = switch (part) {
        .text => |text| .{ .text = text },
        .media_url => |url| .{ .image_url = url },
        .binary => |media| .{ .media = .{
            .mime_type = media.mime_type,
            .data = media.data,
        } },
    };
    return out;
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
    if (messages.len == 0) return error.InvalidGenerationRequest;
    const preflight = try preflightLocalGenerateMessages(messages);
    var admission = try node.beginDirectGenerateAdmission(preflight, 256);
    defer admission.deinit();

    var converted = try convertLocalGenerateMessages(alloc, messages, preflight.decoded_media_bytes);
    defer converted.deinit(alloc);
    return try node.generateMessagesDirectAdmitted(alloc, model, converted.messages, &admission);
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

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
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

const LocalGenerateMediaDescriptor = struct {
    payload: []const u8,
    mime_type: []const u8,
    encoded_bytes: usize,
    decoded_bytes: usize,
};

pub const LocalGenerateDecodeBudget = struct {
    remaining_bytes: usize,

    fn reserve(self: *@This(), bytes: usize) !void {
        if (bytes > self.remaining_bytes) return error.RemoteContentTooLarge;
        self.remaining_bytes -= bytes;
    }
};

fn inspectLocalGenerateDataUri(
    raw: []const u8,
    declared_mime_type: ?[]const u8,
) !LocalGenerateMediaDescriptor {
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

    return .{
        .payload = payload,
        .mime_type = mime_type,
        .encoded_bytes = raw.len,
        .decoded_bytes = try std.base64.standard.Decoder.calcSizeForSlice(payload),
    };
}

fn addLocalGenerateBytes(total: *usize, amount: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch return error.RemoteContentTooLarge;
}

fn addLocalGenerateMediaPreflight(
    preflight: *inference.server.Node.DirectGeneratePreflight,
    descriptor: LocalGenerateMediaDescriptor,
    image_only: bool,
) !void {
    const is_image = std.mem.startsWith(u8, descriptor.mime_type, "image/");
    const is_audio = std.mem.startsWith(u8, descriptor.mime_type, "audio/");
    if (!is_image and (image_only or !is_audio)) return error.UnsupportedGeneratorProvider;

    try addLocalGenerateBytes(&preflight.encoded_media_bytes, descriptor.encoded_bytes);
    try addLocalGenerateBytes(&preflight.decoded_media_bytes, descriptor.decoded_bytes);
    try addLocalGenerateBytes(&preflight.media_count, 1);
    if (is_image) {
        try addLocalGenerateBytes(&preflight.image_count, 1);
    } else {
        preflight.has_audio = true;
    }
}

pub fn preflightLocalGenerateMessages(
    messages: []const antfly.inference.ChatMessage,
) !inference.server.Node.DirectGeneratePreflight {
    var preflight: inference.server.Node.DirectGeneratePreflight = .{};
    for (messages) |message| {
        const content = message.content orelse continue;
        switch (content) {
            .text => |text_value| try addLocalGenerateBytes(&preflight.text_bytes, text_value.len),
            .parts => |parts| for (parts) |part| switch (part) {
                .text => |text_value| try addLocalGenerateBytes(&preflight.text_bytes, text_value.len),
                .image_url => |image_url| try addLocalGenerateMediaPreflight(
                    &preflight,
                    try inspectLocalGenerateDataUri(image_url.url, null),
                    true,
                ),
                .media => |media| try addLocalGenerateMediaPreflight(
                    &preflight,
                    try inspectLocalGenerateDataUri(media.url orelse media.data, media.mime_type),
                    false,
                ),
            },
        }
    }
    return preflight;
}

pub fn convertLocalGenerateMessages(
    alloc: std.mem.Allocator,
    messages: []const antfly.inference.ChatMessage,
    decoded_media_bytes: usize,
) !LocalGenerateMessages {
    var out = LocalGenerateMessages{
        .messages = try alloc.alloc(inference.pipelines.GenerationMessage, messages.len),
    };
    errdefer out.deinit(alloc);

    var decode_budget = LocalGenerateDecodeBudget{ .remaining_bytes = decoded_media_bytes };
    for (messages, 0..) |message, i|
        out.messages[i] = try convertLocalGenerateMessage(alloc, &out, message, &decode_budget);
    if (decode_budget.remaining_bytes != 0) return error.InvalidGenerationAdmission;
    return out;
}

fn convertLocalGenerateMessage(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    message: antfly.inference.ChatMessage,
    decode_budget: *LocalGenerateDecodeBudget,
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
        .parts => |parts| try convertLocalGenerateParts(alloc, owner, role, parts, decode_budget),
    };
}

fn convertLocalGenerateParts(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    role: []const u8,
    parts: []const antfly.inference.ContentPart,
    decode_budget: *LocalGenerateDecodeBudget,
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
                const decoded = try decodeLocalGenerateDataUri(alloc, image_url.url, null, decode_budget);
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
                const decoded = try decodeLocalGenerateDataUri(alloc, raw, media.mime_type, decode_budget);
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

pub fn decodeLocalGenerateDataUri(
    alloc: std.mem.Allocator,
    raw: []const u8,
    declared_mime_type: ?[]const u8,
    decode_budget: *LocalGenerateDecodeBudget,
) !DecodedLocalMedia {
    const descriptor = try inspectLocalGenerateDataUri(raw, declared_mime_type);
    try decode_budget.reserve(descriptor.decoded_bytes);
    const decoded = try alloc.alloc(u8, descriptor.decoded_bytes);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, descriptor.payload);
    return .{ .data = decoded, .mime_type = descriptor.mime_type };
}

// ---------------------------------------------------------------
