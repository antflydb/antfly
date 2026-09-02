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
const inference_chunker = @import("inference_chunker");
const chunking_types = @import("../chunking/types.zig");

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
    /// Model-free invocation seam used by ABI conformance tests. Production
    /// states leave this null and dispatch directly to `node`.
    read_encoded_images_override: ?ReadEncodedImagesHandler = null,
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

pub const ReadEncodedImagesHandler = struct {
    ptr: *anyopaque,
    read_fn: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: antfly.readers.EncodedRequest,
    ) anyerror![]antfly.readers.Result,

    fn read(
        self: @This(),
        alloc: std.mem.Allocator,
        model: []const u8,
        request: antfly.readers.EncodedRequest,
    ) ![]antfly.readers.Result {
        return try self.read_fn(self.ptr, alloc, model, request);
    }
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
    attachment_count: usize = 0,
};

fn validateProviderAttachmentRefs(
    payload_len: usize,
    ref_ptr: ?[*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
) !void {
    if (ref_len != payload_len) return error.InvalidArguments;
    if (ref_len > 0 and ref_ptr == null) return error.InvalidArguments;
    const refs = if (ref_ptr) |ptr| ptr[0..ref_len] else &.{};
    for (refs, 0..) |ref, i| {
        if (ref.attachment_index >= payload_len) return error.InvalidArguments;
        for (refs[0..i]) |prior| {
            if (prior.attachment_index == ref.attachment_index) return error.InvalidArguments;
        }
    }
}

fn providerAttachmentRefForAttachment(
    refs: [*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
    attachment_index: usize,
) ?inference_bridge.ProviderAttachmentRef {
    for (refs[0..ref_len]) |ref| if (ref.attachment_index == attachment_index) return ref;
    return null;
}

fn providerAttachmentRefForItem(
    refs: [*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
    item_index: usize,
) ?inference_bridge.ProviderAttachmentRef {
    for (refs[0..ref_len]) |ref| if (ref.item_index == item_index) return ref;
    return null;
}

fn decodeProviderEmbeddingParts(
    alloc: std.mem.Allocator,
    parts: []const antfly.template.ContentPart,
    expected_count: usize,
    payload_ptr: ?[*]const inference_bridge.ProviderBinaryPayload,
    payload_len: usize,
    ref_ptr: ?[*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
) ![]antfly.template.ContentPart {
    if (expected_count != payload_len or ref_len != payload_len) return error.InvalidArguments;
    if (payload_len > 0 and payload_ptr == null) return error.InvalidArguments;
    try validateProviderAttachmentRefs(payload_len, ref_ptr, ref_len);
    const out = try alloc.alloc(antfly.template.ContentPart, parts.len);
    errdefer alloc.free(out);
    var payload_index: usize = 0;
    for (parts, out, 0..) |part, *decoded, item_index| switch (part) {
        .binary => |binary| {
            if (payload_len == 0) {
                decoded.* = part;
                continue;
            }
            if (binary.data.len != 0 or payload_index >= payload_len) return error.InvalidArguments;
            const ref = providerAttachmentRefForItem(ref_ptr.?, ref_len, item_index) orelse return error.InvalidArguments;
            const payload = payload_ptr.?[ref.attachment_index];
            const attachment = antfly.inference.work.Attachment{
                .bytes = payload.bytes.slice(),
                .content_type = payload.content_type.slice(),
            };
            try attachment.validate();
            if (!mimeDeclarationsCompatible(binary.mime_type, attachment.content_type)) return error.InvalidArguments;
            decoded.* = .{ .binary = .{
                .mime_type = attachment.content_type,
                .data = attachment.bytes,
            } };
            payload_index += 1;
        },
        else => decoded.* = part,
    };
    if (payload_index != payload_len) return error.InvalidArguments;
    return out;
}

test "standalone embedding ABI reconstructs borrowed binary parts" {
    const raw = [_]u8{ 1, 2, 3, 4 };
    const parts = [_]antfly.template.ContentPart{
        .{ .text = "caption" },
        .{ .binary = .{ .mime_type = "image/png", .data = &.{} } },
    };
    const payloads = [_]inference_bridge.ProviderBinaryPayload{.{
        .bytes = inference_bridge.String.init(&raw),
        .content_type = inference_bridge.String.init("image/png"),
    }};
    const refs = [_]inference_bridge.ProviderAttachmentRef{.{ .attachment_index = 0, .item_index = 1 }};
    const decoded = try decodeProviderEmbeddingParts(std.testing.allocator, &parts, 1, &payloads, 1, &refs, 1);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualStrings("caption", decoded[0].text);
    try std.testing.expectEqual(@intFromPtr(raw[0..].ptr), @intFromPtr(decoded[1].binary.data.ptr));
    try std.testing.expectEqualSlices(u8, &raw, decoded[1].binary.data);
    try std.testing.expectError(
        error.InvalidArguments,
        decodeProviderEmbeddingParts(std.testing.allocator, &parts, 2, &payloads, 1, &refs, 1),
    );
}

test "standalone attachment ABI maps several payloads to one generator item" {
    const first = [_]u8{1};
    const second = [_]u8{2};
    const payloads = [_]inference_bridge.ProviderBinaryPayload{
        .{ .bytes = inference_bridge.String.init(&first), .content_type = inference_bridge.String.init("image/png") },
        .{ .bytes = inference_bridge.String.init(&second), .content_type = inference_bridge.String.init("image/png") },
    };
    const refs = [_]inference_bridge.ProviderAttachmentRef{
        .{ .attachment_index = 1, .item_index = 0, .item_id = inference_bridge.OptionalString.init("request-7") },
        .{ .attachment_index = 0, .item_index = 0, .item_id = inference_bridge.OptionalString.init("request-7") },
    };
    const attachments = try decodeProviderAttachments(std.testing.allocator, 2, &payloads, 2, &refs, 2);
    defer std.testing.allocator.free(attachments);
    try std.testing.expectEqualSlices(u8, &first, attachments[0].bytes);
    try std.testing.expectEqualSlices(u8, &second, attachments[1].bytes);
    try std.testing.expectEqualStrings("request-7", attachments[0].identity.item_id);
    try std.testing.expectEqual(@as(usize, 0), refs[0].item_index);
}

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

const GenerateMessagesWithAttachmentsRequest = struct {
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
    attachment_count: usize,
};

const ModelCapabilitiesRequest = struct {
    model: []const u8,
    task: antfly.inference.work.Task,
};

const ReadImagesRequest = struct {
    model: []const u8,
    request: antfly.readers.Request,
};

const ReadEncodedImagesRequest = inference_bridge.ReadEncodedImagesRequest;

pub const DecodedReadEncodedImagesRequest = struct {
    metadata: std.json.Parsed(ReadEncodedImagesRequest),
    images: []antfly.readers.EncodedImage,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.images);
        self.metadata.deinit();
        self.* = undefined;
    }
};

/// Decode the exact JSON-plus-borrowed-binary representation used by the
/// standalone provider ABI. The redundant image count prevents a malformed
/// context from silently changing request cardinality.
pub fn decodeReadEncodedImagesProviderRequest(
    alloc: std.mem.Allocator,
    request_json: []const u8,
    payload_ptr: ?[*]const inference_bridge.ProviderBinaryPayload,
    payload_len: usize,
    ref_ptr: ?[*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
) !DecodedReadEncodedImagesRequest {
    var metadata = try std.json.parseFromSlice(ReadEncodedImagesRequest, alloc, request_json, .{
        .ignore_unknown_fields = true,
    });
    errdefer metadata.deinit();
    if (metadata.value.image_count != payload_len or ref_len != payload_len) return error.InvalidArguments;
    if (payload_len > 0 and payload_ptr == null) return error.InvalidArguments;
    try validateProviderAttachmentRefs(payload_len, ref_ptr, ref_len);

    const images = try alloc.alloc(antfly.readers.EncodedImage, payload_len);
    errdefer alloc.free(images);
    if (payload_ptr) |payloads| {
        for (images, 0..) |*image, i| {
            const ref = providerAttachmentRefForItem(ref_ptr.?, ref_len, i) orelse return error.InvalidArguments;
            const payload = payloads[ref.attachment_index];
            image.* = .{
                .bytes = payload.bytes.slice(),
                .mime_type = payload.content_type.slice(),
                .item_id = ref.item_id.slice() orelse "",
                .source_fingerprint = ref.source_fingerprint.slice(),
                .page_number = if (ref.has_page_number != 0) ref.page_number else null,
            };
        }
    }
    try antfly.readers.validateEncodedRequest(.{ .images = images });
    return .{ .metadata = metadata, .images = images };
}

fn decodeProviderAttachments(
    alloc: std.mem.Allocator,
    expected_count: usize,
    payload_ptr: ?[*]const inference_bridge.ProviderBinaryPayload,
    payload_len: usize,
    ref_ptr: ?[*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
) ![]antfly.inference.work.Attachment {
    if (expected_count != payload_len or ref_len != payload_len) return error.InvalidArguments;
    if (payload_len > 0 and payload_ptr == null) return error.InvalidArguments;
    try validateProviderAttachmentRefs(payload_len, ref_ptr, ref_len);
    const attachments = try alloc.alloc(antfly.inference.work.Attachment, payload_len);
    errdefer alloc.free(attachments);
    if (payload_ptr) |payloads| {
        for (attachments, 0..) |*attachment, i| {
            const ref = providerAttachmentRefForAttachment(ref_ptr.?, ref_len, i) orelse return error.InvalidArguments;
            const payload = payloads[ref.attachment_index];
            attachment.* = .{
                .bytes = payload.bytes.slice(),
                .content_type = payload.content_type.slice(),
                .identity = .{
                    .item_id = ref.item_id.slice() orelse "",
                    .source_fingerprint = ref.source_fingerprint.slice(),
                    .page_number = if (ref.has_page_number != 0) ref.page_number else null,
                },
            };
            try attachment.validate();
        }
    }
    return attachments;
}

const TranscribeAudioRequest = struct {
    model: []const u8,
    request: antfly.transcribing.Request,
};

const ExtractRequest = struct {
    model: []const u8,
    request: antfly.extracting.Request,
    attachment_count: usize = 0,
};

fn decodeExtractionAttachments(
    alloc: std.mem.Allocator,
    input_count: usize,
    expected_count: usize,
    payload_ptr: ?[*]const inference_bridge.ProviderBinaryPayload,
    payload_len: usize,
    ref_ptr: ?[*]const inference_bridge.ProviderAttachmentRef,
    ref_len: usize,
) ![]antfly.extracting.Attachment {
    if (expected_count != payload_len or ref_len != payload_len) return error.InvalidArguments;
    if (payload_len > 0 and (payload_ptr == null or ref_ptr == null)) return error.InvalidArguments;
    try validateProviderAttachmentRefs(payload_len, ref_ptr, ref_len);
    const attachments = try alloc.alloc(antfly.extracting.Attachment, payload_len);
    errdefer alloc.free(attachments);
    if (payload_ptr) |payloads| for (attachments, 0..) |*attachment, i| {
        const ref = providerAttachmentRefForAttachment(ref_ptr.?, ref_len, i) orelse return error.InvalidArguments;
        if (ref.item_index >= input_count) return error.InvalidArguments;
        const payload = payloads[ref.attachment_index];
        if (payload.content_type.len == 0) return error.InvalidArguments;
        attachment.* = .{
            .input_index = ref.item_index,
            .bytes = payload.bytes.slice(),
            .mime_type = payload.content_type.slice(),
        };
    };
    return attachments;
}

const ChunkInputRequest = struct {
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
    attachment_count: usize = 0,
};

const RewriteTextsRequest = struct {
    model: []const u8,
    inputs: []const []const u8,
};

const ClassifyTextsRequest = struct {
    model: []const u8,
    request: antfly.inference.managed_embedder.ClassificationRequest,
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
            const parts = try decodeProviderEmbeddingParts(
                alloc,
                parsed.value.parts,
                parsed.value.attachment_count,
                context.binary_payloads,
                context.binary_payloads_len,
                context.attachment_refs,
                context.attachment_refs_len,
            );
            defer alloc.free(parts);
            const result = try localAntflyEmbedDensePartsWithExecutionContext(
                &state.node,
                alloc,
                parsed.value.model,
                parts,
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
            try validateLinkedTextInvocation(&state.node, state.io, parsed.value.model, .rerank, parsed.value.documents, parsed.value.query.len, parsed.value.documents.len, 0);
            const result = try state.node.rerankTextsDirect(alloc, parsed.value.model, parsed.value.query, parsed.value.documents);
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .generate_text => blk: {
            var parsed = try std.json.parseFromSlice(GenerateTextRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            try validateLinkedTextInvocation(&state.node, state.io, parsed.value.model, .generate, parsed.value.contents, 0, 0, 0);
            const result = try state.node.generateTextDirect(alloc, parsed.value.model, parsed.value.roles, parsed.value.contents);
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .generate_messages => blk: {
            var parsed = try std.json.parseFromSlice(GenerateMessagesRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try localAntflyGenerateMessages(
                &state.node,
                state.io,
                alloc,
                parsed.value.model,
                parsed.value.messages,
            );
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .generate_messages_with_attachments => blk: {
            var parsed = try std.json.parseFromSlice(GenerateMessagesWithAttachmentsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const attachments = try decodeProviderAttachments(
                alloc,
                parsed.value.attachment_count,
                context.binary_payloads,
                context.binary_payloads_len,
                context.attachment_refs,
                context.attachment_refs_len,
            );
            defer alloc.free(attachments);
            const result = try localAntflyGenerateMessagesWithAttachments(
                &state.node,
                state.io,
                alloc,
                parsed.value.model,
                parsed.value.messages,
                attachments,
            );
            defer alloc.free(result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .model_capabilities => blk: {
            var parsed = try std.json.parseFromSlice(ModelCapabilitiesRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const result = try localModelCapabilities(&state.node, state.io, parsed.value.model, parsed.value.task);
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
        .read_encoded_images, .read_encoded_images_reported => blk: {
            var decoded = try decodeReadEncodedImagesProviderRequest(
                alloc,
                request_json,
                context.binary_payloads,
                context.binary_payloads_len,
                context.attachment_refs,
                context.attachment_refs_len,
            );
            defer decoded.deinit(alloc);
            const encoded_request = antfly.readers.EncodedRequest{
                .images = decoded.images,
                .prompt = decoded.metadata.value.prompt,
                .max_tokens = decoded.metadata.value.max_tokens,
                .source_fingerprint = decoded.metadata.value.source_fingerprint,
            };
            // The override is an injected test executor with no model catalog.
            // Every production path resolves and enforces the concrete model
            // contract inside this ABI boundary.
            if (state.read_encoded_images_override == null) {
                const read_capabilities = try localModelCapabilities(
                    &state.node,
                    state.io,
                    decoded.metadata.value.model,
                    .read,
                );
                try validateEncodedReadCapabilities(read_capabilities, encoded_request);
            }
            if (operation == .read_encoded_images_reported and state.read_encoded_images_override == null) {
                var batch = try state.node.readEncodedImagesReportedDirect(alloc, decoded.metadata.value.model, encoded_request);
                defer batch.deinit(alloc);
                break :blk try std.json.Stringify.valueAlloc(alloc, batch, .{});
            }
            const result = if (state.read_encoded_images_override) |handler|
                try handler.read(alloc, decoded.metadata.value.model, encoded_request)
            else
                try state.node.readEncodedImagesDirect(alloc, decoded.metadata.value.model, encoded_request);
            if (operation == .read_encoded_images_reported) {
                var batch = antfly.readers.BatchResult{
                    .items = result,
                    .execution = .{ .requested_items = result.len, .serial_items = result.len },
                };
                defer batch.deinit(alloc);
                break :blk try std.json.Stringify.valueAlloc(alloc, batch, .{});
            }
            defer {
                for (result) |*item| antfly.readers.deinitResult(alloc, item);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .transcribe_audio => blk: {
            var parsed = try std.json.parseFromSlice(TranscribeAudioRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const transcribe_capabilities = try localModelCapabilities(&state.node, state.io, parsed.value.model, .transcribe);
            try transcribe_capabilities.validateInvocation(.transcribe, .{
                .item_count = 1,
                .modalities = .{ .audio = true },
            });
            var result = try state.node.transcribeAudioDirect(alloc, parsed.value.model, parsed.value.request);
            defer antfly.transcribing.deinitResponse(alloc, &result);
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .extract => blk: {
            var parsed = try std.json.parseFromSlice(ExtractRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.request.attachments.len != 0) return error.InvalidArguments;
            const attachments = try decodeExtractionAttachments(
                alloc,
                parsed.value.request.inputs.len,
                parsed.value.attachment_count,
                context.binary_payloads,
                context.binary_payloads_len,
                context.attachment_refs,
                context.attachment_refs_len,
            );
            defer alloc.free(attachments);
            var request = parsed.value.request;
            request.attachments = attachments;
            var extract_shape = antfly.inference.work.InvocationShape{
                .item_count = request.inputs.len,
                .modalities = if (attachments.len > 0) .{ .image = true } else .{ .text = true },
                .schema_bytes = request.schema_json.len,
                .max_media_parts_per_item = if (attachments.len > 0) 1 else 0,
            };
            for (request.inputs) |input| {
                extract_shape.text_bytes = std.math.add(usize, extract_shape.text_bytes, input.content_json.len) catch
                    return error.InferenceTextBytesExceeded;
                extract_shape.max_text_bytes_per_item = @max(extract_shape.max_text_bytes_per_item, input.content_json.len);
            }
            const extract_capabilities = try localModelCapabilities(&state.node, state.io, parsed.value.model, .extract);
            for (attachments) |attachment| {
                try extract_capabilities.validateMimeType(attachment.mime_type);
                extract_shape.encoded_media_bytes = std.math.add(
                    usize,
                    extract_shape.encoded_media_bytes,
                    attachment.bytes.len,
                ) catch return error.InferenceEncodedBytesExceeded;
                const pixels = try antfly.inference.work.encodedImagePixels(attachment.mime_type, attachment.bytes);
                extract_shape.decoded_pixels = std.math.add(u64, extract_shape.decoded_pixels, pixels) catch
                    return error.InferenceDecodedPixelsExceeded;
            }
            try extract_capabilities.validateInvocation(.extract, extract_shape);
            var result = try state.node.extractDirect(alloc, parsed.value.model, request);
            defer result.deinit();
            break :blk try std.json.Stringify.valueAlloc(alloc, result.json, .{});
        },
        .chunk_input => blk: {
            var parsed = try std.json.parseFromSlice(ChunkInputRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            var input = parsed.value.input;
            switch (input) {
                .text => if (parsed.value.attachment_count != 0 or context.binary_payloads_len != 0 or context.attachment_refs_len != 0)
                    return error.InvalidArguments,
                .binary => |*binary| {
                    if (parsed.value.attachment_count != 1 or context.binary_payloads_len != 1 or
                        context.attachment_refs_len != 1 or context.binary_payloads == null)
                        return error.InvalidArguments;
                    try validateProviderAttachmentRefs(1, context.attachment_refs, context.attachment_refs_len);
                    const ref = providerAttachmentRefForItem(context.attachment_refs.?, 1, 0) orelse return error.InvalidArguments;
                    const payload = context.binary_payloads.?[ref.attachment_index];
                    if (binary.data.len != 0 or !mimeDeclarationsCompatible(binary.mime_type, payload.content_type.slice()))
                        return error.InvalidArguments;
                    binary.data = payload.bytes.slice();
                },
            }
            const cfg = parsed.value.config;
            const chunk_capabilities = try localModelCapabilities(&state.node, state.io, parsed.value.model, .chunk);
            var chunk_shape = antfly.inference.work.InvocationShape{ .item_count = 1 };
            switch (input) {
                .text => |text_value| {
                    chunk_shape.modalities.text = true;
                    chunk_shape.text_bytes = text_value.len;
                    chunk_shape.max_text_bytes_per_item = text_value.len;
                    try chunk_capabilities.validateMimeType("text/plain");
                },
                .binary => |binary| {
                    chunk_shape.encoded_media_bytes = binary.data.len;
                    chunk_shape.max_media_parts_per_item = 1;
                    const essence = antfly.inference.work.mimeTypeEssence(binary.mime_type) catch
                        return error.UnsupportedInferenceMimeType;
                    if (std.ascii.startsWithIgnoreCase(essence, "audio/"))
                        chunk_shape.modalities.audio = true
                    else if (std.ascii.startsWithIgnoreCase(essence, "image/"))
                        chunk_shape.modalities.image = true
                    else
                        chunk_shape.modalities.document = true;
                    try chunk_capabilities.validateMimeType(binary.mime_type);
                },
            }
            try chunk_capabilities.validateInvocation(.chunk, chunk_shape);
            const result = try state.node.chunkInputDirect(alloc, parsed.value.model, input, .{
                .model = if (cfg.model.len > 0) cfg.model else "fixed",
                .max_chunks = if (cfg.max_chunks > 0) @intCast(cfg.max_chunks) else 50,
                .threshold = cfg.threshold,
                .text = .{
                    .target_tokens = cfg.defaultedTargetTokens(),
                    .overlap_tokens = cfg.defaultedOverlapTokens(),
                    .separator = cfg.defaultedSeparator(),
                },
                .audio = .{
                    .window_duration_ms = if (cfg.audio.window_duration_ms > 0) cfg.audio.window_duration_ms else 30_000,
                    .overlap_duration_ms = cfg.audio.overlap_duration_ms,
                },
            });
            defer inference_chunker.types.freeChunks(alloc, result);
            // JSON parsing allocates text/data in the receiving allocator. Mark
            // the wire copy as owning those allocations, then restore the
            // borrowed direct result before its local destructor runs.
            const ownership = try alloc.alloc(struct { mime_type: bool, text: bool, data: bool }, result.len);
            defer alloc.free(ownership);
            for (result, ownership) |*chunk, *original| {
                original.* = .{
                    .mime_type = chunk.owns_mime_type,
                    .text = chunk.owns_text,
                    .data = chunk.owns_data,
                };
                chunk.owns_mime_type = true;
                chunk.owns_text = chunk.text != null;
                chunk.owns_data = chunk.data != null;
            }
            defer for (result, ownership) |*chunk, original| {
                chunk.owns_mime_type = original.mime_type;
                chunk.owns_text = original.text;
                chunk.owns_data = original.data;
            };
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .rewrite_texts => blk: {
            var parsed = try std.json.parseFromSlice(RewriteTextsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            try validateLinkedTextInvocation(&state.node, state.io, parsed.value.model, .rewrite, parsed.value.inputs, 0, 0, 0);
            const result = try state.node.rewriteTextsDirect(alloc, parsed.value.model, parsed.value.inputs);
            defer {
                for (result) |item| alloc.free(item);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .classify_texts => blk: {
            var parsed = try std.json.parseFromSlice(ClassifyTextsRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const request = parsed.value.request;
            try validateLinkedTextInvocation(&state.node, state.io, parsed.value.model, .classify, request.texts, 0, request.labels.len, 0);
            const result = try state.node.classifyTextsDirect(
                alloc,
                parsed.value.model,
                request.texts,
                request.labels,
                request.hypothesis_template,
                request.multi_label,
            );
            defer {
                for (result) |item| alloc.free(item);
                alloc.free(result);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, result, .{});
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
    const capabilities = try localModelCapabilities(node, io, model, .embed);
    var shape = antfly.inference.work.InvocationShape{ .item_count = parts.len };
    for (parts) |part| switch (part) {
        .text => |text| {
            shape.modalities.text = true;
            shape.text_bytes = std.math.add(usize, shape.text_bytes, text.len) catch
                return error.InferenceTextBytesExceeded;
            shape.max_text_bytes_per_item = @max(shape.max_text_bytes_per_item, text.len);
            try capabilities.validateMimeType("text/plain");
        },
        .media_url => |url| {
            shape.modalities.image = true;
            shape.max_media_parts_per_item = @max(shape.max_media_parts_per_item, 1);
            if (try antfly.inference.work.parseInlineDataUri(url)) |_| {
                const next_encoded = std.math.add(usize, shape.encoded_media_bytes, url.len) catch
                    return error.InferenceEncodedBytesExceeded;
                if (capabilities.batch.max_encoded_media_bytes) |limit| {
                    if (next_encoded > limit) return error.InferenceEncodedBytesExceeded;
                }
                var decoded = try antfly.inference.work.decodeInlineDataUriAlloc(alloc, url);
                defer decoded.deinit(alloc);
                try capabilities.validateMimeType(decoded.mime_type);
                shape.encoded_media_bytes = next_encoded;
                const pixels = try antfly.inference.work.encodedImagePixels(decoded.mime_type, decoded.data);
                shape.decoded_pixels = std.math.add(u64, shape.decoded_pixels, pixels) catch
                    return error.InferenceDecodedPixelsExceeded;
            }
        },
        .binary => |media| {
            try capabilities.validateMimeType(media.mime_type);
            const is_image = mimeEssenceStartsWith(media.mime_type, "image/");
            if (is_image) {
                shape.modalities.image = true;
            } else if (mimeEssenceStartsWith(media.mime_type, "audio/")) {
                shape.modalities.audio = true;
            } else if (mimeEssencesEqual(media.mime_type, "application/pdf")) {
                shape.modalities.document = true;
            } else if (mimeEssencesEqual(media.mime_type, "text/plain")) {
                shape.modalities.text = true;
            } else return error.UnsupportedInferenceMimeType;
            shape.encoded_media_bytes = std.math.add(usize, shape.encoded_media_bytes, media.data.len) catch
                return error.InferenceEncodedBytesExceeded;
            if (is_image) {
                const pixels = try antfly.inference.work.encodedImagePixels(media.mime_type, media.data);
                shape.decoded_pixels = std.math.add(u64, shape.decoded_pixels, pixels) catch
                    return error.InferenceDecodedPixelsExceeded;
            }
            shape.max_media_parts_per_item = @max(shape.max_media_parts_per_item, 1);
        },
    };
    try capabilities.validateInvocation(.embed, shape);
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
    io: std.Io,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    if (messages.len == 0) return error.InvalidGenerationRequest;
    const capabilities = try localModelCapabilities(node, io, model, .generate);
    const preflight = try preflightLocalGenerateMessagesInternal(messages, null, capabilities);
    try validateLocalGenerateCapabilities(capabilities, preflight, 0);
    var admission = try node.beginDirectGenerateAdmission(preflight, 256);
    defer admission.deinit();

    var converted = try convertLocalGenerateMessages(alloc, messages, preflight.decoded_media_bytes);
    defer converted.deinit(alloc);
    try validateLocalGenerateCapabilities(
        capabilities,
        preflight,
        try localGenerateDecodedPixels(converted.messages),
    );
    return try node.generateMessagesDirectAdmitted(alloc, model, converted.messages, &admission);
}

fn localAntflyGenerateMessagesWithAttachments(
    ptr: *anyopaque,
    io: std.Io,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
    attachments: []const antfly.inference.work.Attachment,
) anyerror![]u8 {
    const node: *inference.server.Node = @ptrCast(@alignCast(ptr));
    if (messages.len == 0) return error.InvalidGenerationRequest;
    const capabilities = try localModelCapabilities(node, io, model, .generate);
    const preflight = try preflightLocalGenerateMessagesInternal(messages, attachments, capabilities);
    try validateLocalGenerateCapabilities(capabilities, preflight, 0);
    var admission = try node.beginDirectGenerateAdmission(preflight, 256);
    defer admission.deinit();

    var converted = try convertLocalGenerateMessagesInternal(alloc, messages, preflight.decoded_media_bytes, attachments);
    defer converted.deinit(alloc);
    try validateLocalGenerateCapabilities(
        capabilities,
        preflight,
        try localGenerateDecodedPixels(converted.messages),
    );
    return try node.generateMessagesDirectAdmitted(alloc, model, converted.messages, &admission);
}

fn validateLocalGenerateCapabilities(
    capabilities: antfly.inference.work.InferenceCapabilities,
    preflight: inference.server.Node.DirectGeneratePreflight,
    decoded_pixels: u64,
) !void {
    try capabilities.validateInvocation(.generate, .{
        .item_count = 1,
        .modalities = .{
            .text = preflight.text_bytes > 0,
            .image = preflight.image_count > 0,
            .audio = preflight.has_audio,
        },
        .encoded_media_bytes = preflight.encoded_media_bytes,
        .decoded_pixels = decoded_pixels,
        .max_media_parts_per_item = preflight.media_count,
        .text_bytes = preflight.text_bytes,
        .max_text_bytes_per_item = preflight.text_bytes,
        .requested_output_tokens_per_item = 256,
    });
}

fn localGenerateDecodedPixels(messages: []const inference.pipelines.GenerationMessage) !u64 {
    var decoded_pixels: u64 = 0;
    for (messages) |message| if (message.image_bytes) |images| {
        for (images) |image| {
            const info = inference.pipelines.image.inspectEncodedForInference(image, null) catch
                return error.InvalidInferenceMedia;
            decoded_pixels = std.math.add(u64, decoded_pixels, try info.pixels()) catch
                return error.InferenceDecodedPixelsExceeded;
        }
    };
    return decoded_pixels;
}

/// The provider ABI is an executor boundary, not a trusted shortcut around
/// model admission. Callers normally preflight through Runtime, but plugins and
/// future linked clients may invoke the stable ABI directly.
fn validateEncodedReadCapabilities(
    capabilities: antfly.inference.work.InferenceCapabilities,
    request: antfly.readers.EncodedRequest,
) !void {
    var encoded_media_bytes: usize = 0;
    var decoded_pixels: u64 = 0;
    for (request.images) |image| {
        try capabilities.validateMimeType(image.mime_type);
        const resident = try antfly.inference.work.AttachmentTransport.borrowed_binary.wireSize(
            image.bytes.len,
            image.mime_type.len,
        );
        encoded_media_bytes = std.math.add(usize, encoded_media_bytes, resident) catch
            return error.InferenceEncodedBytesExceeded;
        const pixels = try antfly.inference.work.encodedImagePixels(image.mime_type, image.bytes);
        decoded_pixels = std.math.add(u64, decoded_pixels, pixels) catch
            return error.InferenceDecodedPixelsExceeded;
    }
    const output_tokens: usize = if (request.max_tokens) |tokens|
        if (tokens > 0) std.math.cast(usize, tokens) orelse std.math.maxInt(usize) else 0
    else
        0;
    const prompt_bytes = if (request.prompt) |prompt| prompt.len else 0;
    try capabilities.validateInvocation(.read, .{
        .item_count = request.images.len,
        .modalities = if (request.images.len > 0) .{ .image = true } else .{},
        .encoded_media_bytes = encoded_media_bytes,
        .decoded_pixels = decoded_pixels,
        .max_media_parts_per_item = if (request.images.len > 0) 1 else 0,
        .text_bytes = prompt_bytes,
        .max_text_bytes_per_item = prompt_bytes,
        // Zero means unknown here. The linked reader owns the tokenizer and
        // enforces its exact, fully rendered prompt before model execution.
        .max_input_tokens_per_item = 0,
        .requested_output_tokens_per_item = output_tokens,
    });
}

test "encoded reader ABI enforces resolved model capabilities" {
    var bytes = [_]u8{0} ** 24;
    @memcpy(bytes[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, bytes[16..20], 2, .big);
    std.mem.writeInt(u32, bytes[20..24], 3, .big);
    const image = antfly.readers.EncodedImage{ .bytes = &bytes, .mime_type = "image/png" };
    const capabilities = antfly.inference.work.InferenceCapabilities{
        .task = .read,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{
            .mode = .none,
            .preferred_items = 1,
            .max_items = 1,
            .max_encoded_media_bytes = bytes.len,
            .max_decoded_pixels = 6,
            .max_media_parts_per_item = 1,
        },
        .task_limits = .{ .max_output_tokens_per_item = 4 },
        .output = .read_result,
    };
    try validateEncodedReadCapabilities(capabilities, .{ .images = &.{image}, .max_tokens = 4 });
    try std.testing.expectError(
        error.InferenceBatchTooLarge,
        validateEncodedReadCapabilities(capabilities, .{ .images = &.{ image, image } }),
    );
    const oversized = bytes ++ [_]u8{0};
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        validateEncodedReadCapabilities(capabilities, .{ .images = &.{.{ .bytes = &oversized, .mime_type = "image/png" }} }),
    );
    try std.testing.expectError(
        error.UnsupportedInferenceMimeType,
        validateEncodedReadCapabilities(capabilities, .{ .images = &.{.{ .bytes = &bytes, .mime_type = "image/jpeg" }} }),
    );
    try std.testing.expectError(
        error.InferenceOutputTokensExceeded,
        validateEncodedReadCapabilities(capabilities, .{ .images = &.{image}, .max_tokens = 5 }),
    );
    var too_many_pixels = bytes;
    std.mem.writeInt(u32, too_many_pixels[16..20], 3, .big);
    try std.testing.expectError(
        error.InferenceDecodedPixelsExceeded,
        validateEncodedReadCapabilities(capabilities, .{ .images = &.{.{ .bytes = &too_many_pixels, .mime_type = "image/png" }} }),
    );
}

fn validateLinkedTextInvocation(
    node: *inference.server.Node,
    io: std.Io,
    model: []const u8,
    task: antfly.inference.work.Task,
    items: []const []const u8,
    additional_text_bytes: usize,
    candidates: usize,
    schema_bytes: usize,
) !void {
    const capabilities = try localModelCapabilities(node, io, model, task);
    try capabilities.validateMimeType("text/plain");
    var shape = antfly.inference.work.InvocationShape{
        .item_count = if (task == .generate) 1 else switch (capabilities.result_cardinality) {
            .one_per_item => items.len,
            .one_per_request => 1,
        },
        .modalities = .{ .text = true },
        .text_bytes = additional_text_bytes,
        .max_text_bytes_per_item = additional_text_bytes,
        .max_candidates_per_request = candidates,
        .schema_bytes = schema_bytes,
    };
    for (items) |item| {
        shape.text_bytes = std.math.add(usize, shape.text_bytes, item.len) catch
            return error.InferenceTextBytesExceeded;
        shape.max_text_bytes_per_item = @max(shape.max_text_bytes_per_item, item.len);
    }
    try capabilities.validateInvocation(task, shape);
}

fn localModelCapabilities(
    node: *inference.server.Node,
    io: std.Io,
    model: []const u8,
    task: antfly.inference.work.Task,
) !antfly.inference.work.InferenceCapabilities {
    if (task == .chunk and
        (std.mem.eql(u8, model, "fixed") or
            std.mem.eql(u8, model, "fixed_bert") or
            std.mem.eql(u8, model, "fixed_bpe") or
            std.mem.eql(u8, model, "fixed-bert-tokenizer")))
    {
        return .{
            .task = .chunk,
            .input_modalities = .{ .text = true },
            .accepted_mime_types = .{ .text_plain = true },
            .input_granularity = .item,
            .batch = .{
                .mode = .none,
                .preferred_items = 1,
                .max_items = 1,
                .max_encoded_media_bytes = 0,
            },
            .output = .chunks,
            .result_cardinality = .one_per_request,
            .prompt_policy = .model_default,
            .borrowed_attachments = false,
        };
    }
    const scope = switch (task) {
        .read => "readers",
        .generate => "generators",
        .embed => "embedders",
        .rerank => "rerankers",
        .chunk => "chunkers",
        .extract => "extractors",
        .rewrite => "rewriters",
        .classify => "classifiers",
        .transcribe => "transcribers",
    };
    const model_path = try node.resolveModelPath(io, if (model.len > 0) model else null, scope);
    defer node.allocator.free(model_path);
    var manifest = try inference.models.manifest.loadFromDir(node.allocator, model_path);
    defer manifest.deinit();

    var modalities = antfly.inference.work.Modalities{};
    for (manifest.inputs) |input| {
        if (std.mem.eql(u8, input, "text")) modalities.text = true;
        if (std.mem.eql(u8, input, "image")) modalities.image = true;
        if (std.mem.eql(u8, input, "audio")) modalities.audio = true;
        if (std.mem.eql(u8, input, "document") or std.mem.eql(u8, input, "pdf")) modalities.document = true;
    }
    // Older manifests may omit explicit inputs. Use resolved architecture
    // metadata, never the user-provided model name, for compatibility.
    if (@as(u8, @bitCast(modalities)) == 0) switch (task) {
        .read => modalities.image = true,
        .generate => {
            modalities.text = true;
            modalities.image = manifest.gguf_projector_path != null;
        },
        .embed => {
            modalities.text = true;
            modalities.image = manifest.native_arch_hint == .clip;
            modalities.audio = manifest.native_arch_hint == .clap;
        },
        .rerank, .chunk, .rewrite, .classify => modalities.text = true,
        .extract => modalities.text = true,
        .transcribe => modalities.audio = true,
    };

    const executor_modalities = inference.server.resolvedExecutorModalities(
        @tagName(task),
        modalities.text,
        modalities.image,
        modalities.audio,
        modalities.document,
    );
    modalities = .{
        .text = executor_modalities.text,
        .image = executor_modalities.image,
        .audio = executor_modalities.audio,
        .document = executor_modalities.document,
    };

    const native_batch_read = task == .read and manifest.native_arch_hint == .florence;
    const task_max_items = inference.server.resolvedTaskMaxItems(@tagName(task));
    const max_images = if (!modalities.image)
        0
    else if (task == .generate)
        std.math.mul(usize, task_max_items, inference.server.max_generate_media_parts_per_item) catch std.math.maxInt(usize)
    else
        task_max_items;
    const resolved_batch = try inference.server.resolveInferenceBatchCapabilities(
        @tagName(task),
        manifest.capabilities,
        native_batch_read,
        inference.server.requestMediaMaxBytes(node),
        if (max_images > 0) inference.server.requestMediaMaxDecodedPixels(node, max_images) else 0,
        modalities.image,
        modalities.audio,
        modalities.document,
    );
    const output = std.meta.stringToEnum(
        antfly.inference.work.OutputKind,
        inference.server.resolvedTaskOutput(@tagName(task)),
    ).?;
    var result = antfly.inference.work.InferenceCapabilities{
        .task = task,
        .input_modalities = modalities,
        .accepted_mime_types = .{
            .text_plain = modalities.text,
            .application_pdf = modalities.document,
            .image_png = modalities.image,
            .image_jpeg = modalities.image,
            .image_webp = modalities.image,
            .audio_wav = modalities.audio,
            .audio_mpeg = modalities.audio,
        },
        .input_granularity = if (modalities.document)
            .document
        else if (modalities.image)
            .page
        else if (modalities.text and (task == .read or task == .generate or task == .embed))
            .chunk
        else
            .item,
        .batch = .{
            .mode = switch (resolved_batch.mode) {
                .none => .none,
                .serial_compatibility => .serial_compatibility,
                .native => .native,
            },
            .preferred_items = resolved_batch.preferred_items,
            .max_items = resolved_batch.max_items,
            .max_encoded_media_bytes = resolved_batch.max_encoded_media_bytes,
            .max_decoded_pixels = resolved_batch.max_decoded_pixels,
            .max_media_parts_per_item = resolved_batch.max_media_parts_per_item,
            .per_item_failures = resolved_batch.per_item_failures,
        },
        .task_limits = .{
            .max_text_bytes_per_item = resolved_batch.max_text_bytes_per_item,
            .max_input_tokens_per_item = resolved_batch.max_input_tokens_per_item,
            .max_output_tokens_per_item = resolved_batch.max_output_tokens_per_item,
            .max_candidates_per_request = resolved_batch.max_candidates_per_request,
            .max_schema_bytes = resolved_batch.max_schema_bytes,
        },
        .output = output,
        .result_cardinality = std.meta.stringToEnum(
            antfly.inference.work.ResultCardinality,
            inference.server.resolvedTaskResultCardinality(@tagName(task)),
        ).?,
        .prompt_policy = std.meta.stringToEnum(
            antfly.inference.work.PromptPolicy,
            inference.server.resolvedTaskPromptPolicy(@tagName(task)),
        ).?,
        .borrowed_attachments = task == .read or task == .generate or task == .embed or task == .extract,
    };
    for (manifest.capabilities) |capability| {
        const prefix = "inference.mime_type=";
        if (std.mem.startsWith(u8, capability, prefix)) {
            const mime_type = capability[prefix.len..];
            if (mimeEssenceStartsWith(mime_type, "image/") and
                !inference.pipelines.image.supportsMimeEssence(mime_type))
            {
                return error.InvalidInferenceCapabilities;
            }
            try result.accepted_mime_types.add(mime_type);
        }
    }
    try result.validate();
    return result;
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
    is_data_uri: bool = false,
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
    var is_data_uri = false;
    var decoded_bytes: usize = undefined;
    if (antfly.inference.work.hasDataUriScheme(raw)) {
        const parsed = (try antfly.inference.work.parseInlineDataUri(raw)) orelse
            return error.UnsupportedGeneratorProvider;
        if (declared_mime_type) |declared| {
            if (!mimeDeclarationsCompatible(declared, parsed.mime_type))
                return error.UnsupportedGeneratorProvider;
        }
        mime_type = parsed.mime_type;
        payload = parsed.payload;
        decoded_bytes = parsed.decoded_size;
        is_data_uri = true;
    } else {
        decoded_bytes = try antfly.inference.work.validateCanonicalStandardBase64(payload);
    }

    return .{
        .payload = payload,
        .mime_type = mime_type,
        .encoded_bytes = raw.len,
        .decoded_bytes = decoded_bytes,
        .is_data_uri = is_data_uri,
    };
}

fn mimeEssencesEqual(a: []const u8, b: []const u8) bool {
    const a_essence = antfly.inference.work.mimeTypeEssence(a) catch return false;
    const b_essence = antfly.inference.work.mimeTypeEssence(b) catch return false;
    return std.ascii.eqlIgnoreCase(a_essence, b_essence);
}

fn mimeDeclarationsCompatible(declared: []const u8, attachment: []const u8) bool {
    return antfly.inference.work.mediaTypesCompatible(declared, attachment);
}

fn mimeEssenceStartsWith(value: []const u8, prefix: []const u8) bool {
    const essence = antfly.inference.work.mimeTypeEssence(value) catch return false;
    return std.ascii.startsWithIgnoreCase(essence, prefix);
}

fn addLocalGenerateBytes(total: *usize, amount: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch return error.RemoteContentTooLarge;
}

fn addLocalGenerateMediaPreflight(
    preflight: *inference.server.Node.DirectGeneratePreflight,
    descriptor: LocalGenerateMediaDescriptor,
    image_only: bool,
) !void {
    const is_image = mimeEssenceStartsWith(descriptor.mime_type, "image/");
    const is_audio = mimeEssenceStartsWith(descriptor.mime_type, "audio/");
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
    return try preflightLocalGenerateMessagesInternal(messages, null, null);
}

fn preflightLocalGenerateMessagesInternal(
    messages: []const antfly.inference.ChatMessage,
    attachments: ?[]const antfly.inference.work.Attachment,
    capabilities: ?antfly.inference.work.InferenceCapabilities,
) !inference.server.Node.DirectGeneratePreflight {
    var preflight: inference.server.Node.DirectGeneratePreflight = .{};
    var attachment_index: usize = 0;
    for (messages) |message| {
        const content = message.content orelse continue;
        switch (content) {
            .text => |text_value| try addLocalGenerateBytes(&preflight.text_bytes, text_value.len),
            .parts => |parts| for (parts) |part| switch (part) {
                .text => |text_value| try addLocalGenerateBytes(&preflight.text_bytes, text_value.len),
                .image_url => |image_url| {
                    const descriptor = try inspectLocalGenerateDataUri(image_url.url, null);
                    if (capabilities) |resolved| try resolved.validateMimeType(descriptor.mime_type);
                    try addLocalGenerateMediaPreflight(&preflight, descriptor, true);
                },
                .media => |media| {
                    const raw = media.url orelse media.data;
                    if (raw.len == 0 and attachments != null) {
                        if (attachment_index >= attachments.?.len) return error.InvalidArguments;
                        const attachment = attachments.?[attachment_index];
                        try attachment.validate();
                        if (capabilities) |resolved| try resolved.validateMimeType(attachment.content_type);
                        if (media.mime_type.len > 0 and !mimeDeclarationsCompatible(media.mime_type, attachment.content_type))
                            return error.InvalidArguments;
                        try addLocalGenerateMediaPreflight(&preflight, .{
                            .payload = attachment.bytes,
                            .mime_type = attachment.content_type,
                            .encoded_bytes = attachment.bytes.len,
                            .decoded_bytes = attachment.bytes.len,
                        }, false);
                        attachment_index += 1;
                    } else {
                        const descriptor = try inspectLocalGenerateDataUri(raw, media.mime_type);
                        if (capabilities) |resolved| try resolved.validateMimeType(descriptor.mime_type);
                        try addLocalGenerateMediaPreflight(
                            &preflight,
                            descriptor,
                            false,
                        );
                    }
                },
            },
        }
    }
    if (attachments) |values| if (attachment_index != values.len) return error.InvalidArguments;
    return preflight;
}

pub fn convertLocalGenerateMessages(
    alloc: std.mem.Allocator,
    messages: []const antfly.inference.ChatMessage,
    decoded_media_bytes: usize,
) !LocalGenerateMessages {
    return try convertLocalGenerateMessagesInternal(alloc, messages, decoded_media_bytes, null);
}

fn convertLocalGenerateMessagesInternal(
    alloc: std.mem.Allocator,
    messages: []const antfly.inference.ChatMessage,
    decoded_media_bytes: usize,
    attachments: ?[]const antfly.inference.work.Attachment,
) !LocalGenerateMessages {
    var out = LocalGenerateMessages{
        .messages = try alloc.alloc(inference.pipelines.GenerationMessage, messages.len),
    };
    errdefer out.deinit(alloc);

    var decode_budget = LocalGenerateDecodeBudget{ .remaining_bytes = decoded_media_bytes };
    var attachment_index: usize = 0;
    for (messages, 0..) |message, i|
        out.messages[i] = try convertLocalGenerateMessage(alloc, &out, message, &decode_budget, attachments, &attachment_index);
    if (decode_budget.remaining_bytes != 0) return error.InvalidGenerationAdmission;
    if (attachments) |values| if (attachment_index != values.len) return error.InvalidArguments;
    return out;
}

fn convertLocalGenerateMessage(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    message: antfly.inference.ChatMessage,
    decode_budget: *LocalGenerateDecodeBudget,
    attachments: ?[]const antfly.inference.work.Attachment,
    attachment_index: *usize,
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
        .parts => |parts| try convertLocalGenerateParts(alloc, owner, role, parts, decode_budget, attachments, attachment_index),
    };
}

fn convertLocalGenerateParts(
    alloc: std.mem.Allocator,
    owner: *LocalGenerateMessages,
    role: []const u8,
    parts: []const antfly.inference.ContentPart,
    decode_budget: *LocalGenerateDecodeBudget,
    attachments: ?[]const antfly.inference.work.Attachment,
    attachment_index: *usize,
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
                if (!mimeEssenceStartsWith(decoded.mime_type, "image/")) {
                    return error.UnsupportedGeneratorProvider;
                }
                _ = try antfly.inference.work.encodedImagePixels(decoded.mime_type, decoded.data);
                try images.append(alloc, decoded.data);
                try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                try owner.owned_media.append(alloc, decoded.data);
                decoded_owned = false;
            },
            .media => |media| {
                const raw = media.url orelse media.data;
                if (raw.len == 0 and attachments != null) {
                    if (attachment_index.* >= attachments.?.len) return error.InvalidArguments;
                    const attachment = attachments.?[attachment_index.*];
                    try attachment.validate();
                    if (media.mime_type.len > 0 and !mimeDeclarationsCompatible(media.mime_type, attachment.content_type))
                        return error.InvalidArguments;
                    try decode_budget.reserve(attachment.bytes.len);
                    if (mimeEssenceStartsWith(attachment.content_type, "image/")) {
                        _ = try antfly.inference.work.encodedImagePixels(attachment.content_type, attachment.bytes);
                        try images.append(alloc, attachment.bytes);
                        try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                    } else if (mimeEssenceStartsWith(attachment.content_type, "audio/")) {
                        try audio.append(alloc, attachment.bytes);
                        try out_parts.append(alloc, .{ .audio = audio.items.len - 1 });
                    } else return error.UnsupportedGeneratorProvider;
                    attachment_index.* += 1;
                    continue;
                }
                const decoded = try decodeLocalGenerateDataUri(alloc, raw, media.mime_type, decode_budget);
                var decoded_owned = true;
                errdefer if (decoded_owned) alloc.free(decoded.data);
                if (mimeEssenceStartsWith(decoded.mime_type, "image/")) {
                    _ = try antfly.inference.work.encodedImagePixels(decoded.mime_type, decoded.data);
                    try images.append(alloc, decoded.data);
                    try out_parts.append(alloc, .{ .image = images.items.len - 1 });
                    try owner.owned_media.append(alloc, decoded.data);
                    decoded_owned = false;
                } else if (mimeEssenceStartsWith(decoded.mime_type, "audio/")) {
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
    if (descriptor.is_data_uri) {
        var decoded = try antfly.inference.work.decodeInlineDataUriAlloc(alloc, raw);
        errdefer decoded.deinit(alloc);
        if (decoded.data.len != descriptor.decoded_bytes or
            !mimeDeclarationsCompatible(descriptor.mime_type, decoded.mime_type))
            return error.InvalidGenerationAdmission;
        alloc.free(decoded.mime_type);
        const data = decoded.data;
        decoded = undefined;
        return .{ .data = data, .mime_type = descriptor.mime_type };
    }
    const decoded = try alloc.alloc(u8, descriptor.decoded_bytes);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, descriptor.payload);
    return .{ .data = decoded, .mime_type = descriptor.mime_type };
}

test "linked generator validates concrete MIME and decoded pixels" {
    const uri = "data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD";
    const messages = [_]antfly.inference.ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{.{ .image_url = .{ .url = uri } }} },
    }};
    var capabilities = antfly.inference.work.InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .image = true },
        .accepted_mime_types = .{ .text_plain = true, .image_jpeg = true },
        .input_granularity = .page,
        .batch = .{
            .mode = .serial_compatibility,
            .preferred_items = 1,
            .max_items = 1,
            .max_encoded_media_bytes = uri.len,
            .max_decoded_pixels = 5,
            .max_media_parts_per_item = 1,
        },
        .output = .generated_text,
    };
    try std.testing.expectError(
        error.UnsupportedInferenceMimeType,
        preflightLocalGenerateMessagesInternal(&messages, null, capabilities),
    );

    capabilities.accepted_mime_types = .{ .text_plain = true, .image_png = true };
    const preflight = try preflightLocalGenerateMessagesInternal(&messages, null, capabilities);
    try validateLocalGenerateCapabilities(capabilities, preflight, 0);
    var converted = try convertLocalGenerateMessages(
        std.testing.allocator,
        &messages,
        preflight.decoded_media_bytes,
    );
    defer converted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 6), try localGenerateDecodedPixels(converted.messages));
    try std.testing.expectError(
        error.InferenceDecodedPixelsExceeded,
        validateLocalGenerateCapabilities(capabilities, preflight, 6),
    );
}

// ---------------------------------------------------------------
