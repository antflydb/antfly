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

//! Consumer-local adapters for the independently code-generated inference
//! runtime. Domain callers retain `AntflyProvider`; only dependency-neutral C
//! ABI descriptors, stable status, and `FailureIdentity` cross the link edge.

const std = @import("std");
const managed_embedder = @import("../inference/managed_embedder.zig");
const inference_types = @import("../inference/types.zig");
const template = @import("../template.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const bridge = @import("inference_bridge.zig");
const failure_identity = @import("runtime_failure_identity");

const DenseApi = struct {
    execute: *const fn (
        *const bridge.DenseEmbeddingRequest,
        *bridge.DenseEmbeddingResult,
        *bridge.FailureIdentity,
    ) callconv(.c) bridge.Status,
    destroy: *const fn (*bridge.DenseEmbeddingResult) callconv(.c) void,
};

const linked_dense_api = DenseApi{
    .execute = bridge.antfly_standalone_inference_embed_dense,
    .destroy = bridge.antfly_standalone_inference_dense_result_destroy,
};

const SparseApi = struct {
    execute: *const fn (*const bridge.DenseEmbeddingRequest, *bridge.SparseEmbeddingResult, *bridge.FailureIdentity) callconv(.c) bridge.Status,
    destroy: *const fn (*bridge.SparseEmbeddingResult) callconv(.c) void,
};
const linked_sparse_api = SparseApi{
    .execute = bridge.antfly_standalone_inference_embed_sparse,
    .destroy = bridge.antfly_standalone_inference_sparse_result_destroy,
};

const RerankApi = struct {
    execute: *const fn (*const bridge.RerankRequest, *bridge.FloatResult, *bridge.FailureIdentity) callconv(.c) bridge.Status,
    destroy: *const fn (*bridge.FloatResult) callconv(.c) void,
};
const linked_rerank_api = RerankApi{
    .execute = bridge.antfly_standalone_inference_rerank,
    .destroy = bridge.antfly_standalone_inference_float_result_destroy,
};

const ListModelsApi = struct {
    execute: *const fn (*const bridge.HandleRequest, *bridge.BytesResult, *bridge.FailureIdentity) callconv(.c) bridge.Status,
    destroy: *const fn (*bridge.BytesResult) callconv(.c) void,
};
const linked_list_models_api = ListModelsApi{
    .execute = bridge.antfly_standalone_inference_list_models,
    .destroy = bridge.antfly_standalone_inference_bytes_result_destroy,
};

pub fn installProviderAdapters(provider: *managed_embedder.AntflyProvider) void {
    // Preserve the provider's existing opaque Node handle while this migration
    // is mixed: both the ABI shims and the remaining legacy callbacks consume
    // that same handle. Once every callback is migrated, the provider table
    // export and this transitional constraint disappear together.
    provider.embed_dense_texts = embedDenseTexts;
    provider.embed_dense_texts_with_context = embedDenseTextsWithContext;
    provider.embed_sparse_texts = embedSparseTexts;
    provider.embed_dense_parts = embedDenseParts;
    provider.embed_dense_parts_with_context = embedDensePartsWithContext;
    provider.rerank_texts = rerankTexts;
    provider.list_models_json = listModelsJson;
    provider.generate_text = generateText;
    provider.generate_messages = generateMessages;
}

fn embedDenseTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![][]f32 {
    return embedDenseTextsInner(handle, alloc, model, texts, null);
}

fn embedDenseTextsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    const result = try embedDenseTextsInner(handle, alloc, model, texts, context);
    context.check() catch |err| {
        freeDenseBatch(alloc, result);
        return err;
    };
    return result;
}

fn embedDenseTextsInner(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: ?managed_embedder.EmbeddingRequestContext,
) ![][]f32 {
    return embedDenseTextsInnerWithApi(handle, alloc, model, texts, context, linked_dense_api);
}

fn embedDenseTextsInnerWithApi(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: ?managed_embedder.EmbeddingRequestContext,
    api: DenseApi,
) ![][]f32 {
    const input = try alloc.alloc(bridge.String, texts.len);
    defer alloc.free(input);
    for (texts, 0..) |text, i| input[i] = .init(text);

    const cancellation = if (context) |ctx| ctx.cancellation else null;
    var result: bridge.DenseEmbeddingResult = .{};
    defer api.destroy(&result);
    var failure: bridge.FailureIdentity = .{};
    const status = api.execute(&.{
        .handle = handle,
        .model = .init(model),
        .texts = if (input.len == 0) null else input.ptr,
        .text_count = input.len,
        .has_deadline = @intFromBool(if (context) |ctx| ctx.deadline_ns != null else false),
        .deadline_ns = if (context) |ctx| ctx.deadline_ns orelse 0 else 0,
        .cancellation_ctx = if (cancellation) |flag| flag else null,
        .cancellation_probe = if (cancellation != null) cancellationProbe else null,
    }, &result, &failure);
    try acceptFailure(status, &failure, .embed_dense_texts);
    try validateDenseResult(result);
    if (result.vector_count != texts.len) return error.InvalidBoundaryQueryResponse;

    const descriptors = if (result.vector_count == 0)
        &.{}
    else
        result.vectors.?[0..result.vector_count];
    const vectors = try alloc.alloc([]f32, descriptors.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| alloc.free(vector);
        alloc.free(vectors);
    }
    for (descriptors, 0..) |descriptor, i| {
        vectors[i] = try alloc.dupe(f32, descriptor.slice());
        initialized += 1;
    }
    return vectors;
}

fn embedSparseTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![]db_embedder.SparseEmbedding {
    const input = try alloc.alloc(bridge.String, texts.len);
    defer alloc.free(input);
    for (texts, 0..) |text_value, i| input[i] = .init(text_value);
    var result: bridge.SparseEmbeddingResult = .{};
    defer linked_sparse_api.destroy(&result);
    var failure: bridge.FailureIdentity = .{};
    const status = linked_sparse_api.execute(&.{
        .handle = handle,
        .model = .init(model),
        .texts = if (input.len == 0) null else input.ptr,
        .text_count = input.len,
    }, &result, &failure);
    try acceptFailure(status, &failure, .embed_sparse_texts);
    try validateSparseResult(result);
    if (result.vector_count != texts.len) return error.InvalidBoundaryQueryResponse;

    const descriptors = if (result.vector_count == 0) &.{} else result.vectors.?[0..result.vector_count];
    const vectors = try alloc.alloc(db_embedder.SparseEmbedding, descriptors.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |*vector| vector.deinit(alloc);
        alloc.free(vectors);
    }
    for (descriptors, 0..) |descriptor, i| {
        const indices = if (descriptor.value_count == 0) &.{} else descriptor.indices.?[0..descriptor.value_count];
        const values = if (descriptor.value_count == 0) &.{} else descriptor.values.?[0..descriptor.value_count];
        vectors[i] = .{
            .indices = try alloc.dupe(u32, indices),
            .values = undefined,
        };
        vectors[i].values = alloc.dupe(f32, values) catch |err| {
            alloc.free(vectors[i].indices);
            return err;
        };
        initialized += 1;
    }
    return vectors;
}

fn embedDenseParts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
) anyerror![][]f32 {
    return embedDensePartsInner(handle, alloc, model, parts, null);
}

fn embedDensePartsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
    context: managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    const vectors = try embedDensePartsInner(handle, alloc, model, parts, context);
    context.check() catch |err| {
        freeDenseBatch(alloc, vectors);
        return err;
    };
    return vectors;
}

fn embedDensePartsInner(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
    context: ?managed_embedder.EmbeddingRequestContext,
) ![][]f32 {
    const wire = try alloc.alloc(bridge.ContentPart, parts.len);
    defer alloc.free(wire);
    for (parts, 0..) |part, i| {
        wire[i] = switch (part) {
            .text => |value| .{ .tag = .text, .value = .init(value), .mime_type = .init("") },
            .media_url => |value| .{ .tag = .media_url, .value = .init(value), .mime_type = .init("") },
            .binary => |value| .{ .tag = .binary, .value = .init(value.data), .mime_type = .init(value.mime_type) },
        };
    }
    const cancellation = if (context) |ctx| ctx.cancellation else null;
    var result: bridge.DenseEmbeddingResult = .{};
    defer bridge.antfly_standalone_inference_dense_result_destroy(&result);
    var failure: bridge.FailureIdentity = .{};
    const status = bridge.antfly_standalone_inference_embed_dense_parts(&.{
        .handle = handle,
        .model = .init(model),
        .parts = if (wire.len == 0) null else wire.ptr,
        .part_count = wire.len,
        .has_deadline = @intFromBool(if (context) |ctx| ctx.deadline_ns != null else false),
        .deadline_ns = if (context) |ctx| ctx.deadline_ns orelse 0 else 0,
        .cancellation_ctx = if (cancellation) |flag| flag else null,
        .cancellation_probe = if (cancellation != null) cancellationProbe else null,
    }, &result, &failure);
    try acceptFailure(status, &failure, .embed_dense_parts);
    try validateDenseResult(result);
    const descriptors = if (result.vector_count == 0) &.{} else result.vectors.?[0..result.vector_count];
    const vectors = try alloc.alloc([]f32, descriptors.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| alloc.free(vector);
        alloc.free(vectors);
    }
    for (descriptors, 0..) |descriptor, i| {
        vectors[i] = try alloc.dupe(f32, descriptor.slice());
        initialized += 1;
    }
    return vectors;
}

fn rerankTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
) anyerror![]f32 {
    const input = try alloc.alloc(bridge.String, documents.len);
    defer alloc.free(input);
    for (documents, 0..) |document, i| input[i] = .init(document);
    var result: bridge.FloatResult = .{};
    defer linked_rerank_api.destroy(&result);
    var failure: bridge.FailureIdentity = .{};
    const status = linked_rerank_api.execute(&.{
        .handle = handle,
        .model = .init(model),
        .query = .init(query),
        .documents = if (input.len == 0) null else input.ptr,
        .document_count = input.len,
    }, &result, &failure);
    try acceptFailure(status, &failure, .rerank_texts);
    try validateFloatResult(result);
    if (result.value_count != documents.len) return error.InvalidBoundaryQueryResponse;
    return try alloc.dupe(f32, if (result.value_count == 0) &.{} else result.values.?[0..result.value_count]);
}

fn listModelsJson(handle: *anyopaque, alloc: std.mem.Allocator) anyerror![]u8 {
    var result: bridge.BytesResult = .{};
    defer linked_list_models_api.destroy(&result);
    var failure: bridge.FailureIdentity = .{};
    const status = linked_list_models_api.execute(&.{ .handle = handle }, &result, &failure);
    try acceptFailure(status, &failure, .list_models_json);
    try validateBytesResult(result);
    return try alloc.dupe(u8, if (result.byte_count == 0) &.{} else result.bytes.?[0..result.byte_count]);
}

fn generateText(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
) anyerror![]u8 {
    if (roles.len != contents.len) return error.InvalidGenerationRequest;
    const role_wire = try alloc.alloc(bridge.String, roles.len);
    defer alloc.free(role_wire);
    const content_wire = try alloc.alloc(bridge.String, contents.len);
    defer alloc.free(content_wire);
    for (roles, 0..) |role, i| role_wire[i] = .init(role);
    for (contents, 0..) |content, i| content_wire[i] = .init(content);
    const request = bridge.GenerateTextRequest{
        .handle = handle,
        .model = .init(model),
        .roles = if (role_wire.len == 0) null else role_wire.ptr,
        .contents = if (content_wire.len == 0) null else content_wire.ptr,
        .message_count = role_wire.len,
    };
    return callBytesOperation(
        alloc,
        .generate_text,
        bridge.antfly_standalone_inference_generate_text,
        &request,
    );
}

fn generateMessages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference_types.ChatMessage,
) anyerror![]u8 {
    const payload = try std.json.Stringify.valueAlloc(alloc, messages, .{});
    defer alloc.free(payload);
    const request = bridge.JsonOperationRequest{
        .handle = handle,
        .model = .init(model),
        .payload_json = .init(payload),
    };
    return callBytesOperation(
        alloc,
        .generate_messages,
        bridge.antfly_standalone_inference_generate_messages,
        &request,
    );
}

fn callBytesOperation(
    alloc: std.mem.Allocator,
    operation: bridge.Operation,
    comptime execute: anytype,
    request: anytype,
) ![]u8 {
    var result: bridge.BytesResult = .{};
    defer bridge.antfly_standalone_inference_bytes_result_destroy(&result);
    var failure: bridge.FailureIdentity = .{};
    const status = execute(request, &result, &failure);
    try acceptFailure(status, &failure, operation);
    try validateBytesResult(result);
    return try alloc.dupe(u8, if (result.byte_count == 0) &.{} else result.bytes.?[0..result.byte_count]);
}

fn cancellationProbe(context: ?*const anyopaque) callconv(.c) u8 {
    const raw_context = context orelse return 0;
    const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw_context));
    return @intFromBool(flag.load(.acquire));
}

fn acceptFailure(
    status: bridge.Status,
    failure: *const bridge.FailureIdentity,
    expected_operation: bridge.Operation,
) !void {
    failure_identity.validateFailureEnvelope(status, failure, bridge.abi_version) catch |err| {
        logMalformedFailure(status, failure);
        return err;
    };
    if (status == .ok) return;
    if (failure.boundary != .inference_runtime or
        failure.operation != @intFromEnum(expected_operation))
    {
        logMalformedFailure(status, failure);
        return error.InvalidBoundaryFailureIdentity;
    }
    if (status == .internal) {
        std.log.err("inference provider failed operation={s} provider_error={s} hash={x}", .{
            @tagName(expected_operation),
            failure.errorName(),
            failure.error_name_hash,
        });
    }
    return failure_identity.statusToError(status);
}

fn validateDenseResult(result: bridge.DenseEmbeddingResult) !void {
    if (result.version != bridge.abi_version or result._reserved0 != 0 or
        result.owner == null or result.vector_count > 1_000_000 or
        (result.vector_count == 0 and result.vectors != null) or
        (result.vector_count != 0 and result.vectors == null))
    {
        return error.InvalidBoundaryQueryResponse;
    }
    const vectors = if (result.vector_count == 0) &.{} else result.vectors.?[0..result.vector_count];
    for (vectors) |vector| {
        if (vector.value_count > 16 * 1024 * 1024 or
            (vector.value_count == 0 and vector.values != null) or
            (vector.value_count != 0 and vector.values == null))
        {
            return error.InvalidBoundaryQueryResponse;
        }
    }
}

fn validateSparseResult(result: bridge.SparseEmbeddingResult) !void {
    if (result.version != bridge.abi_version or result._reserved0 != 0 or result.owner == null or
        result.vector_count > 1_000_000 or
        (result.vector_count == 0 and result.vectors != null) or
        (result.vector_count != 0 and result.vectors == null)) return error.InvalidBoundaryQueryResponse;
    const vectors = if (result.vector_count == 0) &.{} else result.vectors.?[0..result.vector_count];
    for (vectors) |vector| {
        if (vector.value_count > 16 * 1024 * 1024 or
            (vector.value_count == 0 and (vector.indices != null or vector.values != null)) or
            (vector.value_count != 0 and (vector.indices == null or vector.values == null)))
        {
            return error.InvalidBoundaryQueryResponse;
        }
    }
}

fn validateFloatResult(result: bridge.FloatResult) !void {
    if (result.version != bridge.abi_version or result._reserved0 != 0 or result.owner == null or
        result.value_count > 16 * 1024 * 1024 or
        (result.value_count == 0 and result.values != null) or
        (result.value_count != 0 and result.values == null)) return error.InvalidBoundaryQueryResponse;
}

fn validateBytesResult(result: bridge.BytesResult) !void {
    if (result.version != bridge.abi_version or result._reserved0 != 0 or result.owner == null or
        result.byte_count > 256 * 1024 * 1024 or
        (result.byte_count == 0 and result.bytes != null) or
        (result.byte_count != 0 and result.bytes == null)) return error.InvalidBoundaryQueryResponse;
}

fn logMalformedFailure(status: bridge.Status, failure: *const bridge.FailureIdentity) void {
    std.log.err(
        "inference provider returned inconsistent failure status={s} identity_status={s} boundary={d} version={d} operation={d} error={s} hash={x}",
        .{
            @tagName(status),
            @tagName(failure.status),
            @intFromEnum(failure.boundary),
            failure.boundary_version,
            failure.operation,
            failure.boundedErrorName(),
            failure.error_name_hash,
        },
    );
}

fn freeDenseBatch(alloc: std.mem.Allocator, vectors: [][]f32) void {
    for (vectors) |vector| alloc.free(vector);
    alloc.free(vectors);
}

test "dense result validation rejects noncanonical ownership and pointers" {
    try std.testing.expectError(
        error.InvalidBoundaryQueryResponse,
        validateDenseResult(.{}),
    );
    var owner: u8 = 0;
    try validateDenseResult(.{ .owner = &owner });
    try std.testing.expectError(
        error.InvalidBoundaryQueryResponse,
        validateDenseResult(.{ .owner = &owner, .vector_count = 1 }),
    );
}

test "simple inference result families reject ambiguous shapes" {
    var owner: u8 = 0;
    try validateSparseResult(.{ .owner = &owner });
    try validateFloatResult(.{ .owner = &owner });
    try validateBytesResult(.{ .owner = &owner });
    try std.testing.expectError(
        error.InvalidBoundaryQueryResponse,
        validateSparseResult(.{ .owner = &owner, .vector_count = 1 }),
    );
    try std.testing.expectError(
        error.InvalidBoundaryQueryResponse,
        validateFloatResult(.{ .owner = &owner, .value_count = 1 }),
    );
    try std.testing.expectError(
        error.InvalidBoundaryQueryResponse,
        validateBytesResult(.{ .owner = &owner, .byte_count = 1 }),
    );
}

const MockDenseProvider = struct {
    const first = [_]f32{ 1.0, 2.0 };
    const second = [_]f32{ 3.0, 4.0 };
    const descriptors = [_]bridge.DenseVector{
        .{ .values = &first, .value_count = first.len },
        .{ .values = &second, .value_count = second.len },
    };
    var owner: u8 = 0;
    var destroy_count: usize = 0;

    fn execute(
        request: *const bridge.DenseEmbeddingRequest,
        out_result: *bridge.DenseEmbeddingResult,
        out_failure: *bridge.FailureIdentity,
    ) callconv(.c) bridge.Status {
        out_result.* = .{};
        out_failure.* = .{};
        if (!std.mem.eql(u8, request.model.slice(), "model") or request.text_count != 2) {
            out_failure.* = failure_identity.failureFromError(
                error.InvalidArgument,
                .inference_runtime,
                bridge.abi_version,
                @intFromEnum(bridge.Operation.embed_dense_texts),
            );
            return out_failure.status;
        }
        out_result.* = .{
            .owner = &owner,
            .vectors = &descriptors,
            .vector_count = descriptors.len,
        };
        return .ok;
    }

    fn destroy(result: *bridge.DenseEmbeddingResult) callconv(.c) void {
        if (result.owner != null) destroy_count += 1;
        result.* = .{};
    }
};

test "dense adapter copies provider-owned vectors and destroys provider result" {
    MockDenseProvider.destroy_count = 0;
    var handle: u8 = 0;
    const vectors = try embedDenseTextsInnerWithApi(
        &handle,
        std.testing.allocator,
        "model",
        &.{ "alpha", "beta" },
        null,
        .{ .execute = MockDenseProvider.execute, .destroy = MockDenseProvider.destroy },
    );
    defer freeDenseBatch(std.testing.allocator, vectors);
    try std.testing.expectEqual(@as(usize, 1), MockDenseProvider.destroy_count);
    try std.testing.expectEqualSlices(f32, &MockDenseProvider.first, vectors[0]);
    try std.testing.expectEqualSlices(f32, &MockDenseProvider.second, vectors[1]);
}

test "dense adapter preserves registered failure identity and rejects wrong origin" {
    inline for (.{
        .{ .err = error.UnsupportedEmbeddingProvider, .status = bridge.Status.unsupported_embedding_provider },
        .{ .err = error.ModelNotFound, .status = bridge.Status.model_not_found },
        .{ .err = error.ModelNotSpecified, .status = bridge.Status.model_not_specified },
        .{ .err = error.ModelArtifactsChanging, .status = bridge.Status.model_artifacts_changing },
        .{ .err = error.Timeout, .status = bridge.Status.timeout },
        .{ .err = error.Cancelled, .status = bridge.Status.cancelled },
        .{ .err = error.OutOfMemory, .status = bridge.Status.out_of_memory },
    }) |case| {
        const declared = failure_identity.failureFromError(
            case.err,
            .inference_runtime,
            bridge.abi_version,
            @intFromEnum(bridge.Operation.embed_dense_texts),
        );
        try std.testing.expectEqual(case.status, declared.status);
        try std.testing.expectEqualStrings(@errorName(case.err), declared.errorName());
        try std.testing.expectError(
            case.err,
            acceptFailure(case.status, &declared, .embed_dense_texts),
        );
    }

    var declared = failure_identity.failureFromError(
        error.ModelNotFound,
        .inference_runtime,
        bridge.abi_version,
        @intFromEnum(bridge.Operation.embed_dense_texts),
    );
    declared.boundary = .storage_owner;
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        acceptFailure(.model_not_found, &declared, .embed_dense_texts),
    );
}

test "simple inference operations retain their distinct failure origins" {
    inline for (.{
        bridge.Operation.embed_sparse_texts,
        bridge.Operation.rerank_texts,
        bridge.Operation.list_models_json,
        bridge.Operation.generate_text,
        bridge.Operation.generate_messages,
        bridge.Operation.embed_dense_parts,
    }) |operation| {
        const failure = failure_identity.failureFromError(
            error.ModelNotFound,
            .inference_runtime,
            bridge.abi_version,
            @intFromEnum(operation),
        );
        try std.testing.expectError(
            error.ModelNotFound,
            acceptFailure(.model_not_found, &failure, operation),
        );
    }
}

test "generation ABI preserves domain errors and rich message JSON" {
    const failure = failure_identity.failureFromError(
        error.InvalidGenerationRequest,
        .inference_runtime,
        bridge.abi_version,
        @intFromEnum(bridge.Operation.generate_text),
    );
    try std.testing.expectError(
        error.InvalidGenerationRequest,
        acceptFailure(.invalid_generation_request, &failure, .generate_text),
    );

    const messages = [_]inference_types.ChatMessage{.{
        .role = .user,
        .content = .{ .text = "hello" },
    }};
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, &messages, .{});
    defer std.testing.allocator.free(encoded);
    const parsed = try std.json.parseFromSlice(
        []inference_types.ChatMessage,
        std.testing.allocator,
        encoded,
        .{},
    );
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.len);
    try std.testing.expectEqualStrings("hello", parsed.value[0].content.?.text);
}
