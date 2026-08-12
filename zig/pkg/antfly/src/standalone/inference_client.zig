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

pub fn installDenseAdapter(
    provider: *managed_embedder.AntflyProvider,
    inference_handle: *anyopaque,
) void {
    provider.ptr = inference_handle;
    provider.embed_dense_texts = embedDenseTexts;
    provider.embed_dense_texts_with_context = embedDenseTextsWithContext;
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
