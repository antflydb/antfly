// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Storage-local adapter for the process inference archive. The storage
//! context borrows only its opaque lifecycle handle; complete operations cross
//! the existing JSON provider ABI and are decoded into storage-local types.

const std = @import("std");
const platform_time = @import("antfly_platform").time;
const bridge = @import("../standalone/inference_bridge.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const inference_types = @import("../inference/types.zig");
const template = @import("../template.zig");
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const extracting = @import("antfly_extracting");

pub fn provider(handle: *anyopaque) managed_embedder.AntflyProvider {
    return .{
        .ptr = handle,
        .embed_dense_texts = embedDenseTexts,
        .embed_dense_texts_with_context = embedDenseTextsWithContext,
        .embed_sparse_texts = embedSparseTexts,
        .embed_dense_parts = embedDenseParts,
        .embed_dense_parts_with_context = embedDensePartsWithContext,
        .rerank_texts = rerankTexts,
        .generate_text = generateText,
        .generate_messages = generateMessages,
        .read_images = readImages,
        .transcribe_audio = transcribeAudio,
        .extract = extract,
        .list_models_json = listModelsJson,
    };
}

fn invoke(
    comptime Result: type,
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    operation: bridge.ProviderOperation,
    request: anytype,
    deadline_ns: ?u64,
) !Result {
    const request_json = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(request_json);
    var response_handle: ?*anyopaque = null;
    var response_json: bridge.String = undefined;
    const effective_deadline_ns = deadline_ns orelse platform_time.monotonicNs() +| 5 * std.time.ns_per_min;
    const context = bridge.ProviderInvokeContext{
        .abi_version = bridge.abi_version,
        .handle = handle,
        .operation = @intFromEnum(operation),
        .request_json = bridge.String.init(request_json),
        .deadline_ns = effective_deadline_ns,
        .has_deadline = 1,
        .out_response_handle = &response_handle,
        .out_response_json = &response_json,
    };
    const api = try linkedApi(bridge.Capability.provider);
    const status = api.invoke_provider(&context);
    if (!status.isOk()) return bridge.errorFromStatus(status);
    const owned_response = response_handle orelse return error.InferenceRuntimeResponseMissing;
    defer api.destroy_provider_response(owned_response);
    return try std.json.parseFromSliceLeaky(Result, alloc, response_json.slice(), .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn linkedApi(required_capabilities: u64) !*const bridge.FunctionTable {
    const table = bridge.antfly_standalone_inference_get_function_table();
    if (!bridge.validFunctionTable(table, required_capabilities))
        return error.UnsupportedVersion;
    return table;
}

fn embedDenseTexts(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8) anyerror![][]f32 {
    return try invoke([][]f32, alloc, handle, .embed_dense_texts, .{ .model = model, .texts = texts }, null);
}

fn embedDenseTextsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    return try invoke([][]f32, alloc, handle, .embed_dense_texts_with_context, .{ .model = model, .texts = texts }, context.deadline_ns);
}

fn embedSparseTexts(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8) anyerror![]managed_embedder.SparseEmbedding {
    return try invoke([]managed_embedder.SparseEmbedding, alloc, handle, .embed_sparse_texts, .{ .model = model, .texts = texts }, null);
}

fn embedDenseParts(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, parts: []const template.ContentPart) anyerror![][]f32 {
    return try invoke([][]f32, alloc, handle, .embed_dense_parts, .{ .model = model, .parts = parts }, null);
}

fn embedDensePartsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
    context: managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    return try invoke([][]f32, alloc, handle, .embed_dense_parts_with_context, .{ .model = model, .parts = parts }, context.deadline_ns);
}

fn rerankTexts(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) anyerror![]f32 {
    return try invoke([]f32, alloc, handle, .rerank_texts, .{ .model = model, .query = query, .documents = documents }, null);
}

fn generateText(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
) anyerror![]u8 {
    return try invoke([]u8, alloc, handle, .generate_text, .{ .model = model, .roles = roles, .contents = contents }, null);
}

fn generateMessages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference_types.ChatMessage,
) anyerror![]u8 {
    return try invoke([]u8, alloc, handle, .generate_messages, .{ .model = model, .messages = messages }, null);
}

fn readImages(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, request: readers.Request) anyerror![]readers.Result {
    return try invoke([]readers.Result, alloc, handle, .read_images, .{ .model = model, .request = request }, null);
}

fn transcribeAudio(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, request: transcribing.Request) anyerror!transcribing.Response {
    return try invoke(transcribing.Response, alloc, handle, .transcribe_audio, .{ .model = model, .request = request }, null);
}

fn extract(handle: *anyopaque, alloc: std.mem.Allocator, model: []const u8, request: extracting.Request) anyerror!extracting.Response {
    const json = try invoke([]u8, alloc, handle, .extract, .{ .model = model, .request = request }, null);
    return .{ .allocator = alloc, .json = json };
}

fn listModelsJson(handle: *anyopaque, alloc: std.mem.Allocator) anyerror![]u8 {
    return try invoke([]u8, alloc, handle, .list_models_json, .{}, null);
}
