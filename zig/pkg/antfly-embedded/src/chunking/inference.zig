// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const inference_api = @import("inference_api");
const chunking_types = @import("types.zig");
const chunk_mod = @import("chunk.zig");
const Chunk = chunk_mod.Chunk;
const http_common = @import("../common/http/http_common.zig");
const std_http_listener = @import("../common/http/std_http_listener.zig");
const inference_chunker = @import("inference_chunker");
const chunk_provider = @import("provider.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const remote_capabilities = @import("antfly_inference_remote_capabilities");
const inference_work = @import("antfly_inference_work");
const execution_context = @import("antfly_inference_execution_context");
const platform_time = @import("antfly_platform").time;

const Allocator = std.mem.Allocator;
const remote_chunk_max_response_bytes: usize = 16 << 20;
const remote_chunk_max_timeout_ms: u64 = 300_000;

const ChunkInputFn = *const fn (
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
) anyerror![]inference_chunker.Chunk;
const ChunkInputWithContextFn = *const fn (
    ptr: *anyopaque,
    alloc: Allocator,
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
    context: execution_context.RequestContext,
) anyerror![]inference_chunker.Chunk;
pub const ChunkPage = inference_chunker.types.ChunkPage;
pub const RemoteMetadata = inference_chunker.types.ModelMetadata;
const ChunkPageWithContextFn = *const fn (*anyopaque, Allocator, []const u8, inference_chunker.Input, chunking_types.Config, execution_context.RequestContext) anyerror!ChunkPage;
const ChunkProviderVTable = struct {
    chunk_page_with_context: ?ChunkPageWithContextFn = null,
    chunk_input: ?ChunkInputFn = null,
    chunk_input_with_context: ?ChunkInputWithContextFn = null,
};
const ChunkProviderBoundary = runtime_callback_abi.Boundary(ChunkProviderVTable);

pub const RemoteChunk = inference_chunker.Chunk;
pub const RemoteInput = inference_chunker.Input;
pub const RemoteBinaryInput = inference_chunker.BinaryInput;

pub fn chunkText(alloc: Allocator, cfg: chunking_types.Config, text: []const u8) ![]Chunk {
    return try chunkTextWithProvider(alloc, cfg, text, null);
}

pub fn chunkTextWithProvider(
    alloc: Allocator,
    cfg: chunking_types.Config,
    text: []const u8,
    antfly_provider: ?chunk_provider.Provider,
) ![]Chunk {
    const shared_chunks = try chunkInputWithProvider(alloc, cfg, .{ .text = text }, antfly_provider);
    defer freeRemoteChunks(alloc, shared_chunks);

    var chunks = try alloc.alloc(Chunk, shared_chunks.len);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    for (shared_chunks, 0..) |shared, i| {
        if (!std.mem.eql(u8, shared.mime_type, "text/plain")) return error.UnsupportedChunkMediaType;
        const shared_text = shared.text orelse return error.InvalidChunkerResponse;
        const offsets = chunk_mod.completeTextOffsetPair(shared.start_char, shared.end_char, text.len);
        chunks[i] = .{
            .chunk_id = shared.id,
            .text = try alloc.dupe(u8, shared_text),
            .start_offset = offsets.start,
            .end_offset = offsets.end,
        };
        initialized += 1;
        if (shared.metadata) |metadata| chunks[i].metadata = try metadata.clone(alloc);
    }
    return chunks;
}

/// Database adapter preserves media descriptors and fused vectors together.
pub fn chunkMediaWithProvider(a: Allocator, cfg: chunking_types.Config, input: RemoteInput, provider: ?chunk_provider.Provider) ![]Chunk {
    const source = try chunkInputWithProvider(a, cfg, input, provider);
    defer freeRemoteChunks(a, source);
    return cloneMediaChunks(a, source);
}

pub fn cloneMediaChunks(a: Allocator, source: []const RemoteChunk) ![]Chunk {
    const chunks = try a.alloc(Chunk, source.len);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(a);
        a.free(chunks);
    }
    for (source, chunks) |remote, *chunk| {
        chunk.* = .{ .chunk_id = remote.id, .start_offset = remote.start_char, .end_offset = remote.end_char, .start_time_ms = remote.start_time_ms, .end_time_ms = remote.end_time_ms, .frame_index = remote.frame_index, .frame_delay_ms = remote.frame_delay_ms };
        initialized += 1;
        chunk.mime_type = try a.dupe(u8, remote.mime_type);
        chunk.owns_mime_type = true;
        if (remote.text) |text| chunk.text = try a.dupe(u8, text);
        if (remote.data) |data| chunk.data = try a.dupe(u8, data);
        if (remote.metadata) |metadata| chunk.metadata = try metadata.clone(a);
    }
    return chunks;
}

pub fn chunkBinary(alloc: Allocator, cfg: chunking_types.Config, mime_type: []const u8, data: []const u8) ![]RemoteChunk {
    return try chunkInput(alloc, cfg, .{
        .binary = .{
            .mime_type = mime_type,
            .data = data,
        },
    });
}

pub fn chunkInput(alloc: Allocator, cfg: chunking_types.Config, input: RemoteInput) ![]RemoteChunk {
    return try chunkInputWithProvider(alloc, cfg, input, null);
}

pub fn chunkInputWithProvider(
    alloc: Allocator,
    cfg: chunking_types.Config,
    input: RemoteInput,
    antfly_provider: ?chunk_provider.Provider,
) ![]RemoteChunk {
    return chunkInputDispatch(false, alloc, cfg, input, antfly_provider);
}

pub fn supportsPagedCalls(cfg: chunking_types.Config, provider: ?chunk_provider.Provider) bool {
    const resolved = provider orelse return true;
    if (resolved.chunk_page_with_context_callback != null) return true;
    const has_legacy = resolved.chunk_input_callback != null or resolved.chunk_input_with_context_callback != null;
    const trimmed = std.mem.trim(u8, cfg.api_url, " \t\r\n");
    return resolved.execution.resolveAntflyEndpoint(if (trimmed.len != 0) trimmed else null, has_legacy) != null or !has_legacy;
}

/// Discover current output generations without executing any model output.
pub fn probeModelPageWithProvider(a: Allocator, supplied: chunking_types.Config, input: RemoteInput, provider: ?chunk_provider.Provider) !ChunkPage {
    const outputs = inference_chunker.outputs;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var cfg = supplied;
    var consumers = if (cfg.outputs_json.len != 0) try std.json.parseFromSliceLeaky(outputs.Config, scratch, cfg.outputs_json, .{}) else outputs.Config{ .boundary_model = "embedding", .targets = &.{} };
    if (cfg.embedding) |legacy| consumers = try consumers.includingEmbedding(scratch, .{ .name = "embedding", .kind = .embedding, .model = legacy.model, .model_identity = legacy.model_identity, .task_type = legacy.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (legacy.dimensions) |n| std.math.cast(usize, n) orelse return error.InvalidChunkOutputs else null });
    const targets = try scratch.dupe(outputs.Target, consumers.targets);
    for (targets) |*target| target.execute = false;
    consumers.targets = targets;
    cfg.outputs_json = try std.json.Stringify.valueAlloc(scratch, consumers, .{ .emit_null_optional_fields = false });
    cfg.embedding = null;
    cfg.page = true;
    cfg.max_chunks = 1;
    cfg.replay = null;
    return chunkPageWithProvider(a, cfg, input, provider);
}

pub fn chunkPageWithProvider(alloc: Allocator, cfg: chunking_types.Config, input: RemoteInput, provider: ?chunk_provider.Provider) !ChunkPage {
    if (!cfg.page or cfg.max_chunks == 0 or !cfg.isModelAware()) return error.InvalidChunkerConfig;
    const empty = switch (input) {
        .text => |value| value.len == 0,
        .binary => |value| value.data.len == 0,
        .source => false,
    };
    if (empty and cfg.replay == null) {
        const chunks = try alloc.alloc(RemoteChunk, 0);
        errdefer alloc.free(chunks);
        const plan = try alloc.dupe(u8, "");
        errdefer alloc.free(plan);
        const source = try alloc.dupe(u8, "");
        errdefer alloc.free(source);
        return .{ .chunks = chunks, .plan_id = plan, .source_fingerprint = source, .source_version = try alloc.dupe(u8, ""), .start_ordinal = 0, .covered_through_ordinal = 0, .complete = true };
    }
    var page = try chunkInputDispatch(true, alloc, cfg, input, provider);
    errdefer page.deinit(alloc);
    if (input == .source and !std.mem.eql(u8, input.source.version, page.source_version)) return error.EnrichmentSourceChanged;
    return page;
}

fn chunkInputDispatch(comptime paged: bool, alloc: Allocator, cfg: chunking_types.Config, input: RemoteInput, antfly_provider: ?chunk_provider.Provider) !(if (paged) ChunkPage else []RemoteChunk) {
    // An empty source has nothing to chunk. Short-circuit before any
    // provider or remote dispatch: the inference endpoint rejects an empty
    // input as a 400 that the retry classifier treats as transient, which
    // turns a permanently empty document field into a retry loop instead of
    // a clean zero-chunk skip.
    if (!paged) switch (input) {
        .source => if (!cfg.isModelAware()) return error.ModelAwareChunkProviderRequired,
        .text => |text| if (text.len == 0) return try alloc.alloc(RemoteChunk, 0),
        .binary => |binary| if (binary.data.len == 0) return try alloc.alloc(RemoteChunk, 0),
    };
    const execution: execution_context.Context = if (antfly_provider) |provider| provider.execution else .{};
    try execution.check(platform_time.monotonicNs());
    const linked_callback_available = if (antfly_provider) |provider|
        if (paged) provider.chunk_page_with_context_callback != null else provider.chunk_input_with_context_callback != null or provider.chunk_input_callback != null
    else
        false;
    const trimmed_endpoint = std.mem.trim(u8, cfg.api_url, " \t\r\n");
    const explicit_endpoint: ?[]const u8 = if (trimmed_endpoint.len > 0)
        trimmed_endpoint
    else
        null;
    const resolved_endpoint = execution.resolveAntflyEndpoint(explicit_endpoint, linked_callback_available);
    // Execution-only providers carry I/O, routing and deadlines without a
    // linked chunk callback. They still use the local direct chunker when no
    // remote endpoint resolves (for example while rebuilding a restore).
    if (resolved_endpoint == null and linked_callback_available) if (antfly_provider) |provider| {
        const ptr = provider.ptr orelse return error.InvalidChunkProvider;
        const dispatch = provider.boundary_dispatch orelse return error.InvalidChunkProvider;
        const io = execution.io orelse if (builtin.os.tag == .freestanding)
            return error.BackendRuntimeIoUnavailable
        else
            std.Io.Threaded.global_single_threaded.io();
        const request_context = execution.requestContext(io);
        try request_context.check();
        if (paged) {
            const callback: ChunkPageWithContextFn = @ptrCast(@alignCast(provider.chunk_page_with_context_callback orelse return error.InvalidChunkProvider));
            var page = try ChunkProviderBoundary.call("chunk_page_with_context", dispatch, callback, .{ ptr, alloc, if (cfg.model.len > 0) cfg.model else "fixed", input, cfg, request_context });
            errdefer page.deinit(alloc);
            try validateChunkPage(cfg, page);
            try request_context.check();
            return page;
        }
        const chunks = if (provider.chunk_input_with_context_callback) |callback| blk: {
            const chunk_input: ChunkInputWithContextFn = @ptrCast(@alignCast(callback));
            break :blk try ChunkProviderBoundary.call("chunk_input_with_context", dispatch, chunk_input, .{ ptr, alloc, if (cfg.model.len > 0) cfg.model else "fixed", input, cfg, request_context });
        } else if (provider.chunk_input_callback) |callback| blk: {
            const chunk_input: ChunkInputFn = @ptrCast(@alignCast(callback));
            break :blk try ChunkProviderBoundary.call("chunk_input", dispatch, chunk_input, .{ ptr, alloc, if (cfg.model.len > 0) cfg.model else "fixed", input, cfg });
        } else return error.InvalidChunkProvider;
        defer inference_chunker.types.freeChunks(alloc, chunks);
        try validateModelChunks(cfg, chunks);
        try request_context.check();
        return try cloneRemoteChunks(alloc, chunks);
    };
    const endpoint_raw = resolved_endpoint orelse {
        if (paged) return error.ModelAwareChunkProviderRequired;
        return try chunkInputDirect(alloc, cfg, input);
    };
    // Browser storage uses direct chunking or a host callback. Native HTTP
    // discovery/sockets are unavailable in a freestanding module.
    if (comptime builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
    if (cfg.model.len == 0 and !cfg.isModelAware()) return error.InvalidChunkerConfig;
    // Unlike `managed_embedder.zig`'s embedder/extractor base URLs, a
    // chunker's `api_url` reached this call unnormalized: a bare
    // `host:port` (the natural way to configure "the same inference
    // service" for embedder and chunker together) resolves to
    // `{host:port}/chunk` instead of `{host:port}/ai/v1/chunk`, so the
    // request 404s against the real joined API. Normalize the same way the
    // embedder path does, leaving an already-pathed URL untouched.
    const endpoint = try normalizedAntflyChunkEndpointAlloc(alloc, endpoint_raw);
    defer alloc.free(endpoint);

    var fallback_io: ?std.Io.Threaded = null;
    defer if (fallback_io) |*io_impl| io_impl.deinit();
    const io = if (execution.io) |io|
        io
    else blk: {
        fallback_io = std.Io.Threaded.init(alloc, .{});
        break :blk fallback_io.?.io();
    };

    var fallback_http: ?httpx.Client = null;
    defer if (fallback_http) |*client| client.deinit();
    const http = execution.http_client orelse blk: {
        fallback_http = httpx.Client.initWithConfig(alloc, io, .{
            .keep_alive = false,
            .cookies_enabled = false,
        });
        break :blk &fallback_http.?;
    };

    var fallback_capability_cache: ?remote_capabilities.Cache = null;
    defer if (fallback_capability_cache) |*cache| cache.deinit();
    var capability_cache = execution.capability_cache;
    if (capability_cache == null) {
        fallback_capability_cache = remote_capabilities.Cache.init(alloc, http.io);
        capability_cache = &fallback_capability_cache.?;
    }
    var resolved_outputs: ?std.json.Parsed(inference_chunker.outputs.Config) = if (cfg.outputs_json.len > 0) try std.json.parseFromSlice(inference_chunker.outputs.Config, alloc, cfg.outputs_json, .{}) else null;
    defer if (resolved_outputs) |*value| value.deinit();
    if (cfg.embedding) |legacy| if (resolved_outputs) |*value| {
        value.value = try value.value.includingEmbedding(value.arena.allocator(), .{ .name = "embedding", .kind = .embedding, .model = legacy.model, .model_identity = legacy.model_identity, .task_type = legacy.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (legacy.dimensions) |n| std.math.cast(usize, n) orelse return error.InvalidChunkOutputs else null });
    };
    const boundary = if (resolved_outputs) |value| try value.value.boundary() else null;
    const model = if (boundary) |value| value.model else if (cfg.embedding) |target| target.model else if (cfg.model.len > 0) cfg.model else "fixed";
    const capability_task: inference_work.Operation = if (boundary) |value| if (value.kind == .transcribe) .transcribe else .embed else if (cfg.embedding != null) .embed else .chunk;
    var routing_header_storage: [1][2][]const u8 = undefined;
    const routing_header_count = try execution.routing.appendHeaders(&routing_header_storage, 0);
    const routing_headers = routing_header_storage[0..routing_header_count];
    var capability_lease = try capability_cache.?.getOrDiscoverLeaseWithContext(
        http,
        endpoint,
        model,
        capability_task,
        routing_headers,
        execution.waitContext(),
    );
    var attachment_transport: inference_work.AttachmentTransport = .base64_payload;
    if (capability_lease.capabilities) |capabilities| {
        if (cfg.isModelAware() and !capabilities.model_aware_chunking_v1) return error.ModelAwareChunkProviderRequired;
        if (capabilities.framed_attachments and switch (input) {
            .binary => true,
            .text, .source => false,
        }) attachment_transport = .segmented_framed_binary;
        const shape: inference_work.InvocationShape = switch (input) {
            .source => .{ .item_count = 1 },
            .text => |text| .{
                .item_count = 1,
                .modalities = .{ .text = true },
                .text_bytes = text.len,
                .max_text_bytes_per_item = text.len,
            },
            .binary => |binary| .{
                .item_count = 1,
                .modalities = if (cfg.isModelAware() and std.ascii.startsWithIgnoreCase(binary.mime_type, "video/")) .{ .image = true } else inference_work.modalityForMimeType(binary.mime_type) orelse
                    return error.UnsupportedChunkMediaType,
                .encoded_media_bytes = try attachment_transport.wireSize(
                    binary.data.len,
                    binary.mime_type.len,
                ),
                .max_media_parts_per_item = 1,
            },
        };
        if (!cfg.isModelAware()) try capabilities.validateInvocation(.chunk, shape);
    }

    const url = try std.fmt.allocPrint(alloc, "{s}/chunk", .{endpoint});
    defer alloc.free(url);

    var body = try encodeChunkRequest(alloc, cfg, input, attachment_transport);
    defer body.deinit(alloc);

    var header_storage: [4][2][]const u8 = undefined;
    var header_count: usize = 0;
    for (routing_headers) |header| {
        header_storage[header_count] = header;
        header_count += 1;
    }
    if (capability_lease.routing_token) |*token| {
        header_storage[header_count] = .{ remote_capabilities.capability_token_header, token.slice() };
        header_count += 1;
    }
    if (capability_lease.descriptor_revision) |*revision| {
        header_storage[header_count] = .{ remote_capabilities.capability_revision_header, revision.slice() };
        header_count += 1;
    }
    if (attachment_transport == .segmented_framed_binary) {
        header_storage[header_count] = .{ "Content-Type", httpx.attachment_envelope.content_type };
        header_count += 1;
    }
    var resp = try http.post(url, .{
        .json = if (attachment_transport == .segmented_framed_binary) null else body.metadata_or_json,
        .borrowed_body_segments = if (body.envelope) |envelope| envelope.segments else null,
        .headers = header_storage[0..header_count],
        .timeout_ms = try execution.remainingTimeoutMs(platform_time.monotonicNs(), remote_chunk_max_timeout_ms),
        .max_response_size = execution.boundedResponseBytes(remote_chunk_max_response_bytes),
        .cancellation = httpx.CancellationToken.fromCallback(
            execution.cancellation.ptr,
            execution.cancellation.is_cancelled_fn,
        ),
    });
    defer resp.deinit();
    if (!resp.ok()) {
        // Never log the request payload (it embeds the full source document)
        // or the unbounded response body; a routine 429/503 must not copy
        // document contents into production logs. A bounded response prefix
        // is enough to identify the provider error.
        const error_body = resp.body orelse "<none>";
        const bounded_len = @min(error_body.len, 256);
        std.log.warn("chunk request failed url={s} status={d} response_prefix={s}{s}", .{
            url,
            resp.status.code,
            error_body[0..bounded_len],
            if (error_body.len > bounded_len) "..." else "",
        });
        const stale = resp.headers.get(remote_capabilities.capability_stale_header);
        if (resp.status.code == 409 and stale != null and
            std.ascii.eqlIgnoreCase(std.mem.trim(u8, stale.?, " \t"), "true"))
        {
            try capability_cache.?.invalidate(endpoint, model, capability_task, routing_headers);
            return error.InferenceCapabilitiesStale;
        }
        if (paged and resp.status.code == 409) {
            if (std.json.parseFromSlice(struct { @"error": []const u8 }, alloc, resp.body orelse "", .{ .ignore_unknown_fields = true })) |failure| {
                defer failure.deinit();
                return remoteChunkFailure(failure.value.@"error");
            } else |_| {}
        }
        return error.ChunkRequestFailed;
    }
    const response_body = resp.body orelse return error.EmptyResponse;
    if (paged) {
        var page = try parseModelChunkStream(alloc, response_body, cfg);
        errdefer page.deinit(alloc);
        try execution.check(platform_time.monotonicNs());
        return page;
    }
    const chunks = if (cfg.replay) |replay| try parseModelChunkResponse(alloc, response_body, replay) else try parseChunkResponse(alloc, response_body);
    errdefer inference_chunker.types.freeChunks(alloc, chunks);
    try validateModelChunks(cfg, chunks);
    try execution.check(platform_time.monotonicNs());
    return chunks;
}

fn validateModelChunks(cfg: chunking_types.Config, chunks: []const RemoteChunk) !void {
    if (cfg.outputs_json.len != 0) {
        const a = std.heap.smp_allocator;
        var config = try std.json.parseFromSlice(inference_chunker.outputs.Config, a, cfg.outputs_json, .{});
        defer config.deinit();
        if (cfg.embedding) |legacy| config.value = try config.value.includingEmbedding(config.arena.allocator(), .{ .name = "embedding", .kind = .embedding, .model = legacy.model, .model_identity = legacy.model_identity, .task_type = legacy.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (legacy.dimensions) |n| std.math.cast(usize, n) orelse return error.InvalidChunkOutputs else null });
        const boundary = try config.value.boundary();
        for (chunks) |chunk| {
            const metadata = chunk.metadata orelse return error.ModelAwareChunkProviderRequired;
            if (!std.mem.eql(u8, metadata.model, boundary.model)) return error.InvalidChunkOutput;
            const results_json = metadata.outputs_json orelse return error.InvalidChunkOutput;
            var results = try std.json.parseFromSlice([]inference_chunker.outputs.Result, a, results_json, .{});
            defer results.deinit();
            for (config.value.targets) |target| {
                var seen: usize = 0;
                for (results.value) |result| if (std.mem.eql(u8, result.name, target.name)) {
                    try result.validate(target);
                    if (result.sub_index != seen) return error.InvalidChunkOutput;
                    if (result.status == .failed and !cfg.page) return error.IncompleteChunkCoverage;
                    seen += 1;
                };
                if (seen == 0) return error.InvalidChunkOutput;
            }
            for (results.value) |result| {
                var known = false;
                for (config.value.targets) |target| if (std.mem.eql(u8, result.name, target.name)) {
                    known = true;
                    break;
                };
                if (!known) return error.InvalidChunkOutput;
            }
        }
        return;
    }
    const target = cfg.embedding orelse return;
    for (chunks) |chunk| {
        const metadata = chunk.metadata orelse return error.ModelAwareChunkProviderRequired;
        if (!std.mem.eql(u8, target.model, metadata.model) or !std.mem.eql(u8, target.task_type orelse "RETRIEVAL_DOCUMENT", metadata.task_type)) return error.InvalidChunkerResponse;
        if (target.model_identity) |identity| if (!std.mem.eql(u8, identity, metadata.model_identity)) return error.EmbeddingIdentityMismatch;
        if (target.dimensions) |dimensions| if (metadata.dimensions != dimensions) return error.InvalidChunkerResponse;
        if (metadata.embedding_error != null and cfg.page) continue;
        if (target.execute orelse true) {
            const vector = metadata.embedding orelse return error.InvalidEmbeddingResponse;
            if (vector.len != metadata.dimensions or vector.len == 0) return error.InvalidEmbeddingResponse;
            for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
        }
    }
}

/// Mirrors `managed_embedder.zig`'s `normalizeAntflyInferenceBaseUrl`: a bare
/// `scheme://host:port` endpoint (no path) gets `/ai/v1` appended so it lands
/// on the joined public API instead of the process root; an endpoint that
/// already carries a path (including one already ending in `/ai/v1`) is left
/// exactly as configured.
fn normalizedAntflyChunkEndpointAlloc(alloc: Allocator, raw: []const u8) ![]u8 {
    const trimmed = std.mem.trimEnd(u8, raw, "/");
    if (std.mem.endsWith(u8, trimmed, "/ai/v1")) return try alloc.dupe(u8, trimmed);
    const scheme_pos = std.mem.indexOf(u8, trimmed, "://");
    const host_start = if (scheme_pos) |pos| pos + 3 else 0;
    const path_pos = std.mem.indexOfPos(u8, trimmed, host_start, "/");
    if (path_pos == null) return try std.fmt.allocPrint(alloc, "{s}/ai/v1", .{trimmed});
    return try alloc.dupe(u8, trimmed);
}

fn chunkInputDirect(alloc: Allocator, cfg: chunking_types.Config, input: RemoteInput) ![]RemoteChunk {
    if (cfg.isModelAware()) return error.ModelAwareChunkProviderRequired;
    var fixed_cfg = inference_chunker.FixedChunkConfig{};
    if (cfg.model.len > 0) fixed_cfg.model = cfg.model;
    if (cfg.max_chunks > 0) fixed_cfg.max_chunks = @intCast(cfg.max_chunks);
    fixed_cfg.threshold = cfg.threshold;
    if (cfg.text.target_tokens > 0) fixed_cfg.text.target_tokens = @intCast(cfg.text.target_tokens);
    if (cfg.text.target_tokens > 0 or cfg.text.overlap_tokens > 0) fixed_cfg.text.overlap_tokens = @intCast(cfg.text.overlap_tokens);
    if (cfg.text.separator.len > 0) fixed_cfg.text.separator = cfg.text.separator;
    if (cfg.audio.window_duration_ms > 0) fixed_cfg.audio.window_duration_ms = @intCast(cfg.audio.window_duration_ms);
    if (cfg.audio.overlap_duration_ms > 0) fixed_cfg.audio.overlap_duration_ms = @intCast(cfg.audio.overlap_duration_ms);

    const chunks = try inference_chunker.fixed_multimodal.chunkInput(alloc, input, fixed_cfg);
    defer inference_chunker.types.freeChunks(alloc, chunks);
    return try cloneRemoteChunks(alloc, chunks);
}

fn cloneRemoteChunks(alloc: Allocator, source: []const RemoteChunk) ![]RemoteChunk {
    const chunks = try alloc.alloc(RemoteChunk, source.len);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| {
            chunk.deinit(alloc);
        }
        alloc.free(chunks);
    }

    for (source, 0..) |chunk, i| {
        chunks[i] = .{ .id = chunk.id, .mime_type = "", .start_char = chunk.start_char, .end_char = chunk.end_char, .start_time_ms = chunk.start_time_ms, .end_time_ms = chunk.end_time_ms, .frame_index = chunk.frame_index, .frame_delay_ms = chunk.frame_delay_ms };
        initialized += 1;
        chunks[i].mime_type = try alloc.dupe(u8, chunk.mime_type);
        chunks[i].owns_mime_type = true;
        if (chunk.text) |text| {
            chunks[i].text = try alloc.dupe(u8, text);
            chunks[i].owns_text = true;
        }
        if (chunk.data) |data| {
            chunks[i].data = try alloc.dupe(u8, data);
            chunks[i].owns_data = true;
        }
        if (chunk.metadata) |metadata| {
            chunks[i].metadata = try metadata.clone(alloc);
            chunks[i].owns_metadata = true;
        }
    }
    return chunks;
}

pub fn freeRemoteChunks(alloc: Allocator, chunks: []RemoteChunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
}

const EncodedChunkRequest = struct {
    metadata_or_json: []u8,
    envelope: ?httpx.attachment_envelope.EncodedSegments = null,

    pub fn deinit(self: *EncodedChunkRequest, alloc: Allocator) void {
        if (self.envelope) |*envelope| envelope.deinit();
        alloc.free(self.metadata_or_json);
        self.* = undefined;
    }
};

fn convertOptional(comptime T: type, value: anytype) ?T {
    const v = value orelse return null;
    var converted: T = undefined;
    inline for (@typeInfo(T).@"struct".field_names) |name| @field(converted, name) = @field(v, name);
    return converted;
}

fn requestEmbedding(cfg: chunking_types.Config) ?inference_api.ChunkEmbeddingTarget {
    var target = convertOptional(inference_api.ChunkEmbeddingTarget, cfg.embedding) orelse return null;
    if (cfg.page) target.allow_partial = true;
    return target;
}

fn encodeChunkRequest(
    alloc: Allocator,
    cfg: chunking_types.Config,
    input: RemoteInput,
    attachment_transport: inference_work.AttachmentTransport,
) !EncodedChunkRequest {
    var output_config: ?std.json.Parsed(inference_api.ChunkOutputs) = if (cfg.outputs_json.len != 0) try std.json.parseFromSlice(inference_api.ChunkOutputs, alloc, cfg.outputs_json, .{}) else null;
    defer if (output_config) |*value| value.deinit();
    const requested_outputs = if (output_config) |value| value.value else null;
    const continuation = if (cfg.replay) |replay| try std.fmt.allocPrint(alloc, "{s}:{d}", .{ replay.plan_id, replay.ordinal }) else null;
    defer if (continuation) |cursor| alloc.free(cursor);
    const config: inference_api.ChunkConfig = .{
        .model = cfg.model,
        .max_chunks = if (cfg.replay != null and !cfg.page) 1 else if (cfg.max_chunks > 0) cfg.max_chunks else null,
        .threshold = cfg.threshold,
        .text = if (cfg.text.target_tokens > 0 or cfg.text.overlap_tokens > 0 or cfg.text.separator.len > 0) .{
            .target_tokens = if (cfg.text.target_tokens > 0) cfg.text.target_tokens else null,
            .overlap_tokens = if (cfg.text.target_tokens > 0 or cfg.text.overlap_tokens > 0) cfg.text.overlap_tokens else null,
            .separator = if (cfg.text.separator.len > 0) cfg.text.separator else null,
        } else null,
        .audio = if (cfg.audio.window_duration_ms > 0 or cfg.audio.overlap_duration_ms > 0) .{
            .window_duration_ms = if (cfg.audio.window_duration_ms > 0) cfg.audio.window_duration_ms else null,
            .overlap_duration_ms = if (cfg.audio.overlap_duration_ms > 0) cfg.audio.overlap_duration_ms else null,
        } else null,
    };

    switch (input) {
        .source => |reference| {
            if (!cfg.isModelAware()) return error.ModelAwareChunkProviderRequired;
            const request = .{ .input = .{ .type = "source", .uri = reference.uri, .mime_type = reference.mime_type, .version = reference.version, .version_kind = @tagName(reference.version_kind), .length = reference.length }, .config = config, .outputs = requested_outputs, .allow_partial = if (cfg.page) true else null, .embedding = requestEmbedding(cfg), .policy = convertOptional(inference_api.MediaChunkPolicy, cfg.policy), .include_content = !cfg.page, .stream = cfg.page, .continuation = continuation, .source_version = if (cfg.replay) |replay| replay.source_version else null };
            return .{ .metadata_or_json = try httpx.json.Json.stringify(alloc, request) };
        },
        .text => |text| {
            const request = inference_api.ChunkRequest{
                .input = .{ .string = text },
                .config = config,
                .outputs = requested_outputs,
                .allow_partial = if (cfg.page) true else null,
                .embedding = requestEmbedding(cfg),
                .policy = convertOptional(inference_api.MediaChunkPolicy, cfg.policy),
                .include_content = if (cfg.isModelAware()) true else null,
                .stream = if (cfg.page) true else null,
                .continuation = continuation,
                .source_version = if (cfg.replay) |replay| replay.source_version else null,
            };
            return .{ .metadata_or_json = try httpx.json.Json.stringify(alloc, request) };
        },
        .binary => |binary| {
            const framed = attachment_transport == .segmented_framed_binary;
            const data = if (framed)
                "attachment:0"
            else
                try base64EncodeAlloc(alloc, binary.data);
            defer if (!framed) alloc.free(@constCast(data));
            const request = struct {
                input: inference_api.MediaContentPart,
                config: inference_api.ChunkConfig,
                outputs: ?inference_api.ChunkOutputs,
                allow_partial: ?bool,
                embedding: ?inference_api.ChunkEmbeddingTarget,
                policy: ?inference_api.MediaChunkPolicy,
                include_content: ?bool,
                stream: ?bool,
                continuation: ?[]const u8,
                source_version: ?[]const u8,
            }{
                .input = .{
                    .type = "media",
                    .data = data,
                    .mime_type = binary.mime_type,
                },
                .config = config,
                .outputs = requested_outputs,
                .allow_partial = if (cfg.page) true else null,
                .embedding = requestEmbedding(cfg),
                .policy = convertOptional(inference_api.MediaChunkPolicy, cfg.policy),
                .include_content = if (cfg.isModelAware()) !cfg.page else null,
                .stream = if (cfg.page) true else null,
                .continuation = continuation,
                .source_version = if (cfg.replay) |replay| replay.source_version else null,
            };
            const metadata = try httpx.json.Json.stringify(alloc, request);
            if (!framed) return .{ .metadata_or_json = metadata };
            errdefer alloc.free(metadata);
            const attachments = [_]httpx.attachment_envelope.Attachment{.{
                .mime_type = binary.mime_type,
                .data = binary.data,
            }};
            return .{
                .metadata_or_json = metadata,
                .envelope = try httpx.attachment_envelope.encodeSegmentsAlloc(alloc, metadata, &attachments),
            };
        },
    }
}

fn parseChunkResponse(alloc: Allocator, response_body: []const u8) ![]RemoteChunk {
    var tag = try std.json.parseFromSlice(struct { object: ?[]const u8 = null }, alloc, response_body, .{ .ignore_unknown_fields = true });
    defer tag.deinit();
    if (tag.value.object) |kind| if (std.mem.eql(u8, kind, "chunk.embedding.list")) return parseModelChunkResponse(alloc, response_body, null);
    const Response = struct {
        data: []const struct {
            id: i64,
            mime_type: []const u8,
            text: ?[]const u8 = null,
            start_char: ?i64 = null,
            end_char: ?i64 = null,
            data: ?[]const u8 = null,
            start_time_ms: ?f32 = null,
            end_time_ms: ?f32 = null,
            frame_index: ?i64 = null,
            frame_delay_ms: ?i64 = null,
        },
    };

    var parsed = try std.json.parseFromSlice(Response, alloc, response_body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var chunks = try alloc.alloc(RemoteChunk, parsed.value.data.len);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }

    for (parsed.value.data, 0..) |chunk, i| {
        var out = RemoteChunk{
            .id = std.math.cast(u32, chunk.id) orelse return error.InvalidChunkerResponse,
            .mime_type = try alloc.dupe(u8, chunk.mime_type),
            .text = null,
            .start_char = if (chunk.start_char) |v| std.math.cast(u32, v) orelse return error.InvalidChunkerResponse else null,
            .end_char = if (chunk.end_char) |v| std.math.cast(u32, v) orelse return error.InvalidChunkerResponse else null,
            .data = null,
            .start_time_ms = chunk.start_time_ms,
            .end_time_ms = chunk.end_time_ms,
            .frame_index = if (chunk.frame_index) |v| std.math.cast(u32, v) orelse return error.InvalidChunkerResponse else null,
            .frame_delay_ms = if (chunk.frame_delay_ms) |v| std.math.cast(u32, v) orelse return error.InvalidChunkerResponse else null,
            .owns_mime_type = true,
            .owns_text = false,
            .owns_data = false,
        };
        errdefer {
            out.deinit(alloc);
        }
        if (chunk.text) |value| {
            out.text = try alloc.dupe(u8, value);
            out.owns_text = true;
        }
        if (chunk.data) |value| {
            out.data = try base64DecodeAlloc(alloc, value);
            out.owns_data = true;
        }
        chunks[i] = out;
        initialized += 1;
    }

    return chunks;
}

fn parseModelChunkResponse(a: Allocator, body: []const u8, replay: ?chunking_types.Replay) ![]RemoteChunk {
    return parseModelChunks(a, body, replay, false);
}

fn parseModelChunks(a: Allocator, body: []const u8, replay: ?chunking_types.Replay, paged: bool) ![]RemoteChunk {
    var parsed = try std.json.parseFromSlice(inference_api.ChunkEmbeddingResponse, a, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const response = parsed.value;
    if (paged) {
        if (response.@"error") |failure| return remoteChunkFailure(failure.code);
        if ((!std.mem.eql(u8, response.status, "complete") and !std.mem.eql(u8, response.status, "partial")) or response.start_ordinal < 0 or response.covered_through_ordinal < response.start_ordinal or response.covered_through_ordinal - response.start_ordinal != response.data.len) return error.IncompleteChunkCoverage;
    } else if (replay) |expected| {
        if ((!std.mem.eql(u8, response.status, "complete") and !std.mem.eql(u8, response.status, "partial")) or response.gaps.len != 0 or response.data.len != 1 or response.@"error" != null or response.start_ordinal != expected.ordinal or response.covered_through_ordinal != expected.ordinal + 1 or !std.mem.eql(u8, expected.plan_id, response.plan_id) or !std.mem.eql(u8, expected.source_fingerprint, response.source_fingerprint) or !std.mem.eql(u8, expected.source_version, response.source_version)) return error.InvalidChunkReplay;
    } else if (!std.mem.eql(u8, response.status, "complete") or response.next_continuation != null or response.gaps.len != 0 or response.start_ordinal != 0 or response.covered_through_ordinal != response.data.len) return error.IncompleteChunkCoverage;
    if (response.source_fingerprint.len != 64 or response.plan_id.len != 64 or response.model_identity.len != 64 or response.source_version.len == 0 or response.dimensions < 0) return error.InvalidChunkerResponse;
    var plan: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&plan, response.plan_id) catch return error.InvalidChunkerResponse;
    var source: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&source, response.source_fingerprint) catch return error.InvalidChunkerResponse;
    const chunks = try a.alloc(RemoteChunk, response.data.len);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(a);
        a.free(chunks);
    }
    for (response.data, chunks, 0..) |item, *chunk, i| {
        const ordinal: u64 = try std.math.add(u64, if (paged) @intCast(response.start_ordinal) else if (replay) |expected| expected.ordinal else 0, i);
        if ((!paged and item.@"error" != null) or item.descriptor.ordinal != ordinal) return error.InvalidChunkerResponse;
        var cursor: [85]u8 = undefined;
        if (!std.mem.eql(u8, item.descriptor.id, (inference_chunker.planning.Identity{ .plan = plan, .source = source }).cursor(ordinal, &cursor))) return error.InvalidChunkerResponse;
        const raw = item.descriptor.coordinates;
        var coordinates = inference_chunker.planning.Coordinates{ .kind = std.meta.stringToEnum(inference_chunker.planning.Kind, raw.kind) orelse return error.InvalidChunkerResponse };
        inline for (.{ "start_byte", "end_byte", "page", "start_ns", "end_ns", "first_picture", "picture_count", "start_sample", "end_sample", "sample_rate", "start_tick", "end_tick", "timescale", "page_rotation", "raster_width", "raster_height" }) |name| {
            if (@field(raw, name)) |number| @field(coordinates, name) = std.math.cast(@typeInfo(@TypeOf(@field(coordinates, name))).optional.child, number) orelse return error.InvalidChunkerResponse;
        }
        var owned_pictures: ?[]u64 = null;
        defer if (owned_pictures) |pictures| a.free(pictures);
        if (raw.picture_basis) |basis| coordinates.picture_basis = std.meta.stringToEnum(inference_chunker.planning.PictureBasis, basis) orelse return error.InvalidChunkerResponse;
        if (raw.selected_pictures) |pictures| {
            if (pictures.len > 32) return error.InvalidChunkerResponse;
            const selected = try a.alloc(u64, pictures.len);
            owned_pictures = selected;
            for (selected, pictures) |*dest, picture| dest.* = std.math.cast(u64, picture) orelse return error.InvalidChunkerResponse;
            coordinates.selected_pictures = selected;
        }
        if (raw.page_box) |box| {
            if (box.len != 4) return error.InvalidChunkerResponse;
            coordinates.page_box = box[0..4].*;
        }
        if (raw.region) |box| {
            if (box.len != 4) return error.InvalidChunkerResponse;
            var region: [4]u32 = undefined;
            for (&region, box) |*dest, n| dest.* = std.math.cast(u32, n) orelse return error.InvalidChunkerResponse;
            coordinates.region = region;
        }
        try coordinates.validate();
        var policy = inference_chunker.planning.Policy{};
        inline for (@typeInfo(inference_chunker.planning.Policy).@"struct".field_names) |name| if (@field(response.policy, name)) |n| {
            @field(policy, name) = std.math.cast(@TypeOf(@field(policy, name)), n) orelse return error.InvalidChunkerResponse;
        };
        try policy.validate();
        if (item.embedding) |vector| {
            if (vector.len != response.dimensions) return error.InvalidChunkerResponse;
            for (vector) |v| if (!std.math.isFinite(v)) return error.InvalidChunkerResponse;
        }
        chunk.* = .{ .id = std.math.cast(u32, ordinal) orelse return error.InvalidChunkerResponse, .mime_type = "" };
        initialized += 1;
        chunk.mime_type = try a.dupe(u8, item.mime_type);
        chunk.owns_mime_type = true;
        if (item.text) |text| {
            chunk.text = try a.dupe(u8, text);
            chunk.owns_text = true;
        }
        if (item.data) |data| {
            chunk.data = try base64DecodeAlloc(a, data);
            chunk.owns_data = true;
        }
        chunk.start_char = if (coordinates.start_byte) |n| std.math.cast(u32, n) else null;
        chunk.end_char = if (coordinates.end_byte) |n| std.math.cast(u32, n) else null;
        const output_json = if (item.outputs) |values| try std.json.Stringify.valueAlloc(a, values, .{ .emit_null_optional_fields = false }) else null;
        defer if (output_json) |bytes| a.free(bytes);
        chunk.metadata = try (inference_chunker.types.ModelMetadata{ .descriptor = .{ .ordinal = ordinal, .coordinates = coordinates, .input_tokens = std.math.cast(usize, item.descriptor.input_tokens) orelse return error.InvalidChunkerResponse }, .inference_id = item.descriptor.id, .source_fingerprint = response.source_fingerprint, .source_version = response.source_version, .plan_id = response.plan_id, .model = response.model, .model_identity = response.model_identity, .model_revision = response.model_revision, .task_type = response.task_type, .dimensions = @intCast(response.dimensions), .policy = policy, .embedding = item.embedding, .embedding_error = if (item.@"error") |failure| failure.message else null, .outputs_json = output_json }).clone(a);
        chunk.owns_metadata = true;
    }
    return chunks;
}

fn base64EncodeAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

fn base64DecodeAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    try std.base64.standard.Decoder.decode(out, encoded);
    return out;
}

test "antfly chunker compiles" {
    _ = chunkText;
    _ = chunkInput;
    _ = chunkBinary;
}

test "antfly chunk request frames borrowed binary input without base64" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var body = try encodeChunkRequest(
                alloc,
                .{ .provider = .antfly, .model = "fixed" },
                .{ .binary = .{ .mime_type = "image/gif", .data = "GIF89a" } },
                .segmented_framed_binary,
            );
            defer body.deinit(alloc);
            const envelope = body.envelope orelse return error.MissingAttachmentEnvelope;
            try std.testing.expectEqual(@as(usize, 4), envelope.segments.len);
            try std.testing.expectEqualStrings("image/gif", envelope.segments[2]);
            try std.testing.expectEqualStrings("GIF89a", envelope.segments[3]);
            var metadata = try std.json.parseFromSlice(std.json.Value, alloc, body.metadata_or_json, .{});
            defer metadata.deinit();
            const input = metadata.value.object.get("input").?.object;
            try std.testing.expectEqualStrings("attachment:0", input.get("data").?.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "empty chunk input short-circuits without provider or remote dispatch" {
    const alloc = std.testing.allocator;
    // An unreachable endpoint proves nothing was dispatched: a request
    // would fail, an empty input returns zero chunks instead.
    const cfg = chunking_types.Config{
        .provider = .antfly,
        .model = "fixed",
        .api_url = "http://127.0.0.1:1/ai/v1/chunk",
    };
    const text_chunks = try chunkInput(alloc, cfg, .{ .text = "" });
    defer freeRemoteChunks(alloc, text_chunks);
    try std.testing.expectEqual(@as(usize, 0), text_chunks.len);

    const binary_chunks = try chunkInput(alloc, cfg, .{ .binary = .{ .data = "", .mime_type = "application/pdf" } });
    defer freeRemoteChunks(alloc, binary_chunks);
    try std.testing.expectEqual(@as(usize, 0), binary_chunks.len);
}

test "antfly chunker text round trip" {
    const alloc = std.testing.allocator;
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqualStrings("docs", req.header("X-Antfly-Source-Table") orelse "");
            if (req.method == .GET) return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/json"),
                .body = try req_alloc.dupe(u8, "{\"chunkers\":{\"chunker-v1\":{}}}"),
            };
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/chunk"));
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"chunker-v1\"") != null);
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/json"),
                .body = try req_alloc.dupe(u8,
                    \\{"object":"list","data":[
                    \\  {"object":"chunk","index":0,"id":0,"mime_type":"text/plain","text":"alpha body","start_char":0,"end_char":10},
                    \\  {"object":"chunk","index":1,"id":1,"mime_type":"text/plain","text":"beta tail","start_char":11,"end_char":20}
                    \\],"model":"chunker-v1","usage":{"prompt_tokens":4,"completion_tokens":0,"total_tokens":4},"cache_hit":false}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    const cfg = chunking_types.Config{
        .provider = .antfly,
        .model = "chunker-v1",
        .text = .{ .target_tokens = 8, .overlap_tokens = 2 },
    };
    const provider = chunk_provider.Provider{ .execution = .{
        .default_endpoint = base_uri,
        .routing = .{ .source_table = "docs" },
    } };

    const chunks = try chunkTextWithProvider(alloc, cfg, "alpha beta gamma delta", provider);
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }

    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqualStrings("alpha body", chunks[0].text.?);
    try std.testing.expectEqual(@as(?u32, 11), chunks[1].start_offset);
}

test "antfly chunker omits incomplete and invalid provenance spans" {
    const alloc = std.testing.allocator;
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            if (req.method == .GET) {
                try std.testing.expect(std.mem.indexOf(u8, req.uri, "/models?") != null);
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "application/json"),
                    .body = try req_alloc.dupe(u8, "{\"chunkers\":{\"chunker-v1\":{}}}"),
                };
            }
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/chunk"));
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/json"),
                .body = try req_alloc.dupe(u8,
                    \\{"object":"list","data":[
                    \\  {"object":"chunk","index":0,"id":0,"mime_type":"text/plain","text":"alpha","start_char":0},
                    \\  {"object":"chunk","index":1,"id":1,"mime_type":"text/plain","text":"beta","end_char":9},
                    \\  {"object":"chunk","index":2,"id":2,"mime_type":"text/plain","text":"gamma","start_char":8,"end_char":2},
                    \\  {"object":"chunk","index":3,"id":3,"mime_type":"text/plain","text":"delta","start_char":0,"end_char":99}
                    \\],"model":"chunker-v1","usage":{"prompt_tokens":2,"completion_tokens":0,"total_tokens":2},"cache_hit":false}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    const cfg = chunking_types.Config{
        .provider = .antfly,
        .api_url = base_uri,
        .model = "chunker-v1",
        .text = .{ .target_tokens = 8, .overlap_tokens = 2 },
    };

    const chunks = try chunkText(alloc, cfg, "alpha beta");
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }

    try std.testing.expectEqual(@as(usize, 4), chunks.len);
    try std.testing.expectEqual(@as(?u32, null), chunks[0].start_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[0].end_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[1].start_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[1].end_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[2].start_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[2].end_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[3].start_offset);
    try std.testing.expectEqual(@as(?u32, null), chunks[3].end_offset);
}

test "antfly chunker binary round trip" {
    const alloc = std.testing.allocator;
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            if (req.method == .GET) {
                try std.testing.expect(std.mem.indexOf(u8, req.uri, "/models?") != null);
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "application/json"),
                    .body = try req_alloc.dupe(u8, "{\"chunkers\":{\"chunker-v1\":{}}}"),
                };
            }
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"mime_type\":\"image/gif\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"data\":\"R0lG\"") != null);
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/json"),
                .body = try req_alloc.dupe(u8,
                    \\{"object":"list","data":[
                    \\  {"object":"chunk","index":0,"id":0,"mime_type":"image/png","data":"iVBORw0KGgo=","frame_index":0,"frame_delay_ms":50}
                    \\],"model":"chunker-v1","usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0},"cache_hit":false}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    const cfg = chunking_types.Config{
        .provider = .antfly,
        .api_url = base_uri,
        .model = "chunker-v1",
    };

    const chunks = try chunkBinary(alloc, cfg, "image/gif", "GIF");
    defer freeRemoteChunks(alloc, chunks);

    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expectEqualStrings("image/png", chunks[0].mime_type);
    try std.testing.expectEqual(@as(?u32, 0), chunks[0].frame_index);
    try std.testing.expectEqual(@as(?u32, 50), chunks[0].frame_delay_ms);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, chunks[0].data.?[0..8]);
}

test "antfly chunker with empty api_url runs locally" {
    const alloc = std.testing.allocator;
    const cfg = chunking_types.Config{
        .provider = .antfly,
        .model = "fixed-bert-tokenizer",
        .max_chunks = 2,
        .text = .{ .target_tokens = 3, .overlap_tokens = 0, .separator = " " },
    };

    const chunks = try chunkText(alloc, cfg, "alpha beta gamma delta epsilon");
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }

    try std.testing.expect(chunks.len > 0);
    try std.testing.expect(chunks.len <= 2);
    try std.testing.expect(chunks[0].text != null);
}

test "antfly chunker local path preserves explicit zero overlap when target is set" {
    const alloc = std.testing.allocator;
    const cfg = chunking_types.Config{
        .provider = .antfly,
        .model = "fixed-bert-tokenizer",
        .text = .{ .target_tokens = 4, .overlap_tokens = 0, .separator = " " },
    };

    const chunks = try chunkText(alloc, cfg, "alpha beta gamma delta epsilon");
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }

    try std.testing.expect(chunks.len > 0);
    try std.testing.expect(chunks[0].text != null);
}

test "antfly chunker uses the embedded provider executor when available" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        legacy_calls: usize = 0,
        context_calls: usize = 0,

        fn chunk(
            ptr: *anyopaque,
            result_alloc: Allocator,
            model: []const u8,
            input: inference_chunker.Input,
            _: chunking_types.Config,
        ) ![]inference_chunker.Chunk {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.legacy_calls += 1;
            _ = result_alloc;
            _ = model;
            _ = input;
            return error.TestUnexpectedResult;
        }

        fn chunkWithContext(
            ptr: *anyopaque,
            result_alloc: Allocator,
            model: []const u8,
            input: inference_chunker.Input,
            _: chunking_types.Config,
            context: execution_context.RequestContext,
        ) ![]inference_chunker.Chunk {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.check();
            try std.testing.expect(context.deadline_ns != null);
            self.context_calls += 1;
            try std.testing.expectEqualStrings("fixed", model);
            try std.testing.expectEqualStrings("provider input", input.text);
            const out = try result_alloc.alloc(inference_chunker.Chunk, 1);
            out[0] = .{
                .id = 0,
                .mime_type = try result_alloc.dupe(u8, "text/plain"),
                .text = try result_alloc.dupe(u8, "provider output"),
                .owns_mime_type = true,
                .owns_text = true,
            };
            return out;
        }
    };
    var fake = Fake{};
    const provider = chunk_provider.Provider{
        .ptr = &fake,
        .boundary_dispatch = ChunkProviderBoundary.local_dispatch,
        .chunk_input_callback = @ptrCast(&Fake.chunk),
        .chunk_input_with_context_callback = @ptrCast(&Fake.chunkWithContext),
        .execution = .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
    };
    const cfg = chunking_types.Config{ .provider = .antfly, .model = "fixed" };
    const chunks = try chunkTextWithProvider(alloc, cfg, "provider input", provider);
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.context_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.legacy_calls);
    try std.testing.expectEqualStrings("provider output", chunks[0].text.?);

    var canceled = std.atomic.Value(bool).init(true);
    var canceled_provider = provider;
    canceled_provider.execution.cancellation = .fromAtomic(&canceled);
    try std.testing.expectError(
        error.Canceled,
        chunkTextWithProvider(alloc, cfg, "provider input", canceled_provider),
    );
    try std.testing.expectEqual(@as(usize, 1), fake.context_calls);
}

test "antfly chunker execution-only provider preserves local fallback and cancellation" {
    const alloc = std.testing.allocator;
    const cfg = chunking_types.Config{
        .provider = .antfly,
        .model = "fixed",
        .max_chunks = 2,
        .text = .{ .target_tokens = 3, .overlap_tokens = 0, .separator = " " },
    };
    var provider: chunk_provider.Provider = .{ .execution = .{ .io = std.testing.io } };
    const chunks = try chunkTextWithProvider(alloc, cfg, "alpha beta gamma delta epsilon", provider);
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    try std.testing.expect(chunks.len > 0 and chunks.len <= 2);
    try std.testing.expect(chunks[0].text != null);
    var canceled = std.atomic.Value(bool).init(true);
    provider.execution.cancellation = .fromAtomic(&canceled);
    try std.testing.expectError(error.Canceled, chunkTextWithProvider(alloc, cfg, "alpha beta", provider));
}

const model_page_fixture =
    \\{"object":"chunk.embedding.list","data":[{"descriptor":{"id":"1111111111111111111111111111111111111111111111111111111111111111:0","ordinal":0,"coordinates":{"kind":"video","start_ns":0,"end_ns":1000000000,"start_tick":0,"end_tick":90000,"timescale":90000},"input_tokens":144},"mime_type":"video/mp4","embedding":[0.6,0.8]}],"status":"complete","source_fingerprint":"2222222222222222222222222222222222222222222222222222222222222222","source_version":"v1","plan_id":"1111111111111111111111111111111111111111111111111111111111111111","model":"model","model_identity":"3333333333333333333333333333333333333333333333333333333333333333","model_revision":"4444444444444444444444444444444444444444444444444444444444444444","task_type":"RETRIEVAL_DOCUMENT","dimensions":2,"policy":{},"start_ordinal":0,"covered_through_ordinal":1,"gaps":[]}
;

test "model aware media transport retains vector provenance and rejects incomplete coverage" {
    const Fixture = struct {
        const raw = model_page_fixture;
        fn qualify(a: Allocator) !void {
            const chunks = try parseChunkResponse(a, raw);
            defer freeRemoteChunks(a, chunks);
            const copy = try cloneRemoteChunks(a, chunks);
            defer freeRemoteChunks(a, copy);
            try std.testing.expectEqual(@as(usize, 1), copy.len);
            const metadata = copy[0].metadata.?;
            try std.testing.expectEqualStrings("v1", metadata.source_version);
            try std.testing.expectEqual(@as(?u32, 90000), metadata.descriptor.coordinates.timescale);
            try std.testing.expectEqualSlices(f32, &.{ 0.6, 0.8 }, metadata.embedding.?);
        }
    };
    try Fixture.qualify(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.qualify, .{});
    const partial = try std.mem.replaceOwned(u8, std.testing.allocator, Fixture.raw, "\"complete\"", "\"partial\"");
    defer std.testing.allocator.free(partial);
    try std.testing.expectError(error.IncompleteChunkCoverage, parseChunkResponse(std.testing.allocator, partial));
}

fn remoteChunkFailure(code: []const u8) anyerror {
    if (std.mem.eql(u8, code, "StaleChunkContinuation") or std.mem.eql(u8, code, "SourceVersionChanged")) return error.StaleChunkContinuation;
    if (std.mem.eql(u8, code, "EmbeddingIdentityMismatch")) return error.EmbeddingIdentityMismatch;
    return error.ChunkRequestFailed;
}

fn parseModelChunkPage(a: Allocator, body: []const u8, cfg: chunking_types.Config) !ChunkPage {
    var parsed = try std.json.parseFromSlice(inference_api.ChunkEmbeddingResponse, a, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const r = parsed.value;
    const chunks = try parseModelChunks(a, body, cfg.replay, true);
    errdefer freeRemoteChunks(a, chunks);
    const plan = try a.dupe(u8, r.plan_id);
    errdefer a.free(plan);
    const source = try a.dupe(u8, r.source_fingerprint);
    errdefer a.free(source);
    const version = try a.dupe(u8, r.source_version);
    errdefer a.free(version);
    const page = ChunkPage{ .chunks = chunks, .plan_id = plan, .source_fingerprint = source, .source_version = version, .start_ordinal = @intCast(r.start_ordinal), .covered_through_ordinal = @intCast(r.covered_through_ordinal), .complete = r.source_complete orelse (std.mem.eql(u8, r.status, "complete") and r.next_continuation == null) };
    if (r.next_continuation) |next| {
        if (page.complete) return error.InvalidChunkerResponse;
        var buffer: [85]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buffer, "{s}:{d}", .{ page.plan_id, page.covered_through_ordinal });
        if (!std.mem.eql(u8, expected, next)) return error.InvalidChunkerResponse;
    } else if (!page.complete) return error.IncompleteChunkCoverage;
    var gaps: usize = 0;
    for (chunks) |chunk| if (chunk.metadata.?.embedding_error != null) {
        gaps += 1;
    };
    if (gaps != r.gaps.len) return error.InvalidChunkerResponse;
    try validateChunkPage(cfg, page);
    return page;
}

pub fn validateChunkPage(cfg: chunking_types.Config, page: ChunkPage) !void {
    const start = if (cfg.replay) |position| position.ordinal else 0;
    if (page.chunks.len != 0 or start != 0) {
        if (page.plan_id.len != 64 or page.source_fingerprint.len != 64 or page.source_version.len == 0) return error.InvalidChunkerResponse;
        var digest: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&digest, page.plan_id) catch return error.InvalidChunkerResponse;
        _ = std.fmt.hexToBytes(&digest, page.source_fingerprint) catch return error.InvalidChunkerResponse;
    }
    if (page.start_ordinal != start or page.covered_through_ordinal < start or page.covered_through_ordinal - start != page.chunks.len or page.chunks.len > cfg.max_chunks or (!page.complete and page.chunks.len == 0)) return error.IncompleteChunkCoverage;
    if (cfg.replay) |position| if (!std.mem.eql(u8, position.plan_id, page.plan_id) or !std.mem.eql(u8, position.source_fingerprint, page.source_fingerprint) or !std.mem.eql(u8, position.source_version, page.source_version)) return error.StaleChunkContinuation;
    for (page.chunks, 0..) |chunk, i| {
        const metadata = chunk.metadata orelse return error.InvalidChunkerResponse;
        if (metadata.model_identity.len != 64 or metadata.model_revision.len != 64) return error.InvalidChunkerResponse;
        try metadata.descriptor.coordinates.validate();
        try metadata.policy.validate();
        var id_buffer: [85]u8 = undefined;
        const expected_id = try std.fmt.bufPrint(&id_buffer, "{s}:{d}", .{ page.plan_id, start + i });
        if (!std.mem.eql(u8, metadata.inference_id, expected_id)) return error.InvalidChunkerResponse;
        if (metadata.descriptor.ordinal != start + i or chunk.id != start + i or !std.mem.eql(u8, metadata.plan_id, page.plan_id) or !std.mem.eql(u8, metadata.source_fingerprint, page.source_fingerprint) or !std.mem.eql(u8, metadata.source_version, page.source_version)) return error.InvalidChunkerResponse;
        if (metadata.embedding_error != null and metadata.embedding != null) return error.InvalidChunkerResponse;
    }
    try validateModelChunks(cfg, page.chunks);
}

test "model aware media pages preserve continuation coverage and partial output failures" {
    const a = std.testing.allocator;
    const cfg = chunking_types.Config{ .provider = .antfly, .embedding = .{ .model = "model", .dimensions = 2 }, .page = true, .max_chunks = 1 };
    var page = try parseModelChunkPage(a, model_page_fixture, cfg);
    defer page.deinit(a);
    try std.testing.expect(page.complete);
    const partial = try std.mem.replaceOwned(u8, a, model_page_fixture, "\"status\":\"complete\"", "\"status\":\"partial\",\"next_continuation\":\"1111111111111111111111111111111111111111111111111111111111111111:1\"");
    defer a.free(partial);
    var prefix = try parseModelChunkPage(a, partial, cfg);
    defer prefix.deinit(a);
    try std.testing.expect(!prefix.complete);
    var resumed = cfg;
    resumed.replay = .{ .plan_id = page.plan_id, .source_fingerprint = page.source_fingerprint, .source_version = page.source_version, .ordinal = 1 };
    try std.testing.expectError(error.IncompleteChunkCoverage, parseModelChunkPage(a, partial, resumed));
    const empty = try std.mem.replaceOwned(u8, a, partial, "\"covered_through_ordinal\":1", "\"covered_through_ordinal\":0");
    defer a.free(empty);
    try std.testing.expectError(error.IncompleteChunkCoverage, parseModelChunkPage(a, empty, cfg));
    const failed = try std.mem.replaceOwned(u8, a, model_page_fixture, "\"embedding\":[0.6,0.8]", "\"error\":{\"code\":\"CHUNK_EMBEDDING_FAILED\",\"message\":\"InvalidAudio\"}");
    defer a.free(failed);
    const gaps = try std.mem.replaceOwned(u8, a, failed, "\"gaps\":[]", "\"gaps\":[{\"kind\":\"video\",\"start_ns\":0,\"end_ns\":1000000000,\"start_tick\":0,\"end_tick\":90000,\"timescale\":90000}]");
    defer a.free(gaps);
    var degraded = try parseModelChunkPage(a, gaps, cfg);
    defer degraded.deinit(a);
    try std.testing.expect(degraded.complete);
    try std.testing.expectEqualStrings("InvalidAudio", degraded.chunks[0].metadata.?.embedding_error.?);
    try std.testing.expect(degraded.chunks[0].metadata.?.embedding == null);
    const exhausted = try std.mem.replaceOwned(u8, a, gaps, "\"status\":\"complete\"", "\"status\":\"partial\",\"source_complete\":true");
    defer a.free(exhausted);
    var exhausted_page = try parseModelChunkPage(a, exhausted, cfg);
    defer exhausted_page.deinit(a);
    try std.testing.expect(exhausted_page.complete);
}

fn parseModelChunkStream(a: Allocator, body: []const u8, cfg: chunking_types.Config) !ChunkPage {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temporary = arena.allocator();
    const Response = inference_api.ChunkEmbeddingResponse;
    const WireItem = @typeInfo(@TypeOf(@as(Response, undefined).data)).pointer.child;
    var header: ?inference_api.ChunkEmbeddingContext = null;
    var terminal: ?Response = null;
    var items: std.ArrayListUnmanaged(WireItem) = .empty;
    var lines = std.mem.splitScalar(u8, body, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        if (terminal != null) return error.InvalidChunkerResponse;
        const tag = try std.json.parseFromSliceLeaky(struct { object: []const u8 }, temporary, trimmed, .{ .ignore_unknown_fields = true });
        if (std.mem.eql(u8, tag.object, "chunk.embedding.error")) {
            const failure = try std.json.parseFromSliceLeaky(struct { code: []const u8 }, temporary, trimmed, .{ .ignore_unknown_fields = true });
            return remoteChunkFailure(failure.code);
        } else if (std.mem.eql(u8, tag.object, "chunk.embedding.start")) {
            if (header != null or items.items.len != 0) return error.InvalidChunkerResponse;
            header = (try std.json.parseFromSliceLeaky(struct { context: inference_api.ChunkEmbeddingContext }, temporary, trimmed, .{ .ignore_unknown_fields = true })).context;
        } else if (std.mem.eql(u8, tag.object, "chunk.embedding")) {
            if (header == null or items.items.len >= cfg.max_chunks) return error.InvalidChunkerResponse;
            const event = try std.json.parseFromSliceLeaky(struct { data: WireItem }, temporary, trimmed, .{ .ignore_unknown_fields = true });
            try items.append(temporary, event.data);
        } else if (std.mem.eql(u8, tag.object, "chunk.embedding.complete")) {
            if (header == null) return error.InvalidChunkerResponse;
            terminal = (try std.json.parseFromSliceLeaky(struct { response: Response }, temporary, trimmed, .{ .ignore_unknown_fields = true })).response;
        } else return error.InvalidChunkerResponse;
    }
    var response = terminal orelse return error.IncompleteChunkCoverage;
    const start = header orelse return error.InvalidChunkerResponse;
    inline for (.{ "plan_id", "source_fingerprint", "source_version", "model", "model_identity", "model_revision", "task_type" }) |field| {
        if (!std.mem.eql(u8, @field(start, field), @field(response, field))) return error.InvalidChunkerResponse;
    }
    if (start.start_ordinal != response.start_ordinal or !std.meta.eql(start.policy, response.policy) or response.data.len != 0) return error.InvalidChunkerResponse;
    if (start.dimensions) |dimensions| if (dimensions != response.dimensions) return error.InvalidChunkerResponse;
    response.data = items.items;
    const encoded = try std.json.Stringify.valueAlloc(temporary, response, .{});
    return parseModelChunkPage(a, encoded, cfg);
}

test "model aware media NDJSON acknowledges only matching terminal coverage" {
    const a = std.testing.allocator;
    var response = try std.json.parseFromSlice(inference_api.ChunkEmbeddingResponse, a, model_page_fixture, .{});
    defer response.deinit();
    const r = response.value;
    const context = inference_api.ChunkEmbeddingContext{ .source_fingerprint = r.source_fingerprint, .source_version = r.source_version, .plan_id = r.plan_id, .model = r.model, .model_identity = r.model_identity, .model_revision = r.model_revision, .task_type = r.task_type, .dimensions = r.dimensions, .policy = r.policy, .start_ordinal = r.start_ordinal };
    const start = try std.json.Stringify.valueAlloc(a, .{ .object = "chunk.embedding.start", .context = context }, .{});
    defer a.free(start);
    const event = try std.json.Stringify.valueAlloc(a, .{ .object = "chunk.embedding", .data = r.data[0] }, .{});
    defer a.free(event);
    var terminal_response = r;
    terminal_response.data = &.{};
    const terminal = try std.json.Stringify.valueAlloc(a, .{ .object = "chunk.embedding.complete", .response = terminal_response }, .{});
    defer a.free(terminal);
    const stream = try std.fmt.allocPrint(a, "{s}\n{s}\n{s}\n", .{ start, event, terminal });
    defer a.free(stream);
    const cfg = chunking_types.Config{ .provider = .antfly, .embedding = .{ .model = "model", .dimensions = 2 }, .page = true, .max_chunks = 64 };
    var page = try parseModelChunkStream(a, stream, cfg);
    defer page.deinit(a);
    try std.testing.expectEqual(@as(u64, 1), page.covered_through_ordinal);
    try std.testing.expectEqual(@as(usize, 1), page.chunks.len);
    const disconnected = try std.fmt.allocPrint(a, "{s}\n{s}\n", .{ start, event });
    defer a.free(disconnected);
    try std.testing.expectError(error.IncompleteChunkCoverage, parseModelChunkStream(a, disconnected, cfg));
    const missing = try std.fmt.allocPrint(a, "{s}\n{s}\n", .{ start, terminal });
    defer a.free(missing);
    try std.testing.expectError(error.IncompleteChunkCoverage, parseModelChunkStream(a, missing, cfg));
    try std.testing.expectError(error.StaleChunkContinuation, parseModelChunkStream(a, "{\"object\":\"chunk.embedding.error\",\"code\":\"StaleChunkContinuation\"}\n", cfg));
}

test "model aware media remote transcription pages request partial results without a legacy embedding target" {
    const a = std.testing.allocator;
    const config = chunking_types.Config{ .provider = .antfly, .page = true, .max_chunks = 2, .outputs_json = "{\"boundary_model\":\"speech\",\"targets\":[{\"name\":\"speech\",\"kind\":\"transcribe\",\"model\":\"whisper\"}]}" };
    var encoded = try encodeChunkRequest(a, config, .{ .binary = .{ .mime_type = "audio/wav", .data = &.{ 1, 2, 3 } } }, .base64_payload);
    defer encoded.deinit(a);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, encoded.metadata_or_json, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("allow_partial").?.bool);
    try std.testing.expect(parsed.value.object.get("embedding") == null or parsed.value.object.get("embedding").? == .null);
    try std.testing.expectEqualStrings("speech", parsed.value.object.get("outputs").?.object.get("boundary_model").?.string);
}
