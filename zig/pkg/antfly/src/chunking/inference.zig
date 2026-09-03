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

const std = @import("std");
const httpx = @import("httpx");
const inference_api = @import("inference_api");
const chunking_types = @import("types.zig");
const chunk_mod = @import("chunk.zig");
const Chunk = chunk_mod.Chunk;
const http_common = @import("../raft/transport/http_common.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");
const inference_chunker = @import("inference_chunker");
const chunk_provider = @import("provider.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const remote_capabilities = @import("../inference/remote_capabilities.zig");
const inference_work = @import("../inference/work.zig");

const Allocator = std.mem.Allocator;

const ChunkInputFn = *const fn (
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
) anyerror![]inference_chunker.Chunk;
const ChunkProviderVTable = struct {
    chunk_input: ?ChunkInputFn = null,
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
    if (cfg.api_url.len == 0) if (antfly_provider) |provider| {
        const chunk_input: ChunkInputFn = @ptrCast(@alignCast(provider.chunk_input_callback));
        const chunks = try ChunkProviderBoundary.call("chunk_input", provider.boundary_dispatch, chunk_input, .{ provider.ptr, alloc, if (cfg.model.len > 0) cfg.model else "fixed", input, cfg });
        defer inference_chunker.types.freeChunks(alloc, chunks);
        return try cloneRemoteChunks(alloc, chunks);
    };
    if (cfg.api_url.len == 0) return try chunkInputDirect(alloc, cfg, input);
    if (cfg.model.len == 0) return error.InvalidChunkerConfig;

    var fallback_io: ?std.Io.Threaded = null;
    defer if (fallback_io) |*io_impl| io_impl.deinit();
    const io = if (antfly_provider) |provider|
        provider.io orelse blk: {
            fallback_io = std.Io.Threaded.init(alloc, .{});
            break :blk fallback_io.?.io();
        }
    else blk: {
        fallback_io = std.Io.Threaded.init(alloc, .{});
        break :blk fallback_io.?.io();
    };

    var http = httpx.Client.init(alloc, io);
    defer http.deinit();

    var fallback_capability_cache: ?remote_capabilities.Cache = null;
    defer if (fallback_capability_cache) |*cache| cache.deinit();
    var capability_cache = if (antfly_provider) |provider| provider.remote_capability_cache else null;
    if (capability_cache == null) {
        fallback_capability_cache = remote_capabilities.Cache.init(alloc, http.io);
        capability_cache = &fallback_capability_cache.?;
    }
    const model = if (cfg.model.len > 0) cfg.model else "fixed";
    const capability_lease = try capability_cache.?.getOrDiscoverLease(
        &http,
        cfg.api_url,
        model,
        .chunk,
        &.{},
    );
    if (capability_lease.capabilities) |capabilities| {
        const shape: inference_work.InvocationShape = switch (input) {
            .text => |text| .{
                .item_count = 1,
                .modalities = .{ .text = true },
                .text_bytes = text.len,
                .max_text_bytes_per_item = text.len,
            },
            .binary => |binary| .{
                .item_count = 1,
                .modalities = inference_work.modalityForMimeType(binary.mime_type) orelse
                    return error.UnsupportedChunkMediaType,
                .encoded_media_bytes = binary.data.len,
                .max_media_parts_per_item = 1,
            },
        };
        try capabilities.validateInvocation(.chunk, shape);
    }

    const url = try std.fmt.allocPrint(alloc, "{s}/chunk", .{cfg.api_url});
    defer alloc.free(url);

    const body = try encodeChunkRequest(alloc, cfg, input);
    defer alloc.free(body);

    var header_storage: [2][2][]const u8 = undefined;
    var header_count: usize = 0;
    if (capability_lease.routing_token) |token| {
        header_storage[header_count] = .{ remote_capabilities.capability_token_header, token.slice() };
        header_count += 1;
    }
    if (capability_lease.descriptor_revision) |revision| {
        header_storage[header_count] = .{ remote_capabilities.capability_revision_header, revision.slice() };
        header_count += 1;
    }
    var resp = try http.post(url, .{ .json = body, .headers = header_storage[0..header_count] });
    defer resp.deinit();
    if (!resp.ok()) {
        const stale = resp.headers.get(remote_capabilities.capability_stale_header);
        if (resp.status.code == 409 and stale != null and
            std.ascii.eqlIgnoreCase(std.mem.trim(u8, stale.?, " \t"), "true"))
        {
            try capability_cache.?.invalidate(cfg.api_url, model, .chunk, &.{});
            return error.InferenceCapabilitiesStale;
        }
        return error.ChunkRequestFailed;
    }
    const response_body = resp.body orelse return error.EmptyResponse;
    return try parseChunkResponse(alloc, response_body);
}

fn chunkInputDirect(alloc: Allocator, cfg: chunking_types.Config, input: RemoteInput) ![]RemoteChunk {
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
        chunks[i] = .{
            .id = chunk.id,
            .mime_type = try alloc.dupe(u8, chunk.mime_type),
            .text = if (chunk.text) |text| try alloc.dupe(u8, text) else null,
            .start_char = chunk.start_char,
            .end_char = chunk.end_char,
            .data = if (chunk.data) |data| try alloc.dupe(u8, data) else null,
            .start_time_ms = chunk.start_time_ms,
            .end_time_ms = chunk.end_time_ms,
            .frame_index = chunk.frame_index,
            .frame_delay_ms = chunk.frame_delay_ms,
            .owns_mime_type = true,
            .owns_text = chunk.text != null,
            .owns_data = chunk.data != null,
        };
        initialized += 1;
    }
    return chunks;
}

pub fn freeRemoteChunks(alloc: Allocator, chunks: []RemoteChunk) void {
    for (chunks) |*chunk| chunk.deinit(alloc);
    alloc.free(chunks);
}

fn encodeChunkRequest(alloc: Allocator, cfg: chunking_types.Config, input: RemoteInput) ![]u8 {
    const config: inference_api.ChunkConfig = .{
        .model = cfg.model,
        .max_chunks = if (cfg.max_chunks > 0) cfg.max_chunks else null,
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
        .text => |text| {
            const request = inference_api.ChunkRequest{
                .input = .{ .string = text },
                .config = config,
            };
            return try httpx.json.Json.stringify(alloc, request);
        },
        .binary => |binary| {
            const data_b64 = try base64EncodeAlloc(alloc, binary.data);
            defer alloc.free(data_b64);
            const request = struct {
                input: inference_api.MediaContentPart,
                config: inference_api.ChunkConfig,
            }{
                .input = .{
                    .type = "media",
                    .data = data_b64,
                    .mime_type = binary.mime_type,
                },
                .config = config,
            };
            return try httpx.json.Json.stringify(alloc, request);
        },
    }
}

fn parseChunkResponse(alloc: Allocator, response_body: []const u8) ![]RemoteChunk {
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
    errdefer {
        for (chunks) |*chunk| chunk.deinit(alloc);
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
        .api_url = base_uri,
        .model = "chunker-v1",
        .text = .{ .target_tokens = 8, .overlap_tokens = 2 },
    };

    const chunks = try chunkText(alloc, cfg, "alpha beta gamma delta");
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
        calls: usize = 0,

        fn chunk(
            ptr: *anyopaque,
            result_alloc: Allocator,
            model: []const u8,
            input: inference_chunker.Input,
            _: chunking_types.Config,
        ) ![]inference_chunker.Chunk {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
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
    };
    const cfg = chunking_types.Config{ .provider = .antfly, .model = "fixed" };
    const chunks = try chunkTextWithProvider(alloc, cfg, "provider input", provider);
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqualStrings("provider output", chunks[0].text.?);
}
