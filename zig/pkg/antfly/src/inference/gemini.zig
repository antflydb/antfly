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

// Native Google Gemini / Vertex AI embedding provider.

const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test)
    @import("../storage/db/template_stub.zig")
else
    @import("../template.zig");

pub const HeaderPair = [2][]const u8;

pub const ApiKind = enum {
    gemini,
    vertex,
};

pub const Provider = struct {
    allocator: std.mem.Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    kind: ApiKind,
    project_id: []const u8 = "",
    location: []const u8 = "",
    headers: []HeaderPair = &.{},

    pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, base_url: []const u8, kind: ApiKind) Provider {
        return .{
            .allocator = allocator,
            .http = http,
            .base_url = base_url,
            .kind = kind,
        };
    }

    pub fn deinit(self: *Provider) void {
        for (self.headers) |header| {
            self.allocator.free(header[0]);
            self.allocator.free(header[1]);
        }
        if (self.headers.len > 0) self.allocator.free(self.headers);
        self.headers = &.{};
    }

    pub fn setHeaders(self: *Provider, headers: []const HeaderPair) !void {
        for (self.headers) |header| {
            self.allocator.free(header[0]);
            self.allocator.free(header[1]);
        }
        if (self.headers.len > 0) self.allocator.free(self.headers);
        self.headers = try self.allocator.alloc(HeaderPair, headers.len);
        errdefer self.allocator.free(self.headers);
        for (headers, 0..) |header, i| {
            self.headers[i] = .{
                try self.allocator.dupe(u8, header[0]),
                try self.allocator.dupe(u8, header[1]),
            };
        }
    }

    pub fn embedTextBatch(
        self: *Provider,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
        dims: u32,
    ) ![]const []const f32 {
        if (self.kind == .gemini) {
            const part_sets = try alloc.alloc([]const template_mod.ContentPart, texts.len);
            defer alloc.free(part_sets);
            const text_parts = try alloc.alloc(template_mod.ContentPart, texts.len);
            defer alloc.free(text_parts);
            for (texts, 0..) |text, i| {
                text_parts[i] = .{ .text = text };
                part_sets[i] = text_parts[i .. i + 1];
            }
            return try self.embedGeminiBatch(alloc, model, part_sets, dims);
        }

        const vectors = try alloc.alloc([]const f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
            alloc.free(vectors);
        }
        for (texts, 0..) |text, i| {
            const parts = [_]template_mod.ContentPart{.{ .text = text }};
            vectors[i] = try self.embedParts(alloc, model, &parts, dims);
            initialized += 1;
        }
        return vectors;
    }

    pub fn embedParts(
        self: *Provider,
        alloc: std.mem.Allocator,
        model: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
    ) ![]f32 {
        if (self.kind == .gemini) {
            const part_sets = [_][]const template_mod.ContentPart{parts};
            const vectors = try self.embedGeminiBatch(alloc, model, &part_sets, dims);
            defer alloc.free(vectors);
            if (vectors.len == 0) return error.EmptyEmbeddingResponse;
            return @constCast(vectors[0]);
        }

        const url = try endpointUrlAlloc(alloc, self.kind, self.base_url, self.project_id, self.location, model);
        defer alloc.free(url);

        const json_body = try embedContentRequestBodyAlloc(alloc, parts, dims);
        defer alloc.free(json_body);

        var resp = try self.http.post(url, .{ .json = json_body, .headers = self.headers });
        defer resp.deinit();

        if (!resp.ok()) return mapEmbedStatus(resp.status.code);
        const body = resp.body orelse return error.EmptyResponse;
        const Response = struct {
            embedding: struct {
                values: []const f32,
            },
        };
        var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.embedding.values.len == 0) return error.EmptyEmbeddingResponse;
        return try alloc.dupe(f32, parsed.value.embedding.values);
    }

    fn embedGeminiBatch(
        self: *Provider,
        alloc: std.mem.Allocator,
        model: []const u8,
        part_sets: []const []const template_mod.ContentPart,
        dims: u32,
    ) ![]const []const f32 {
        const url = try geminiBatchEndpointUrlAlloc(alloc, self.base_url, model);
        defer alloc.free(url);

        const json_body = try batchEmbedContentRequestBodyAlloc(alloc, model, part_sets, dims);
        defer alloc.free(json_body);

        var resp = try self.http.post(url, .{ .json = json_body, .headers = self.headers });
        defer resp.deinit();

        if (!resp.ok()) return mapEmbedStatus(resp.status.code);
        const body = resp.body orelse return error.EmptyResponse;
        const Response = struct {
            embeddings: []const struct {
                values: []const f32,
            },
        };
        var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.embeddings.len == 0) return error.EmptyEmbeddingResponse;

        const vectors = try alloc.alloc([]const f32, parsed.value.embeddings.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
            alloc.free(vectors);
        }
        for (parsed.value.embeddings, 0..) |embedding, i| {
            if (embedding.values.len == 0) return error.EmptyEmbeddingResponse;
            vectors[i] = try alloc.dupe(f32, embedding.values);
            initialized += 1;
        }
        return vectors;
    }
};

pub fn vertexBaseUrlAlloc(alloc: std.mem.Allocator, location: []const u8) ![]u8 {
    const loc = if (location.len > 0) location else "us-central1";
    return try std.fmt.allocPrint(alloc, "https://{s}-aiplatform.googleapis.com/v1beta1", .{loc});
}

pub fn endpointUrlAlloc(
    alloc: std.mem.Allocator,
    kind: ApiKind,
    base_url: []const u8,
    project_id: []const u8,
    location: []const u8,
    model: []const u8,
) ![]u8 {
    const base = trimRightSlash(base_url);
    const model_name = normalizedModelName(model);
    return switch (kind) {
        .gemini => try std.fmt.allocPrint(alloc, "{s}/models/{s}:embedContent", .{ base, model_name }),
        .vertex => try std.fmt.allocPrint(
            alloc,
            "{s}/projects/{s}/locations/{s}/publishers/google/models/{s}:embedContent",
            .{ base, project_id, location, model_name },
        ),
    };
}

pub fn geminiBatchEndpointUrlAlloc(
    alloc: std.mem.Allocator,
    base_url: []const u8,
    model: []const u8,
) ![]u8 {
    const base = trimRightSlash(base_url);
    return try std.fmt.allocPrint(alloc, "{s}/models/{s}:batchEmbedContents", .{ base, normalizedModelName(model) });
}

fn normalizedModelName(model: []const u8) []const u8 {
    if (std.mem.startsWith(u8, model, "models/")) return model["models/".len..];
    if (std.mem.startsWith(u8, model, "publishers/google/models/")) return model["publishers/google/models/".len..];
    if (std.mem.lastIndexOf(u8, model, "/models/")) |idx| return model[idx + "/models/".len ..];
    return model;
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

pub fn embedContentRequestBodyAlloc(
    alloc: std.mem.Allocator,
    parts: []const template_mod.ContentPart,
    dims: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"content\":");
    try appendContentObject(alloc, &out, parts);
    if (dims > 0) {
        try out.appendSlice(alloc, ",\"embedContentConfig\":{\"outputDimensionality\":");
        const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
        defer alloc.free(dims_json);
        try out.appendSlice(alloc, dims_json);
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');

    return try out.toOwnedSlice(alloc);
}

pub fn batchEmbedContentRequestBodyAlloc(
    alloc: std.mem.Allocator,
    model: []const u8,
    part_sets: []const []const template_mod.ContentPart,
    dims: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"requests\":[");
    for (part_sets, 0..) |parts, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"model\":");
        const model_resource = try std.fmt.allocPrint(alloc, "models/{s}", .{normalizedModelName(model)});
        defer alloc.free(model_resource);
        try appendJsonString(alloc, &out, model_resource);
        try out.appendSlice(alloc, ",\"content\":");
        try appendContentObject(alloc, &out, parts);
        if (dims > 0) {
            try out.appendSlice(alloc, ",\"outputDimensionality\":");
            const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
            defer alloc.free(dims_json);
            try out.appendSlice(alloc, dims_json);
        }
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");

    return try out.toOwnedSlice(alloc);
}

fn appendContentObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    parts: []const template_mod.ContentPart,
) !void {
    try out.appendSlice(alloc, "{\"parts\":[");
    for (parts, 0..) |part, i| {
        if (i > 0) try out.append(alloc, ',');
        switch (part) {
            .text => |text| {
                try out.appendSlice(alloc, "{\"text\":");
                try appendJsonString(alloc, out, text);
                try out.append(alloc, '}');
            },
            .media_url => |url| {
                // The Gemini embedding API accepts inline bytes, not arbitrary URLs.
                // Match the Go implementation by embedding unfetched URLs as text.
                try out.appendSlice(alloc, "{\"text\":");
                try appendJsonString(alloc, out, url);
                try out.append(alloc, '}');
            },
            .binary => |binary| {
                const encoded_len = std.base64.standard.Encoder.calcSize(binary.data.len);
                const encoded = try alloc.alloc(u8, encoded_len);
                defer alloc.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, binary.data);

                try out.appendSlice(alloc, "{\"inlineData\":{\"mimeType\":");
                try appendJsonString(alloc, out, binary.mime_type);
                try out.appendSlice(alloc, ",\"data\":");
                try appendJsonString(alloc, out, encoded);
                try out.appendSlice(alloc, "}}");
            },
        }
    }
    try out.appendSlice(alloc, "]}");
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn mapEmbedStatus(status: u16) anyerror {
    return switch (status) {
        429 => error.EmbedRateLimited,
        408, 502, 503, 504 => error.EmbedTransientFailure,
        else => if (status >= 500 and status <= 599) error.EmbedTransientFailure else error.EmbedRequestFailed,
    };
}

pub fn testRequestBodyPreservesTextAndInlineBinaryParts() !void {
    const alloc = std.testing.allocator;
    const parts = [_]template_mod.ContentPart{
        .{ .text = "A red square" },
        .{ .binary = .{ .mime_type = "image/png", .data = "\x01\x02\x03" } },
    };
    const body = try embedContentRequestBodyAlloc(alloc, &parts, 768);
    defer alloc.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"text\":\"A red square\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"inlineData\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"mimeType\":\"image/png\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"data\":\"AQID\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"outputDimensionality\":768") != null);

    const part_sets = [_][]const template_mod.ContentPart{&parts};
    const batch_body = try batchEmbedContentRequestBodyAlloc(alloc, "gemini-embedding-2", &part_sets, 768);
    defer alloc.free(batch_body);
    try std.testing.expect(std.mem.indexOf(u8, batch_body, "\"requests\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, batch_body, "\"model\":\"models/gemini-embedding-2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, batch_body, "\"inlineData\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, batch_body, "\"outputDimensionality\":768") != null);
}

pub fn testEndpointUrlNormalizesModelResourceNames() !void {
    const alloc = std.testing.allocator;
    const gemini_url = try endpointUrlAlloc(alloc, .gemini, "https://generativelanguage.googleapis.com/v1beta/", "", "", "models/gemini-embedding-001");
    defer alloc.free(gemini_url);
    try std.testing.expectEqualStrings("https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent", gemini_url);
    const gemini_batch_url = try geminiBatchEndpointUrlAlloc(alloc, "https://generativelanguage.googleapis.com/v1beta/", "models/gemini-embedding-001");
    defer alloc.free(gemini_batch_url);
    try std.testing.expectEqualStrings("https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents", gemini_batch_url);

    const gemini_embedding_2_batch_url = try geminiBatchEndpointUrlAlloc(alloc, "https://generativelanguage.googleapis.com/v1beta/", "gemini-embedding-2");
    defer alloc.free(gemini_embedding_2_batch_url);
    try std.testing.expectEqualStrings("https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:batchEmbedContents", gemini_embedding_2_batch_url);

    const vertex_url = try endpointUrlAlloc(alloc, .vertex, "https://us-central1-aiplatform.googleapis.com/v1beta1", "p1", "us-central1", "publishers/google/models/gemini-embedding-001");
    defer alloc.free(vertex_url);
    try std.testing.expectEqualStrings(
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p1/locations/us-central1/publishers/google/models/gemini-embedding-001:embedContent",
        vertex_url,
    );
}

pub fn testProviderEmbedsThroughMockServer() !void {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/v1beta/models/gemini-embedding-001:batchEmbedContents", .respond = .{
            .content_type = "application/json",
            .body = "{\"embeddings\":[{\"values\":[1.0,2.0,3.0]}]}",
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;
    var ok = false;
    var result_err: anyerror = error.None;
    var vector: ?[]f32 = null;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, err_out: *anyerror, vec_out: *?[]f32) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();
            var provider = Provider.init(a, &client, base, .gemini);
            defer provider.deinit();
            const parts = [_]template_mod.ContentPart{.{ .text = "hello" }};
            const v = provider.embedParts(a, "gemini-embedding-001", &parts, 3) catch |err| {
                err_out.* = err;
                return;
            };
            ok_out.* = true;
            vec_out.* = v;
        }
    };

    const base_url = try std.fmt.allocPrint(alloc, "{s}/v1beta", .{ts.baseUrl()});
    defer alloc.free(base_url);

    group.concurrent(io, Fiber.run, .{ alloc, io, base_url, &ok, &result_err, &vector }) catch return;
    try ts.handleOne();
    group.await(io) catch {};

    defer if (vector) |v| alloc.free(v);
    if (!ok) {
        std.debug.print("gemini provider fiber error: {}\n", .{result_err});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, vector.?);
}

test "gemini request body preserves text and inline binary parts" {
    try testRequestBodyPreservesTextAndInlineBinaryParts();
}

test "gemini endpoint URL normalizes model resource names" {
    try testEndpointUrlNormalizesModelResourceNames();
}

test "gemini provider embeds through mock server" {
    try testProviderEmbedsThroughMockServer();
}
