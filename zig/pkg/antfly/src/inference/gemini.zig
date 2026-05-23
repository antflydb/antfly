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

pub const VertexRequestShape = enum {
    embed_content,
    predict,
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

        if (vertexRequestShape(model) == .predict) {
            const part_sets = try alloc.alloc([]const template_mod.ContentPart, texts.len);
            defer alloc.free(part_sets);
            const text_parts = try alloc.alloc(template_mod.ContentPart, texts.len);
            defer alloc.free(text_parts);
            for (texts, 0..) |text, i| {
                text_parts[i] = .{ .text = text };
                part_sets[i] = text_parts[i .. i + 1];
            }
            return try self.embedVertexPredictBatch(alloc, model, part_sets, dims);
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

        if (self.kind == .vertex and vertexRequestShape(model) == .predict) {
            const part_sets = [_][]const template_mod.ContentPart{parts};
            const vectors = try self.embedVertexPredictBatch(alloc, model, &part_sets, dims);
            defer alloc.free(vectors);
            if (vectors.len == 0) return error.EmptyEmbeddingResponse;
            return @constCast(vectors[0]);
        }

        const url = try endpointUrlAlloc(alloc, self.kind, self.base_url, self.project_id, self.location, model);
        defer alloc.free(url);

        const json_body = switch (self.kind) {
            .gemini => try embedContentRequestBodyAlloc(alloc, parts, dims),
            .vertex => try vertexEmbedContentRequestBodyAlloc(alloc, parts, dims),
        };
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

    fn embedVertexPredictBatch(
        self: *Provider,
        alloc: std.mem.Allocator,
        model: []const u8,
        part_sets: []const []const template_mod.ContentPart,
        dims: u32,
    ) ![]const []const f32 {
        const url = try endpointUrlAlloc(alloc, self.kind, self.base_url, self.project_id, self.location, model);
        defer alloc.free(url);

        const json_body = try vertexPredictRequestBodyAlloc(alloc, model, part_sets, dims);
        defer alloc.free(json_body);

        var resp = try self.http.post(url, .{ .json = json_body, .headers = self.headers });
        defer resp.deinit();

        if (!resp.ok()) return mapEmbedStatus(resp.status.code);
        const body = resp.body orelse return error.EmptyResponse;
        const Prediction = struct {
            embeddings: ?struct {
                values: []const f32,
            } = null,
            textEmbedding: ?[]const f32 = null,
            imageEmbedding: ?[]const f32 = null,
            videoEmbeddings: ?[]const struct {
                embedding: []const f32,
            } = null,
        };
        const Response = struct {
            predictions: []const Prediction,
        };
        var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.predictions.len == 0) return error.EmptyEmbeddingResponse;

        const vectors = try alloc.alloc([]const f32, parsed.value.predictions.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
            alloc.free(vectors);
        }
        for (parsed.value.predictions, 0..) |prediction, i| {
            const values = vertexPredictionValues(prediction) orelse return error.EmptyEmbeddingResponse;
            if (values.len == 0) return error.EmptyEmbeddingResponse;
            vectors[i] = try alloc.dupe(f32, values);
            initialized += 1;
        }
        return vectors;
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
    return switch (kind) {
        .gemini => try std.fmt.allocPrint(alloc, "{s}/models/{s}:embedContent", .{ base, normalizedModelName(model) }),
        .vertex => blk: {
            const model_resource = try vertexModelResourceNameAlloc(alloc, project_id, location, model);
            defer alloc.free(model_resource);
            break :blk try std.fmt.allocPrint(
                alloc,
                "{s}/{s}:{s}",
                .{ base, model_resource, vertexMethodName(model) },
            );
        },
    };
}

pub fn vertexModelResourceNameAlloc(
    alloc: std.mem.Allocator,
    project_id: []const u8,
    location: []const u8,
    model: []const u8,
) ![]u8 {
    const resource = try vertexPublisherModelResourceAlloc(alloc, model);
    defer alloc.free(resource);
    if (std.mem.startsWith(u8, resource, "projects/")) return try alloc.dupe(u8, resource);
    return try std.fmt.allocPrint(
        alloc,
        "projects/{s}/locations/{s}/{s}",
        .{ project_id, location, resource },
    );
}

fn vertexPublisherModelResourceAlloc(alloc: std.mem.Allocator, model: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, model, "projects/")) return try alloc.dupe(u8, model);
    if (std.mem.startsWith(u8, model, "publishers/")) return try alloc.dupe(u8, model);
    if (std.mem.startsWith(u8, model, "models/")) return try alloc.dupe(u8, model);
    if (std.mem.indexOf(u8, model, "/")) |idx| {
        return try std.fmt.allocPrint(
            alloc,
            "publishers/{s}/models/{s}",
            .{ model[0..idx], canonicalVertexLeafModelName(model[idx + 1 ..]) },
        );
    }
    return try std.fmt.allocPrint(alloc, "publishers/google/models/{s}", .{canonicalVertexLeafModelName(model)});
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

pub fn vertexRequestShape(model: []const u8) VertexRequestShape {
    const model_name = vertexLeafModelName(model);
    const is_gemini_embed_content = std.mem.indexOf(u8, model_name, "gemini") != null and
        !std.mem.eql(u8, model_name, "gemini-embedding-001");
    const is_maas = std.mem.indexOf(u8, model_name, "maas") != null;
    return if (is_gemini_embed_content or is_maas) .embed_content else .predict;
}

fn vertexMethodName(model: []const u8) []const u8 {
    return switch (vertexRequestShape(model)) {
        .embed_content => "embedContent",
        .predict => "predict",
    };
}

fn vertexLeafModelName(model: []const u8) []const u8 {
    if (std.mem.lastIndexOf(u8, model, "/models/")) |idx| return model[idx + "/models/".len ..];
    if (std.mem.startsWith(u8, model, "models/")) return model["models/".len..];
    if (std.mem.indexOf(u8, model, "/")) |idx| return model[idx + 1 ..];
    return model;
}

fn canonicalVertexLeafModelName(model: []const u8) []const u8 {
    if (std.mem.eql(u8, model, "multimodalembedding")) return "multimodalembedding@001";
    return model;
}

fn isVertexMultimodalEmbeddingModel(model: []const u8) bool {
    return std.mem.startsWith(u8, canonicalVertexLeafModelName(vertexLeafModelName(model)), "multimodalembedding");
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

pub fn vertexEmbedContentRequestBodyAlloc(
    alloc: std.mem.Allocator,
    parts: []const template_mod.ContentPart,
    dims: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"content\":");
    try appendContentObject(alloc, &out, parts);
    if (dims > 0) {
        try out.appendSlice(alloc, ",\"outputDimensionality\":");
        const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
        defer alloc.free(dims_json);
        try out.appendSlice(alloc, dims_json);
    }
    try out.append(alloc, '}');

    return try out.toOwnedSlice(alloc);
}

pub fn vertexPredictRequestBodyAlloc(
    alloc: std.mem.Allocator,
    model: []const u8,
    part_sets: []const []const template_mod.ContentPart,
    dims: u32,
) ![]u8 {
    if (isVertexMultimodalEmbeddingModel(model)) {
        return try vertexMultimodalPredictRequestBodyAlloc(alloc, part_sets, dims);
    }

    return try vertexTextPredictRequestBodyAlloc(alloc, part_sets, dims);
}

pub fn vertexTextPredictRequestBodyAlloc(
    alloc: std.mem.Allocator,
    part_sets: []const []const template_mod.ContentPart,
    dims: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"instances\":[");
    for (part_sets, 0..) |parts, i| {
        if (i > 0) try out.append(alloc, ',');
        const content = try vertexPredictContentTextAlloc(alloc, parts);
        defer alloc.free(content);
        try out.appendSlice(alloc, "{\"content\":");
        try appendJsonString(alloc, &out, content);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
    if (dims > 0) {
        try out.appendSlice(alloc, ",\"parameters\":{\"outputDimensionality\":");
        const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
        defer alloc.free(dims_json);
        try out.appendSlice(alloc, dims_json);
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');

    return try out.toOwnedSlice(alloc);
}

pub fn vertexMultimodalPredictRequestBodyAlloc(
    alloc: std.mem.Allocator,
    part_sets: []const []const template_mod.ContentPart,
    dims: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"instances\":[");
    for (part_sets, 0..) |parts, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendVertexMultimodalInstanceObject(alloc, &out, parts);
    }
    try out.append(alloc, ']');
    if (dims > 0) {
        try out.appendSlice(alloc, ",\"parameters\":{\"dimension\":");
        const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
        defer alloc.free(dims_json);
        try out.appendSlice(alloc, dims_json);
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');

    return try out.toOwnedSlice(alloc);
}

fn vertexPredictContentTextAlloc(
    alloc: std.mem.Allocator,
    parts: []const template_mod.ContentPart,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    var saw_text = false;
    for (parts) |part| {
        switch (part) {
            .text => |text| {
                if (saw_text) try out.append(alloc, ' ');
                try out.appendSlice(alloc, text);
                saw_text = true;
            },
            .media_url => |url| {
                if (saw_text) try out.append(alloc, ' ');
                try out.appendSlice(alloc, url);
                saw_text = true;
            },
            .binary => return error.UnsupportedEmbeddingProvider,
        }
    }
    if (!saw_text) return error.UnsupportedEmbeddingProvider;
    return try out.toOwnedSlice(alloc);
}

fn appendVertexMultimodalInstanceObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    parts: []const template_mod.ContentPart,
) !void {
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(alloc);
    var saw_text = false;
    var saw_media = false;
    var out_fields: usize = 0;

    try out.append(alloc, '{');
    for (parts) |part| {
        switch (part) {
            .text => |value| {
                if (saw_text) try text.append(alloc, ' ');
                try text.appendSlice(alloc, value);
                saw_text = true;
            },
            .media_url => |url| {
                if (saw_text) try text.append(alloc, ' ');
                try text.appendSlice(alloc, url);
                saw_text = true;
            },
            .binary => |binary| {
                if (saw_media) return error.UnsupportedEmbeddingProvider;
                if (out_fields > 0) try out.append(alloc, ',');
                if (std.mem.startsWith(u8, binary.mime_type, "image/")) {
                    try out.appendSlice(alloc, "\"image\":");
                    try appendVertexMediaObject(alloc, out, binary, true);
                } else if (std.mem.startsWith(u8, binary.mime_type, "video/")) {
                    try out.appendSlice(alloc, "\"video\":");
                    try appendVertexMediaObject(alloc, out, binary, false);
                } else {
                    return error.UnsupportedEmbeddingProvider;
                }
                saw_media = true;
                out_fields += 1;
            },
        }
    }
    if (saw_text) {
        if (out_fields > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"text\":");
        try appendJsonString(alloc, out, text.items);
        out_fields += 1;
    }
    if (!saw_text and !saw_media) return error.UnsupportedEmbeddingProvider;
    try out.append(alloc, '}');
}

fn appendVertexMediaObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    binary: template_mod.ContentPart.BinaryContent,
    include_mime_type: bool,
) !void {
    const encoded_len = std.base64.standard.Encoder.calcSize(binary.data.len);
    const encoded = try alloc.alloc(u8, encoded_len);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, binary.data);

    try out.appendSlice(alloc, "{\"bytesBase64Encoded\":");
    try appendJsonString(alloc, out, encoded);
    if (include_mime_type) {
        try out.appendSlice(alloc, ",\"mimeType\":");
        try appendJsonString(alloc, out, binary.mime_type);
    }
    try out.append(alloc, '}');
}

fn vertexPredictionValues(prediction: anytype) ?[]const f32 {
    if (prediction.imageEmbedding) |values| return values;
    if (prediction.videoEmbeddings) |items| {
        if (items.len > 0) return items[0].embedding;
    }
    if (prediction.textEmbedding) |values| return values;
    if (prediction.embeddings) |embedding| return embedding.values;
    return null;
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

    const vertex_embed_content_body = try vertexEmbedContentRequestBodyAlloc(alloc, &parts, 768);
    defer alloc.free(vertex_embed_content_body);
    try std.testing.expect(std.mem.indexOf(u8, vertex_embed_content_body, "\"content\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_embed_content_body, "\"inlineData\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_embed_content_body, "\"outputDimensionality\":768") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_embed_content_body, "\"embedContentConfig\"") == null);

    const text_only_parts = [_]template_mod.ContentPart{.{ .text = "hello vertex" }};
    const text_only_sets = [_][]const template_mod.ContentPart{&text_only_parts};
    const vertex_predict_body = try vertexPredictRequestBodyAlloc(alloc, "gemini-embedding-001", &text_only_sets, 3072);
    defer alloc.free(vertex_predict_body);
    try std.testing.expect(std.mem.indexOf(u8, vertex_predict_body, "\"instances\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_predict_body, "\"content\":\"hello vertex\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_predict_body, "\"parameters\":{\"outputDimensionality\":3072}") != null);

    const vertex_multimodal_body = try vertexPredictRequestBodyAlloc(alloc, "multimodalembedding", &part_sets, 1408);
    defer alloc.free(vertex_multimodal_body);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"instances\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"text\":\"A red square\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"bytesBase64Encoded\":\"AQID\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"mimeType\":\"image/png\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"parameters\":{\"dimension\":1408}") != null);
    try std.testing.expect(std.mem.indexOf(u8, vertex_multimodal_body, "\"outputDimensionality\"") == null);
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
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p1/locations/us-central1/publishers/google/models/gemini-embedding-001:predict",
        vertex_url,
    );

    const vertex_embed_content_url = try endpointUrlAlloc(alloc, .vertex, "https://us-central1-aiplatform.googleapis.com/v1beta1", "p1", "us-central1", "gemini-embedding-2");
    defer alloc.free(vertex_embed_content_url);
    try std.testing.expectEqualStrings(
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p1/locations/us-central1/publishers/google/models/gemini-embedding-2:embedContent",
        vertex_embed_content_url,
    );

    const vertex_multimodal_url = try endpointUrlAlloc(alloc, .vertex, "https://us-central1-aiplatform.googleapis.com/v1beta1", "p1", "us-central1", "multimodalembedding");
    defer alloc.free(vertex_multimodal_url);
    try std.testing.expectEqualStrings(
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p1/locations/us-central1/publishers/google/models/multimodalembedding@001:predict",
        vertex_multimodal_url,
    );

    const vertex_maas_shorthand_url = try endpointUrlAlloc(alloc, .vertex, "https://us-central1-aiplatform.googleapis.com/v1beta1", "p1", "us-central1", "acme/maas-embed");
    defer alloc.free(vertex_maas_shorthand_url);
    try std.testing.expectEqualStrings(
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p1/locations/us-central1/publishers/acme/models/maas-embed:embedContent",
        vertex_maas_shorthand_url,
    );

    const vertex_maas_publisher_url = try endpointUrlAlloc(alloc, .vertex, "https://us-central1-aiplatform.googleapis.com/v1beta1", "p1", "us-central1", "publishers/acme/models/maas-embed");
    defer alloc.free(vertex_maas_publisher_url);
    try std.testing.expectEqualStrings(
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p1/locations/us-central1/publishers/acme/models/maas-embed:embedContent",
        vertex_maas_publisher_url,
    );

    const vertex_maas_full_resource_url = try endpointUrlAlloc(alloc, .vertex, "https://us-central1-aiplatform.googleapis.com/v1beta1", "p1", "us-central1", "projects/p2/locations/europe-west4/publishers/acme/models/maas-embed");
    defer alloc.free(vertex_maas_full_resource_url);
    try std.testing.expectEqualStrings(
        "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p2/locations/europe-west4/publishers/acme/models/maas-embed:embedContent",
        vertex_maas_full_resource_url,
    );

    try std.testing.expectEqual(VertexRequestShape.predict, vertexRequestShape("gemini-embedding-001"));
    try std.testing.expectEqual(VertexRequestShape.predict, vertexRequestShape("multimodalembedding"));
    try std.testing.expectEqual(VertexRequestShape.embed_content, vertexRequestShape("gemini-embedding-2"));
    try std.testing.expectEqual(VertexRequestShape.embed_content, vertexRequestShape("acme/maas-embed"));
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

pub fn testVertexProviderBranchesByModelThroughMockServer() !void {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/v1beta1/projects/p1/locations/us-central1/publishers/google/models/gemini-embedding-001:predict", .respond = .{
            .content_type = "application/json",
            .body = "{\"predictions\":[{\"embeddings\":{\"values\":[1.0,2.0,3.0]}}]}",
        } },
        .{ .method = .POST, .path = "/v1beta1/projects/p1/locations/us-central1/publishers/google/models/gemini-embedding-2:embedContent", .respond = .{
            .content_type = "application/json",
            .body = "{\"embedding\":{\"values\":[4.0,5.0,6.0]}}",
        } },
        .{ .method = .POST, .path = "/v1beta1/projects/p1/locations/us-central1/publishers/google/models/multimodalembedding@001:predict", .respond = .{
            .content_type = "application/json",
            .body = "{\"predictions\":[{\"imageEmbedding\":[7.0,8.0,9.0]}]}",
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;
    var ok = false;
    var result_err: anyerror = error.None;
    var predict_vector: ?[]f32 = null;
    var embed_content_vector: ?[]f32 = null;
    var multimodal_vector: ?[]f32 = null;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, err_out: *anyerror, predict_out: *?[]f32, embed_content_out: *?[]f32, multimodal_out: *?[]f32) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();
            var provider = Provider.init(a, &client, base, .vertex);
            provider.project_id = "p1";
            provider.location = "us-central1";
            defer provider.deinit();

            const parts = [_]template_mod.ContentPart{.{ .text = "hello" }};
            predict_out.* = provider.embedParts(a, "gemini-embedding-001", &parts, 3) catch |err| {
                err_out.* = err;
                return;
            };
            embed_content_out.* = provider.embedParts(a, "gemini-embedding-2", &parts, 3) catch |err| {
                err_out.* = err;
                return;
            };
            const multimodal_parts = [_]template_mod.ContentPart{
                .{ .text = "hello" },
                .{ .binary = .{ .mime_type = "image/png", .data = "\x01\x02\x03" } },
            };
            multimodal_out.* = provider.embedParts(a, "multimodalembedding", &multimodal_parts, 3) catch |err| {
                err_out.* = err;
                return;
            };
            ok_out.* = true;
        }
    };

    const base_url = try std.fmt.allocPrint(alloc, "{s}/v1beta1", .{ts.baseUrl()});
    defer alloc.free(base_url);

    group.concurrent(io, Fiber.run, .{ alloc, io, base_url, &ok, &result_err, &predict_vector, &embed_content_vector, &multimodal_vector }) catch return;
    try ts.handleOne();
    try ts.handleOne();
    try ts.handleOne();
    group.await(io) catch {};

    defer if (predict_vector) |v| alloc.free(v);
    defer if (embed_content_vector) |v| alloc.free(v);
    defer if (multimodal_vector) |v| alloc.free(v);
    if (!ok) {
        std.debug.print("vertex provider fiber error: {}\n", .{result_err});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, predict_vector.?);
    try std.testing.expectEqualSlices(f32, &.{ 4.0, 5.0, 6.0 }, embed_content_vector.?);
    try std.testing.expectEqualSlices(f32, &.{ 7.0, 8.0, 9.0 }, multimodal_vector.?);
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

test "vertex provider branches by model through mock server" {
    try testVertexProviderBranchesByModelThroughMockServer();
}
