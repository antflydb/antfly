// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const httpx = @import("httpx");
const inference = @import("antfly_inference_types");
const inference_work = @import("antfly_inference_work");
const aws = @import("antfly_credentials").aws;
const provider_defaults = @import("antfly_inference_provider_defaults");
const template_mod = @import("antfly_template_content");

const HeaderPair = [2][]const u8;
pub const cohere_max_batch_size = provider_defaults.cohere_max_embedding_batch_size;
const single_input_batch_size: usize = 1;

pub const Credentials = aws.Credentials;
pub const ProfileCredentialSource = aws.ProfileCredentialSource;
pub const WebIdentityCredentialSource = aws.WebIdentityCredentialSource;
pub const CredentialSource = aws.CredentialSource;
pub const CredentialCache = aws.CredentialCache;
pub const testSharedCredentialsProfileParser = aws.testSharedCredentialsProfileParser;
pub const testMetadataCredentialParsers = aws.testMetadataCredentialParsers;
pub const testCredentialUrlEncoding = aws.testCredentialUrlEncoding;
pub const testCredentialFilesUseSuppliedFilesystemAuthority = aws.testCredentialFilesUseSuppliedFilesystemAuthority;
pub const testCredentialSourceKeysAreStructured = aws.testCredentialSourceKeysAreStructured;
const RequestContext = aws.RequestContext;
const resolveCredentialsUncached = aws.resolveCredentialsUncached;
const percentEncodeAlloc = aws.percentEncodeAlloc;
const percentEncodePathSegmentAlloc = aws.percentEncodePathSegmentAlloc;
const currentUnixSeconds = aws.currentUnixSeconds;
const unixSecondsFromTimestamp = aws.unixSecondsFromTimestamp;

pub const RequestFormat = enum {
    auto,
    titan_text,
    titan_multimodal,
    cohere_v3,
    cohere_v4,
};

pub fn parseRequestFormat(value: []const u8) !RequestFormat {
    if (value.len == 0) return .auto;
    return std.meta.stringToEnum(RequestFormat, value) orelse error.InvalidBedrockRequestFormat;
}

/// Resolve the provider-specific JSON contract independently from the Bedrock
/// invocation target. Application profiles and provisioned/custom model ARNs
/// can have opaque names, so callers must configure those explicitly.
pub fn resolveRequestFormat(model: []const u8, configured: RequestFormat) !RequestFormat {
    if (configured != .auto) return configured;

    var identifier = std.mem.trim(u8, model, " \t\r\n");
    if (identifier.len == 0) return error.BedrockRequestFormatRequired;
    // These resources intentionally decouple the invocation target name from
    // the underlying foundation model. Guessing from an operator-chosen name
    // can silently send a valid payload for the wrong provider contract.
    for ([_][]const u8{
        ":application-inference-profile/",
        ":provisioned-model/",
        ":custom-model/",
        ":custom-model-deployment/",
        ":imported-model/",
    }) |opaque_resource| {
        if (std.mem.indexOf(u8, identifier, opaque_resource) != null)
            return error.BedrockRequestFormatRequired;
    }
    if (std.mem.lastIndexOfScalar(u8, identifier, '/')) |separator| {
        identifier = identifier[separator + 1 ..];
    }
    for ([_][]const u8{ "global.", "us.", "eu.", "apac." }) |prefix| {
        if (std.mem.startsWith(u8, identifier, prefix)) {
            identifier = identifier[prefix.len..];
            break;
        }
    }

    if (std.mem.startsWith(u8, identifier, "amazon.titan-embed-image")) return .titan_multimodal;
    if (std.mem.startsWith(u8, identifier, "amazon.titan-embed-text")) return .titan_text;
    if (std.mem.startsWith(u8, identifier, "cohere.embed-v4")) return .cohere_v4;
    if (std.mem.startsWith(u8, identifier, "cohere.embed-")) return .cohere_v3;
    return error.BedrockRequestFormatRequired;
}

pub const Options = struct {
    /// Model invocation admission only; credential resolution is a distinct
    /// upstream operation and must not consume this model's quota.
    attempt_observer: ?httpx.AttemptObserver = null,
    credential_source: CredentialSource = .default,
    region: []const u8,
    endpoint: []const u8,
    request_format: RequestFormat = .auto,
    input_type: []const u8 = "",
    truncate: []const u8 = "",
    dimension: u32 = 0,
    cancellation: ?CancellationToken = null,
    timeout_ms: ?u64 = null,
};

pub const Provider = struct {
    allocator: std.mem.Allocator,
    http: *httpx.Client,
    options: Options,
    owned_credential_cache: CredentialCache = .{},
    credential_cache: ?*CredentialCache = null,

    pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, options: Options) Provider {
        return .{
            .allocator = allocator,
            .http = http,
            .options = options,
        };
    }

    pub fn initWithCredentialCache(allocator: std.mem.Allocator, http: *httpx.Client, options: Options, credential_cache: *CredentialCache) Provider {
        return .{
            .allocator = allocator,
            .http = http,
            .options = options,
            .credential_cache = credential_cache,
        };
    }

    pub fn deinit(self: *Provider) void {
        if (self.credential_cache == null) self.owned_credential_cache.deinit(self.allocator);
    }

    pub fn embedText(self: *Provider, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8) !inference.EmbedResult {
        return try self.embedTextWithContext(
            alloc,
            model,
            texts,
            RequestContext.init(self.http.io, self.options.timeout_ms, self.options.cancellation),
        );
    }

    fn embedTextWithContext(self: *Provider, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8, request_context: RequestContext) !inference.EmbedResult {
        const request_format = try resolveRequestFormat(model, self.options.request_format);
        if (request_format == .cohere_v3 or request_format == .cohere_v4) {
            return try self.embedCohereText(alloc, model, texts, request_format, request_context);
        }

        const vectors = try alloc.alloc([]const f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
            alloc.free(vectors);
        }
        for (texts, 0..) |text, i| {
            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();

            const body = try textEmbeddingBody(arena, request_format, text, self.options.dimension);
            var result = try self.invokeEmbeddingsValue(alloc, model, .{ .object = body }, request_context);
            defer result.deinit();
            if (result.vectors.len == 0) return error.EmptyEmbeddingResponse;
            vectors[i] = try alloc.dupe(f32, result.vectors[0]);
            initialized += 1;
        }
        return .{ .vectors = vectors, .dimension = if (vectors.len > 0) vectors[0].len else 0, .allocator = alloc };
    }

    pub fn embedParts(self: *Provider, alloc: std.mem.Allocator, model: []const u8, parts: []const template_mod.ContentPart) !inference.EmbedResult {
        const request_context = RequestContext.init(self.http.io, self.options.timeout_ms, self.options.cancellation);
        const request_format = try resolveRequestFormat(model, self.options.request_format);
        if (request_format == .titan_multimodal) {
            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            return try self.invokeEmbeddingsValue(alloc, model, .{ .object = try titanMultimodalBody(arena, parts, self.options.dimension) }, request_context);
        }
        if (request_format == .cohere_v4) {
            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            return try self.invokeEmbeddingsValue(alloc, model, .{ .object = try cohereV4Body(arena, parts, self.options.input_type, self.options.truncate, self.options.dimension) }, request_context);
        }
        const flattened = try flattenPartsToText(alloc, parts);
        defer alloc.free(flattened);
        return try self.embedTextWithContext(alloc, model, &.{flattened}, request_context);
    }

    fn invokeEmbeddingsValue(self: *Provider, alloc: std.mem.Allocator, model: []const u8, body_value: std.json.Value, request_context: RequestContext) !inference.EmbedResult {
        const json_body = try httpx.json.Json.stringify(alloc, body_value);
        defer alloc.free(json_body);
        return try self.invokeEmbeddingsJson(alloc, model, json_body, request_context);
    }

    fn invokeEmbeddingsJson(self: *Provider, alloc: std.mem.Allocator, model: []const u8, json_body: []const u8, request_context: RequestContext) !inference.EmbedResult {
        const cache = self.credential_cache orelse &self.owned_credential_cache;
        // Cached snapshots outlive this invocation and may be refreshed by a
        // concurrent request. Never allocate them from the caller's arena.
        var creds = try cache.getForSourceWithContext(
            alloc,
            std.heap.smp_allocator,
            self.http,
            self.options.region,
            self.options.credential_source,
            request_context,
        );
        defer creds.deinit(alloc);

        const endpoint = try endpointBaseAlloc(alloc, self.options.endpoint);
        defer alloc.free(endpoint);
        const path = try bedrockInvokePathAlloc(alloc, model);
        defer alloc.free(path);
        const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ endpoint, path });
        defer alloc.free(url);
        const host = try endpointHostAlloc(alloc, endpoint);
        defer alloc.free(host);

        const signed = try signHeadersAlloc(alloc, creds, self.options.region, host, path, json_body);
        defer freeHeaderPairs(alloc, signed);

        var resp = try self.http.request(.POST, url, .{
            .attempt_observer = self.options.attempt_observer,
            .headers = signed,
            .body = json_body,
            .timeout_ms = try request_context.remainingTimeoutMs(self.http.io),
            .cookies_enabled = false,
            .cancellation = request_context.httpCancellation(),
        });
        defer resp.deinit();
        if (!resp.ok()) return mapStatus(resp.status.code);
        const response_body = resp.body orelse return error.EmptyResponse;
        return try parseEmbeddingResponse(alloc, response_body);
    }

    fn embedCohereText(self: *Provider, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8, request_format: RequestFormat, request_context: RequestContext) !inference.EmbedResult {
        if (texts.len <= cohere_max_batch_size) {
            return try self.embedCohereTextBatch(alloc, model, texts, request_format, request_context);
        }

        var out = std.ArrayListUnmanaged([]const f32).empty;
        errdefer {
            for (out.items) |vector| alloc.free(@constCast(vector));
            out.deinit(alloc);
        }
        var dimension: usize = 0;
        var offset: usize = 0;
        while (offset < texts.len) {
            const end = @min(texts.len, offset + cohere_max_batch_size);
            var result = try self.embedCohereTextBatch(alloc, model, texts[offset..end], request_format, request_context);
            {
                errdefer result.deinit();
                try out.ensureUnusedCapacity(alloc, result.vectors.len);
                for (result.vectors) |vector| out.appendAssumeCapacity(vector);
                if (dimension == 0 and result.dimension > 0) dimension = result.dimension;
                alloc.free(result.vectors);
                result.vectors = &.{};
            }
            offset = end;
        }
        const vectors = try out.toOwnedSlice(alloc);
        return .{ .vectors = vectors, .dimension = dimension, .allocator = alloc };
    }

    fn embedCohereTextBatch(self: *Provider, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8, request_format: RequestFormat, request_context: RequestContext) !inference.EmbedResult {
        var arena_state = std.heap.ArenaAllocator.init(alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var values = std.json.Array.init(arena);
        for (texts) |text| try values.append(.{ .string = text });

        var body = std.json.ObjectMap.empty;
        try body.put(arena, "texts", .{ .array = values });
        try body.put(arena, "input_type", .{ .string = if (self.options.input_type.len > 0) self.options.input_type else "search_document" });
        if (self.options.truncate.len > 0) try body.put(arena, "truncate", .{ .string = self.options.truncate });
        if (self.options.dimension > 0 and request_format == .cohere_v4) try body.put(arena, "output_dimension", .{ .integer = self.options.dimension });
        return try self.invokeEmbeddingsValue(alloc, model, .{ .object = body }, request_context);
    }
};

/// Fetch the Bedrock control-plane foundation-models listing for a region.
/// Returns the raw JSON response body; the caller owns it.
pub fn listFoundationModelsBodyAlloc(alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8, endpoint_override: ?[]const u8, timeout_ms: u64) ![]u8 {
    const request_context = RequestContext.init(http.io, timeout_ms, null);
    var creds = try resolveCredentialsUncached(alloc, http, region, .default, request_context);
    defer creds.deinit(alloc);

    const default_endpoint = try std.fmt.allocPrint(alloc, "https://bedrock.{s}.amazonaws.com", .{region});
    defer alloc.free(default_endpoint);
    const endpoint = try endpointBaseAlloc(alloc, endpoint_override orelse default_endpoint);
    defer alloc.free(endpoint);
    const host = try endpointHostAlloc(alloc, endpoint);
    defer alloc.free(host);
    const path = "/foundation-models";
    const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ endpoint, path });
    defer alloc.free(url);

    const signed = try signRequestHeadersAlloc(alloc, creds, region, "GET", host, path, "");
    defer freeHeaderPairs(alloc, signed);

    var resp = try http.request(.GET, url, .{
        .headers = signed,
        .timeout_ms = try request_context.remainingTimeoutMs(http.io),
        .cookies_enabled = false,
    });
    defer resp.deinit();
    if (!resp.ok()) return mapStatus(resp.status.code);
    const body = resp.body orelse return error.EmptyResponse;
    return try alloc.dupe(u8, body);
}

pub fn maxBatchSize(model: []const u8) usize {
    return requestShape(model).text_inputs_per_request;
}

pub fn maxBatchSizeForFormat(request_format: RequestFormat) usize {
    return requestShapeForFormat(request_format).text_inputs_per_request;
}

pub const RequestShape = struct {
    text_inputs_per_request: usize,
    multimodal_inputs_per_request: usize,
};

pub fn requestShape(model: []const u8) RequestShape {
    const request_format = resolveRequestFormat(model, .auto) catch return requestShapeForFormat(.titan_text);
    return requestShapeForFormat(request_format);
}

pub fn requestShapeForFormat(request_format: RequestFormat) RequestShape {
    if (request_format == .cohere_v3 or request_format == .cohere_v4) {
        return .{
            .text_inputs_per_request = cohere_max_batch_size,
            .multimodal_inputs_per_request = cohere_max_batch_size,
        };
    }
    return .{
        .text_inputs_per_request = single_input_batch_size,
        .multimodal_inputs_per_request = single_input_batch_size,
    };
}

fn titanMultimodalBody(alloc: std.mem.Allocator, parts: []const template_mod.ContentPart, dimension: u32) !std.json.ObjectMap {
    var body = std.json.ObjectMap.empty;
    errdefer body.deinit(alloc);
    var saw_content = false;
    var text_out = std.ArrayListUnmanaged(u8).empty;
    errdefer text_out.deinit(alloc);
    var image_seen = false;
    for (parts) |part| switch (part) {
        .text => |text| {
            try appendTitanInputText(alloc, &text_out, text);
            if (std.mem.trim(u8, text, " \t\r\n").len > 0) saw_content = true;
        },
        .binary => |binary| {
            if (!std.ascii.startsWithIgnoreCase(binary.mime_type, "image/")) return error.UnsupportedMediaType;
            if (image_seen) return error.TooManyImages;
            image_seen = true;
            const encoded_len = std.base64.standard.Encoder.calcSize(binary.data.len);
            const encoded = try alloc.alloc(u8, encoded_len);
            _ = std.base64.standard.Encoder.encode(encoded, binary.data);
            try body.put(alloc, "inputImage", .{ .string = encoded });
            saw_content = true;
        },
        .media_url => |url| {
            const trimmed = std.mem.trim(u8, url, " \t\r\n");
            if (trimmed.len == 0) continue;
            const binary = try bedrockImageDataUri(alloc, trimmed);
            if (image_seen) return error.TooManyImages;
            image_seen = true;
            const encoded_len = std.base64.standard.Encoder.calcSize(binary.data.len);
            const encoded = try alloc.alloc(u8, encoded_len);
            _ = std.base64.standard.Encoder.encode(encoded, binary.data);
            try body.put(alloc, "inputImage", .{ .string = encoded });
            saw_content = true;
        },
    };
    if (text_out.items.len > 0) {
        try body.put(alloc, "inputText", .{ .string = try text_out.toOwnedSlice(alloc) });
    }
    if (!saw_content) return error.EmptyEmbeddingRequest;
    if (dimension > 0) {
        var cfg = std.json.ObjectMap.empty;
        errdefer cfg.deinit(alloc);
        try cfg.put(alloc, "outputEmbeddingLength", .{ .integer = dimension });
        try body.put(alloc, "embeddingConfig", .{ .object = cfg });
    }
    return body;
}

fn textEmbeddingBody(alloc: std.mem.Allocator, request_format: RequestFormat, text: []const u8, dimension: u32) !std.json.ObjectMap {
    if (request_format == .titan_multimodal) {
        return try titanMultimodalBody(alloc, &.{.{ .text = text }}, dimension);
    }

    var body = std.json.ObjectMap.empty;
    errdefer body.deinit(alloc);
    try body.put(alloc, "inputText", .{ .string = text });
    if (dimension > 0) try body.put(alloc, "dimensions", .{ .integer = dimension });
    return body;
}

fn appendTitanInputText(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), raw: []const u8) !void {
    const text = std.mem.trim(u8, raw, " \t\r\n");
    if (text.len == 0) return;
    if (out.items.len > 0) try out.append(alloc, ' ');
    try out.appendSlice(alloc, text);
}

fn cohereV4Body(alloc: std.mem.Allocator, parts: []const template_mod.ContentPart, input_type: []const u8, truncate: []const u8, dimension: u32) !std.json.ObjectMap {
    var content = std.json.Array.init(alloc);
    errdefer content.deinit();
    for (parts) |part| switch (part) {
        .text => |text| if (text.len > 0) {
            var obj = std.json.ObjectMap.empty;
            errdefer obj.deinit(alloc);
            try obj.put(alloc, "type", .{ .string = "text" });
            try obj.put(alloc, "text", .{ .string = text });
            try content.append(.{ .object = obj });
        },
        .binary => |binary| {
            if (!std.ascii.startsWithIgnoreCase(binary.mime_type, "image/")) return error.UnsupportedMediaType;
            const data_uri = try imageDataUriAlloc(alloc, binary.mime_type, binary.data);
            var image_url = std.json.ObjectMap.empty;
            errdefer image_url.deinit(alloc);
            try image_url.put(alloc, "url", .{ .string = data_uri });
            var obj = std.json.ObjectMap.empty;
            errdefer obj.deinit(alloc);
            try obj.put(alloc, "type", .{ .string = "image_url" });
            try obj.put(alloc, "image_url", .{ .object = image_url });
            try content.append(.{ .object = obj });
        },
        .media_url => |url| if (std.mem.trim(u8, url, " \t\r\n").len > 0) {
            const binary = try bedrockImageDataUri(alloc, std.mem.trim(u8, url, " \t\r\n"));
            const data_uri = try imageDataUriAlloc(alloc, binary.mime_type, binary.data);
            var obj = std.json.ObjectMap.empty;
            errdefer obj.deinit(alloc);
            var image_url = std.json.ObjectMap.empty;
            errdefer image_url.deinit(alloc);
            try image_url.put(alloc, "url", .{ .string = data_uri });
            try obj.put(alloc, "type", .{ .string = "image_url" });
            try obj.put(alloc, "image_url", .{ .object = image_url });
            try content.append(.{ .object = obj });
        },
    };
    if (content.items.len == 0) return error.EmptyEmbeddingRequest;

    var input = std.json.ObjectMap.empty;
    errdefer input.deinit(alloc);
    try input.put(alloc, "content", .{ .array = content });

    var inputs = std.json.Array.init(alloc);
    errdefer inputs.deinit();
    try inputs.append(.{ .object = input });

    var embedding_types = std.json.Array.init(alloc);
    errdefer embedding_types.deinit();
    try embedding_types.append(.{ .string = "float" });

    var body = std.json.ObjectMap.empty;
    errdefer body.deinit(alloc);
    try body.put(alloc, "inputs", .{ .array = inputs });
    try body.put(alloc, "embedding_types", .{ .array = embedding_types });
    try body.put(alloc, "input_type", .{ .string = if (input_type.len > 0) input_type else "search_document" });
    if (dimension > 0) try body.put(alloc, "output_dimension", .{ .integer = dimension });
    if (truncate.len > 0) try body.put(alloc, "truncate", .{ .string = truncate });
    return body;
}

fn imageDataUriAlloc(alloc: std.mem.Allocator, mime_type: []const u8, data: []const u8) ![]u8 {
    const encoded_len = std.base64.standard.Encoder.calcSize(data.len);
    const encoded = try alloc.alloc(u8, encoded_len);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, data);
    return try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ mime_type, encoded });
}

fn bedrockImageDataUri(alloc: std.mem.Allocator, url: []const u8) !template_mod.ContentPart.BinaryContent {
    if (!inference_work.hasDataUriScheme(url)) return error.RemoteMediaRequired;
    var decoded = inference_work.decodeInlineDataUriAlloc(alloc, url) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDataURI,
    };
    errdefer decoded.deinit(alloc);
    if (!std.ascii.startsWithIgnoreCase(decoded.mime_type, "image/"))
        return error.UnsupportedMediaType;
    const result = template_mod.ContentPart.BinaryContent{
        .mime_type = decoded.mime_type,
        .data = decoded.data,
    };
    decoded = undefined;
    return result;
}

fn flattenPartsToText(alloc: std.mem.Allocator, parts: []const template_mod.ContentPart) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var saw_text = false;
    for (parts) |part| if (part == .text and part.text.len > 0) {
        if (saw_text) try out.append(alloc, ' ');
        try out.appendSlice(alloc, part.text);
        saw_text = true;
    };
    if (!saw_text) return error.EmptyEmbeddingRequest;
    return try out.toOwnedSlice(alloc);
}

fn parseEmbeddingResponse(alloc: std.mem.Allocator, body: []const u8) !inference.EmbedResult {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidEmbeddingResponse,
    };
    if (root.get("embedding")) |embedding| {
        const vector = try vectorFromJson(alloc, embedding);
        errdefer alloc.free(vector);
        const vectors = try alloc.alloc([]const f32, 1);
        vectors[0] = vector;
        return .{ .vectors = vectors, .dimension = vector.len, .allocator = alloc };
    }
    const embeddings = root.get("embeddings") orelse return error.InvalidEmbeddingResponse;
    const array_value = if (embeddings == .object)
        embeddings.object.get("float") orelse return error.InvalidEmbeddingResponse
    else
        embeddings;
    if (array_value != .array) return error.InvalidEmbeddingResponse;
    const vectors = try alloc.alloc([]const f32, array_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
        alloc.free(vectors);
    }
    for (array_value.array.items, 0..) |item, i| {
        vectors[i] = try vectorFromJson(alloc, item);
        initialized += 1;
    }
    return .{ .vectors = vectors, .dimension = if (vectors.len > 0) vectors[0].len else 0, .allocator = alloc };
}

fn vectorFromJson(alloc: std.mem.Allocator, value: std.json.Value) ![]const f32 {
    if (value != .array) return error.InvalidEmbeddingResponse;
    const vector = try alloc.alloc(f32, value.array.items.len);
    errdefer alloc.free(vector);
    for (value.array.items, 0..) |item, i| {
        vector[i] = switch (item) {
            .float => |v| @floatCast(v),
            .integer => |v| @floatFromInt(v),
            else => return error.InvalidEmbeddingResponse,
        };
    }
    return vector;
}

fn bedrockInvokePathAlloc(alloc: std.mem.Allocator, model: []const u8) ![]u8 {
    const encoded_model = try percentEncodeAlloc(alloc, model);
    defer alloc.free(encoded_model);
    return try std.fmt.allocPrint(alloc, "/model/{s}/invoke", .{encoded_model});
}

fn signHeadersAlloc(alloc: std.mem.Allocator, creds: Credentials, region: []const u8, host: []const u8, path: []const u8, body: []const u8) ![]HeaderPair {
    return try signRequestHeadersAlloc(alloc, creds, region, "POST", host, path, body);
}

fn signRequestHeadersAlloc(alloc: std.mem.Allocator, creds: Credentials, region: []const u8, method: []const u8, host: []const u8, path: []const u8, body: []const u8) ![]HeaderPair {
    const timestamp = try currentUnixSeconds();
    const amz_date = try formatAmzDateAlloc(alloc, timestamp);
    errdefer alloc.free(amz_date);
    const scope_date = try formatScopeDateAlloc(alloc, timestamp);
    defer alloc.free(scope_date);
    const payload_hash = try sha256HexAlloc(alloc, body);
    defer alloc.free(payload_hash);

    var headers = std.ArrayListUnmanaged(HeaderPair).empty;
    errdefer freeHeaderPairs(alloc, headers.items);
    try headers.append(alloc, .{ try alloc.dupe(u8, "accept"), try alloc.dupe(u8, "application/json") });
    if (body.len > 0) try headers.append(alloc, .{ try alloc.dupe(u8, "content-type"), try alloc.dupe(u8, "application/json") });
    try headers.append(alloc, .{ try alloc.dupe(u8, "host"), try alloc.dupe(u8, host) });
    try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-content-sha256"), try alloc.dupe(u8, payload_hash) });
    try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-date"), amz_date });
    if (creds.session_token) |token| try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-security-token"), try alloc.dupe(u8, token) });
    const auth = try authorizationValueAlloc(alloc, creds, region, method, path, headers.items, payload_hash, amz_date, scope_date);
    errdefer alloc.free(auth);
    try headers.append(alloc, .{ try alloc.dupe(u8, "authorization"), auth });
    return try headers.toOwnedSlice(alloc);
}

fn authorizationValueAlloc(alloc: std.mem.Allocator, creds: Credentials, region: []const u8, method: []const u8, path: []const u8, headers: []const HeaderPair, payload_hash: []const u8, amz_date: []const u8, scope_date: []const u8) ![]u8 {
    var canonical_headers = try canonicalHeadersAlloc(alloc, headers);
    defer canonical_headers.deinit(alloc);
    const canonical_path = try canonicalUriPathAlloc(alloc, path);
    defer alloc.free(canonical_path);
    const canonical_request = try std.fmt.allocPrint(alloc, "{s}\n{s}\n\n{s}\n{s}\n{s}", .{ method, canonical_path, canonical_headers.header_block, canonical_headers.signed_headers, payload_hash });
    defer alloc.free(canonical_request);
    const canonical_hash = try sha256HexAlloc(alloc, canonical_request);
    defer alloc.free(canonical_hash);
    const scope = try std.fmt.allocPrint(alloc, "{s}/{s}/bedrock/aws4_request", .{ scope_date, region });
    defer alloc.free(scope);
    const string_to_sign = try std.fmt.allocPrint(alloc, "AWS4-HMAC-SHA256\n{s}\n{s}\n{s}", .{ amz_date, scope, canonical_hash });
    defer alloc.free(string_to_sign);
    const key = try signingKeyAlloc(alloc, creds.secret_access_key, scope_date, region);
    defer alloc.free(key);
    const signature = try hmacSha256HexAlloc(alloc, key, string_to_sign);
    defer alloc.free(signature);
    return try std.fmt.allocPrint(alloc, "AWS4-HMAC-SHA256 Credential={s}/{s}, SignedHeaders={s}, Signature={s}", .{ creds.access_key_id, scope, canonical_headers.signed_headers, signature });
}

fn canonicalUriPathAlloc(alloc: std.mem.Allocator, encoded_path: []const u8) ![]u8 {
    return try percentEncodePathSegmentAlloc(alloc, encoded_path);
}

const CanonicalHeaders = struct {
    header_block: []u8,
    signed_headers: []u8,
    pub fn deinit(self: *CanonicalHeaders, alloc: std.mem.Allocator) void {
        alloc.free(self.header_block);
        alloc.free(self.signed_headers);
        self.* = undefined;
    }
};

fn canonicalHeadersAlloc(alloc: std.mem.Allocator, headers: []const HeaderPair) !CanonicalHeaders {
    const sorted = try alloc.dupe(HeaderPair, headers);
    defer alloc.free(sorted);
    std.mem.sort(HeaderPair, sorted, {}, struct {
        fn lessThan(_: void, a: HeaderPair, b: HeaderPair) bool {
            return std.mem.lessThan(u8, a[0], b[0]);
        }
    }.lessThan);
    var block = std.ArrayListUnmanaged(u8).empty;
    errdefer block.deinit(alloc);
    var names = std.ArrayListUnmanaged(u8).empty;
    errdefer names.deinit(alloc);
    for (sorted, 0..) |pair, i| {
        const line = try std.fmt.allocPrint(alloc, "{s}:{s}\n", .{ pair[0], std.mem.trim(u8, pair[1], " \t\r\n") });
        defer alloc.free(line);
        try block.appendSlice(alloc, line);
        if (i > 0) try names.append(alloc, ';');
        try names.appendSlice(alloc, pair[0]);
    }
    return .{ .header_block = try block.toOwnedSlice(alloc), .signed_headers = try names.toOwnedSlice(alloc) };
}

fn endpointHostAlloc(alloc: std.mem.Allocator, endpoint: []const u8) ![]u8 {
    const parsed = try std.Uri.parse(endpoint);
    const host = parsed.host orelse return error.InvalidEndpoint;
    if (parsed.port) |port| return try std.fmt.allocPrint(alloc, "{s}:{d}", .{ host.percent_encoded, port });
    return try alloc.dupe(u8, host.percent_encoded);
}

const endpointBaseAlloc = aws.endpointBaseAlloc;

pub fn testBedrockSigningClockUsesUnixWallTime() !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const before = try unixSecondsFromTimestamp(std.Io.Timestamp.now(io_impl.io(), .real));
    const actual = try currentUnixSeconds();
    const after = try unixSecondsFromTimestamp(std.Io.Timestamp.now(io_impl.io(), .real));

    try std.testing.expect(actual >= before);
    try std.testing.expect(actual <= after);

    const started = std.Io.Timestamp.fromNanoseconds(10 * std.time.ns_per_ms);
    const bounded = RequestContext.initAt(started, 7, null);
    try std.testing.expectEqual(
        @as(?u64, 2),
        try bounded.remainingTimeoutMsAt(std.Io.Timestamp.fromNanoseconds(15 * std.time.ns_per_ms)),
    );
    try std.testing.expectError(
        error.Timeout,
        bounded.remainingTimeoutMsAt(std.Io.Timestamp.fromNanoseconds(17 * std.time.ns_per_ms)),
    );
    const unbounded = RequestContext.initAt(started, 0, null);
    try std.testing.expectEqual(@as(?u64, null), try unbounded.remainingTimeoutMsAt(started));

    var cancelled = std.atomic.Value(bool).init(true);
    const cancelled_context = RequestContext.initAt(started, 7, CancellationToken.fromAtomic(&cancelled));
    try std.testing.expectError(error.Cancelled, cancelled_context.remainingTimeoutMsAt(started));
}

pub fn testBedrockSigningDatesUseCalendarMonthNumbers() !void {
    const amz_date = try formatAmzDateAlloc(std.testing.allocator, 0);
    defer std.testing.allocator.free(amz_date);
    const scope_date = try formatScopeDateAlloc(std.testing.allocator, 0);
    defer std.testing.allocator.free(scope_date);

    try std.testing.expectEqualStrings("19700101T000000Z", amz_date);
    try std.testing.expectEqualStrings("19700101", scope_date);
}

test "bedrock signing clock uses Unix wall time" {
    try testBedrockSigningClockUsesUnixWallTime();
}

test "bedrock signing dates use calendar month numbers" {
    try testBedrockSigningDatesUseCalendarMonthNumbers();
}

fn formatAmzDateAlloc(alloc: std.mem.Allocator, unix_seconds: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = unix_seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        year_day.year,
        @backingInt(month_day.month),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
}

fn formatScopeDateAlloc(alloc: std.mem.Allocator, unix_seconds: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = unix_seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    return try std.fmt.allocPrint(alloc, "{d:0>4}{d:0>2}{d:0>2}", .{ year_day.year, @backingInt(month_day.month), month_day.day_index + 1 });
}

fn signingKeyAlloc(alloc: std.mem.Allocator, secret: []const u8, scope_date: []const u8, region: []const u8) ![]u8 {
    const k_secret = try std.fmt.allocPrint(alloc, "AWS4{s}", .{secret});
    defer alloc.free(k_secret);
    const k_date = try hmacSha256Alloc(alloc, k_secret, scope_date);
    defer alloc.free(k_date);
    const k_region = try hmacSha256Alloc(alloc, k_date, region);
    defer alloc.free(k_region);
    const k_service = try hmacSha256Alloc(alloc, k_region, "bedrock");
    defer alloc.free(k_service);
    return try hmacSha256Alloc(alloc, k_service, "aws4_request");
}

fn hmacSha256Alloc(alloc: std.mem.Allocator, key: []const u8, data: []const u8) ![]u8 {
    var mac: [std.crypto.auth.hmac.sha2.HmacSha256.mac_length]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(mac[0..], data, key);
    return try alloc.dupe(u8, mac[0..]);
}

fn hmacSha256HexAlloc(alloc: std.mem.Allocator, key: []const u8, data: []const u8) ![]u8 {
    var mac: [std.crypto.auth.hmac.sha2.HmacSha256.mac_length]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(mac[0..], data, key);
    return try bytesToHexAlloc(alloc, mac[0..]);
}

fn sha256HexAlloc(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    return try bytesToHexAlloc(alloc, digest[0..]);
}

fn bytesToHexAlloc(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn freeHeaderPairs(alloc: std.mem.Allocator, headers: []const HeaderPair) void {
    for (headers) |pair| {
        alloc.free(pair[0]);
        alloc.free(pair[1]);
    }
    alloc.free(@constCast(headers));
}

fn mapStatus(status: u16) anyerror {
    return switch (status) {
        429 => error.EmbedRateLimited,
        408, 502, 503, 504 => error.EmbedTransientFailure,
        else => if (status >= 500) error.EmbedTransientFailure else error.EmbedRequestFailed,
    };
}

pub fn testTitanMultimodalBodyOmitsEmptyInputText() !void {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const body = try titanMultimodalBody(arena, &.{.{ .binary = .{ .mime_type = "image/png", .data = "abc" } }}, 384);
    try std.testing.expect(body.get("inputText") == null);
    try std.testing.expect(body.get("inputImage") != null);
    try std.testing.expect(body.get("embeddingConfig") != null);
}

pub fn testTitanMultimodalTextBodyUsesEmbeddingConfig() !void {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const body = try textEmbeddingBody(arena_state.allocator(), .titan_multimodal, "text only", 1024);
    try std.testing.expectEqualStrings("text only", body.get("inputText").?.string);
    try std.testing.expect(body.get("dimensions") == null);
    try std.testing.expectEqual(@as(i64, 1024), body.get("embeddingConfig").?.object.get("outputEmbeddingLength").?.integer);
}

pub fn testTitanMultimodalBodyCombinesTextAndRejectsMultipleImages() !void {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const body = try titanMultimodalBody(arena, &.{
        .{ .text = "first" },
        .{ .binary = .{ .mime_type = "image/png", .data = "abc" } },
        .{ .text = "second" },
    }, 0);
    try std.testing.expectEqualStrings("first second", body.get("inputText").?.string);
    try std.testing.expectError(error.TooManyImages, titanMultimodalBody(arena, &.{
        .{ .binary = .{ .mime_type = "image/png", .data = "abc" } },
        .{ .binary = .{ .mime_type = "image/jpeg", .data = "def" } },
    }, 0));
}

pub fn testTitanMultimodalBodyAcceptsDataUriAndRejectsRemoteUrl() !void {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const body = try titanMultimodalBody(arena, &.{.{ .media_url = "data:image/png;base64,AQID" }}, 0);
    try std.testing.expect(body.get("inputText") == null);
    try std.testing.expectEqualStrings("AQID", body.get("inputImage").?.string);
    try std.testing.expectError(error.RemoteMediaRequired, titanMultimodalBody(arena, &.{.{ .media_url = "https://example.com/image.png" }}, 0));
}

pub fn testCohereV4BodyUsesBedrockImageUrlDataUri() !void {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const body = try cohereV4Body(arena, &.{
        .{ .text = "caption" },
        .{ .binary = .{ .mime_type = "image/png", .data = "abc" } },
    }, "search_document", "RIGHT", 512);
    const inputs = body.get("inputs").?.array.items;
    const content = inputs[0].object.get("content").?.array.items;
    const image_part = content[1].object;
    try std.testing.expectEqualStrings("image_url", image_part.get("type").?.string);
    try std.testing.expectEqualStrings("data:image/png;base64,YWJj", image_part.get("image_url").?.object.get("url").?.string);
    try std.testing.expectEqual(@as(i64, 512), body.get("output_dimension").?.integer);
}

pub fn testCohereV4BodyAcceptsDataUriAndRejectsRemoteUrl() !void {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const body = try cohereV4Body(arena, &.{.{ .media_url = "data:image/png;base64,AQID" }}, "search_document", "", 0);
    const inputs = body.get("inputs").?.array.items;
    const content = inputs[0].object.get("content").?.array.items;
    const image_part = content[0].object;
    try std.testing.expectEqualStrings("image_url", image_part.get("type").?.string);
    try std.testing.expectEqualStrings("data:image/png;base64,AQID", image_part.get("image_url").?.object.get("url").?.string);
    try std.testing.expectError(error.RemoteMediaRequired, cohereV4Body(arena, &.{.{ .media_url = "https://example.com/image.png" }}, "search_document", "", 0));
}

pub fn testRequestShapeBatchesByProviderRequest() !void {
    try std.testing.expectEqual(@as(usize, cohere_max_batch_size), maxBatchSize("cohere.embed-v4:0"));
    try std.testing.expectEqual(@as(usize, cohere_max_batch_size), maxBatchSize("cohere.embed-english-v3"));
    try std.testing.expectEqual(@as(usize, single_input_batch_size), maxBatchSize("amazon.titan-embed-text-v2:0"));
    try std.testing.expectEqual(@as(usize, single_input_batch_size), maxBatchSize("amazon.titan-embed-image-v1"));
    try std.testing.expectEqual(@as(usize, cohere_max_batch_size), maxBatchSize("us.cohere.embed-v4:0"));
}

pub fn testBedrockRequestFormatResolution() !void {
    try std.testing.expectEqual(RequestFormat.titan_text, try resolveRequestFormat("amazon.titan-embed-text-v2:0", .auto));
    try std.testing.expectEqual(RequestFormat.titan_multimodal, try resolveRequestFormat("us.amazon.titan-embed-image-v1:0", .auto));
    try std.testing.expectEqual(RequestFormat.cohere_v4, try resolveRequestFormat(
        "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.cohere.embed-v4:0",
        .auto,
    ));
    try std.testing.expectEqual(RequestFormat.titan_text, try resolveRequestFormat(
        "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0",
        .auto,
    ));
    try std.testing.expectEqual(RequestFormat.titan_multimodal, try resolveRequestFormat(
        "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/team-embeddings",
        .titan_multimodal,
    ));
    try std.testing.expectError(
        error.BedrockRequestFormatRequired,
        resolveRequestFormat("arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/team-embeddings", .auto),
    );
    try std.testing.expectError(
        error.BedrockRequestFormatRequired,
        resolveRequestFormat("arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/us.amazon.titan-embed-image-v1:0", .auto),
    );
    try std.testing.expectError(error.InvalidBedrockRequestFormat, parseRequestFormat("future_format"));
}

pub fn testBedrockInvokePathEscapesModelId() !void {
    const alloc = std.testing.allocator;
    const titan = try bedrockInvokePathAlloc(alloc, "amazon.titan-embed-text-v2:0");
    defer alloc.free(titan);
    try std.testing.expectEqualStrings("/model/amazon.titan-embed-text-v2%3A0/invoke", titan);

    const arn = try bedrockInvokePathAlloc(alloc, "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.amazon.titan-embed-text-v2:0");
    defer alloc.free(arn);
    try std.testing.expectEqualStrings("/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Ainference-profile%2Fus.amazon.titan-embed-text-v2%3A0/invoke", arn);
}

pub fn testBedrockCanonicalUriDoubleEncodesEscapedModelId() !void {
    const alloc = std.testing.allocator;
    const path = try bedrockInvokePathAlloc(alloc, "amazon.titan-embed-text-v2:0");
    defer alloc.free(path);
    const canonical_path = try canonicalUriPathAlloc(alloc, path);
    defer alloc.free(canonical_path);
    try std.testing.expectEqualStrings("/model/amazon.titan-embed-text-v2%253A0/invoke", canonical_path);
}

pub fn testBedrockSignerUsesBedrockServiceScope() !void {
    const alloc = std.testing.allocator;
    const creds = Credentials{ .access_key_id = "AKIA", .secret_access_key = "secret" };
    const headers = [_]HeaderPair{
        .{ "host", "bedrock-runtime.us-east-1.amazonaws.com" },
        .{ "x-amz-date", "20260102T030405Z" },
        .{ "x-amz-content-sha256", "hash" },
    };
    const auth = try authorizationValueAlloc(alloc, creds, "us-east-1", "POST", "/model/amazon.titan-embed-image-v1/invoke", &headers, "hash", "20260102T030405Z", "20260102");
    defer alloc.free(auth);
    try std.testing.expect(std.mem.indexOf(u8, auth, "/bedrock/aws4_request") != null);
}

pub fn testBedrockSignerSignsGetRequests() !void {
    const alloc = std.testing.allocator;
    const creds = Credentials{ .access_key_id = "AKIA", .secret_access_key = "secret" };
    const headers = [_]HeaderPair{
        .{ "host", "bedrock.us-east-1.amazonaws.com" },
        .{ "x-amz-date", "20260102T030405Z" },
        .{ "x-amz-content-sha256", "hash" },
    };
    const get_auth = try authorizationValueAlloc(alloc, creds, "us-east-1", "GET", "/foundation-models", &headers, "hash", "20260102T030405Z", "20260102");
    defer alloc.free(get_auth);
    try std.testing.expect(std.mem.indexOf(u8, get_auth, "/bedrock/aws4_request") != null);
    try std.testing.expect(std.mem.indexOf(u8, get_auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") != null);

    // The method participates in the canonical request, so GET and POST
    // signatures over otherwise identical inputs must differ.
    const post_auth = try authorizationValueAlloc(alloc, creds, "us-east-1", "POST", "/foundation-models", &headers, "hash", "20260102T030405Z", "20260102");
    defer alloc.free(post_auth);
    try std.testing.expect(!std.mem.eql(u8, get_auth, post_auth));

    // GET requests sign an empty payload and omit content-type.
    const signed = try signRequestHeadersAlloc(alloc, creds, "us-east-1", "GET", "bedrock.us-east-1.amazonaws.com", "/foundation-models", "");
    defer freeHeaderPairs(alloc, signed);
    for (signed) |pair| {
        try std.testing.expect(!std.mem.eql(u8, pair[0], "content-type"));
        if (std.mem.eql(u8, pair[0], "x-amz-content-sha256")) {
            // SHA-256 of the empty string.
            try std.testing.expectEqualStrings("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", pair[1]);
        }
    }
}

pub fn testEndpointHostIncludesExplicitPort() !void {
    const alloc = std.testing.allocator;
    const host = try endpointHostAlloc(alloc, "http://localhost:4566");
    defer alloc.free(host);
    try std.testing.expectEqualStrings("localhost:4566", host);

    const endpoint = try endpointBaseAlloc(alloc, "http://localhost:4566/");
    defer alloc.free(endpoint);
    try std.testing.expectEqualStrings("http://localhost:4566", endpoint);
}

test "titan multimodal body omits empty inputText" {
    try testTitanMultimodalBodyOmitsEmptyInputText();
}

test "cohere v4 body uses bedrock image_url data uri" {
    try testCohereV4BodyUsesBedrockImageUrlDataUri();
}

test "titan multimodal body accepts data URI and rejects remote URL" {
    try testTitanMultimodalBodyAcceptsDataUriAndRejectsRemoteUrl();
}

test "cohere v4 body accepts data URI and rejects remote URL" {
    try testCohereV4BodyAcceptsDataUriAndRejectsRemoteUrl();
}

test "bedrock image media uses shared RFC 2397 decoding" {
    const alloc = std.testing.allocator;
    const binary = try bedrockImageDataUri(alloc, "DATA:IMAGE/PNG,%01%02");
    defer {
        alloc.free(@constCast(binary.mime_type));
        alloc.free(@constCast(binary.data));
    }
    try std.testing.expectEqualStrings("IMAGE/PNG", binary.mime_type);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, binary.data);
}

test "titan multimodal body combines text and rejects multiple images" {
    try testTitanMultimodalBodyCombinesTextAndRejectsMultipleImages();
}

test "request shape batches by provider request" {
    try testRequestShapeBatchesByProviderRequest();
}

test "bedrock invoke path escapes model id" {
    try testBedrockInvokePathEscapesModelId();
}

test "bedrock canonical uri double encodes escaped model id" {
    try testBedrockCanonicalUriDoubleEncodesEscapedModelId();
}

test "bedrock signer uses bedrock service scope" {
    try testBedrockSignerUsesBedrockServiceScope();
}

test "bedrock signer signs get requests" {
    try testBedrockSignerSignsGetRequests();
}

test "endpoint host includes explicit port" {
    try testEndpointHostIncludesExplicitPort();
}
