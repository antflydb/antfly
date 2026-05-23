// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const inference = @import("types.zig");
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test)
    @import("../storage/db/template_stub.zig")
else
    @import("../template.zig");

const HeaderPair = [2][]const u8;
pub const cohere_max_batch_size: usize = 96;
const single_input_batch_size: usize = 1;
const imds_default_endpoint = "http://169.254.169.254";
const ecs_credentials_endpoint = "http://169.254.170.2";

const Credentials = struct {
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8 = null,
    expires_at_unix: ?u64 = null,

    fn deinit(self: *Credentials, alloc: std.mem.Allocator) void {
        alloc.free(self.access_key_id);
        alloc.free(self.secret_access_key);
        if (self.session_token) |value| alloc.free(value);
        self.* = undefined;
    }

    fn clone(self: Credentials, alloc: std.mem.Allocator) !Credentials {
        return try dupCredentials(alloc, self.access_key_id, self.secret_access_key, self.session_token, self.expires_at_unix);
    }

    fn isFresh(self: Credentials, now_unix: u64) bool {
        const refresh_skew_seconds: u64 = 300;
        return if (self.expires_at_unix) |expires|
            expires > now_unix + refresh_skew_seconds
        else
            true;
    }
};

pub const CredentialCache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    cached: ?Credentials = null,

    fn lock(self: *CredentialCache) void {
        while (!self.mutex.tryLock()) {
            if (comptime builtin.os.tag == .freestanding) {
                std.atomic.spinLoopHint();
                continue;
            }
            std.Thread.yield() catch {};
        }
    }

    fn unlock(self: *CredentialCache) void {
        self.mutex.unlock();
    }

    pub fn deinit(self: *CredentialCache, alloc: std.mem.Allocator) void {
        self.lock();
        defer self.unlock();
        if (self.cached) |*creds| creds.deinit(alloc);
        self.cached = null;
    }

    fn get(self: *CredentialCache, alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8) !Credentials {
        const now = currentUnixSeconds();
        self.lock();
        if (self.cached) |creds| {
            if (creds.isFresh(now)) {
                const cloned = creds.clone(alloc) catch |err| {
                    self.unlock();
                    return err;
                };
                self.unlock();
                return cloned;
            }
        }
        self.unlock();

        var fresh = try resolveCredentialsUncached(alloc, http, region);
        errdefer fresh.deinit(alloc);
        const cached_copy = try fresh.clone(alloc);
        errdefer {
            var copy = cached_copy;
            copy.deinit(alloc);
        }

        self.lock();
        defer self.unlock();
        if (self.cached) |*old| old.deinit(alloc);
        self.cached = cached_copy;
        return fresh;
    }
};

pub const Options = struct {
    region: []const u8,
    endpoint: []const u8,
    input_type: []const u8 = "",
    truncate: []const u8 = "",
    dimension: u32 = 0,
};

pub const Provider = struct {
    allocator: std.mem.Allocator,
    http: *httpx.Client,
    options: Options,
    owned_credential_cache: CredentialCache = .{},
    credential_cache: ?*CredentialCache = null,

    pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, options: Options) Provider {
        return .{ .allocator = allocator, .http = http, .options = options };
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
        if (std.mem.startsWith(u8, model, "cohere.embed-")) {
            return try self.embedCohereText(alloc, model, texts);
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

            var body = std.json.ObjectMap.empty;
            try body.put(arena, "inputText", .{ .string = text });
            if (self.options.dimension > 0) try body.put(arena, "dimensions", .{ .integer = self.options.dimension });
            var result = try self.invokeEmbeddingsValue(alloc, model, .{ .object = body });
            defer result.deinit();
            if (result.vectors.len == 0) return error.EmptyEmbeddingResponse;
            vectors[i] = try alloc.dupe(f32, result.vectors[0]);
            initialized += 1;
        }
        return .{ .vectors = vectors, .dimension = if (vectors.len > 0) vectors[0].len else 0, .allocator = alloc };
    }

    pub fn embedParts(self: *Provider, alloc: std.mem.Allocator, model: []const u8, parts: []const template_mod.ContentPart) !inference.EmbedResult {
        if (std.mem.startsWith(u8, model, "amazon.titan-embed-image")) {
            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            return try self.invokeEmbeddingsValue(alloc, model, .{ .object = try titanMultimodalBody(arena, parts, self.options.dimension) });
        }
        if (isCohereV4(model)) {
            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            return try self.invokeEmbeddingsValue(alloc, model, .{ .object = try cohereV4Body(arena, parts, self.options.input_type, self.options.truncate, self.options.dimension) });
        }
        const flattened = try flattenPartsToText(alloc, parts);
        defer alloc.free(flattened);
        return try self.embedText(alloc, model, &.{flattened});
    }

    fn invokeEmbeddingsValue(self: *Provider, alloc: std.mem.Allocator, model: []const u8, body_value: std.json.Value) !inference.EmbedResult {
        const json_body = try httpx.json.Json.stringify(alloc, body_value);
        defer alloc.free(json_body);
        return try self.invokeEmbeddingsJson(alloc, model, json_body);
    }

    fn invokeEmbeddingsJson(self: *Provider, alloc: std.mem.Allocator, model: []const u8, json_body: []const u8) !inference.EmbedResult {
        const cache = self.credential_cache orelse &self.owned_credential_cache;
        var creds = try cache.get(alloc, self.http, self.options.region);
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

        var resp = try self.http.request(.POST, url, .{ .headers = signed, .body = json_body });
        defer resp.deinit();
        if (!resp.ok()) return mapStatus(resp.status.code);
        const response_body = resp.body orelse return error.EmptyResponse;
        return try parseEmbeddingResponse(alloc, response_body);
    }

    fn embedCohereText(self: *Provider, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8) !inference.EmbedResult {
        if (texts.len <= cohere_max_batch_size) {
            return try self.embedCohereTextBatch(alloc, model, texts);
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
            var result = try self.embedCohereTextBatch(alloc, model, texts[offset..end]);
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

    fn embedCohereTextBatch(self: *Provider, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8) !inference.EmbedResult {
        var arena_state = std.heap.ArenaAllocator.init(alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var values = std.json.Array.init(arena);
        for (texts) |text| try values.append(.{ .string = text });

        var body = std.json.ObjectMap.empty;
        try body.put(arena, "texts", .{ .array = values });
        try body.put(arena, "input_type", .{ .string = if (self.options.input_type.len > 0) self.options.input_type else "search_document" });
        if (self.options.truncate.len > 0) try body.put(arena, "truncate", .{ .string = self.options.truncate });
        if (self.options.dimension > 0 and isCohereV4(model)) try body.put(arena, "output_dimension", .{ .integer = self.options.dimension });
        return try self.invokeEmbeddingsValue(alloc, model, .{ .object = body });
    }
};

fn isCohereV4(model: []const u8) bool {
    return std.mem.startsWith(u8, model, "cohere.embed-v4");
}

pub fn maxBatchSize(model: []const u8) usize {
    return requestShape(model).text_inputs_per_request;
}

pub const RequestShape = struct {
    text_inputs_per_request: usize,
    multimodal_inputs_per_request: usize,
};

pub fn requestShape(model: []const u8) RequestShape {
    if (std.mem.startsWith(u8, model, "cohere.embed-")) {
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
            if (!std.mem.startsWith(u8, binary.mime_type, "image/")) return error.UnsupportedMediaType;
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
            if (!std.mem.startsWith(u8, binary.mime_type, "image/")) return error.UnsupportedMediaType;
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
    if (!std.mem.startsWith(u8, url, "data:")) return error.RemoteMediaRequired;
    const after_data = url[5..];
    const base64_marker = ";base64,";
    const sep_idx = std.mem.indexOf(u8, after_data, base64_marker) orelse return error.InvalidDataURI;
    const mime_type_slice = after_data[0..sep_idx];
    if (!std.mem.startsWith(u8, mime_type_slice, "image/")) return error.UnsupportedMediaType;

    const mime_type = try alloc.dupe(u8, mime_type_slice);
    errdefer alloc.free(mime_type);

    const encoded = after_data[sep_idx + base64_marker.len ..];
    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidDataURI;
    const data = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(data);
    std.base64.standard.Decoder.decode(data, encoded) catch return error.InvalidDataURI;

    return .{ .mime_type = mime_type, .data = data };
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

fn resolveCredentialsUncached(alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8) !Credentials {
    if (getEnvOwned(alloc, "AWS_ACCESS_KEY_ID")) |access| {
        errdefer alloc.free(access);
        const secret = getEnvOwned(alloc, "AWS_SECRET_ACCESS_KEY") orelse return error.MissingSecretAccessKey;
        errdefer alloc.free(secret);
        const token = getEnvOwned(alloc, "AWS_SESSION_TOKEN");
        return .{ .access_key_id = access, .secret_access_key = secret, .session_token = token };
    }
    if (credentialsFromWebIdentity(alloc, http, region)) |creds| return creds else |_| {}
    if (credentialsFromSharedFiles(alloc)) |creds| return creds else |_| {}
    if (credentialsFromEcsMetadata(alloc, http)) |creds| return creds else |_| {}
    if (credentialsFromInstanceMetadata(alloc, http)) |creds| return creds else |_| {}
    return error.MissingAwsCredentials;
}

fn credentialsFromSharedFiles(alloc: std.mem.Allocator) !Credentials {
    const profile = getEnvOwned(alloc, "AWS_PROFILE") orelse try alloc.dupe(u8, "default");
    defer alloc.free(profile);
    const path = getEnvOwned(alloc, "AWS_SHARED_CREDENTIALS_FILE") orelse blk: {
        const home = getEnvOwned(alloc, "HOME") orelse return error.MissingAwsCredentials;
        defer alloc.free(home);
        break :blk try std.fmt.allocPrint(alloc, "{s}/.aws/credentials", .{home});
    };
    defer alloc.free(path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const data = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(1 << 20)) catch return error.MissingAwsCredentials;
    defer alloc.free(data);
    return try parseProfileCredentials(alloc, data, profile);
}

fn parseProfileCredentials(alloc: std.mem.Allocator, data: []const u8, profile: []const u8) !Credentials {
    var in_profile = false;
    var access: ?[]u8 = null;
    var secret: ?[]u8 = null;
    var token: ?[]u8 = null;
    errdefer {
        if (access) |v| alloc.free(v);
        if (secret) |v| alloc.free(v);
        if (token) |v| alloc.free(v);
    }
    var lines = std.mem.splitScalar(u8, data, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0 or line[0] == '#') continue;
        if (line[0] == '[' and line[line.len - 1] == ']') {
            var section = std.mem.trim(u8, line[1 .. line.len - 1], " \t");
            if (std.mem.startsWith(u8, section, "profile ")) section = std.mem.trim(u8, section["profile ".len..], " \t");
            in_profile = std.mem.eql(u8, section, profile);
            continue;
        }
        if (!in_profile) continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const value = std.mem.trim(u8, line[eq + 1 ..], " \t");
        if (std.mem.eql(u8, key, "aws_access_key_id")) access = try alloc.dupe(u8, value);
        if (std.mem.eql(u8, key, "aws_secret_access_key")) secret = try alloc.dupe(u8, value);
        if (std.mem.eql(u8, key, "aws_session_token")) token = try alloc.dupe(u8, value);
    }
    return .{
        .access_key_id = access orelse return error.MissingAccessKeyId,
        .secret_access_key = secret orelse return error.MissingSecretAccessKey,
        .session_token = token,
    };
}

fn credentialsFromWebIdentity(alloc: std.mem.Allocator, http: *httpx.Client, region: []const u8) !Credentials {
    const role_arn = getEnvOwned(alloc, "AWS_ROLE_ARN") orelse return error.MissingAwsCredentials;
    defer alloc.free(role_arn);
    const token_file = getEnvOwned(alloc, "AWS_WEB_IDENTITY_TOKEN_FILE") orelse return error.MissingAwsCredentials;
    defer alloc.free(token_file);
    const session_name = getEnvOwned(alloc, "AWS_ROLE_SESSION_NAME") orelse try alloc.dupe(u8, "antfly-bedrock");
    defer alloc.free(session_name);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const token = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), token_file, alloc, .limited(1 << 20)) catch return error.MissingAwsCredentials;
    defer alloc.free(token);

    const sts_endpoint = if (getEnvOwned(alloc, "AWS_STS_ENDPOINT")) |endpoint| endpoint else try std.fmt.allocPrint(alloc, "https://sts.{s}.amazonaws.com", .{region});
    defer alloc.free(sts_endpoint);
    const encoded_role = try percentEncodeAlloc(alloc, role_arn);
    defer alloc.free(encoded_role);
    const encoded_session = try percentEncodeAlloc(alloc, session_name);
    defer alloc.free(encoded_session);
    const encoded_token = try percentEncodeAlloc(alloc, std.mem.trim(u8, token, " \t\r\n"));
    defer alloc.free(encoded_token);
    const url = try std.fmt.allocPrint(alloc, "{s}/?Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={s}&RoleSessionName={s}&WebIdentityToken={s}", .{
        sts_endpoint,
        encoded_role,
        encoded_session,
        encoded_token,
    });
    defer alloc.free(url);

    var resp = http.request(.GET, url, .{}) catch return error.MissingAwsCredentials;
    defer resp.deinit();
    if (!resp.ok()) return error.MissingAwsCredentials;
    const body = resp.body orelse return error.MissingAwsCredentials;
    return try parseStsCredentials(alloc, body);
}

fn credentialsFromEcsMetadata(alloc: std.mem.Allocator, http: *httpx.Client) !Credentials {
    const full_uri = getEnvOwned(alloc, "AWS_CONTAINER_CREDENTIALS_FULL_URI");
    const relative_uri = if (full_uri == null) getEnvOwned(alloc, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") else null;
    defer if (full_uri) |value| alloc.free(value);
    defer if (relative_uri) |value| alloc.free(value);

    const url = if (full_uri) |value|
        try alloc.dupe(u8, value)
    else if (relative_uri) |value|
        try std.fmt.allocPrint(alloc, "{s}{s}", .{ ecs_credentials_endpoint, value })
    else
        return error.MissingAwsCredentials;
    defer alloc.free(url);

    const auth_token = containerAuthorizationToken(alloc);
    defer if (auth_token) |value| alloc.free(value);
    const headers = if (auth_token) |token| &[_]HeaderPair{.{ "authorization", token }} else &[_]HeaderPair{};
    var resp = http.request(.GET, url, .{ .headers = headers }) catch return error.MissingAwsCredentials;
    defer resp.deinit();
    if (!resp.ok()) return error.MissingAwsCredentials;
    const body = resp.body orelse return error.MissingAwsCredentials;
    return try parseMetadataCredentials(alloc, body);
}

fn credentialsFromInstanceMetadata(alloc: std.mem.Allocator, http: *httpx.Client) !Credentials {
    if (getEnvOwned(alloc, "AWS_EC2_METADATA_DISABLED")) |disabled| {
        defer alloc.free(disabled);
        if (std.ascii.eqlIgnoreCase(disabled, "true")) return error.MissingAwsCredentials;
    }
    const endpoint = getEnvOwned(alloc, "AWS_EC2_METADATA_SERVICE_ENDPOINT") orelse try alloc.dupe(u8, imds_default_endpoint);
    defer alloc.free(endpoint);
    const base = try endpointBaseAlloc(alloc, endpoint);
    defer alloc.free(base);

    const token = imdsToken(alloc, http, base) catch null;
    defer if (token) |value| alloc.free(value);
    if (token == null) {
        if (getEnvOwned(alloc, "AWS_EC2_METADATA_V1_DISABLED")) |disabled| {
            defer alloc.free(disabled);
            if (std.ascii.eqlIgnoreCase(disabled, "true")) return error.MissingAwsCredentials;
        }
    }
    const headers = if (token) |value| &[_]HeaderPair{.{ "x-aws-ec2-metadata-token", value }} else &[_]HeaderPair{};

    const role_url = try std.fmt.allocPrint(alloc, "{s}/latest/meta-data/iam/security-credentials/", .{base});
    defer alloc.free(role_url);
    var role_resp = http.request(.GET, role_url, .{ .headers = headers }) catch return error.MissingAwsCredentials;
    defer role_resp.deinit();
    if (!role_resp.ok()) return error.MissingAwsCredentials;
    const role_body = role_resp.body orelse return error.MissingAwsCredentials;
    const role_name = std.mem.trim(u8, role_body, " \t\r\n");
    if (role_name.len == 0) return error.MissingAwsCredentials;

    const encoded_role = try percentEncodePathSegmentAlloc(alloc, role_name);
    defer alloc.free(encoded_role);
    const creds_url = try std.fmt.allocPrint(alloc, "{s}/latest/meta-data/iam/security-credentials/{s}", .{ base, encoded_role });
    defer alloc.free(creds_url);
    var creds_resp = http.request(.GET, creds_url, .{ .headers = headers }) catch return error.MissingAwsCredentials;
    defer creds_resp.deinit();
    if (!creds_resp.ok()) return error.MissingAwsCredentials;
    const body = creds_resp.body orelse return error.MissingAwsCredentials;
    return try parseMetadataCredentials(alloc, body);
}

fn imdsToken(alloc: std.mem.Allocator, http: *httpx.Client, endpoint: []const u8) ![]u8 {
    const url = try std.fmt.allocPrint(alloc, "{s}/latest/api/token", .{endpoint});
    defer alloc.free(url);
    const headers = [_]HeaderPair{.{ "x-aws-ec2-metadata-token-ttl-seconds", "21600" }};
    var resp = try http.request(.PUT, url, .{ .headers = &headers, .body = "" });
    defer resp.deinit();
    if (!resp.ok()) return error.MissingAwsCredentials;
    const body = resp.body orelse return error.MissingAwsCredentials;
    return try alloc.dupe(u8, std.mem.trim(u8, body, " \t\r\n"));
}

fn containerAuthorizationToken(alloc: std.mem.Allocator) ?[]u8 {
    if (getEnvOwned(alloc, "AWS_CONTAINER_AUTHORIZATION_TOKEN")) |token| return token;
    const token_file = getEnvOwned(alloc, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") orelse return null;
    defer alloc.free(token_file);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), token_file, alloc, .limited(1 << 20)) catch return null;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (trimmed.len == raw.len) return raw;
    const out = alloc.dupe(u8, trimmed) catch {
        alloc.free(raw);
        return null;
    };
    alloc.free(raw);
    return out;
}

fn parseMetadataCredentials(alloc: std.mem.Allocator, body: []const u8) !Credentials {
    const MetadataCredentials = struct {
        AccessKeyId: []const u8,
        SecretAccessKey: []const u8,
        Token: ?[]const u8 = null,
        Expiration: ?[]const u8 = null,
    };
    var parsed = try std.json.parseFromSlice(MetadataCredentials, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const expires_at = if (parsed.value.Expiration) |value| parseAwsIso8601(value) catch null else null;
    return try dupCredentials(alloc, parsed.value.AccessKeyId, parsed.value.SecretAccessKey, parsed.value.Token, expires_at);
}

fn parseStsCredentials(alloc: std.mem.Allocator, body: []const u8) !Credentials {
    const access = extractXmlTag(body, "AccessKeyId") orelse return error.MissingAccessKeyId;
    const secret = extractXmlTag(body, "SecretAccessKey") orelse return error.MissingSecretAccessKey;
    const token = extractXmlTag(body, "SessionToken");
    const expires_at = if (extractXmlTag(body, "Expiration")) |value| parseAwsIso8601(value) catch null else null;
    return try dupCredentials(alloc, access, secret, token, expires_at);
}

fn dupCredentials(alloc: std.mem.Allocator, access: []const u8, secret: []const u8, token: ?[]const u8, expires_at_unix: ?u64) !Credentials {
    const access_copy = try alloc.dupe(u8, access);
    errdefer alloc.free(access_copy);
    const secret_copy = try alloc.dupe(u8, secret);
    errdefer alloc.free(secret_copy);
    const token_copy = if (token) |value| try alloc.dupe(u8, value) else null;
    errdefer if (token_copy) |value| alloc.free(value);
    return .{ .access_key_id = access_copy, .secret_access_key = secret_copy, .session_token = token_copy, .expires_at_unix = expires_at_unix };
}

fn extractXmlTag(body: []const u8, tag: []const u8) ?[]const u8 {
    var open_buf: [64]u8 = undefined;
    var close_buf: [67]u8 = undefined;
    if (tag.len + 2 > open_buf.len or tag.len + 3 > close_buf.len) return null;
    const open = std.fmt.bufPrint(open_buf[0..], "<{s}>", .{tag}) catch return null;
    const close = std.fmt.bufPrint(close_buf[0..], "</{s}>", .{tag}) catch return null;
    const start = std.mem.indexOf(u8, body, open) orelse return null;
    const value_start = start + open.len;
    const end_rel = std.mem.indexOf(u8, body[value_start..], close) orelse return null;
    return body[value_start .. value_start + end_rel];
}

fn percentEncodeAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    return percentEncodeWithSlash(alloc, raw, false);
}

fn percentEncodePathSegmentAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    return percentEncodeWithSlash(alloc, raw, true);
}

fn percentEncodeWithSlash(alloc: std.mem.Allocator, raw: []const u8, keep_slash: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (raw) |byte| {
        const unreserved =
            (byte >= 'A' and byte <= 'Z') or
            (byte >= 'a' and byte <= 'z') or
            (byte >= '0' and byte <= '9') or
            byte == '-' or byte == '_' or byte == '.' or byte == '~' or
            (keep_slash and byte == '/');
        if (unreserved) {
            try out.append(alloc, byte);
        } else {
            try out.append(alloc, '%');
            try out.append(alloc, std.fmt.digitToChar(byte >> 4, .upper));
            try out.append(alloc, std.fmt.digitToChar(byte & 0x0f, .upper));
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn bedrockInvokePathAlloc(alloc: std.mem.Allocator, model: []const u8) ![]u8 {
    const encoded_model = try percentEncodeAlloc(alloc, model);
    defer alloc.free(encoded_model);
    return try std.fmt.allocPrint(alloc, "/model/{s}/invoke", .{encoded_model});
}

fn parseAwsIso8601(value: []const u8) !u64 {
    if (value.len < "2006-01-02T15:04:05Z".len) return error.InvalidTimestamp;
    if (value[4] != '-' or value[7] != '-' or value[10] != 'T' or value[13] != ':' or value[16] != ':') return error.InvalidTimestamp;
    const year = try std.fmt.parseInt(i64, value[0..4], 10);
    const month = try std.fmt.parseUnsigned(u8, value[5..7], 10);
    const day = try std.fmt.parseUnsigned(u8, value[8..10], 10);
    const hour = try std.fmt.parseUnsigned(u8, value[11..13], 10);
    const minute = try std.fmt.parseUnsigned(u8, value[14..16], 10);
    const second = try std.fmt.parseUnsigned(u8, value[17..19], 10);
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour > 23 or minute > 59 or second > 60) return error.InvalidTimestamp;
    if (value[19] != 'Z') {
        if (value[19] != '.') return error.InvalidTimestamp;
        if (std.mem.indexOfScalar(u8, value[20..], 'Z') == null) return error.InvalidTimestamp;
    }
    const days = daysFromCivil(year, month, day);
    if (days < 0) return error.InvalidTimestamp;
    return @as(u64, @intCast(days)) * std.time.s_per_day + @as(u64, hour) * std.time.s_per_hour + @as(u64, minute) * std.time.s_per_min + @as(u64, second);
}

fn daysFromCivil(year_in: i64, month_in: u8, day_in: u8) i64 {
    var year = year_in;
    const month: i64 = month_in;
    const day: i64 = day_in;
    if (month <= 2) year -= 1;
    const era = @divFloor(year, 400);
    const yoe = year - era * 400;
    const shifted_month = if (month > 2) month - 3 else month + 9;
    const doy = @divFloor(153 * shifted_month + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn signHeadersAlloc(alloc: std.mem.Allocator, creds: Credentials, region: []const u8, host: []const u8, path: []const u8, body: []const u8) ![]HeaderPair {
    const timestamp = currentUnixSeconds();
    const amz_date = try formatAmzDateAlloc(alloc, timestamp);
    errdefer alloc.free(amz_date);
    const scope_date = try formatScopeDateAlloc(alloc, timestamp);
    defer alloc.free(scope_date);
    const payload_hash = try sha256HexAlloc(alloc, body);
    defer alloc.free(payload_hash);

    var headers = std.ArrayListUnmanaged(HeaderPair).empty;
    errdefer freeHeaderPairs(alloc, headers.items);
    try headers.append(alloc, .{ try alloc.dupe(u8, "accept"), try alloc.dupe(u8, "application/json") });
    try headers.append(alloc, .{ try alloc.dupe(u8, "content-type"), try alloc.dupe(u8, "application/json") });
    try headers.append(alloc, .{ try alloc.dupe(u8, "host"), try alloc.dupe(u8, host) });
    try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-content-sha256"), try alloc.dupe(u8, payload_hash) });
    try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-date"), amz_date });
    if (creds.session_token) |token| try headers.append(alloc, .{ try alloc.dupe(u8, "x-amz-security-token"), try alloc.dupe(u8, token) });
    const auth = try authorizationValueAlloc(alloc, creds, region, path, headers.items, payload_hash, amz_date, scope_date);
    errdefer alloc.free(auth);
    try headers.append(alloc, .{ try alloc.dupe(u8, "authorization"), auth });
    return try headers.toOwnedSlice(alloc);
}

fn authorizationValueAlloc(alloc: std.mem.Allocator, creds: Credentials, region: []const u8, path: []const u8, headers: []const HeaderPair, payload_hash: []const u8, amz_date: []const u8, scope_date: []const u8) ![]u8 {
    var canonical_headers = try canonicalHeadersAlloc(alloc, headers);
    defer canonical_headers.deinit(alloc);
    const canonical_request = try std.fmt.allocPrint(alloc, "POST\n{s}\n\n{s}\n{s}\n{s}", .{ path, canonical_headers.header_block, canonical_headers.signed_headers, payload_hash });
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

const CanonicalHeaders = struct {
    header_block: []u8,
    signed_headers: []u8,
    fn deinit(self: *CanonicalHeaders, alloc: std.mem.Allocator) void {
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

fn endpointBaseAlloc(alloc: std.mem.Allocator, endpoint: []const u8) ![]u8 {
    var end = endpoint.len;
    while (end > 0 and endpoint[end - 1] == '/') end -= 1;
    if (end == 0) return error.InvalidEndpoint;
    return try alloc.dupe(u8, endpoint[0..end]);
}

fn currentUnixSeconds() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    const ns: u64 = @intCast(now.toNanoseconds());
    return ns / std.time.ns_per_s;
}

fn formatAmzDateAlloc(alloc: std.mem.Allocator, unix_seconds: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = unix_seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        year_day.year,
        @intFromEnum(month_day.month) + 1,
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
    return try std.fmt.allocPrint(alloc, "{d:0>4}{d:0>2}{d:0>2}", .{ year_day.year, @intFromEnum(month_day.month) + 1, month_day.day_index + 1 });
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

fn getEnvOwned(alloc: std.mem.Allocator, name: [:0]const u8) ?[]u8 {
    if (!builtin.link_libc) return null;
    const value = std.c.getenv(name) orelse return null;
    const text = std.mem.span(value);
    if (text.len == 0) return null;
    return alloc.dupe(u8, text) catch null;
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

pub fn testSharedCredentialsProfileParser() !void {
    const alloc = std.testing.allocator;
    var creds = try parseProfileCredentials(alloc,
        \\[default]
        \\aws_access_key_id = AKIADEFAULT
        \\aws_secret_access_key = defaultsecret
        \\[prod]
        \\aws_access_key_id = AKIAPROD
        \\aws_secret_access_key = prodsecret
        \\aws_session_token = token
    , "prod");
    defer creds.deinit(alloc);
    try std.testing.expectEqualStrings("AKIAPROD", creds.access_key_id);
    try std.testing.expectEqualStrings("prodsecret", creds.secret_access_key);
    try std.testing.expectEqualStrings("token", creds.session_token.?);
}

pub fn testMetadataCredentialParsers() !void {
    const alloc = std.testing.allocator;
    var ecs = try parseMetadataCredentials(alloc,
        \\{
        \\  "AccessKeyId": "AKIAECS",
        \\  "SecretAccessKey": "ecssecret",
        \\  "Token": "ecstoken",
        \\  "Expiration": "2026-01-02T03:04:05Z"
        \\}
    );
    defer ecs.deinit(alloc);
    try std.testing.expectEqualStrings("AKIAECS", ecs.access_key_id);
    try std.testing.expectEqualStrings("ecssecret", ecs.secret_access_key);
    try std.testing.expectEqualStrings("ecstoken", ecs.session_token.?);
    try std.testing.expectEqual(try parseAwsIso8601("2026-01-02T03:04:05Z"), ecs.expires_at_unix.?);

    var sts = try parseStsCredentials(alloc,
        \\<AssumeRoleWithWebIdentityResponse>
        \\  <AssumeRoleWithWebIdentityResult>
        \\    <Credentials>
        \\      <AccessKeyId>AKIASTS</AccessKeyId>
        \\      <SecretAccessKey>stssecret</SecretAccessKey>
        \\      <SessionToken>ststoken</SessionToken>
        \\      <Expiration>2026-01-02T03:04:05.000Z</Expiration>
        \\    </Credentials>
        \\  </AssumeRoleWithWebIdentityResult>
        \\</AssumeRoleWithWebIdentityResponse>
    );
    defer sts.deinit(alloc);
    try std.testing.expectEqualStrings("AKIASTS", sts.access_key_id);
    try std.testing.expectEqualStrings("stssecret", sts.secret_access_key);
    try std.testing.expectEqualStrings("ststoken", sts.session_token.?);
    try std.testing.expectEqual(try parseAwsIso8601("2026-01-02T03:04:05Z"), sts.expires_at_unix.?);
}

pub fn testCredentialUrlEncoding() !void {
    const alloc = std.testing.allocator;
    const query = try percentEncodeAlloc(alloc, "arn:aws:iam::123456789012:role/antfly bedrock");
    defer alloc.free(query);
    try std.testing.expectEqualStrings("arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fantfly%20bedrock", query);

    const path = try percentEncodePathSegmentAlloc(alloc, "role/name with space");
    defer alloc.free(path);
    try std.testing.expectEqualStrings("role/name%20with%20space", path);
}

pub fn testRequestShapeBatchesByProviderRequest() !void {
    try std.testing.expectEqual(@as(usize, cohere_max_batch_size), maxBatchSize("cohere.embed-v4"));
    try std.testing.expectEqual(@as(usize, cohere_max_batch_size), maxBatchSize("cohere.embed-english-v3"));
    try std.testing.expectEqual(@as(usize, single_input_batch_size), maxBatchSize("amazon.titan-embed-text-v2:0"));
    try std.testing.expectEqual(@as(usize, single_input_batch_size), maxBatchSize("amazon.titan-embed-image-v1"));
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

pub fn testBedrockSignerUsesBedrockServiceScope() !void {
    const alloc = std.testing.allocator;
    const creds = Credentials{ .access_key_id = "AKIA", .secret_access_key = "secret" };
    const headers = [_]HeaderPair{
        .{ "host", "bedrock-runtime.us-east-1.amazonaws.com" },
        .{ "x-amz-date", "20260102T030405Z" },
        .{ "x-amz-content-sha256", "hash" },
    };
    const auth = try authorizationValueAlloc(alloc, creds, "us-east-1", "/model/amazon.titan-embed-image-v1/invoke", &headers, "hash", "20260102T030405Z", "20260102");
    defer alloc.free(auth);
    try std.testing.expect(std.mem.indexOf(u8, auth, "/bedrock/aws4_request") != null);
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

test "titan multimodal body combines text and rejects multiple images" {
    try testTitanMultimodalBodyCombinesTextAndRejectsMultipleImages();
}

test "shared credentials profile parser" {
    try testSharedCredentialsProfileParser();
}

test "metadata credential parsers" {
    try testMetadataCredentialParsers();
}

test "credential url encoding" {
    try testCredentialUrlEncoding();
}

test "request shape batches by provider request" {
    try testRequestShapeBatchesByProviderRequest();
}

test "bedrock invoke path escapes model id" {
    try testBedrockInvokePathEscapesModelId();
}

test "bedrock signer uses bedrock service scope" {
    try testBedrockSignerUsesBedrockServiceScope();
}

test "endpoint host includes explicit port" {
    try testEndpointHostIncludesExplicitPort();
}
