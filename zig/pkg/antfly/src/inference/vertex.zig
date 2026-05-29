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
const google_auth = @import("antfly_google").auth;
const inference = @import("types.zig");

const Allocator = std.mem.Allocator;
const vertex_auth_scope = "https://www.googleapis.com/auth/cloud-platform";

pub const GeminiOptions = struct {
    base_url: []const u8 = "https://generativelanguage.googleapis.com/v1beta",
    api_key: []const u8,
};

pub const GeminiProvider = struct {
    allocator: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    api_key_header: [2][]const u8,

    pub fn init(allocator: Allocator, http: *httpx.Client, options: GeminiOptions) !GeminiProvider {
        var provider = GeminiProvider{
            .allocator = allocator,
            .http = http,
            .base_url = &.{},
            .api_key_header = .{ "x-goog-api-key", &.{} },
        };
        errdefer provider.deinit();

        provider.base_url = try allocator.dupe(u8, options.base_url);
        provider.api_key_header[1] = try allocator.dupe(u8, options.api_key);

        return provider;
    }

    pub fn deinit(self: *GeminiProvider) void {
        self.allocator.free(self.base_url);
        self.allocator.free(self.api_key_header[1]);
        self.* = undefined;
    }

    pub fn generator(self: *GeminiProvider) inference.Generator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &generator_vtable,
        };
    }

    fn generateImpl(ptr: *anyopaque, alloc: Allocator, model: []const u8, messages: []const inference.ChatMessage) anyerror!inference.GenerateResult {
        const self: *GeminiProvider = @ptrCast(@alignCast(ptr));

        const url = try std.fmt.allocPrint(self.allocator, "{s}/models/{s}:generateContent", .{ self.base_url, model });
        defer self.allocator.free(url);

        const json_body = try vertexGenerateRequestJsonAlloc(alloc, messages);
        defer alloc.free(json_body);

        const headers = [_][2][]const u8{self.api_key_header};
        var resp = try self.http.post(url, .{ .json = json_body, .headers = &headers });
        defer resp.deinit();
        if (!resp.ok()) return error.GenerateRequestFailed;
        return try parseGenerateResponseAlloc(alloc, resp.body orelse return error.EmptyResponse);
    }

    const generator_vtable = inference.Generator.VTable{
        .generate = &generateImpl,
    };
};

pub const Options = struct {
    base_url: []const u8 = "https://aiplatform.googleapis.com/v1",
    project_id: ?[]const u8 = null,
    location: []const u8 = "us-central1",
    credentials_path: ?[]const u8 = null,
    bearer_token: ?[]const u8 = null,
};

pub const Provider = struct {
    allocator: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    project_id: []const u8,
    location: []const u8,
    auth_header: ?[2][]const u8 = null,
    token_source: ?*google_auth.CachedTokenSource = null,

    pub fn init(allocator: Allocator, http: *httpx.Client, options: Options) !Provider {
        var provider = Provider{
            .allocator = allocator,
            .http = http,
            .base_url = &.{},
            .project_id = &.{},
            .location = &.{},
        };
        errdefer provider.deinit();

        provider.base_url = try allocator.dupe(u8, options.base_url);
        provider.project_id = if (options.project_id) |value|
            try allocator.dupe(u8, value)
        else
            (try vertexProjectIdFromConfigAlloc(allocator, options.credentials_path) orelse return error.MissingVertexCredentials);
        provider.location = try allocator.dupe(u8, options.location);

        if (options.bearer_token) |token| {
            try provider.setBearer(token);
        } else {
            provider.token_source = try initVertexTokenSource(allocator, options.credentials_path);
        }

        return provider;
    }

    pub fn deinit(self: *Provider) void {
        self.allocator.free(self.base_url);
        self.allocator.free(self.project_id);
        self.allocator.free(self.location);
        if (self.auth_header) |header| self.allocator.free(header[1]);
        if (self.token_source) |source| {
            source.deinit();
            self.allocator.destroy(source);
        }
        self.* = undefined;
    }

    pub fn setBearer(self: *Provider, token: []const u8) !void {
        if (self.auth_header) |header| self.allocator.free(header[1]);
        self.auth_header = .{
            "Authorization",
            try std.fmt.allocPrint(self.allocator, "Bearer {s}", .{token}),
        };
    }

    pub fn generator(self: *Provider) inference.Generator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &generator_vtable,
        };
    }

    fn generateImpl(ptr: *anyopaque, alloc: Allocator, model: []const u8, messages: []const inference.ChatMessage) anyerror!inference.GenerateResult {
        const self: *Provider = @ptrCast(@alignCast(ptr));

        const model_path = try self.vertexModelPathAlloc(self.allocator, model);
        defer self.allocator.free(model_path);
        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}:generateContent",
            .{ self.base_url, model_path },
        );
        defer self.allocator.free(url);

        const json_body = try vertexGenerateRequestJsonAlloc(alloc, messages);
        defer alloc.free(json_body);

        var headers = std.ArrayList([2][]const u8).empty;
        defer headers.deinit(alloc);
        var minted_auth: ?[]u8 = null;
        defer if (minted_auth) |value| alloc.free(value);
        try self.appendAuthHeaders(alloc, &headers, &minted_auth);

        var resp = try self.http.post(url, .{ .json = json_body, .headers = headers.items });
        defer resp.deinit();
        if (!resp.ok()) return error.GenerateRequestFailed;
        const body = resp.body orelse return error.EmptyResponse;

        return try parseGenerateResponseAlloc(alloc, body);
    }

    fn vertexModelPathAlloc(self: *const Provider, alloc: Allocator, model: []const u8) ![]u8 {
        if (std.mem.startsWith(u8, model, "projects/") or std.mem.startsWith(u8, model, "publishers/")) {
            return try alloc.dupe(u8, model);
        }
        return try std.fmt.allocPrint(
            alloc,
            "projects/{s}/locations/{s}/publishers/google/models/{s}",
            .{ self.project_id, self.location, model },
        );
    }

    fn appendAuthHeaders(
        self: *Provider,
        alloc: Allocator,
        headers: *std.ArrayList([2][]const u8),
        minted_auth: *?[]u8,
    ) !void {
        if (self.auth_header) |header| {
            try headers.append(alloc, header);
            return;
        }
        if (self.token_source) |source| {
            minted_auth.* = try source.authorizationValueAlloc(alloc);
            try headers.append(alloc, .{ "Authorization", minted_auth.*.? });
        }
    }

    const generator_vtable = inference.Generator.VTable{
        .generate = &generateImpl,
    };
};

fn parseGenerateResponseAlloc(alloc: Allocator, body: []const u8) !inference.GenerateResult {
    const Response = struct {
        candidates: []const struct {
            content: struct {
                parts: []const struct {
                    text: ?[]const u8 = null,
                } = &.{},
            },
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    if (parsed.value.candidates.len == 0) return error.GenerateRequestFailed;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (parsed.value.candidates[0].content.parts) |part| {
        if (part.text) |text| try out.appendSlice(alloc, text);
    }
    if (out.items.len == 0) return error.GenerateRequestFailed;
    return .{ .content = try out.toOwnedSlice(alloc), .allocator = alloc };
}

fn vertexGenerateRequestJsonAlloc(alloc: Allocator, messages: []const inference.ChatMessage) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var wrote_system = false;
    try out.append(alloc, '{');
    for (messages) |message| {
        if (message.role != .system) continue;
        const content = message.content orelse continue;
        if (wrote_system) continue;
        try out.appendSlice(alloc, "\"systemInstruction\":{\"parts\":");
        try appendVertexParts(alloc, &out, content);
        try out.append(alloc, '}');
        wrote_system = true;
    }

    if (wrote_system) try out.append(alloc, ',');
    try out.appendSlice(alloc, "\"contents\":[");
    var count: usize = 0;
    for (messages) |message| {
        if (message.role == .system) continue;
        if (count > 0) try out.append(alloc, ',');
        try appendVertexContent(alloc, &out, message);
        count += 1;
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn appendVertexContent(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), message: inference.ChatMessage) !void {
    try out.appendSlice(alloc, "{\"role\":");
    try appendJsonString(alloc, out, switch (message.role) {
        .assistant => "model",
        else => "user",
    });
    try out.appendSlice(alloc, ",\"parts\":");
    if (message.content) |content| {
        try appendVertexParts(alloc, out, content);
    } else {
        try out.appendSlice(alloc, "[]");
    }
    try out.append(alloc, '}');
}

fn appendVertexParts(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), content: inference.ChatMessageContent) !void {
    switch (content) {
        .text => |text| {
            try out.appendSlice(alloc, "[{\"text\":");
            try appendJsonString(alloc, out, text);
            try out.appendSlice(alloc, "}]");
        },
        .parts => |parts| {
            try out.append(alloc, '[');
            for (parts, 0..) |part, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendVertexPart(alloc, out, part);
            }
            try out.append(alloc, ']');
        },
    }
}

fn appendVertexPart(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), part: inference.ContentPart) !void {
    switch (part) {
        .text => |text| {
            try out.appendSlice(alloc, "{\"text\":");
            try appendJsonString(alloc, out, text);
            try out.append(alloc, '}');
        },
        .image_url => |image_url| try appendVertexMediaUrl(alloc, out, image_url.url, "image/png"),
        .media => |media| {
            if (media.url) |url| {
                try appendVertexMediaUrl(alloc, out, url, media.mime_type);
            } else {
                try appendVertexInlineData(alloc, out, media.mime_type, media.data);
            }
        },
    }
}

fn appendVertexMediaUrl(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), url: []const u8, fallback_mime_type: []const u8) !void {
    if (parseDataUri(url)) |data_uri| {
        try appendVertexInlineData(alloc, out, data_uri.mime_type, data_uri.data);
        return;
    }
    try out.appendSlice(alloc, "{\"fileData\":{");
    if (fallback_mime_type.len > 0) {
        try out.appendSlice(alloc, "\"mimeType\":");
        try appendJsonString(alloc, out, fallback_mime_type);
        try out.append(alloc, ',');
    }
    try out.appendSlice(alloc, "\"fileUri\":");
    try appendJsonString(alloc, out, url);
    try out.appendSlice(alloc, "}}");
}

fn appendVertexInlineData(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), mime_type: []const u8, data: []const u8) !void {
    if (mime_type.len == 0 or data.len == 0) return error.UnsupportedVertexContentPart;
    try out.appendSlice(alloc, "{\"inlineData\":{\"mimeType\":");
    try appendJsonString(alloc, out, mime_type);
    try out.appendSlice(alloc, ",\"data\":");
    try appendJsonString(alloc, out, data);
    try out.appendSlice(alloc, "}}");
}

const DataUri = struct {
    mime_type: []const u8,
    data: []const u8,
};

fn parseDataUri(value: []const u8) ?DataUri {
    if (!std.mem.startsWith(u8, value, "data:")) return null;
    const rest = value["data:".len..];
    const marker = ";base64,";
    const marker_idx = std.mem.indexOf(u8, rest, marker) orelse return null;
    return .{
        .mime_type = rest[0..marker_idx],
        .data = rest[marker_idx + marker.len ..],
    };
}

fn initVertexTokenSource(alloc: Allocator, credentials_path: ?[]const u8) !*google_auth.CachedTokenSource {
    var cfg = if (credentials_path) |path| blk: {
        var service_account = google_auth.serviceAccountFromFileAlloc(alloc, path) catch return error.MissingVertexCredentials;
        errdefer service_account.deinit(alloc);
        break :blk google_auth.configFromServiceAccountAlloc(alloc, service_account, vertex_auth_scope) catch return error.MissingVertexCredentials;
    } else google_auth.configFromEnvAlloc(alloc, vertex_auth_scope) catch return error.MissingVertexCredentials;
    errdefer cfg.deinit(alloc);

    const source = try alloc.create(google_auth.CachedTokenSource);
    errdefer alloc.destroy(source);
    source.* = try google_auth.CachedTokenSource.init(alloc, cfg);
    return source;
}

fn vertexProjectIdFromConfigAlloc(alloc: Allocator, credentials_path: ?[]const u8) !?[]u8 {
    if (credentials_path) |path| {
        var service_account = google_auth.serviceAccountFromFileAlloc(alloc, path) catch return null;
        defer service_account.deinit(alloc);
        return if (service_account.project_id) |value| try alloc.dupe(u8, value) else null;
    }
    return try google_auth.serviceAccountEnvProjectIdAlloc(alloc);
}

fn appendJsonString(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

test "vertex provider exchanges service account credentials and generates content" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/token", .respond = .{
            .body = "{\"access_token\":\"vertex-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}",
        } },
        .{ .method = .POST, .path = "/projects/proj-from-json/locations/us-central1/publishers/google/models/gemini-test:generateContent", .assert_request = expectVertexGenerateRequest, .respond = .{
            .body = "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"generated from vertex\"}]}}]}",
        } },
    });
    defer server.deinit();

    const token_uri = try std.fmt.allocPrint(alloc, "{s}/token", .{server.baseUrl()});
    defer alloc.free(token_uri);
    const credentials_json = try fakeVertexCredentialsJsonAlloc(alloc, token_uri);
    defer alloc.free(credentials_json);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "credentials.json", .data = credentials_json });
    const credentials_path = try std.fs.path.join(alloc, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "credentials.json" });
    defer alloc.free(credentials_path);

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    var provider = try Provider.init(alloc, &client, .{
        .base_url = server.baseUrl(),
        .credentials_path = credentials_path,
    });
    defer provider.deinit();

    const parts = [_]inference.ContentPart{
        .{ .text = "describe this" },
        .{ .media = .{ .mime_type = "image/png", .data = "YWJj" } },
    };
    const messages = [_]inference.ChatMessage{
        .{ .role = .system, .content = .{ .text = "be brief" } },
        .{ .role = .user, .content = .{ .parts = &parts } },
    };

    var result: ?inference.GenerateResult = null;
    defer if (result) |*value| value.deinit();
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, generator: inference.Generator, out: *?inference.GenerateResult, err_out: *?anyerror, msgs: []const inference.ChatMessage) std.Io.Cancelable!void {
            out.* = generator.generate(a, "gemini-test", msgs) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, provider.generator(), &result, &run_err, &messages }) catch return;
    try server.handleOne();
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("generated from vertex", result.?.content);
}

test "gemini provider sends api key and generates content" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/models/gemini-test:generateContent", .assert_request = expectGeminiGenerateRequest, .respond = .{
            .body = "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"generated from gemini\"}]}}]}",
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    var provider = try GeminiProvider.init(alloc, &client, .{
        .base_url = server.baseUrl(),
        .api_key = "gemini-key",
    });
    defer provider.deinit();

    const messages = [_]inference.ChatMessage{
        .{ .role = .user, .content = .{ .text = "hello" } },
    };

    var result: ?inference.GenerateResult = null;
    defer if (result) |*value| value.deinit();
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, generator: inference.Generator, out: *?inference.GenerateResult, err_out: *?anyerror, msgs: []const inference.ChatMessage) std.Io.Cancelable!void {
            out.* = generator.generate(a, "gemini-test", msgs) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, provider.generator(), &result, &run_err, &messages }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("generated from gemini", result.?.content);
}

fn expectVertexGenerateRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(httpx.Method.POST, req.method);
    try std.testing.expectEqualStrings("Bearer vertex-token", req.header("Authorization") orelse return error.MissingHeader);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"systemInstruction\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"text\":\"describe this\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"YWJj\"}") != null);
}

fn expectGeminiGenerateRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(httpx.Method.POST, req.method);
    try std.testing.expectEqualStrings("gemini-key", req.header("x-goog-api-key") orelse return error.MissingHeader);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"text\":\"hello\"") != null);
}

const fake_vertex_private_key_json =
    "-----BEGIN PRIVATE KEY-----\\n" ++
    "MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAOXaLd9jk03zcJ95\\n" ++
    "CfwKjyqHiZAaf0KC4rwRWd+TSvrqdiZUHneOXchF4FtwAJ6m+qi5KsTyazOWv4S0\\n" ++
    "FRLd49XFNv8op9e8x+gnItgt4QoQ2UT+QU7qG+wyavU25+m61G2CFB8+I9wXzH3x\\n" ++
    "HMfUuOWgqfy+szxUFNRf3sEfGW8DAgMBAAECgYEAmR1LG5mQggfeCU2vGgfKsRES\\n" ++
    "0Tzlc2APPCruzKGo/Bb917CHjyr2TDhIKYEl2InxRj37QLEgOoB8WiFAPI41e2mZ\\n" ++
    "r/sshHAB74N7OOCG6G4Jin1qsnQKgSwloBctDxtvUydD1ApmjfKQB1vENL6h4jKU\\n" ++
    "VMBm/65DU/4iWJkWgBECQQD4oRPl63IemtUsRTnz+j8tEC5MsH7CNvwNj5os2ptm\\n" ++
    "X3/rAge3BKYMWlN237K6yapZMHfiLj3K3fv8Kkbn7VwpAkEA7KqY97XZaLr4sI3a\\n" ++
    "9EHgbB2GjzJAsnzXSfn7OXLuc812rDpK/+6mcXFSbe1OmQTbzPIOJIARcIz3fqXI\\n" ++
    "uAHXSwJAOlA1RYjKVElGVELMS9/Wr3ALG+uNX2ncBiY3J+wB5Knja7AnNRK/C0io\\n" ++
    "KMpgthSUgqSuiXsE/S7BaixUQxNVuQJBAJC8hHB5tkxmjFDtcEqRPz7fj7tjcE24\\n" ++
    "K7ICP7ISp+IKddk+jT+YJBKcy1yPFNJgNkxQfHW2HPRIQdQib26ZMaECQQCcW21U\\n" ++
    "jsnUTXZp0WrOnzoqkJtQmmey1Bb9ZxBym/IoaQdDefgbdlyeFQTz2tWKDwqAlEsl\\n" ++
    "8peeQ6Fmi8Vuw9qK\\n" ++
    "-----END PRIVATE KEY-----\\n";

fn fakeVertexCredentialsJsonAlloc(alloc: Allocator, token_uri: []const u8) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        \\{{
        \\  "project_id": "proj-from-json",
        \\  "private_key_id": "kid-1",
        \\  "private_key": "{s}",
        \\  "client_email": "svc@example.iam.gserviceaccount.com",
        \\  "token_uri": "{s}"
        \\}}
    ,
        .{ fake_vertex_private_key_json, token_uri },
    );
}
