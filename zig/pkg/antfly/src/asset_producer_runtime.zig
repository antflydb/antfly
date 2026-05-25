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
const generating_runtime = @import("generating/mod.zig");
const managed_embedder = @import("inference/managed_embedder.zig");
const common_secrets = @import("common/secrets.zig");
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const asset_producer = @import("storage/db/enrichment/asset_producer.zig");

const Allocator = std.mem.Allocator;

pub const Runtime = struct {
    alloc: Allocator,
    http: *httpx.Client,
    owned_http: ?*httpx.Client = null,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,

    pub const Options = struct {
        local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
        secret_store: ?*common_secrets.FileStore = null,
    };

    pub fn init(alloc: Allocator, http: *httpx.Client) Runtime {
        return initWithOptions(alloc, http, .{});
    }

    pub fn initWithOptions(alloc: Allocator, http: *httpx.Client, options: Options) Runtime {
        return .{
            .alloc = alloc,
            .http = http,
            .local_termite_provider = options.local_termite_provider,
            .secret_store = options.secret_store,
        };
    }

    pub fn createOwned(alloc: Allocator, io: std.Io, options: Options) !*Runtime {
        const runtime = try alloc.create(Runtime);
        errdefer alloc.destroy(runtime);

        const client = try alloc.create(httpx.Client);
        errdefer alloc.destroy(client);
        client.* = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        errdefer client.deinit();

        runtime.* = Runtime.initWithOptions(alloc, client, options);
        runtime.owned_http = client;
        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        if (self.owned_http) |client| {
            client.deinit();
            self.alloc.destroy(client);
            self.owned_http = null;
        }
        self.* = undefined;
    }

    pub fn producer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            .vtable = &.{ .produce = produce },
        };
    }

    pub fn ownedProducer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            .vtable = &.{ .produce = produce, .deinit = deinitProducer },
        };
    }

    fn deinitProducer(ptr: *anyopaque, alloc: Allocator) void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        self.deinit();
        alloc.destroy(self);
    }

    fn produce(ptr: *anyopaque, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        return switch (request.producer_type) {
            .copy => try alloc.dupe(u8, request.source_text),
            .generator => try self.generate(alloc, request),
            .reader => try self.read(alloc, request),
            .transcriber => try self.transcribe(alloc, request),
        };
    }

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg = try generating_runtime.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);
        if (request.source_parts_json) |raw_parts| {
            if (raw_parts.len > 0) return try self.generateMultimodal(alloc, cfg, request.source_text, raw_parts);
        }
        const link = generating_runtime.ChainLink{ .generator = cfg };
        var result = try generating_runtime.executeChainWithOptions(alloc, self.http, &.{link}, .{
            .local_termite_provider = self.local_termite_provider,
            .secret_store = self.secret_store,
        }, &.{
            .{ .role = .user, .content = request.source_text },
        });
        defer result.deinit();
        return try alloc.dupe(u8, result.content);
    }

    fn generateMultimodal(
        self: *Runtime,
        alloc: Allocator,
        cfg: generating_runtime.GeneratorConfig,
        source_text: []const u8,
        source_parts_json: []const u8,
    ) ![]u8 {
        const endpoint = try generatorEndpointUrlAlloc(alloc, cfg);
        defer alloc.free(endpoint);

        const content_json = switch (cfg.provider) {
            .openai, .ollama => try openAiContentJsonAlloc(alloc, source_text, source_parts_json),
            .termite, .antfly => try alloc.dupe(u8, source_parts_json),
            else => return error.UnsupportedGeneratorProvider,
        };
        defer alloc.free(content_json);

        const body = try generatorRequestJsonAlloc(alloc, cfg.model, content_json);
        defer alloc.free(body);

        var auth_header_storage: ?[]u8 = null;
        defer if (auth_header_storage) |value| alloc.free(value);
        const headers: ?[]const [2][]const u8 = if (cfg.api_key) |api_key| blk: {
            auth_header_storage = try std.fmt.allocPrint(alloc, "Bearer {s}", .{api_key});
            break :blk &[_][2][]const u8{.{ "Authorization", auth_header_storage.? }};
        } else null;

        var resp = try self.http.post(endpoint, .{
            .json = body,
            .headers = headers,
            .timeout_ms = 300_000,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.GenerateRequestFailed;
        const resp_body = resp.body orelse return error.EmptyResponse;

        const Response = struct {
            choices: []const struct {
                message: struct {
                    content: ?[]const u8 = null,
                },
            },
        };
        var parsed = try std.json.parseFromSlice(Response, alloc, resp_body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.choices.len == 0) return error.EmptyResponse;
        return try alloc.dupe(u8, parsed.value.choices[0].message.content orelse return error.EmptyResponse);
    }

    fn read(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        var registry = readers.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = readers.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        var source = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
        defer source.deinit(alloc);

        const provider = try runtime.get("asset");
        const results = try provider.read(alloc, .{
            .images = source.images,
            .prompt = source.prompt,
        });
        defer {
            for (results) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        }

        return try encodeReaderResults(alloc, request.content_type, results);
    }

    fn transcribe(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        var registry = transcribing.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = transcribing.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        const provider = try runtime.get("asset");
        var result = try provider.transcribe(alloc, .{ .url = request.source_text });
        defer transcribing.deinitResponse(alloc, &result);

        if (isJsonContentType(request.content_type)) {
            return try std.json.Stringify.valueAlloc(alloc, result, .{});
        }
        return try alloc.dupe(u8, result.text orelse "");
    }
};

const ReaderSource = struct {
    images: []const []const u8,
    prompt: ?[]const u8 = null,

    fn deinit(self: *ReaderSource, alloc: Allocator) void {
        for (self.images) |image| alloc.free(@constCast(image));
        alloc.free(self.images);
        if (self.prompt) |prompt| alloc.free(@constCast(prompt));
        self.* = undefined;
    }
};

fn parseReaderSource(alloc: Allocator, source_text: []const u8, source_parts_json: ?[]const u8) !ReaderSource {
    if (source_parts_json) |raw_parts| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
        defer parsed.deinit();
        if (parsed.value == .array) {
            var images = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (images.items) |image| alloc.free(@constCast(image));
                images.deinit(alloc);
            }
            var prompt = std.ArrayListUnmanaged(u8).empty;
            errdefer prompt.deinit(alloc);

            for (parsed.value.array.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "text")) {
                    const text = part.object.get("text") orelse continue;
                    if (text != .string) continue;
                    if (prompt.items.len > 0) try prompt.append(alloc, '\n');
                    try prompt.appendSlice(alloc, text.string);
                } else if (std.mem.eql(u8, type_value.string, "media")) {
                    if (part.object.get("url")) |url| {
                        if (url == .string) try images.append(alloc, try alloc.dupe(u8, url.string));
                    } else if (part.object.get("mime_type")) |mime| {
                        const data = part.object.get("data") orelse continue;
                        if (mime == .string and data == .string) {
                            try images.append(alloc, try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ mime.string, data.string }));
                        }
                    }
                }
            }

            if (images.items.len > 0) {
                return .{
                    .images = try images.toOwnedSlice(alloc),
                    .prompt = if (prompt.items.len > 0) try prompt.toOwnedSlice(alloc) else null,
                };
            }
            prompt.deinit(alloc);
            images.deinit(alloc);
        }
    }

    const images = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(images);
    images[0] = try alloc.dupe(u8, source_text);
    return .{ .images = images };
}

fn encodeReaderResults(alloc: Allocator, content_type: []const u8, results: []const readers.Result) ![]u8 {
    if (isJsonContentType(content_type)) {
        return try std.json.Stringify.valueAlloc(alloc, results, .{});
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (results, 0..) |result, i| {
        if (i > 0) try out.append(alloc, '\n');
        try out.appendSlice(alloc, result.text);
    }
    return try out.toOwnedSlice(alloc);
}

fn isJsonContentType(content_type: []const u8) bool {
    return std.mem.eql(u8, content_type, "application/json") or
        std.mem.endsWith(u8, content_type, "+json");
}

fn generatorEndpointUrlAlloc(alloc: Allocator, cfg: generating_runtime.GeneratorConfig) ![]u8 {
    const base = switch (cfg.provider) {
        .termite => if (cfg.url.len > 0) cfg.url else "http://127.0.0.1:8082",
        .antfly => if (cfg.url.len > 0) cfg.url else return error.UnsupportedGeneratorProvider,
        .openai, .ollama => cfg.url,
        else => return error.UnsupportedGeneratorProvider,
    };
    const path = switch (cfg.provider) {
        .termite, .antfly => "/generate",
        .openai, .ollama => "/chat/completions",
        else => unreachable,
    };
    if (base.len == 0) return error.UnsupportedGeneratorProvider;
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, path });
}

fn generatorRequestJsonAlloc(alloc: Allocator, model: []const u8, content_json: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"model\":");
    try appendJsonString(alloc, &out, model);
    try out.appendSlice(alloc, ",\"messages\":[{\"role\":\"user\",\"content\":");
    try out.appendSlice(alloc, content_json);
    try out.appendSlice(alloc, "}]}");
    return try out.toOwnedSlice(alloc);
}

fn openAiContentJsonAlloc(alloc: Allocator, source_text: []const u8, raw_parts: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return try jsonStringAlloc(alloc, source_text);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    var first = true;
    for (parsed.value.array.items) |part| {
        if (part != .object) continue;
        const type_value = part.object.get("type") orelse continue;
        if (type_value != .string) continue;
        if (std.mem.eql(u8, type_value.string, "text")) {
            const text = part.object.get("text") orelse continue;
            if (text != .string) continue;
            if (!first) try out.append(alloc, ',');
            first = false;
            try out.appendSlice(alloc, "{\"type\":\"text\",\"text\":");
            try appendJsonString(alloc, &out, text.string);
            try out.append(alloc, '}');
        } else if (std.mem.eql(u8, type_value.string, "media")) {
            const url = if (part.object.get("url")) |url_value| blk: {
                if (url_value != .string) continue;
                break :blk url_value.string;
            } else if (part.object.get("mime_type")) |mime| blk: {
                const data = part.object.get("data") orelse continue;
                if (mime != .string or data != .string) continue;
                break :blk try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ mime.string, data.string });
            } else continue;
            const owned_url = part.object.get("url") == null;
            defer if (owned_url) alloc.free(@constCast(url));
            if (!first) try out.append(alloc, ',');
            first = false;
            try out.appendSlice(alloc, "{\"type\":\"image_url\",\"image_url\":{\"url\":");
            try appendJsonString(alloc, &out, url);
            try out.appendSlice(alloc, "}}");
        }
    }
    try out.append(alloc, ']');
    if (first) {
        out.deinit(alloc);
        return try jsonStringAlloc(alloc, source_text);
    }
    return try out.toOwnedSlice(alloc);
}

fn jsonStringAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJsonString(alloc, &out, value);
    return try out.toOwnedSlice(alloc);
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

test "asset producer runtime parses reader multimodal parts" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "", "[{\"type\":\"text\",\"text\":\"read\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]");
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), source.images.len);
    try std.testing.expectEqualStrings("read", source.prompt.?);
}

fn expectOpenAiMultimodalGeneratorRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"gemma4\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"content\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"image_url\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "data:image/png;base64,aaa") != null);
}

test "asset producer runtime passes rendered media parts to generators" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/chat/completions", .assert_request = expectOpenAiMultimodalGeneratorRequest, .respond = .{
            .body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"vision result\"}}]}",
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    const producer = runtime.producer();

    const cfg_json = try std.fmt.allocPrint(alloc, "{{\"provider\":\"openai\",\"model\":\"gemma4\",\"url\":\"{s}\"}}", .{server.baseUrl()});
    defer alloc.free(cfg_json);

    var result: ?[]u8 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            cfg: []const u8,
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produce(a, .{
                .producer_type = .generator,
                .config_json = cfg,
                .source_text = "describe",
                .source_parts_json = "[{\"type\":\"text\",\"text\":\"describe\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]",
                .content_type = "text/plain",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, producer, cfg_json, &result, &run_err });
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer alloc.free(result.?);
    try std.testing.expectEqualStrings("vision result", result.?);
}
