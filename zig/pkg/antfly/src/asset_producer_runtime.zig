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
const extracting = @import("antfly_extracting");
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
            .extractor => try self.extract(alloc, request),
        };
    }

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg = try generating_runtime.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);
        var parts: ?[]generating_runtime.ContentPart = null;
        defer if (parts) |items| freeGeneratorContentParts(alloc, items);
        const content: generating_runtime.ChatMessageContent = if (request.source_parts_json) |raw_parts| blk: {
            if (raw_parts.len == 0) break :blk .{ .text = request.source_text };
            parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts);
            break :blk .{ .parts = parts.? };
        } else .{ .text = request.source_text };
        const link = generating_runtime.ChainLink{ .generator = cfg };
        var result = try generating_runtime.executeChainWithOptions(alloc, self.http, &.{link}, .{
            .local_termite_provider = self.local_termite_provider,
            .secret_store = self.secret_store,
        }, &.{
            .{ .role = .user, .content = content },
        });
        defer result.deinit();
        return try alloc.dupe(u8, result.content);
    }

    fn read(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        var source = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
        defer source.deinit(alloc);

        if (cfg_parsed.value.provider == .antfly and cfg_parsed.value.resolvedUrl() == null) {
            const local = self.local_termite_provider orelse return error.UnsupportedReaderProvider;
            const read_images = local.read_images orelse return error.UnsupportedReaderProvider;
            const results = try read_images(local.ptr, alloc, cfg_parsed.value.model orelse "", .{
                .images = source.images,
                .prompt = source.prompt,
                .max_tokens = cfg_parsed.value.max_tokens,
            });
            defer {
                for (results) |*result| readers.deinitResult(alloc, result);
                alloc.free(results);
            }
            return try encodeReaderResults(alloc, request.content_type, results);
        }

        var registry = readers.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = readers.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

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

        if (cfg_parsed.value.provider == .antfly and cfg_parsed.value.resolvedUrl() == null) {
            const local = self.local_termite_provider orelse return error.UnsupportedTranscriberProvider;
            const transcribe_audio = local.transcribe_audio orelse return error.UnsupportedTranscriberProvider;
            var result = try transcribe_audio(local.ptr, alloc, cfg_parsed.value.model orelse "", .{
                .url = request.source_text,
                .language = cfg_parsed.value.language_code,
            });
            defer transcribing.deinitResponse(alloc, &result);

            if (isJsonContentType(request.content_type)) {
                return try std.json.Stringify.valueAlloc(alloc, result, .{});
            }
            return try alloc.dupe(u8, result.text orelse "");
        }

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

    fn extract(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg = try extracting.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);

        const content_json = try extractionContentJsonAlloc(alloc, request.source_text, request.source_parts_json);
        defer alloc.free(content_json);
        const input = extracting.Input{ .content_json = content_json };
        const extract_request = extracting.Request{
            .inputs = &.{input},
            .schema_json = cfg.schema_json,
            .options_json = cfg.options_json,
        };

        var response = if (cfg.provider == .antfly and cfg.resolvedUrl() == null) blk: {
            const local = self.local_termite_provider orelse return error.UnsupportedExtractionProvider;
            const extract_fn = local.extract orelse return error.UnsupportedExtractionProvider;
            break :blk try extract_fn(local.ptr, alloc, cfg.model, extract_request);
        } else try extracting.extractWithConfig(alloc, self.http, cfg, extract_request);
        defer response.deinit();

        if (isJsonContentType(request.content_type) or request.content_type.len == 0) {
            return try extracting.firstResultJsonAlloc(alloc, response.json);
        }
        return try alloc.dupe(u8, response.json);
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

    return try parseReaderSourceText(alloc, source_text);
}

fn parseReaderSourceText(alloc: Allocator, source_text: []const u8) !ReaderSource {
    const trimmed = std.mem.trim(u8, source_text, &std.ascii.whitespace);
    if (trimmed.len > 0 and (trimmed[0] == '[' or trimmed[0] == '"')) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch |err| switch (err) {
            std.mem.Allocator.Error.OutOfMemory => return err,
            else => null,
        };
        if (parsed) |*value| {
            defer value.deinit();
            switch (value.value) {
                .string => |url| return try singleReaderImage(alloc, url),
                .array => |array| {
                    var images = std.ArrayListUnmanaged([]const u8).empty;
                    errdefer {
                        for (images.items) |image| alloc.free(@constCast(image));
                        images.deinit(alloc);
                    }
                    for (array.items) |item| {
                        if (item == .string) try images.append(alloc, try alloc.dupe(u8, item.string));
                    }
                    return .{ .images = try images.toOwnedSlice(alloc) };
                },
                else => {},
            }
        }
    }
    return try singleReaderImage(alloc, source_text);
}

fn singleReaderImage(alloc: Allocator, url: []const u8) !ReaderSource {
    const images = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(images);
    images[0] = try alloc.dupe(u8, url);
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

fn extractionContentJsonAlloc(alloc: Allocator, source_text: []const u8, source_parts_json: ?[]const u8) ![]u8 {
    if (source_parts_json) |raw_parts| {
        if (raw_parts.len > 0) return try alloc.dupe(u8, raw_parts);
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(source_text, .{})});
}

fn parseGeneratorContentParts(alloc: Allocator, source_text: []const u8, raw_parts: []const u8) ![]generating_runtime.ContentPart {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
    defer parsed.deinit();
    if (parsed.value != .array) {
        const items = try alloc.alloc(generating_runtime.ContentPart, 1);
        items[0] = .{ .text = try alloc.dupe(u8, source_text) };
        return items;
    }

    var parts = std.ArrayListUnmanaged(generating_runtime.ContentPart).empty;
    errdefer freeGeneratorContentParts(alloc, parts.items);
    for (parsed.value.array.items) |part| {
        if (part != .object) continue;
        const type_value = part.object.get("type") orelse continue;
        if (type_value != .string) continue;
        if (std.mem.eql(u8, type_value.string, "text")) {
            const text = part.object.get("text") orelse continue;
            if (text != .string) continue;
            try parts.append(alloc, .{ .text = try alloc.dupe(u8, text.string) });
        } else if (std.mem.eql(u8, type_value.string, "image_url")) {
            const image_url = part.object.get("image_url") orelse continue;
            if (image_url != .object) continue;
            const url = image_url.object.get("url") orelse continue;
            if (url != .string) continue;
            try parts.append(alloc, .{ .image_url = .{ .url = try alloc.dupe(u8, url.string) } });
        } else if (std.mem.eql(u8, type_value.string, "media")) {
            if (part.object.get("url")) |url| {
                if (url == .string) {
                    const mime_type = if (part.object.get("mime_type")) |mime|
                        if (mime == .string) mime.string else ""
                    else
                        "";
                    try parts.append(alloc, .{ .media = .{
                        .url = try alloc.dupe(u8, url.string),
                        .mime_type = if (mime_type.len > 0) try alloc.dupe(u8, mime_type) else "",
                    } });
                }
            } else if (part.object.get("mime_type")) |mime| {
                const data = part.object.get("data") orelse continue;
                if (mime == .string and data == .string) {
                    try parts.append(alloc, .{ .media = .{
                        .data = try alloc.dupe(u8, data.string),
                        .mime_type = try alloc.dupe(u8, mime.string),
                    } });
                }
            }
        }
    }

    if (parts.items.len == 0) {
        try parts.append(alloc, .{ .text = try alloc.dupe(u8, source_text) });
    }
    return try parts.toOwnedSlice(alloc);
}

fn freeGeneratorContentParts(alloc: Allocator, parts: []generating_runtime.ContentPart) void {
    for (parts) |part| {
        switch (part) {
            .text => |text| alloc.free(@constCast(text)),
            .image_url => |image_url| alloc.free(@constCast(image_url.url)),
            .media => |media| {
                if (media.data.len > 0) alloc.free(@constCast(media.data));
                if (media.mime_type.len > 0) alloc.free(@constCast(media.mime_type));
                if (media.url) |url| alloc.free(@constCast(url));
            },
        }
    }
    alloc.free(parts);
}

test "asset producer runtime parses reader multimodal parts" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "", "[{\"type\":\"text\",\"text\":\"read\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]");
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), source.images.len);
    try std.testing.expectEqualStrings("read", source.prompt.?);
}

test "asset producer runtime parses reader string array source" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "[\"data:image/png;base64,aaa\",\"data:image/jpeg;base64,bbb\"]", null);
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), source.images.len);
    try std.testing.expectEqualStrings("data:image/png;base64,aaa", source.images[0]);
    try std.testing.expectEqualStrings("data:image/jpeg;base64,bbb", source.images[1]);
}

test "asset producer runtime parses empty reader array source as empty input" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "[]", null);
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), source.images.len);
}

fn expectOpenAiMultimodalGeneratorRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"gemma4\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"content\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"media\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"url\":\"data:audio/wav;base64,aaa\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"image_url\"") == null);
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
                .source_parts_json = "[{\"type\":\"text\",\"text\":\"describe\"},{\"type\":\"media\",\"url\":\"data:audio/wav;base64,aaa\",\"mime_type\":\"audio/wav\"}]",
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

test "asset producer runtime routes antfly reader without url to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        read_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.LocalTermiteProvider {
            return .{
                .ptr = self,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .read_images = readImages,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn readImages(ptr: *anyopaque, a: Allocator, model: []const u8, request: readers.Request) ![]readers.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.read_calls += 1;
            try std.testing.expectEqualStrings("local-reader", model);
            try std.testing.expectEqual(@as(usize, 1), request.images.len);
            try std.testing.expectEqualStrings("data:image/png;base64,aaa", request.images[0]);
            try std.testing.expectEqualStrings("extract", request.prompt.?);

            const out = try a.alloc(readers.Result, 1);
            out[0] = .{ .text = try a.dupe(u8, "local read text") };
            return out;
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .local_termite_provider = local.provider() });
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
        .source_text = "",
        .source_parts_json = "[{\"type\":\"text\",\"text\":\"extract\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]",
        .content_type = "text/plain",
    });
    defer alloc.free(result);

    try std.testing.expectEqualStrings("local read text", result);
    try std.testing.expectEqual(@as(usize, 1), local.read_calls);
}

test "asset producer runtime routes antfly transcriber without url to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        transcribe_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.LocalTermiteProvider {
            return .{
                .ptr = self,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .transcribe_audio = transcribeAudio,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn transcribeAudio(ptr: *anyopaque, a: Allocator, model: []const u8, request: transcribing.Request) !transcribing.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.transcribe_calls += 1;
            try std.testing.expectEqualStrings("local-transcriber", model);
            try std.testing.expectEqualStrings("file:///tmp/audio.wav", request.url);
            try std.testing.expectEqualStrings("en-US", request.language.?);
            return .{
                .text = try a.dupe(u8, "local transcript"),
                .language = try a.dupe(u8, "en-US"),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .local_termite_provider = local.provider() });
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .transcriber,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-transcriber\",\"language_code\":\"en-US\"}",
        .source_text = "file:///tmp/audio.wav",
        .content_type = "text/plain",
    });
    defer alloc.free(result);

    try std.testing.expectEqualStrings("local transcript", result);
    try std.testing.expectEqual(@as(usize, 1), local.transcribe_calls);
}

test "asset producer runtime routes antfly extractor without url to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        extract_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.LocalTermiteProvider {
            return .{
                .ptr = self,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .extract = extract,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn extract(ptr: *anyopaque, a: Allocator, model: []const u8, request: extracting.Request) !extracting.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.extract_calls += 1;
            try std.testing.expectEqualStrings("local-extractor", model);
            try std.testing.expectEqual(@as(usize, 1), request.inputs.len);
            try std.testing.expect(std.mem.indexOf(u8, request.inputs[0].content_json, "Ada") != null);
            try std.testing.expect(std.mem.indexOf(u8, request.schema_json, "person") != null);
            return .{
                .allocator = a,
                .json = try a.dupe(u8, "{\"object\":\"extraction\",\"model\":\"local-extractor\",\"data\":[{\"entities\":[{\"label\":\"person\",\"text\":\"Ada\"}],\"relations\":[]}]}"),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .local_termite_provider = local.provider() });
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .extractor,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-extractor\",\"schema\":{\"entities\":[\"person\"]}}",
        .source_text = "Ada works at Antfly.",
        .content_type = "application/json",
    });
    defer alloc.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "\"entities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"Ada\"") != null);
    try std.testing.expectEqual(@as(usize, 1), local.extract_calls);
}
