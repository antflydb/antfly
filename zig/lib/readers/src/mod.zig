// Copyright 2026 Antfly, Inc.
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
const httpx = @import("httpx");
const termite_api = @import("termite_api");

const Allocator = std.mem.Allocator;

pub const Provider = enum {
    antfly,
    termite,
    openai,
    vertex,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
            .termite => "termite",
            .openai => "openai",
            .vertex => "vertex",
        });
    }

    pub fn jsonParse(_: Allocator, source: anytype, _: std.json.ParseOptions) !@This() {
        const raw = switch (try source.next()) {
            .string => |value| value,
            else => return error.UnexpectedToken,
        };
        if (std.mem.eql(u8, raw, "antfly")) return .antfly;
        if (std.mem.eql(u8, raw, "termite")) return .termite;
        if (std.mem.eql(u8, raw, "openai")) return .openai;
        if (std.mem.eql(u8, raw, "vertex")) return .vertex;
        return error.UnexpectedToken;
    }
};

pub const Config = struct {
    provider: Provider,
    model: ?[]const u8 = null,
    prompt: ?[]const u8 = null,
    max_tokens: ?i64 = null,
    api_key: ?[]const u8 = null,
    bearer_token: ?[]const u8 = null,
    base_url: ?[]const u8 = null,
    url: ?[]const u8 = null,
    api_url: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    credentials_path: ?[]const u8 = null,

    pub fn resolvedUrl(self: Config) ?[]const u8 {
        return self.url orelse self.api_url;
    }
};

pub const Request = struct {
    images: []const []const u8,
    prompt: ?[]const u8 = null,
    max_tokens: ?i64 = null,
};

pub const Result = struct {
    text: []const u8,
    fields_json: ?[]const u8 = null,
    regions_json: ?[]const u8 = null,
};

pub fn deinitResult(alloc: Allocator, result: *Result) void {
    alloc.free(@constCast(result.text));
    if (result.fields_json) |value| alloc.free(@constCast(value));
    if (result.regions_json) |value| alloc.free(@constCast(value));
    result.* = undefined;
}

pub const Reader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ptr: *anyopaque, alloc: Allocator, req: Request) anyerror![]Result,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn read(self: Reader, alloc: Allocator, req: Request) ![]Result {
        return try self.vtable.read(self.ptr, alloc, req);
    }

    pub fn deinit(self: Reader) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const Runtime = struct {
    allocator: Allocator,
    readers: std.StringArrayHashMapUnmanaged(Reader) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Runtime {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Runtime) void {
        var it = self.readers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.readers.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn loadFromRegistry(self: *Runtime, http: *httpx.Client, registry: *const Registry) !void {
        var it = registry.configs.iterator();
        while (it.next()) |entry| {
            const reader = try initReader(self.allocator, http, entry.value_ptr.*);
            errdefer reader.deinit();
            try self.registerOwnedReader(entry.key_ptr.*, reader);
        }
        if (registry.default_provider) |name| {
            const idx = self.readers.getIndex(name) orelse return error.UnknownReaderProvider;
            self.default_provider = self.readers.keys()[idx];
        }
    }

    pub fn registerOwnedReader(self: *Runtime, name: []const u8, reader: Reader) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const gop = try self.readers.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            reader.deinit();
            return error.DuplicateReaderProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = reader;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn get(self: *const Runtime, name: ?[]const u8) !Reader {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultReaderProvider;
        return self.readers.get(resolved) orelse return error.UnknownReaderProvider;
    }
};

threadlocal var active_runtime: ?*const Runtime = null;

pub fn setActiveRuntime(runtime: ?*const Runtime) void {
    active_runtime = runtime;
}

pub fn getActiveRuntime() ?*const Runtime {
    return active_runtime;
}

pub const Registry = struct {
    allocator: Allocator,
    configs: std.StringArrayHashMapUnmanaged(Config) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Registry {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.configs.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            deinitConfig(self.allocator, entry.value_ptr);
        }
        self.configs.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn parseFromValue(alloc: Allocator, value: std.json.Value) !Registry {
        if (value != .object) return error.InvalidReaderConfig;
        var registry = Registry.init(alloc);
        errdefer registry.deinit();
        var it = value.object.iterator();
        while (it.next()) |entry| {
            var parsed = try std.json.parseFromValue(Config, alloc, entry.value_ptr.*, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            try registry.registerConfig(entry.key_ptr.*, parsed.value);
        }
        return registry;
    }

    pub fn registerConfig(self: *Registry, name: []const u8, cfg: Config) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const owned = try cloneConfig(self.allocator, cfg);
        errdefer deinitConfigValue(self.allocator, owned);
        const gop = try self.configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            deinitConfigValue(self.allocator, owned);
            return error.DuplicateReaderProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn defaultProviderName(self: *const Registry) ?[]const u8 {
        return self.default_provider;
    }

    pub fn getConfig(self: *const Registry, name: ?[]const u8) !Config {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultReaderProvider;
        return self.configs.get(resolved) orelse return error.UnknownReaderProvider;
    }
};

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    return .{
        .provider = cfg.provider,
        .model = try dupOpt(alloc, cfg.model),
        .prompt = try dupOpt(alloc, cfg.prompt),
        .max_tokens = cfg.max_tokens,
        .api_key = try dupOpt(alloc, cfg.api_key),
        .bearer_token = try dupOpt(alloc, cfg.bearer_token),
        .base_url = try dupOpt(alloc, cfg.base_url),
        .url = try dupOpt(alloc, cfg.url),
        .api_url = try dupOpt(alloc, cfg.api_url),
        .project_id = try dupOpt(alloc, cfg.project_id),
        .location = try dupOpt(alloc, cfg.location),
        .credentials_path = try dupOpt(alloc, cfg.credentials_path),
    };
}

pub fn deinitConfig(alloc: Allocator, cfg: *Config) void {
    freeOpt(alloc, cfg.model);
    freeOpt(alloc, cfg.prompt);
    freeOpt(alloc, cfg.api_key);
    freeOpt(alloc, cfg.bearer_token);
    freeOpt(alloc, cfg.base_url);
    freeOpt(alloc, cfg.url);
    freeOpt(alloc, cfg.api_url);
    freeOpt(alloc, cfg.project_id);
    freeOpt(alloc, cfg.location);
    freeOpt(alloc, cfg.credentials_path);
    cfg.* = undefined;
}

fn deinitConfigValue(alloc: Allocator, cfg: Config) void {
    var owned = cfg;
    deinitConfig(alloc, &owned);
}

fn initReader(alloc: Allocator, http: *httpx.Client, cfg: Config) !Reader {
    return switch (cfg.provider) {
        .antfly, .termite => try AntflyReaderState.init(alloc, http, cfg),
        .openai => try OpenAiReaderState.init(alloc, http, cfg),
        .vertex => try VertexReaderState.init(alloc, http, cfg),
    };
}

const AntflyReaderState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    api_url: []const u8,
    model: []const u8,
    prompt: ?[]const u8 = null,
    max_tokens: ?i64 = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Reader {
        const state = try alloc.create(AntflyReaderState);
        errdefer alloc.destroy(state);
        state.* = .{
            .alloc = alloc,
            .http = http,
            .api_url = try alloc.dupe(u8, cfg.resolvedUrl() orelse "http://127.0.0.1:8080"),
            .model = try alloc.dupe(u8, cfg.model orelse ""),
            .prompt = try dupOpt(alloc, cfg.prompt),
            .max_tokens = cfg.max_tokens,
        };
        return .{ .ptr = state, .vtable = &.{ .read = read, .deinit = deinit } };
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *AntflyReaderState = @ptrCast(@alignCast(ptr));
        self.alloc.free(self.api_url);
        self.alloc.free(self.model);
        freeOpt(self.alloc, self.prompt);
        self.alloc.destroy(self);
    }

    fn read(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror![]Result {
        const self: *AntflyReaderState = @ptrCast(@alignCast(ptr));
        const images = try alloc.alloc(termite_api.types.ImageURL, req.images.len);
        defer alloc.free(images);
        for (req.images, 0..) |image, i| images[i] = .{ .url = image };

        const body = try httpx.json.Json.stringify(alloc, termite_api.types.ReadRequest{
            .model = self.model,
            .images = images,
            .prompt = req.prompt orelse self.prompt,
            .max_tokens = req.max_tokens orelse self.max_tokens,
        });
        defer alloc.free(body);

        const url = try std.fmt.allocPrint(alloc, "{s}/read", .{self.api_url});
        defer alloc.free(url);
        var resp = try self.http.post(url, .{ .json = body });
        defer resp.deinit();
        if (!resp.ok()) return error.ReadRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(termite_api.types.ReadResponse, alloc, payload, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        const out = try alloc.alloc(Result, parsed.value.data.len);
        errdefer alloc.free(out);
        for (parsed.value.data, 0..) |item, i| {
            out[i] = .{
                .text = try alloc.dupe(u8, item.text),
                .fields_json = if (item.fields) |fields| try std.json.Stringify.valueAlloc(alloc, fields, .{}) else null,
                .regions_json = if (item.regions) |regions| try std.json.Stringify.valueAlloc(alloc, regions, .{}) else null,
            };
        }
        return out;
    }
};

const OpenAiReaderState = CloudReaderState(.openai);
const VertexReaderState = CloudReaderState(.vertex);

fn CloudReaderState(comptime provider: Provider) type {
    return struct {
        alloc: Allocator,
        http: *httpx.Client,
        base_url: []const u8,
        auth_header: ?[2][]const u8 = null,
        model: []const u8,
        prompt: ?[]const u8 = null,
        max_tokens: ?i64 = null,
        project_id: ?[]const u8 = null,
        location: ?[]const u8 = null,

        const Self = @This();

        fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Reader {
            const state = try alloc.create(Self);
            errdefer alloc.destroy(state);
            state.* = .{
                .alloc = alloc,
                .http = http,
                .base_url = try alloc.dupe(u8, cfg.base_url orelse switch (provider) {
                    .openai => "https://api.openai.com/v1",
                    .vertex => "https://generativelanguage.googleapis.com/v1beta",
                    else => unreachable,
                }),
                .model = try alloc.dupe(u8, cfg.model orelse switch (provider) {
                    .openai => "gpt-4.1-mini",
                    .vertex => "gemini-2.5-flash",
                    else => unreachable,
                }),
                .prompt = try dupOpt(alloc, cfg.prompt),
                .max_tokens = cfg.max_tokens,
                .project_id = try dupOpt(alloc, cfg.project_id),
                .location = try dupOpt(alloc, cfg.location),
            };
            errdefer state.deinitState();
            if (cfg.bearer_token orelse cfg.api_key) |token| try state.setBearer(token) else if (cfg.credentials_path != null) return error.UnsupportedVertexServiceAccountCredentials;
            return .{ .ptr = state, .vtable = &.{ .read = read, .deinit = deinit } };
        }

        fn deinitState(self: *Self) void {
            self.alloc.free(self.base_url);
            self.alloc.free(self.model);
            freeOpt(self.alloc, self.prompt);
            freeOpt(self.alloc, self.project_id);
            freeOpt(self.alloc, self.location);
            if (self.auth_header) |header| self.alloc.free(header[1]);
        }

        fn deinit(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.deinitState();
            self.alloc.destroy(self);
        }

        fn setBearer(self: *Self, token: []const u8) !void {
            if (self.auth_header) |header| self.alloc.free(header[1]);
            self.auth_header = .{ "Authorization", try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{token}) };
        }

        fn read(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror![]Result {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return switch (provider) {
                .openai => try self.readOpenAi(alloc, req),
                .vertex => try self.readVertex(alloc, req),
                else => unreachable,
            };
        }

        fn readOpenAi(self: *Self, alloc: Allocator, req: Request) ![]Result {
            var content = std.json.Array.init(alloc);
            defer content.deinit();
            try appendTextPart(alloc, &content, req.prompt orelse self.prompt orelse "Read the image and return the text.");
            for (req.images) |image| try appendImageUrlPart(alloc, &content, image);
            const messages = [_]struct { role: []const u8, content: std.json.Value }{
                .{ .role = "user", .content = .{ .array = content } },
            };
            const Body = struct {
                model: []const u8,
                messages: []const @TypeOf(messages[0]),
                max_tokens: ?i64 = null,
            };
            const body = try httpx.json.Json.stringify(alloc, Body{
                .model = self.model,
                .messages = &messages,
                .max_tokens = req.max_tokens orelse self.max_tokens,
            });
            defer alloc.free(body);
            const url = try std.fmt.allocPrint(alloc, "{s}/chat/completions", .{self.base_url});
            defer alloc.free(url);
            var headers = std.ArrayList([2][]const u8).empty;
            defer headers.deinit(alloc);
            if (self.auth_header) |header| try headers.append(alloc, header);
            var resp = try self.http.post(url, .{ .json = body, .headers = headers.items });
            defer resp.deinit();
            if (!resp.ok()) return error.ReadRequestFailed;
            const Response = struct { choices: []const struct { message: struct { content: ?[]const u8 = null } } = &.{} };
            var parsed = try std.json.parseFromSlice(Response, alloc, resp.body orelse return error.EmptyResponse, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.choices.len == 0) return error.EmptyResponse;
            return try singleTextResult(alloc, parsed.value.choices[0].message.content orelse "");
        }

        fn readVertex(self: *Self, alloc: Allocator, req: Request) ![]Result {
            var parts = std.json.Array.init(alloc);
            defer parts.deinit();
            try appendVertexTextPart(alloc, &parts, req.prompt orelse self.prompt orelse "Read the image and return the text.");
            for (req.images) |image| try appendVertexImagePart(alloc, &parts, image);
            const contents = [_]struct { role: []const u8, parts: std.json.Value }{
                .{ .role = "user", .parts = .{ .array = parts } },
            };
            const Body = struct { contents: []const @TypeOf(contents[0]) };
            const body = try httpx.json.Json.stringify(alloc, Body{ .contents = &contents });
            defer alloc.free(body);
            const url = try std.fmt.allocPrint(alloc, "{s}/models/{s}:generateContent", .{ self.base_url, self.model });
            defer alloc.free(url);
            var headers = std.ArrayList([2][]const u8).empty;
            defer headers.deinit(alloc);
            if (self.auth_header) |header| try headers.append(alloc, header);
            var resp = try self.http.post(url, .{ .json = body, .headers = headers.items });
            defer resp.deinit();
            if (!resp.ok()) return error.ReadRequestFailed;
            const Response = struct {
                candidates: []const struct {
                    content: struct {
                        parts: []const struct { text: ?[]const u8 = null } = &.{},
                    },
                } = &.{},
            };
            var parsed = try std.json.parseFromSlice(Response, alloc, resp.body orelse return error.EmptyResponse, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.candidates.len == 0 or parsed.value.candidates[0].content.parts.len == 0) return error.EmptyResponse;
            return try singleTextResult(alloc, parsed.value.candidates[0].content.parts[0].text orelse "");
        }
    };
}

fn appendTextPart(alloc: Allocator, parts: *std.json.Array, text: []const u8) !void {
    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);
    try obj.put(alloc, "type", .{ .string = "text" });
    try obj.put(alloc, "text", .{ .string = text });
    try parts.append(.{ .object = obj });
}

fn appendImageUrlPart(alloc: Allocator, parts: *std.json.Array, url: []const u8) !void {
    var image = std.json.ObjectMap.empty;
    errdefer image.deinit(alloc);
    try image.put(alloc, "url", .{ .string = url });
    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);
    try obj.put(alloc, "type", .{ .string = "image_url" });
    try obj.put(alloc, "image_url", .{ .object = image });
    try parts.append(.{ .object = obj });
}

fn appendVertexTextPart(alloc: Allocator, parts: *std.json.Array, text: []const u8) !void {
    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);
    try obj.put(alloc, "text", .{ .string = text });
    try parts.append(.{ .object = obj });
}

fn appendVertexImagePart(alloc: Allocator, parts: *std.json.Array, url: []const u8) !void {
    var obj = std.json.ObjectMap.empty;
    errdefer obj.deinit(alloc);
    if (std.mem.startsWith(u8, url, "data:")) {
        try obj.put(alloc, "inlineData", .{ .string = url });
    } else {
        try obj.put(alloc, "fileData", .{ .string = url });
    }
    try parts.append(.{ .object = obj });
}

fn singleTextResult(alloc: Allocator, text: []const u8) ![]Result {
    const out = try alloc.alloc(Result, 1);
    errdefer alloc.free(out);
    out[0] = .{ .text = try alloc.dupe(u8, text) };
    return out;
}

fn dupOpt(alloc: Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |v| try alloc.dupe(u8, v) else null;
}

fn freeOpt(alloc: Allocator, value: ?[]const u8) void {
    if (value) |v| alloc.free(v);
}

test "reader registry preserves named providers" {
    const alloc = std.testing.allocator;
    var cfg = std.json.ObjectMap.empty;
    defer cfg.deinit(alloc);
    try cfg.put(alloc, "provider", .{ .string = "antfly" });
    try cfg.put(alloc, "model", .{ .string = "reader-model" });

    var obj = std.json.ObjectMap.empty;
    defer obj.deinit(alloc);
    try obj.put(alloc, "ocr", .{ .object = cfg });

    var parsed = try Registry.parseFromValue(alloc, .{ .object = obj });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("ocr", parsed.defaultProviderName().?);
    try std.testing.expectEqual(Provider.antfly, (try parsed.getConfig(null)).provider);
}
