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
const audio = @import("antfly_audio_openapi");
const httpx = @import("httpx");

const Allocator = std.mem.Allocator;

threadlocal var active_runtime: ?*const Runtime = null;

pub const Provider = audio.TTSProvider;
pub const Config = audio.TTSConfig;
pub const Request = audio.TTSRequest;
pub const Response = audio.TTSResponse;
pub const Voice = audio.Voice;

pub const SynthesisResult = struct {
    audio: ?[]u8 = null,
    response: Response = .{},
};

pub const Synthesizer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        synthesize: *const fn (ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!SynthesisResult,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn synthesize(self: Synthesizer, alloc: Allocator, req: Request) !SynthesisResult {
        return try self.vtable.synthesize(self.ptr, alloc, req);
    }

    pub fn deinit(self: Synthesizer) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const Runtime = struct {
    allocator: Allocator,
    synthesizers: std.StringArrayHashMapUnmanaged(Synthesizer) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Runtime {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Runtime) void {
        var it = self.synthesizers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.synthesizers.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn registerOwnedSynthesizer(self: *Runtime, name: []const u8, synthesizer: Synthesizer) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const gop = try self.synthesizers.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            synthesizer.deinit();
            return error.DuplicateSynthesizingProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = synthesizer;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn loadFromRegistry(self: *Runtime, http: *httpx.Client, registry: *const Registry) !void {
        var it = registry.configs.iterator();
        while (it.next()) |entry| {
            const synthesizer = try initSynthesizer(self.allocator, http, entry.value_ptr.*);
            errdefer synthesizer.deinit();
            try self.registerOwnedSynthesizer(entry.key_ptr.*, synthesizer);
        }
        if (registry.default_provider) |name| {
            const idx = self.synthesizers.getIndex(name) orelse return error.UnknownSynthesizingProvider;
            self.default_provider = self.synthesizers.keys()[idx];
        }
    }

    pub fn get(self: *const Runtime, name: ?[]const u8) !Synthesizer {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultSynthesizingProvider;
        return self.synthesizers.get(canonicalName(resolved)) orelse return error.UnknownSynthesizingProvider;
    }
};

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
        if (value != .object) return error.InvalidSynthesizingConfig;

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
            return error.DuplicateSynthesizingProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn defaultProviderName(self: *const Registry) ?[]const u8 {
        return self.default_provider;
    }

    pub fn getConfig(self: *const Registry, name: ?[]const u8) !Config {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultSynthesizingProvider;
        return self.configs.get(canonicalName(resolved)) orelse return error.UnknownSynthesizingProvider;
    }
};

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    var owned = cfg;
    inline for (@typeInfo(Config).@"struct".field_names, @typeInfo(Config).@"struct".field_types) |name, T| {
        if (T == ?[]const u8) @field(owned, name) = null;
    }
    errdefer deinitConfig(alloc, &owned);
    inline for (@typeInfo(Config).@"struct".field_names, @typeInfo(Config).@"struct".field_types) |name, T| {
        if (T == ?[]const u8) @field(owned, name) = try dupOpt(alloc, @field(cfg, name));
    }
    return owned;
}

pub fn deinitConfig(alloc: Allocator, cfg: *Config) void {
    inline for (@typeInfo(Config).@"struct".field_names, @typeInfo(Config).@"struct".field_types) |name, T| {
        if (T == ?[]const u8) freeOpt(alloc, @field(cfg, name));
    }
    cfg.* = undefined;
}

pub fn deinitResult(alloc: Allocator, result: *SynthesisResult) void {
    if (result.audio) |audio_bytes| alloc.free(audio_bytes);
    freeOpt(alloc, result.response.s3_url);
    result.* = undefined;
}

fn deinitConfigValue(alloc: Allocator, cfg: Config) void {
    var owned = cfg;
    deinitConfig(alloc, &owned);
}

pub const RemoteOptions = struct {
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,
};

pub fn synthesizeWithConfig(alloc: Allocator, http: *httpx.Client, cfg: Config, request: Request, options: RemoteOptions) !SynthesisResult {
    const synthesizer = try initSynthesizer(alloc, http, cfg);
    defer synthesizer.deinit();
    if (request.text.len == 0 or request.text.len > 65536 or request.s3_output != null or request.s3_credentials != null) return error.UnsupportedSynthesisOptions;
    if (request.speed) |speed| if (!std.math.isFinite(speed) or speed < 0.25 or speed > 4) return error.UnsupportedSynthesisOptions;
    switch (cfg.provider) {
        .cloudflare => {
            const state: *CloudflareSynthesizerState = @ptrCast(@alignCast(synthesizer.ptr));
            state.options = options;
        },
        .openai => {
            const state: *OpenAiSynthesizerState = @ptrCast(@alignCast(synthesizer.ptr));
            state.options = options;
        },
        else => {
            const state: *RemoteSynthesizerState = @ptrCast(@alignCast(synthesizer.ptr));
            state.options = options;
        },
    }
    return synthesizer.synthesize(alloc, request);
}

pub fn initSynthesizer(alloc: Allocator, http: *httpx.Client, cfg: Config) !Synthesizer {
    return switch (cfg.provider) {
        .cloudflare => try CloudflareSynthesizerState.init(alloc, http, cfg),
        .openai => try OpenAiSynthesizerState.init(alloc, http, cfg),
        .vertex, .elevenlabs => try RemoteSynthesizerState.init(alloc, http, cfg),
    };
}

const CloudflareSynthesizerState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    cfg: Config,
    options: RemoteOptions = .{},
    const cf = @import("antfly_cloudflare");
    fn init(a: Allocator, http: *httpx.Client, cfg: Config) !Synthesizer {
        try cf.routing.identifier(cfg.account_id orelse return error.InvalidCloudflareConfig);
        if (cfg.gateway_id) |id| try cf.routing.identifier(id);
        const model = cfg.model orelse return error.UnsupportedCloudflareModel;
        if (!std.mem.eql(u8, model, "@cf/deepgram/aura-2-en") and !std.mem.eql(u8, model, "@cf/myshell-ai/melotts")) return error.UnsupportedCloudflareModel;
        if (cfg.voice_id != null or cfg.voice_name != null or cfg.model_id != null or cfg.stability != null or cfg.similarity_boost != null or cfg.style != null) return error.UnsupportedSynthesisOptions;
        if (std.mem.eql(u8, model, "@cf/deepgram/aura-2-en")) if (cfg.language_code) |language| {
            if (!std.mem.eql(u8, language, "en") and !std.mem.eql(u8, language, "en-US")) return error.UnsupportedSynthesisOptions;
        };
        const self = try a.create(@This());
        errdefer a.destroy(self);
        self.* = .{ .alloc = a, .http = http, .cfg = try cloneConfig(a, cfg) };
        return .{ .ptr = self, .vtable = &.{ .synthesize = synthesize, .deinit = deinit } };
    }
    fn deinit(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        deinitConfig(self.alloc, &self.cfg);
        self.alloc.destroy(self);
    }
    fn synthesize(raw: *anyopaque, a: Allocator, req: Request) anyerror!SynthesisResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if ((req.format orelse .mp3) != .mp3 or (req.speed orelse 1) != 1) return error.UnsupportedSynthesisOptions;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const work = arena.allocator();
        const melo = std.mem.eql(u8, self.cfg.model.?, "@cf/myshell-ai/melotts");
        if (melo and (req.voice != null or self.cfg.voice != null)) return error.UnsupportedSynthesisOptions;
        const wire = if (melo) try std.json.Stringify.valueAlloc(work, cf.routing.wire.types.MelottsInput{ .prompt = req.text, .lang = self.cfg.language_code }, .{ .emit_null_optional_fields = false }) else try std.json.Stringify.valueAlloc(work, cf.routing.wire.types.Aura2EnInput{ .text = req.text, .speaker = req.voice orelse self.cfg.voice, .encoding = "mp3" }, .{ .emit_null_optional_fields = false });
        var response = try cf.postNative(self.http, work, .{ .account_id = self.cfg.account_id.?, .model = self.cfg.model.?, .api_key = self.cfg.api_key orelse return error.CloudflareCredentialMissing, .gateway_id = self.cfg.gateway_id, .url = self.cfg.base_url }, try std.json.parseFromSliceLeaky(std.json.Value, work, wire, .{}), .{ .max_response_bytes = @as(usize, 64 << 20), .timeout_ms = self.options.timeout_ms, .cancellation = self.options.cancellation });
        defer response.deinit();
        const bytes = response.body orelse return error.InvalidCloudflareResponse;
        const audio_bytes = if (std.mem.startsWith(u8, response.headers.get("content-type") orelse "", "application/json")) decoded: {
            const result = try std.json.parseFromSliceLeaky(std.json.Value, work, bytes, .{});
            if (cf.field(result, "success")) |success| if (success != .bool or !success.bool) return error.CloudflareInferenceFailed;
            const value = cf.field(result, "result") orelse return error.InvalidCloudflareResponse;
            const encoded = try cf.text(cf.field(value, "audio") orelse return error.InvalidCloudflareResponse);
            const decoded = try a.alloc(u8, std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidCloudflareResponse);
            errdefer a.free(decoded);
            std.base64.standard.Decoder.decode(decoded, encoded) catch return error.InvalidCloudflareResponse;
            break :decoded decoded;
        } else try a.dupe(u8, bytes);
        return .{ .audio = audio_bytes, .response = .{ .format = .mp3 } };
    }
};

const OpenAiSynthesizerState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,
    options: RemoteOptions = .{},
    model: []const u8,
    voice: []const u8,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Synthesizer {
        const state = try alloc.create(OpenAiSynthesizerState);
        errdefer alloc.destroy(state);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .base_url = try alloc.dupe(u8, cfg.base_url orelse "https://api.openai.com/v1"),
            .model = try alloc.dupe(u8, cfg.model orelse "tts-1"),
            .voice = try alloc.dupe(u8, cfg.voice orelse "alloy"),
        };
        if (cfg.api_key) |api_key| try state.setApiKey(api_key);

        return .{
            .ptr = state,
            .vtable = &.{
                .synthesize = synthesize,
                .deinit = deinit,
            },
        };
    }

    pub fn deinit(ptr: *anyopaque) void {
        const self: *OpenAiSynthesizerState = @ptrCast(@alignCast(ptr));
        self.alloc.free(self.base_url);
        self.alloc.free(self.model);
        self.alloc.free(self.voice);
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.alloc.destroy(self);
    }

    fn setApiKey(self: *OpenAiSynthesizerState, api_key: []const u8) !void {
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.auth_header = .{
            "Authorization",
            try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{api_key}),
        };
    }

    fn synthesize(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!SynthesisResult {
        const self: *OpenAiSynthesizerState = @ptrCast(@alignCast(ptr));
        if (req.format) |format| switch (format) {
            .ogg, .webm => return error.UnsupportedSynthesisOptions,
            else => {},
        };
        const url = try std.fmt.allocPrint(alloc, "{s}/audio/speech", .{self.base_url});
        defer alloc.free(url);

        const wire = try httpx.json.Json.stringify(alloc, .{
            .model = self.model,
            .input = req.text,
            .voice = req.voice orelse self.voice,
            .response_format = req.format,
            .speed = req.speed,
        });
        defer alloc.free(wire);

        var headers = std.ArrayList([2][]const u8).empty;
        defer headers.deinit(alloc);
        if (self.auth_header) |header| try headers.append(alloc, header);

        var resp = try self.http.post(url, .{
            .json = wire,
            .headers = headers.items,
            .max_response_size = 64 << 20,
            .timeout_ms = self.options.timeout_ms,
            .cancellation = self.options.cancellation,
            .max_retries = 0,
            .follow_redirects = false,
            .cookies_enabled = false,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.SynthesizeRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        return .{
            .audio = try alloc.dupe(u8, payload),
            .response = .{
                .format = req.format orelse .mp3,
                .characters_used = null,
            },
        };
    }
};

fn dupOpt(alloc: Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |v| try alloc.dupe(u8, v) else null;
}

fn freeOpt(alloc: Allocator, value: ?[]const u8) void {
    if (value) |v| alloc.free(v);
}

test "synthesizing registry preserves named providers and default" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "nova": { "provider": "openai", "model": "tts-1", "voice": "nova" },
        \\  "eleven": { "provider": "elevenlabs", "voice_id": "voice-123" }
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var registry = try Registry.parseFromValue(alloc, parsed.value);
    defer registry.deinit();

    try std.testing.expectEqualStrings("nova", registry.defaultProviderName().?);
    const default_cfg = try registry.getConfig(null);
    try std.testing.expectEqual(Provider.openai, default_cfg.provider);
    try std.testing.expectEqualStrings("nova", default_cfg.voice.?);

    const explicit_cfg = try registry.getConfig("eleven");
    try std.testing.expectEqual(Provider.elevenlabs, explicit_cfg.provider);
    try std.testing.expectEqualStrings("voice-123", explicit_cfg.voice_id.?);
}

test "synthesizing runtime loads openai provider and returns inline audio" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/audio/speech", .respond = .{
            .body = "FAKEAUDIO",
        } },
    });
    defer server.deinit();

    const base_url = try std.fmt.allocPrint(alloc, "{s}", .{server.baseUrl()});
    defer alloc.free(base_url);
    const cfg_json = try std.fmt.allocPrint(
        alloc,
        \\{{"narrator":{{"provider":"openai","base_url":"{s}","api_key":"secret","model":"tts-1","voice":"nova"}}}}
    ,
        .{base_url},
    );
    defer alloc.free(cfg_json);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, cfg_json, .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var registry = try Registry.parseFromValue(alloc, parsed.value);
    defer registry.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.loadFromRegistry(&client, &registry);

    var result: ?SynthesisResult = null;
    defer if (result) |*value| deinitResult(alloc, value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, synth_runtime: *Runtime, out: *?SynthesisResult, err_out: *?anyerror) std.Io.Cancelable!void {
            const synthesizer = synth_runtime.get(null) catch |err| {
                err_out.* = err;
                return;
            };
            out.* = synthesizer.synthesize(a, .{
                .text = "hello there",
                .format = .mp3,
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, &runtime, &result, &run_err }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("FAKEAUDIO", result.?.audio.?);
    try std.testing.expectEqual(audio.AudioFormat.mp3, result.?.response.format.?);
    try std.testing.expectEqual(@as(?i64, null), result.?.response.characters_used);
}

const RemoteSynthesizerState = struct {
    allocator: Allocator,
    http: *httpx.Client,
    cfg: Config,
    options: RemoteOptions = .{},
    fn init(a: Allocator, http: *httpx.Client, cfg: Config) !Synthesizer {
        const self = try a.create(RemoteSynthesizerState);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .http = http, .cfg = try cloneConfig(a, cfg) };
        return .{ .ptr = self, .vtable = &.{ .synthesize = synthesize, .deinit = deinit } };
    }
    fn deinit(raw: *anyopaque) void {
        const self: *RemoteSynthesizerState = @ptrCast(@alignCast(raw));
        deinitConfig(self.allocator, &self.cfg);
        self.allocator.destroy(self);
    }
    fn synthesize(raw: *anyopaque, a: Allocator, request: Request) anyerror!SynthesisResult {
        const self: *RemoteSynthesizerState = @ptrCast(@alignCast(raw));
        const cfg = self.cfg;
        const auth = @import("antfly_google").auth;
        const control = auth.RequestControl.fromTimeout(self.options.timeout_ms, self.options.cancellation);
        const format = request.format orelse .mp3;
        const base = std.mem.trimEnd(u8, cfg.base_url orelse (if (cfg.provider == .elevenlabs) "https://api.elevenlabs.io/v1" else "https://texttospeech.googleapis.com/v1"), "/");
        var headers: std.ArrayList([2][]const u8) = .empty;
        defer headers.deinit(a);
        var token_source: ?@import("antfly_google").auth.CachedTokenSource = null;
        defer if (token_source) |*source| source.deinit();
        var authorization: ?[]u8 = null;
        defer if (authorization) |value| a.free(value);
        var url: []u8 = undefined;
        var body: []u8 = undefined;
        if (cfg.provider == .elevenlabs) {
            const voice = request.voice orelse cfg.voice_id orelse return error.InvalidSynthesizingConfig;
            if (voice.len == 0 or std.mem.indexOfAny(u8, voice, "/?#%") != null) return error.InvalidSynthesizingConfig;
            if (format != .mp3 or (request.speed != null and request.speed.? != 1)) return error.UnsupportedSynthesisOptions;
            const key = cfg.api_key orelse return error.MissingSynthesisApiKey;
            try headers.append(a, .{ "xi-api-key", key });
            url = try std.fmt.allocPrint(a, "{s}/text-to-speech/{s}?output_format=mp3_44100_128", .{ base, voice });
            body = try std.json.Stringify.valueAlloc(a, .{ .text = request.text, .model_id = cfg.model_id orelse "eleven_multilingual_v2", .voice_settings = .{ .stability = cfg.stability orelse 0.5, .similarity_boost = cfg.similarity_boost orelse 0.75, .style = cfg.style orelse 0 } }, .{});
        } else {
            if (request.text.len > 5000) return error.UnsupportedSynthesisOptions;
            const encoding = switch (format) {
                .mp3 => "MP3",
                .wav => "LINEAR16",
                .opus => "OGG_OPUS",
                else => return error.UnsupportedSynthesisOptions,
            };
            if (cfg.api_key) |key| {
                authorization = try std.fmt.allocPrint(a, "Bearer {s}", .{key});
            } else {
                const credentials = if (cfg.credentials_path) |path| try auth.configFromFileAlloc(a, path, "https://www.googleapis.com/auth/cloud-platform") else try auth.configFromEnvAlloc(a, "https://www.googleapis.com/auth/cloud-platform");
                token_source = try auth.CachedTokenSource.init(a, credentials);
                authorization = try token_source.?.authorizationValueAllocWithControl(a, control);
            }
            try headers.append(a, .{ "Authorization", authorization.? });
            if (cfg.project_id) |project| try headers.append(a, .{ "x-goog-user-project", project });
            url = try std.fmt.allocPrint(a, "{s}/text:synthesize", .{base});
            body = try std.json.Stringify.valueAlloc(a, .{ .input = .{ .text = request.text }, .voice = .{ .languageCode = cfg.language_code orelse "en-US", .name = request.voice orelse cfg.voice_name orelse "en-US-Neural2-A" }, .audioConfig = .{ .audioEncoding = encoding, .speakingRate = request.speed orelse 1 } }, .{});
        }
        defer a.free(url);
        defer a.free(body);
        var response = try self.http.post(url, .{ .json = body, .headers = headers.items, .timeout_ms = try control.remainingTimeoutMs(), .cancellation = self.options.cancellation, .max_response_size = 64 << 20, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false });
        defer response.deinit();
        if (!response.ok()) return error.SynthesizeRequestFailed;
        const payload = response.body orelse return error.EmptyResponse;
        if (cfg.provider == .elevenlabs) return .{ .audio = try a.dupe(u8, payload), .response = .{ .format = format } };
        const parsed = try std.json.parseFromSlice(struct { audioContent: []const u8 }, a, payload, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const bytes = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(parsed.value.audioContent));
        errdefer a.free(bytes);
        try std.base64.standard.Decoder.decode(bytes, parsed.value.audioContent);
        return .{ .audio = bytes, .response = .{ .format = format } };
    }
};

fn canonicalName(name: []const u8) []const u8 {
    const prefix = "synthesizers/";
    return if (std.mem.startsWith(u8, name, prefix)) name[prefix.len..] else name;
}

test "synthesizing remote adapters preserve provider wire contracts and actual usage" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Checks = struct {
        fn vertex(request: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer fixture-token", request.header("authorization").?);
            const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, request.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("hello", parsed.value.object.get("input").?.object.get("text").?.string);
            try std.testing.expectEqualStrings("LINEAR16", parsed.value.object.get("audioConfig").?.object.get("audioEncoding").?.string);
        }
        fn eleven(request: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("fixture-token", request.header("xi-api-key").?);
            try std.testing.expectEqualStrings("output_format=mp3_44100_128", request.query);
            const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, request.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("hello", parsed.value.object.get("text").?.string);
            try std.testing.expectEqualStrings("fixture-model", parsed.value.object.get("model_id").?.string);
        }
        fn run(alloc: Allocator, client: *httpx.Client, cfg: Config, format: audio.AudioFormat, out: *?SynthesisResult, failure: *?anyerror) std.Io.Cancelable!void {
            out.* = synthesizeWithConfig(alloc, client, cfg, .{ .text = "hello", .format = format }, .{ .timeout_ms = 5000 }) catch |err| {
                failure.* = err;
                return;
            };
        }
    };
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/text:synthesize", .respond = .{ .body = "{\"audioContent\":\"RkFLRUFVRElP\"}" }, .assert_request = Checks.vertex },
        .{ .method = .POST, .path = "/text-to-speech/fixture-voice", .respond = .{ .body = "FAKEAUDIO" }, .assert_request = Checks.eleven },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer client.deinit();
    for ([_]Provider{ .vertex, .elevenlabs }) |provider| {
        var result: ?SynthesisResult = null;
        defer if (result) |*value| deinitResult(a, value);
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        const format: audio.AudioFormat = if (provider == .vertex) .wav else .mp3;
        try group.concurrent(io, Checks.run, .{ a, &client, Config{ .provider = provider, .base_url = server.baseUrl(), .api_key = "fixture-token", .voice_id = "fixture-voice", .model_id = "fixture-model" }, format, &result, &failure });
        try server.handleOne();
        group.await(io) catch {};
        if (failure) |err| return err;
        try std.testing.expectEqualStrings("FAKEAUDIO", result.?.audio.?);
        try std.testing.expectEqual(format, result.?.response.format.?);
        try std.testing.expectEqual(@as(?i64, null), result.?.response.characters_used);
    }
    try std.testing.expectError(error.UnsupportedSynthesisOptions, synthesizeWithConfig(a, &client, .{ .provider = .elevenlabs, .api_key = "fixture-token", .voice_id = "fixture-voice" }, .{ .text = "hello", .format = .wav }, .{}));
}

pub fn testCloudflareSynthesis() !void {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var server = try httpx.TestServer.start(a, io, &.{.{ .method = .POST, .path = "/", .respond = .{ .body = "ID3fixture", .content_type = "audio/mpeg" } }});
    defer server.deinit();
    var client = httpx.Client.init(a, io);
    defer client.deinit();
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(alloc: Allocator, http: *httpx.Client, url: []const u8, out: *?anyerror) std.Io.Cancelable!void {
            var result = synthesizeWithConfig(alloc, http, .{ .provider = .cloudflare, .model = "@cf/deepgram/aura-2-en", .account_id = "account", .api_key = "fixture", .base_url = url }, .{ .text = "hello" }, .{}) catch |err| {
                out.* = err;
                return;
            };
            defer deinitResult(alloc, &result);
            std.testing.expectEqualStrings("ID3fixture", result.audio.?) catch |err| {
                out.* = err;
            };
        }
    };
    group.concurrent(io, Fiber.run, .{ a, &client, server.baseUrl(), &failure }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (failure) |err| return err;
}
