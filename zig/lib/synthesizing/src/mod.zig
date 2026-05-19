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
        return self.synthesizers.get(resolved) orelse return error.UnknownSynthesizingProvider;
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
        return self.configs.get(resolved) orelse return error.UnknownSynthesizingProvider;
    }
};

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    return .{
        .model = try dupOpt(alloc, cfg.model),
        .api_key = try dupOpt(alloc, cfg.api_key),
        .voice = try dupOpt(alloc, cfg.voice),
        .base_url = try dupOpt(alloc, cfg.base_url),
        .project_id = try dupOpt(alloc, cfg.project_id),
        .location = try dupOpt(alloc, cfg.location),
        .credentials_path = try dupOpt(alloc, cfg.credentials_path),
        .language_code = try dupOpt(alloc, cfg.language_code),
        .voice_name = try dupOpt(alloc, cfg.voice_name),
        .voice_id = try dupOpt(alloc, cfg.voice_id),
        .model_id = try dupOpt(alloc, cfg.model_id),
        .stability = cfg.stability,
        .similarity_boost = cfg.similarity_boost,
        .style = cfg.style,
        .provider = cfg.provider,
    };
}

pub fn deinitConfig(alloc: Allocator, cfg: *Config) void {
    freeOpt(alloc, cfg.model);
    freeOpt(alloc, cfg.api_key);
    freeOpt(alloc, cfg.voice);
    freeOpt(alloc, cfg.base_url);
    freeOpt(alloc, cfg.project_id);
    freeOpt(alloc, cfg.location);
    freeOpt(alloc, cfg.credentials_path);
    freeOpt(alloc, cfg.language_code);
    freeOpt(alloc, cfg.voice_name);
    freeOpt(alloc, cfg.voice_id);
    freeOpt(alloc, cfg.model_id);
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

fn initSynthesizer(alloc: Allocator, http: *httpx.Client, cfg: Config) !Synthesizer {
    return switch (cfg.provider) {
        .openai => try OpenAiSynthesizerState.init(alloc, http, cfg),
        else => error.UnsupportedSynthesizingProvider,
    };
}

const OpenAiSynthesizerState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,
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

    fn deinit(ptr: *anyopaque) void {
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
        });
        defer resp.deinit();
        if (!resp.ok()) return error.SynthesizeRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        return .{
            .audio = try alloc.dupe(u8, payload),
            .response = .{
                .format = req.format orelse .mp3,
                .characters_used = @intCast(req.text.len),
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
    try std.testing.expectEqual(@as(i64, 11), result.?.response.characters_used.?);
}
