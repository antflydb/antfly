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

const Allocator = std.mem.Allocator;

pub const Provider = enum {
    antfly,
    openai,
    google,
};

pub const Config = struct {
    model: ?[]u8 = null,
    api_key: ?[]u8 = null,
    base_url: ?[]u8 = null,
    project_id: ?[]u8 = null,
    location: ?[]u8 = null,
    credentials_path: ?[]u8 = null,
    language_code: ?[]u8 = null,
    enable_automatic_punctuation: ?bool = null,
    use_enhanced: ?bool = null,
    api_url: ?[]u8 = null,
    provider: Provider = .antfly,
};

pub const Request = struct {
    url: []const u8,
    language: ?[]const u8 = null,
    timestamps: ?bool = null,
    diarization: ?bool = null,
};

pub const WordTimestamp = struct {};

pub const Segment = struct {
    text: ?[]const u8 = null,
    speaker: ?[]const u8 = null,
};

pub const Speaker = struct {
    name: ?[]const u8 = null,
};

pub const Response = struct {
    text: ?[]const u8 = null,
    language: ?[]const u8 = null,
    segments: ?[]const Segment = null,
    speakers: ?[]const Speaker = null,
};

pub const Transcriber = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        transcribe: *const fn (ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn transcribe(self: Transcriber, alloc: Allocator, req: Request) !Response {
        return try self.vtable.transcribe(self.ptr, alloc, req);
    }

    pub fn deinit(self: Transcriber) void {
        self.vtable.deinit(self.ptr);
    }
};

threadlocal var active_runtime: ?*const Runtime = null;

pub const Runtime = struct {
    allocator: Allocator,
    transcribers: std.StringArrayHashMapUnmanaged(Transcriber) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Runtime {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Runtime) void {
        var it = self.transcribers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.transcribers.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn registerOwnedTranscriber(self: *Runtime, name: []const u8, transcriber: Transcriber) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const gop = try self.transcribers.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            transcriber.deinit();
            return error.DuplicateTranscribingProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = transcriber;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn loadFromRegistry(self: *Runtime, _: *httpx.Client, registry: *const Registry) !void {
        self.default_provider = registry.default_provider;
    }

    pub fn get(self: *const Runtime, name: ?[]const u8) !Transcriber {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultTranscribingProvider;
        return self.transcribers.get(resolved) orelse return error.UnknownTranscribingProvider;
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
        if (value != .object) return error.InvalidTranscribingConfig;

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
            return error.DuplicateTranscribingProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn defaultProviderName(self: *const Registry) ?[]const u8 {
        return self.default_provider;
    }

    pub fn getConfig(self: *const Registry, name: ?[]const u8) !Config {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultTranscribingProvider;
        return self.configs.get(resolved) orelse return error.UnknownTranscribingProvider;
    }
};

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    return .{
        .model = try dupOpt(alloc, cfg.model),
        .api_key = try dupOpt(alloc, cfg.api_key),
        .base_url = try dupOpt(alloc, cfg.base_url),
        .project_id = try dupOpt(alloc, cfg.project_id),
        .location = try dupOpt(alloc, cfg.location),
        .credentials_path = try dupOpt(alloc, cfg.credentials_path),
        .language_code = try dupOpt(alloc, cfg.language_code),
        .enable_automatic_punctuation = cfg.enable_automatic_punctuation,
        .use_enhanced = cfg.use_enhanced,
        .api_url = try dupOpt(alloc, cfg.api_url),
        .provider = cfg.provider,
    };
}

pub fn deinitConfig(alloc: Allocator, cfg: *Config) void {
    freeOpt(alloc, cfg.model);
    freeOpt(alloc, cfg.api_key);
    freeOpt(alloc, cfg.base_url);
    freeOpt(alloc, cfg.project_id);
    freeOpt(alloc, cfg.location);
    freeOpt(alloc, cfg.credentials_path);
    freeOpt(alloc, cfg.language_code);
    freeOpt(alloc, cfg.api_url);
    cfg.* = undefined;
}

pub fn deinitResponse(alloc: Allocator, response: *Response) void {
    freeOpt(alloc, response.text);
    freeOpt(alloc, response.language);
    if (response.segments) |segments| {
        for (segments) |segment| {
            const owned = segment;
            freeOpt(alloc, owned.text);
            freeOpt(alloc, owned.speaker);
        }
        alloc.free(@constCast(segments));
    }
    if (response.speakers) |speakers| {
        for (speakers) |speaker| {
            const owned = speaker;
            freeOpt(alloc, owned.name);
        }
        alloc.free(@constCast(speakers));
    }
    response.* = undefined;
}

fn deinitConfigValue(alloc: Allocator, cfg: Config) void {
    var owned = cfg;
    deinitConfig(alloc, &owned);
}

fn dupOpt(alloc: Allocator, value: ?[]const u8) !?[]u8 {
    return if (value) |v| try alloc.dupe(u8, v) else null;
}

fn freeOpt(alloc: Allocator, value: ?[]const u8) void {
    if (value) |v| alloc.free(@constCast(v));
}
