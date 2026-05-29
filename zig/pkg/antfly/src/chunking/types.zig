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
const build_options = @import("build_options");

const lib = if (build_options.bench_minimal_deps) struct {} else @import("antfly_chunking");
const Allocator = std.mem.Allocator;

pub const Provider = if (build_options.bench_minimal_deps) enum {
    mock,
    antfly,

    pub fn fromSlice(raw: []const u8) !Provider {
        if (std.mem.eql(u8, raw, "mock")) return .mock;
        if (std.mem.eql(u8, raw, "antfly")) return .antfly;
        return error.InvalidChunkerConfig;
    }
} else lib.Provider;

pub const TextChunkOptions = if (build_options.bench_minimal_deps) struct {
    target_tokens: u32 = 0,
    overlap_tokens: u32 = 0,
    separator: []const u8 = "",

    pub fn clone(self: TextChunkOptions, alloc: Allocator) !TextChunkOptions {
        return .{
            .target_tokens = self.target_tokens,
            .overlap_tokens = self.overlap_tokens,
            .separator = if (self.separator.len > 0) try alloc.dupe(u8, self.separator) else "",
        };
    }

    pub fn deinit(self: *TextChunkOptions, alloc: Allocator) void {
        if (self.separator.len > 0) alloc.free(self.separator);
        self.* = undefined;
    }
} else lib.TextChunkOptions;

pub const AudioChunkOptions = if (build_options.bench_minimal_deps) struct {
    window_duration_ms: u32 = 0,
    overlap_duration_ms: u32 = 0,
} else lib.AudioChunkOptions;

pub const Config = if (build_options.bench_minimal_deps) struct {
    provider: Provider,
    api_url: []const u8 = "",
    model: []const u8 = "",
    store_chunks: bool = false,
    full_text_index_json: []const u8 = "",
    max_chunks: u32 = 0,
    threshold: ?f32 = null,
    text: TextChunkOptions = .{},
    audio: AudioChunkOptions = .{},

    pub fn clone(self: Config, alloc: Allocator) !Config {
        return .{
            .provider = self.provider,
            .api_url = if (self.api_url.len > 0) try alloc.dupe(u8, self.api_url) else "",
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .store_chunks = self.store_chunks,
            .full_text_index_json = if (self.full_text_index_json.len > 0) try alloc.dupe(u8, self.full_text_index_json) else "",
            .max_chunks = self.max_chunks,
            .threshold = self.threshold,
            .text = try self.text.clone(alloc),
            .audio = self.audio,
        };
    }

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.api_url.len > 0) alloc.free(self.api_url);
        if (self.model.len > 0) alloc.free(self.model);
        if (self.full_text_index_json.len > 0) alloc.free(self.full_text_index_json);
        self.text.deinit(alloc);
        self.* = undefined;
    }

    pub fn hasTextChunking(self: Config) bool {
        return self.text.target_tokens > 0;
    }

    pub fn defaultedTargetTokens(self: Config) u32 {
        return if (self.text.target_tokens > 0) self.text.target_tokens else 500;
    }

    pub fn defaultedOverlapTokens(self: Config) u32 {
        return self.text.overlap_tokens;
    }

    pub fn defaultedSeparator(self: Config) []const u8 {
        return if (self.text.separator.len > 0) self.text.separator else "\n\n";
    }
} else lib.Config;

pub fn parseConfigFromSlice(alloc: Allocator, raw: []const u8) !Config {
    if (!build_options.bench_minimal_deps) return lib.parseConfigFromSlice(alloc, raw);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    return try parseConfigFromValue(alloc, parsed.value);
}

pub fn parseConfigFromValue(alloc: Allocator, value: std.json.Value) !Config {
    if (!build_options.bench_minimal_deps) return lib.parseConfigFromValue(alloc, value);
    if (value != .object) return error.InvalidChunkerConfig;

    var cfg = Config{
        .provider = .antfly,
    };
    errdefer cfg.deinit(alloc);

    if (value.object.get("provider")) |provider_value| {
        if (provider_value != .string) return error.InvalidChunkerConfig;
        cfg.provider = try Provider.fromSlice(provider_value.string);
    }
    if (value.object.get("api_url")) |api_url| {
        if (api_url != .string) return error.InvalidChunkerConfig;
        cfg.api_url = try alloc.dupe(u8, api_url.string);
    }
    if (value.object.get("model")) |model| {
        if (model != .string) return error.InvalidChunkerConfig;
        cfg.model = try alloc.dupe(u8, model.string);
    }
    if (value.object.get("store_chunks")) |store_chunks| {
        if (store_chunks != .bool) return error.InvalidChunkerConfig;
        cfg.store_chunks = store_chunks.bool;
    }
    if (value.object.get("full_text_index")) |full_text_index| {
        cfg.full_text_index_json = try std.json.Stringify.valueAlloc(alloc, full_text_index, .{});
    }
    if (value.object.get("max_chunks")) |max_chunks| {
        cfg.max_chunks = parseU32Value(max_chunks) orelse return error.InvalidChunkerConfig;
    }
    if (value.object.get("threshold")) |threshold| {
        cfg.threshold = switch (threshold) {
            .float => @floatCast(threshold.float),
            .integer => @floatFromInt(threshold.integer),
            else => return error.InvalidChunkerConfig,
        };
    }
    if (value.object.get("text")) |text_value| {
        if (text_value != .object) return error.InvalidChunkerConfig;
        if (text_value.object.get("target_tokens")) |target_tokens| {
            cfg.text.target_tokens = parseU32Value(target_tokens) orelse return error.InvalidChunkerConfig;
        }
        if (text_value.object.get("overlap_tokens")) |overlap_tokens| {
            cfg.text.overlap_tokens = parseU32Value(overlap_tokens) orelse return error.InvalidChunkerConfig;
        }
        if (text_value.object.get("separator")) |separator| {
            if (separator != .string) return error.InvalidChunkerConfig;
            cfg.text.separator = try alloc.dupe(u8, separator.string);
        }
    }
    if (value.object.get("audio")) |audio_value| {
        if (audio_value != .object) return error.InvalidChunkerConfig;
        if (audio_value.object.get("window_duration_ms")) |window_ms| {
            cfg.audio.window_duration_ms = parseU32Value(window_ms) orelse return error.InvalidChunkerConfig;
        }
        if (audio_value.object.get("overlap_duration_ms")) |overlap_ms| {
            cfg.audio.overlap_duration_ms = parseU32Value(overlap_ms) orelse return error.InvalidChunkerConfig;
        }
    }

    if (cfg.provider == .antfly and cfg.api_url.len > 0 and cfg.model.len == 0) return error.InvalidChunkerConfig;
    return cfg;
}

pub fn parseStoreChunksFromSlice(alloc: Allocator, raw: []const u8) !bool {
    if (!build_options.bench_minimal_deps) return lib.parseStoreChunksFromSlice(alloc, raw);
    if (raw.len == 0) return false;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    return cfg.store_chunks;
}

pub fn parseHasFullTextIndexFromSlice(alloc: Allocator, raw: []const u8) !bool {
    if (!build_options.bench_minimal_deps) return lib.parseHasFullTextIndexFromSlice(alloc, raw);
    if (raw.len == 0) return false;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    return cfg.full_text_index_json.len > 0;
}

pub fn stringifyAlloc(alloc: Allocator, cfg: Config) ![]u8 {
    if (!build_options.bench_minimal_deps) return lib.stringifyAlloc(alloc, cfg);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    var wrote = false;
    try appendJsonFieldName(alloc, &out, &wrote, "provider");
    try appendJsonString(alloc, &out, switch (cfg.provider) {
        .mock => "mock",
        .antfly => "antfly",
    });
    if (cfg.api_url.len > 0) {
        try appendJsonFieldName(alloc, &out, &wrote, "api_url");
        try appendJsonString(alloc, &out, cfg.api_url);
    }
    if (cfg.model.len > 0) {
        try appendJsonFieldName(alloc, &out, &wrote, "model");
        try appendJsonString(alloc, &out, cfg.model);
    }
    if (cfg.store_chunks) {
        try appendJsonFieldName(alloc, &out, &wrote, "store_chunks");
        try out.appendSlice(alloc, "true");
    }
    if (cfg.full_text_index_json.len > 0) {
        try appendJsonFieldName(alloc, &out, &wrote, "full_text_index");
        try out.appendSlice(alloc, cfg.full_text_index_json);
    }
    if (cfg.max_chunks != 0) {
        try appendJsonFieldName(alloc, &out, &wrote, "max_chunks");
        try appendJsonNumber(alloc, &out, cfg.max_chunks);
    }
    if (cfg.threshold) |threshold| {
        try appendJsonFieldName(alloc, &out, &wrote, "threshold");
        try appendJsonFloat(alloc, &out, threshold);
    }
    if (cfg.text.target_tokens != 0 or cfg.text.overlap_tokens != 0 or cfg.text.separator.len > 0) {
        try appendJsonFieldName(alloc, &out, &wrote, "text");
        try out.append(alloc, '{');
        var wrote_text = false;
        if (cfg.text.target_tokens != 0) {
            try appendJsonFieldName(alloc, &out, &wrote_text, "target_tokens");
            try appendJsonNumber(alloc, &out, cfg.text.target_tokens);
        }
        if (cfg.text.overlap_tokens != 0) {
            try appendJsonFieldName(alloc, &out, &wrote_text, "overlap_tokens");
            try appendJsonNumber(alloc, &out, cfg.text.overlap_tokens);
        }
        if (cfg.text.separator.len > 0) {
            try appendJsonFieldName(alloc, &out, &wrote_text, "separator");
            try appendJsonString(alloc, &out, cfg.text.separator);
        }
        try out.append(alloc, '}');
    }
    if (cfg.audio.window_duration_ms != 0 or cfg.audio.overlap_duration_ms != 0) {
        try appendJsonFieldName(alloc, &out, &wrote, "audio");
        try out.append(alloc, '{');
        var wrote_audio = false;
        if (cfg.audio.window_duration_ms != 0) {
            try appendJsonFieldName(alloc, &out, &wrote_audio, "window_duration_ms");
            try appendJsonNumber(alloc, &out, cfg.audio.window_duration_ms);
        }
        if (cfg.audio.overlap_duration_ms != 0) {
            try appendJsonFieldName(alloc, &out, &wrote_audio, "overlap_duration_ms");
            try appendJsonNumber(alloc, &out, cfg.audio.overlap_duration_ms);
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn parseU32Value(value: std.json.Value) ?u32 {
    return switch (value) {
        .integer => std.math.cast(u32, value.integer),
        else => null,
    };
}

fn appendJsonFieldName(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), wrote_any: *bool, name: []const u8) !void {
    if (wrote_any.*) try out.append(alloc, ',');
    wrote_any.* = true;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try out.append(alloc, '"');
    for (value) |c| switch (c) {
        '"' => try out.appendSlice(alloc, "\\\""),
        '\\' => try out.appendSlice(alloc, "\\\\"),
        '\n' => try out.appendSlice(alloc, "\\n"),
        '\r' => try out.appendSlice(alloc, "\\r"),
        '\t' => try out.appendSlice(alloc, "\\t"),
        else => if (c < 0x20) {
            var buf: [6]u8 = undefined;
            _ = try std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{@as(u32, c)});
            try out.appendSlice(alloc, &buf);
        } else {
            try out.append(alloc, c);
        },
    };
    try out.append(alloc, '"');
}

fn appendJsonNumber(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: anytype) !void {
    const raw = try std.fmt.allocPrint(alloc, "{}", .{value});
    defer alloc.free(raw);
    try out.appendSlice(alloc, raw);
}

fn appendJsonFloat(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: f32) !void {
    const raw = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(raw);
    try out.appendSlice(alloc, raw);
}
