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
const json = @import("antfly-json");
const provider_openapi = @import("antfly_chunking_openapi");
const api_openapi = @import("antfly_chunking_api_openapi");

const Allocator = std.mem.Allocator;

pub const Provider = enum {
    mock,
    antfly,

    pub fn fromSlice(raw: []const u8) !Provider {
        if (std.mem.eql(u8, raw, "mock")) return .mock;
        if (std.mem.eql(u8, raw, "antfly")) return .antfly;
        return error.InvalidChunkerConfig;
    }

    pub fn toSlice(self: Provider) []const u8 {
        return switch (self) {
            .mock => "mock",
            .antfly => "antfly",
        };
    }
};

pub const TextChunkOptions = struct {
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
};

pub const AudioChunkOptions = struct {
    window_duration_ms: u32 = 0,
    overlap_duration_ms: u32 = 0,
};

pub const Config = struct {
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
};

pub fn parseConfigFromSlice(alloc: Allocator, raw: []const u8) !Config {
    const parsed = try json.parseFromSlice(json.Value, alloc, raw, .{});
    defer parsed.deinit();
    return try parseConfigFromValue(alloc, parsed.value);
}

pub fn parseConfigFromValue(alloc: Allocator, value: json.Value) !Config {
    const provider_cfg = try json.parseFromValue(provider_openapi.ChunkerConfig, alloc, value, .{
        .ignore_unknown_fields = true,
    });
    defer provider_cfg.deinit();

    const chunk_options = try json.parseFromValue(api_openapi.ChunkOptions, alloc, value, .{
        .ignore_unknown_fields = true,
    });
    defer chunk_options.deinit();

    var cfg = try configFromOpenApi(alloc, provider_cfg.value, chunk_options.value);
    errdefer cfg.deinit(alloc);
    if (value == .object) {
        if (value.object.get("url")) |url_value| {
            if (url_value == .null) return cfg;
            if (url_value != .string) return error.InvalidChunkerConfig;
            if (cfg.api_url.len > 0) alloc.free(@constCast(cfg.api_url));
            cfg.api_url = try alloc.dupe(u8, url_value.string);
        }
    }
    return cfg;
}

pub fn parseStoreChunksFromSlice(alloc: Allocator, raw: []const u8) !bool {
    if (raw.len == 0) return false;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    return cfg.store_chunks;
}

pub fn parseHasFullTextIndexFromSlice(alloc: Allocator, raw: []const u8) !bool {
    if (raw.len == 0) return false;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    return cfg.full_text_index_json.len > 0;
}

pub fn stringifyAlloc(alloc: Allocator, cfg: Config) ![]u8 {
    var generated = openApiFromConfig(cfg);
    if (cfg.full_text_index_json.len > 0) {
        const parsed = try json.parseFromSlice(json.Value, alloc, cfg.full_text_index_json, .{});
        defer parsed.deinit();
        generated.full_text_index = parsed.value;
        return try json.Stringify.valueAlloc(alloc, generated, .{});
    }
    return try json.Stringify.valueAlloc(alloc, generated, .{});
}

fn configFromOpenApi(
    alloc: Allocator,
    provider_cfg: provider_openapi.ChunkerConfig,
    chunk_options: api_openapi.ChunkOptions,
) !Config {
    var cfg = Config{
        .provider = try Provider.fromSlice(@tagName(provider_cfg.provider)),
        .api_url = if (provider_cfg.api_url) |api_url| try alloc.dupe(u8, api_url) else "",
        .model = if (provider_cfg.model) |model| try alloc.dupe(u8, model) else "",
        .store_chunks = provider_cfg.store_chunks orelse false,
        .full_text_index_json = "",
        .max_chunks = if (chunk_options.max_chunks) |max_chunks|
            std.math.cast(u32, max_chunks) orelse return error.InvalidChunkerConfig
        else
            0,
        .threshold = chunk_options.threshold,
        .text = try textChunkOptionsFromOpenApi(alloc, chunk_options.text),
        .audio = try audioChunkOptionsFromOpenApi(chunk_options.audio),
    };
    errdefer cfg.deinit(alloc);

    if (provider_cfg.full_text_index) |full_text_index| {
        cfg.full_text_index_json = try json.Stringify.valueAlloc(alloc, full_text_index, .{});
    }

    if (cfg.provider == .antfly and cfg.api_url.len > 0 and cfg.model.len == 0) return error.InvalidChunkerConfig;
    return cfg;
}

fn textChunkOptionsFromOpenApi(alloc: Allocator, generated: ?api_openapi.TextChunkOptions) !TextChunkOptions {
    return if (generated) |value| .{
        .target_tokens = if (value.target_tokens) |target|
            std.math.cast(u32, target) orelse return error.InvalidChunkerConfig
        else
            0,
        .overlap_tokens = if (value.overlap_tokens) |overlap|
            std.math.cast(u32, overlap) orelse return error.InvalidChunkerConfig
        else
            0,
        .separator = if (value.separator) |separator| try alloc.dupe(u8, separator) else "",
    } else .{};
}

fn audioChunkOptionsFromOpenApi(generated: ?api_openapi.AudioChunkOptions) !AudioChunkOptions {
    return if (generated) |value| .{
        .window_duration_ms = if (value.window_duration_ms) |window|
            std.math.cast(u32, window) orelse return error.InvalidChunkerConfig
        else
            0,
        .overlap_duration_ms = if (value.overlap_duration_ms) |overlap|
            std.math.cast(u32, overlap) orelse return error.InvalidChunkerConfig
        else
            0,
    } else .{};
}

const CombinedChunkerConfig = struct {
    api_url: ?[]const u8 = null,
    url: ?[]const u8 = null,
    model: ?[]const u8 = null,
    provider: provider_openapi.ChunkerProvider,
    store_chunks: ?bool = null,
    full_text_index: ?json.Value = null,
    max_chunks: ?u32 = null,
    threshold: ?f32 = null,
    text: ?api_openapi.TextChunkOptions = null,
    audio: ?api_openapi.AudioChunkOptions = null,
};

fn openApiFromConfig(cfg: Config) CombinedChunkerConfig {
    const provider: provider_openapi.ChunkerProvider = switch (cfg.provider) {
        .antfly => .antfly,
        .mock => .mock,
    };
    return .{
        .max_chunks = if (cfg.max_chunks > 0) cfg.max_chunks else null,
        .threshold = cfg.threshold,
        .text = if (cfg.text.target_tokens > 0 or cfg.text.overlap_tokens > 0 or cfg.text.separator.len > 0) .{
            .target_tokens = if (cfg.text.target_tokens > 0) cfg.text.target_tokens else null,
            .overlap_tokens = if (cfg.text.overlap_tokens > 0) cfg.text.overlap_tokens else null,
            .separator = if (cfg.text.separator.len > 0) cfg.text.separator else null,
        } else null,
        .audio = if (cfg.audio.window_duration_ms > 0 or cfg.audio.overlap_duration_ms > 0) .{
            .window_duration_ms = if (cfg.audio.window_duration_ms > 0) cfg.audio.window_duration_ms else null,
            .overlap_duration_ms = if (cfg.audio.overlap_duration_ms > 0) cfg.audio.overlap_duration_ms else null,
        } else null,
        .api_url = if (cfg.provider == .termite and cfg.api_url.len > 0) cfg.api_url else null,
        .url = if (cfg.provider == .antfly and cfg.api_url.len > 0) cfg.api_url else null,
        .model = if (cfg.model.len > 0) cfg.model else null,
        .provider = provider,
        .store_chunks = if (cfg.store_chunks) true else null,
        .full_text_index = null,
    };
}

test "chunker config round trip" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"antfly","api_url":"http://localhost:8080","model":"fixed","store_chunks":true,"max_chunks":7,"threshold":0.5,"text":{"target_tokens":128,"overlap_tokens":16,"separator":"\n\n"},"full_text_index":{}}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqual(.antfly, cfg.provider);
    try std.testing.expectEqualStrings("http://localhost:8080", cfg.api_url);
    try std.testing.expectEqualStrings("fixed", cfg.model);
    try std.testing.expect(cfg.store_chunks);
    try std.testing.expectEqual(@as(u32, 128), cfg.text.target_tokens);

    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.antfly, reparsed.provider);
    try std.testing.expectEqualStrings("fixed", reparsed.model);
    try std.testing.expectEqual(@as(u32, 16), reparsed.text.overlap_tokens);
}

test "chunker config requires model for remote antfly provider" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidChunkerConfig,
        parseConfigFromSlice(alloc, "{\"provider\":\"antfly\",\"api_url\":\"http://localhost:8080\"}"),
    );
}

test "chunker config detects full text indexing" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!(try parseHasFullTextIndexFromSlice(alloc, "")));
    try std.testing.expect(try parseHasFullTextIndexFromSlice(
        alloc,
        "{\"provider\":\"antfly\",\"text\":{\"target_tokens\":64},\"full_text_index\":{}}",
    ));
    try std.testing.expect(!(try parseHasFullTextIndexFromSlice(
        alloc,
        "{\"provider\":\"antfly\",\"text\":{\"target_tokens\":64}}",
    )));
    try std.testing.expect(!(try parseStoreChunksFromSlice(alloc, "")));
}
