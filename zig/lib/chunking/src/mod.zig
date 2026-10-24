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

pub const EmbeddingTarget = api_openapi.ChunkEmbeddingTarget;
pub const MediaPolicy = api_openapi.MediaChunkPolicy;

/// Internal replay of one member from an already authenticated chunk generation.
/// This never certifies whole-source coverage and is not persisted in policy.
pub const Replay = struct { source_version: []const u8, source_fingerprint: []const u8, plan_id: []const u8, ordinal: u64 };
pub fn cloneReplay(a: Allocator, value: Replay) !Replay {
    const version = try a.dupe(u8, value.source_version);
    errdefer a.free(version);
    const source = try a.dupe(u8, value.source_fingerprint);
    errdefer a.free(source);
    return .{ .source_version = version, .source_fingerprint = source, .plan_id = try a.dupe(u8, value.plan_id), .ordinal = value.ordinal };
}
pub fn freeReplay(a: Allocator, value: Replay) void {
    a.free(value.source_version);
    a.free(value.source_fingerprint);
    a.free(value.plan_id);
}

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
    outputs_json: []const u8 = "",
    embedding: ?EmbeddingTarget = null,
    policy: ?MediaPolicy = null,
    replay: ?Replay = null,
    /// Internal bounded media-pass request; never part of the persisted policy.
    page: bool = false,

    pub fn clone(self: Config, alloc: Allocator) !Config {
        var copy = Config{ .provider = self.provider, .store_chunks = self.store_chunks, .max_chunks = self.max_chunks, .threshold = self.threshold, .audio = self.audio, .policy = self.policy, .page = self.page };
        errdefer copy.deinit(alloc);
        copy.api_url = try alloc.dupe(u8, self.api_url);
        copy.model = try alloc.dupe(u8, self.model);
        copy.full_text_index_json = try alloc.dupe(u8, self.full_text_index_json);
        copy.text = try self.text.clone(alloc);
        copy.replay = if (self.replay) |value| try cloneReplay(alloc, value) else null;
        copy.outputs_json = try alloc.dupe(u8, self.outputs_json);
        copy.embedding = if (self.embedding) |value| try cloneEmbeddingTarget(alloc, value) else null;
        return copy;
    }

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.outputs_json.len > 0) alloc.free(self.outputs_json);
        if (self.replay) |value| freeReplay(alloc, value);
        if (self.embedding) |value| {
            alloc.free(value.model);
            if (value.task_type) |task| alloc.free(task);
            if (value.model_identity) |identity| alloc.free(identity);
        }
        if (self.api_url.len > 0) alloc.free(self.api_url);
        if (self.model.len > 0) alloc.free(self.model);
        if (self.full_text_index_json.len > 0) alloc.free(self.full_text_index_json);
        self.text.deinit(alloc);
        self.* = undefined;
    }

    pub fn isModelAware(self: Config) bool {
        return self.embedding != null or self.outputs_json.len > 0;
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
            const replacement = try alloc.dupe(u8, url_value.string);
            if (cfg.api_url.len > 0) alloc.free(@constCast(cfg.api_url));
            cfg.api_url = replacement;
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
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var generated = openApiFromConfig(cfg);
    if (cfg.full_text_index_json.len > 0) generated.full_text_index = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), cfg.full_text_index_json, .{});
    if (cfg.outputs_json.len > 0) generated.outputs = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), cfg.outputs_json, .{});
    return json.Stringify.valueAlloc(alloc, generated, .{ .emit_null_optional_fields = false });
}

fn configFromOpenApi(
    alloc: Allocator,
    provider_cfg: provider_openapi.ChunkerConfig,
    chunk_options: api_openapi.ChunkOptions,
) !Config {
    var cfg = Config{ .provider = try Provider.fromSlice(@tagName(provider_cfg.provider)) };
    errdefer cfg.deinit(alloc);
    cfg.api_url = if (provider_cfg.api_url) |value| try alloc.dupe(u8, value) else "";
    cfg.model = if (provider_cfg.model) |value| try alloc.dupe(u8, value) else "";
    cfg.store_chunks = provider_cfg.store_chunks orelse false;
    cfg.max_chunks = if (chunk_options.max_chunks) |n| std.math.cast(u32, n) orelse return error.InvalidChunkerConfig else 0;
    cfg.threshold = chunk_options.threshold;
    cfg.text = try textChunkOptionsFromOpenApi(alloc, chunk_options.text);
    cfg.audio = try audioChunkOptionsFromOpenApi(chunk_options.audio);
    cfg.embedding = if (chunk_options.embedding) |value| try cloneEmbeddingTarget(alloc, value) else null;
    cfg.policy = chunk_options.policy;
    if (chunk_options.outputs) |value| cfg.outputs_json = try json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });

    if (provider_cfg.full_text_index) |full_text_index| {
        cfg.full_text_index_json = try json.Stringify.valueAlloc(alloc, full_text_index, .{});
    }

    if (cfg.provider == .antfly and cfg.api_url.len > 0 and cfg.model.len == 0 and !cfg.isModelAware()) return error.InvalidChunkerConfig;
    return cfg;
}

fn cloneEmbeddingTarget(a: Allocator, value: EmbeddingTarget) !EmbeddingTarget {
    var result = value;
    result.model = try a.dupe(u8, value.model);
    errdefer a.free(result.model);
    result.task_type = if (value.task_type) |task| try a.dupe(u8, task) else null;
    errdefer if (result.task_type) |task| a.free(task);
    result.model_identity = if (value.model_identity) |identity| try a.dupe(u8, identity) else null;
    return result;
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
    embedding: ?EmbeddingTarget = null,
    outputs: ?json.Value = null,
    policy: ?MediaPolicy = null,
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
        .api_url = if (cfg.provider == .antfly and cfg.api_url.len > 0) cfg.api_url else null,
        .url = null,
        .model = if (cfg.model.len > 0) cfg.model else null,
        .provider = provider,
        .store_chunks = if (cfg.store_chunks) true else null,
        .full_text_index = null,
        .embedding = cfg.embedding,
        .policy = cfg.policy,
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
    var encoded_value = try json.parseFromSlice(json.Value, alloc, encoded, .{});
    defer encoded_value.deinit();
    try std.testing.expect(encoded_value.value.object.get("audio") == null);
    try std.testing.expect(encoded_value.value.object.get("url") == null);
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
