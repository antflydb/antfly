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
const openapi = @import("antfly_embeddings_openapi");

const Allocator = std.mem.Allocator;

pub const Provider = openapi.EmbedderProvider;
pub const OpenApiConfig = openapi.EmbedderConfig;

pub const Config = struct {
    provider: Provider,
    model: []const u8 = "",
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    project_id: []const u8 = "",
    location: []const u8 = "",
    credentials_path: []const u8 = "",
    region: []const u8 = "",
    dimension: ?u32 = null,
    dimensions: ?u32 = null,
    input_type: []const u8 = "",
    truncate: []const u8 = "",
    batch_size: ?u32 = null,
    strip_new_lines: ?bool = null,
    multimodal: bool = false,

    pub fn clone(self: Config, alloc: Allocator) !Config {
        return .{
            .provider = self.provider,
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .url = if (self.url.len > 0) try alloc.dupe(u8, self.url) else "",
            .api_key = if (self.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
            .project_id = if (self.project_id.len > 0) try alloc.dupe(u8, self.project_id) else "",
            .location = if (self.location.len > 0) try alloc.dupe(u8, self.location) else "",
            .credentials_path = if (self.credentials_path.len > 0) try alloc.dupe(u8, self.credentials_path) else "",
            .region = if (self.region.len > 0) try alloc.dupe(u8, self.region) else "",
            .dimension = self.dimension,
            .dimensions = self.dimensions,
            .input_type = if (self.input_type.len > 0) try alloc.dupe(u8, self.input_type) else "",
            .truncate = if (self.truncate.len > 0) try alloc.dupe(u8, self.truncate) else "",
            .batch_size = self.batch_size,
            .strip_new_lines = self.strip_new_lines,
            .multimodal = self.multimodal,
        };
    }

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.model.len > 0) alloc.free(self.model);
        if (self.url.len > 0) alloc.free(self.url);
        if (self.api_key) |api_key| alloc.free(api_key);
        if (self.project_id.len > 0) alloc.free(self.project_id);
        if (self.location.len > 0) alloc.free(self.location);
        if (self.credentials_path.len > 0) alloc.free(self.credentials_path);
        if (self.region.len > 0) alloc.free(self.region);
        if (self.input_type.len > 0) alloc.free(self.input_type);
        if (self.truncate.len > 0) alloc.free(self.truncate);
        self.* = undefined;
    }

    pub fn validate(self: Config) !void {
        switch (self.provider) {
            .antfly, .mock => {},
            else => if (self.model.len == 0) return error.InvalidEmbedderConfig,
        }
        if (self.dimension) |dimension| {
            if (dimension == 0) return error.InvalidEmbedderConfig;
        }
        if (self.dimensions) |dimensions| {
            if (dimensions == 0) return error.InvalidEmbedderConfig;
        }
        if (self.batch_size) |batch_size| {
            if (batch_size == 0) return error.InvalidEmbedderConfig;
        }
    }

    pub fn defaultedUrl(self: Config) []const u8 {
        if (self.url.len > 0) return self.url;
        return switch (self.provider) {
            .openai => "https://api.openai.com",
            .ollama => "http://127.0.0.1:11434",
            .termite => "http://127.0.0.1:8082",
            else => "",
        };
    }
};

pub fn parseConfigFromSlice(alloc: Allocator, raw: []const u8) !Config {
    const parsed = try json.parseFromSlice(openapi.EmbedderConfig, alloc, raw, .{});
    defer parsed.deinit();
    return try configFromOpenApi(alloc, parsed.value);
}

pub fn parseConfigFromValue(alloc: Allocator, value: json.Value) !Config {
    const parsed = try json.parseFromValue(openapi.EmbedderConfig, alloc, value, .{});
    defer parsed.deinit();
    return try configFromOpenApi(alloc, parsed.value);
}

pub fn stringifyAlloc(alloc: Allocator, cfg: Config) ![]u8 {
    try cfg.validate();
    return try json.Stringify.valueAlloc(alloc, openApiFromConfig(cfg), .{});
}

pub fn configFromOpenApi(alloc: Allocator, generated: openapi.EmbedderConfig) !Config {
    var cfg = Config{
        .provider = generated.provider,
        .model = if (generated.model) |model| try alloc.dupe(u8, model) else "",
        .url = if (generated.url) |url|
            try alloc.dupe(u8, url)
        else if (generated.api_url) |api_url|
            try alloc.dupe(u8, api_url)
        else
            "",
        .api_key = if (generated.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
        .project_id = if (generated.project_id) |project_id| try alloc.dupe(u8, project_id) else "",
        .location = if (generated.location) |location| try alloc.dupe(u8, location) else "",
        .credentials_path = if (generated.credentials_path) |credentials_path| try alloc.dupe(u8, credentials_path) else "",
        .region = if (generated.region) |region| try alloc.dupe(u8, region) else "",
        .dimension = if (generated.dimension) |dimension|
            std.math.cast(u32, dimension) orelse return error.InvalidEmbedderConfig
        else
            null,
        .dimensions = if (generated.dimensions) |dimensions|
            std.math.cast(u32, dimensions) orelse return error.InvalidEmbedderConfig
        else
            null,
        .input_type = if (generated.input_type) |input_type| try alloc.dupe(u8, input_type) else "",
        .truncate = if (generated.truncate) |truncate| try alloc.dupe(u8, truncate) else "",
        .batch_size = if (generated.batch_size) |batch_size|
            std.math.cast(u32, batch_size) orelse return error.InvalidEmbedderConfig
        else
            null,
        .strip_new_lines = generated.strip_new_lines,
        .multimodal = generated.multimodal orelse false,
    };
    errdefer cfg.deinit(alloc);
    try cfg.validate();
    return cfg;
}

pub fn openApiFromConfig(cfg: Config) openapi.EmbedderConfig {
    return .{
        .provider = cfg.provider,
        .model = if (cfg.model.len > 0) cfg.model else null,
        .url = switch (cfg.provider) {
            .termite => null,
            else => if (cfg.url.len > 0) cfg.url else null,
        },
        .api_url = switch (cfg.provider) {
            .termite => if (cfg.url.len > 0) cfg.url else null,
            else => null,
        },
        .api_key = cfg.api_key,
        .project_id = if (cfg.project_id.len > 0) cfg.project_id else null,
        .location = if (cfg.location.len > 0) cfg.location else null,
        .credentials_path = if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
        .region = if (cfg.region.len > 0) cfg.region else null,
        .dimension = if (cfg.dimension) |dimension| dimension else null,
        .dimensions = if (cfg.dimensions) |dimensions| dimensions else null,
        .input_type = if (cfg.input_type.len > 0) cfg.input_type else null,
        .truncate = if (cfg.truncate.len > 0) cfg.truncate else null,
        .batch_size = if (cfg.batch_size) |batch_size| batch_size else null,
        .strip_new_lines = cfg.strip_new_lines,
        .multimodal = if (cfg.multimodal) true else null,
    };
}

test "embedder config round trip" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"openai","model":"text-embedding-3-small","url":"https://api.openai.com","api_key":"sk-test","dimensions":1536}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqual(.openai, cfg.provider);
    try std.testing.expectEqualStrings("text-embedding-3-small", cfg.model);
    try std.testing.expectEqualStrings("https://api.openai.com", cfg.url);
    try std.testing.expectEqual(@as(?u32, 1536), cfg.dimensions);

    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.openai, reparsed.provider);
    try std.testing.expectEqualStrings("text-embedding-3-small", reparsed.model);
}

test "embedder config supports termite api_url normalization" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"termite","model":"bge-base-en-v1.5","api_url":"http://localhost:8082"}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(.termite, cfg.provider);
    try std.testing.expectEqualStrings("http://localhost:8082", cfg.url);
}

test "embedder config validates model for remote providers" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidEmbedderConfig,
        parseConfigFromSlice(alloc, "{\"provider\":\"openai\"}"),
    );
}
