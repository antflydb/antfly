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
const openapi = @import("antfly_reranking_openapi");

const Allocator = std.mem.Allocator;

pub const Provider = openapi.RerankerProvider;
pub const OpenApiConfig = openapi.RerankerConfig;

pub const Config = struct {
    provider: Provider,
    field: []const u8 = "",
    template: []const u8 = "",
    model: []const u8 = "",
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    project_id: []const u8 = "",
    credentials_path: []const u8 = "",
    top_n: ?u32 = null,
    max_chunks_per_doc: ?u32 = null,

    pub fn clone(self: Config, alloc: Allocator) !Config {
        return .{
            .provider = self.provider,
            .field = if (self.field.len > 0) try alloc.dupe(u8, self.field) else "",
            .template = if (self.template.len > 0) try alloc.dupe(u8, self.template) else "",
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .url = if (self.url.len > 0) try alloc.dupe(u8, self.url) else "",
            .api_key = if (self.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
            .project_id = if (self.project_id.len > 0) try alloc.dupe(u8, self.project_id) else "",
            .credentials_path = if (self.credentials_path.len > 0) try alloc.dupe(u8, self.credentials_path) else "",
            .top_n = self.top_n,
            .max_chunks_per_doc = self.max_chunks_per_doc,
        };
    }

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.field.len > 0) alloc.free(self.field);
        if (self.template.len > 0) alloc.free(self.template);
        if (self.model.len > 0) alloc.free(self.model);
        if (self.url.len > 0) alloc.free(self.url);
        if (self.api_key) |api_key| alloc.free(api_key);
        if (self.project_id.len > 0) alloc.free(self.project_id);
        if (self.credentials_path.len > 0) alloc.free(self.credentials_path);
        self.* = undefined;
    }

    pub fn validate(self: Config) !void {
        if (self.field.len == 0 and self.template.len == 0) return error.InvalidRerankerConfig;
        switch (self.provider) {
            .antfly => {},
            .ollama, .cohere, .vertex => {
                if (self.model.len == 0) return error.InvalidRerankerConfig;
            },
        }
        if (self.top_n) |top_n| {
            if (top_n == 0) return error.InvalidRerankerConfig;
        }
        if (self.max_chunks_per_doc) |max_chunks_per_doc| {
            if (max_chunks_per_doc == 0) return error.InvalidRerankerConfig;
        }
    }

    pub fn defaultedUrl(self: Config) []const u8 {
        if (self.url.len > 0) return self.url;
        return switch (self.provider) {
            .ollama => "http://127.0.0.1:11434",
            .antfly => "http://127.0.0.1:8082",
            else => "",
        };
    }
};

pub fn parseConfigFromSlice(alloc: Allocator, raw: []const u8) !Config {
    const parsed = try json.parseFromSlice(openapi.RerankerConfig, alloc, raw, .{});
    defer parsed.deinit();
    return try configFromOpenApi(alloc, parsed.value);
}

pub fn parseConfigFromValue(alloc: Allocator, value: json.Value) !Config {
    const parsed = try json.parseFromValue(openapi.RerankerConfig, alloc, value, .{});
    defer parsed.deinit();
    return try configFromOpenApi(alloc, parsed.value);
}

pub fn stringifyAlloc(alloc: Allocator, cfg: Config) ![]u8 {
    try cfg.validate();
    return try json.Stringify.valueAlloc(alloc, openApiFromConfig(cfg), .{});
}

pub fn configFromOpenApi(alloc: Allocator, generated: openapi.RerankerConfig) !Config {
    var cfg = Config{
        .provider = generated.provider,
        .field = if (generated.field) |field| try alloc.dupe(u8, field) else "",
        .template = if (generated.template) |template| try alloc.dupe(u8, template) else "",
        .model = if (generated.model) |model| try alloc.dupe(u8, model) else "",
        .url = if (generated.url) |url| try alloc.dupe(u8, url) else "",
        .api_key = if (generated.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
        .project_id = if (generated.project_id) |project_id| try alloc.dupe(u8, project_id) else "",
        .credentials_path = if (generated.credentials_path) |credentials_path| try alloc.dupe(u8, credentials_path) else "",
        .top_n = if (generated.top_n) |top_n|
            std.math.cast(u32, top_n) orelse return error.InvalidRerankerConfig
        else
            null,
        .max_chunks_per_doc = if (generated.max_chunks_per_doc) |max_chunks_per_doc|
            std.math.cast(u32, max_chunks_per_doc) orelse return error.InvalidRerankerConfig
        else
            null,
    };
    errdefer cfg.deinit(alloc);
    try cfg.validate();
    return cfg;
}

pub fn openApiFromConfig(cfg: Config) openapi.RerankerConfig {
    return .{
        .provider = cfg.provider,
        .field = if (cfg.field.len > 0) cfg.field else null,
        .template = if (cfg.template.len > 0) cfg.template else null,
        .model = if (cfg.model.len > 0) cfg.model else null,
        .url = if (cfg.url.len > 0) cfg.url else null,
        .api_key = cfg.api_key,
        .project_id = if (cfg.project_id.len > 0) cfg.project_id else null,
        .credentials_path = if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
        .top_n = if (cfg.top_n) |top_n| top_n else null,
        .max_chunks_per_doc = if (cfg.max_chunks_per_doc) |max_chunks_per_doc| max_chunks_per_doc else null,
    };
}

test "reranker config round trip" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"antfly","model":"cross-encoder/ms-marco-MiniLM-L-6-v2","url":"http://localhost:8082","field":"body","top_n":8}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqual(.antfly, cfg.provider);
    try std.testing.expectEqualStrings("body", cfg.field);
    try std.testing.expectEqualStrings("cross-encoder/ms-marco-MiniLM-L-6-v2", cfg.model);
    try std.testing.expectEqual(@as(?u32, 8), cfg.top_n);

    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.antfly, reparsed.provider);
    try std.testing.expectEqualStrings("body", reparsed.field);
}

test "reranker config requires field or template" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidRerankerConfig,
        parseConfigFromSlice(alloc, "{\"provider\":\"antfly\"}"),
    );
}
