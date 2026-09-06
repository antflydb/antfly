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

const RetrievalCapabilities = struct {
    legacy_input_type: bool = false,
    role_input_types: bool = false,
    query_instruction: bool = false,
};

fn retrievalCapabilities(provider: Provider, model: []const u8, request_format: []const u8) RetrievalCapabilities {
    return switch (provider) {
        .cohere => .{ .legacy_input_type = true, .role_input_types = true },
        .gemini, .vertex => .{ .role_input_types = true },
        .bedrock => if (std.mem.eql(u8, request_format, "cohere_v3") or
            std.mem.eql(u8, request_format, "cohere_v4") or
            ((request_format.len == 0 or std.mem.eql(u8, request_format, "auto")) and
                std.mem.indexOf(u8, model, "cohere.embed-") != null))
            .{ .legacy_input_type = true, .role_input_types = true }
        else
            .{},
        .antfly => .{ .query_instruction = true },
        .openai, .openrouter, .ollama => .{},
    };
}

pub const Config = struct {
    rate_limit: ?openapi.RateLimitConfig = null,
    provider: Provider,
    model: []const u8 = "",
    request_format: []const u8 = "",
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    project_id: []const u8 = "",
    location: []const u8 = "",
    credentials_path: []const u8 = "",
    region: []const u8 = "",
    dimension: ?u32 = null,
    dimensions: ?u32 = null,
    input_type: []const u8 = "",
    query_input_type: []const u8 = "",
    document_input_type: []const u8 = "",
    query_instruction: []const u8 = "",
    truncate: []const u8 = "",
    batch_size: ?u32 = null,
    strip_new_lines: ?bool = null,
    multimodal: bool = false,

    pub fn clone(self: Config, alloc: Allocator) !Config {
        return .{
            .rate_limit = self.rate_limit,
            .provider = self.provider,
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .request_format = if (self.request_format.len > 0) try alloc.dupe(u8, self.request_format) else "",
            .url = if (self.url.len > 0) try alloc.dupe(u8, self.url) else "",
            .api_key = if (self.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
            .project_id = if (self.project_id.len > 0) try alloc.dupe(u8, self.project_id) else "",
            .location = if (self.location.len > 0) try alloc.dupe(u8, self.location) else "",
            .credentials_path = if (self.credentials_path.len > 0) try alloc.dupe(u8, self.credentials_path) else "",
            .region = if (self.region.len > 0) try alloc.dupe(u8, self.region) else "",
            .dimension = self.dimension,
            .dimensions = self.dimensions,
            .input_type = if (self.input_type.len > 0) try alloc.dupe(u8, self.input_type) else "",
            .query_input_type = if (self.query_input_type.len > 0) try alloc.dupe(u8, self.query_input_type) else "",
            .document_input_type = if (self.document_input_type.len > 0) try alloc.dupe(u8, self.document_input_type) else "",
            .query_instruction = if (self.query_instruction.len > 0) try alloc.dupe(u8, self.query_instruction) else "",
            .truncate = if (self.truncate.len > 0) try alloc.dupe(u8, self.truncate) else "",
            .batch_size = self.batch_size,
            .strip_new_lines = self.strip_new_lines,
            .multimodal = self.multimodal,
        };
    }

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.model.len > 0) alloc.free(self.model);
        if (self.request_format.len > 0) alloc.free(self.request_format);
        if (self.url.len > 0) alloc.free(self.url);
        if (self.api_key) |api_key| alloc.free(api_key);
        if (self.project_id.len > 0) alloc.free(self.project_id);
        if (self.location.len > 0) alloc.free(self.location);
        if (self.credentials_path.len > 0) alloc.free(self.credentials_path);
        if (self.region.len > 0) alloc.free(self.region);
        if (self.input_type.len > 0) alloc.free(self.input_type);
        if (self.query_input_type.len > 0) alloc.free(self.query_input_type);
        if (self.document_input_type.len > 0) alloc.free(self.document_input_type);
        if (self.query_instruction.len > 0) alloc.free(self.query_instruction);
        if (self.truncate.len > 0) alloc.free(self.truncate);
        self.* = undefined;
    }

    pub fn validate(self: Config) !void {
        switch (self.provider) {
            .antfly => {},
            else => if (self.model.len == 0) return error.InvalidEmbedderConfig,
        }
        if (self.request_format.len > 0) {
            if (self.provider != .bedrock) return error.InvalidEmbedderConfig;
            if (!validBedrockRequestFormat(self.request_format)) return error.InvalidEmbedderConfig;
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
        const capabilities = retrievalCapabilities(self.provider, self.model, self.request_format);
        if (!capabilities.legacy_input_type and self.input_type.len > 0)
            return error.UnsupportedEmbeddingRetrievalConfig;
        if (!capabilities.role_input_types and
            (self.query_input_type.len > 0 or self.document_input_type.len > 0))
            return error.UnsupportedEmbeddingRetrievalConfig;
        if (!capabilities.query_instruction and self.query_instruction.len > 0)
            return error.UnsupportedEmbeddingRetrievalConfig;
    }

    pub fn defaultedUrl(self: Config) []const u8 {
        if (self.url.len > 0) return self.url;
        return switch (self.provider) {
            .openai => "https://api.openai.com",
            .ollama => "http://127.0.0.1:11434",
            .antfly => "http://127.0.0.1:8082",
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
    const provider_name = generated.provider orelse return error.InvalidEmbedderConfig;
    const retrieval = generated.retrieval;
    const query_input_type = try resolveRetrievalField(
        if (retrieval) |value| value.query_input_type else null,
        generated.query_input_type,
    );
    const document_input_type = try resolveRetrievalField(
        if (retrieval) |value| value.document_input_type else null,
        generated.document_input_type,
    );
    const query_instruction = try resolveRetrievalField(
        if (retrieval) |value| value.query_instruction else null,
        generated.query_instruction,
    );
    var cfg = Config{
        .rate_limit = generated.rate_limit,
        .provider = std.meta.stringToEnum(Provider, provider_name) orelse return error.InvalidEmbedderConfig,
        .model = if (generated.model) |model| try alloc.dupe(u8, model) else "",
        .request_format = if (generated.request_format) |request_format| try alloc.dupe(u8, request_format) else "",
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
        .query_input_type = if (query_input_type) |value| try alloc.dupe(u8, value) else "",
        .document_input_type = if (document_input_type) |value| try alloc.dupe(u8, value) else "",
        .query_instruction = if (query_instruction) |value| try alloc.dupe(u8, value) else "",
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
        .rate_limit = cfg.rate_limit,
        .provider = @tagName(cfg.provider),
        .model = if (cfg.model.len > 0) cfg.model else null,
        .request_format = if (cfg.request_format.len > 0) cfg.request_format else null,
        .url = switch (cfg.provider) {
            .antfly => null,
            else => if (cfg.url.len > 0) cfg.url else null,
        },
        .api_url = switch (cfg.provider) {
            .antfly => if (cfg.url.len > 0) cfg.url else null,
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
        // Always emit the canonical nested form. The flat fields remain
        // parser-only compatibility for configurations admitted before the
        // retrieval object was introduced.
        .query_input_type = null,
        .document_input_type = null,
        .query_instruction = null,
        .retrieval = openApiRetrievalConfig(cfg),
        .truncate = if (cfg.truncate.len > 0) cfg.truncate else null,
        .batch_size = if (cfg.batch_size) |batch_size| batch_size else null,
        .strip_new_lines = cfg.strip_new_lines,
        .multimodal = if (cfg.multimodal) true else null,
    };
}

fn resolveRetrievalField(nested: ?[]const u8, legacy: ?[]const u8) !?[]const u8 {
    if (nested) |nested_value| {
        if (legacy) |legacy_value| {
            if (!std.mem.eql(u8, nested_value, legacy_value)) return error.InvalidEmbedderConfig;
        }
        return nested_value;
    }
    return legacy;
}

fn openApiRetrievalConfig(cfg: Config) ?openapi.EmbeddingRetrievalConfig {
    if (cfg.query_input_type.len == 0 and
        cfg.document_input_type.len == 0 and
        cfg.query_instruction.len == 0) return null;
    return .{
        .query_input_type = if (cfg.query_input_type.len > 0) cfg.query_input_type else null,
        .document_input_type = if (cfg.document_input_type.len > 0) cfg.document_input_type else null,
        .query_instruction = if (cfg.query_instruction.len > 0) cfg.query_instruction else null,
    };
}

fn validBedrockRequestFormat(value: []const u8) bool {
    return std.mem.eql(u8, value, "auto") or
        std.mem.eql(u8, value, "titan_text") or
        std.mem.eql(u8, value, "titan_multimodal") or
        std.mem.eql(u8, value, "cohere_v3") or
        std.mem.eql(u8, value, "cohere_v4");
}

test "embedder config round trip" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"cohere","model":"embed-english-v3.0","api_key":"test-key","dimensions":1024,"retrieval":{"query_input_type":"search_query","document_input_type":"search_document"}}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqual(.cohere, cfg.provider);
    try std.testing.expectEqualStrings("embed-english-v3.0", cfg.model);
    try std.testing.expectEqual(@as(?u32, 1024), cfg.dimensions);
    try std.testing.expectEqualStrings("search_query", cfg.query_input_type);
    try std.testing.expectEqualStrings("search_document", cfg.document_input_type);
    try std.testing.expectEqualStrings("", cfg.query_instruction);

    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.cohere, reparsed.provider);
    try std.testing.expectEqualStrings("embed-english-v3.0", reparsed.model);
    try std.testing.expectEqualStrings("search_query", reparsed.query_input_type);
    try std.testing.expectEqualStrings("search_document", reparsed.document_input_type);
    try std.testing.expectEqualStrings("", reparsed.query_instruction);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retrieval\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_input_type\":\"search_query\"") != null);
}

test "embedder config accepts matching legacy retrieval fields and rejects conflicts" {
    const alloc = std.testing.allocator;
    const compatible =
        \\{"provider":"cohere","model":"embed-english-v3.0","query_input_type":"search_query","retrieval":{"query_input_type":"search_query"}}
    ;
    var cfg = try parseConfigFromSlice(alloc, compatible);
    defer cfg.deinit(alloc);
    try std.testing.expectEqualStrings("search_query", cfg.query_input_type);

    const conflicting =
        \\{"provider":"cohere","model":"embed-english-v3.0","query_input_type":"classification","retrieval":{"query_input_type":"search_query"}}
    ;
    try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(alloc, conflicting));
}

test "embedder config rejects retrieval controls a provider would ignore" {
    const alloc = std.testing.allocator;
    const Case = struct { raw: []const u8, supported: bool };
    const cases = [_]Case{
        .{ .raw = "{\"provider\":\"cohere\",\"model\":\"embed-v4.0\",\"input_type\":\"search_document\",\"retrieval\":{\"query_input_type\":\"search_query\"}}", .supported = true },
        .{ .raw = "{\"provider\":\"gemini\",\"model\":\"gemini-embedding-001\",\"retrieval\":{\"query_input_type\":\"RETRIEVAL_QUERY\"}}", .supported = true },
        .{ .raw = "{\"provider\":\"vertex\",\"model\":\"gemini-embedding-001\",\"retrieval\":{\"document_input_type\":\"RETRIEVAL_DOCUMENT\"}}", .supported = true },
        .{ .raw = "{\"provider\":\"bedrock\",\"model\":\"cohere.embed-v4:0\",\"retrieval\":{\"query_input_type\":\"search_query\"}}", .supported = true },
        .{ .raw = "{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\",\"retrieval\":{\"query_instruction\":\"retrieve passages\"}}", .supported = true },
        .{ .raw = "{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"retrieval\":{\"query_input_type\":\"search_query\"}}", .supported = false },
        .{ .raw = "{\"provider\":\"openrouter\",\"model\":\"openai/text-embedding-3-small\",\"input_type\":\"search_query\"}", .supported = false },
        .{ .raw = "{\"provider\":\"ollama\",\"model\":\"nomic-embed-text\",\"retrieval\":{\"query_instruction\":\"retrieve passages\"}}", .supported = false },
        .{ .raw = "{\"provider\":\"gemini\",\"model\":\"gemini-embedding-001\",\"input_type\":\"RETRIEVAL_QUERY\"}", .supported = false },
        .{ .raw = "{\"provider\":\"vertex\",\"model\":\"gemini-embedding-001\",\"retrieval\":{\"query_instruction\":\"retrieve passages\"}}", .supported = false },
        .{ .raw = "{\"provider\":\"bedrock\",\"model\":\"amazon.titan-embed-text-v2:0\",\"retrieval\":{\"query_input_type\":\"search_query\"}}", .supported = false },
        .{ .raw = "{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\",\"input_type\":\"search_query\"}", .supported = false },
    };
    for (cases) |case| {
        if (case.supported) {
            var cfg = try parseConfigFromSlice(alloc, case.raw);
            cfg.deinit(alloc);
        } else {
            try std.testing.expectError(
                error.UnsupportedEmbeddingRetrievalConfig,
                parseConfigFromSlice(alloc, case.raw),
            );
        }
    }
}

test "embedder config supports antfly api_url normalization" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"antfly","model":"bge-base-en-v1.5","api_url":"http://localhost:8082"}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(.antfly, cfg.provider);
    try std.testing.expectEqualStrings("http://localhost:8082", cfg.url);
}

test "embedder config preserves explicit Bedrock request format" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"bedrock","model":"arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/team-embeddings","request_format":"titan_multimodal","region":"us-east-1"}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    try std.testing.expectEqualStrings("titan_multimodal", cfg.request_format);

    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"request_format\":\"titan_multimodal\"") != null);

    try std.testing.expectError(
        error.InvalidEmbedderConfig,
        parseConfigFromSlice(alloc, "{\"provider\":\"bedrock\",\"model\":\"profile\",\"request_format\":\"unknown\"}"),
    );
}

test "embedder config validates model for remote providers" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidEmbedderConfig,
        parseConfigFromSlice(alloc, "{\"provider\":\"openai\"}"),
    );
}

test "embeddings config preserves shared rate limits through cloning and JSON" {
    const alloc = std.testing.allocator;
    var cfg = try parseConfigFromSlice(alloc, "{\"provider\":\"antfly\",\"model\":\"test\",\"rate_limit\":{\"requests_per_minute\":120,\"burst\":2,\"tokens_per_minute\":1000,\"max_concurrency\":4}}");
    defer cfg.deinit(alloc);
    cfg.rate_limit.?.pacing = .completion;
    cfg.rate_limit.?.burst = 1;
    var cloned = try cfg.clone(alloc);
    defer cloned.deinit(alloc);
    const encoded = try stringifyAlloc(alloc, cloned);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqualDeep(cfg.rate_limit, reparsed.rate_limit);
    try std.testing.expectEqual(.completion, reparsed.rate_limit.?.pacing.?);
    try std.testing.expectEqual(@as(?i64, 4), reparsed.rate_limit.?.max_concurrency);
}
