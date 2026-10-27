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
pub const cloudflare = @import("antfly_cloudflare_config");
const json = @import("antfly-json");
const openapi = @import("antfly_embeddings_openapi");

const Allocator = std.mem.Allocator;

pub const Provider = openapi.EmbedderProvider;
pub const OpenApiConfig = openapi.EmbedderConfig;

pub const openrouter_default_url = "https://openrouter.ai/api/v1";

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
        .cloudflare, .openai, .openrouter, .ollama => .{},
    };
}

/// Input types an embedding model accepts. Configuration sets this only to
/// override discovered capabilities; see `Config.inputs`.
pub const Inputs = struct {
    text: bool = false,
    image: bool = false,
    audio: bool = false,

    pub fn hasMedia(self: Inputs) bool {
        return self.image or self.audio;
    }

    fn fromNames(input_names: []const []const u8) !Inputs {
        if (input_names.len == 0) return error.InvalidEmbedderConfig;
        var inputs = Inputs{};
        for (input_names) |name| {
            const slot: *bool = if (std.mem.eql(u8, name, "text"))
                &inputs.text
            else if (std.mem.eql(u8, name, "image"))
                &inputs.image
            else if (std.mem.eql(u8, name, "audio"))
                &inputs.audio
            else
                return error.InvalidEmbedderConfig;
            if (slot.*) return error.InvalidEmbedderConfig;
            slot.* = true;
        }
        return inputs;
    }

    pub fn subsetOf(self: Inputs, other: Inputs) bool {
        return (!self.text or other.text) and (!self.image or other.image) and (!self.audio or other.audio);
    }

    fn index(self: Inputs) usize {
        return @as(usize, @intFromBool(self.text)) | @as(usize, @intFromBool(self.image)) << 1 | @as(usize, @intFromBool(self.audio)) << 2;
    }

    /// Canonical names in a fixed order, borrowed from static storage.
    fn names(self: Inputs) []const []const u8 {
        const table = comptime blk: {
            var out: [8][]const []const u8 = undefined;
            for (0..8) |bits| {
                var list: []const []const u8 = &.{};
                if (bits & 1 != 0) list = list ++ &[_][]const u8{"text"};
                if (bits & 2 != 0) list = list ++ &[_][]const u8{"image"};
                if (bits & 4 != 0) list = list ++ &[_][]const u8{"audio"};
                out[bits] = list;
            }
            break :blk out;
        };
        return table[self.index()];
    }
};

pub const Config = struct {
    account_id: ?[]const u8 = null,
    gateway_id: ?[]const u8 = null,
    sparse: ?bool = null,
    rate_limit: ?openapi.RateLimitConfig = null,
    provider: Provider,
    model: []const u8 = "",
    model_identity: []const u8 = "",
    request_format: []const u8 = "",
    task_encoding: []const u8 = "",
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
    /// Declared input types. Null means "use the model's discovered
    /// capabilities"; a value replaces them, for models Antfly cannot
    /// discover yet.
    inputs: ?Inputs = null,

    pub fn clone(self: Config, alloc: std.mem.Allocator) !Config {
        var result = self;
        inline for (@typeInfo(Config).@"struct".field_names, @typeInfo(Config).@"struct".field_types) |name, T| {
            if (T == []const u8) @field(result, name) = "";
            if (T == ?[]const u8) @field(result, name) = null;
        }
        errdefer result.deinit(alloc);
        inline for (@typeInfo(Config).@"struct".field_names, @typeInfo(Config).@"struct".field_types) |name, T| {
            if (T == []const u8) {
                if (@field(self, name).len > 0) @field(result, name) = try alloc.dupe(u8, @field(self, name));
            }
            if (T == ?[]const u8) if (@field(self, name)) |value| {
                @field(result, name) = try alloc.dupe(u8, value);
            };
        }
        return result;
    }

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.account_id) |v| alloc.free(v);
        if (self.gateway_id) |v| alloc.free(v);
        if (self.model.len > 0) alloc.free(self.model);
        if (self.model_identity.len > 0) alloc.free(self.model_identity);
        if (self.request_format.len > 0) alloc.free(self.request_format);
        if (self.task_encoding.len > 0) alloc.free(self.task_encoding);
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
        if (self.provider == .cloudflare and (self.sparse orelse false)) return error.InvalidEmbedderConfig;
        if (self.provider == .cloudflare) {
            try cloudflare.identifier(self.account_id orelse return error.InvalidCloudflareConfig);
            try cloudflare.modelId(self.model);
            if (self.gateway_id) |id| try cloudflare.identifier(id);
        } else if (self.account_id != null or self.gateway_id != null) return error.InvalidCloudflareConfig;
        if (self.model_identity.len > 0) {
            if (self.provider != .antfly or self.model_identity.len != 64) return error.InvalidEmbedderConfig;
            for (self.model_identity) |c| if (!(c >= '0' and c <= '9') and !(c >= 'a' and c <= 'f')) return error.InvalidEmbedderConfig;
        }
        switch (self.provider) {
            .antfly => {},
            else => if (self.model.len == 0) return error.InvalidEmbedderConfig,
        }
        if (self.request_format.len > 0) {
            const valid = switch (self.provider) {
                .bedrock => validBedrockRequestFormat(self.request_format),
                .vertex => std.mem.eql(u8, self.request_format, "embed_content") or std.mem.eql(u8, self.request_format, "predict"),
                .gemini => std.mem.eql(u8, self.request_format, "embed_content") or std.mem.eql(u8, self.request_format, "batch_embed_contents"),
                else => false,
            };
            if (!valid) return error.InvalidEmbedderConfig;
        }
        if (self.task_encoding.len > 0) {
            if (self.provider != .gemini and self.provider != .vertex) return error.InvalidEmbedderConfig;
            if (!std.mem.eql(u8, self.task_encoding, "prompt") and !std.mem.eql(u8, self.task_encoding, "task_type") and !std.mem.eql(u8, self.task_encoding, "none")) return error.InvalidEmbedderConfig;
        }
        if (std.mem.eql(u8, self.request_format, "embed_content") and self.batch_size != null and self.batch_size.? != 1) return error.InvalidEmbedderConfig;
        if (self.dimension) |dimension| {
            if (dimension == 0) return error.InvalidEmbedderConfig;
        }
        if (self.dimensions) |dimensions| {
            if (dimensions == 0) return error.InvalidEmbedderConfig;
        }
        if (self.batch_size) |batch_size| {
            if (batch_size == 0) return error.InvalidEmbedderConfig;
        }
        if (self.inputs) |inputs| {
            // Only adapters that can carry media may declare it; the others
            // would otherwise drop images or audio without a trace.
            const allowed: Inputs = switch (self.provider) {
                .antfly => .{ .text = true, .image = true, .audio = true },
                .bedrock => .{ .text = true, .image = true },
                else => .{ .text = true },
            };
            if (!inputs.subsetOf(allowed)) return error.InvalidEmbedderConfig;
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
            .openrouter => openrouter_default_url,
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

test "antfly model identity survives config admission cloning and serialization" {
    const a = std.testing.allocator;
    const identity = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const raw = "{\"provider\":\"antfly\",\"model\":\"google/embeddinggemma-2\",\"api_url\":\"http://localhost:8082\",\"model_identity\":\"" ++ identity ++ "\"}";
    var config = try parseConfigFromSlice(a, raw);
    defer config.deinit(a);
    var cloned = try config.clone(a);
    defer cloned.deinit(a);
    const encoded = try stringifyAlloc(a, cloned);
    defer a.free(encoded);
    var restored = try parseConfigFromSlice(a, encoded);
    defer restored.deinit(a);
    try std.testing.expectEqualStrings(identity, restored.model_identity);
    try std.testing.expectEqualStrings(config.url, restored.url);
    inline for (.{ "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }) |invalid| {
        try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(a, "{\"provider\":\"antfly\",\"model\":\"m\",\"model_identity\":\"" ++ invalid ++ "\"}"));
    }
    try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(a, "{\"provider\":\"openai\",\"model\":\"m\",\"model_identity\":\"" ++ identity ++ "\"}"));
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
    const cfg = Config{
        .rate_limit = generated.rate_limit,
        .sparse = generated.sparse,
        .provider = std.meta.stringToEnum(Provider, provider_name) orelse return error.InvalidEmbedderConfig,
        .account_id = if (generated.account_id) |v| v else null,
        .gateway_id = if (generated.gateway_id) |v| v else null,
        .model = if (generated.model) |model| model else "",
        .model_identity = if (generated.model_identity) |identity| identity else "",
        .request_format = if (generated.request_format) |request_format| request_format else "",
        .task_encoding = if (generated.task_encoding) |value| value else "",
        .url = if (generated.url) |url|
            url
        else if (generated.api_url) |api_url|
            api_url
        else
            "",
        .api_key = if (generated.api_key) |api_key| api_key else null,
        .project_id = if (generated.project_id) |project_id| project_id else "",
        .location = if (generated.location) |location| location else "",
        .credentials_path = if (generated.credentials_path) |credentials_path| credentials_path else "",
        .region = if (generated.region) |region| region else "",
        .dimension = if (generated.dimension) |dimension|
            std.math.cast(u32, dimension) orelse return error.InvalidEmbedderConfig
        else
            null,
        .dimensions = if (generated.dimensions) |dimensions|
            std.math.cast(u32, dimensions) orelse return error.InvalidEmbedderConfig
        else
            null,
        .input_type = if (generated.input_type) |input_type| input_type else "",
        .query_input_type = if (query_input_type) |value| value else "",
        .document_input_type = if (document_input_type) |value| value else "",
        .query_instruction = if (query_instruction) |value| value else "",
        .truncate = if (generated.truncate) |truncate| truncate else "",
        .batch_size = if (generated.batch_size) |batch_size|
            std.math.cast(u32, batch_size) orelse return error.InvalidEmbedderConfig
        else
            null,
        .strip_new_lines = generated.strip_new_lines,
        .inputs = if (generated.inputs) |input_names| try Inputs.fromNames(input_names) else null,
    };
    try cfg.validate();
    return cfg.clone(alloc);
}

pub fn openApiFromConfig(cfg: Config) openapi.EmbedderConfig {
    return .{
        .rate_limit = cfg.rate_limit,
        .account_id = cfg.account_id,
        .gateway_id = cfg.gateway_id,
        .sparse = cfg.sparse,
        .provider = @tagName(cfg.provider),
        .model = if (cfg.model.len > 0) cfg.model else null,
        .request_format = if (cfg.request_format.len > 0) cfg.request_format else null,
        .task_encoding = if (cfg.task_encoding.len > 0) cfg.task_encoding else null,
        .url = switch (cfg.provider) {
            .antfly => null,
            else => if (cfg.url.len > 0) cfg.url else null,
        },
        .api_url = switch (cfg.provider) {
            .antfly => if (cfg.url.len > 0) cfg.url else null,
            else => null,
        },
        .model_identity = if (cfg.model_identity.len > 0) cfg.model_identity else null,
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
        .inputs = if (cfg.inputs) |inputs| inputs.names() else null,
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

test "embedder inputs override parses, validates per provider, and round trips" {
    const alloc = std.testing.allocator;
    var cfg = try parseConfigFromSlice(alloc,
        \\{"provider":"antfly","model":"future","inputs":["image","text"]}
    );
    defer cfg.deinit(alloc);
    try cfg.validate();
    try std.testing.expect(cfg.inputs.?.text and cfg.inputs.?.image and !cfg.inputs.?.audio);
    try std.testing.expect(cfg.inputs.?.hasMedia());
    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    // Serialized in canonical order.
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"inputs\":[\"text\",\"image\"]") != null);

    var absent = try parseConfigFromSlice(alloc, "{\"provider\":\"antfly\",\"model\":\"m\"}");
    defer absent.deinit(alloc);
    try std.testing.expect(absent.inputs == null);

    var bedrock = try parseConfigFromSlice(alloc, "{\"provider\":\"bedrock\",\"model\":\"m\",\"inputs\":[\"text\",\"image\"]}");
    defer bedrock.deinit(alloc);
    try bedrock.validate();
    try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(alloc, "{\"provider\":\"bedrock\",\"model\":\"m\",\"inputs\":[\"audio\"]}"));
    // A text-only adapter must not silently drop declared media.
    try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(alloc, "{\"provider\":\"openai\",\"model\":\"m\",\"inputs\":[\"text\",\"image\"]}"));
    var openai_text = try parseConfigFromSlice(alloc, "{\"provider\":\"openai\",\"model\":\"m\",\"inputs\":[\"text\"]}");
    defer openai_text.deinit(alloc);
    try openai_text.validate();

    for ([_][]const u8{
        "{\"provider\":\"antfly\",\"inputs\":[]}",
        "{\"provider\":\"antfly\",\"inputs\":[\"text\",\"text\"]}",
        "{\"provider\":\"antfly\",\"inputs\":[\"video\"]}",
    }) |raw| try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(alloc, raw));
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

test "Google transport and task encoding round trip independently of model names" {
    const alloc = std.testing.allocator;
    const cases = [_][]const u8{
        \\{"provider":"vertex","model":"future-model","request_format":"embed_content","task_encoding":"prompt","batch_size":1}
        ,
        \\{"provider":"vertex","model":"custom-legacy","request_format":"predict","task_encoding":"none","batch_size":1}
        ,
        \\{"provider":"gemini","model":"future-model","request_format":"embed_content","task_encoding":"task_type"}
        ,
        \\{"provider":"gemini","model":"legacy-model","request_format":"batch_embed_contents","task_encoding":"task_type"}
        ,
    };
    for (cases) |raw| {
        var config = try parseConfigFromSlice(alloc, raw);
        defer config.deinit(alloc);
        var cloned = try config.clone(alloc);
        defer cloned.deinit(alloc);
        const encoded = try json.Stringify.valueAlloc(alloc, openApiFromConfig(cloned), .{ .emit_null_optional_fields = false });
        defer alloc.free(encoded);
        var reparsed = try parseConfigFromSlice(alloc, encoded);
        defer reparsed.deinit(alloc);
        try std.testing.expectEqualStrings(config.request_format, reparsed.request_format);
        try std.testing.expectEqualStrings(config.task_encoding, reparsed.task_encoding);
    }
    const invalid = [_][]const u8{
        \\{"provider":"vertex","model":"x","request_format":"batch_embed_contents"}
        ,
        \\{"provider":"gemini","model":"x","request_format":"predict"}
        ,
        \\{"provider":"vertex","model":"x","request_format":"embed_content","batch_size":2}
        ,
        \\{"provider":"vertex","model":"x","task_encoding":"auto"}
        ,
    };
    for (invalid) |raw| try std.testing.expectError(error.InvalidEmbedderConfig, parseConfigFromSlice(alloc, raw));
}
