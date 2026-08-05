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

// Provider model listing.
//
// Queries each inference provider's list-models API where one exists and
// normalizes the responses into a single task-typed model list. Used by the
// /connections endpoint to report connected models per provider instance.

const std = @import("std");
const httpx = @import("httpx");
const bedrock = @import("bedrock.zig");
const vertex = @import("vertex.zig");

/// Mirrors the inference registry's task taxonomy plus "other" for models
/// whose task type the provider's listing API does not classify.
pub const ModelKind = enum {
    embedder,
    generator,
    reranker,
    chunker,
    classifier,
    rewriter,
    reader,
    transcriber,
    extractor,
    other,

    pub fn groupKey(self: ModelKind) []const u8 {
        return switch (self) {
            .embedder => "embedders",
            .generator => "generators",
            .reranker => "rerankers",
            .chunker => "chunkers",
            .classifier => "classifiers",
            .rewriter => "rewriters",
            .reader => "readers",
            .transcriber => "transcribers",
            .extractor => "extractors",
            .other => "other",
        };
    }
};

pub const ProviderTag = enum {
    gemini,
    vertex,
    ollama,
    openai,
    openrouter,
    bedrock,
    cohere,
    anthropic,
    antfly,
    mock,
};

pub const ListedModel = struct {
    name: []u8,
    display_name: ?[]u8 = null,
    kind: ModelKind = .other,
    dimensions: ?u32 = null,

    fn deinit(self: *ListedModel, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.display_name) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ListResult = struct {
    models: []ListedModel = &.{},

    pub fn deinit(self: *ListResult, alloc: std.mem.Allocator) void {
        for (self.models) |*model| model.deinit(alloc);
        alloc.free(self.models);
        self.* = undefined;
    }
};

pub const Endpoint = struct {
    provider: ProviderTag,
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    region: []const u8 = "",
    project_id: []const u8 = "",
    location: []const u8 = "",
    credentials_path: []const u8 = "",
};

const default_timeout_ms: u64 = 5_000;

/// Query the provider's list-models API and normalize the response.
/// The caller owns the returned result and frees it with `deinit(alloc)`.
pub fn listModels(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const timeout = if (timeout_ms == 0) default_timeout_ms else timeout_ms;
    return switch (ep.provider) {
        .openai => try listOpenAi(alloc, http, ep, timeout, "https://api.openai.com"),
        .openrouter => try listOpenAi(alloc, http, ep, timeout, "https://openrouter.ai/api/v1"),
        .ollama => try listOllama(alloc, http, ep, timeout),
        .gemini => try listGemini(alloc, http, ep, timeout),
        .vertex => try listVertex(alloc, http, ep, timeout),
        .anthropic => try listAnthropic(alloc, http, ep, timeout),
        .cohere => try listCohere(alloc, http, ep, timeout),
        .bedrock => try listBedrock(alloc, http, ep, timeout),
        .antfly => try listAntfly(alloc, http, ep, timeout),
        .mock => try mockListResult(alloc),
    };
}

fn listOpenAi(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64, default_base: []const u8) !ListResult {
    const raw = if (ep.url.len > 0) ep.url else default_base;
    const base = try appendPathIfMissing(alloc, std.mem.trimEnd(u8, raw, "/"), "/v1");
    defer alloc.free(base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models", .{base});
    defer alloc.free(url);
    const body = try getBodyWithBearerAlloc(alloc, http, url, ep.api_key, timeout_ms);
    defer alloc.free(body);
    return try parseOpenAiModels(alloc, body);
}

fn listOllama(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    var base = std.mem.trimEnd(u8, if (ep.url.len > 0) ep.url else "http://localhost:11434", "/");
    if (std.mem.endsWith(u8, base, "/v1")) base = base[0 .. base.len - "/v1".len];
    const url = try std.fmt.allocPrint(alloc, "{s}/api/tags", .{base});
    defer alloc.free(url);
    const body = try getBodyAlloc(alloc, http, url, null, timeout_ms);
    defer alloc.free(body);
    return try parseOllamaTags(alloc, body);
}

fn listGemini(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const base = std.mem.trimEnd(u8, if (ep.url.len > 0) ep.url else "https://generativelanguage.googleapis.com/v1beta", "/");
    const url = try std.fmt.allocPrint(alloc, "{s}/models?pageSize=200", .{base});
    defer alloc.free(url);
    const headers = [_][2][]const u8{.{ "x-goog-api-key", ep.api_key orelse "" }};
    const body = try getBodyAlloc(alloc, http, url, &headers, timeout_ms);
    defer alloc.free(body);
    return try parseGeminiModels(alloc, body);
}

fn listVertex(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const base = std.mem.trimEnd(u8, if (ep.url.len > 0) ep.url else "https://aiplatform.googleapis.com/v1beta1", "/");
    const project_id = if (ep.project_id.len > 0)
        ep.project_id
    else
        (try vertex.vertexProjectIdFromConfigAlloc(alloc, if (ep.credentials_path.len > 0) ep.credentials_path else null) orelse return error.MissingVertexCredentials);
    defer if (ep.project_id.len == 0) alloc.free(project_id);
    const location = if (ep.location.len > 0) ep.location else "us-central1";
    const url = try std.fmt.allocPrint(alloc, "{s}/projects/{s}/locations/{s}/publishers/google/models?pageSize=100", .{ base, project_id, location });
    defer alloc.free(url);
    const auth = try vertex.mintAuthorizationValueAlloc(alloc, if (ep.credentials_path.len > 0) ep.credentials_path else null);
    defer alloc.free(auth);
    const headers = [_][2][]const u8{.{ "Authorization", auth }};
    const body = try getBodyAlloc(alloc, http, url, &headers, timeout_ms);
    defer alloc.free(body);
    return try parseVertexPublisherModels(alloc, body);
}

fn listAnthropic(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const raw = if (ep.url.len > 0) ep.url else "https://api.anthropic.com";
    const base = try appendPathIfMissing(alloc, std.mem.trimEnd(u8, raw, "/"), "/v1");
    defer alloc.free(base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models?limit=100", .{base});
    defer alloc.free(url);
    const headers = [_][2][]const u8{
        .{ "x-api-key", ep.api_key orelse "" },
        .{ "anthropic-version", "2023-06-01" },
    };
    const body = try getBodyAlloc(alloc, http, url, &headers, timeout_ms);
    defer alloc.free(body);
    return try parseAnthropicModels(alloc, body);
}

fn listCohere(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const raw = if (ep.url.len > 0) ep.url else "https://api.cohere.com";
    const base = try appendPathIfMissing(alloc, std.mem.trimEnd(u8, raw, "/"), "/v1");
    defer alloc.free(base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models?page_size=100", .{base});
    defer alloc.free(url);
    const body = try getBodyWithBearerAlloc(alloc, http, url, ep.api_key, timeout_ms);
    defer alloc.free(body);
    return try parseCohereModels(alloc, body);
}

fn listBedrock(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const region = if (ep.region.len > 0) ep.region else "us-east-1";
    const body = try bedrock.listFoundationModelsBodyAlloc(alloc, http, region, if (ep.url.len > 0) ep.url else null, timeout_ms);
    defer alloc.free(body);
    return try parseBedrockFoundationModels(alloc, body);
}

fn listAntfly(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const base = try normalizeAntflyBaseUrl(alloc, if (ep.url.len > 0) ep.url else "http://localhost:8082");
    defer alloc.free(base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models", .{base});
    defer alloc.free(url);
    const body = try getBodyWithBearerAlloc(alloc, http, url, ep.api_key, timeout_ms);
    defer alloc.free(body);
    return try parseAntflyModels(alloc, body);
}

fn mockListResult(alloc: std.mem.Allocator) !ListResult {
    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    try builder.append("mock-embedder", null, .embedder, null);
    try builder.append("mock-generator", null, .generator, null);
    try builder.append("mock-reranker", null, .reranker, null);
    try builder.append("mock-chunker", null, .chunker, null);
    return try builder.toOwned();
}

fn getBodyWithBearerAlloc(alloc: std.mem.Allocator, http: *httpx.Client, url: []const u8, api_key: ?[]const u8, timeout_ms: u64) ![]u8 {
    const key = api_key orelse "";
    if (key.len == 0) return try getBodyAlloc(alloc, http, url, null, timeout_ms);
    const auth = try std.fmt.allocPrint(alloc, "Bearer {s}", .{key});
    defer alloc.free(auth);
    const headers = [_][2][]const u8{.{ "Authorization", auth }};
    return try getBodyAlloc(alloc, http, url, &headers, timeout_ms);
}

fn getBodyAlloc(alloc: std.mem.Allocator, http: *httpx.Client, url: []const u8, headers: ?[]const [2][]const u8, timeout_ms: u64) ![]u8 {
    var resp = try http.request(.GET, url, .{ .headers = headers, .timeout_ms = timeout_ms });
    defer resp.deinit();
    if (!resp.ok()) return mapListStatus(resp.status.code);
    const body = resp.body orelse return error.EmptyResponse;
    return try alloc.dupe(u8, body);
}

fn mapListStatus(status: u16) anyerror {
    return switch (status) {
        401, 403 => error.ListModelsUnauthorized,
        404 => error.ListModelsUnsupported,
        429 => error.ListModelsRateLimited,
        else => error.ListModelsRequestFailed,
    };
}

/// Builds a normalized base url ending in /ai/v1, matching the antfly
/// inference service's public API prefix.
fn normalizeAntflyBaseUrl(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    const trimmed = std.mem.trimEnd(u8, raw, "/");
    if (std.mem.endsWith(u8, trimmed, "/ai/v1")) return try alloc.dupe(u8, trimmed);
    return try appendPathIfMissing(alloc, trimmed, "/ai/v1");
}

fn appendPathIfMissing(alloc: std.mem.Allocator, raw: []const u8, suffix: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, raw, suffix)) return try alloc.dupe(u8, raw);

    const scheme_pos = std.mem.indexOf(u8, raw, "://");
    const host_start = if (scheme_pos) |pos| pos + 3 else 0;
    const path_pos = std.mem.indexOfPos(u8, raw, host_start, "/");
    if (path_pos == null) return try std.fmt.allocPrint(alloc, "{s}{s}", .{ raw, suffix });
    if (path_pos.? == raw.len - 1) {
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ raw[0 .. raw.len - 1], suffix });
    }
    return try alloc.dupe(u8, raw);
}

const ResultBuilder = struct {
    alloc: std.mem.Allocator,
    models: std.ArrayListUnmanaged(ListedModel) = .empty,

    fn init(alloc: std.mem.Allocator) ResultBuilder {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *ResultBuilder) void {
        for (self.models.items) |*model| model.deinit(self.alloc);
        self.models.deinit(self.alloc);
    }

    fn append(self: *ResultBuilder, name: []const u8, display_name: ?[]const u8, kind: ModelKind, dimensions: ?u32) !void {
        const owned_name = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned_name);
        const owned_display = if (display_name) |value| try self.alloc.dupe(u8, value) else null;
        errdefer if (owned_display) |value| self.alloc.free(value);
        try self.models.append(self.alloc, .{
            .name = owned_name,
            .display_name = owned_display,
            .kind = kind,
            .dimensions = dimensions,
        });
    }

    fn toOwned(self: *ResultBuilder) !ListResult {
        return .{ .models = try self.models.toOwnedSlice(self.alloc) };
    }
};

// --- Wire-format parsers (pure, unit-testable) ---

/// OpenAI-compatible `{"data":[{"id":...}]}` listing (OpenAI, OpenRouter).
pub fn parseOpenAiModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        data: []const struct {
            id: []const u8,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.data) |entry| {
        try builder.append(entry.id, null, openAiKindHeuristic(entry.id), null);
    }
    return try builder.toOwned();
}

fn openAiKindHeuristic(id: []const u8) ModelKind {
    if (containsIgnoreCase(id, "embed")) return .embedder;
    if (containsIgnoreCase(id, "whisper") or containsIgnoreCase(id, "transcribe")) return .transcriber;
    if (containsIgnoreCase(id, "rerank")) return .reranker;
    return .other;
}

/// Ollama native `{"models":[{"name":...}]}` tags listing.
pub fn parseOllamaTags(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        models: []const struct {
            name: []const u8,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.models) |entry| {
        const kind: ModelKind = if (containsIgnoreCase(entry.name, "embed")) .embedder else .other;
        try builder.append(entry.name, null, kind, null);
    }
    return try builder.toOwned();
}

/// Google AI `{"models":[{"name":"models/...","supportedGenerationMethods":[...]}]}` listing.
pub fn parseGeminiModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        models: []const struct {
            name: []const u8,
            displayName: ?[]const u8 = null,
            supportedGenerationMethods: []const []const u8 = &.{},
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.models) |entry| {
        var kind: ModelKind = .other;
        for (entry.supportedGenerationMethods) |method| {
            if (std.mem.eql(u8, method, "embedContent") or std.mem.eql(u8, method, "embedText")) {
                kind = .embedder;
                break;
            }
            if (std.mem.eql(u8, method, "generateContent")) kind = .generator;
        }
        const name = if (std.mem.startsWith(u8, entry.name, "models/")) entry.name["models/".len..] else entry.name;
        try builder.append(name, entry.displayName, kind, null);
    }
    return try builder.toOwned();
}

/// Vertex `{"publisherModels":[{"name":"publishers/google/models/..."}]}` listing.
pub fn parseVertexPublisherModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        publisherModels: []const struct {
            name: []const u8,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.publisherModels) |entry| {
        const prefix = "publishers/google/models/";
        const name = if (std.mem.startsWith(u8, entry.name, prefix)) entry.name[prefix.len..] else entry.name;
        const kind: ModelKind = if (containsIgnoreCase(name, "embedding") or containsIgnoreCase(name, "gecko"))
            .embedder
        else if (containsIgnoreCase(name, "gemini"))
            .generator
        else
            .other;
        try builder.append(name, null, kind, null);
    }
    return try builder.toOwned();
}

/// Anthropic `{"data":[{"id":...,"display_name":...}]}` listing. All generators.
pub fn parseAnthropicModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        data: []const struct {
            id: []const u8,
            display_name: ?[]const u8 = null,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.data) |entry| {
        try builder.append(entry.id, entry.display_name, .generator, null);
    }
    return try builder.toOwned();
}

/// Cohere `{"models":[{"name":...,"endpoints":[...]}]}` listing.
pub fn parseCohereModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        models: []const struct {
            name: []const u8,
            endpoints: []const []const u8 = &.{},
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.models) |entry| {
        var kind: ModelKind = .other;
        for (entry.endpoints) |endpoint| {
            if (std.mem.eql(u8, endpoint, "embed")) {
                kind = .embedder;
                break;
            }
            if (std.mem.eql(u8, endpoint, "rerank")) {
                kind = .reranker;
                break;
            }
            if (std.mem.eql(u8, endpoint, "chat") or std.mem.eql(u8, endpoint, "generate")) kind = .generator;
        }
        try builder.append(entry.name, null, kind, null);
    }
    return try builder.toOwned();
}

/// Bedrock `{"modelSummaries":[{"modelId":...,"outputModalities":[...]}]}` listing.
pub fn parseBedrockFoundationModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    const Response = struct {
        modelSummaries: []const struct {
            modelId: []const u8,
            modelName: ?[]const u8 = null,
            outputModalities: []const []const u8 = &.{},
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();
    for (parsed.value.modelSummaries) |entry| {
        var kind: ModelKind = .other;
        for (entry.outputModalities) |modality| {
            if (std.mem.eql(u8, modality, "EMBEDDING")) {
                kind = .embedder;
                break;
            }
            if (std.mem.eql(u8, modality, "TEXT")) kind = .generator;
        }
        try builder.append(entry.modelId, entry.modelName, kind, null);
    }
    return try builder.toOwned();
}

/// Antfly inference `/ai/v1/models` task-keyed listing. Group keys are
/// pluralized ModelKind values; each group is an object keyed by model name.
pub fn parseAntflyModels(alloc: std.mem.Allocator, body: []const u8) !ListResult {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnexpectedResponse;

    var builder = ResultBuilder.init(alloc);
    errdefer builder.deinit();

    const groups = [_]ModelKind{ .embedder, .generator, .reranker, .chunker, .classifier, .rewriter, .reader, .transcriber, .extractor };
    for (groups) |kind| {
        const group_value = parsed.value.object.get(kind.groupKey()) orelse continue;
        if (group_value != .object) continue;
        var it = group_value.object.iterator();
        while (it.next()) |entry| {
            var dimensions: ?u32 = null;
            if (entry.value_ptr.* == .object) {
                if (entry.value_ptr.object.get("dimensions")) |dims| {
                    if (dims == .integer and dims.integer > 0) dimensions = @intCast(dims.integer);
                }
            }
            try builder.append(entry.key_ptr.*, null, kind, dimensions);
        }
    }
    return try builder.toOwned();
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    return std.ascii.indexOfIgnoreCase(haystack, needle) != null;
}

// --- Tests ---

test "parse openai models with kind heuristics" {
    const alloc = std.testing.allocator;
    const body =
        \\{"object":"list","data":[{"id":"gpt-4o","object":"model"},{"id":"text-embedding-3-small","object":"model"},{"id":"whisper-1","object":"model"}]}
    ;
    var result = try parseOpenAiModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), result.models.len);
    try std.testing.expectEqualStrings("gpt-4o", result.models[0].name);
    try std.testing.expectEqual(ModelKind.other, result.models[0].kind);
    try std.testing.expectEqual(ModelKind.embedder, result.models[1].kind);
    try std.testing.expectEqual(ModelKind.transcriber, result.models[2].kind);
}

test "parse ollama tags" {
    const alloc = std.testing.allocator;
    const body =
        \\{"models":[{"name":"llama3:latest","size":1},{"name":"nomic-embed-text:latest","size":2}]}
    ;
    var result = try parseOllamaTags(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.models.len);
    try std.testing.expectEqual(ModelKind.other, result.models[0].kind);
    try std.testing.expectEqual(ModelKind.embedder, result.models[1].kind);
}

test "parse gemini models via supported generation methods" {
    const alloc = std.testing.allocator;
    const body =
        \\{"models":[
        \\  {"name":"models/gemini-2.0-flash","displayName":"Gemini 2.0 Flash","supportedGenerationMethods":["generateContent","countTokens"]},
        \\  {"name":"models/text-embedding-004","displayName":"Text Embedding","supportedGenerationMethods":["embedContent"]}
        \\]}
    ;
    var result = try parseGeminiModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.models.len);
    try std.testing.expectEqualStrings("gemini-2.0-flash", result.models[0].name);
    try std.testing.expectEqual(ModelKind.generator, result.models[0].kind);
    try std.testing.expectEqualStrings("Gemini 2.0 Flash", result.models[0].display_name.?);
    try std.testing.expectEqual(ModelKind.embedder, result.models[1].kind);
}

test "parse vertex publisher models" {
    const alloc = std.testing.allocator;
    const body =
        \\{"publisherModels":[
        \\  {"name":"publishers/google/models/gemini-2.0-flash"},
        \\  {"name":"publishers/google/models/textembedding-gecko"}
        \\]}
    ;
    var result = try parseVertexPublisherModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.models.len);
    try std.testing.expectEqualStrings("gemini-2.0-flash", result.models[0].name);
    try std.testing.expectEqual(ModelKind.generator, result.models[0].kind);
    try std.testing.expectEqual(ModelKind.embedder, result.models[1].kind);
}

test "parse anthropic models as generators" {
    const alloc = std.testing.allocator;
    const body =
        \\{"data":[{"id":"claude-sonnet-4-5","display_name":"Claude Sonnet 4.5","type":"model"}],"has_more":false}
    ;
    var result = try parseAnthropicModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.models.len);
    try std.testing.expectEqual(ModelKind.generator, result.models[0].kind);
    try std.testing.expectEqualStrings("Claude Sonnet 4.5", result.models[0].display_name.?);
}

test "parse cohere models via endpoints" {
    const alloc = std.testing.allocator;
    const body =
        \\{"models":[
        \\  {"name":"command-r","endpoints":["chat","generate"]},
        \\  {"name":"embed-english-v3.0","endpoints":["embed","classify"]},
        \\  {"name":"rerank-v3.5","endpoints":["rerank"]}
        \\]}
    ;
    var result = try parseCohereModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), result.models.len);
    try std.testing.expectEqual(ModelKind.generator, result.models[0].kind);
    try std.testing.expectEqual(ModelKind.embedder, result.models[1].kind);
    try std.testing.expectEqual(ModelKind.reranker, result.models[2].kind);
}

test "parse bedrock foundation models via output modalities" {
    const alloc = std.testing.allocator;
    const body =
        \\{"modelSummaries":[
        \\  {"modelId":"amazon.titan-embed-text-v2:0","modelName":"Titan Text Embeddings V2","outputModalities":["EMBEDDING"]},
        \\  {"modelId":"anthropic.claude-sonnet-4-5","modelName":"Claude Sonnet 4.5","outputModalities":["TEXT"]}
        \\]}
    ;
    var result = try parseBedrockFoundationModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.models.len);
    try std.testing.expectEqual(ModelKind.embedder, result.models[0].kind);
    try std.testing.expectEqual(ModelKind.generator, result.models[1].kind);
}

test "parse antfly task keyed models" {
    const alloc = std.testing.allocator;
    const body =
        \\{"object":"list","data":[],
        \\ "embedders":{"bge-base-en-v1.5":{"capabilities":["dense"],"dimensions":768}},
        \\ "generators":{"gemma-3-1b":{}},
        \\ "chunkers":{"fixed":{}},
        \\ "transcribers":{"whisper-small":{}}}
    ;
    var result = try parseAntflyModels(alloc, body);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), result.models.len);
    try std.testing.expectEqualStrings("bge-base-en-v1.5", result.models[0].name);
    try std.testing.expectEqual(ModelKind.embedder, result.models[0].kind);
    try std.testing.expectEqual(@as(u32, 768), result.models[0].dimensions.?);
    try std.testing.expectEqual(ModelKind.generator, result.models[1].kind);
    try std.testing.expectEqual(ModelKind.chunker, result.models[2].kind);
    try std.testing.expectEqual(ModelKind.transcriber, result.models[3].kind);
}

test "mock provider lists static models" {
    const alloc = std.testing.allocator;
    var result = try mockListResult(alloc);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), result.models.len);
    try std.testing.expectEqual(ModelKind.embedder, result.models[0].kind);
}

test "antfly base url normalization" {
    const alloc = std.testing.allocator;
    const cases = [_][2][]const u8{
        .{ "http://localhost:8082", "http://localhost:8082/ai/v1" },
        .{ "http://localhost:8082/", "http://localhost:8082/ai/v1" },
        .{ "http://localhost:8082/ai/v1", "http://localhost:8082/ai/v1" },
    };
    for (cases) |case| {
        const normalized = try normalizeAntflyBaseUrl(alloc, case[0]);
        defer alloc.free(normalized);
        try std.testing.expectEqualStrings(case[1], normalized);
    }
}
