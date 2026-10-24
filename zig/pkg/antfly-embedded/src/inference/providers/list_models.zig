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

// Provider model listing.
//
// Queries each inference provider's list-models API where one exists and
// normalizes the responses into a single task-typed model list. Used by the
// /connections endpoint to report connected models per provider instance.

const std = @import("std");
const httpx = @import("httpx");
const bedrock = @import("antfly_inference_bedrock");
const vertex = @import("antfly_inference_vertex");
const provider_defaults = @import("antfly_inference_provider_defaults");

/// Mirrors the inference registry's task taxonomy plus "other" for models
/// whose task type the provider's listing API does not classify.
pub const ModelKind = enum {
    embedder,
    generator,
    chain,
    synthesizer,
    predictor,
    reranker,
    chunker,
    classifier,
    decider,
    rewriter,
    reader,
    transcriber,
    extractor,
    other,

    pub fn groupKey(self: ModelKind) []const u8 {
        return switch (self) {
            .embedder => "embedders",
            .generator => "generators",
            .chain => "chains",
            .synthesizer => "synthesizers",
            .predictor => "predictors",
            .reranker => "rerankers",
            .chunker => "chunkers",
            .classifier => "classifiers",
            .decider => "deciders",
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
    capabilities: [][]u8 = &.{},
    inputs: [][]u8 = &.{},

    pub fn deinit(self: *ListedModel, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.display_name) |value| alloc.free(value);
        for (self.capabilities) |value| alloc.free(value);
        if (self.capabilities.len > 0) alloc.free(self.capabilities);
        for (self.inputs) |value| alloc.free(value);
        if (self.inputs.len > 0) alloc.free(self.inputs);
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
    workspace_id: []const u8 = "",
};

const default_timeout_ms: u64 = 5_000;

/// Query the provider's list-models API and normalize the response.
/// The caller owns the returned result and frees it with `deinit(alloc)`.
pub fn listModels(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const timeout = if (timeout_ms == 0) default_timeout_ms else timeout_ms;
    return switch (ep.provider) {
        .openai => try listOpenAi(alloc, http, ep, timeout, provider_defaults.openai_origin),
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
    var base = std.mem.trimEnd(u8, if (ep.url.len > 0) ep.url else provider_defaults.ollama_origin, "/");
    if (std.mem.endsWith(u8, base, "/v1")) base = base[0 .. base.len - "/v1".len];
    const url = try std.fmt.allocPrint(alloc, "{s}/api/tags", .{base});
    defer alloc.free(url);
    const body = try getBodyAlloc(alloc, http, url, null, timeout_ms);
    defer alloc.free(body);
    return try parseOllamaTags(alloc, body);
}

fn listGemini(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const base = provider_defaults.normalizedBase(ep.url, provider_defaults.gemini_v1beta_base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models?pageSize=200", .{base});
    defer alloc.free(url);
    const headers = [_][2][]const u8{.{ "x-goog-api-key", ep.api_key orelse "" }};
    const body = try getBodyAlloc(alloc, http, url, &headers, timeout_ms);
    defer alloc.free(body);
    return try parseGeminiModels(alloc, body);
}

fn listVertex(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const control = vertex.RequestControl.fromTimeout(timeout_ms, http.config.request_cancellation);
    try control.check();
    const base = provider_defaults.normalizedBase(ep.url, provider_defaults.vertex_v1beta1_base);
    const project_id = if (ep.project_id.len > 0)
        ep.project_id
    else
        (try vertex.vertexProjectIdFromConfigAllocWithControl(alloc, if (ep.credentials_path.len > 0) ep.credentials_path else null, control) orelse return error.MissingVertexCredentials);
    defer if (ep.project_id.len == 0) alloc.free(project_id);
    const location = if (ep.location.len > 0) ep.location else provider_defaults.default_google_location;
    const url = try std.fmt.allocPrint(alloc, "{s}/projects/{s}/locations/{s}/publishers/google/models?pageSize=100", .{ base, project_id, location });
    defer alloc.free(url);
    const auth = try vertex.mintAuthorizationValueAllocWithControl(alloc, if (ep.credentials_path.len > 0) ep.credentials_path else null, control);
    defer alloc.free(auth);
    const headers = [_][2][]const u8{.{ "Authorization", auth }};
    const body = try getBodyAlloc(alloc, http, url, &headers, (try control.remainingTimeoutMs()) orelse timeout_ms);
    defer alloc.free(body);
    return try parseVertexPublisherModels(alloc, body);
}

fn listAnthropic(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const raw = if (ep.url.len > 0) ep.url else "https://api.anthropic.com";
    const base = try appendPathIfMissing(alloc, std.mem.trimEnd(u8, raw, "/"), "/v1");
    defer alloc.free(base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models?limit=100", .{base});
    defer alloc.free(url);
    const headers = [3][2][]const u8{
        .{ "x-api-key", ep.api_key orelse "" },
        .{ "anthropic-version", provider_defaults.anthropic_api_version },
        .{ "anthropic-workspace-id", ep.workspace_id },
    };
    const body = try getBodyAlloc(alloc, http, url, headers[0..if (ep.workspace_id.len > 0) @as(usize, 3) else 2], timeout_ms);
    defer alloc.free(body);
    return try parseAnthropicModels(alloc, body);
}

fn listCohere(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const raw = if (ep.url.len > 0) ep.url else provider_defaults.cohere_origin;
    const base = try appendPathIfMissing(alloc, std.mem.trimEnd(u8, raw, "/"), "/v1");
    defer alloc.free(base);
    const url = try std.fmt.allocPrint(alloc, "{s}/models?page_size=100", .{base});
    defer alloc.free(url);
    const body = try getBodyWithBearerAlloc(alloc, http, url, ep.api_key, timeout_ms);
    defer alloc.free(body);
    return try parseCohereModels(alloc, body);
}

fn listBedrock(alloc: std.mem.Allocator, http: *httpx.Client, ep: Endpoint, timeout_ms: u64) !ListResult {
    const region = if (ep.region.len > 0) ep.region else provider_defaults.default_aws_region;
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
    roles: std.StringHashMapUnmanaged([std.meta.tags(ModelKind).len]?usize) = .empty,

    fn init(alloc: std.mem.Allocator) ResultBuilder {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *ResultBuilder) void {
        for (self.models.items) |*model| model.deinit(self.alloc);
        self.models.deinit(self.alloc);
        self.roles.deinit(self.alloc);
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

    fn appendAntfly(
        self: *ResultBuilder,
        name: []const u8,
        display_name: ?[]const u8,
        kind: ModelKind,
        dimensions: ?u32,
        info: ?*const std.json.Value,
    ) !void {
        try self.appendRole(name, display_name, kind, dimensions);
        const slot = @backingInt(if (kind == .chain) ModelKind.generator else kind);
        const index = (self.roles.get(name) orelse return)[slot] orelse return;
        const model = &self.models.items[index];
        const object = if (info) |value| switch (value.*) {
            .object => |*object| object,
            else => return,
        } else return;
        for ([_][]const u8{ "capabilities", "inputs" }) |field_name| {
            if (std.mem.eql(u8, field_name, "capabilities") and model.capabilities.len > 0) continue;
            if (std.mem.eql(u8, field_name, "inputs") and model.inputs.len > 0) continue;
            const raw = object.get(field_name) orelse continue;
            if (raw != .array) continue;
            var values = std.ArrayList([]u8).empty;
            errdefer {
                for (values.items) |value| self.alloc.free(value);
                values.deinit(self.alloc);
            }
            for (raw.array.items) |item| {
                if (item != .string) continue;
                const owned_value = try self.alloc.dupe(u8, item.string);
                errdefer self.alloc.free(owned_value);
                try values.append(self.alloc, owned_value);
            }
            if (std.mem.eql(u8, field_name, "capabilities")) {
                model.capabilities = try values.toOwnedSlice(self.alloc);
            } else {
                model.inputs = try values.toOwnedSlice(self.alloc);
            }
        }
    }

    fn hasName(self: *const ResultBuilder, name: []const u8) bool {
        return self.roles.contains(name);
    }

    fn appendRole(self: *ResultBuilder, name: []const u8, display_name: ?[]const u8, kind: ModelKind, dimensions: ?u32) !void {
        const entry = try self.roles.getOrPut(self.alloc, name);
        if (!entry.found_existing) entry.value_ptr.* = @splat(null);
        const slot = @backingInt(if (kind == .chain) ModelKind.generator else kind);
        if (entry.value_ptr[slot]) |index| {
            if (self.models.items[index].dimensions == null) self.models.items[index].dimensions = dimensions;
            return;
        }
        if (kind == .other and entry.found_existing) return;
        if (kind != .other) if (entry.value_ptr[@backingInt(ModelKind.other)]) |index| {
            self.models.items[index].kind = kind;
            self.models.items[index].dimensions = dimensions orelse self.models.items[index].dimensions;
            entry.value_ptr[@backingInt(ModelKind.other)] = null;
            entry.value_ptr[slot] = index;
            return;
        };
        try self.append(name, display_name, kind, dimensions);
        entry.value_ptr[slot] = self.models.items.len - 1;
    }

    fn toOwned(self: *ResultBuilder) !ListResult {
        const models = try self.models.toOwnedSlice(self.alloc);
        self.roles.deinit(self.alloc);
        self.roles = .empty;
        return .{ .models = models };
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

    // Native groups and task-qualified OpenAI descriptors are complementary
    // views of the same catalog. Preserve every known role without letting
    // an unclassified data entry erase a grouped role or duplicate its ID.
    for (std.meta.tags(ModelKind)) |kind| {
        const group = parsed.value.object.get(kind.groupKey()) orelse continue;
        if (group != .object) continue;
        var it = group.object.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.len == 0) continue;
            try builder.appendAntfly(entry.key_ptr.*, null, kind, antflyDimensions(entry.value_ptr.*), entry.value_ptr);
        }
    }
    if (parsed.value.object.get("data")) |data| if (data == .array) {
        for (data.array.items) |*item| {
            if (item.* != .object) continue;
            const id = item.object.get("id") orelse continue;
            if (id != .string or id.string.len == 0) continue;
            const display = if (item.object.get("display_name")) |value| if (value == .string) value.string else null else null;
            const dimensions = antflyDimensions(item.*);
            var classified = false;
            if (item.object.get("tasks")) |tasks| if (tasks == .array) {
                for (tasks.array.items) |task| if (antflyTaskKind(task)) |kind| {
                    try builder.appendAntfly(id.string, display, kind, dimensions, item);
                    classified = true;
                };
            };
            if (!classified) if (item.object.get("task")) |task| if (antflyTaskKind(task)) |kind| {
                try builder.appendAntfly(id.string, display, kind, dimensions, item);
                classified = true;
            };
            if (!classified and !builder.hasName(id.string)) try builder.appendAntfly(id.string, display, .other, dimensions, item);
        }
    };
    return try builder.toOwned();
}

fn antflyDimensions(value: std.json.Value) ?u32 {
    if (value != .object) return null;
    const dimensions = value.object.get("dimensions") orelse blk: {
        const limits = value.object.get("limits") orelse return null;
        if (limits != .object) return null;
        break :blk limits.object.get("dimensions") orelse return null;
    };
    return if (dimensions == .integer and dimensions.integer > 0) std.math.cast(u32, dimensions.integer) else null;
}

fn antflyTaskKind(value: std.json.Value) ?ModelKind {
    if (value != .string) return null;
    const Pair = struct { []const u8, ModelKind };
    for ([_]Pair{
        .{ "generate", .generator },     .{ "embed", .embedder },         .{ "decide", .decider },
        .{ "rerank", .reranker },        .{ "chunk", .chunker },          .{ "read", .reader },
        .{ "transcribe", .transcriber }, .{ "synthesize", .synthesizer }, .{ "predict", .predictor },
        .{ "extract", .extractor },      .{ "rewrite", .rewriter },
    }) |pair| if (std.mem.eql(u8, value.string, pair[0])) return pair[1];
    return null;
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;
    for (0..haystack.len - needle.len + 1) |i| {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
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

test "parse antfly metadata remains valid across every allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var result = try parseAntflyModels(alloc,
                \\{ "allow_downloads": false, "embedders": { "owner/embed": {
                \\  "capabilities": ["dense", "sparse"], "inputs": ["text", "image"],
                \\  "qualified_limits": { "max_tokens": 256 }
                \\} }, "deciders": { "owner/decide": { "capabilities": ["classification"] } } }
            );
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), result.models.len);
            try std.testing.expectEqualStrings("sparse", result.models[0].capabilities[1]);
            try std.testing.expectEqualStrings("image", result.models[0].inputs[1]);
            try std.testing.expectEqual(ModelKind.decider, result.models[1].kind);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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

fn checkAntflyMergedCatalog(alloc: std.mem.Allocator) !void {
    var result = try parseAntflyModels(alloc,
        \\{"generators":{"g":{"capabilities":["generation"],"inputs":["text"]}},"chains":{"chains/c":{}},
        \\ "synthesizers":{"voice":{}},"predictors":{"risk":{}},
        \\ "data":[{"id":"chains/c","task":"generate"},{"id":"voice","tasks":["synthesize"]},
        \\ {"id":"g","task":"generate","capabilities":["ignored"]},
        \\ {"id":"multi","tasks":["generate","embed","embed"],"capabilities":["dense"],"inputs":["image"],"limits":{"dimensions":768}},
        \\ {"id":"later"},{"id":"later","task":"read"},{"id":"unknown","tasks":["future"]},
        \\ {"id":""},{"id":false},null,{"id":"huge","task":"embed","dimensions":4294967296}]}
    );
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 9), result.models.len);
    const expected = [_]ModelKind{ .generator, .chain, .synthesizer, .predictor, .generator, .embedder, .reader, .other, .embedder };
    for (result.models, expected) |model, kind| try std.testing.expectEqual(kind, model.kind);
    try std.testing.expectEqualStrings("generation", result.models[0].capabilities[0]);
    try std.testing.expectEqualStrings("text", result.models[0].inputs[0]);
    try std.testing.expectEqualStrings("dense", result.models[5].capabilities[0]);
    try std.testing.expectEqualStrings("image", result.models[5].inputs[0]);
    try std.testing.expectEqual(@as(?u32, 768), result.models[5].dimensions);
    try std.testing.expectEqual(@as(?u32, null), result.models[8].dimensions);
}

test "parse antfly merges grouped and task qualified catalogs without losing roles" {
    try checkAntflyMergedCatalog(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkAntflyMergedCatalog, .{});
}
