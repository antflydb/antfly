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
pub const max_candidate_count: u32 = 1000;
pub const vertex_max_candidate_count: u32 = 200;

pub const CredentialKind = enum { optional_api_key, api_key, google_adc };

/// Executable provider behavior resolved before retrieval begins. Keeping
/// defaults and cost ceilings together gives validation, SDK-facing defaults,
/// and dispatch one stable provider capability boundary.
pub const ProviderCapabilities = struct {
    default_model: []const u8,
    default_url: []const u8,
    max_candidate_count: u32,
    model_required: bool,
    credential_kind: CredentialKind,
    credential_env: ?[]const u8 = null,
};

pub fn providerCapabilities(provider: Provider) ProviderCapabilities {
    return switch (provider) {
        .antfly => .{
            .default_model = "",
            .default_url = "http://127.0.0.1:8082",
            .max_candidate_count = max_candidate_count,
            .model_required = false,
            .credential_kind = .optional_api_key,
            .credential_env = "ANTFLY_INFERENCE_API_KEY",
        },
        .cohere => .{
            .default_model = "rerank-english-v3.0",
            .default_url = "https://api.cohere.com",
            .max_candidate_count = max_candidate_count,
            .model_required = true,
            .credential_kind = .api_key,
            .credential_env = "COHERE_API_KEY",
        },
        .vertex => .{
            .default_model = "semantic-ranker-default@latest",
            .default_url = "https://discoveryengine.googleapis.com/v1",
            .max_candidate_count = vertex_max_candidate_count,
            .model_required = true,
            .credential_kind = .google_adc,
        },
    };
}

pub const CandidateLimitDiagnostic = struct {
    provider: Provider,
    maximum: u32,
};

/// Request-owned storage for the structured detail attached to
/// `RerankerCandidateLimitExceeded`. Zig errors intentionally carry no
/// payload, so public request boundaries bind this storage while synchronous
/// query validation runs.
pub const CandidateLimitDiagnosticStorage = struct {
    diagnostic: ?CandidateLimitDiagnostic = null,
};

threadlocal var active_candidate_limit_storage: ?*CandidateLimitDiagnosticStorage = null;

pub const CandidateLimitDiagnosticBinding = struct {
    previous: ?*CandidateLimitDiagnosticStorage,

    pub fn deinit(self: CandidateLimitDiagnosticBinding) void {
        active_candidate_limit_storage = self.previous;
    }
};

pub fn bindCandidateLimitDiagnostic(storage: *CandidateLimitDiagnosticStorage) CandidateLimitDiagnosticBinding {
    const previous = active_candidate_limit_storage;
    active_candidate_limit_storage = storage;
    return .{ .previous = previous };
}

pub fn resetCandidateLimitDiagnostic() void {
    if (active_candidate_limit_storage) |storage| storage.diagnostic = null;
}

pub fn takeCandidateLimitDiagnostic() ?CandidateLimitDiagnostic {
    const storage = active_candidate_limit_storage orelse return null;
    const diagnostic = storage.diagnostic;
    storage.diagnostic = null;
    return diagnostic;
}

fn recordCandidateLimitDiagnostic(provider: Provider) void {
    const storage = active_candidate_limit_storage orelse return;
    storage.diagnostic = .{
        .provider = provider,
        .maximum = maxCandidateCountForProvider(provider),
    };
}

fn candidateLimitExceeded(provider: Provider) error{RerankerCandidateLimitExceeded} {
    recordCandidateLimitDiagnostic(provider);
    return error.RerankerCandidateLimitExceeded;
}

/// The provider request is deliberately a single globally-ranked window. Keep
/// its ceiling aligned with each upstream API instead of accepting work that
/// the selected provider cannot execute or splitting scores into incomparable
/// batches.
pub fn maxCandidateCountForProvider(provider: Provider) u32 {
    return providerCapabilities(provider).max_candidate_count;
}

/// Stable provider defaults live at the executable boundary so every client
/// (generated or hand-written) gets the same behavior when `model` is omitted.
pub fn defaultModelForProvider(provider: Provider) []const u8 {
    return providerCapabilities(provider).default_model;
}

pub const Config = struct {
    rate_limit: ?openapi.RateLimitConfig = null,
    provider: Provider,
    field: []const u8 = "",
    template: []const u8 = "",
    model: []const u8 = "",
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    project_id: []const u8 = "",
    credentials_path: []const u8 = "",
    candidate_count: ?u32 = null,
    top_n: ?u32 = null,

    pub fn clone(self: Config, alloc: Allocator) !Config {
        return .{
            .rate_limit = self.rate_limit,
            .provider = self.provider,
            .field = if (self.field.len > 0) try alloc.dupe(u8, self.field) else "",
            .template = if (self.template.len > 0) try alloc.dupe(u8, self.template) else "",
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .url = if (self.url.len > 0) try alloc.dupe(u8, self.url) else "",
            .api_key = if (self.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
            .project_id = if (self.project_id.len > 0) try alloc.dupe(u8, self.project_id) else "",
            .credentials_path = if (self.credentials_path.len > 0) try alloc.dupe(u8, self.credentials_path) else "",
            .candidate_count = self.candidate_count,
            .top_n = self.top_n,
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
        const capabilities = providerCapabilities(self.provider);
        if (capabilities.model_required and self.model.len == 0) return error.InvalidRerankerConfig;
        const provider_max = capabilities.max_candidate_count;
        if (self.candidate_count) |candidate_count| {
            if (candidate_count == 0) return error.InvalidRerankerConfig;
            if (candidate_count > provider_max) return candidateLimitExceeded(self.provider);
        }
        if (self.top_n) |top_n| {
            if (top_n == 0) return error.InvalidRerankerConfig;
            if (top_n > provider_max) return candidateLimitExceeded(self.provider);
        }
        if (self.candidate_count != null and self.top_n != null and self.top_n.? > self.candidate_count.?) {
            return error.InvalidRerankerConfig;
        }
    }

    pub fn defaultedUrl(self: Config) []const u8 {
        if (self.url.len > 0) return self.url;
        return providerCapabilities(self.provider).default_url;
    }
};

/// Validate the effective retrieval window before any shard fan-out begins.
/// `candidate_count` is optional for ergonomic small queries, but omitting it
/// must not let `offset + limit` bypass the same provider-work ceiling.
pub fn validateQueryWindow(cfg: Config, offset: u32, limit: u32) !void {
    try cfg.validate();
    const output_limit = cfg.top_n orelse limit;
    const page_end = offset +| output_limit;
    if (cfg.candidate_count) |candidate_count| {
        // An explicit cost window must still contain the requested page. Fail
        // before retrieval instead of returning a misleading short page.
        if (candidate_count < page_end) return error.InvalidRerankerConfig;
    }
    const effective_candidates = cfg.candidate_count orelse page_end;
    if (effective_candidates > maxCandidateCountForProvider(cfg.provider))
        return candidateLimitExceeded(cfg.provider);
}

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
    const model = generated.model orelse defaultModelForProvider(generated.provider);
    var cfg = Config{
        .rate_limit = generated.rate_limit,
        .provider = generated.provider,
        .field = if (generated.field) |field| try alloc.dupe(u8, field) else "",
        .template = if (generated.template) |template| try alloc.dupe(u8, template) else "",
        .model = if (model.len > 0) try alloc.dupe(u8, model) else "",
        .url = if (generated.url) |url| try alloc.dupe(u8, url) else "",
        .api_key = if (generated.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
        .project_id = if (generated.project_id) |project_id| try alloc.dupe(u8, project_id) else "",
        .credentials_path = if (generated.credentials_path) |credentials_path| try alloc.dupe(u8, credentials_path) else "",
        .candidate_count = if (generated.candidate_count) |candidate_count|
            std.math.cast(u32, candidate_count) orelse return error.InvalidRerankerConfig
        else
            null,
        .top_n = if (generated.top_n) |top_n|
            std.math.cast(u32, top_n) orelse return error.InvalidRerankerConfig
        else
            null,
    };
    errdefer cfg.deinit(alloc);
    try cfg.validate();
    return cfg;
}

test "external reranker models have executable defaults" {
    const antfly = providerCapabilities(.antfly);
    try std.testing.expectEqualStrings("", antfly.default_model);
    try std.testing.expectEqualStrings("http://127.0.0.1:8082", antfly.default_url);
    try std.testing.expectEqual(CredentialKind.optional_api_key, antfly.credential_kind);
    try std.testing.expectEqualStrings("ANTFLY_INFERENCE_API_KEY", antfly.credential_env.?);
    try std.testing.expect(!antfly.model_required);

    var cohere = try configFromOpenApi(std.testing.allocator, .{
        .provider = .cohere,
        .field = "body",
    });
    defer cohere.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("rerank-english-v3.0", cohere.model);
    try std.testing.expectEqualStrings("https://api.cohere.com", cohere.defaultedUrl());
    try std.testing.expectEqual(CredentialKind.api_key, providerCapabilities(.cohere).credential_kind);

    var vertex = try configFromOpenApi(std.testing.allocator, .{
        .provider = .vertex,
        .field = "body",
    });
    defer vertex.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("semantic-ranker-default@latest", vertex.model);
    try std.testing.expectEqualStrings("https://discoveryengine.googleapis.com/v1", vertex.defaultedUrl());
    try std.testing.expectEqual(vertex_max_candidate_count, providerCapabilities(.vertex).max_candidate_count);
    try std.testing.expectEqual(CredentialKind.google_adc, providerCapabilities(.vertex).credential_kind);
}

pub fn openApiFromConfig(cfg: Config) openapi.RerankerConfig {
    return .{
        .rate_limit = cfg.rate_limit,
        .provider = cfg.provider,
        .field = if (cfg.field.len > 0) cfg.field else null,
        .template = if (cfg.template.len > 0) cfg.template else null,
        .model = if (cfg.model.len > 0) cfg.model else null,
        .url = if (cfg.url.len > 0) cfg.url else null,
        .api_key = cfg.api_key,
        .project_id = if (cfg.project_id.len > 0) cfg.project_id else null,
        .credentials_path = if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
        .candidate_count = if (cfg.candidate_count) |candidate_count| candidate_count else null,
        .top_n = if (cfg.top_n) |top_n| top_n else null,
    };
}

test "reranker config round trip" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"antfly","model":"cross-encoder/ms-marco-MiniLM-L-6-v2","url":"http://localhost:8082","field":"body","candidate_count":16,"top_n":8}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);

    try std.testing.expectEqual(.antfly, cfg.provider);
    try std.testing.expectEqualStrings("body", cfg.field);
    try std.testing.expectEqualStrings("cross-encoder/ms-marco-MiniLM-L-6-v2", cfg.model);
    try std.testing.expectEqual(@as(?u32, 16), cfg.candidate_count);
    try std.testing.expectEqual(@as(?u32, 8), cfg.top_n);

    const encoded = try stringifyAlloc(alloc, cfg);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.antfly, reparsed.provider);
    try std.testing.expectEqualStrings("body", reparsed.field);
}

test "reranker output count cannot exceed candidate count" {
    const cfg = Config{
        .provider = .antfly,
        .field = "body",
        .model = "reranker",
        .candidate_count = 4,
        .top_n = 5,
    };
    try std.testing.expectError(error.InvalidRerankerConfig, cfg.validate());
}

test "reranker work is bounded with and without an explicit candidate count" {
    const base = Config{ .provider = .antfly, .field = "body" };
    var explicit = base;
    explicit.candidate_count = max_candidate_count + 1;
    try std.testing.expectError(error.RerankerCandidateLimitExceeded, explicit.validate());
    try std.testing.expectError(error.RerankerCandidateLimitExceeded, validateQueryWindow(base, 1, max_candidate_count));
    try validateQueryWindow(base, 0, max_candidate_count);
}

test "reranker candidate window must contain the requested page" {
    const cfg = Config{
        .provider = .antfly,
        .field = "body",
        .candidate_count = 14,
    };
    try std.testing.expectError(error.InvalidRerankerConfig, validateQueryWindow(cfg, 5, 10));

    var exact = cfg;
    exact.candidate_count = 15;
    try validateQueryWindow(exact, 5, 10);
}

test "reranker candidate limits follow provider request capabilities" {
    const vertex = Config{
        .provider = .vertex,
        .field = "body",
        .model = "semantic-ranker-default@latest",
    };
    try validateQueryWindow(vertex, 0, vertex_max_candidate_count);
    try std.testing.expectError(
        error.RerankerCandidateLimitExceeded,
        validateQueryWindow(vertex, 0, vertex_max_candidate_count + 1),
    );

    const cohere = Config{
        .provider = .cohere,
        .field = "body",
        .model = "rerank-v4.0-pro",
    };
    try validateQueryWindow(cohere, 0, max_candidate_count);
}

test "reranker candidate limit diagnostics are request scoped and provider specific" {
    var outer_storage: CandidateLimitDiagnosticStorage = .{};
    const outer_binding = bindCandidateLimitDiagnostic(&outer_storage);
    defer outer_binding.deinit();

    const cohere = Config{
        .provider = .cohere,
        .field = "body",
        .model = "rerank-v4.0-pro",
        .candidate_count = max_candidate_count + 1,
    };
    try std.testing.expectError(error.RerankerCandidateLimitExceeded, cohere.validate());
    const outer = takeCandidateLimitDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(Provider.cohere, outer.provider);
    try std.testing.expectEqual(max_candidate_count, outer.maximum);
    try std.testing.expect(takeCandidateLimitDiagnostic() == null);

    recordCandidateLimitDiagnostic(.antfly);
    var inner_storage: CandidateLimitDiagnosticStorage = .{};
    const inner_binding = bindCandidateLimitDiagnostic(&inner_storage);
    const vertex = Config{
        .provider = .vertex,
        .field = "body",
        .model = "semantic-ranker-default@latest",
    };
    try std.testing.expectError(
        error.RerankerCandidateLimitExceeded,
        validateQueryWindow(vertex, 0, vertex_max_candidate_count + 1),
    );
    const inner = takeCandidateLimitDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(Provider.vertex, inner.provider);
    try std.testing.expectEqual(vertex_max_candidate_count, inner.maximum);
    inner_binding.deinit();

    const restored = takeCandidateLimitDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(Provider.antfly, restored.provider);
    try std.testing.expectEqual(max_candidate_count, restored.maximum);
}

test "reranker config requires field or template" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidRerankerConfig,
        parseConfigFromSlice(alloc, "{\"provider\":\"antfly\"}"),
    );
}

test "antfly reranker config permits the inference default model" {
    const alloc = std.testing.allocator;
    var cfg = try parseConfigFromSlice(alloc, "{\"provider\":\"antfly\",\"field\":\"body\"}");
    defer cfg.deinit(alloc);

    try std.testing.expectEqual(Provider.antfly, cfg.provider);
    try std.testing.expectEqualStrings("", cfg.model);
    try std.testing.expectEqualStrings("body", cfg.field);
}

test "reranking config preserves shared rate limits through cloning and JSON" {
    const alloc = std.testing.allocator;
    var cfg = try parseConfigFromSlice(alloc, "{\"provider\":\"antfly\",\"model\":\"test\",\"field\":\"body\",\"rate_limit\":{\"requests_per_minute\":120,\"burst\":2,\"tokens_per_minute\":1000,\"max_concurrency\":4}}");
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
