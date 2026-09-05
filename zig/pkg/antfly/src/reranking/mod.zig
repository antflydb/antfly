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
const httpx = @import("httpx");
const lib = @import("antfly_reranking");
const managed_embedder = @import("../inference/managed_embedder.zig");
const inference_request_context = @import("../inference/request_context.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const antfly_provider = @import("../inference/local.zig");
const vertex_provider = @import("../inference/vertex.zig");
const common_secrets = @import("../common/secrets.zig");
const request_admission = @import("../common/request_admission.zig");
const common_cancellation = @import("../common/cancellation.zig");
const google_auth = @import("antfly_google").auth;

pub const Config = lib.Config;
pub const Provider = lib.Provider;
pub const ProviderCapabilities = lib.ProviderCapabilities;
pub const providerCapabilities = lib.providerCapabilities;
pub const max_candidate_count = lib.max_candidate_count;

/// Long-lived resources shared by reranking requests. This keeps HTTP
/// connections warm and lets ADC refresh single-flight per credential/scope.
pub const Runtime = struct {
    io: std.Io,
    http: httpx.Client,
    credentials: google_auth.CredentialManager,
    admission: request_admission.RequestAdmission = request_admission.RequestAdmission.init(16),

    pub fn init(alloc: std.mem.Allocator, io: std.Io) Runtime {
        return .{
            .io = io,
            .http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = true }),
            .credentials = google_auth.CredentialManager.init(alloc, io),
        };
    }

    /// Runs one batched provider call through the process-level pool. The
    /// concurrency gate protects provider sockets and credential refresh from
    /// request fan-in while preserving I/O cancellation.
    pub fn rerank(
        self: *Runtime,
        alloc: std.mem.Allocator,
        cfg: Config,
        dependencies: Options,
        query: []const u8,
        documents: []const []const u8,
    ) ![]f32 {
        var lease = try self.acquire(dependencies.execution_context);
        defer lease.release();
        return try self.rerankAdmitted(alloc, cfg, dependencies, query, documents);
    }

    /// Admits the complete reranking phase, including document rendering that
    /// callers may need to perform before invoking the provider.
    pub fn acquire(
        self: *Runtime,
        execution_context: ?inference_request_context.RequestContext,
    ) !AdmissionLease {
        if (execution_context) |context| try context.check();
        // Provider work is deliberately fail-fast rather than queued. The
        // parent query already owns admission and a deadline; another hidden
        // queue only consumes that budget and amplifies tail latency.
        return self.admission.tryAcquireLease() orelse error.RerankRateLimited;
    }

    /// Executes provider work for a caller that already owns an admission
    /// lease. Keeping this separate prevents a second admission attempt after
    /// the caller has rendered the candidate documents.
    pub fn rerankAdmitted(
        self: *Runtime,
        alloc: std.mem.Allocator,
        cfg: Config,
        dependencies: Options,
        query: []const u8,
        documents: []const []const u8,
    ) ![]f32 {
        if (dependencies.execution_context) |context| try context.check();
        var options = dependencies;
        options.runtime = self;
        return try rerankDocumentsWithOptions(
            alloc,
            &self.http,
            cfg,
            options,
            query,
            documents,
        );
    }

    pub fn deinit(self: *Runtime) void {
        self.credentials.deinit();
        self.http.deinit();
        self.* = undefined;
    }
};

pub const AdmissionLease = request_admission.RequestAdmission.Lease;

/// Collapses provider and transport failures into the stable query-layer
/// taxonomy shared by table, global, and retrieval-agent queries.
pub fn normalizeOperationalError(err: anyerror) anyerror {
    return switch (err) {
        error.RerankRateLimited,
        error.RerankTransientFailure,
        error.RerankUpstreamFailure,
        error.Timeout,
        => err,
        // httpx spells transport cancellation `Canceled`; collapse both
        // spellings to the process-wide semantic cancellation before this
        // error crosses into query dependency classification.
        error.Canceled,
        error.Cancelled,
        => error.Cancelled,
        error.QueueFull => error.RerankRateLimited,
        error.RerankRequestFailed,
        error.EmptyResponse,
        error.InvalidRerankerResponse,
        error.InvalidResponse,
        => error.RerankUpstreamFailure,
        error.ConnectionTimedOut => error.Timeout,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.HostUnreachable,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        => error.RerankTransientFailure,
        else => err,
    };
}

pub fn statusError(status: u16) anyerror {
    return switch (status) {
        408, 504 => error.Timeout,
        429 => error.RerankRateLimited,
        500...503, 505...599 => error.RerankTransientFailure,
        else => error.RerankRequestFailed,
    };
}

test "reranking runtime failures use stable query dependency classes" {
    try std.testing.expectEqual(error.RerankRateLimited, statusError(429));
    try std.testing.expectEqual(error.RerankTransientFailure, statusError(503));
    try std.testing.expectEqual(error.Timeout, statusError(504));
    try std.testing.expectEqual(error.RerankRequestFailed, statusError(401));
    try std.testing.expectEqual(error.RerankUpstreamFailure, normalizeOperationalError(error.InvalidRerankerResponse));
    try std.testing.expectEqual(error.RerankTransientFailure, normalizeOperationalError(error.ConnectionRefused));
    try std.testing.expectEqual(error.Timeout, normalizeOperationalError(error.ConnectionTimedOut));
    try std.testing.expectEqual(error.Cancelled, normalizeOperationalError(error.Canceled));
    try std.testing.expectEqual(error.Cancelled, normalizeOperationalError(error.Cancelled));
    try std.testing.expectEqual(error.RerankRateLimited, normalizeOperationalError(error.QueueFull));
    try std.testing.expectEqual(error.RerankRateLimited, normalizeOperationalError(error.RerankRateLimited));
    try std.testing.expectEqual(error.RerankTransientFailure, normalizeOperationalError(error.RerankTransientFailure));
    try std.testing.expectEqual(error.RerankUpstreamFailure, normalizeOperationalError(error.RerankUpstreamFailure));
    try std.testing.expectEqual(error.Timeout, normalizeOperationalError(error.Timeout));
}

test "reranking runtime rejects saturation and expired work before provider dispatch" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var runtime = Runtime.init(alloc, io_impl.io());
    defer runtime.deinit();
    runtime.admission = request_admission.RequestAdmission.init(1);
    try std.testing.expect(runtime.admission.tryAcquire());
    defer runtime.admission.release();

    const cfg = Config{ .provider = .antfly, .field = "body" };
    try std.testing.expectError(
        error.RerankRateLimited,
        runtime.rerank(alloc, cfg, .{}, "query", &.{"document"}),
    );
    try std.testing.expectError(
        error.Timeout,
        runtime.rerank(alloc, cfg, .{ .execution_context = .{
            .io = io_impl.io(),
            .deadline_ns = 0,
        } }, "query", &.{"document"}),
    );
}

pub fn rerankDocuments(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    query: []const u8,
    documents: []const []const u8,
) ![]f32 {
    return try rerankDocumentsWithAntflyProvider(alloc, http, cfg, null, query, documents);
}

pub fn rerankDocumentsWithAntflyProvider(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    embedded_antfly_provider: ?managed_embedder.AntflyProvider,
    query: []const u8,
    documents: []const []const u8,
) ![]f32 {
    return try rerankDocumentsWithOptions(alloc, http, cfg, .{ .antfly_provider = embedded_antfly_provider }, query, documents);
}

pub const Options = struct {
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    runtime: ?*Runtime = null,
    execution_context: ?inference_request_context.RequestContext = null,
};

pub fn rerankDocumentsWithOptions(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    options: Options,
    query: []const u8,
    documents: []const []const u8,
) ![]f32 {
    try cfg.validate();
    if (options.execution_context) |context| try context.check();
    const capabilities = providerCapabilities(cfg.provider);
    const configured_secret = switch (capabilities.credential_kind) {
        .api_key => try common_secrets.SecretValue.initConfigOrEnv(alloc, cfg.api_key, "COHERE_API_KEY"),
        .none, .google_adc => try common_secrets.SecretValue.initConfig(alloc, cfg.api_key),
    };
    const api_key = if (configured_secret) |secret_value| blk: {
        var owned_secret = secret_value;
        defer owned_secret.deinit(alloc);
        break :blk try owned_secret.resolveOwned(alloc, options.secret_store);
    } else null;
    defer if (api_key) |value| alloc.free(value);

    switch (cfg.provider) {
        .antfly => {
            if (cfg.url.len == 0) {
                if (options.antfly_provider) |local| {
                    if (options.execution_context) |context| {
                        if (local.rerank_texts_with_context) |rerank| {
                            const scores = try rerank(local.ptr, alloc, cfg.model, query, documents, context);
                            try context.check();
                            return scores;
                        }
                    }
                    if (local.rerank_texts) |rerank| {
                        const scores = try rerank(local.ptr, alloc, cfg.model, query, documents);
                        errdefer alloc.free(scores);
                        try validateScores(scores, documents.len);
                        if (options.execution_context) |context| try context.check();
                        return scores;
                    }
                }
            }
            var provider = antfly_provider.Provider.init(alloc, http, cfg.defaultedUrl());
            defer provider.deinit();
            if (options.execution_context) |context| {
                provider.setRequestCancellation(context.cancellation);
                provider.setRequestTimeoutMs(try context.remainingTimeoutMs());
            }
            var result = try provider.reranker().rerank(alloc, cfg.model, query, documents);
            defer result.deinit();
            try validateScores(result.scores, documents.len);
            if (options.execution_context) |context| try context.check();
            return try alloc.dupe(f32, result.scores);
        },
        .cohere => {
            const token = api_key orelse return error.InvalidRerankerConfig;
            const scores = try rerankCohere(alloc, http, cfg, token, options.execution_context, query, documents);
            errdefer alloc.free(scores);
            try validateScores(scores, documents.len);
            if (options.execution_context) |context| try context.check();
            return scores;
        },
        .vertex => {
            const token_source = if (api_key == null and options.runtime != null)
                options.runtime.?.credentials.tokenSource(
                    if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
                    "https://www.googleapis.com/auth/cloud-platform",
                ) catch return error.InvalidRerankerConfig
            else
                null;
            var provider = try vertex_provider.Provider.init(alloc, http, .{
                .base_url = cfg.defaultedUrl(),
                .project_id = if (cfg.project_id.len > 0) cfg.project_id else null,
                .credentials_path = if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
                .bearer_token = api_key,
                .token_source = token_source,
            });
            defer provider.deinit();
            const scores = try provider.rerank(alloc, cfg.model, query, documents, .{
                .timeout_ms = if (options.execution_context) |context| try context.remainingTimeoutMs() else null,
                .cancellation = httpCancellation(if (options.execution_context) |context| context.cancellation else null),
            });
            errdefer alloc.free(scores);
            try validateScores(scores, documents.len);
            if (options.execution_context) |context| try context.check();
            return scores;
        },
    }
}

fn validateScores(scores: []const f32, document_count: usize) !void {
    if (scores.len != document_count) return error.InvalidRerankerResponse;
    for (scores) |score| if (!std.math.isFinite(score)) return error.InvalidRerankerResponse;
}

fn rerankCohere(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    api_key: []const u8,
    execution_context: ?inference_request_context.RequestContext,
    query: []const u8,
    documents: []const []const u8,
) ![]f32 {
    const body = try std.json.Stringify.valueAlloc(alloc, .{
        .model = cfg.model,
        .query = query,
        .documents = documents,
    }, .{});
    defer alloc.free(body);
    const url = try std.fmt.allocPrint(
        alloc,
        "{s}/v2/rerank",
        .{cfg.defaultedUrl()},
    );
    defer alloc.free(url);
    const authorization = try std.fmt.allocPrint(alloc, "Bearer {s}", .{api_key});
    defer alloc.free(authorization);
    const headers = [_][2][]const u8{.{ "Authorization", authorization }};
    var response = try http.post(url, .{
        .json = body,
        .headers = &headers,
        .timeout_ms = if (execution_context) |context| try context.remainingTimeoutMs() else null,
        .cancellation = httpCancellation(if (execution_context) |context| context.cancellation else null),
    });
    defer response.deinit();
    if (!response.ok()) return statusError(response.status.code);
    const Response = struct {
        results: []const struct {
            index: usize,
            relevance_score: f32,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Response, alloc, response.body orelse return error.EmptyResponse, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return try scoresByIndexAlloc(alloc, documents.len, parsed.value.results);
}

fn httpCancellation(cancellation: ?common_cancellation.CancellationToken) ?httpx.CancellationToken {
    const token = cancellation orelse return null;
    return httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn);
}

fn scoresByIndexAlloc(alloc: std.mem.Allocator, count: usize, results: anytype) ![]f32 {
    const scores = try alloc.alloc(f32, count);
    errdefer alloc.free(scores);
    @memset(scores, 0);
    const seen = try alloc.alloc(bool, count);
    defer alloc.free(seen);
    @memset(seen, false);
    for (results) |result| {
        if (result.index >= count or seen[result.index]) return error.InvalidRerankerResponse;
        scores[result.index] = result.relevance_score;
        seen[result.index] = true;
    }
    for (seen) |present| if (!present) return error.InvalidRerankerResponse;
    return scores;
}

test "reranking runtime maps Cohere scores back to input order" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var server = try httpx.TestServer.start(alloc, io, &.{.{
        .method = .POST,
        .path = "/v2/rerank",
        .respond = .{ .body = "{\"results\":[{\"index\":1,\"relevance_score\":0.9},{\"index\":0,\"relevance_score\":0.25}]}" },
    }});
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var scores: ?[]f32 = null;
    defer if (scores) |value| alloc.free(value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_client: *httpx.Client, base_url: []const u8, out: *?[]f32, err_out: *?anyerror) std.Io.Cancelable!void {
            out.* = rerankDocuments(a, test_client, .{
                .provider = .cohere,
                .model = "rerank-v4.0-pro",
                .url = base_url,
                .api_key = "test-token",
                .field = "body",
            }, "query", &.{ "first", "second" }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    group.concurrent(io, Fiber.run, .{ alloc, &client, server.baseUrl(), &scores, &run_err }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), scores.?[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), scores.?[1], 0.0001);
}

test "reranking runtime delegates to antfly provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/rerank", .respond = .{
            .body = "{\"object\":\"list\",\"data\":[{\"object\":\"rerank.score\",\"index\":0,\"score\":0.9},{\"object\":\"rerank.score\",\"index\":1,\"score\":0.25}],\"model\":\"cross-encoder/ms-marco-MiniLM-L-6-v2\",\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":0,\"total_tokens\":4}}",
        } },
    });
    defer ts.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const url = try std.fmt.allocPrint(alloc, "{s}", .{ts.baseUrl()});
    defer alloc.free(url);
    var scores: ?[]f32 = null;
    defer if (scores) |value| alloc.free(value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: std.mem.Allocator,
            test_io: std.Io,
            test_client: *httpx.Client,
            reranker_url: []const u8,
            out: *?[]f32,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            _ = test_io;
            const cfg = Config{
                .provider = .antfly,
                .model = "cross-encoder/ms-marco-MiniLM-L-6-v2",
                .url = reranker_url,
                .field = "body",
            };
            out.* = rerankDocuments(a, test_client, cfg, "query", &.{ "doc1", "doc2" }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, &client, url, &scores, &run_err }) catch return;
    try ts.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqual(@as(usize, 2), scores.?.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), scores.?[0], 0.0001);
}

test "reranking runtime routes antfly provider to local antfly" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    const State = struct {
        called: bool = false,

        fn dense(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![][]f32 {
            return try a.alloc([]f32, 0);
        }

        fn sparse(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![]db_embedder.SparseEmbedding {
            return try a.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn rerank(ptr: *anyopaque, a: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) anyerror![]f32 {
            const state: *@This() = @ptrCast(@alignCast(ptr));
            state.called = true;
            try std.testing.expectEqualStrings("", model);
            try std.testing.expectEqualStrings("query", query);
            try std.testing.expectEqual(@as(usize, 2), documents.len);
            const scores = try a.alloc(f32, 2);
            scores[0] = 0.2;
            scores[1] = 0.8;
            return scores;
        }
    };

    var state = State{};
    const local = managed_embedder.AntflyProvider{
        .ptr = &state,
        .embed_dense_texts = State.dense,
        .embed_sparse_texts = State.sparse,
        .rerank_texts = State.rerank,
    };
    const cfg = Config{
        .provider = .antfly,
        .field = "body",
    };
    const scores = try rerankDocumentsWithAntflyProvider(alloc, &client, cfg, local, "query", &.{ "doc1", "doc2" });
    defer alloc.free(scores);
    try std.testing.expect(state.called);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), scores[1], 0.0001);
}
