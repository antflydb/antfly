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
const provider_limits = @import("../common/provider_limits.zig");
const credential_identity = @import("../common/credential_source_identity.zig");
const google_auth = @import("antfly_google").auth;

pub const Config = lib.Config;
pub const Provider = lib.Provider;
pub const ProviderCapabilities = lib.ProviderCapabilities;
pub const providerCapabilities = lib.providerCapabilities;
pub const max_candidate_count = lib.max_candidate_count;

/// Long-lived resources shared by reranking requests. This keeps HTTP
/// connections warm and lets ADC refresh single-flight per credential/scope.
pub const Runtime = struct {
    limits: *provider_limits.Registry = &provider_limits.process_registry,
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
        error.QueueFull, error.ProviderQuotaRegistryFull => error.RerankRateLimited,
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
    try std.testing.expectEqual(error.RerankRateLimited, normalizeOperationalError(error.ProviderQuotaRegistryFull));
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
    limits: *provider_limits.Registry = &provider_limits.process_registry,
    execution_context: ?inference_request_context.RequestContext = null,
};

/// One request-owned authentication snapshot drives both the wire credential
/// and quota identity. Keep source ownership until the quota lease is released;
/// secret rotation changes the token, not the source identity.
const RequestAuthentication = struct {
    secret: ?common_secrets.SecretValue,
    token: ?[]u8,

    fn init(alloc: std.mem.Allocator, cfg: Config, store: ?*common_secrets.FileStore) !RequestAuthentication {
        const capabilities = providerCapabilities(cfg.provider);
        var secret = if (capabilities.credential_env) |env_name|
            try common_secrets.SecretValue.initConfigOrEnv(alloc, cfg.api_key, env_name)
        else
            try common_secrets.SecretValue.initConfig(alloc, cfg.api_key);
        errdefer if (secret) |*value| value.deinit(alloc);
        const token = if (secret) |value| try value.resolveOwned(alloc, store) else null;
        errdefer if (token) |value| alloc.free(value);
        if (token) |value| {
            if (value.len == 0) return error.InvalidRerankerConfig;
        } else if (capabilities.credential_kind == .api_key) return error.InvalidRerankerConfig;
        return .{ .secret = secret, .token = token };
    }

    fn identity(self: *const RequestAuthentication, cfg: Config) credential_identity.CredentialSourceIdentity {
        if (self.token != null) return credential_identity.fromSecretValue(self.secret);
        if (cfg.provider == .vertex)
            return credential_identity.CredentialSourceIdentity.googleAdc(if (cfg.credentials_path.len > 0) cfg.credentials_path else null);
        return credential_identity.CredentialSourceIdentity.none();
    }

    fn deinit(self: *RequestAuthentication, alloc: std.mem.Allocator) void {
        if (self.token) |value| alloc.free(value);
        if (self.secret) |*value| value.deinit(alloc);
        self.* = undefined;
    }
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
    const policy = try provider_limits.Policy.fromConfig(cfg.rate_limit);
    // Select embedded execution before touching outbound credentials. Local
    // callbacks have no bearer credential or outbound quota scope.
    if (cfg.provider == .antfly and cfg.url.len == 0) {
        if (options.antfly_provider) |local| {
            if (policy.enabled()) return error.UnsupportedLocalRateLimit;
            if (options.execution_context) |context| {
                if (local.rerank_texts_with_context) |rerank| {
                    const scores = try rerank(local.ptr, alloc, cfg.model, query, documents, context);
                    try context.check();
                    return scores;
                }
            }
            if (local.rerank_texts) |rerank| {
                const scores = try rerank(local.ptr, alloc, cfg.model, query, documents);
                if (options.execution_context) |context| try context.check();
                return scores;
            }
        }
    }

    var auth = try RequestAuthentication.init(alloc, cfg, options.secret_store);
    defer auth.deinit(alloc);
    const source = auth.identity(cfg);
    const api_key = auth.token;
    switch (cfg.provider) {
        .antfly => {
            var provider = antfly_provider.Provider.init(alloc, http, cfg.defaultedUrl());
            defer provider.deinit();
            if (api_key) |token| try provider.setApiKey(token);
            var quota = try acquireQuota(cfg, options, source, "", "", policy);
            defer quota.release();
            provider.attempt_observer = quota.limiter().observer(0);
            if (options.execution_context) |context| {
                provider.setRequestCancellation(context.cancellation);
                provider.setRequestTimeoutMs(try context.remainingTimeoutMs());
            }
            var result = try provider.reranker().rerank(alloc, cfg.model, query, documents);
            defer result.deinit();
            if (options.execution_context) |context| try context.check();
            return try alloc.dupe(f32, result.scores);
        },
        .cohere => {
            const token = api_key orelse return error.InvalidRerankerConfig;
            var quota = try acquireQuota(cfg, options, source, "", "", policy);
            defer quota.release();
            const scores = try rerankCohere(alloc, http, cfg, token, options.execution_context, quota.limiter().observer(0), query, documents);
            errdefer alloc.free(scores);
            if (options.execution_context) |context| try context.check();
            return scores;
        },
        .vertex => {
            const auth_control = @import("antfly_google").RequestControl{
                .deadline_ns = if (options.execution_context) |context| context.deadline_ns else null,
                .cancellation = httpCancellation(if (options.execution_context) |context| context.cancellation else null),
            };
            const token_source = if (api_key == null and options.runtime != null)
                options.runtime.?.credentials.tokenSourceWithControl(
                    if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
                    "https://www.googleapis.com/auth/cloud-platform",
                    auth_control,
                ) catch |err| switch (err) {
                    error.Cancelled, error.Timeout, error.OutOfMemory => return err,
                    else => return error.InvalidRerankerConfig,
                }
            else
                null;
            var provider = try vertex_provider.Provider.init(alloc, http, .{
                .base_url = cfg.defaultedUrl(),
                .project_id = if (cfg.project_id.len > 0) cfg.project_id else null,
                .credentials_path = if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
                .bearer_token = api_key,
                .token_source = token_source,
                .request_control = auth_control,
            });
            defer provider.deinit();
            var quota = try acquireQuota(cfg, options, source, provider.project_id, "global", policy);
            defer quota.release();
            provider.attempt_observer = quota.limiter().observer(0);
            const scores = try provider.rerank(alloc, cfg.model, query, documents, .{
                .timeout_ms = if (options.execution_context) |context| try context.remainingTimeoutMs() else null,
                .cancellation = httpCancellation(if (options.execution_context) |context| context.cancellation else null),
            });
            errdefer alloc.free(scores);
            if (options.execution_context) |context| try context.check();
            return scores;
        },
    }
}

fn acquireQuota(cfg: Config, options: Options, source: credential_identity.CredentialSourceIdentity, project: []const u8, location: []const u8, policy: provider_limits.Policy) !provider_limits.Handle {
    const registry = if (options.runtime) |runtime| runtime.limits else options.limits;
    return registry.acquire(.{ .operation = .reranking, .endpoint = .{
        .provider = std.meta.stringToEnum(provider_limits.Provider, @tagName(cfg.provider)).?,
        .endpoint = cfg.defaultedUrl(),
        .model = cfg.model,
        .project = project,
        .location = location,
        .credentials = source,
    } }, policy);
}

fn rerankCohere(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    api_key: []const u8,
    execution_context: ?inference_request_context.RequestContext,
    observer: httpx.AttemptObserver,
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
        .attempt_observer = observer,
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

test "reranking runtime validates policies and rejects oversized text before dispatch" {
    const alloc = std.testing.allocator;
    var registry = provider_limits.Registry.init(alloc);
    defer registry.deinit();
    var client = httpx.Client.initWithConfig(alloc, std.testing.io, .{});
    defer client.deinit();
    var cfg = Config{ .provider = .cohere, .model = "rerank-v4.0-pro", .url = "http://127.0.0.1:1", .api_key = "test", .field = "body", .rate_limit = .{ .requests_per_minute = 0 } };
    const options = Options{ .limits = &registry };
    try std.testing.expectError(error.InvalidRateLimitPolicy, rerankDocumentsWithOptions(alloc, &client, cfg, options, "query", &.{"document"}));
    cfg.rate_limit = .{ .tokens_per_minute = 1 };
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, rerankDocumentsWithOptions(alloc, &client, cfg, options, "query", &.{"document"}));
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

    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer explicit-test-token", req.header("Authorization") orelse return error.TestUnexpectedResult);
        }
    };
    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/rerank", .assert_request = Check.request, .respond = .{
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
                .api_key = "explicit-test-token",
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
        // Embedded execution must not attempt to resolve an outbound secret.
        .api_key = "${secret:missing.embedded.reranker.key}",
    };
    const scores = try rerankDocumentsWithAntflyProvider(alloc, &client, cfg, local, "query", &.{ "doc1", "doc2" });
    defer alloc.free(scores);
    try std.testing.expect(state.called);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), scores[1], 0.0001);
}

test "reranking runtime authentication owns wire credentials and stable quota sources" {
    const alloc = std.testing.allocator;
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            inline for (.{ Provider.antfly, Provider.cohere, Provider.vertex }) |provider| {
                const cfg = Config{ .provider = provider, .api_key = "explicit", .field = "body" };
                var auth = try RequestAuthentication.init(a, cfg, null);
                defer auth.deinit(a);
                try std.testing.expectEqualStrings("explicit", auth.token.?);
                try std.testing.expect(auth.identity(cfg).eql(credential_identity.CredentialSourceIdentity.literalSecret("explicit")));
            }
            const adc_cfg = Config{ .provider = .vertex, .field = "body", .credentials_path = "test-adc.json" };
            var adc = try RequestAuthentication.init(a, adc_cfg, null);
            defer adc.deinit(a);
            try std.testing.expect(adc.token == null);
            try std.testing.expect(adc.identity(adc_cfg).eql(credential_identity.CredentialSourceIdentity.googleAdc("test-adc.json")));
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{});
    inline for (.{ Provider.antfly, Provider.cohere, Provider.vertex }) |provider| {
        try std.testing.expectError(error.InvalidRerankerConfig, RequestAuthentication.init(alloc, .{ .provider = provider, .api_key = "", .field = "body" }, null));
    }
}

test "reranking runtime remote Antfly secret rotation changes headers without resetting quota" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "secrets.json", .data = "{\"secrets\":[]}" });
    const path = try tmp.dir.realPathFileAlloc(io, "secrets.json", alloc);
    defer alloc.free(path);
    var store = try common_secrets.FileStore.init(alloc, path);
    defer store.deinit();
    var initial = try store.put(alloc, "reranker.key", "first");
    defer initial.deinit(alloc);
    var limits = provider_limits.Registry.init(alloc);
    defer limits.deinit();
    const Check = struct {
        fn first(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer first", req.header("Authorization") orelse return error.TestUnexpectedResult);
        }
        fn second(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer second", req.header("Authorization") orelse return error.TestUnexpectedResult);
        }
    };
    var routes = [_]httpx.testing_mod.Route{.{ .method = .POST, .path = "/rerank", .assert_request = Check.first, .respond = .{ .body = "{\"scores\":[0.9]}" } }};
    var server = try httpx.TestServer.start(alloc, io, &routes);
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .timeouts = .{ .request_ms = 500 } });
    defer client.deinit();
    const cfg = Config{ .provider = .antfly, .model = "test", .field = "body", .url = server.baseUrl(), .api_key = "${secret:reranker.key}", .rate_limit = .{ .requests_per_minute = 1, .burst = 2, .max_concurrency = 1 } };
    const options = Options{ .limits = &limits, .secret_store = &store };
    var auth = try RequestAuthentication.init(alloc, cfg, &store);
    defer auth.deinit(alloc);
    const policy = try provider_limits.Policy.fromConfig(cfg.rate_limit);
    var held = try acquireQuota(cfg, options, auth.identity(cfg), "", "", policy);
    defer held.release();
    const Run = struct {
        fn run(a: std.mem.Allocator, http: *httpx.Client, config: Config, opts: Options, err_out: *?anyerror) !void {
            const scores = rerankDocumentsWithOptions(a, http, config, opts, "query", &.{"document"}) catch |err| {
                err_out.* = err;
                return;
            };
            defer a.free(scores);
            std.testing.expectEqual(@as(usize, 1), scores.len) catch |err| {
                err_out.* = err;
            };
        }
    };
    for (0..2) |i| {
        if (i == 1) {
            var rotated = try store.put(alloc, "reranker.key", "second");
            defer rotated.deinit(alloc);
            routes[0].assert_request = Check.second;
        }
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Run.run, .{ alloc, &client, cfg, options, &failure });
        try server.handleOne();
        try group.await(io);
        if (failure) |err| return err;
    }
    try std.testing.expect(held.limiter().requests < 1);
    try std.testing.expectEqual(@as(u32, 0), held.limiter().in_flight);
    // No server handler: the third attempt must expire before dispatch.
    try std.testing.expectError(error.Timeout, rerankDocumentsWithOptions(alloc, &client, cfg, options, "query", &.{"document"}));
    var rotated_auth = try RequestAuthentication.init(alloc, cfg, &store);
    defer rotated_auth.deinit(alloc);
    try std.testing.expect(auth.identity(cfg).eql(rotated_auth.identity(cfg)));
    var conflicting = cfg;
    conflicting.rate_limit.?.requests_per_minute = 2;
    try std.testing.expectError(error.ConflictingRateLimitPolicy, rerankDocumentsWithOptions(alloc, &client, conflicting, options, "query", &.{"document"}));
    // A genuinely different credential source owns independent capacity.
    var literal_cfg = cfg;
    literal_cfg.api_key = "second";
    var literal_auth = try RequestAuthentication.init(alloc, literal_cfg, &store);
    defer literal_auth.deinit(alloc);
    var independent = try acquireQuota(literal_cfg, options, literal_auth.identity(literal_cfg), "", "", policy);
    defer independent.release();
    try std.testing.expect(independent.limiter() != held.limiter());
    try std.testing.expectEqual(@as(f64, 2), independent.limiter().requests);
}

test "reranking runtime remote Antfly defaults match anonymous or environment authentication" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            const a = std.testing.allocator;
            const token = common_secrets.envValueOwned(a, "ANTFLY_INFERENCE_API_KEY");
            defer if (token) |value| a.free(value);
            if (token) |value| {
                const expected = try std.fmt.allocPrint(a, "Bearer {s}", .{value});
                defer a.free(expected);
                try std.testing.expectEqualStrings(expected, req.header("Authorization") orelse return error.TestUnexpectedResult);
            } else try std.testing.expect(req.header("Authorization") == null);
        }
    };
    var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = .POST, .path = "/rerank", .assert_request = Check.request, .respond = .{ .body = "{\"scores\":[0.9]}" } }});
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var limits = provider_limits.Registry.init(alloc);
    defer limits.deinit();
    const options = Options{ .limits = &limits };
    const cfg = Config{ .provider = .antfly, .url = server.baseUrl(), .field = "body", .rate_limit = .{ .requests_per_minute = 1 } };
    var auth = try RequestAuthentication.init(alloc, cfg, null);
    defer auth.deinit(alloc);
    try std.testing.expectEqualStrings("ANTFLY_INFERENCE_API_KEY", auth.secret.?.env_var);
    const expected_source = if (auth.token != null)
        credential_identity.CredentialSourceIdentity.environmentVariable("ANTFLY_INFERENCE_API_KEY")
    else
        credential_identity.CredentialSourceIdentity.none();
    try std.testing.expect(auth.identity(cfg).eql(expected_source));
    var held = try acquireQuota(cfg, options, expected_source, "", "", try provider_limits.Policy.fromConfig(cfg.rate_limit));
    defer held.release();
    const Run = struct {
        fn run(a: std.mem.Allocator, http: *httpx.Client, config: Config, opts: Options, err_out: *?anyerror) !void {
            const scores = rerankDocumentsWithOptions(a, http, config, opts, "query", &.{"document"}) catch |err| {
                err_out.* = err;
                return;
            };
            a.free(scores);
        }
    };
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Run.run, .{ alloc, &client, cfg, options, &failure });
    try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expect(held.limiter().requests < 1);
    try std.testing.expectEqual(@as(u32, 0), held.limiter().in_flight);
}
