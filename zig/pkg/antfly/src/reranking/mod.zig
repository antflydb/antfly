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
const builtin = @import("builtin");
const platform_time = @import("antfly_platform").time;
const httpx = @import("httpx");
const lib = @import("antfly_reranking");
const managed_embedder = @import("../inference/managed_embedder.zig");
const inference_request_context = @import("../inference/execution_context.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const antfly_provider = @import("../inference/local.zig");
const remote_capabilities = @import("../inference/remote_capabilities.zig");
const execution_context = @import("../inference/execution_context.zig");
const runtime_error_abi = @import("../runtime_error_abi.zig");
const runtime_native_abi = @import("../runtime_native_abi.zig");
const vertex_provider = @import("../inference/vertex.zig");
const common_secrets = @import("../common/secrets.zig");
const request_admission = @import("../common/request_admission.zig");
const common_cancellation = @import("../common/cancellation.zig");
const provider_limits = @import("../common/provider_limits.zig");
const credential_identity = @import("../common/credential_source_identity.zig");
const google_auth = @import("antfly_google").auth;
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test)
    @import("../storage/db/template_stub.zig")
else
    @import("../template.zig");

pub const ContentPart = template_mod.ContentPart;

/// Rendered rerank candidates. `texts[i]` is document i as text and is what
/// text-only providers score. `parts`, parallel to `texts`, holds each
/// document's ordered content parts; callers set it only when a rendered
/// template produced media.
pub const Documents = struct {
    texts: []const []const u8,
    parts: ?[]const []const ContentPart = null,

    pub fn hasMedia(self: Documents) bool {
        return antfly_provider.rerankDocumentsHaveMedia(self.parts);
    }

    fn mediaShape(self: Documents) struct { encoded_bytes: usize, max_parts_per_document: usize } {
        var encoded_bytes: usize = 0;
        var max_parts: usize = 0;
        const documents = self.parts orelse return .{ .encoded_bytes = 0, .max_parts_per_document = 0 };
        for (documents) |document| {
            var count: usize = 0;
            for (document) |part| switch (part) {
                .text => {},
                .media_url => count += 1,
                .binary => |binary| {
                    count += 1;
                    encoded_bytes +|= binary.data.len;
                },
            };
            max_parts = @max(max_parts, count);
        }
        return .{ .encoded_bytes = encoded_bytes, .max_parts_per_document = max_parts };
    }
};

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
            // Runtime.rerank admits concurrent requests. httpx uses its client
            // allocator for request-local URLs, headers, and bodies as well as
            // pool state, so an arena-backed cache owner is not a safe client
            // allocator even though the connection pool synchronizes itself.
            .http = httpx.Client.initWithConfig(std.heap.smp_allocator, io, .{
                .keep_alive = true,
                .cookies_enabled = false,
            }),
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
        request_ctx: ?inference_request_context.RequestContext,
    ) !AdmissionLease {
        if (request_ctx) |context| try context.check();
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

    /// Like `rerankAdmitted`, for documents that may carry media parts.
    pub fn rerankContentAdmitted(
        self: *Runtime,
        alloc: std.mem.Allocator,
        cfg: Config,
        dependencies: Options,
        query: []const u8,
        documents: Documents,
    ) ![]f32 {
        if (dependencies.execution_context) |context| try context.check();
        var options = dependencies;
        options.runtime = self;
        return try rerankContentWithOptions(alloc, &self.http, cfg, options, query, documents);
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
        error.RerankerMediaUnsupported,
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
        error.ResourceTemporarilyUnavailable,
        error.ConcurrencyUnavailable,
        => error.RerankTransientFailure,
        else => err,
    };
}

pub fn statusError(status: u16) anyerror {
    return @import("../inference/types.zig").rerankStatusError(status);
}

test "reranking runtime failures use stable query dependency classes" {
    try std.testing.expectEqual(error.RerankRateLimited, normalizeOperationalError(error.ProviderQuotaRegistryFull));
    try std.testing.expectEqual(error.RerankRateLimited, statusError(429));
    try std.testing.expectEqual(error.RerankTransientFailure, statusError(503));
    try std.testing.expectEqual(error.Timeout, statusError(504));
    try std.testing.expectEqual(error.RerankRequestFailed, statusError(401));
    try std.testing.expectEqual(error.RerankUpstreamFailure, normalizeOperationalError(error.InvalidRerankerResponse));
    try std.testing.expectEqual(error.RerankTransientFailure, normalizeOperationalError(error.ConnectionRefused));
    try std.testing.expectEqual(error.RerankTransientFailure, normalizeOperationalError(error.ResourceTemporarilyUnavailable));
    try std.testing.expectEqual(error.RerankTransientFailure, normalizeOperationalError(error.ConcurrencyUnavailable));
    // Embedded execution crosses the stable ABI before query classification.
    try std.testing.expectEqual(error.RerankTransientFailure, normalizeOperationalError(
        runtime_error_abi.errorFromStatus(runtime_error_abi.statusFromError(error.ResourceTemporarilyUnavailable)),
    ));
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
    try std.testing.expect(runtime.http.allocator.vtable == std.heap.smp_allocator.vtable);
    try std.testing.expect(!runtime.http.config.cookies_enabled);
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
    capability_cache: ?*remote_capabilities.Cache = null,
    execution: execution_context.Context = .{},
    runtime: ?*Runtime = null,
    limits: *provider_limits.Registry = &provider_limits.process_registry,
    execution_context: ?inference_request_context.RequestContext = null,
};

const remote_rerank_max_response_bytes: usize = 4 << 20;
const remote_rerank_max_timeout_ms: u64 = 300_000;

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
    return rerankContentWithOptions(alloc, http, cfg, options, query, .{ .texts = documents });
}

/// Reranks documents that may carry media. Only the Antfly provider scores
/// images, and only through a model that accepts them; everything else fails
/// with `error.RerankerMediaUnsupported` before any provider call.
pub fn rerankContentWithOptions(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    options: Options,
    query: []const u8,
    content: Documents,
) ![]f32 {
    const documents = content.texts;
    if (content.parts) |parts| if (parts.len != documents.len) return error.InvalidArguments;
    const has_media = content.hasMedia();
    const media_shape = content.mediaShape();
    try cfg.validate();
    if (has_media and cfg.provider != .antfly) return error.RerankerMediaUnsupported;
    const request_context = options.execution_context orelse inference_request_context.RequestContext{
        .io = options.execution.io orelse http.io,
        .deadline_ns = options.execution.deadline_ns,
        .cancellation = if (options.execution.cancellation.ptr != null and options.execution.cancellation.is_cancelled_fn != null)
            options.execution.cancellation
        else
            null,
    };
    try request_context.check();
    const policy = try provider_limits.Policy.fromConfig(cfg.rate_limit);

    switch (cfg.provider) {
        .antfly => {
            const explicit_endpoint = if (std.mem.trim(u8, cfg.url, " \t\r\n").len > 0) cfg.url else null;
            const linked_reranker = if (options.antfly_provider) |local| local.rerank_texts else null;
            const linked_context_reranker = if (options.antfly_provider) |local| local.rerank_texts_with_context else null;
            const resolved_endpoint = options.execution.resolveAntflyEndpoint(
                explicit_endpoint,
                linked_context_reranker != null or linked_reranker != null,
            );
            if (resolved_endpoint == null and (linked_context_reranker != null or linked_reranker != null)) {
                if (policy.enabled()) return error.UnsupportedLocalRateLimit;
                const local = options.antfly_provider.?;
                try request_context.check();
                if (has_media) {
                    const rerank_parts = local.rerank_documents_with_context orelse return error.RerankerMediaUnsupported;
                    if (local.model_capabilities) |resolve| {
                        const capabilities = try managed_embedder.AntflyProviderBoundary.call(
                            "model_capabilities",
                            local.boundary_dispatch,
                            resolve,
                            .{ local.ptr, alloc, cfg.model, .rerank },
                        );
                        if (!capabilities.input_modalities.image) return error.RerankerMediaUnsupported;
                    }
                    const scores = try managed_embedder.AntflyProviderBoundary.call(
                        "rerank_documents_with_context",
                        local.boundary_dispatch,
                        rerank_parts,
                        .{ local.ptr, alloc, cfg.model, query, content.parts.?, request_context },
                    );
                    errdefer alloc.free(scores);
                    try validateScores(scores, documents.len);
                    try request_context.check();
                    return scores;
                }
                const scores = if (linked_context_reranker) |rerank|
                    try managed_embedder.AntflyProviderBoundary.call(
                        "rerank_texts_with_context",
                        local.boundary_dispatch,
                        rerank,
                        .{ local.ptr, alloc, cfg.model, query, documents, request_context },
                    )
                else
                    try managed_embedder.AntflyProviderBoundary.call(
                        "rerank_texts",
                        local.boundary_dispatch,
                        linked_reranker.?,
                        .{ local.ptr, alloc, cfg.model, query, documents },
                    );
                errdefer alloc.free(scores);
                try validateScores(scores, documents.len);
                try request_context.check();
                return scores;
            }
            const endpoint = resolved_endpoint orelse cfg.defaultedUrl();
            var auth = try RequestAuthentication.init(alloc, cfg, options.secret_store);
            defer auth.deinit(alloc);
            const api_key = auth.token;
            var quota = try acquireQuotaForEndpoint(cfg, options, endpoint, auth.identity(cfg), "", "", policy);
            defer quota.release();
            var provider = antfly_provider.Provider.init(alloc, http, endpoint);
            defer provider.deinit();
            provider.attempt_observer = quota.limiter().observer(0);
            try provider.setSourceTable(options.execution.routing.source_table);
            if (request_context.cancellation) |cancellation|
                provider.setRequestCancellation(cancellation);
            provider.setMaxResponseBytes(options.execution.boundedResponseBytes(
                remote_rerank_max_response_bytes,
            ));
            var authorization_header: ?[]u8 = null;
            defer if (authorization_header) |value| alloc.free(value);
            var header_storage: [2][2][]const u8 = undefined;
            var header_count: usize = 0;
            if (api_key) |value| {
                authorization_header = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
                header_storage[header_count] = .{ "Authorization", authorization_header.? };
                header_count += 1;
            }
            header_count = try options.execution.routing.appendHeaders(&header_storage, header_count);
            const headers = header_storage[0..header_count];
            if (authorization_header) |value| try provider.setAuthorizationHeader(value);
            var fallback_cache: ?remote_capabilities.Cache = null;
            defer if (fallback_cache) |*cache| cache.deinit();
            var capability_cache = options.execution.capability_cache orelse options.capability_cache;
            if (capability_cache == null) {
                if (options.antfly_provider) |local| capability_cache = local.remote_capability_cache;
            }
            if (capability_cache == null) {
                fallback_cache = remote_capabilities.Cache.init(alloc, http.io);
                capability_cache = &fallback_cache.?;
            }
            if (cfg.model.len > 0) {
                const capability_lease = try capability_cache.?.getOrDiscoverLeaseWithContext(
                    http,
                    endpoint,
                    cfg.model,
                    .rerank,
                    headers,
                    .{
                        .deadline_ns = request_context.deadline_ns,
                        .cancellation = request_context.cancellation orelse .none,
                    },
                );
                if (capability_lease.capabilities) |discovered| {
                    discovered.validateInvocation(.rerank, .{
                        .item_count = 1,
                        .modalities = .{ .text = true, .image = has_media },
                        .text_bytes = query.len +| documentBytes(documents),
                        .max_text_bytes_per_item = query.len +| maximumDocumentBytes(documents),
                        .max_candidates_per_request = documents.len,
                        .encoded_media_bytes = media_shape.encoded_bytes,
                        .max_media_parts_per_item = media_shape.max_parts_per_document,
                    }) catch |err| return if (has_media and err == error.UnsupportedInferenceModality)
                        error.RerankerMediaUnsupported
                    else
                        err;
                    provider.setRerankDocuments(discovered.rerank_documents_v1);
                    provider.setFramedAttachments(discovered.framed_attachments);
                }
                if (capability_lease.routing_token) |token|
                    try provider.setCapabilityToken(token.slice());
                if (capability_lease.descriptor_revision) |revision|
                    try provider.setCapabilityRevision(revision.slice());
            }
            // Discovery and execution share one absolute deadline. Recompute
            // the transport duration only after discovery so catalog latency
            // cannot extend the owning query.
            const remaining_timeout_ms = try request_context.remainingTimeoutMs();
            provider.setRequestTimeoutMs(if (remaining_timeout_ms) |timeout_ms|
                @min(timeout_ms, remote_rerank_max_timeout_ms)
            else
                null);
            var result = provider.rerankDocuments(alloc, cfg.model, query, documents, content.parts) catch |err| {
                if (err == error.InferenceCapabilitiesStale and cfg.model.len > 0)
                    try capability_cache.?.invalidate(endpoint, cfg.model, .rerank, headers);
                return err;
            };
            defer result.deinit();
            try validateScores(result.scores, documents.len);
            try request_context.check();
            return try alloc.dupe(f32, result.scores);
        },
        .cohere => {
            var auth = try RequestAuthentication.init(alloc, cfg, options.secret_store);
            defer auth.deinit(alloc);
            const token = auth.token orelse return error.InvalidRerankerConfig;
            var quota = try acquireQuota(cfg, options, auth.identity(cfg), "", "", policy);
            defer quota.release();
            const scores = try rerankCohere(alloc, http, cfg, token, request_context, quota.limiter().observer(0), query, documents);
            errdefer alloc.free(scores);
            try validateScores(scores, documents.len);
            try request_context.check();
            return scores;
        },
        .vertex => {
            var auth = try RequestAuthentication.init(alloc, cfg, options.secret_store);
            defer auth.deinit(alloc);
            const api_key = auth.token;
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
            var quota = try acquireQuota(cfg, options, auth.identity(cfg), provider.project_id, "global", policy);
            defer quota.release();
            provider.attempt_observer = quota.limiter().observer(0);
            const scores = try provider.rerank(alloc, cfg.model, query, documents, .{
                .timeout_ms = try request_context.remainingTimeoutMs(),
                .cancellation = httpCancellation(request_context.cancellation),
            });
            errdefer alloc.free(scores);
            try validateScores(scores, documents.len);
            try request_context.check();
            return scores;
        },
    }
}

fn documentBytes(documents: []const []const u8) usize {
    var total: usize = 0;
    for (documents) |document| total +|= document.len;
    return total;
}

fn maximumDocumentBytes(documents: []const []const u8) usize {
    var maximum: usize = 0;
    for (documents) |document| maximum = @max(maximum, document.len);
    return maximum;
}

fn expectRerankerAuthorization(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqualStrings("Bearer test-token", req.header("Authorization") orelse "");
    try std.testing.expectEqualStrings("docs", req.header("X-Antfly-Source-Table") orelse "");
}

fn validateScores(scores: []const f32, document_count: usize) !void {
    if (scores.len != document_count) return error.InvalidRerankerResponse;
    for (scores) |score| {
        if (!std.math.isFinite(score)) return error.InvalidRerankerResponse;
    }
}

fn acquireQuota(cfg: Config, options: Options, source: credential_identity.CredentialSourceIdentity, project: []const u8, location: []const u8, policy: provider_limits.Policy) !provider_limits.Handle {
    return acquireQuotaForEndpoint(cfg, options, cfg.defaultedUrl(), source, project, location, policy);
}

fn acquireQuotaForEndpoint(cfg: Config, options: Options, endpoint: []const u8, source: credential_identity.CredentialSourceIdentity, project: []const u8, location: []const u8, policy: provider_limits.Policy) !provider_limits.Handle {
    const registry = if (options.runtime) |runtime| runtime.limits else options.limits;
    return registry.acquire(.{ .operation = .reranking, .endpoint = .{
        .provider = std.meta.stringToEnum(provider_limits.Provider, @tagName(cfg.provider)).?,
        .endpoint = endpoint,
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
    request_ctx: ?inference_request_context.RequestContext,
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
        .timeout_ms = if (request_ctx) |context| try context.remainingTimeoutMs() else null,
        .cancellation = httpCancellation(if (request_ctx) |context| context.cancellation else null),
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

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/ai/v1/models", .assert_request = expectRerankerAuthorization, .respond = .{
            .body = "{\"rerankers\":{\"cross-encoder/ms-marco-MiniLM-L-6-v2\":{}}}",
        } },
        .{ .method = .POST, .path = "/rerank", .assert_request = expectRerankerAuthorization, .respond = .{
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
                .api_key = "test-token",
                .field = "body",
            };
            out.* = rerankDocumentsWithOptions(a, test_client, cfg, .{
                .execution = .{ .routing = .{ .source_table = "docs" } },
            }, "query", &.{ "doc1", "doc2" }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, &client, url, &scores, &run_err }) catch return;
    try ts.handleOne();
    try ts.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqual(@as(usize, 2), scores.?.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), scores.?[0], 0.0001);
}

test "reranking runtime preserves antfly HTTP failure classes" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Case = struct { status: u16, body: []const u8 = "{}", expected: anyerror };
    const cases = [_]Case{
        .{ .status = 503, .body = "{\"error\":\"MODEL_RESOURCE_BUSY\",\"reason\":\"inference_capacity\",\"retryable\":true,\"retry_after_ms\":1000}", .expected = error.RerankTransientFailure },
        .{ .status = 429, .expected = error.RerankRateLimited },
        .{ .status = 500, .expected = error.RerankTransientFailure },
        .{ .status = 502, .expected = error.RerankTransientFailure },
        .{ .status = 408, .expected = error.Timeout },
        .{ .status = 504, .expected = error.Timeout },
        .{ .status = 401, .expected = error.RerankUpstreamFailure },
        .{ .status = 422, .body = "{\"error\":\"MODEL_RESOURCE_LIMIT\",\"retryable\":false}", .expected = error.RerankUpstreamFailure },
    };
    for (cases) |tc| {
        var server = try httpx.TestServer.start(alloc, io, &.{
            .{ .method = .POST, .path = "/rerank", .respond = .{ .status = tc.status, .body = tc.body } },
        });
        defer server.deinit();
        var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        defer client.deinit();
        var run_err: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        const Fiber = struct {
            fn run(a: std.mem.Allocator, http: *httpx.Client, url: []const u8, err_out: *?anyerror) std.Io.Cancelable!void {
                // Use the endpoint's default model to exercise execution without discovery.
                const scores = rerankDocumentsWithOptions(a, http, .{
                    .provider = .antfly,
                    .url = url,
                    .field = "body",
                }, .{}, "query", &.{"document"}) catch |err| {
                    err_out.* = normalizeOperationalError(err);
                    return;
                };
                a.free(scores);
            }
        };
        try group.concurrent(io, Fiber.run, .{ alloc, &client, server.baseUrl(), &run_err });
        try server.handleOne();
        try group.await(io);
        try std.testing.expectEqual(@as(?anyerror, tc.expected), run_err);
    }
}

test "reranking runtime cancels active antfly HTTP work and releases admission" {
    if (@import("builtin").os.tag == .windows or @import("builtin").os.tag == .freestanding) return;
    const State = struct {
        var started = std.atomic.Value(bool).init(false);
        var canceled = std.atomic.Value(bool).init(false);
        var release = std.atomic.Value(bool).init(false);
        fn handler(ctx: *httpx.Context) anyerror!httpx.Response {
            if (started.swap(true, .acq_rel)) return ctx.json(.{ .scores = [_]f32{0.5} });
            while (!release.load(.acquire)) {
                if (ctx.isCancellationRequested()) {
                    canceled.store(true, .release);
                    return error.Canceled;
                }
                try ctx.io.sleep(.fromMilliseconds(1), .awake);
            }
            return ctx.json(.{ .scores = [_]f32{0.5} });
        }
    };
    State.started.store(false, .release);
    State.canceled.store(false, .release);
    State.release.store(false, .release);
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var server = httpx.Server.initWithConfig(alloc, io, .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.post("/rerank", State.handler);
    try server.bind();
    var listener = try std.testing.io.concurrent(struct {
        fn run(s: *httpx.Server) void {
            s.listen() catch {};
        }
    }.run, .{&server});
    defer {
        State.release.store(true, .release);
        server.stop();
        listener.await(std.testing.io);
    }
    while (!server.listen_started.load(.acquire)) try io.sleep(.fromMilliseconds(1), .awake);
    const url = try std.fmt.allocPrint(alloc, "http://127.0.0.1:{d}", .{server.boundAddress().?.getPort()});
    defer alloc.free(url);
    var runtime = Runtime.init(alloc, io);
    defer runtime.deinit();
    runtime.admission = .init(1);
    var cancellation = std.atomic.Value(bool).init(false);
    const cfg = Config{ .provider = .antfly, .url = url, .field = "body" };
    var request = try io.concurrent(struct {
        fn run(a: std.mem.Allocator, r: *Runtime, config: Config, signal: *std.atomic.Value(bool)) anyerror!void {
            const scores = try r.rerank(a, config, .{ .execution_context = .{
                .io = r.io,
                .deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s,
                .cancellation = common_cancellation.CancellationToken.fromAtomic(signal),
            } }, "query", &.{"document"});
            a.free(scores);
        }
    }.run, .{ alloc, &runtime, cfg, &cancellation });
    defer request.cancel(io) catch {};
    for (0..5000) |_| {
        if (State.started.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(State.started.load(.acquire));
    cancellation.store(true, .release);
    try std.testing.expectError(error.Cancelled, request.await(io));
    for (0..2000) |_| {
        if (State.canceled.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(State.canceled.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), runtime.admission.stats().in_flight);
    const scores = try runtime.rerank(alloc, cfg, .{}, "next query", &.{"document"});
    defer alloc.free(scores);
    try std.testing.expectEqual(@as(f32, 0.5), scores[0]);
}

test "reranking runtime routes antfly provider to local antfly" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    const State = struct {
        legacy_called: bool = false,
        context_calls: usize = 0,
        failure: ?anyerror = null,

        fn dense(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![][]f32 {
            return try a.alloc([]f32, 0);
        }

        fn sparse(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![]db_embedder.SparseEmbedding {
            return try a.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn rerank(ptr: *anyopaque, a: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) anyerror![]f32 {
            const state: *@This() = @ptrCast(@alignCast(ptr));
            state.legacy_called = true;
            _ = a;
            _ = model;
            _ = query;
            _ = documents;
            return error.TestUnexpectedResult;
        }

        fn rerankWithContext(ptr: *anyopaque, a: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8, context: inference_request_context.RequestContext) anyerror![]f32 {
            const state: *@This() = @ptrCast(@alignCast(ptr));
            try context.check();
            try std.testing.expect(context.deadline_ns != null);
            state.context_calls += 1;
            if (state.failure) |err| return err;
            try std.testing.expectEqualStrings("local-reranker", model);
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
        .rerank_texts_with_context = State.rerankWithContext,
    };
    const cfg = Config{
        .provider = .antfly,
        .model = "local-reranker",
        .field = "body",
        // Embedded execution must not attempt to resolve an outbound secret.
        .api_key = "${secret:missing.embedded.reranker.key}",
    };
    const scores = try rerankDocumentsWithOptions(alloc, &client, cfg, .{
        .antfly_provider = local,
        .execution = .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
    }, "query", &.{ "doc1", "doc2" });
    defer alloc.free(scores);
    try std.testing.expectEqual(@as(usize, 1), state.context_calls);
    try std.testing.expect(!state.legacy_called);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), scores[1], 0.0001);

    const RejectingBoundary = struct {
        fn dispatch(
            _: *const runtime_native_abi.CallContract,
            _: *const anyopaque,
            _: *const anyopaque,
            _: ?*anyopaque,
        ) callconv(.c) runtime_error_abi.Status {
            return runtime_error_abi.statusFromError(error.UnsupportedVersion);
        }
    };
    var rejected = local;
    rejected.boundary_dispatch = RejectingBoundary.dispatch;
    try std.testing.expectError(error.UnsupportedVersion, rerankDocumentsWithOptions(
        alloc,
        &client,
        cfg,
        .{
            .antfly_provider = rejected,
            .execution = .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
        },
        "query",
        &.{ "doc1", "doc2" },
    ));
    try std.testing.expectEqual(@as(usize, 1), state.context_calls);

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, rerankDocumentsWithOptions(alloc, &client, cfg, .{
        .antfly_provider = local,
        .execution = .{ .cancellation = .fromAtomic(&canceled) },
    }, "query", &.{ "doc1", "doc2" }));
    try std.testing.expectEqual(@as(usize, 1), state.context_calls);

    for ([_]anyerror{ error.ResourceTemporarilyUnavailable, error.ConcurrencyUnavailable }) |err| {
        state.failure = err;
        try std.testing.expectError(err, rerankDocumentsWithOptions(alloc, &client, cfg, .{
            .antfly_provider = local,
            .execution = .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
        }, "query", &.{ "doc1", "doc2" }));
    }
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
    var routes = [_]httpx.testing_mod.Route{
        .{ .method = .GET, .path = "/ai/v1/models", .respond = .{ .body = "{\"rerankers\":{\"test\":{}}}" } },
        .{ .method = .POST, .path = "/rerank", .assert_request = Check.first, .respond = .{ .body = "{\"scores\":[0.9]}" } },
    };
    var server = try httpx.TestServer.start(alloc, io, &routes);
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .timeouts = .{ .request_ms = 500 } });
    defer client.deinit();
    const cfg = Config{ .provider = .antfly, .model = "test", .field = "body", .url = server.baseUrl(), .api_key = "${secret:reranker.key}", .rate_limit = .{ .requests_per_minute = 1, .burst = 2, .max_concurrency = 1 } };
    var capabilities = remote_capabilities.Cache.init(alloc, io);
    defer capabilities.deinit();
    const options = Options{ .limits = &limits, .secret_store = &store, .capability_cache = &capabilities };
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
            routes[1].assert_request = Check.second;
        }
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Run.run, .{ alloc, &client, cfg, options, &failure });
        // Capability cache authorization includes the resolved bearer value,
        // so credential rotation intentionally revalidates the remote catalog.
        try server.handleOne();
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

fn testRerankCatalog(comptime documents_flag: []const u8) []const u8 {
    return "{\"rerankers\":{\"owner/colqwen\":{\"inputs\":[\"text\",\"image\"],\"inference_capabilities\":{\"version\":4,\"task\":\"rerank\",\"input_modalities\":[\"text\",\"image\"],\"accepted_mime_types\":[\"text/plain\",\"image/png\"],\"input_granularity\":\"item\",\"output\":\"ranked_items\",\"result_cardinality\":\"one_per_request\",\"prompt_policy\":\"explicit\",\"borrowed_attachments\":false,\"framed_attachments\":false,\"numeric_responses_v1\":false" ++ documents_flag ++ ",\"task_limits\":{\"max_text_bytes_per_item\":null,\"max_input_tokens_per_item\":null,\"max_output_tokens_per_item\":null,\"max_candidates_per_request\":null,\"max_schema_bytes\":null},\"batch\":{\"mode\":\"native\",\"preferred_items\":8,\"max_items\":64,\"max_encoded_media_bytes\":1048576,\"max_decoded_pixels\":16777216,\"max_media_parts_per_item\":4,\"per_item_failures\":false}}}}}";
}

const test_rerank_scores_body = "{\"object\":\"list\",\"data\":[{\"object\":\"rerank.score\",\"index\":0,\"score\":0.7},{\"object\":\"rerank.score\",\"index\":1,\"score\":0.1}],\"model\":\"owner/colqwen\",\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":0,\"total_tokens\":2}}";

fn runRemoteContentRerank(
    catalog: []const u8,
    check: *const fn (httpx.testing_mod.RequestInfo) anyerror!void,
    content: Documents,
    expect_rerank_request: bool,
) !?[]f32 {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/ai/v1/models", .respond = .{ .body = catalog } },
        .{ .method = .POST, .path = "/rerank", .assert_request = check, .respond = .{ .body = test_rerank_scores_body } },
    });
    defer ts.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var scores: ?[]f32 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_client: *httpx.Client, url: []const u8, docs: Documents, out: *?[]f32, err_out: *?anyerror) std.Io.Cancelable!void {
            const cfg = Config{ .provider = .antfly, .model = "owner/colqwen", .url = url, .field = "body" };
            out.* = rerankContentWithOptions(a, test_client, cfg, .{}, "invoice total", docs) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    group.concurrent(io, Fiber.run, .{ alloc, &client, ts.baseUrl(), content, &scores, &run_err }) catch return error.SkipZigTest;
    try ts.handleOne();
    if (expect_rerank_request) try ts.handleOne();
    group.await(io) catch {};
    if (run_err) |err| {
        if (scores) |value| alloc.free(value);
        return err;
    }
    return scores;
}

test "reranking runtime sends image documents to servers that accept documents" {
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"prompts\"") == null);
            // The image document is an array of parts with inline base64 media;
            // the text-only document stays a plain string.
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"documents\":[[{\"type\":\"text\",\"text\":\"page one\"},{\"type\":\"media\",\"mime_type\":\"image/png\",\"data\":\"iVBORw==\"}],\"plain page\"]") != null);
        }
    };
    const png = [_]u8{ 0x89, 'P', 'N', 'G' };
    const with_image = [_]ContentPart{ .{ .text = "page one" }, .{ .binary = .{ .mime_type = "image/png", .data = &png } } };
    const text_only = [_]ContentPart{.{ .text = "plain page" }};
    const parts = [_][]const ContentPart{ &with_image, &text_only };
    const scores = (try runRemoteContentRerank(testRerankCatalog(",\"rerank_documents_v1\":true"), Check.request, .{
        .texts = &.{ "page one", "plain page" },
        .parts = &parts,
    }, true)).?;
    defer std.testing.allocator.free(scores);
    try std.testing.expectApproxEqAbs(@as(f32, 0.7), scores[0], 0.0001);
}

test "reranking runtime keeps prompts for servers without rerank documents" {
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"prompts\":[\"page one\",\"plain page\"]") != null);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"documents\"") == null);
        }
    };
    const scores = (try runRemoteContentRerank(testRerankCatalog(""), Check.request, .{
        .texts = &.{ "page one", "plain page" },
    }, true)).?;
    defer std.testing.allocator.free(scores);

    // The same server cannot receive images, so they fail before `/rerank`.
    const png = [_]u8{ 0x89, 'P', 'N', 'G' };
    const with_image = [_]ContentPart{.{ .binary = .{ .mime_type = "image/png", .data = &png } }};
    const text_only = [_]ContentPart{.{ .text = "plain page" }};
    const parts = [_][]const ContentPart{ &with_image, &text_only };
    try std.testing.expectError(error.RerankerMediaUnsupported, runRemoteContentRerank(testRerankCatalog(""), Check.request, .{
        .texts = &.{ "", "plain page" },
        .parts = &parts,
    }, false));
}

test "reranking runtime rejects image documents for text-only providers before dispatch" {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(std.testing.allocator, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();
    const with_image = [_]ContentPart{.{ .media_url = "https://example.invalid/page.png" }};
    const parts = [_][]const ContentPart{&with_image};
    for ([_]Provider{ .cohere, .vertex }) |provider| {
        const cfg = Config{ .provider = provider, .model = "rerank", .api_key = "key", .field = "body", .url = "http://127.0.0.1:1" };
        try std.testing.expectError(error.RerankerMediaUnsupported, rerankContentWithOptions(
            std.testing.allocator,
            &client,
            cfg,
            .{},
            "query",
            .{ .texts = &.{""}, .parts = &parts },
        ));
    }
}

test "reranking runtime sends image documents to linked rerankers that accept images" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    const State = struct {
        accepts_images: bool,
        document_calls: usize = 0,

        fn dense(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![][]f32 {
            return try a.alloc([]f32, 0);
        }
        fn sparse(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![]db_embedder.SparseEmbedding {
            return try a.alloc(db_embedder.SparseEmbedding, 0);
        }
        fn rerankTexts(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const []const u8, _: inference_request_context.RequestContext) anyerror![]f32 {
            return error.TestUnexpectedResult;
        }
        fn rerankDocuments(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const u8, documents: []const []const ContentPart, _: inference_request_context.RequestContext) anyerror![]f32 {
            const state: *@This() = @ptrCast(@alignCast(ptr));
            state.document_calls += 1;
            try std.testing.expectEqual(@as(usize, 2), documents.len);
            try std.testing.expect(documents[0][1] == .binary);
            try std.testing.expectEqualStrings("plain page", documents[1][0].text);
            const scores = try a.alloc(f32, 2);
            scores[0] = 0.9;
            scores[1] = 0.3;
            return scores;
        }
        fn capabilities(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, task: @import("../inference/work.zig").Task) anyerror!@import("../inference/work.zig").InferenceCapabilities {
            const state: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .task = task, .input_modalities = .{ .text = true, .image = state.accepts_images }, .input_granularity = .item, .output = .ranked_items, .result_cardinality = .one_per_request };
        }
    };
    const png = [_]u8{ 0x89, 'P', 'N', 'G' };
    const with_image = [_]ContentPart{ .{ .text = "page one" }, .{ .binary = .{ .mime_type = "image/png", .data = &png } } };
    const text_only = [_]ContentPart{.{ .text = "plain page" }};
    const parts = [_][]const ContentPart{ &with_image, &text_only };
    const content = Documents{ .texts = &.{ "page one", "plain page" }, .parts = &parts };
    const cfg = Config{ .provider = .antfly, .model = "local-colqwen", .field = "body" };

    var state = State{ .accepts_images = true };
    var local = managed_embedder.AntflyProvider{
        .ptr = &state,
        .embed_dense_texts = State.dense,
        .embed_sparse_texts = State.sparse,
        .rerank_texts_with_context = State.rerankTexts,
        .rerank_documents_with_context = State.rerankDocuments,
        .model_capabilities = State.capabilities,
    };
    const scores = try rerankContentWithOptions(alloc, &client, cfg, .{ .antfly_provider = local }, "invoice total", content);
    defer alloc.free(scores);
    try std.testing.expectEqual(@as(usize, 1), state.document_calls);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), scores[0], 0.0001);

    state.accepts_images = false;
    try std.testing.expectError(error.RerankerMediaUnsupported, rerankContentWithOptions(alloc, &client, cfg, .{ .antfly_provider = local }, "invoice total", content));
    try std.testing.expectEqual(@as(usize, 1), state.document_calls);

    local.rerank_documents_with_context = null;
    state.accepts_images = true;
    try std.testing.expectError(error.RerankerMediaUnsupported, rerankContentWithOptions(alloc, &client, cfg, .{ .antfly_provider = local }, "invoice total", content));
}
