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
const platform_time = @import("antfly_platform").time;
const httpx = @import("httpx");
const lib = @import("antfly_reranking");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const antfly_provider = @import("../inference/local.zig");
const remote_capabilities = @import("../inference/remote_capabilities.zig");
const common_secrets = @import("../common/secrets.zig");
const execution_context = @import("../inference/execution_context.zig");

pub const Config = lib.Config;
pub const Provider = lib.Provider;

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
};

const remote_rerank_max_response_bytes: usize = 4 << 20;
const remote_rerank_max_timeout_ms: u64 = 300_000;

pub fn rerankDocumentsWithOptions(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    options: Options,
    query: []const u8,
    documents: []const []const u8,
) ![]f32 {
    try cfg.validate();
    const api_key = if (try common_secrets.SecretValue.initConfig(alloc, cfg.api_key)) |secret_value| blk: {
        var owned_secret = secret_value;
        defer owned_secret.deinit(alloc);
        break :blk try owned_secret.resolveOwned(alloc, options.secret_store);
    } else null;
    defer if (api_key) |value| alloc.free(value);

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
                const local = options.antfly_provider.?;
                const io = options.execution.io orelse http.io;
                const request_context = options.execution.requestContext(io);
                try request_context.check();
                const scores = if (linked_context_reranker) |rerank|
                    try rerank(local.ptr, alloc, cfg.model, query, documents, request_context)
                else
                    try linked_reranker.?(local.ptr, alloc, cfg.model, query, documents);
                errdefer alloc.free(scores);
                try request_context.check();
                return scores;
            }
            const endpoint = resolved_endpoint orelse cfg.defaultedUrl();
            var provider = antfly_provider.Provider.init(alloc, http, endpoint);
            defer provider.deinit();
            try provider.setSourceTable(options.execution.routing.source_table);
            provider.setRequestCancellation(options.execution.cancellation);
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
            const capability_lease = try capability_cache.?.getOrDiscoverLeaseWithContext(
                http,
                endpoint,
                cfg.model,
                .rerank,
                headers,
                options.execution.waitContext(),
            );
            if (capability_lease.capabilities) |capabilities| {
                try capabilities.validateInvocation(.rerank, .{
                    .item_count = 1,
                    .modalities = .{ .text = true },
                    .text_bytes = query.len +| documentBytes(documents),
                    .max_text_bytes_per_item = query.len +| maximumDocumentBytes(documents),
                    .max_candidates_per_request = documents.len,
                });
            }
            if (capability_lease.routing_token) |token|
                try provider.setCapabilityToken(token.slice());
            if (capability_lease.descriptor_revision) |revision|
                try provider.setCapabilityRevision(revision.slice());
            // Discovery and execution share one absolute deadline. Recompute
            // the transport duration only after discovery so catalog latency
            // cannot extend the owning query.
            provider.setRequestTimeoutMs(try options.execution.remainingTimeoutMs(
                platform_time.monotonicNs(),
                remote_rerank_max_timeout_ms,
            ));
            var result = provider.reranker().rerank(alloc, cfg.model, query, documents) catch |err| {
                if (err == error.InferenceCapabilitiesStale)
                    try capability_cache.?.invalidate(endpoint, cfg.model, .rerank, headers);
                return err;
            };
            defer result.deinit();
            return try alloc.dupe(f32, result.scores);
        },
        else => return error.UnsupportedRerankerProvider,
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

test "reranking runtime routes antfly provider to local antfly" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    const State = struct {
        legacy_called: bool = false,
        context_calls: usize = 0,

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

        fn rerankWithContext(ptr: *anyopaque, a: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8, context: execution_context.RequestContext) anyerror![]f32 {
            const state: *@This() = @ptrCast(@alignCast(ptr));
            try context.check();
            try std.testing.expect(context.deadline_ns != null);
            state.context_calls += 1;
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
    };
    const scores = try rerankDocumentsWithOptions(alloc, &client, cfg, .{
        .antfly_provider = local,
        .execution = .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
    }, "query", &.{ "doc1", "doc2" });
    defer alloc.free(scores);
    try std.testing.expectEqual(@as(usize, 1), state.context_calls);
    try std.testing.expect(!state.legacy_called);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), scores[1], 0.0001);

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, rerankDocumentsWithOptions(alloc, &client, cfg, .{
        .antfly_provider = local,
        .execution = .{ .cancellation = .fromAtomic(&canceled) },
    }, "query", &.{ "doc1", "doc2" }));
    try std.testing.expectEqual(@as(usize, 1), state.context_calls);
}
