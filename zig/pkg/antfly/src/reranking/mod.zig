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
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const antfly_provider = @import("../inference/local.zig");
const vertex_provider = @import("../inference/vertex.zig");
const common_secrets = @import("../common/secrets.zig");
const google_auth = @import("antfly_google").auth;

pub const Config = lib.Config;
pub const Provider = lib.Provider;

/// Long-lived resources shared by reranking requests. This keeps HTTP
/// connections warm and lets ADC refresh single-flight per credential/scope.
pub const Runtime = struct {
    http: httpx.Client,
    credentials: google_auth.CredentialManager,

    pub fn init(alloc: std.mem.Allocator, io: std.Io) Runtime {
        return .{
            .http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = true }),
            .credentials = google_auth.CredentialManager.init(alloc, io),
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.credentials.deinit();
        self.http.deinit();
        self.* = undefined;
    }
};

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
    const configured_secret = switch (cfg.provider) {
        .cohere => try common_secrets.SecretValue.initConfigOrEnv(alloc, cfg.api_key, "COHERE_API_KEY"),
        else => try common_secrets.SecretValue.initConfig(alloc, cfg.api_key),
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
                    if (local.rerank_texts) |rerank| {
                        return try rerank(local.ptr, alloc, cfg.model, query, documents);
                    }
                }
            }
            var provider = antfly_provider.Provider.init(alloc, http, cfg.defaultedUrl());
            defer provider.deinit();
            var result = try provider.reranker().rerank(alloc, cfg.model, query, documents);
            defer result.deinit();
            return try alloc.dupe(f32, result.scores);
        },
        .cohere => {
            const token = api_key orelse return error.InvalidRerankerConfig;
            return try rerankCohere(alloc, http, cfg, token, query, documents);
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
                .base_url = if (cfg.url.len > 0) cfg.url else "https://discoveryengine.googleapis.com/v1",
                .project_id = if (cfg.project_id.len > 0) cfg.project_id else null,
                .credentials_path = if (cfg.credentials_path.len > 0) cfg.credentials_path else null,
                .bearer_token = api_key,
                .token_source = token_source,
            });
            defer provider.deinit();
            return try provider.rerank(alloc, cfg.model, query, documents);
        },
    }
}

fn rerankCohere(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: Config,
    api_key: []const u8,
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
        .{if (cfg.url.len > 0) cfg.url else "https://api.cohere.com"},
    );
    defer alloc.free(url);
    const authorization = try std.fmt.allocPrint(alloc, "Bearer {s}", .{api_key});
    defer alloc.free(authorization);
    const headers = [_][2][]const u8{.{ "Authorization", authorization }};
    var response = try http.post(url, .{ .json = body, .headers = &headers });
    defer response.deinit();
    if (!response.ok()) return error.RerankRequestFailed;
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
