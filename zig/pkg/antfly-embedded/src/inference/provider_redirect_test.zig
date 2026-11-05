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

//! Configured inference endpoints are credential and input boundaries. Reject
//! redirects even when the caller supplies an HTTP client that follows them.

const std = @import("std");
const httpx = @import("httpx");
const openai = @import("openai");
const local = @import("local");
const vertex = @import("vertex");
const bedrock = @import("bedrock");
const list_models = @import("list_models");
const capabilities = @import("capabilities");
const google_auth = @import("google").auth;

const input = "private-inference-redirect-marker";
const key = "dummy-provider-key";
const inference = @import("inference_types");
const messages: []const inference.ChatMessage = &.{.{ .role = .user, .content = .{ .text = input } }};

const Operation = enum {
    openai_embed,
    openai_chat,
    gemini_embed,
    gemini_batch_embed,
    gemini_chat,
    vertex_embed,
    vertex_predict,
    vertex_chat,
    vertex_rerank,
    antfly_embed,
    antfly_sparse,
    antfly_framed_embed,
    antfly_chat,
    antfly_rerank,
    bedrock_embed,
    bedrock_chat,
    aws_web_identity,
    google_token,
    models_openai,
    models_openrouter,
    models_gemini,
    models_anthropic,
    models_cohere,
    models_antfly,
    capabilities,
    capabilities_cached,

    fn method(self: Operation) httpx.Method {
        return if (self.isDiscovery()) .GET else .POST;
    }

    fn isDiscovery(self: Operation) bool {
        return @backingInt(self) >= @backingInt(Operation.models_openai);
    }

    fn path(self: Operation) []const u8 {
        return switch (self) {
            .openai_embed => "/embeddings",
            .openai_chat => "/chat/completions",
            .gemini_embed => "/models/model:embedContent",
            .gemini_batch_embed => "/models/model:batchEmbedContents",
            .gemini_chat => "/models/model:generateContent",
            .aws_web_identity => "/sts",
            .google_token => "/token",
            .vertex_embed => "/projects/project/locations/us-central1/publishers/google/models/model:embedContent",
            .vertex_predict => "/projects/project/locations/us-central1/publishers/google/models/model:predict",
            .vertex_chat => "/projects/project/locations/us-central1/publishers/google/models/model:generateContent",
            .vertex_rerank => "/projects/project/locations/global/rankingConfigs/default_ranking_config:rank",
            .antfly_embed => "/embed",
            .antfly_sparse, .antfly_framed_embed => "/embed",
            .antfly_chat => "/generate",
            .antfly_rerank => "/rerank",
            .bedrock_embed => "/model/amazon.titan-embed-text-v2%3A0/invoke",
            .bedrock_chat => "/model/amazon.titan-embed-text-v2%3A0/converse",
            .models_openai, .models_openrouter, .models_anthropic, .models_cohere => "/v1/models",
            .models_gemini => "/models",
            .models_antfly, .capabilities, .capabilities_cached => "/ai/v1/models",
        };
    }

    fn invoke(self: Operation, a: std.mem.Allocator, http: *httpx.Client, base: []const u8, credentials_path: []const u8) !void {
        switch (self) {
            .openai_embed, .openai_chat => {
                var provider = openai.Provider.init(a, http, base);
                defer provider.deinit();
                try provider.setApiKey(key);
                if (self == .openai_embed) {
                    var result = try provider.embedder().embed(a, "model", &.{input});
                    defer result.deinit();
                } else {
                    var result = try provider.generator().generate(a, "model", messages);
                    defer result.deinit();
                }
            },
            .gemini_embed, .gemini_batch_embed, .gemini_chat => {
                var provider = try vertex.GeminiProvider.init(a, http, .{ .base_url = base, .api_key = key });
                defer provider.deinit();
                if (self == .gemini_chat) {
                    var result = try provider.generator().generate(a, "model", messages);
                    defer result.deinit();
                } else {
                    var result = try provider.embedText(a, "model", &.{input}, .{
                        .task_type = "RETRIEVAL_DOCUMENT",
                        .request_format = if (self == .gemini_embed) .embed_content else .batch_embed_contents,
                    });
                    defer result.deinit();
                }
            },
            .vertex_embed, .vertex_predict, .vertex_chat, .vertex_rerank => {
                var provider = try vertex.Provider.init(a, http, .{ .base_url = base, .project_id = "project", .bearer_token = key });
                defer provider.deinit();
                if (self == .vertex_chat) {
                    var result = try provider.generator().generate(a, "model", messages);
                    defer result.deinit();
                } else if (self == .vertex_rerank) {
                    const result = try provider.rerank(a, "model", input, &.{input}, .{});
                    defer a.free(result);
                } else {
                    var result = try provider.embedTextRequest(a, "model", &.{input}, .{
                        .task_type = "RETRIEVAL_DOCUMENT",
                        .request_format = if (self == .vertex_embed) .embed_content else .predict,
                    });
                    defer result.deinit();
                }
            },
            .antfly_embed, .antfly_sparse, .antfly_framed_embed, .antfly_chat, .antfly_rerank => {
                var provider = local.Provider.init(a, http, base);
                defer provider.deinit();
                try provider.setApiKey(key);
                switch (self) {
                    .antfly_embed => {
                        var result = try provider.embedder().embed(a, "model", &.{input});
                        defer result.deinit();
                    },
                    .antfly_framed_embed => {
                        provider.setFramedAttachments(true);
                        var result = try provider.embedGroupWithTask(a, "model", &.{ .{ .text = input }, .{ .binary = .{ .mime_type = "image/png", .data = "private-binary-input" } } }, null, null);
                        defer result.deinit();
                    },
                    .antfly_sparse => {
                        var result = try provider.embedSparse(a, "model", &.{input});
                        defer result.deinit();
                    },
                    .antfly_chat => {
                        var result = try provider.generator().generate(a, "model", messages);
                        defer result.deinit();
                    },
                    .antfly_rerank => {
                        var result = try provider.reranker().rerank(a, "model", input, &.{input});
                        defer result.deinit();
                    },
                    else => unreachable,
                }
            },
            .bedrock_embed, .bedrock_chat => {
                var provider = bedrock.Provider.init(a, http, .{
                    .region = "us-east-1",
                    .endpoint = base,
                    .credential_source = .{ .profile = .{ .name = "fixture", .shared_credentials_file = credentials_path } },
                });
                defer provider.deinit();
                if (self == .bedrock_embed) {
                    var result = try provider.embedText(a, "amazon.titan-embed-text-v2:0", &.{input});
                    defer result.deinit();
                } else {
                    var result = try provider.generator().generate(a, "amazon.titan-embed-text-v2:0", messages);
                    defer result.deinit();
                }
            },
            .aws_web_identity => {
                const endpoint = try std.fmt.allocPrint(a, "{s}/sts", .{base});
                defer a.free(endpoint);
                var provider = bedrock.Provider.init(a, http, .{
                    .region = "us-east-1",
                    .endpoint = base,
                    .credential_source = .{ .web_identity = .{ .role_arn = key, .session_name = input, .token_file = credentials_path, .sts_endpoint = endpoint } },
                });
                defer provider.deinit();
                var result = try provider.embedText(a, "amazon.titan-embed-text-v2:0", &.{input});
                defer result.deinit();
            },
            .google_token => {
                var source = try google_auth.CachedTokenSource.initWithIo(a, .{
                    .scope = try a.dupe(u8, "scope"),
                    .credentials = .{ .authorized_user = .{
                        .client_id = try a.dupe(u8, "client"),
                        .client_secret = try a.dupe(u8, key),
                        .refresh_token = try a.dupe(u8, input),
                        .token_uri = try std.fmt.allocPrint(a, "{s}/token", .{base}),
                    } },
                }, http.io);
                defer source.deinit();
                const token = try source.accessTokenAllocWithControl(a, google_auth.RequestControl.fromTimeout(2_000, null));
                defer a.free(token);
            },
            .capabilities_cached => {
                var cache = capabilities.Cache.init(a, http.io);
                defer cache.deinit();
                _ = try cache.getOrDiscoverWithContext(http, base, "model", .embed, &.{.{ "Authorization", "Bearer " ++ key }}, .{});
            },
            .capabilities => {
                _ = try capabilities.discover(a, http, base, "model", .embed, &.{.{ "Authorization", "Bearer " ++ key }});
            },
            else => {
                const tag: list_models.ProviderTag = switch (self) {
                    .models_openai => .openai,
                    .models_openrouter => .openrouter,
                    .models_gemini => .gemini,
                    .models_anthropic => .anthropic,
                    .models_cohere => .cohere,
                    .models_antfly => .antfly,
                    else => unreachable,
                };
                var result = try list_models.listModels(a, http, .{ .provider = tag, .url = base, .api_key = key }, 2_000);
                defer result.deinit(a);
            },
        }
    }
};

fn assertProviderRequest(request: httpx.testing_mod.RequestInfo) !void {
    if (std.mem.eql(u8, request.path, "/sts") or std.mem.eql(u8, request.path, "/token")) {
        try std.testing.expect(std.mem.indexOf(u8, request.body, key) != null);
        try std.testing.expect(std.mem.indexOf(u8, request.body, input) != null);
        return;
    }
    const auth = request.header("Authorization") orelse request.header("x-goog-api-key") orelse request.header("x-api-key") orelse return error.MissingProviderCredential;
    try std.testing.expect(std.mem.indexOf(u8, auth, key) != null);
    if (request.method == .POST) try std.testing.expect(std.mem.indexOf(u8, request.body, input) != null);
}

fn serve(server: *httpx.TestServer, failure: *?anyerror) std.Io.Cancelable!void {
    while (true) server.handleOne() catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            failure.* = err;
            return;
        },
    };
}

test "provider inference and discovery reject redirects without forwarding credentials or input" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "credentials", .data = "[fixture]\naws_access_key_id = " ++ key ++ "\naws_secret_access_key = dummy-secret\naws_session_token = dummy-session\n" });
    const credentials_path = try tmp.dir.realPathFileAlloc(io, "credentials", a);
    defer a.free(credentials_path);

    for (std.meta.tags(Operation)) |operation| {
        for ([_]u16{ 301, 302, 303, 307, 308 }) |status| {
            for ([_]bool{ false, true }) |same_origin| {
                errdefer std.debug.print("redirect case: {s}, status={d}, same_origin={}\n", .{ @tagName(operation), status, same_origin });
                var destination = try httpx.TestServer.start(a, io, &.{
                    .{ .method = .GET, .path = "/capture", .respond = .{ .status = 401 } },
                    .{ .method = .POST, .path = "/capture", .respond = .{ .status = 401 } },
                });
                defer destination.deinit();
                const cross_origin = try destination.url("/capture");
                defer a.free(cross_origin);
                const headers: []const httpx.testing_mod.HeaderPair = &.{.{ .name = "Location", .value = if (same_origin) "/capture" else cross_origin }};
                var origin = try httpx.TestServer.start(a, io, &.{
                    .{ .method = operation.method(), .path = operation.path(), .assert_request = assertProviderRequest, .respond = .{ .status = status, .headers = headers } },
                    .{ .method = .GET, .path = "/capture", .respond = .{ .status = 401 } },
                    .{ .method = .POST, .path = "/capture", .respond = .{ .status = 401 } },
                });
                defer origin.deinit();
                var origin_failure: ?anyerror = null;
                var destination_failure: ?anyerror = null;
                var group: std.Io.Group = .init;
                defer group.cancel(io);
                try group.concurrent(io, serve, .{ &origin, &origin_failure });
                try group.concurrent(io, serve, .{ &destination, &destination_failure });
                var client = httpx.Client.initWithConfig(a, io, .{
                    .keep_alive = false,
                    .timeouts = .{ .request_ms = 2_000 },
                    .retry_policy = .{ .max_retries = 0 },
                    // The provider must override even an explicitly permissive client.
                    .redirect_policy = .{ .follow_redirects = true },
                });
                defer client.deinit();
                if (operation.invoke(a, &client, origin.baseUrl(), credentials_path)) |_| {
                    return error.ExpectedRedirectFailure;
                } else |_| {}
                group.cancel(io);
                if (origin_failure) |err| return err;
                if (destination_failure) |err| return err;
                try std.testing.expectEqual(@as(usize, 1), origin.route_hits[0]);
                try std.testing.expectEqualSlices(usize, &.{ 0, 0 }, origin.route_hits[1..]);
                try std.testing.expectEqualSlices(usize, &.{ 0, 0 }, destination.route_hits);
            }
        }
    }
}
