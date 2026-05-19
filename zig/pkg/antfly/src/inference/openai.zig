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

// OpenAI-compatible inference provider.
//
// Works with OpenAI, Ollama, vLLM, and any OpenAI-compatible API.
// Uses the generated client from the official OpenAI 3.1 spec.

const std = @import("std");
const httpx = @import("httpx");
const openai_api = @import("openai_api");
const inference = @import("types.zig");

pub const Provider = struct {
    allocator: std.mem.Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,

    pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, base_url: []const u8) Provider {
        return .{
            .allocator = allocator,
            .http = http,
            .base_url = base_url,
        };
    }

    pub fn deinit(self: *Provider) void {
        if (self.auth_header) |h| {
            self.allocator.free(h[1]);
            self.auth_header = null;
        }
    }

    /// Set Bearer token for authentication (OpenAI API key).
    pub fn setApiKey(self: *Provider, api_key: []const u8) !void {
        const auth_header = try std.fmt.allocPrint(self.allocator, "Bearer {s}", .{api_key});
        defer self.allocator.free(auth_header);
        try self.setAuthorizationHeader(auth_header);
    }

    pub fn setAuthorizationHeader(self: *Provider, auth_header: []const u8) !void {
        if (self.auth_header) |h| {
            if (std.mem.eql(u8, h[1], auth_header)) return;
            self.allocator.free(h[1]);
        }
        self.auth_header = .{ "Authorization", try self.allocator.dupe(u8, auth_header) };
    }

    pub fn embedder(self: *Provider) inference.Embedder {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &embedder_vtable,
        };
    }

    pub fn generator(self: *Provider) inference.Generator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &generator_vtable,
        };
    }

    fn embedImpl(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) anyerror!inference.EmbedResult {
        const self: *Provider = @ptrCast(@alignCast(ptr));

        var input_array = std.json.Array.init(alloc);
        defer input_array.deinit();
        for (inputs) |input| try input_array.append(.{ .string = input });

        const url = try std.fmt.allocPrint(self.allocator, "{s}/embeddings", .{self.base_url});
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, openai_api.types.CreateEmbeddingRequest{
            .model = .{ .string = model },
            .input = .{ .array = input_array },
        });
        defer self.allocator.free(json_body);
        var resp = try self.http.post(url, .{ .json = json_body, .headers = self.authHeaders() });
        defer resp.deinit();
        if (!resp.ok()) return mapEmbedStatus(resp.status);
        const body = resp.body orelse return error.EmptyResponse;
        const EmbeddingResponse = struct {
            data: []const struct {
                embedding: []const f32,
            },
        };
        var parsed = try std.json.parseFromSlice(EmbeddingResponse, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        const data = parsed.value.data;
        const vectors = try alloc.alloc([]const f32, data.len);
        errdefer {
            for (vectors) |v| if (v.len > 0) alloc.free(v);
            alloc.free(vectors);
        }

        var dimension: usize = 0;
        for (data, 0..) |item, i| {
            const embedding = item.embedding;
            if (dimension == 0) dimension = embedding.len;
            vectors[i] = try alloc.dupe(f32, embedding);
        }

        return .{
            .vectors = vectors,
            .dimension = dimension,
            .allocator = alloc,
        };
    }

    fn generateImpl(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, messages: []const inference.ChatMessage) anyerror!inference.GenerateResult {
        const self: *Provider = @ptrCast(@alignCast(ptr));

        const Message = struct {
            role: []const u8,
            content: []const u8,
        };
        const Request = struct {
            model: []const u8,
            messages: []const Message,
        };
        const Response = struct {
            choices: []const struct {
                message: struct {
                    content: ?[]const u8 = null,
                },
            },
        };

        const api_messages = try alloc.alloc(Message, messages.len);
        defer alloc.free(api_messages);
        for (messages, 0..) |msg, i| {
            api_messages[i] = switch (msg.role) {
                .system => .{ .role = "system", .content = msg.content },
                .user => .{ .role = "user", .content = msg.content },
                .assistant => .{ .role = "assistant", .content = msg.content },
            };
        }

        const url = try std.fmt.allocPrint(self.allocator, "{s}/chat/completions", .{self.base_url});
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, Request{
            .model = model,
            .messages = api_messages,
        });
        defer self.allocator.free(json_body);
        var resp = try self.http.post(url, .{ .json = json_body, .headers = self.authHeaders() });
        defer resp.deinit();
        if (!resp.ok()) return error.GenerateRequestFailed;
        const body = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        const choices = parsed.value.choices;
        if (choices.len > 0) {
            if (choices[0].message.content) |content| {
                return .{
                    .content = try alloc.dupe(u8, content),
                    .allocator = alloc,
                };
            }
        }

        return error.GenerateRequestFailed;
    }

    fn authHeaders(self: *const Provider) ?[]const [2][]const u8 {
        if (self.auth_header) |*h| return @as(*const [1][2][]const u8, h);
        return null;
    }

    const embedder_vtable = inference.Embedder.VTable{
        .embed = &embedImpl,
    };
    const generator_vtable = inference.Generator.VTable{
        .generate = &generateImpl,
    };
};

fn mapEmbedStatus(status: u16) anyerror {
    return switch (status) {
        429 => error.EmbedRateLimited,
        408, 502, 503, 504 => error.EmbedTransientFailure,
        else => if (status >= 500 and status <= 599) error.EmbedTransientFailure else error.EmbedRequestFailed,
    };
}

test "openai provider compiles" {
    _ = Provider;
}

test "openai embed round trip and non-200 response" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/ok/embeddings", .respond = .{
            .body =
            \\{"data":[{"embedding":[1.0,2.0,3.0],"index":0}],"model":"text-embedding-3-small","usage":{"prompt_tokens":1,"total_tokens":1}}
            ,
        } },
        .{ .method = .POST, .path = "/err/embeddings", .respond = .{
            .status = 401,
            .body = "{\"error\":\"unauthorized\"}",
        } },
    });
    defer ts.deinit();

    const ok_base = try std.fmt.allocPrint(alloc, "{s}/ok", .{ts.baseUrl()});
    defer alloc.free(ok_base);
    const err_base = try std.fmt.allocPrint(alloc, "{s}/err", .{ts.baseUrl()});
    defer alloc.free(err_base);

    var state: struct {
        ok: bool = false,
        dim: usize = 0,
        vec_count: usize = 0,
        first_vec: ?[]const f32 = null,
        err: anyerror = error.None,
    } = .{};

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, dim_out: *usize, count_out: *usize, vec_out: *?[]const f32) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var emb = provider.embedder();
            var result = emb.embed(a, "text-embedding-3-small", &.{"hello"}) catch return;
            defer result.deinit();

            ok_out.* = true;
            dim_out.* = result.dimension;
            count_out.* = result.vectors.len;
            if (result.vectors.len > 0) {
                vec_out.* = a.dupe(f32, result.vectors[0]) catch null;
            }
        }

        fn runErr(a: std.mem.Allocator, test_io: std.Io, base: []const u8, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();
            provider.setApiKey("secret") catch |e| {
                err_out.* = e;
                return;
            };

            var emb = provider.embedder();
            _ = emb.embed(a, "text-embedding-3-small", &.{"hello"}) catch |e| {
                err_out.* = e;
                return;
            };
            err_out.* = error.TestUnexpectedResult;
        }
    };

    var group = std.Io.Group.init;
    group.concurrent(io, Fiber.run, .{ alloc, io, ok_base, &state.ok, &state.dim, &state.vec_count, &state.first_vec }) catch return;
    group.concurrent(io, Fiber.runErr, .{ alloc, io, err_base, &state.err }) catch return;

    try ts.handleOne();
    try ts.handleOne();
    group.await(io) catch {};

    defer if (state.first_vec) |v| alloc.free(v);

    try std.testing.expect(state.ok);
    try std.testing.expectEqual(@as(usize, 3), state.dim);
    try std.testing.expectEqual(@as(usize, 1), state.vec_count);
    const vec = state.first_vec orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(f32, 1.0), vec[0]);
    try std.testing.expectEqual(@as(f32, 2.0), vec[1]);
    try std.testing.expectEqual(@as(f32, 3.0), vec[2]);

    try std.testing.expectEqual(error.EmbedRequestFailed, state.err);
}

test "openai generate round trip and empty choices failure" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/ok/chat/completions", .respond = .{
            .body =
            \\{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-4","choices":[{"index":0,"message":{"role":"assistant","content":"Hello there!"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}
            ,
        } },
        .{ .method = .POST, .path = "/empty/chat/completions", .respond = .{
            .body = "{\"choices\":[]}",
        } },
    });
    defer ts.deinit();

    const ok_base = try std.fmt.allocPrint(alloc, "{s}/ok", .{ts.baseUrl()});
    defer alloc.free(ok_base);
    const empty_base = try std.fmt.allocPrint(alloc, "{s}/empty", .{ts.baseUrl()});
    defer alloc.free(empty_base);

    var state: struct {
        err: anyerror = error.None,
        ok: bool = false,
        content: ?[]const u8 = null,
    } = .{};

    const Fiber = struct {
        fn runOk(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, content_out: *?[]const u8) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var gen = provider.generator();
            var result = gen.generate(a, "gpt-4", &.{.{ .role = .user, .content = "Hi" }}) catch return;
            defer result.deinit();

            ok_out.* = true;
            content_out.* = a.dupe(u8, result.content) catch null;
        }

        fn runErr(a: std.mem.Allocator, test_io: std.Io, base: []const u8, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var gen = provider.generator();
            _ = gen.generate(a, "gpt-4", &.{.{ .role = .user, .content = "Hi" }}) catch |e| {
                err_out.* = e;
                return;
            };
            err_out.* = error.TestUnexpectedResult;
        }
    };

    var group = std.Io.Group.init;
    group.concurrent(io, Fiber.runOk, .{ alloc, io, ok_base, &state.ok, &state.content }) catch return;
    try ts.handleOne();
    group.await(io) catch {};

    defer if (state.content) |c| alloc.free(c);

    try std.testing.expect(state.ok);
    try std.testing.expectEqualStrings("Hello there!", state.content orelse "NO CONTENT");

    group = std.Io.Group.init;
    group.concurrent(io, Fiber.runErr, .{ alloc, io, empty_base, &state.err }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    try std.testing.expectEqual(error.GenerateRequestFailed, state.err);
}
