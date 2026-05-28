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

// Local inference provider.
//
// Wraps the inference API client to implement the
// provider-neutral Embedder, Generator, and Reranker interfaces.

const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const inference_api = @import("inference_api");
const inference = @import("types.zig");
const binary = @import("binary.zig");
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test)
    @import("../storage/db/template_stub.zig")
else
    @import("../template.zig");

const EmbedWireRequest = struct {
    model: []const u8,
    input: std.json.Value,
    encoding_format: []const u8 = "float",
};

pub const Provider = struct {
    allocator: std.mem.Allocator,
    http: *httpx.Client,
    base_url: []const u8,

    pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, base_url: []const u8) Provider {
        return .{
            .allocator = allocator,
            .http = http,
            .base_url = base_url,
        };
    }

    pub fn deinit(self: *Provider) void {
        _ = self;
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

    pub fn reranker(self: *Provider) inference.Reranker {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &reranker_vtable,
        };
    }

    pub fn embedSparse(self: *Provider, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) !inference.SparseEmbedResult {
        const url = try std.fmt.allocPrint(self.allocator, "{s}/embed", .{self.base_url});
        defer self.allocator.free(url);
        var input_array = std.json.Array.init(alloc);
        defer input_array.deinit();
        for (inputs) |input| try input_array.append(.{ .string = input });
        const json_body = try httpx.json.Json.stringify(self.allocator, EmbedWireRequest{
            .model = model,
            .input = .{ .array = input_array },
        });
        defer self.allocator.free(json_body);
        var resp = try self.http.post(url, .{ .json = json_body });
        defer resp.deinit();

        if (!resp.ok()) {
            logEmbedFailure("sparse", url, resp.status.code, resp.body);
            return mapEmbedStatus(resp.status.code);
        }

        const body = resp.body orelse return error.EmptyResponse;
        if (resp.contentType()) |ct| {
            if (std.mem.startsWith(u8, ct, "application/octet-stream")) {
                var result = try binary.deserializeSparse(alloc, body);
                defer result.deinit(alloc);

                const indices = try alloc.alloc([]const i32, result.vectors.len);
                errdefer alloc.free(indices);
                const values = try alloc.alloc([]const f32, result.vectors.len);
                errdefer alloc.free(values);

                for (result.vectors, 0..) |vector, i| {
                    indices[i] = try alloc.dupe(i32, vector.indices);
                    values[i] = try alloc.dupe(f32, vector.values);
                }
                return .{
                    .indices = indices,
                    .values = values,
                    .allocator = alloc,
                };
            }
        }

        const JsonSparseVector = struct {
            indices: []const i32,
            values: []const f32,
        };
        const JsonEmbeddingObject = struct {
            embedding: JsonSparseVector,
        };
        const JsonSparseResponse = struct {
            data: []const JsonEmbeddingObject,
        };
        var parsed = try std.json.parseFromSlice(JsonSparseResponse, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        const indices = try alloc.alloc([]const i32, parsed.value.data.len);
        errdefer alloc.free(indices);
        const values = try alloc.alloc([]const f32, parsed.value.data.len);
        errdefer alloc.free(values);

        for (parsed.value.data, 0..) |item, i| {
            const vector = item.embedding;
            indices[i] = try alloc.dupe(i32, vector.indices);
            values[i] = try alloc.dupe(f32, vector.values);
        }
        return .{
            .indices = indices,
            .values = values,
            .allocator = alloc,
        };
    }

    pub fn embedParts(self: *Provider, alloc: std.mem.Allocator, model: []const u8, parts: []const template_mod.ContentPart) !inference.EmbedResult {
        var values = std.json.Array.init(alloc);
        defer values.deinit();

        for (parts) |part| {
            switch (part) {
                .text => |text| {
                    var obj = std.json.ObjectMap.empty;
                    errdefer obj.deinit(alloc);
                    try obj.put(alloc, "type", .{ .string = "text" });
                    try obj.put(alloc, "text", .{ .string = text });
                    try values.append(.{ .object = obj });
                },
                .media_url => |url| {
                    var image_url = std.json.ObjectMap.empty;
                    errdefer image_url.deinit(alloc);
                    try image_url.put(alloc, "url", .{ .string = url });

                    var obj = std.json.ObjectMap.empty;
                    errdefer obj.deinit(alloc);
                    try obj.put(alloc, "type", .{ .string = "image_url" });
                    try obj.put(alloc, "image_url", .{ .object = image_url });
                    try values.append(.{ .object = obj });
                },
                .binary => |binary_part| {
                    const encoded_len = std.base64.standard.Encoder.calcSize(binary_part.data.len);
                    const encoded = try alloc.alloc(u8, encoded_len);
                    defer alloc.free(encoded);
                    _ = std.base64.standard.Encoder.encode(encoded, binary_part.data);

                    var obj = std.json.ObjectMap.empty;
                    errdefer obj.deinit(alloc);
                    try obj.put(alloc, "type", .{ .string = "media" });
                    try obj.put(alloc, "data", .{ .string = encoded });
                    try obj.put(alloc, "mime_type", .{ .string = binary_part.mime_type });
                    try values.append(.{ .object = obj });
                },
            }
        }

        return try self.embedJsonInput(alloc, model, .{ .array = values });
    }

    fn embedImpl(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) anyerror!inference.EmbedResult {
        const self: *Provider = @ptrCast(@alignCast(ptr));
        var input_array = std.json.Array.init(alloc);
        defer input_array.deinit();
        for (inputs) |input| try input_array.append(.{ .string = input });
        return try self.embedJsonInput(alloc, model, .{ .array = input_array });
    }

    fn embedJsonInput(self: *Provider, alloc: std.mem.Allocator, model: []const u8, input: std.json.Value) !inference.EmbedResult {
        const url = try std.fmt.allocPrint(self.allocator, "{s}/embed", .{self.base_url});
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, EmbedWireRequest{
            .model = model,
            .input = input,
        });
        defer self.allocator.free(json_body);
        var resp = try self.http.post(url, .{ .json = json_body });
        defer resp.deinit();

        if (!resp.ok()) {
            logEmbedFailure("dense", url, resp.status.code, resp.body);
            return mapEmbedStatus(resp.status.code);
        }

        const body = resp.body orelse return error.EmptyResponse;
        if (resp.contentType()) |ct| {
            if (std.mem.startsWith(u8, ct, "application/octet-stream")) {
                var result = try binary.deserializeDense(alloc, body);
                const vectors = result.vectors;
                const dim = result.dimension;
                result.vectors = &.{};
                return .{
                    .vectors = vectors,
                    .dimension = dim,
                    .allocator = alloc,
                };
            }
        }

        const JsonEmbeddingObject = struct {
            embedding: []const f32,
        };
        const JsonResponse = struct {
            data: []const JsonEmbeddingObject,
        };
        var parsed = try std.json.parseFromSlice(JsonResponse, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.data.len == 0) return error.EmptyResponse;

        const vectors = try alloc.alloc([]const f32, parsed.value.data.len);
        errdefer alloc.free(vectors);
        for (parsed.value.data, 0..) |item, i| {
            vectors[i] = try alloc.dupe(f32, item.embedding);
        }
        return .{
            .vectors = vectors,
            .dimension = parsed.value.data[0].embedding.len,
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
            api_messages[i] = .{
                .role = switch (msg.role) {
                    .system => "system",
                    .user => "user",
                    .assistant => "assistant",
                },
                .content = msg.content,
            };
        }

        const url = try std.fmt.allocPrint(self.allocator, "{s}/generate", .{self.base_url});
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, Request{
            .model = model,
            .messages = api_messages,
        });
        defer self.allocator.free(json_body);
        var resp = try self.http.post(url, .{
            .json = json_body,
            .timeout_ms = 300_000,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.GenerateRequestFailed;
        const body = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const choices = parsed.value.choices;
        if (choices.len == 0) return error.EmptyResponse;
        const content = choices[0].message.content orelse return error.EmptyResponse;

        return .{
            .content = try alloc.dupe(u8, content),
            .allocator = alloc,
        };
    }

    fn rerankImpl(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) anyerror!inference.RerankResult {
        const self: *Provider = @ptrCast(@alignCast(ptr));
        const Request = struct {
            model: []const u8,
            query: []const u8,
            prompts: []const []const u8,
        };
        const Response = struct {
            data: ?[]const struct {
                score: f32,
            } = null,
            scores: ?[]const f32 = null,
        };
        const url = try std.fmt.allocPrint(self.allocator, "{s}/rerank", .{self.base_url});
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, Request{
            .model = model,
            .query = query,
            .prompts = documents,
        });
        defer self.allocator.free(json_body);
        var resp = try self.http.post(url, .{ .json = json_body });
        defer resp.deinit();
        if (!resp.ok()) return error.RerankRequestFailed;
        const body = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(Response, alloc, body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const scores = if (parsed.value.scores) |scores_src| blk: {
            break :blk try alloc.dupe(f32, scores_src);
        } else if (parsed.value.data) |scores_src| blk: {
            const out = try alloc.alloc(f32, scores_src.len);
            for (scores_src, 0..) |item, i| out[i] = item.score;
            break :blk out;
        } else return error.InvalidRerankerResponse;
        return .{
            .scores = scores,
            .allocator = alloc,
        };
    }

    const embedder_vtable = inference.Embedder.VTable{
        .embed = &embedImpl,
    };
    const generator_vtable = inference.Generator.VTable{
        .generate = &generateImpl,
    };
    const reranker_vtable = inference.Reranker.VTable{
        .rerank = &rerankImpl,
    };
};

fn mapEmbedStatus(status: u16) anyerror {
    return switch (status) {
        429 => error.EmbedRateLimited,
        408, 502, 503, 504 => error.EmbedTransientFailure,
        else => if (status >= 500 and status <= 599) error.EmbedTransientFailure else error.EmbedRequestFailed,
    };
}

fn logEmbedFailure(kind: []const u8, url: []const u8, status: u16, body: ?[]const u8) void {
    const raw = body orelse "";
    const clipped = raw[0..@min(raw.len, 512)];
    std.log.warn("antfly {s} embed failed status={d} url={s} body={s}", .{ kind, status, url, clipped });
}

test "antfly provider compiles" {
    _ = Provider;
}

test "antfly embed request omits nullable generated fields" {
    const alloc = std.testing.allocator;
    var input = std.json.Array.init(alloc);
    defer input.deinit();
    try input.append(.{ .string = "hello" });

    const body = try httpx.json.Json.stringify(alloc, EmbedWireRequest{
        .model = "antflydb/clipclap",
        .input = .{ .array = input },
    });
    defer alloc.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"encoding_format\":\"float\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"dimensions\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, body, "null") == null);
}

test "antfly generate round trip" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/generate", .respond = .{
            .body =
            \\{"choices":[{"message":{"role":"assistant","content":"Hi from Antfly!"}}]}
            ,
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;

    var result_ok: bool = false;
    var result_content: ?[]const u8 = null;
    var result_err: anyerror = error.None;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, content_out: *?[]const u8, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var gen = provider.generator();
            var result = gen.generate(a, "test-model", &.{.{ .role = .user, .content = "Hello" }}) catch |e| {
                err_out.* = e;
                return;
            };
            defer result.deinit();

            ok_out.* = true;
            content_out.* = a.dupe(u8, result.content) catch null;
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, ts.baseUrl(), &result_ok, &result_content, &result_err }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    defer if (result_content) |c| alloc.free(c);

    if (!result_ok) {
        std.debug.print("generate fiber error: {}\n", .{result_err});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqualStrings("Hi from Antfly!", result_content orelse "NO CONTENT");
}

test "antfly sparse embed round trip" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/embed", .respond = .{
            .content_type = "application/json",
            .body =
            \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":{"indices":[7,42],"values":[1.5,0.5]}}],"model":"sparse-model","usage":{"prompt_tokens":1,"total_tokens":1}}
            ,
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;

    var ok: bool = false;
    var result_err: anyerror = error.None;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var result = provider.embedSparse(a, "sparse-model", &.{"alpha body"}) catch |err| {
                err_out.* = err;
                return;
            };
            defer result.deinit();

            std.testing.expectEqual(@as(usize, 1), result.indices.len) catch |err| {
                err_out.* = err;
                return;
            };
            std.testing.expectEqual(@as(i32, 42), result.indices[0][1]) catch |err| {
                err_out.* = err;
                return;
            };
            std.testing.expectEqual(@as(f32, 0.5), result.values[0][1]) catch |err| {
                err_out.* = err;
                return;
            };
            ok_out.* = true;
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, ts.io, ts.baseUrl(), &ok, &result_err }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    if (result_err != error.None) return result_err;
    try std.testing.expect(ok);
}

test "antfly rerank round trip" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/rerank", .respond = .{
            .body =
            \\{"object":"list","data":[{"object":"rerank.score","index":0,"score":0.9},{"object":"rerank.score","index":1,"score":0.1},{"object":"rerank.score","index":2,"score":0.5}],"model":"reranker-v1","usage":{"prompt_tokens":4,"completion_tokens":0,"total_tokens":4}}
            ,
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;

    var result_ok: bool = false;
    var result_score_count: usize = 0;
    var result_first_score: f32 = 0;
    var result_err: anyerror = error.None;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, count_out: *usize, first_out: *f32, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var rr = provider.reranker();
            var result = rr.rerank(a, "reranker-v1", "query", &.{ "doc1", "doc2", "doc3" }) catch |e| {
                err_out.* = e;
                return;
            };
            defer result.deinit();

            ok_out.* = true;
            count_out.* = result.scores.len;
            if (result.scores.len > 0) first_out.* = result.scores[0];
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, ts.baseUrl(), &result_ok, &result_score_count, &result_first_score, &result_err }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    if (!result_ok) {
        std.debug.print("rerank fiber error: {}\n", .{result_err});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, 3), result_score_count);
    try std.testing.expectEqual(@as(f32, 0.9), result_first_score);
}

test "antfly rerank accepts scores array response" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/rerank", .respond = .{
            .body =
            \\{"scores":[0.8,0.2]}
            ,
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;

    var result_ok: bool = false;
    var result_score_count: usize = 0;
    var result_first_score: f32 = 0;
    var result_err: anyerror = error.None;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, count_out: *usize, first_out: *f32, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var rr = provider.reranker();
            var result = rr.rerank(a, "reranker-v1", "query", &.{ "doc1", "doc2" }) catch |e| {
                err_out.* = e;
                return;
            };
            defer result.deinit();

            ok_out.* = true;
            count_out.* = result.scores.len;
            if (result.scores.len > 0) first_out.* = result.scores[0];
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, ts.baseUrl(), &result_ok, &result_score_count, &result_first_score, &result_err }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    if (!result_ok) {
        std.debug.print("rerank fiber error: {}\n", .{result_err});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, 2), result_score_count);
    try std.testing.expectEqual(@as(f32, 0.8), result_first_score);
}

test "antfly embed round trip (binary)" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    // Build a binary dense embedding response:
    // Header: u64 num_vectors (1) + u64 dimension (3) + 3 x f32 values
    var bin_buf: [16 + 3 * 4]u8 = undefined;
    std.mem.writeInt(u64, bin_buf[0..8], 1, .little); // num_vectors
    std.mem.writeInt(u64, bin_buf[8..16], 3, .little); // dimension
    // f32 values: 0.5, 1.5, 2.5
    bin_buf[16..20].* = @bitCast(@as(f32, 0.5));
    bin_buf[20..24].* = @bitCast(@as(f32, 1.5));
    bin_buf[24..28].* = @bitCast(@as(f32, 2.5));

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/embed", .respond = .{
            .body = &bin_buf,
            .content_type = "application/octet-stream",
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;

    var result_ok: bool = false;
    var result_dim: usize = 0;
    var result_first_vec: ?[]const f32 = null;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, ok_out: *bool, dim_out: *usize, vec_out: *?[]const f32) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var emb = provider.embedder();
            var result = emb.embed(a, "bge-small", &.{"test input"}) catch return;
            defer result.deinit();

            ok_out.* = true;
            dim_out.* = result.dimension;
            if (result.vectors.len > 0) {
                vec_out.* = a.dupe(f32, result.vectors[0]) catch null;
            }
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, ts.baseUrl(), &result_ok, &result_dim, &result_first_vec }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    defer if (result_first_vec) |v| alloc.free(v);

    try std.testing.expect(result_ok);
    try std.testing.expectEqual(@as(usize, 3), result_dim);
    const vec = result_first_vec orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(f32, 0.5), vec[0]);
    try std.testing.expectEqual(@as(f32, 1.5), vec[1]);
    try std.testing.expectEqual(@as(f32, 2.5), vec[2]);
}

test "antfly embed fails on non-200 response" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/embed", .respond = .{
            .status = 503,
            .body = "unavailable",
            .content_type = "text/plain",
        } },
    });
    defer ts.deinit();

    var group = std.Io.Group.init;
    var result_err: anyerror = error.None;

    const Fiber = struct {
        fn run(a: std.mem.Allocator, test_io: std.Io, base: []const u8, err_out: *anyerror) std.Io.Cancelable!void {
            var client = httpx.Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer client.deinit();

            var provider = Provider.init(a, &client, base);
            defer provider.deinit();

            var emb = provider.embedder();
            _ = emb.embed(a, "bge-small", &.{"test input"}) catch |e| {
                err_out.* = e;
                return;
            };
            err_out.* = error.TestUnexpectedResult;
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, ts.baseUrl(), &result_err }) catch return;

    try ts.handleOne();
    group.await(io) catch {};

    try std.testing.expectEqual(error.EmbedRequestFailed, result_err);
}
