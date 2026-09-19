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

// Antfly inference HTTP client wrapper.
//
// Wraps the generated OpenAPI client with convenience methods, including the
// negotiated numeric response that keeps embedding floats out of JSON text.

const std = @import("std");
const api = @import("inference_api");
const httpx = @import("httpx");
const numeric = httpx.numeric_response;

/// Generated types from the inference OpenAPI spec.
pub const Types = api.types;

/// Dense vectors, one per embedded input, owned by the caller.
pub const DenseEmbeddings = struct {
    dimension: usize,
    vectors: []const []const f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DenseEmbeddings) void {
        for (self.vectors) |vector| self.allocator.free(vector);
        self.allocator.free(self.vectors);
        self.vectors = &.{};
    }
};

/// Raw generated client -- exposes every inference API operation.
pub const RawClient = api.client.Client;

/// One transcription result. `object()` is the first (and only) transcript
/// object; `segments` carries its timestamped phrases when the server
/// produced them.
pub const Transcription = struct {
    parsed: std.json.Parsed(api.TranscribeResponse),

    pub fn object(self: *const Transcription) api.TranscribeObject {
        return self.parsed.value.data[0];
    }

    pub fn text(self: *const Transcription) []const u8 {
        return self.object().text;
    }

    pub fn segments(self: *const Transcription) []const api.DictationSegment {
        return self.object().segments orelse &.{};
    }

    pub fn deinit(self: *Transcription) void {
        self.parsed.deinit();
    }
};

/// High-level Antfly inference client with convenience helpers.
pub const Client = struct {
    raw: RawClient,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, http: *@import("httpx").Client, base_url: []const u8) Client {
        return .{
            .raw = RawClient.init(allocator, http, base_url),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Client) void {
        self.raw.deinit();
    }

    /// Embed text inputs and return dense f32 vectors.
    ///
    /// The request asks for the numeric frame, which keeps every float out of
    /// JSON text. A server that does not produce it for this model — an older
    /// one, or a model whose vectors are sparse — answers with the ordinary
    /// JSON body, which is decoded here instead.
    pub fn embedDense(self: *Client, model: []const u8, inputs: []const []const u8) !DenseEmbeddings {
        var input_array = std.json.Array.init(self.allocator);
        defer input_array.deinit();
        for (inputs) |input| try input_array.append(.{ .string = input });
        var resp = try self.raw.createEmbedding(.{
            .model = model,
            .input = .{ .array = input_array },
        }, numeric.accept);
        defer resp.deinit();

        if (resp.status_code < 200 or resp.status_code >= 300) return error.EmbedRequestFailed;

        if (resp.bytes) |frame| {
            const content_type = resp.content_type orelse return error.UnexpectedContentType;
            if (!std.ascii.eqlIgnoreCase(content_type, numeric.content_type)) return error.UnexpectedContentType;
            const view = try numeric.parse(frame, .dense, inputs.len, null);
            return .{
                .dimension = view.columns,
                .vectors = try view.denseAlloc(self.allocator),
                .allocator = self.allocator,
            };
        }

        const parsed = resp.data orelse return error.EmptyResponse;
        return denseFromJson(self.allocator, parsed.value, inputs.len);
    }

    /// Transcribe one encoded audio clip (any container the server decodes:
    /// WAV, MP3, M4A, MP4, Ogg, WebM, FLAC) and return the transcript with
    /// timestamped segments. Clips longer than one Whisper window are
    /// transcribed in windows cut at pauses by the server.
    pub fn transcribe(self: *Client, model: []const u8, audio: []const u8, language: ?[]const u8) !Transcription {
        const encoded = try self.allocator.alloc(u8, std.base64.standard.Encoder.calcSize(audio.len));
        defer self.allocator.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, audio);
        var resp = try self.raw.transcribeAudio(.{
            .model = model,
            .audio = encoded,
            .language = language,
        });
        defer resp.deinit();

        if (resp.status_code < 200 or resp.status_code >= 300) {
            return error.TranscribeRequestFailed;
        }
        const parsed = resp.data orelse return error.EmptyResponse;
        if (parsed.value.data.len == 0) return error.EmptyResponse;
        // The transcript outlives the response it arrived in, so the caller
        // takes the parsed payload and the deferred cleanup is left with
        // nothing to free.
        resp.data = null;
        return .{ .parsed = parsed };
    }

    /// List available models on the Antfly inference server.
    pub fn listModels(self: *Client) !api.client.ApiResponse(Types.ModelsResponse) {
        return try self.raw.listModels();
    }
};

/// The JSON body carries each vector as an array of numbers. A sparse model's
/// object shape has no dense reading, so it is rejected rather than silently
/// turned into an empty vector.
fn denseFromJson(allocator: std.mem.Allocator, response: Types.EmbedResponse, expected_count: usize) !DenseEmbeddings {
    if (response.data.len != expected_count) return error.InvalidEmbeddingResponse;
    const vectors = try allocator.alloc([]const f32, response.data.len);
    var filled: usize = 0;
    errdefer {
        for (vectors[0..filled]) |vector| allocator.free(vector);
        allocator.free(vectors);
    }

    var dimension: usize = 0;
    for (response.data, 0..) |object, i| {
        const value = object.embedding orelse return error.UnexpectedEmbeddingShape;
        if (value != .array) return error.UnexpectedEmbeddingShape;
        const items = value.array.items;
        if (i == 0) dimension = items.len else if (items.len != dimension) return error.UnexpectedEmbeddingShape;
        const vector = try allocator.alloc(f32, items.len);
        vectors[i] = vector;
        filled += 1;
        for (items, vector) |item, *out| out.* = switch (item) {
            .float => |float| @floatCast(float),
            .integer => |integer| @floatFromInt(integer),
            else => return error.UnexpectedEmbeddingShape,
        };
    }
    return .{ .dimension = dimension, .vectors = vectors, .allocator = allocator };
}

test "client module compiles" {
    _ = Client;
    _ = RawClient;
    _ = Types;
    _ = DenseEmbeddings;
    // Referencing the methods themselves is what forces Zig to analyse
    // their bodies; naming the type alone leaves them unchecked, which is
    // how transcribe came to read a field the response does not have.
    _ = &Client.embedDense;
    _ = &denseFromJson;
    _ = &Client.transcribe;
    _ = &Client.listModels;
    _ = &DenseEmbeddings.deinit;
    _ = &Transcription.text;
    _ = &Transcription.segments;
}

fn checkDenseFromJsonAllocationFailures(allocator: std.mem.Allocator) !void {
    var first = std.json.Array.init(allocator);
    defer first.deinit();
    try first.append(.{ .float = 0.25 });
    try first.append(.{ .integer = 1 });
    var second = std.json.Array.init(allocator);
    defer second.deinit();
    try second.append(.{ .float = -0.5 });
    try second.append(.{ .integer = 0 });

    const objects = [_]Types.EmbeddingObject{
        .{ .object = "embedding", .index = 0, .embedding = .{ .array = first } },
        .{ .object = "embedding", .index = 1, .embedding = .{ .array = second } },
    };
    const response = Types.EmbedResponse{
        .object = "list",
        .data = &objects,
        .model = "antfly/embed",
        .usage = .{ .prompt_tokens = 2, .total_tokens = 2 },
    };

    var embeddings = try denseFromJson(allocator, response, objects.len);
    defer embeddings.deinit();
    try std.testing.expectEqual(@as(usize, 2), embeddings.dimension);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 1 }, embeddings.vectors[0]);
    try std.testing.expectEqualSlices(f32, &.{ -0.5, 0 }, embeddings.vectors[1]);
}

test "decoding a json body keeps no vector when an allocation fails" {
    // A vector is published into the result before its values are filled in,
    // so a failure partway through must still free everything already taken.
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkDenseFromJsonAllocationFailures, .{});
}

test "embedDense decodes the numeric frame a negotiating server returns" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const frame = try numeric.allocFrame(allocator, .dense, 2, 3);
    defer allocator.free(frame);
    for ([_]f32{ 0.25, 0.5, 0.75, -1, 0, 1 }, 0..) |value, i| try numeric.setValue(frame, i, value);

    const Assert = struct {
        fn accept(req: httpx.testing_mod.RequestInfo) anyerror!void {
            // Without this header the server has no reason to answer with a
            // frame, so the whole negotiated path would go untested.
            try std.testing.expectEqualStrings(numeric.accept, req.header("Accept") orelse return error.MissingAccept);
        }
    };
    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .POST,
        .path = "/embeddings",
        .respond = .{ .body = frame, .content_type = numeric.content_type },
        .assert_request = Assert.accept,
    }});
    defer server.deinit();

    var embeddings = try runEmbedDense(allocator, io, &server);
    defer embeddings.deinit();
    try std.testing.expectEqual(@as(usize, 3), embeddings.dimension);
    try std.testing.expectEqual(@as(usize, 2), embeddings.vectors.len);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5, 0.75 }, embeddings.vectors[0]);
    try std.testing.expectEqualSlices(f32, &.{ -1, 0, 1 }, embeddings.vectors[1]);
}

test "embedDense falls back to the JSON body when the server sends no frame" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const body =
        "{\"object\":\"list\",\"data\":[" ++
        "{\"object\":\"embedding\",\"index\":0,\"embedding\":[0.25,0.5,0.75]}," ++
        "{\"object\":\"embedding\",\"index\":1,\"embedding\":[-1,0,1]}]," ++
        "\"model\":\"antfly/embed\",\"usage\":{\"prompt_tokens\":2,\"total_tokens\":2}}";
    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .POST,
        .path = "/embeddings",
        .respond = .{ .body = body, .content_type = "application/json" },
    }});
    defer server.deinit();

    var embeddings = try runEmbedDense(allocator, io, &server);
    defer embeddings.deinit();
    try std.testing.expectEqual(@as(usize, 3), embeddings.dimension);
    try std.testing.expectEqual(@as(usize, 2), embeddings.vectors.len);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5, 0.75 }, embeddings.vectors[0]);
    // Whole numbers arrive as JSON integers, which the decoder must accept.
    try std.testing.expectEqualSlices(f32, &.{ -1, 0, 1 }, embeddings.vectors[1]);
}

/// Embed two inputs against a fixture server, serving its one response.
fn runEmbedDense(allocator: std.mem.Allocator, io: std.Io, server: *httpx.TestServer) !DenseEmbeddings {
    const endpoint = try allocator.dupe(u8, server.baseUrl());
    defer allocator.free(endpoint);
    var http = httpx.Client.initWithConfig(allocator, io, .{ .keep_alive = false });
    defer http.deinit();

    var result: ?DenseEmbeddings = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(a: std.mem.Allocator, h: *httpx.Client, url: []const u8, out: *?DenseEmbeddings, err_out: *?anyerror) std.Io.Cancelable!void {
            var client = Client.init(a, h, url);
            defer client.deinit();
            out.* = client.embedDense("antfly/embed", &.{ "first", "second" }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    try group.concurrent(io, Fiber.run, .{ allocator, &http, endpoint, &result, &run_err });
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;
    return result orelse error.NoEmbeddings;
}

test "transcribe returns the transcript and owns it after the response is freed" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const body =
        "{\"object\":\"list\",\"data\":[{\"object\":\"transcription\",\"index\":0," ++
        "\"text\":\"hello there\",\"language\":\"en\",\"duration_ms\":900," ++
        "\"segments\":[{\"text\":\"hello there\",\"start_ms\":0,\"end_ms\":900,\"words\":[]," ++
        "\"speaker\":\"SPEAKER_00\"}],\"speakers\":[\"SPEAKER_00\"]}]," ++
        "\"model\":\"openai/whisper-tiny\",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":2,\"total_tokens\":2}}";

    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .POST,
        .path = "/transcribe",
        .respond = .{ .body = body },
    }});
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(allocator, "{s}", .{server.baseUrl()});
    defer allocator.free(endpoint);
    var http = httpx.Client.initWithConfig(allocator, io, .{ .keep_alive = false });
    defer http.deinit();

    var result: ?Transcription = null;
    defer if (result) |*value| value.deinit();
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(a: std.mem.Allocator, h: *httpx.Client, url: []const u8, out: *?Transcription, err_out: *?anyerror) std.Io.Cancelable!void {
            var client = Client.init(a, h, url);
            defer client.deinit();
            out.* = client.transcribe("openai/whisper-tiny", "fake audio", null) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    try group.concurrent(io, Fiber.run, .{ allocator, &http, endpoint, &result, &run_err });
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    // The transcript is readable after the HTTP response it arrived in has
    // been freed, which is the whole point of taking ownership of it.
    const transcription = &result.?;
    try std.testing.expectEqualStrings("hello there", transcription.text());
    const segments = transcription.segments();
    try std.testing.expectEqual(@as(usize, 1), segments.len);
    try std.testing.expectEqualStrings("SPEAKER_00", segments[0].speaker.?);
}
