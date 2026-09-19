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
// Wraps the generated OpenAPI client with convenience methods and
// binary response deserialization for embeddings.

const std = @import("std");
const api = @import("inference_api");
const binary = @import("binary.zig");

pub const Binary = binary;
pub const DenseEmbeddings = binary.DenseEmbeddings;
pub const SparseEmbeddings = binary.SparseEmbeddings;
pub const SparseVector = binary.SparseVector;

/// Generated types from the inference OpenAPI spec.
pub const Types = api.types;

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

    /// Embed text inputs and return dense f32 vectors (binary format).
    /// This is the most efficient path — avoids JSON serialization of float arrays.
    ///
    /// Known broken, and older than the transcription helper below it: the
    /// generated client parses every response as JSON and returns the parsed
    /// value, so there is no `body` to hand to the binary decoder and this
    /// does not compile when called. Reaching an octet-stream endpoint needs
    /// a raw HTTP call rather than the generated one, so the fix is a change
    /// to the generator, not to this file. `embedSparseBinary` below has the
    /// same problem; neither is referenced by the package's tests, which is
    /// why the build stays green.
    pub fn embedBinary(self: *Client, model: []const u8, inputs: []const []const u8) !DenseEmbeddings {
        var resp = try self.raw.createEmbedding(.{
            .model = model,
            .input = .{ .texts = inputs },
        });
        defer resp.deinit();

        if (resp.status_code < 200 or resp.status_code >= 300) {
            return error.EmbedRequestFailed;
        }

        const body = resp.body orelse return error.EmptyResponse;

        // Check content type — binary responses have application/octet-stream
        if (resp.content_type) |ct| {
            if (std.mem.startsWith(u8, ct, "application/octet-stream")) {
                return try binary.deserializeDense(self.allocator, body);
            }
        }

        // Fall back to JSON parsing if server didn't return binary
        return error.UnexpectedContentType;
    }

    /// Embed text inputs and return sparse vectors (binary format).
    pub fn embedSparseBinary(self: *Client, model: []const u8, inputs: []const []const u8) !SparseEmbeddings {
        var resp = try self.raw.createSparseEmbedding(.{
            .model = model,
            .input = inputs,
        });
        defer resp.deinit();

        if (resp.status_code < 200 or resp.status_code >= 300) {
            return error.EmbedRequestFailed;
        }

        const body = resp.body orelse return error.EmptyResponse;

        if (resp.content_type) |ct| {
            if (std.mem.startsWith(u8, ct, "application/octet-stream")) {
                return try binary.deserializeSparse(self.allocator, body);
            }
        }

        return error.UnexpectedContentType;
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

test "client module compiles" {
    _ = Client;
    _ = RawClient;
    _ = Types;
    _ = Binary;
    // Referencing the methods themselves is what forces Zig to analyse
    // their bodies; naming the type alone leaves them unchecked, which is
    // how transcribe came to read a field the response does not have.
    _ = &Client.transcribe;
    _ = &Client.listModels;
    _ = &Transcription.text;
    _ = &Transcription.segments;
}

test "transcribe returns the transcript and owns it after the response is freed" {
    const allocator = std.testing.allocator;
    const httpx = @import("httpx");
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
