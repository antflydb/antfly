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
        const body = resp.body orelse return error.EmptyResponse;
        const parsed = try std.json.parseFromSlice(api.TranscribeResponse, self.allocator, body, .{
            .ignore_unknown_fields = true,
        });
        errdefer parsed.deinit();
        if (parsed.value.data.len == 0) return error.EmptyResponse;
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
}
