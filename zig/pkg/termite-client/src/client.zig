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

// Termite HTTP client wrapper.
//
// Wraps the generated OpenAPI client with convenience methods and
// binary response deserialization for embeddings.

const std = @import("std");
const api = @import("termite_api");
const binary = @import("binary.zig");

pub const Binary = binary;
pub const DenseEmbeddings = binary.DenseEmbeddings;
pub const SparseEmbeddings = binary.SparseEmbeddings;
pub const SparseVector = binary.SparseVector;

/// Generated types from the Termite OpenAPI spec.
pub const Types = api.types;

/// Raw generated client — exposes every Termite API operation.
pub const RawClient = api.client.Client;

/// High-level Termite client with convenience helpers.
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

    /// List available models on the Termite server.
    pub fn listModels(self: *Client) !api.client.ApiResponse(Types.ModelsResponse) {
        return try self.raw.listModels();
    }

    /// Get server version.
    pub fn getVersion(self: *Client) !api.client.ApiResponse(Types.VersionResponse) {
        return try self.raw.getVersion();
    }
};

test "client module compiles" {
    _ = Client;
    _ = RawClient;
    _ = Types;
    _ = Binary;
}
