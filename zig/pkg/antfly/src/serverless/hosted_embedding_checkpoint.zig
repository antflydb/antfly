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

//! Disposable computation checkpoints. Trusted coordinator metadata binds each
//! exact provider/input identity to immutable, checksum-verified R2 bytes.
//! Serving documents inline the final vectors; they never reference this cache.
const std = @import("std");
const objects = @import("objectstore");
const A = std.mem.Allocator;
const Sha = std.crypto.hash.sha2.Sha256;
const magic = "AFEC0001";
const header_bytes = 48;

pub const Context = struct {
    client: objects.Client,
    bucket: []const u8,
    prefix: []const u8,
    namespace: []const u8,
    identity: []const u8,
    day: u32,
    enabled: bool = false,
    max_provider_calls: usize = 8,
    provider_calls: usize = 0,
    hits: usize = 0,
    writes: usize = 0,
    cancellation: ?objects.CancellationToken = null,

    pub fn inputDigest(self: Context, a: A, texts: []const []const u8, dims: u32) ![32]u8 {
        const json = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .identity = self.identity, .dimensions = dims, .texts = texts }, .{});
        defer a.free(json);
        var digest: [32]u8 = undefined;
        Sha.hash(json, &digest, .{});
        return digest;
    }
    fn metadataKey(self: Context, a: A, input: [32]u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/progress/{s}/EMBEDDING_CACHE/{x:0>8}/{s}", .{ self.prefix, self.namespace, self.day, std.fmt.bytesToHex(input, .lower) });
    }
    fn payloadKey(self: Context, a: A, digest: [32]u8) ![]u8 {
        var domain: [32]u8 = undefined;
        Sha.hash(self.namespace, &domain, .{});
        return std.fmt.allocPrint(a, "{s}/embedding-cache/{s}/{x:0>8}/{s}", .{ self.prefix, std.fmt.bytesToHex(domain, .lower), self.day, std.fmt.bytesToHex(digest, .lower) });
    }
    pub fn load(self: *Context, a: A, input: [32]u8, rows: usize, dims: u32) !?[]const []const f32 {
        if (self.cancellation) |token| try token.check();
        const key = try self.metadataKey(a, input);
        defer a.free(key);
        var metadata = self.client.getObject(self.bucket, key, .{ .max_response_bytes = 1024, .cancellation = self.cancellation }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer metadata.deinit(self.client.allocator);
        const pointer = try std.json.parseFromSlice(struct { version: u8, digest: [32]u8, bytes: u32 }, a, metadata.body, .{});
        defer pointer.deinit();
        const expected = try payloadSize(rows, dims);
        if (pointer.value.version != 1 or pointer.value.bytes != expected) return error.HostedEmbeddingCheckpointInvalid;
        const payload_key = try self.payloadKey(a, pointer.value.digest);
        defer a.free(payload_key);
        var payload = self.client.getObject(self.bucket, payload_key, .{ .max_response_bytes = expected, .cancellation = self.cancellation }) catch |err| switch (err) {
            // Cache eviction at any time is safe: redo the computation.
            error.FileNotFound => return null,
            else => return err,
        };
        defer payload.deinit(self.client.allocator);
        var digest: [32]u8 = undefined;
        Sha.hash(payload.body, &digest, .{});
        if (!std.mem.eql(u8, &digest, &pointer.value.digest)) return error.HostedEmbeddingCheckpointInvalid;
        const result = try decodeAlloc(a, payload.body, input, rows, dims);
        self.hits += 1;
        return result;
    }
    pub fn save(self: *Context, a: A, input: [32]u8, vectors: []const []const f32, dims: u32) !void {
        if (self.cancellation) |token| try token.check();
        const payload = try encodeAlloc(a, input, vectors, dims);
        defer a.free(payload);
        var digest: [32]u8 = undefined;
        Sha.hash(payload, &digest, .{});
        const payload_key = try self.payloadKey(a, digest);
        defer a.free(payload_key);
        // Repeating this immutable content identity is harmless. The pointer
        // is published only after R2 confirms the complete bytes.
        var uploaded = try self.client.putObject(self.bucket, payload_key, payload, .{ .cancellation = self.cancellation });
        uploaded.deinit(self.client.allocator);
        const key = try self.metadataKey(a, input);
        defer a.free(key);
        const pointer = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .digest = digest, .bytes = payload.len }, .{});
        defer a.free(pointer);
        var committed = try self.client.putObject(self.bucket, key, pointer, .{ .cancellation = self.cancellation });
        committed.deinit(self.client.allocator);
        self.writes += 1;
    }
};

fn payloadSize(rows: usize, dims: u32) !usize {
    if (rows == 0 or rows > 64 or dims == 0 or dims > 1536) return error.HostedEmbeddingCheckpointInvalid;
    return header_bytes + rows * @as(usize, dims) * 4;
}
fn encodeAlloc(a: A, input: [32]u8, vectors: []const []const f32, dims: u32) ![]u8 {
    const bytes = try a.alloc(u8, try payloadSize(vectors.len, dims));
    errdefer a.free(bytes);
    @memcpy(bytes[0..8], magic);
    std.mem.writeInt(u32, bytes[8..12], dims, .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(vectors.len), .little);
    @memcpy(bytes[16..48], &input);
    var offset: usize = header_bytes;
    for (vectors) |vector| {
        if (vector.len != dims) return error.HostedEmbeddingCheckpointInvalid;
        for (vector) |value| {
            if (!std.math.isFinite(value)) return error.HostedEmbeddingCheckpointInvalid;
            std.mem.writeInt(u32, bytes[offset..][0..4], @bitCast(value), .little);
            offset += 4;
        }
    }
    return bytes;
}
fn decodeAlloc(a: A, bytes: []const u8, input: [32]u8, rows: usize, dims: u32) ![]const []const f32 {
    if (bytes.len != try payloadSize(rows, dims) or !std.mem.eql(u8, bytes[0..8], magic) or
        std.mem.readInt(u32, bytes[8..12], .little) != dims or std.mem.readInt(u32, bytes[12..16], .little) != rows or
        !std.mem.eql(u8, bytes[16..48], &input)) return error.HostedEmbeddingCheckpointInvalid;
    const vectors = try a.alloc([]const f32, rows);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| a.free(vector);
        a.free(vectors);
    }
    var offset: usize = header_bytes;
    for (vectors) |*vector| {
        const values = try a.alloc(f32, dims);
        vector.* = values;
        initialized += 1;
        for (values) |*value| {
            value.* = @bitCast(std.mem.readInt(u32, bytes[offset..][0..4], .little));
            if (!std.math.isFinite(value.*)) return error.HostedEmbeddingCheckpointInvalid;
            offset += 4;
        }
    }
    return vectors;
}

test "serverless embedding checkpoints authenticate shape and unwind allocation failure" {
    const Exercise = struct {
        fn run(a: A) !void {
            const input: [32]u8 = @splat(7);
            const bytes = try encodeAlloc(a, input, &.{ &.{ 1, 2 }, &.{ 3, 4 } }, 2);
            defer a.free(bytes);
            const rows = try decodeAlloc(a, bytes, input, 2, 2);
            defer {
                for (rows) |row| a.free(row);
                a.free(rows);
            }
            try std.testing.expectEqual(@as(f32, 4), rows[1][1]);
            try std.testing.expectError(error.HostedEmbeddingCheckpointInvalid, decodeAlloc(a, bytes, @splat(8), 2, 2));
            try std.testing.expectError(error.HostedEmbeddingCheckpointInvalid, decodeAlloc(a, bytes[0..47], input, 2, 2));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}
