// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Opaque retained-cut envelopes bind the exact provider version without
//! changing legacy content identities. Legacy filesystem tokens still decode.
const std = @import("std");
const stores = @import("../serverless/artifacts/store.zig");
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Envelope = struct { id: []const u8, bytes: usize, generation: ?[]u8 = null };
pub fn artifactId(prefix: []const u8, token: []const u8) ![]const u8 {
    if (!std.mem.startsWith(u8, token, prefix)) return error.InvalidQueryRequest;
    const end = std.mem.lastIndexOfScalar(u8, token, ':') orelse return error.InvalidQueryRequest;
    const body = token[prefix.len..end];
    return if (std.mem.indexOf(u8, body, ":g:")) |split| body[0..split] else body;
}
pub fn encode(a: A, prefix: []const u8, metadata: stores.ArtifactMetadata) ![]u8 {
    const version = metadata.provider_version orelse return std.fmt.allocPrint(a, "{s}{s}:{d}", .{ prefix, metadata.artifact_id, metadata.byte_len });
    if (version.len == 0 or version.len > @import("antfly_local_sources").serverless_manifest_artifact_ref.max_provider_generation_bytes or std.mem.eql(u8, version, "null")) return error.InvalidQueryRequest;
    const base64 = std.base64.url_safe_no_pad;
    const encoded = try a.alloc(u8, base64.Encoder.calcSize(version.len));
    defer a.free(encoded);
    _ = base64.Encoder.encode(encoded, version);
    return std.fmt.allocPrint(a, "{s}{s}:g:{s}:{d}", .{ prefix, metadata.artifact_id, encoded, metadata.byte_len });
}
pub fn decode(a: A, prefix: []const u8, token: []const u8, limit: usize) !Envelope {
    const id = try artifactId(prefix, token);
    _ = try stores.sha256ChecksumFromArtifactId(id);
    const end = std.mem.lastIndexOfScalar(u8, token, ':').?;
    const length = std.fmt.parseUnsigned(usize, token[end + 1 ..], 10) catch return error.InvalidQueryRequest;
    if (length == 0 or length > limit) return error.InvalidQueryRequest;
    const suffix = token[prefix.len + id.len .. end];
    if (suffix.len == 0) return .{ .id = id, .bytes = length };
    if (!std.mem.startsWith(u8, suffix, ":g:")) return error.InvalidQueryRequest;
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const size = decoder.calcSizeForSlice(suffix[3..]) catch return error.InvalidQueryRequest;
    if (size == 0 or size > @import("antfly_local_sources").serverless_manifest_artifact_ref.max_provider_generation_bytes) return error.InvalidQueryRequest;
    const generation = try a.alloc(u8, size);
    errdefer a.free(generation);
    decoder.decode(generation, suffix[3..]) catch return error.InvalidQueryRequest;
    if (std.mem.eql(u8, generation, "null")) return error.InvalidQueryRequest;
    return .{ .id = id, .bytes = length, .generation = generation };
}
pub fn read(store: *stores.ArtifactStore, envelope: Envelope, token: Token) ![]u8 {
    return readFor(store, envelope, token, .{});
}
pub fn readFor(store: *stores.ArtifactStore, envelope: Envelope, token: Token, context: stores.ReadContext) ![]u8 {
    try context.ensureActive();
    if (envelope.generation) |generation| {
        const object = store.providerStore() orelse return error.CloudGenerationStoreUnavailable;
        const suffix = try stores.storageSuffixAlloc(store.allocator, envelope.id);
        defer store.allocator.free(suffix);
        const path = try std.fmt.allocPrint(store.allocator, "{s}{s}{s}", .{ object.prefix, if (object.prefix.len == 0) "" else "/", suffix });
        defer store.allocator.free(path);
        var client = object.client;
        client.allocator = store.allocator;
        const options: @import("objectstore").GetOptions = .{ .version_id = generation, .skip_metadata_probe = true, .max_response_bytes = envelope.bytes, .cancellation = @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) };
        var result = try @import("antfly_local_sources").sql_execution_wait.runOwned(context.wait, @import("objectstore").Client.getObject, .{ &client, object.bucket, path, options }, discardGet, .{store.allocator});
        defer result.deinit(store.allocator);
        if (result.body.len != envelope.bytes) return error.ArtifactIntegrityMismatch;
        try context.ensureActive();
        try stores.validatePayloadSha256For(result.body, try stores.sha256ChecksumFromArtifactId(envelope.id), token, context);
        const body = result.body;
        result.body = &.{};
        return body;
    }
    return store.getVerifiedAllocFor(store.allocator, envelope.id, envelope.bytes, try stores.sha256ChecksumFromArtifactId(envelope.id), token, context);
}
test "external lake global frontier retained envelopes preserve exact cloud selectors and legacy identities" {
    const a = std.testing.allocator;
    const scope: stores.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const id = try scope.artifactId(&(@as([64]u8, @splat('a'))));
    const metadata: stores.ArtifactMetadata = .{ .artifact_id = @constCast(&id), .checksum = @constCast(id[7..71]), .byte_len = 42, .provider_version = @constCast("version/+one") };
    const token = try encode(a, "lake2:", metadata);
    defer a.free(token);
    const envelope = try decode(a, "lake2:", token, 42);
    defer a.free(envelope.generation.?);
    try std.testing.expectEqualStrings(&id, envelope.id);
    try std.testing.expectEqualStrings("version/+one", envelope.generation.?);
    try std.testing.expectEqualStrings(&id, try artifactId("lake2:", token));
    var legacy = metadata;
    legacy.provider_version = null;
    const old = try encode(a, "lake2:", legacy);
    defer a.free(old);
    try std.testing.expect((try decode(a, "lake2:", old, 42)).generation == null);
    try std.testing.expectError(error.InvalidQueryRequest, decode(a, "lake2:", token, 41));
}

fn discardGet(value: @import("objectstore").GetResult, a: A) void {
    var result = value;
    result.deinit(a);
}
