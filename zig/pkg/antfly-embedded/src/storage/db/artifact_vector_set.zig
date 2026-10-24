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

//! Document/embedding-set revision. Every ordered vector publication advances
//! this witness atomically, so a consumer can fence an arbitrarily large set
//! without retaining one read guard per vector. It never replaces provenance.
const std = @import("std");
const keys = @import("../internal_keys.zig");
pub const suffix: u8 = 0x55;
pub fn keyAlloc(a: std.mem.Allocator, document: []const u8, embedding: []const u8) ![]u8 {
    const base = try keys.artifactNamedPrefixAlloc(a, document, "embedding", embedding);
    defer a.free(base);
    const out = try a.alloc(u8, base.len + 1);
    @memcpy(out[0..base.len], base);
    out[base.len] = suffix;
    return out;
}
pub fn isKey(key: []const u8) bool {
    return key.len > 1 and key[key.len - 1] == suffix and keys.isEmbeddingArtifactKey(key[0 .. key.len - 1]);
}
pub fn forArtifactAlloc(a: std.mem.Allocator, artifact: []const u8) !?[]u8 {
    if (!keys.isDerivedEmbeddingArtifactKey(artifact)) return null;
    var ref = (try @import("artifact_ids.zig").decodeArtifactRefAlloc(a, artifact)) orelse return null;
    defer ref.deinit(a);
    return try keyAlloc(a, ref.document_id, ref.name);
}
pub fn update(hash: *std.crypto.hash.sha2.Sha256, key: []const u8, value: []const u8) void {
    for ([_][]const u8{ key, value }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hash.update(&length);
        hash.update(part);
    }
}
pub fn indices(a: std.mem.Allocator, payload: []const u8) ![]u32 {
    const metadata = try @import("../../chunking/chunk.zig").modelMetadataAlloc(a, payload);
    defer if (metadata) |value| value.deinit(a);
    if (metadata) |value| if (value.outputs_json) |raw| {
        var parsed = try std.json.parseFromSlice([]@import("inference_chunker").outputs.Result, a, raw, .{});
        defer parsed.deinit();
        return indicesFromResults(a, parsed.value);
    };
    return indicesFromResults(a, &.{});
}
pub fn indicesFromResults(a: std.mem.Allocator, results: []const @import("inference_chunker").outputs.Result) ![]u32 {
    var result = std.ArrayList(u32).empty;
    errdefer result.deinit(a);
    for (results) |receipt| if (receipt.kind == .embedding and std.mem.eql(u8, receipt.name, "embedding")) {
        if (receipt.status != .complete) return if (receipt.status == .failed and receipt.terminal) error.RequiredMediaOutputFailed else error.ArtifactPublicationPending;
        try result.append(a, receipt.sub_index);
    };
    if (result.items.len == 0) try result.append(a, 0);
    std.mem.sort(u32, result.items, {}, std.sort.asc(u32));
    for (result.items, 0..) |value, i| if (value != i) return error.InvalidChunkOutput;
    return result.toOwnedSlice(a);
}

test "model aware media vector set guard advances atomically with any child vector" {
    const a = std.testing.allocator;
    const publication = @import("artifact_publication.zig");
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    var erased = try @import("../backend_erased.zig").storeFrom(a, store);
    defer erased.deinit();
    const parent = try keys.chunkArtifactKeyAlloc(a, "doc", "clips", 7);
    defer a.free(parent);
    const child = try keys.mediaWindowChildKeyAlloc(a, parent, 1);
    defer a.free(child);
    const vector = try keys.derivedEmbeddingArtifactKeyAlloc(a, child, "model");
    defer a.free(vector);
    const witness = try keyAlloc(a, "doc", "model");
    defer a.free(witness);
    const command = publication.Command{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "model", .producer_generation = 1, .producer_artifact_name = "model", .sources = &.{}, .mutations = &.{.{ .family = .derived_vector, .key = vector, .value = "first", .source_index = 0 }}, .publication_digest = @splat(0) };
    {
        var write = try erased.beginWrite();
        errdefer write.abort();
        try publication.stageArtifactRevisions(&write, command, .{ .raft = .{ .term = 1, .index = 1 } });
        try write.commit();
    }
    var guard: publication.ArtifactSource = undefined;
    {
        var read = try erased.beginProbe();
        defer read.abort();
        const raw = try read.get(witness);
        guard = .{ .key = witness, .content_digest = undefined, .input_position = try publication.artifactRevision(&read, command.namespace, witness), .source_index = 0 };
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
        guard.content_digest = digest;
        try publication.validateArtifactSources(a, &read, command.namespace, &.{.{ .document_key = "doc", .timestamp = 1, .content_digest = @splat(1), .input_position = null }}, &.{guard});
    }
    {
        var write = try erased.beginWrite();
        errdefer write.abort();
        try publication.stageArtifactRevisions(&write, command, .{ .raft = .{ .term = 1, .index = 2 } });
        try write.commit();
    }
    var read = try erased.beginProbe();
    defer read.abort();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(a, &read, command.namespace, &.{.{ .document_key = "doc", .timestamp = 1, .content_digest = @splat(1), .input_position = null }}, &.{guard}));
}
