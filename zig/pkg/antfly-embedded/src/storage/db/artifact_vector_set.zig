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
/// Ordered scalar replacement retires child vectors atomically. A present
/// scalar therefore selects the scalar topology; parent receipts describe
/// boundary work and cannot veto independently accepted text computation.
/// Callers still authenticate every selected vector's current causal proof.
pub fn storedIndices(a: std.mem.Allocator, txn: anytype, member: []const u8, embedding: []const u8, payload: []const u8) ![]u32 {
    const scalar = try keys.derivedEmbeddingArtifactKeyAlloc(a, member, embedding);
    defer a.free(scalar);
    const exists = txn.get(scalar) catch |err| if (err == error.NotFound) null else return err;
    if (exists != null) return a.dupe(u32, &.{0});
    return indices(a, payload);
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
        try publication.stageArtifactRevisions(a, &write, command, .{ .raft = .{ .term = 1, .index = 1 } });
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
        try publication.stageArtifactRevisions(a, &write, command, .{ .raft = .{ .term = 1, .index = 2 } });
        try write.commit();
    }
    var read = try erased.beginProbe();
    defer read.abort();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(a, &read, command.namespace, &.{.{ .document_key = "doc", .timestamp = 1, .content_digest = @splat(1), .input_position = null }}, &.{guard}));
}

test "model aware media stored vector selection ignores obsolete parent topology but requires a complete fused set otherwise" {
    const a = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    var erased = try @import("../backend_erased.zig").storeFrom(a, store);
    defer erased.deinit();
    const member = try keys.chunkArtifactKeyAlloc(a, "doc", "clips", 0);
    defer a.free(member);
    const scalar = try keys.derivedEmbeddingArtifactKeyAlloc(a, member, "vectors");
    defer a.free(scalar);
    const O = @import("inference_chunker").outputs;
    const failed = O.Result{ .name = "embedding", .kind = .embedding, .required = true, .model = "model", .model_identity = "", .model_revision = "", .recipe_id = "", .status = .failed, .terminal = true, .code = "OldFailure" };
    const encoded = try std.json.Stringify.valueAlloc(a, [_]O.Result{failed}, .{});
    defer a.free(encoded);
    var metadata = @import("inference_chunker").types.ModelMetadata{ .descriptor = .{ .ordinal = 0, .coordinates = .{ .kind = .text }, .input_tokens = 1 }, .inference_id = "plan:0", .plan_id = "plan", .source_fingerprint = "source", .source_version = "v1", .model = "model", .model_identity = "", .model_revision = "", .task_type = "RETRIEVAL_DOCUMENT", .dimensions = 2, .policy = .{}, .outputs_json = encoded };
    const payload = try std.json.Stringify.valueAlloc(a, .{ .body = "retained text", ._chunk = metadata }, .{});
    defer a.free(payload);
    {
        var txn = try erased.beginWrite();
        errdefer txn.abort();
        try txn.put(scalar, "accepted vector");
        try txn.commit();
    }
    {
        var read = try erased.beginProbe();
        defer read.abort();
        const selected = try storedIndices(a, &read, member, "vectors", payload);
        defer a.free(selected);
        try std.testing.expectEqualSlices(u32, &.{0}, selected);
        try std.testing.expectError(error.RequiredMediaOutputFailed, storedIndices(a, &read, member, "other", payload));
    }
    var windows = [_]O.Result{ failed, failed };
    for (&windows, 0..) |*value, i| {
        value.status = .complete;
        value.terminal = false;
        value.code = null;
        value.sub_index = @intCast(i);
    }
    const raw = try std.json.Stringify.valueAlloc(a, windows, .{});
    defer a.free(raw);
    metadata.outputs_json = raw;
    const split = try std.json.Stringify.valueAlloc(a, .{ .body = "retained text", ._chunk = metadata }, .{});
    defer a.free(split);
    var read = try erased.beginProbe();
    defer read.abort();
    const independent = try storedIndices(a, &read, member, "vectors", split);
    defer a.free(independent);
    try std.testing.expectEqualSlices(u32, &.{0}, independent);
    const fused = try storedIndices(a, &read, member, "other", split);
    defer a.free(fused);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, fused);
}

test "model aware media vector revision staging reuses bounded caller scratch and aborts on exhaustion" {
    const a = std.testing.allocator;
    const publication = @import("artifact_publication.zig");
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "bounded-revisions" });
    defer store.deinit();
    var erased = try @import("../backend_erased.zig").storeFrom(a, store);
    defer erased.deinit();
    var mutations: [64]publication.Mutation = undefined;
    var initialized: usize = 0;
    defer for (mutations[0..initialized]) |mutation| a.free(mutation.key);
    for (&mutations, 0..) |*mutation, i| {
        var document: [64]u8 = undefined;
        const parent = try keys.chunkArtifactKeyAlloc(a, try std.fmt.bufPrint(&document, "allocator-document-{d}", .{i}), "clips", @intCast(i));
        defer a.free(parent);
        const key = try keys.derivedEmbeddingArtifactKeyAlloc(a, parent, "model");
        mutation.* = .{ .family = .derived_vector, .key = key, .value = "vector", .source_index = 0 };
        initialized += 1;
    }
    const command = publication.Command{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "model", .producer_generation = 1, .producer_artifact_name = "model", .sources = &.{}, .mutations = &mutations, .publication_digest = @splat(0) };
    const position: publication.Position = .{ .raft = .{ .term = 1, .index = 1 } };
    {
        var bytes: [1]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&bytes);
        var write = try erased.beginWrite();
        defer write.abort();
        try std.testing.expectError(error.OutOfMemory, publication.stageArtifactRevisions(bounded.allocator(), &write, command, position));
    }
    {
        var read = try erased.beginProbe();
        defer read.abort();
        try std.testing.expect((try publication.artifactRevision(&read, command.namespace, mutations[0].key)) == null);
    }
    // All 64 distinct witnesses fit one small reusable region. Keeping every
    // parser allocation until batch completion would exceed this fixed budget.
    var bytes: [2048]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bytes);
    {
        var write = try erased.beginWrite();
        errdefer write.abort();
        try publication.stageArtifactRevisions(bounded.allocator(), &write, command, position);
        try write.commit();
    }
    var read = try erased.beginProbe();
    defer read.abort();
    for (mutations) |mutation| {
        const witness = (try forArtifactAlloc(a, mutation.key)).?;
        defer a.free(witness);
        const actual = (try publication.artifactRevision(&read, command.namespace, witness)).?;
        try std.testing.expectEqual(@as(u64, 1), actual.raft.index);
    }
}
