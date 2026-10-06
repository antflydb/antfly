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

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const internal_keys = @import("../internal_keys.zig");
const docstore_mod = @import("../docstore.zig");
const chunk_artifact_mod = @import("../../chunking/chunk.zig");
pub const ChunkEmbeddingSource = struct { key: []u8, text: []const u8 };
pub fn freeChunkEmbeddingSources(alloc: Allocator, sources: []const ChunkEmbeddingSource) void {
    for (sources) |source| {
        alloc.free(source.key);
        alloc.free(source.text);
    }
    if (sources.len > 0) alloc.free(sources);
}

pub fn clearChunkEmbeddingSourceList(alloc: Allocator, sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource)) void {
    for (sources.items) |source| {
        alloc.free(source.key);
        alloc.free(source.text);
    }
    sources.clearRetainingCapacity();
}

pub fn chunkPayloadTextAlloc(alloc: Allocator, payload: []const u8, source_field: []const u8) !?[]u8 {
    return try chunk_artifact_mod.artifactTextAlloc(alloc, payload, source_field);
}

pub fn collectChunkEmbeddingSourcesFromWrites(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
    seen: *std.StringHashMapUnmanaged(void),
    writes: []const types.BatchWrite,
    doc_key: []const u8,
    artifact_name: []const u8,
    source_field: []const u8,
) !void {
    const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "chunk", artifact_name);
    defer alloc.free(prefix);
    for (writes) |write| {
        if (!std.mem.startsWith(u8, write.key, prefix) or
            !internal_keys.matchesChunkArtifactName(write.key, artifact_name)) continue;
        if (seen.contains(write.key)) continue;
        try appendPayload(alloc, out, seen, write.key, write.value, source_field);
    }
}

pub fn collectChunkEmbeddingSourcesFromStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    out: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
    seen: *std.StringHashMapUnmanaged(void),
    doc_key: []const u8,
    artifact_name: []const u8,
    source_field: []const u8,
    pending_writes: anytype,
    pending_deletes: *const std.StringHashMapUnmanaged(void),
) !void {
    const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "chunk", artifact_name);
    defer alloc.free(prefix);
    const existing = try store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, existing);

    for (existing) |entry| {
        if (!internal_keys.isChunkArtifactRecordKey(entry.key)) continue;
        if (seen.contains(entry.key)) continue;
        if (pending_writes.get(entry.key) != null) continue;
        if (pending_deletes.contains(entry.key)) continue;
        try appendPayload(alloc, out, seen, entry.key, entry.value, source_field);
    }
}

fn appendPayload(alloc: Allocator, out: *std.ArrayListUnmanaged(ChunkEmbeddingSource), seen: *std.StringHashMapUnmanaged(void), key: []const u8, payload: []const u8, source_field: []const u8) !void {
    const text = (try chunkPayloadTextAlloc(alloc, payload, source_field)) orelse return;
    errdefer alloc.free(text);
    try out.ensureUnusedCapacity(alloc, 1);
    try seen.ensureUnusedCapacity(alloc, 1);
    const owned_key = try alloc.dupe(u8, key);
    out.appendAssumeCapacity(.{ .key = owned_key, .text = text });
    seen.putAssumeCapacity(owned_key, {});
}

test "materialized sources keep first pending row and release growth failures" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var sources: std.ArrayListUnmanaged(ChunkEmbeddingSource) = .empty;
            defer {
                clearChunkEmbeddingSourceList(alloc, &sources);
                sources.deinit(alloc);
            }
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            defer seen.deinit(alloc);
            for (0..16) |i| {
                const key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", @intCast(i));
                defer alloc.free(key);
                try collectChunkEmbeddingSourcesFromWrites(alloc, &sources, &seen, &.{ .{ .key = key, .value = "{\"text\":\"first\"}" }, .{ .key = key, .value = "{\"text\":\"later\"}" } }, "doc", "chunks", "text");
            }
            try std.testing.expectEqual(@as(usize, 16), sources.items.len);
            try std.testing.expectEqualStrings("first", sources.items[0].text);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}
