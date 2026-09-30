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

//! The online direct-field family carries only document-owned vector values.
//! Derived/chunk, graph, asset and producer-job namespaces are deliberately
//! excluded: their publication and external payload lifetimes need other proof.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const codec = @import("enrichment/artifact_codec.zig");

// Ordered protocol v14/v20 certifies document-owned base vector capture,
// independently of whether an active index currently consumes those values.
pub const enabled = true;
pub fn isEnabled() bool {
    return enabled;
}

pub fn isKey(key: []const u8) bool {
    return keys.isEmbeddingArtifactKey(key);
}

/// Allocation-free, endian/alignment independent validation at both retained
/// frame and receiver boundaries. Tombstones carry exactly the same key class.
pub fn validate(key: []const u8, value: ?[]const u8) !void {
    if (!isKey(key)) return error.InvalidMergePage;
    try validatePayload(value);
}

/// Shared physical vector validation; callers must separately authenticate
/// the key family and producer scope before accepting derived outputs.
pub fn validatePayload(value: ?[]const u8) !void {
    const bytes = value orelse return;
    const header = codec.decodeHeader(bytes) catch return error.InvalidMergePage;
    if (header.flags.has_graph_generation or header.flags.portable_unbound_graph_generation or header.flags._reserved != 0 or header.payload_len < 4)
        return error.InvalidMergePage;
    const payload = bytes[codec.header_len..];
    const count: usize = std.mem.readInt(u32, payload[0..4], .little);
    const stride: usize = switch (header.kind) {
        .dense_embedding => 4,
        .sparse_embedding => 8,
        else => return error.InvalidMergePage,
    };
    if (count != (payload.len - 4) / stride or (payload.len - 4) % stride != 0) return error.InvalidMergePage;
    var values_offset: usize = 4;
    if (header.kind == .sparse_embedding) {
        // Native sparse writes accept caller order and repeated dimensions.
        // Transfer preserves those bytes and their existing projection
        // semantics instead of imposing a new canonical input restriction.
        values_offset += 4 * count;
    }
    for (0..count) |i| {
        const number: f32 = @bitCast(std.mem.readInt(u32, payload[values_offset + 4 * i ..][0..4], .little));
        if (!std.math.isFinite(number)) return error.InvalidMergePage;
    }
}

test "online direct vector artifacts validate exact bounded values and tombstones" {
    const alloc = std.testing.allocator;
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "vector");
    defer alloc.free(key);
    const dense = try codec.encodeDenseEmbeddingAlloc(alloc, 9, &.{ 1, 2 });
    defer alloc.free(dense);
    try validate(key, dense);
    try validate(key, null);
    try std.testing.expectError(error.InvalidMergePage, validate(key, dense[0 .. dense.len - 1]));
    const sparse = try codec.encodeSparseEmbeddingAlloc(alloc, 10, &.{ 2, 7 }, &.{ 1, 3 });
    defer alloc.free(sparse);
    try validate(key, sparse);
    std.mem.writeInt(u32, sparse[codec.header_len + 8 ..][0..4], 2, .little);
    try validate(key, sparse);
    std.mem.writeInt(u32, sparse[codec.header_len + 4 ..][0..4], 9, .little);
    try validate(key, sparse);
    std.mem.writeInt(u32, dense[dense.len - 4 ..][0..4], @bitCast(std.math.inf(f32)), .little);
    try std.testing.expectError(error.InvalidMergePage, validate(key, dense));
    const chunk = try keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 1);
    defer alloc.free(chunk);
    const derived = try keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk, "vector");
    defer alloc.free(derived);
    try std.testing.expectError(error.InvalidMergePage, validate(derived, null));
}
