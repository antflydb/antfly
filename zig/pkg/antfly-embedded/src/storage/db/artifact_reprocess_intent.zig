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

//! A document-owned target generation, staged with the source mutation.
//! Deferred replay reads this instead of depending on a transient force flag.
//! One bounded record per producer remains after fulfillment; source deletion
//! retires it with the document range. Assets scope targets to configuration;
//! embeddings retain a monotonic epoch across configuration changes.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const kind = keys.asset_reprocess_intent_kind;
pub const encoded_len = 76;

pub fn keyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    return keyForKindAlloc(alloc, document, producer, kind);
}

pub fn embeddingKeyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    return keyForKindAlloc(alloc, document, producer, keys.embedding_reprocess_intent_kind);
}

/// Embedding epochs belong to the document/producer, not its configuration.
/// Resetting on a config change aliases distinct A -> B -> A executions.
/// Catalog templates deliberately omit this document-owned execution state.
pub fn requireEmbeddingGeneration(alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8, expected: u64) !void {
    const key = try embeddingKeyAlloc(alloc, document, producer);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => {
            if (expected != 0) return error.EnrichmentSourceChanged;
            return;
        },
        else => return err,
    };
    if (try generation(raw) != expected) return error.EnrichmentSourceChanged;
}

fn keyForKindAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, record_kind: u8) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    defer result.deinit(alloc);
    try keys.appendDocumentPrefix(&result, alloc, document);
    try result.append(alloc, record_kind);
    try keys.appendEncodedComponent(&result, alloc, producer);
    return result.toOwnedSlice(alloc);
}

pub fn encode(config: []const u8, target: u64) [encoded_len]u8 {
    var raw: [encoded_len]u8 = undefined;
    @memcpy(raw[0..4], "ARP1");
    std.crypto.hash.Blake3.hash(config, raw[4..36], .{});
    std.mem.writeInt(u64, raw[36..44], target, .little);
    std.crypto.hash.Blake3.hash(raw[0..44], raw[44..76], .{});
    return raw;
}

pub fn pending(raw: []const u8, config: []const u8, completed_generation: u64) !bool {
    return try generationForConfig(raw, config) > completed_generation;
}

/// Read a checked, configuration-independent embedding epoch. Keep the ARP1
/// representation compatible with existing stores; assets still use the
/// config-scoped accessor below.
pub fn generation(raw: []const u8) !u64 {
    if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "ARP1")) return error.InvalidArtifactPayload;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw[0..44], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[44..76])) return error.InvalidArtifactPayload;
    return std.mem.readInt(u64, raw[36..44], .little);
}

pub fn generationForConfig(raw: []const u8, config: []const u8) !u64 {
    const target = try generation(raw);
    var config_hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(config, &config_hash, .{});
    return if (std.mem.eql(u8, &config_hash, raw[4..36])) target else 0;
}

test "deferred reprocess intent is bounded generation and config fenced" {
    var raw = encode("producer", 2);
    try std.testing.expect(try pending(&raw, "producer", 1));
    try std.testing.expect(!try pending(&raw, "producer", 2));
    try std.testing.expect(!try pending(&raw, "replacement", 1));
    try std.testing.expectEqual(@as(u64, 2), try generationForConfig(&raw, "producer"));
    try std.testing.expectEqual(@as(u64, 0), try generationForConfig(&raw, "replacement"));
    raw[36] ^= 1;
    try std.testing.expectError(error.InvalidArtifactPayload, pending(&raw, "producer", 1));
    const alloc = std.testing.allocator;
    const key = try keyAlloc(alloc, "doc\x00\xff", "producer\x00");
    defer alloc.free(key);
    const owner = (try keys.decodeDocumentComponentAlloc(alloc, key)).?;
    defer alloc.free(owner);
    try std.testing.expectEqualStrings("doc\x00\xff", owner);
    const embedding_key = try embeddingKeyAlloc(alloc, "doc\x00\xff", "producer\x00");
    defer alloc.free(embedding_key);
    try std.testing.expect(!std.mem.eql(u8, key, embedding_key));
    try std.testing.expect(keys.isRestoreArtifactKey(embedding_key));
}

test "embedding epochs fence zero and repeated model incarnations without resetting" {
    const a = std.testing.allocator;
    const Read = struct {
        raw: ?[]const u8 = null,
        pub fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.raw orelse error.NotFound;
        }
    };
    var read = Read{};
    try requireEmbeddingGeneration(a, &read, "doc", "vectors", 0);
    try std.testing.expectError(error.EnrichmentSourceChanged, requireEmbeddingGeneration(a, &read, "doc", "vectors", 1));
    const b = encode("B", 2);
    read.raw = &b;
    try std.testing.expectEqual(@as(u64, 2), try generation(&b));
    try requireEmbeddingGeneration(a, &read, "doc", "vectors", 2);
    // Config-scoped asset lookup must not be used as an embedding epoch.
    try std.testing.expectEqual(@as(u64, 0), try generationForConfig(&b, "A"));
    try std.testing.expectError(error.EnrichmentSourceChanged, requireEmbeddingGeneration(a, &read, "doc", "vectors", 0));
    const new_a = encode("A", 3);
    read.raw = &new_a;
    try std.testing.expectError(error.EnrichmentSourceChanged, requireEmbeddingGeneration(a, &read, "doc", "vectors", 1));
    try requireEmbeddingGeneration(a, &read, "doc", "vectors", 3);
    var corrupt = new_a;
    corrupt[36] ^= 1;
    read.raw = &corrupt;
    try std.testing.expectError(error.InvalidArtifactPayload, requireEmbeddingGeneration(a, &read, "doc", "vectors", 3));
}
