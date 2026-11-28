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
const receipt_len = 53;

pub const RetryReason = enum(u8) { provider_capacity = 1, provider_retry_after = 2, transient = 3 };
pub const RetryObservation = struct {
    retry_at_ms: u64,
    reason: RetryReason,
};

/// The companion preserves the existing ARP1 reader during a rolling upgrade.
/// It describes a replay sequence in the accepting store's journal.
pub fn receiptKeyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    return keyForKindAlloc(alloc, document, producer, keys.asset_reprocess_receipt_kind);
}

pub fn newReceipt(intent: *const [encoded_len]u8) [receipt_len]u8 {
    return "ARR1".* ++ intent[44..76].* ++ @as([17]u8, @splat(0));
}

pub fn stampReceipt(raw: []u8, sequence: u64) !void {
    if (raw.len != receipt_len or !std.mem.eql(u8, raw[0..4], "ARR1") or sequence == 0)
        return error.InvalidArtifactPayload;
    std.mem.writeInt(u64, raw[36..44], sequence, .little);
}

pub fn receiptSequence(raw: []const u8, checked_intent: []const u8) ?u64 {
    if (raw.len != receipt_len or checked_intent.len != encoded_len or
        !std.mem.eql(u8, raw[0..4], "ARR1") or
        !std.mem.eql(u8, raw[4..36], checked_intent[44..76])) return null;
    const sequence = std.mem.readInt(u64, raw[36..44], .little);
    return if (sequence == 0) null else sequence;
}

pub fn observeRetry(raw: []const u8) ?RetryObservation {
    if (raw.len != receipt_len or !std.mem.eql(u8, raw[0..4], "ARR1")) return null;
    const reason = std.enums.fromInt(RetryReason, raw[52]) orelse return null;
    const retry_at_ms = std.mem.readInt(u64, raw[44..52], .little);
    if (retry_at_ms == 0) return null;
    return .{ .reason = reason, .retry_at_ms = retry_at_ms };
}

pub fn withRetry(raw: []const u8, accepted_sequence: u64, retry: RetryObservation) ?[receipt_len]u8 {
    if (raw.len != receipt_len or !std.mem.eql(u8, raw[0..4], "ARR1") or
        std.mem.readInt(u64, raw[36..44], .little) != accepted_sequence) return null;
    var revised = raw[0..receipt_len].*;
    std.mem.writeInt(u64, revised[44..52], retry.retry_at_ms, .little);
    revised[52] = @backingInt(retry.reason);
    return revised;
}

pub fn isPublished(accepted: u64, target: u64, published: u64, published_generation: u64) bool {
    return published == accepted and published_generation >= target;
}

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

test "extraction receipt requires its own publication witness even when another recipe reached its target" {
    const target = encode("recipe A", 3);
    var receipt = newReceipt(&target);
    try stampReceipt(&receipt, 90);
    try std.testing.expectEqual(@as(?u64, 90), receiptSequence(&receipt, &target));
    try std.testing.expectEqual(@as(?u64, null), receiptSequence(&receipt, &encode("recipe B", 3)));
    try std.testing.expect(!isPublished(90, 3, 0, 3));
    try std.testing.expect(!isPublished(90, 3, 91, 4));
    try std.testing.expect(isPublished(90, 3, 90, 3));
    const retry = RetryObservation{ .retry_at_ms = 200, .reason = .provider_retry_after };
    try std.testing.expect(withRetry(&receipt, 89, retry) == null);
    const deferred = withRetry(&receipt, 90, retry).?;
    try std.testing.expectEqual(@as(?u64, 90), receiptSequence(&deferred, &target));
    try std.testing.expectEqual(retry, observeRetry(&deferred).?);
}
