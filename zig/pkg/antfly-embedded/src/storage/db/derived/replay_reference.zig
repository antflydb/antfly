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

//! Hint projections reference the one canonical payload in the same primary
//! transaction. Resolution always uses the reference reader's captured epoch;
//! retirement removes every reference before removing its canonical record.
const std = @import("std");
const Allocator = std.mem.Allocator;
const journal = @import("change_journal.zig");
const keys = @import("../../internal_keys.zig");
const chunks = @import("replay_chunks.zig");
const magic = "AFRREF01";
pub const reference_size = 21;

pub fn encodeReference(sequence: u64, hint: journal.TargetHint) [reference_size]u8 {
    var raw: [reference_size]u8 = undefined;
    @memcpy(raw[0..8], magic);
    std.mem.writeInt(u64, raw[8..16], sequence, .little);
    raw[16] = @intCast(@backingInt(hint));
    std.mem.writeInt(u32, raw[17..21], std.hash.Crc32.hash(raw[0..17]), .little);
    return raw;
}

pub fn isReference(raw: []const u8) bool {
    return std.mem.startsWith(u8, raw, magic);
}

/// Borrow the canonical encoding through the caller's captured transaction.
/// The descriptor binds sequence and hint, including before seekable reads.
pub fn canonicalRaw(txn: anytype, sequence: u64, lane: u8, raw: []const u8) ![]const u8 {
    if (!isReference(raw)) return raw;
    if (raw.len != reference_size or lane > 7 or raw[16] != lane or
        std.mem.readInt(u64, raw[8..16], .little) != sequence or
        std.mem.readInt(u32, raw[17..21], .little) != std.hash.Crc32.hash(raw[0..17])) return error.InvalidReplayReference;
    const key = keys.replayEntryKey(keys.replay_all_kind, sequence);
    const canonical = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return error.InvalidReplayReference,
        else => return err,
    };
    if (isReference(canonical)) return error.InvalidReplayReference;
    return canonical;
}

/// Null preserves the legacy inline lane payload. A malformed or dangling
/// reference is a recovery error, never an empty lane or a skipped record.
pub fn resolve(a: Allocator, txn: anytype, sequence: u64, lane: u8, raw: []const u8) !?[]u8 {
    if (chunks.isManifest(raw)) {
        if (lane != keys.replay_all_kind or (try chunks.Manifest.decode(raw)).sequence != sequence) return error.InvalidReplayReference;
        return try chunks.resolveAll(a, txn, raw);
    }
    if (!isReference(raw)) return null;
    if (raw.len != reference_size or lane > 7 or raw[16] != lane or
        std.mem.readInt(u64, raw[8..16], .little) != sequence or
        std.mem.readInt(u32, raw[17..21], .little) != std.hash.Crc32.hash(raw[0..17]))
        return error.InvalidReplayReference;
    const canonical_key = keys.replayEntryKey(keys.replay_all_kind, sequence);
    const canonical = txn.get(&canonical_key) catch |err| switch (err) {
        error.NotFound => return error.InvalidReplayReference,
        else => return err,
    };
    if (isReference(canonical)) return error.InvalidReplayReference;
    const hint: journal.TargetHint = @fromBackingInt(@intCast(lane));
    const canonical_payload = if (chunks.isManifest(canonical)) try chunks.resolveAll(a, txn, canonical) else null;
    defer if (canonical_payload) |owned| a.free(owned);
    const encoded = canonical_payload orelse canonical;
    var decoded = try journal.decodeRecord(a, encoded);
    defer decoded.deinit();
    if (decoded.record.sequence != sequence or !try journal.encodedRecordHasHint(encoded, hint))
        return error.InvalidReplayReference;
    return try encodeForHint(a, decoded.record, hint);
}

fn isEmbeddingReplayArtifactKey(key: []const u8) bool {
    return keys.isEmbeddingArtifactKey(key) or keys.isDerivedEmbeddingArtifactKey(key);
}

fn appendReplayArtifactsForHint(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    artifact_keys: []const []const u8,
    hint: journal.TargetHint,
) !void {
    for (artifact_keys) |key| {
        const keep = switch (hint) {
            .dense_vector, .sparse_vector => isEmbeddingReplayArtifactKey(key),
            .graph => keys.isGraphEdgeArtifactKey(key) or
                keys.isAssetArtifactKey(key) or
                keys.isResolutionArtifactKey(key),
            .resolution => keys.isAssetArtifactKey(key),
            .promotion => keys.isResolutionArtifactKey(key),
            .enrichment, .full_text, .algebraic => false,
        };
        if (keep) try out.append(alloc, key);
    }
}

pub fn encodeForHint(
    alloc: Allocator,
    record: journal.Record,
    hint: journal.TargetHint,
) ![]u8 {
    var target_hints = [_]journal.TargetHint{hint};
    var artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer artifact_keys.deinit(alloc);
    try appendReplayArtifactsForHint(alloc, &artifact_keys, record.changed_artifact_keys, hint);

    var filtered = journal.Record{
        .version = record.version,
        .sequence = record.sequence,
        .target_hints = target_hints[0..],
    };
    switch (hint) {
        .enrichment => {
            filtered.changed_doc_keys = record.changed_doc_keys;
        },
        .full_text, .algebraic => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
        },
        .dense_vector, .sparse_vector => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .graph, .resolution, .promotion => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
    }
    return try journal.encodeRecord(alloc, filtered);
}

/// Bounded retirement can rotate away from a partially deleted lane. The
/// canonical row remains charged until every referring lane is physically gone.
pub fn canonicalMayRetire(txn: anytype, sequence: u64) !bool {
    for (0..8) |lane| {
        const key = keys.replayEntryKey(@intCast(lane), sequence);
        const raw = txn.get(&key) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        if (isReference(raw)) return false;
    }
    return true;
}

test "replay references validate sequence lane checksum and canonical ownership" {
    const a = std.testing.allocator;
    const canonical = try journal.encodeRecord(a, .{ .sequence = 11, .changed_doc_keys = &.{ "你好", "nul\x00key" }, .deleted_doc_keys = &.{"gone"}, .target_hints = &.{ .full_text, .enrichment } });
    defer a.free(canonical);
    const Fixture = struct {
        canonical: ?[]const u8,
        reference: ?[]const u8 = null,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (std.mem.eql(u8, key, &keys.replayEntryKey(keys.replay_all_kind, 11))) return self.canonical orelse error.NotFound;
            if (std.mem.eql(u8, key, &keys.replayEntryKey(1, 11))) return self.reference orelse error.NotFound;
            return error.NotFound;
        }
    };
    var fixture: Fixture = .{ .canonical = canonical };
    const reference = encodeReference(11, .enrichment);
    const projected = (try resolve(a, &fixture, 11, 0, &reference)).?;
    defer a.free(projected);
    var decoded = try journal.decodeRecord(a, projected);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 2), decoded.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.record.deleted_doc_keys.len);
    try std.testing.expectEqualStrings("nul\x00key", decoded.record.changed_doc_keys[1]);
    try std.testing.expectError(error.InvalidReplayReference, resolve(a, &fixture, 12, 0, &reference));
    try std.testing.expectError(error.InvalidReplayReference, resolve(a, &fixture, 11, 1, &reference));
    var corrupt = reference;
    corrupt[18] ^= 1;
    try std.testing.expectError(error.InvalidReplayReference, resolve(a, &fixture, 11, 0, &corrupt));
    fixture.canonical = null;
    try std.testing.expectError(error.InvalidReplayReference, resolve(a, &fixture, 11, 0, &reference));
    fixture.reference = &reference;
    try std.testing.expect(!try canonicalMayRetire(&fixture, 11));
    fixture.reference = null;
    try std.testing.expect(try canonicalMayRetire(&fixture, 11));
}
