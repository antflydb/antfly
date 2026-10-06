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

//! Persisted pending replication effects. This owner contains only storage
//! keys and codecs; replication log publication and acknowledgement belong to
//! the replication adapter. Keep formats stable across crash/reopen and upgrades.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Kind = enum(u8) {
    batch = 0,
    replay = 1,
    schema = 2,
    restore_batch = 3,
    primary_effect = 4,
    row_policy = 5,
};

pub const replication_batch_outbox_key = "\x00\x00__metadata__:ha_batch_outbox_v1";

pub const replication_replay_outbox_key = "\x00\x00__metadata__:ha_replay_outbox_v1";

pub const replication_schema_outbox_key = "\x00\x00__metadata__:ha_schema_outbox_v1";

pub const replication_outbox_v2_prefix = "\x00\x00__metadata__:ha_outbox_v2:";

pub const replication_outbox_id_len: usize = 16;

pub const replication_outbox_magic = "AHO1";

pub const replication_outbox_header_len: usize = 16;

pub const replication_outbox_checksum_len: usize = 4;

pub const replication_outbox_recovery_batch_size: usize = 16;

pub const DurableReplicationOutbox = struct {
    from_lsn: u64,
    payload: []const u8,
};

pub fn durableReplicationOutboxKeyAlloc(
    alloc: Allocator,
    kind: Kind,
    from_lsn: u64,
    root_generation: u64,
    payload: []const u8,
) ![]u8 {
    if (from_lsn == 0) return error.InvalidHAOutbox;
    var id: [replication_outbox_id_len]u8 = undefined;
    // Bind the identity to both the WAL fence and logical mutation. Schema
    // requests prepare before apply admission and may observe the same next
    // LSN; distinct payloads must still never overwrite each other's outbox.
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update(&.{@backingInt(kind)});
    var identity_context: [16]u8 = undefined;
    std.mem.writeInt(u64, identity_context[0..8], root_generation, .big);
    std.mem.writeInt(u64, identity_context[8..16], from_lsn, .big);
    hasher.update(&identity_context);
    hasher.update(payload);
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hasher.final(&digest);
    @memcpy(&id, digest[0..id.len]);
    const key = try alloc.alloc(u8, replication_outbox_v2_prefix.len + 1 + id.len);
    @memcpy(key[0..replication_outbox_v2_prefix.len], replication_outbox_v2_prefix);
    key[replication_outbox_v2_prefix.len] = @backingInt(kind);
    @memcpy(key[replication_outbox_v2_prefix.len + 1 ..], &id);
    return key;
}

pub fn durableReplicationOutboxKindFromKey(key: []const u8) !Kind {
    if (!std.mem.startsWith(u8, key, replication_outbox_v2_prefix) or
        key.len != replication_outbox_v2_prefix.len + 1 + replication_outbox_id_len)
        return error.InvalidHAOutbox;
    return switch (key[replication_outbox_v2_prefix.len]) {
        @backingInt(Kind.batch) => .batch,
        @backingInt(Kind.replay) => .replay,
        @backingInt(Kind.schema) => .schema,
        @backingInt(Kind.restore_batch) => .restore_batch,
        @backingInt(Kind.primary_effect) => .primary_effect,
        @backingInt(Kind.row_policy) => .row_policy,
        else => error.InvalidHAOutbox,
    };
}

pub fn encodeDurableReplicationOutboxAlloc(alloc: Allocator, from_lsn: u64, payload: []const u8) ![]u8 {
    if (from_lsn == 0) return error.InvalidHAOutbox;
    const payload_len = std.math.cast(u32, payload.len) orelse return error.InvalidHAOutbox;
    const out = try alloc.alloc(u8, replication_outbox_header_len + payload.len + replication_outbox_checksum_len);
    errdefer alloc.free(out);
    @memcpy(out[0..4], replication_outbox_magic);
    std.mem.writeInt(u64, out[4..12], from_lsn, .little);
    std.mem.writeInt(u32, out[12..16], payload_len, .little);
    @memcpy(out[replication_outbox_header_len..][0..payload.len], payload);
    const checksum = @import("antfly_hash").Crc32.hash(out[0 .. out.len - replication_outbox_checksum_len]);
    std.mem.writeInt(u32, out[out.len - replication_outbox_checksum_len ..][0..replication_outbox_checksum_len], checksum, .little);
    return out;
}

pub fn decodeDurableReplicationOutbox(raw: []const u8) !DurableReplicationOutbox {
    if (raw.len < replication_outbox_header_len + replication_outbox_checksum_len or !std.mem.eql(u8, raw[0..4], replication_outbox_magic))
        return error.InvalidHAOutbox;
    const payload_len: usize = @intCast(std.mem.readInt(u32, raw[12..16], .little));
    if (payload_len != raw.len - replication_outbox_header_len - replication_outbox_checksum_len) return error.InvalidHAOutbox;
    const checksum = std.mem.readInt(u32, raw[raw.len - replication_outbox_checksum_len ..][0..replication_outbox_checksum_len], .little);
    if (@import("antfly_hash").Crc32.hash(raw[0 .. raw.len - replication_outbox_checksum_len]) != checksum) return error.InvalidHAOutbox;
    const from_lsn = std.mem.readInt(u64, raw[4..12], .little);
    if (from_lsn == 0) return error.InvalidHAOutbox;
    return .{ .from_lsn = from_lsn, .payload = raw[replication_outbox_header_len .. raw.len - replication_outbox_checksum_len] };
}

test "storage.hot_standby durable outbox codec preserves pre-extraction persisted bytes" {
    // Independent fixture: AHO1, little-endian LSN and length, payload, IEEE CRC32.
    const fixture = [_]u8{ 0x41, 0x48, 0x4f, 0x31, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x03, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x7e, 0x55, 0xb7, 0xf9 };
    const decoded = try decodeDurableReplicationOutbox(&fixture);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), decoded.from_lsn);
    try std.testing.expectEqualStrings("abc", decoded.payload);
    const encoded = try encodeDurableReplicationOutboxAlloc(std.testing.allocator, decoded.from_lsn, decoded.payload);
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualSlices(u8, &fixture, encoded);
}

test "storage.hot_standby durable outbox codec rejects corrupt and incomplete obligations" {
    const alloc = std.testing.allocator;
    const valid = try encodeDurableReplicationOutboxAlloc(alloc, 7, "pending mutation");
    defer alloc.free(valid);
    for (0..valid.len) |i| {
        const corrupt = try alloc.dupe(u8, valid);
        defer alloc.free(corrupt);
        corrupt[i] ^= 1;
        try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(corrupt));
        try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(valid[0..i]));
    }
    // Valid checksum alone cannot authorize a record with no WAL fence.
    @memset(valid[4..12], 0);
    std.mem.writeInt(u32, valid[valid.len - 4 ..][0..4], @import("antfly_hash").Crc32.hash(valid[0 .. valid.len - 4]), .little);
    try std.testing.expectError(error.InvalidHAOutbox, decodeDurableReplicationOutbox(valid));
    try std.testing.expectError(error.InvalidHAOutbox, encodeDurableReplicationOutboxAlloc(alloc, 0, "pending"));
}

test "storage.hot_standby durable outbox keys isolate payload generation fence and kind" {
    const alloc = std.testing.allocator;
    const baseline = try durableReplicationOutboxKeyAlloc(alloc, .batch, 7, 2, "first");
    defer alloc.free(baseline);
    const duplicate = try durableReplicationOutboxKeyAlloc(alloc, .batch, 7, 2, "first");
    defer alloc.free(duplicate);
    try std.testing.expectEqualSlices(u8, baseline, duplicate);
    const variants = [_]struct { kind: Kind = .batch, lsn: u64 = 7, generation: u64 = 2, payload: []const u8 = "first" }{
        .{ .payload = "second" }, .{ .generation = 3 }, .{ .lsn = 8 }, .{ .kind = .schema },
    };
    for (variants) |variant| {
        const key = try durableReplicationOutboxKeyAlloc(alloc, variant.kind, variant.lsn, variant.generation, variant.payload);
        defer alloc.free(key);
        try std.testing.expect(!std.mem.eql(u8, baseline, key));
        try std.testing.expectEqual(variant.kind, try durableReplicationOutboxKindFromKey(key));
    }
    baseline[replication_outbox_v2_prefix.len] = 255;
    try std.testing.expectError(error.InvalidHAOutbox, durableReplicationOutboxKindFromKey(baseline));
    try std.testing.expectError(error.InvalidHAOutbox, durableReplicationOutboxKindFromKey(replication_batch_outbox_key));
    try std.testing.expectError(error.InvalidHAOutbox, durableReplicationOutboxKindFromKey(duplicate[0 .. duplicate.len - 1]));
}
