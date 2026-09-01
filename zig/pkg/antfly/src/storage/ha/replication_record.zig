// Copyright 2026 Antfly, Inc.
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

//! Versioned HA replication record envelope.
//!
//! This is the stable boundary between primary commit/effects production and
//! standby receive/apply. It deliberately does not expose the current LSM WAL
//! byte layout; storage-specific WALs remain implementation details beneath this
//! logical/effects replication stream.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;

pub const magic = [8]u8{ 'A', 'F', 'H', 'A', 'W', 'A', 'L', '\n' };
pub const format_version: u16 = 1;
pub const header_size: u16 = 100;

const version_offset: usize = 8;
const header_len_offset: usize = 10;
const record_kind_offset: usize = 12;
const payload_codec_offset: usize = 14;
const flags_offset: usize = 16;
const cluster_id_offset: usize = 20;
const shard_id_offset: usize = 28;
const table_id_offset: usize = 36;
const timeline_id_offset: usize = 44;
const epoch_offset: usize = 52;
const lsn_offset: usize = 60;
const previous_lsn_offset: usize = 68;
const commit_timestamp_ns_offset: usize = 76;
const payload_len_offset: usize = 84;
const payload_crc_offset: usize = 92;
const header_crc_offset: usize = 96;

comptime {
    std.debug.assert(header_crc_offset + 4 == header_size);
}

pub const RecordKind = enum(u16) {
    batch_mutation = 0x0001,
    metadata_mutation = 0x0002,
    derived_effect = 0x0003,
    backup_start = 0x0010,
    backup_end = 0x0011,
    checkpoint = 0x0012,
    manifest = 0x0013,
    truncate = 0x0014,
    timeline_switch = 0x0020,
    _,
};

pub const PayloadCodec = enum(u16) {
    raw = 0x0000,
    json = 0x0001,
    binary = 0x0002,
    _,
};

pub const Record = struct {
    kind: RecordKind,
    payload_codec: PayloadCodec = .raw,
    flags: u32 = 0,
    cluster_id: u64,
    shard_id: u64 = 0,
    table_id: u64 = 0,
    timeline_id: u64,
    epoch: u64,
    lsn: u64,
    previous_lsn: u64,
    commit_timestamp_ns: i64 = 0,
    payload: []const u8 = &.{},
};

pub const RecordView = Record;

fn decodeRecordKind(raw: u16) !RecordKind {
    return switch (raw) {
        @backingInt(RecordKind.batch_mutation) => .batch_mutation,
        @backingInt(RecordKind.metadata_mutation) => .metadata_mutation,
        @backingInt(RecordKind.derived_effect) => .derived_effect,
        @backingInt(RecordKind.backup_start) => .backup_start,
        @backingInt(RecordKind.backup_end) => .backup_end,
        @backingInt(RecordKind.checkpoint) => .checkpoint,
        @backingInt(RecordKind.manifest) => .manifest,
        @backingInt(RecordKind.truncate) => .truncate,
        @backingInt(RecordKind.timeline_switch) => .timeline_switch,
        else => error.UnsupportedRecordKind,
    };
}

fn decodePayloadCodec(raw: u16) !PayloadCodec {
    return switch (raw) {
        @backingInt(PayloadCodec.raw) => .raw,
        @backingInt(PayloadCodec.json) => .json,
        @backingInt(PayloadCodec.binary) => .binary,
        else => error.UnsupportedPayloadCodec,
    };
}

pub fn encodedLen(payload_len: usize) !usize {
    return std.math.add(usize, header_size, payload_len);
}

pub fn encodeAlloc(alloc: Allocator, record: Record) ![]u8 {
    const len = try encodedLen(record.payload.len);
    const out = try alloc.alloc(u8, len);
    errdefer alloc.free(out);
    try encodeInto(out, record);
    return out;
}

pub fn encodeInto(out: []u8, record: Record) !void {
    const required = try encodedLen(record.payload.len);
    if (out.len != required) return error.InvalidOutputLength;
    const record_kind = try decodeRecordKind(@backingInt(record.kind));
    const payload_codec = try decodePayloadCodec(@backingInt(record.payload_codec));

    @memset(out[0..header_size], 0);
    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], format_version, .little);
    std.mem.writeInt(u16, out[header_len_offset..][0..2], header_size, .little);
    std.mem.writeInt(u16, out[record_kind_offset..][0..2], @backingInt(record_kind), .little);
    std.mem.writeInt(u16, out[payload_codec_offset..][0..2], @backingInt(payload_codec), .little);
    std.mem.writeInt(u32, out[flags_offset..][0..4], record.flags, .little);
    std.mem.writeInt(u64, out[cluster_id_offset..][0..8], record.cluster_id, .little);
    std.mem.writeInt(u64, out[shard_id_offset..][0..8], record.shard_id, .little);
    std.mem.writeInt(u64, out[table_id_offset..][0..8], record.table_id, .little);
    std.mem.writeInt(u64, out[timeline_id_offset..][0..8], record.timeline_id, .little);
    std.mem.writeInt(u64, out[epoch_offset..][0..8], record.epoch, .little);
    std.mem.writeInt(u64, out[lsn_offset..][0..8], record.lsn, .little);
    std.mem.writeInt(u64, out[previous_lsn_offset..][0..8], record.previous_lsn, .little);
    std.mem.writeInt(i64, out[commit_timestamp_ns_offset..][0..8], record.commit_timestamp_ns, .little);
    std.mem.writeInt(u64, out[payload_len_offset..][0..8], @intCast(record.payload.len), .little);
    std.mem.writeInt(u32, out[payload_crc_offset..][0..4], Crc32.hash(record.payload), .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
    @memcpy(out[header_size..], record.payload);
}

pub fn decode(bytes: []const u8) !RecordView {
    if (bytes.len < header_size) return error.EndOfStream;
    if (!std.mem.eql(u8, bytes[0..8], &magic)) return error.InvalidMagic;

    const version = std.mem.readInt(u16, bytes[version_offset..][0..2], .little);
    if (version == 0 or version > format_version) return error.UnsupportedVersion;

    const header_len = std.mem.readInt(u16, bytes[header_len_offset..][0..2], .little);
    if (header_len != header_size) return error.UnsupportedHeaderLength;

    const stored_header_crc = std.mem.readInt(u32, bytes[header_crc_offset..][0..4], .little);
    const computed_header_crc = Crc32.hash(bytes[0..header_crc_offset]);
    if (stored_header_crc != computed_header_crc) return error.HeaderCrcMismatch;

    const payload_len_u64 = std.mem.readInt(u64, bytes[payload_len_offset..][0..8], .little);
    if (payload_len_u64 > std.math.maxInt(usize)) return error.PayloadTooLarge;
    const payload_len: usize = @intCast(payload_len_u64);
    const total_len = try encodedLen(payload_len);
    if (bytes.len < total_len) return error.EndOfStream;
    if (bytes.len != total_len) return error.TrailingBytes;

    const payload = bytes[header_size..total_len];
    const stored_payload_crc = std.mem.readInt(u32, bytes[payload_crc_offset..][0..4], .little);
    const computed_payload_crc = Crc32.hash(payload);
    if (stored_payload_crc != computed_payload_crc) return error.PayloadCrcMismatch;

    return .{
        .kind = try decodeRecordKind(std.mem.readInt(u16, bytes[record_kind_offset..][0..2], .little)),
        .payload_codec = try decodePayloadCodec(std.mem.readInt(u16, bytes[payload_codec_offset..][0..2], .little)),
        .flags = std.mem.readInt(u32, bytes[flags_offset..][0..4], .little),
        .cluster_id = std.mem.readInt(u64, bytes[cluster_id_offset..][0..8], .little),
        .shard_id = std.mem.readInt(u64, bytes[shard_id_offset..][0..8], .little),
        .table_id = std.mem.readInt(u64, bytes[table_id_offset..][0..8], .little),
        .timeline_id = std.mem.readInt(u64, bytes[timeline_id_offset..][0..8], .little),
        .epoch = std.mem.readInt(u64, bytes[epoch_offset..][0..8], .little),
        .lsn = std.mem.readInt(u64, bytes[lsn_offset..][0..8], .little),
        .previous_lsn = std.mem.readInt(u64, bytes[previous_lsn_offset..][0..8], .little),
        .commit_timestamp_ns = std.mem.readInt(i64, bytes[commit_timestamp_ns_offset..][0..8], .little),
        .payload = payload,
    };
}

test "ha replication record round trips all envelope fields" {
    const payload = "doc:alpha={\"ok\":true}";
    const encoded = try encodeAlloc(std.testing.allocator, .{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .flags = 0xA5A5,
        .cluster_id = 11,
        .shard_id = 22,
        .table_id = 33,
        .timeline_id = 44,
        .epoch = 55,
        .lsn = 66,
        .previous_lsn = 65,
        .commit_timestamp_ns = 123456789,
        .payload = payload,
    });
    defer std.testing.allocator.free(encoded);

    const decoded = try decode(encoded);
    try std.testing.expectEqual(RecordKind.batch_mutation, decoded.kind);
    try std.testing.expectEqual(PayloadCodec.json, decoded.payload_codec);
    try std.testing.expectEqual(@as(u32, 0xA5A5), decoded.flags);
    try std.testing.expectEqual(@as(u64, 11), decoded.cluster_id);
    try std.testing.expectEqual(@as(u64, 22), decoded.shard_id);
    try std.testing.expectEqual(@as(u64, 33), decoded.table_id);
    try std.testing.expectEqual(@as(u64, 44), decoded.timeline_id);
    try std.testing.expectEqual(@as(u64, 55), decoded.epoch);
    try std.testing.expectEqual(@as(u64, 66), decoded.lsn);
    try std.testing.expectEqual(@as(u64, 65), decoded.previous_lsn);
    try std.testing.expectEqual(@as(i64, 123456789), decoded.commit_timestamp_ns);
    try std.testing.expectEqualStrings(payload, decoded.payload);
}

test "ha replication record rejects payload corruption" {
    var encoded = try encodeAlloc(std.testing.allocator, .{
        .kind = .derived_effect,
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
        .payload = "artifact",
    });
    defer std.testing.allocator.free(encoded);

    encoded[encoded.len - 1] ^= 0xff;
    try std.testing.expectError(error.PayloadCrcMismatch, decode(encoded));
}

test "ha replication record rejects header corruption" {
    var encoded = try encodeAlloc(std.testing.allocator, .{
        .kind = .metadata_mutation,
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
        .payload = "schema",
    });
    defer std.testing.allocator.free(encoded);

    encoded[flags_offset] ^= 0x01;
    try std.testing.expectError(error.HeaderCrcMismatch, decode(encoded));
}

test "ha replication record validates framing before returning a view" {
    const encoded = try encodeAlloc(std.testing.allocator, .{
        .kind = .backup_start,
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
        .payload = "manifest",
    });
    defer std.testing.allocator.free(encoded);

    try std.testing.expectError(error.EndOfStream, decode(encoded[0 .. encoded.len - 1]));

    var with_trailing = try std.testing.allocator.alloc(u8, encoded.len + 1);
    defer std.testing.allocator.free(with_trailing);
    @memcpy(with_trailing[0..encoded.len], encoded);
    with_trailing[encoded.len] = 0;
    try std.testing.expectError(error.TrailingBytes, decode(with_trailing));
}

test "ha replication record rejects unsupported versions and bad magic" {
    var encoded = try encodeAlloc(std.testing.allocator, .{
        .kind = .timeline_switch,
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
    });
    defer std.testing.allocator.free(encoded);

    encoded[0] = 'X';
    try std.testing.expectError(error.InvalidMagic, decode(encoded));

    encoded[0] = magic[0];
    std.mem.writeInt(u16, encoded[version_offset..][0..2], format_version + 1, .little);
    std.mem.writeInt(u32, encoded[header_crc_offset..][0..4], Crc32.hash(encoded[0..header_crc_offset]), .little);
    try std.testing.expectError(error.UnsupportedVersion, decode(encoded));
}

test "ha replication record rejects unknown current-version kind and codec" {
    var encoded = try encodeAlloc(std.testing.allocator, .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
        .payload = "mutation",
    });
    defer std.testing.allocator.free(encoded);

    std.mem.writeInt(u16, encoded[record_kind_offset..][0..2], 0xffff, .little);
    std.mem.writeInt(u32, encoded[header_crc_offset..][0..4], Crc32.hash(encoded[0..header_crc_offset]), .little);
    try std.testing.expectError(error.UnsupportedRecordKind, decode(encoded));

    std.mem.writeInt(u16, encoded[record_kind_offset..][0..2], @backingInt(RecordKind.batch_mutation), .little);
    std.mem.writeInt(u16, encoded[payload_codec_offset..][0..2], 0xffff, .little);
    std.mem.writeInt(u32, encoded[header_crc_offset..][0..4], Crc32.hash(encoded[0..header_crc_offset]), .little);
    try std.testing.expectError(error.UnsupportedPayloadCodec, decode(encoded));
}

test "ha replication record refuses to encode unknown current-version kind and codec" {
    try std.testing.expectError(error.UnsupportedRecordKind, encodeAlloc(std.testing.allocator, .{
        .kind = @fromBackingInt(@intCast(0xffff)),
        .payload_codec = .raw,
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
    }));

    try std.testing.expectError(error.UnsupportedPayloadCodec, encodeAlloc(std.testing.allocator, .{
        .kind = .batch_mutation,
        .payload_codec = @fromBackingInt(@intCast(0xffff)),
        .cluster_id = 1,
        .timeline_id = 2,
        .epoch = 3,
        .lsn = 4,
        .previous_lsn = 3,
    }));
}
