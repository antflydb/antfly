// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Golden compatibility tests for HA wire formats.
//!
//! Replication records are the stable boundary between primary WAL production,
//! streaming transport, and standby receive/apply. These fixtures intentionally
//! hard-code v1 bytes so accidental header, endian, enum, CRC, or payload layout
//! drift is caught before two Antfly versions fail to replicate.

const std = @import("std");
const replication_record = @import("replication_record.zig");

const v1_payload = "v1-fixture";

const v1_record = replication_record.Record{
    .kind = .batch_mutation,
    .payload_codec = .json,
    .flags = 0x01020304,
    .cluster_id = 0x0102030405060708,
    .shard_id = 0x1112131415161718,
    .table_id = 0x2122232425262728,
    .timeline_id = 0x3132333435363738,
    .epoch = 0x4142434445464748,
    .lsn = 0x5152535455565758,
    .previous_lsn = 0x5152535455565757,
    .commit_timestamp_ns = -123456789012345,
    .payload = v1_payload,
};

const v1_encoded = [_]u8{
    0x41, 0x46, 0x48, 0x41, 0x57, 0x41, 0x4c, 0x0a,
    0x01, 0x00, 0x64, 0x00, 0x01, 0x00, 0x01, 0x00,
    0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05,
    0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15,
    0x14, 0x13, 0x12, 0x11, 0x28, 0x27, 0x26, 0x25,
    0x24, 0x23, 0x22, 0x21, 0x38, 0x37, 0x36, 0x35,
    0x34, 0x33, 0x32, 0x31, 0x48, 0x47, 0x46, 0x45,
    0x44, 0x43, 0x42, 0x41, 0x58, 0x57, 0x56, 0x55,
    0x54, 0x53, 0x52, 0x51, 0x57, 0x57, 0x56, 0x55,
    0x54, 0x53, 0x52, 0x51, 0x87, 0x20, 0xf2, 0x79,
    0xb7, 0x8f, 0xff, 0xff, 0x0a, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x2f, 0xa1, 0x6d, 0xdb,
    0xba, 0x8a, 0xe4, 0x72, 0x76, 0x31, 0x2d, 0x66,
    0x69, 0x78, 0x74, 0x75, 0x72, 0x65,
};

test "storage.ha compat decodes v1 replication record fixture" {
    const decoded = try replication_record.decode(&v1_encoded);

    try std.testing.expectEqual(v1_record.kind, decoded.kind);
    try std.testing.expectEqual(v1_record.payload_codec, decoded.payload_codec);
    try std.testing.expectEqual(v1_record.flags, decoded.flags);
    try std.testing.expectEqual(v1_record.cluster_id, decoded.cluster_id);
    try std.testing.expectEqual(v1_record.shard_id, decoded.shard_id);
    try std.testing.expectEqual(v1_record.table_id, decoded.table_id);
    try std.testing.expectEqual(v1_record.timeline_id, decoded.timeline_id);
    try std.testing.expectEqual(v1_record.epoch, decoded.epoch);
    try std.testing.expectEqual(v1_record.lsn, decoded.lsn);
    try std.testing.expectEqual(v1_record.previous_lsn, decoded.previous_lsn);
    try std.testing.expectEqual(v1_record.commit_timestamp_ns, decoded.commit_timestamp_ns);
    try std.testing.expectEqualStrings(v1_payload, decoded.payload);
}

test "storage.ha compat keeps v1 replication record encoding stable" {
    const encoded = try replication_record.encodeAlloc(std.testing.allocator, v1_record);
    defer std.testing.allocator.free(encoded);

    try std.testing.expectEqual(@as(usize, replication_record.header_size + v1_payload.len), encoded.len);
    try std.testing.expectEqualSlices(u8, &v1_encoded, encoded);
}
