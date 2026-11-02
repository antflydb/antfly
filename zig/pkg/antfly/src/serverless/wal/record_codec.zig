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

//! Shared WAL wire format; independent of native transports.
const std = @import("std");
const wal_types = @import("types.zig");

pub fn encodeRecordAlloc(alloc: std.mem.Allocator, lsn: u64, timestamp_ns: u64, payload: []const u8) ![]u8 {
    // The high length bit identifies records carrying an operation ID. Keep
    // newly written legacy records below that boundary so both encodings are
    // unambiguous to the shared parser.
    if (payload.len > idempotent_payload_max) return error.WalRecordTooLarge;
    const buf = try alloc.alloc(u8, 8 + 8 + 4 + payload.len);
    var pos: usize = 0;
    std.mem.writeInt(u64, buf[pos..][0..8], lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], timestamp_ns, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(payload.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..payload.len], payload);
    return buf;
}

pub const idempotent_record_flag: u32 = @as(u32, 1) << 31;
pub const idempotent_payload_max: usize = idempotent_record_flag - 1;

pub fn encodeIdempotentRecordAlloc(
    alloc: std.mem.Allocator,
    lsn: u64,
    timestamp_ns: u64,
    payload: []const u8,
    operation_id: []const u8,
) ![]u8 {
    if (payload.len > idempotent_payload_max) return error.WalRecordTooLarge;
    if (operation_id.len == 0 or operation_id.len > std.math.maxInt(u16))
        return error.InvalidWalOperationId;
    const buf = try alloc.alloc(u8, 8 + 8 + 4 + 2 + operation_id.len + payload.len);
    var pos: usize = 0;
    std.mem.writeInt(u64, buf[pos..][0..8], lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], timestamp_ns, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], idempotent_record_flag | @as(u32, @intCast(payload.len)), .little);
    pos += 4;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(operation_id.len), .little);
    pos += 2;
    @memcpy(buf[pos..][0..operation_id.len], operation_id);
    pos += operation_id.len;
    @memcpy(buf[pos..][0..payload.len], payload);
    return buf;
}

pub fn encodeRecordsAlloc(alloc: std.mem.Allocator, records: []const wal_types.Record) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (records) |record| {
        const encoded = if (record.operation_id) |operation_id|
            try encodeIdempotentRecordAlloc(alloc, record.lsn, record.timestamp_ns, record.payload, operation_id)
        else
            try encodeRecordAlloc(alloc, record.lsn, record.timestamp_ns, record.payload);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    return try out.toOwnedSlice(alloc);
}

pub const ParsedRecord = struct {
    lsn: u64,
    timestamp_ns: u64,
    payload: []const u8,
    operation_id: ?[]const u8,
    next_cursor: usize,
};

pub fn parseRecord(raw: []const u8, start: usize) !ParsedRecord {
    var cursor = start;
    if (cursor > raw.len or raw.len - cursor < 8 + 8 + 4) return error.InvalidWal;
    const lsn = std.mem.readInt(u64, raw[cursor..][0..8], .little);
    cursor += 8;
    const timestamp_ns = std.mem.readInt(u64, raw[cursor..][0..8], .little);
    cursor += 8;
    const length_tag = std.mem.readInt(u32, raw[cursor..][0..4], .little);
    cursor += 4;
    const idempotent = length_tag & idempotent_record_flag != 0;
    const payload_len: usize = @intCast(length_tag & ~idempotent_record_flag);
    var operation_id: ?[]const u8 = null;
    if (idempotent) {
        if (raw.len - cursor < 2) return error.InvalidWal;
        const operation_id_len = std.mem.readInt(u16, raw[cursor..][0..2], .little);
        cursor += 2;
        if (operation_id_len == 0 or operation_id_len > raw.len - cursor) return error.InvalidWal;
        operation_id = raw[cursor .. cursor + operation_id_len];
        cursor += operation_id_len;
    }
    if (payload_len > raw.len - cursor) return error.InvalidWal;
    const payload = raw[cursor .. cursor + payload_len];
    cursor += payload_len;
    return .{
        .lsn = lsn,
        .timestamp_ns = timestamp_ns,
        .payload = payload,
        .operation_id = operation_id,
        .next_cursor = cursor,
    };
}

pub fn decodeRecordsFromAlloc(alloc: std.mem.Allocator, raw: []const u8, start_lsn: u64) ![]wal_types.Record {
    var cursor: usize = 0;
    var out = std.ArrayListUnmanaged(wal_types.Record).empty;
    errdefer {
        for (out.items) |record| {
            alloc.free(record.payload);
            if (record.operation_id) |id| alloc.free(id);
        }
        out.deinit(alloc);
    }

    while (cursor < raw.len) {
        const parsed = try parseRecord(raw, cursor);
        if (parsed.lsn >= start_lsn) {
            const payload = try alloc.dupe(u8, parsed.payload);
            errdefer alloc.free(payload);
            const operation_id = if (parsed.operation_id) |id| try alloc.dupe(u8, id) else null;
            errdefer if (operation_id) |id| alloc.free(id);
            try out.append(alloc, .{
                .lsn = parsed.lsn,
                .timestamp_ns = parsed.timestamp_ns,
                .payload = payload,
                .operation_id = operation_id,
            });
        }
        cursor = parsed.next_cursor;
    }

    return try out.toOwnedSlice(alloc);
}

pub fn lastLsn(raw: []const u8) !u64 {
    var cursor: usize = 0;
    var latest: u64 = 0;
    while (cursor < raw.len) {
        const parsed = try parseRecord(raw, cursor);
        latest = parsed.lsn;
        cursor = parsed.next_cursor;
    }
    return latest;
}

pub fn findOperation(raw: []const u8, operation_id: []const u8) !?ParsedRecord {
    var cursor: usize = 0;
    while (cursor < raw.len) {
        const parsed = try parseRecord(raw, cursor);
        if (parsed.operation_id) |candidate| {
            if (std.mem.eql(u8, candidate, operation_id)) return parsed;
        }
        cursor = parsed.next_cursor;
    }
    return null;
}

test "serverless indexed record decoder bounds offsets and declared lengths" {
    try std.testing.expectError(error.InvalidWal, parseRecord("", std.math.maxInt(usize)));
    var malformed: [20]u8 = @splat(0);
    std.mem.writeInt(u32, malformed[16..20], idempotent_payload_max, .little);
    try std.testing.expectError(error.InvalidWal, parseRecord(&malformed, 0));
}
