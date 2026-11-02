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

//! Versioned HA replication record envelope.
//!
//! This is the stable boundary between primary commit/effects production and
//! standby receive/apply. It deliberately does not expose the current LSM WAL
//! byte layout; storage-specific WALs remain implementation details beneath this
//! logical/effects replication stream.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = @import("antfly_hash").Crc32;

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
    _ = try decodeRecordKind(@backingInt(record.kind));
    _ = try decodePayloadCodec(@backingInt(record.payload_codec));
    try encodeHeader(out[0..header_size], record, record.payload.len, Crc32.hash(record.payload));
    @memcpy(out[header_size..], record.payload);
}

fn encodeHeader(out: *[header_size]u8, record: Record, payload_len: usize, payload_crc: u32) !void {
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
    std.mem.writeInt(u64, out[payload_len_offset..][0..8], @intCast(payload_len), .little);
    std.mem.writeInt(u32, out[payload_crc_offset..][0..4], payload_crc, .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
}

/// Envelope storage and the payload checksum are acquired before the local
/// commit. Sealing only writes the fixed-size header; it does not allocate,
/// copy the payload, or rescan it. No request control survives construction.
pub const Prepared = struct {
    allocator: Allocator,
    encoded: []u8,
    payload_crc: u32,

    pub fn init(allocator: Allocator, payload: []const u8, control: ?@import("../../common/read_admission.zig").Port) !Prepared {
        if (control) |port| try port.checkpoint();
        const encoded = try allocator.alloc(u8, try encodedLen(payload.len));
        errdefer allocator.free(encoded);
        var crc = Crc32.init();
        var offset: usize = 0;
        while (offset < payload.len) {
            if (control) |port| try port.checkpoint();
            const end = offset + @min(payload.len - offset, 4096);
            @memcpy(encoded[header_size + offset .. header_size + end], payload[offset..end]);
            crc.update(payload[offset..end]);
            offset = end;
        }
        if (control) |port| try port.checkpoint();
        return .{ .allocator = allocator, .encoded = encoded, .payload_crc = crc.final() };
    }

    pub fn seal(self: *Prepared, metadata: Record) ![]const u8 {
        // Payload ownership is internal; metadata cannot substitute other bytes
        // after precommit admission or reuse a checksum for a different value.
        if (metadata.payload.len != 0) return error.InvalidArgument;
        try encodeHeader(self.encoded[0..header_size], metadata, self.encoded.len - header_size, self.payload_crc);
        return self.encoded;
    }

    pub fn deinit(self: *Prepared) void {
        self.allocator.free(self.encoded);
        self.* = undefined;
    }
};

pub fn decode(bytes: []const u8) !RecordView {
    return decodeWithAdmission(bytes, null);
}

/// Caller-held publication/snapshot dependencies must pass a retained-lifetime
/// port. Neither this synchronous decoder nor its borrowed result owns it.
pub fn decodeWithAdmission(bytes: []const u8, control: ?@import("../../common/read_admission.zig").Port) !RecordView {
    if (control) |port| try port.checkpoint();
    const payload_len = try declaredPayloadLen(bytes);
    const total_len = try encodedLen(payload_len);
    if (bytes.len < total_len) return error.EndOfStream;
    if (bytes.len != total_len) return error.TrailingBytes;

    const payload = bytes[header_size..total_len];
    const stored_payload_crc = std.mem.readInt(u32, bytes[payload_crc_offset..][0..4], .little);
    const computed_payload_crc = if (control) |port| blk: {
        var crc = Crc32.init();
        var offset: usize = 0;
        while (offset < payload.len) {
            try port.checkpoint();
            const end = offset + @min(64 * 1024, payload.len - offset);
            crc.update(payload[offset..end]);
            offset = end;
        }
        try port.checkpoint();
        break :blk crc.final();
    } else Crc32.hash(payload);
    if (stored_payload_crc != computed_payload_crc) return error.PayloadCrcMismatch;

    return decodeFields(bytes, payload);
}

fn declaredPayloadLen(bytes: []const u8) !usize {
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
    return payload_len;
}

fn decodeFields(bytes: []const u8, payload: []const u8) !RecordView {
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

/// Scalar streaming verification/matching state. Input and expected payload
/// are fresh synchronous borrows; no provider, allocator or Port is retained.
pub const MatchCursor = struct {
    pub const Expected = struct { lsn: u64, kind: RecordKind, payload: []const u8, shard_id: u64, table_id: u64 };
    header: [header_size]u8 = undefined,
    offset: usize = 0,
    payload_len: ?usize = null,
    crc: Crc32 = .init(),
    equal: bool = true,
    result: ?bool = null,
    failure: ?anyerror = null,

    /// Consume at most 64 KiB exactly once, in order. `last` identifies the end
    /// of the entire owned WAL value, not just the current input window.
    pub fn consume(self: *@This(), bytes: []const u8, last: bool, expected: Expected, control: @import("../../common/read_admission.zig").Port) !?bool {
        if (bytes.len > 64 * 1024 or (bytes.len == 0 and !last)) return error.InvalidArgument;
        if (self.failure) |err| return err;
        if (self.result != null) return error.InvalidArgument;
        return self.advance(bytes, last, expected, control) catch |err| {
            self.failure = err;
            return err;
        };
    }
    fn advance(self: *@This(), bytes: []const u8, last: bool, expected: Expected, control: @import("../../common/read_admission.zig").Port) !?bool {
        try control.checkpoint();
        var input = bytes;
        if (self.offset < header_size) {
            const take = @min(input.len, header_size - self.offset);
            @memcpy(self.header[self.offset..][0..take], input[0..take]);
            self.offset += take;
            input = input[take..];
            if (self.offset == header_size) self.payload_len = try declaredPayloadLen(&self.header);
        }
        if (self.payload_len) |len| {
            const total = try encodedLen(len);
            const next = try std.math.add(usize, self.offset, input.len);
            if (next > total) return error.TrailingBytes;
            const payload_offset = self.offset - header_size;
            self.crc.update(input);
            if (len != expected.payload.len) self.equal = false;
            if (self.equal and !std.mem.eql(u8, input, expected.payload[payload_offset..][0..input.len])) self.equal = false;
            self.offset = next;
            if (last) {
                if (self.offset < total) return error.EndOfStream;
                if (self.crc.final() != std.mem.readInt(u32, self.header[payload_crc_offset..][0..4], .little)) return error.PayloadCrcMismatch;
                // Kind/codec and WAL continuity follow payload validation,
                // matching the complete decoder's corruption precedence.
                const record = try decodeFields(&self.header, &.{});
                if (record.lsn != expected.lsn) return error.RecordWalLsnMismatch;
                const next_lsn = std.math.add(u64, record.previous_lsn, 1) catch return error.RecordPreviousLsnMismatch;
                if (next_lsn != record.lsn) return error.RecordPreviousLsnMismatch;
                const matched = self.equal and record.kind == expected.kind and record.shard_id == expected.shard_id and record.table_id == expected.table_id;
                try control.checkpoint();
                self.result = matched;
                return matched;
            }
        } else if (last) return error.EndOfStream;
        try control.checkpoint();
        return null;
    }
};

test "replication record round trips all envelope fields" {
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

test "replication record rejects payload corruption" {
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

test "replication record rejects header corruption" {
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

test "replication record validates framing before returning a view" {
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

test "replication record rejects unsupported versions and bad magic" {
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

test "replication record rejects unknown current-version kind and codec" {
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

test "replication record refuses to encode unknown current-version kind and codec" {
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

test "replication record controlled decoding rejects original lifetime between payload checksum quanta" {
    const read = @import("../../common/read_admission.zig");
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
    };
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 3 * 64 * 1024 + 17);
    defer a.free(bytes);
    @memset(bytes, 93);
    const encoded = try encodeAlloc(a, .{ .kind = .batch_mutation, .cluster_id = 1, .shard_id = 2, .table_id = 3, .timeline_id = 1, .epoch = 1, .lsn = 7, .previous_lsn = 6, .payload = bytes });
    defer a.free(encoded);
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const control = read.Port.init(&payload);
    try std.testing.expectEqualDeep(try decode(encoded), try decodeWithAdmission(encoded, control));
    const checks = state.checks;
    try std.testing.expect(checks >= 6);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            state = .{ .cause = cause, .reject_at = reject_at };
            try std.testing.expectError(cause, decodeWithAdmission(encoded, control));
            try std.testing.expectEqual(reject_at, state.checks);
        }
    }
    state = .{};
    encoded[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.PayloadCrcMismatch, decodeWithAdmission(encoded, control));
    try std.testing.expectError(error.PayloadCrcMismatch, decode(encoded));
}

fn drainMatchForTest(encoded: []const u8, expected: MatchCursor.Expected, quantum: usize) !bool {
    const read_control = @import("../../common/read_admission.zig");
    const Check = struct {
        active: bool = true,
        fn run(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.active) return error.ExpiredMatchBorrow;
        }
    };
    var cursor: MatchCursor = .{};
    while (true) {
        const offset = cursor.offset;
        const take = @min(quantum, encoded.len - offset);
        const source = try std.testing.allocator.dupe(u8, encoded[offset..][0..take]);
        defer std.testing.allocator.free(source);
        const payload = try std.testing.allocator.dupe(u8, expected.payload);
        defer std.testing.allocator.free(payload);
        var state: Check = .{};
        const authority: read_control.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Check.run, .check_lifetime = Check.run } };
        var fresh = expected;
        fresh.payload = payload;
        const result = cursor.consume(source, offset + take == encoded.len, fresh, read_control.Port.init(&authority));
        state.active = false;
        @memset(source, 0xDD);
        @memset(payload, 0xCC);
        if (try result) |matched| return matched;
        try std.testing.expectEqual(offset + take, cursor.offset);
    }
}

test "replication record controlled continuation verifies exact payload with fresh window and expectation borrows" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 65536 + 19);
    defer a.free(payload);
    for (payload, 0..) |*byte, i| byte.* = @truncate(i *% 73);
    const encoded = try encodeAlloc(a, .{ .kind = .batch_mutation, .cluster_id = 1, .shard_id = 2, .table_id = 3, .timeline_id = 4, .epoch = 5, .lsn = 7, .previous_lsn = 6, .payload = payload });
    defer a.free(encoded);
    const expected: MatchCursor.Expected = .{ .lsn = 7, .kind = .batch_mutation, .payload = payload, .shard_id = 2, .table_id = 3 };
    for ([_]usize{ 97, 4093, 65536 }) |quantum| try std.testing.expect(try drainMatchForTest(encoded, expected, quantum));
    const wrong = try a.dupe(u8, payload);
    defer a.free(wrong);
    wrong[wrong.len - 1] ^= 1;
    var mismatch = expected;
    mismatch.payload = wrong;
    try std.testing.expect(!try drainMatchForTest(encoded, mismatch, 4093));
    mismatch = expected;
    mismatch.table_id += 1;
    try std.testing.expect(!try drainMatchForTest(encoded, mismatch, 4093));
    const empty = try encodeAlloc(a, .{ .kind = .batch_mutation, .cluster_id = 1, .shard_id = 2, .table_id = 3, .timeline_id = 4, .epoch = 5, .lsn = 7, .previous_lsn = 6 });
    defer a.free(empty);
    mismatch = expected;
    mismatch.payload = "";
    try std.testing.expect(try drainMatchForTest(empty, mismatch, 1));
}

test "replication record controlled continuation preserves corruption precedence and WAL continuity" {
    const a = std.testing.allocator;
    const valid = try encodeAlloc(a, .{ .kind = .batch_mutation, .cluster_id = 1, .shard_id = 2, .table_id = 3, .timeline_id = 4, .epoch = 5, .lsn = 7, .previous_lsn = 6, .payload = "wanted" });
    defer a.free(valid);
    const expected: MatchCursor.Expected = .{ .lsn = 7, .kind = .batch_mutation, .payload = "wanted", .shard_id = 2, .table_id = 3 };
    for (0..14) |variant| {
        const bytes = try a.alloc(u8, valid.len + 1);
        defer a.free(bytes);
        @memcpy(bytes[0..valid.len], valid);
        bytes[valid.len] = 0;
        var len = valid.len;
        switch (variant) {
            0 => bytes[0] ^= 1,
            1 => std.mem.writeInt(u16, bytes[version_offset..][0..2], 0, .little),
            2 => std.mem.writeInt(u16, bytes[header_len_offset..][0..2], header_size + 1, .little),
            3 => bytes[header_crc_offset] ^= 1,
            4 => bytes[header_size] ^= 1,
            5, 12 => std.mem.writeInt(u16, bytes[record_kind_offset..][0..2], 0xFFFF, .little),
            6 => std.mem.writeInt(u16, bytes[payload_codec_offset..][0..2], 0xFFFF, .little),
            7 => std.mem.writeInt(u64, bytes[lsn_offset..][0..8], 9, .little),
            8 => std.mem.writeInt(u64, bytes[previous_lsn_offset..][0..8], 7, .little),
            9 => std.mem.writeInt(u64, bytes[payload_len_offset..][0..8], 7, .little),
            10 => len += 1,
            11 => len = header_size - 1,
            13 => std.mem.writeInt(u64, bytes[payload_len_offset..][0..8], std.math.maxInt(u64), .little),
            else => unreachable,
        }
        if (variant == 12) bytes[header_size] ^= 1;
        if ((variant >= 5 and variant <= 9) or variant == 12 or variant == 13)
            std.mem.writeInt(u32, bytes[header_crc_offset..][0..4], Crc32.hash(bytes[0..header_crc_offset]), .little);
        const Reference = struct {
            fn validate(input: []const u8) !void {
                const record = try decode(input);
                if (record.lsn != 7) return error.RecordWalLsnMismatch;
                const next = std.math.add(u64, record.previous_lsn, 1) catch return error.RecordPreviousLsnMismatch;
                if (next != record.lsn) return error.RecordPreviousLsnMismatch;
            }
        };
        Reference.validate(bytes[0..len]) catch |cause| {
            try std.testing.expectError(cause, drainMatchForTest(bytes[0..len], expected, 17));
            continue;
        };
        return error.ExpectedCorruptRecord;
    }
}

test "replication record prepared envelope matches wire bytes without postprepare allocations or payload borrows" {
    const alloc = std.testing.allocator;
    var input: [9001]u8 = @splat(91);
    input[3] = 0;
    const original = input;
    var storage: [header_size + input.len]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var prepared = try Prepared.init(fixed.allocator(), &input, null);
    defer prepared.deinit();
    @memset(&input, 1);
    try std.testing.expectError(error.OutOfMemory, fixed.allocator().alloc(u8, 1));
    for ([_]u64{ 1, 71, 999999 }) |lsn| {
        const metadata: Record = .{ .kind = .batch_mutation, .payload_codec = .json, .flags = 7, .cluster_id = 8, .shard_id = 9, .table_id = 10, .timeline_id = 11, .epoch = 12, .lsn = lsn, .previous_lsn = lsn - 1, .commit_timestamp_ns = 1234 };
        const actual = try prepared.seal(metadata);
        var record = metadata;
        record.payload = &original;
        const expected = try encodeAlloc(alloc, record);
        defer alloc.free(expected);
        try std.testing.expectEqualSlices(u8, expected, actual);
        try std.testing.expectEqualSlices(u8, &original, (try decode(actual)).payload);
        try std.testing.expectError(error.InvalidArgument, prepared.seal(record));
    }
}

test "replication record prepared envelope preserves original refusal and retires partial construction" {
    const admission = @import("../../common/read_admission.zig");
    const Fixture = struct {
        calls: usize = 0,
        fail_at: usize,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.DeadlineExceeded;
        }
    };
    const input: [12289]u8 = @splat(31);
    // Initial fence, four copy/checksum quanta, and final construction fence.
    for (1..7) |fail_at| {
        var fixture: Fixture = .{ .fail_at = fail_at };
        var payload: admission.Payload = .{ .ptr = &fixture, .vtable = &.{ .checkpoint = Fixture.check } };
        try std.testing.expectError(error.DeadlineExceeded, Prepared.init(std.testing.allocator, &input, admission.Port.init(&payload)));
        try std.testing.expectEqual(fail_at, fixture.calls);
    }
    var storage: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    try std.testing.expectError(error.OutOfMemory, Prepared.init(fixed.allocator(), &input, null));
}
