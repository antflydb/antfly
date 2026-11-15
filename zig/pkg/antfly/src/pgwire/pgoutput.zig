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

//! PostgreSQL logical replication protocol v1 encoding. Positions are assigned
//! by the durable slot owner; this codec never derives an LSN from a source
//! checkpoint, native sequence, delivery ordinal, or wall clock.
//!
//! Wire reference (PostgreSQL 18):
//! https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html
//! https://www.postgresql.org/docs/18/protocol-replication.html
//! Streaming/two-phase protocols require separately negotiated implementations.
const std = @import("std");
const A = std.mem.Allocator;
pub const Lsn = enum(u64) { _ };
pub const max_message_bytes = 16 * 1024 * 1024;
pub const Column = struct { key: bool = false, name: []const u8, type_oid: u32, type_modifier: i32 = -1 };
pub const Identity = enum(u8) { default = 'd', nothing = 'n', full = 'f', index = 'i' };
pub const Cell = union(enum) { null, unchanged_toast, text: []const u8, binary: []const u8 };
pub const Old = union(enum) { key: []const Cell, full: []const Cell };
pub const Message = union(enum) {
    begin: struct { final_lsn: Lsn, commit_time: i64, xid: u32 },
    commit: struct { commit_lsn: Lsn, end_lsn: Lsn, commit_time: i64 },
    relation: struct { oid: u32, namespace: []const u8, name: []const u8, identity: Identity, columns: []const Column },
    type: struct { oid: u32, namespace: []const u8, name: []const u8 },
    insert: struct { relation: u32, values: []const Cell },
    update: struct { relation: u32, old: ?Old = null, values: []const Cell },
    delete: struct { relation: u32, old: Old },
    truncate: struct { relations: []const u32, cascade: bool = false, restart_identity: bool = false },
    origin: struct { commit_lsn: Lsn, name: []const u8 },
    logical_message: struct { transactional: bool, lsn: Lsn, prefix: []const u8, content: []const u8 },
};
const Encoder = struct {
    a: A,
    data: std.ArrayList(u8) = .empty,
    limit: usize,
    fn bytes(self: *Encoder, value: []const u8) !void {
        if (value.len > self.limit - self.data.items.len) return error.PgoutputMessageTooLarge;
        try self.data.appendSlice(self.a, value);
    }
    fn int(self: *Encoder, comptime T: type, value: T) !void {
        var buffer: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buffer, value, .big);
        try self.bytes(&buffer);
    }
    fn byte(self: *Encoder, value: u8) !void {
        try self.bytes(&.{value});
    }
    fn string(self: *Encoder, value: []const u8) !void {
        if (std.mem.indexOfScalar(u8, value, 0) != null or !std.unicode.utf8ValidateSlice(value)) return error.InvalidPgoutput;
        try self.bytes(value);
        try self.byte(0);
    }
    fn oid(self: *Encoder, value: u32) !void {
        if (value == 0) return error.InvalidPgoutput;
        try self.int(u32, value);
    }
    fn tuple(self: *Encoder, cells: []const Cell) !void {
        if (cells.len > std.math.maxInt(u16)) return error.PgoutputMessageTooLarge;
        try self.int(u16, @intCast(cells.len));
        for (cells) |cell| switch (cell) {
            .null => try self.byte('n'),
            .unchanged_toast => try self.byte('u'),
            .text, .binary => |value| {
                if (value.len > std.math.maxInt(i32)) return error.PgoutputMessageTooLarge;
                if (cell == .text and (std.mem.indexOfScalar(u8, value, 0) != null or !std.unicode.utf8ValidateSlice(value))) return error.InvalidPgoutput;
                try self.byte(if (cell == .text) 't' else 'b');
                try self.int(i32, @intCast(value.len));
                try self.bytes(value);
            },
        };
    }
    fn oldTuple(self: *Encoder, old: Old) !void {
        switch (old) {
            .key => |cells| {
                try self.byte('K');
                try self.tuple(cells);
            },
            .full => |cells| {
                try self.byte('O');
                try self.tuple(cells);
            },
        }
    }
};
/// Returns one logical message, without a CopyData/WAL envelope. No partially
/// encoded bytes escape on invalid input, size limits, or allocation failure.
pub fn encode(a: A, message: Message, limit: usize) ![]u8 {
    if (limit == 0 or limit > max_message_bytes) return error.InvalidPgoutput;
    var e: Encoder = .{ .a = a, .limit = limit };
    defer e.data.deinit(a);
    switch (message) {
        .begin => |m| {
            if (@backingInt(m.final_lsn) == 0 or m.xid == 0) return error.InvalidPgoutput;
            try e.byte('B');
            try e.int(u64, @backingInt(m.final_lsn));
            try e.int(i64, m.commit_time);
            try e.int(u32, m.xid);
        },
        .commit => |m| {
            if (@backingInt(m.commit_lsn) == 0 or @backingInt(m.end_lsn) < @backingInt(m.commit_lsn)) return error.InvalidPgoutput;
            try e.bytes("C\x00");
            try e.int(u64, @backingInt(m.commit_lsn));
            try e.int(u64, @backingInt(m.end_lsn));
            try e.int(i64, m.commit_time);
        },
        .relation => |m| {
            if (m.columns.len > std.math.maxInt(u16)) return error.PgoutputMessageTooLarge;
            try e.byte('R');
            try e.oid(m.oid);
            try e.string(m.namespace);
            try e.string(m.name);
            try e.byte(@backingInt(m.identity));
            try e.int(u16, @intCast(m.columns.len));
            for (m.columns) |c| {
                try e.byte(@intFromBool(c.key));
                try e.string(c.name);
                try e.oid(c.type_oid);
                try e.int(i32, c.type_modifier);
            }
        },
        .type => |m| {
            try e.byte('Y');
            try e.oid(m.oid);
            try e.string(m.namespace);
            try e.string(m.name);
        },
        .insert => |m| {
            try e.byte('I');
            try e.oid(m.relation);
            try e.byte('N');
            try e.tuple(m.values);
        },
        .update => |m| {
            if (m.old) |old| {
                const cells = switch (old) {
                    .key, .full => |v| v,
                };
                if (cells.len != m.values.len) return error.InvalidPgoutput;
            }
            try e.byte('U');
            try e.oid(m.relation);
            if (m.old) |old| try e.oldTuple(old);
            try e.byte('N');
            try e.tuple(m.values);
        },
        .delete => |m| {
            try e.byte('D');
            try e.oid(m.relation);
            try e.oldTuple(m.old);
        },
        .truncate => |m| {
            if (m.relations.len > std.math.maxInt(i32)) return error.PgoutputMessageTooLarge;
            try e.byte('T');
            try e.int(i32, @intCast(m.relations.len));
            try e.byte(@as(u8, @intFromBool(m.cascade)) | (@as(u8, @intFromBool(m.restart_identity)) << 1));
            for (m.relations) |id| try e.oid(id);
        },
        .origin => |m| {
            try e.byte('O');
            try e.int(u64, @backingInt(m.commit_lsn));
            try e.string(m.name);
        },
        .logical_message => |m| {
            if (m.content.len > std.math.maxInt(i32)) return error.PgoutputMessageTooLarge;
            try e.byte('M');
            try e.byte(@intFromBool(m.transactional));
            try e.int(u64, @backingInt(m.lsn));
            try e.string(m.prefix);
            try e.int(i32, @intCast(m.content.len));
            try e.bytes(m.content);
        },
    }
    return e.data.toOwnedSlice(a);
}

pub const WalData = struct { start: Lsn, end: Lsn, server_time: i64, payload: []const u8 };
pub const Keepalive = struct { end: Lsn, server_time: i64, request_reply: bool };
/// A complete backend CopyData frame. The caller bounds and spools individual
/// logical messages before invoking this function; no transaction-sized buffer
/// is required by the codec.
pub fn copyData(a: A, message: union(enum) { wal: WalData, keepalive: Keepalive }, limit: usize) ![]u8 {
    if (limit < 5 or limit > max_message_bytes + 30) return error.InvalidPgoutput;
    var e: Encoder = .{ .a = a, .limit = limit };
    defer e.data.deinit(a);
    try e.byte('d');
    try e.int(u32, 0);
    switch (message) {
        .wal => |m| {
            if (@backingInt(m.end) < @backingInt(m.start)) return error.InvalidPgoutput;
            try e.byte('w');
            try e.int(u64, @backingInt(m.start));
            try e.int(u64, @backingInt(m.end));
            try e.int(i64, m.server_time);
            try e.bytes(m.payload);
        },
        .keepalive => |m| {
            try e.byte('k');
            try e.int(u64, @backingInt(m.end));
            try e.int(i64, m.server_time);
            try e.byte(@intFromBool(m.request_reply));
        },
    }
    std.mem.writeInt(u32, e.data.items[1..5], @intCast(e.data.items.len - 1), .big);
    return e.data.toOwnedSlice(a);
}
pub const Feedback = struct { written: Lsn, flushed: Lsn, applied: Lsn, client_time: i64, request_reply: bool };
/// Decode the payload of a frontend CopyData frame. Positions are claims from
/// an untrusted peer. Only the slot owner's issued-LSN map may turn a flush
/// position into a durable feed acknowledgement or a retention decision.
pub fn feedback(bytes: []const u8) !Feedback {
    if (bytes.len != 34 or bytes[0] != 'r' or bytes[33] > 1) return error.InvalidPgoutput;
    return .{
        .written = @fromBackingInt(@intCast(std.mem.readInt(u64, bytes[1..9], .big))),
        .flushed = @fromBackingInt(@intCast(std.mem.readInt(u64, bytes[9..17], .big))),
        .applied = @fromBackingInt(@intCast(std.mem.readInt(u64, bytes[17..25], .big))),
        .client_time = std.mem.readInt(i64, bytes[25..33], .big),
        .request_reply = bytes[33] == 1,
    };
}
pub fn postgresTime(unix_nanoseconds: i128) !i64 {
    const epoch_ns: i128 = 946684800 * std.time.ns_per_s;
    const delta = std.math.sub(i128, unix_nanoseconds, epoch_ns) catch return error.InvalidPgoutput;
    return std.math.cast(i64, @divFloor(delta, std.time.ns_per_us)) orelse error.InvalidPgoutput;
}

test "pgoutput v1 independent wire fixtures preserve signed fields and tuple distinctions" {
    const a = std.testing.allocator;
    const begin = try encode(a, .{ .begin = .{ .final_lsn = @fromBackingInt(@intCast(0x0102030405060708)), .commit_time = -1, .xid = 42 } }, 128);
    defer a.free(begin);
    try std.testing.expectEqualSlices(u8, "B\x01\x02\x03\x04\x05\x06\x07\x08" ++ "\xff\xff\xff\xff\xff\xff\xff\xff" ++ "\x00\x00\x00\x2a", begin);
    const relation = try encode(a, .{ .relation = .{ .oid = 42, .namespace = "public", .name = "rows", .identity = .full, .columns = &.{.{ .key = true, .name = "id", .type_oid = 20 }} } }, 128);
    defer a.free(relation);
    try std.testing.expectEqualSlices(u8, "R\x00\x00\x00\x2apublic\x00rows\x00f\x00\x01\x01id\x00\x00\x00\x00\x14\xff\xff\xff\xff", relation);
    const update = try encode(a, .{ .update = .{ .relation = 42, .old = .{ .full = &.{ .{ .text = "a" }, .null, .{ .text = "" } } }, .values = &.{ .{ .text = "b" }, .unchanged_toast, .{ .binary = "\x00\xff" } } } }, 128);
    defer a.free(update);
    try std.testing.expectEqualSlices(u8, "U\x00\x00\x00\x2aO\x00\x03t\x00\x00\x00\x01an" ++ "t\x00\x00\x00\x00N\x00\x03t\x00\x00\x00\x01bu" ++ "b\x00\x00\x00\x02\x00\xff", update);
}
test "pgoutput CopyData frames and feedback use network order without trusting positions" {
    const a = std.testing.allocator;
    const wire = try copyData(a, .{ .wal = .{ .start = @fromBackingInt(@intCast(1)), .end = @fromBackingInt(@intCast(2)), .server_time = -1, .payload = "xyz" } }, 128);
    defer a.free(wire);
    try std.testing.expectEqualSlices(u8, "d\x00\x00\x00\x20w" ++ "\x00\x00\x00\x00\x00\x00\x00" ++ "\x01" ++ "\x00\x00\x00\x00\x00\x00\x00" ++ "\x02" ++ "\xff\xff\xff\xff\xff\xff\xff\xff" ++ "xyz", wire);
    const keep = try copyData(a, .{ .keepalive = .{ .end = @fromBackingInt(@intCast(2)), .server_time = 0, .request_reply = true } }, 128);
    defer a.free(keep);
    try std.testing.expectEqualSlices(u8, "d\x00\x00\x00\x16k" ++ "\x00\x00\x00\x00\x00\x00\x00" ++ "\x02" ++ "\x00\x00\x00\x00\x00\x00\x00\x00" ++ "\x01", keep);
    var status = ("r" ++ "\xff\xff\xff\xff\xff\xff\xff\xff" ++ "\x00\x00\x00\x00\x00\x00\x00" ++ "\x03" ++ "\x00\x00\x00\x00\x00\x00\x00" ++ "\x02" ++ "\xff\xff\xff\xff\xff\xff\xff\xff" ++ "\x01").*;
    const value = try feedback(&status);
    try std.testing.expectEqual(std.math.maxInt(u64), @backingInt(value.written));
    try std.testing.expectEqual(@as(u64, 3), @backingInt(value.flushed));
    try std.testing.expectEqual(@as(i64, -1), value.client_time);
    try std.testing.expect(value.request_reply);
    for (0..status.len) |n| try std.testing.expectError(error.InvalidPgoutput, feedback(status[0..n]));
    status[33] = 2;
    try std.testing.expectError(error.InvalidPgoutput, feedback(&status));
    try std.testing.expectEqual(@as(i64, 0), try postgresTime(946684800000000000));
    try std.testing.expectEqual(@as(i64, -1), try postgresTime(946684799999999999));
    try std.testing.expectError(error.InvalidPgoutput, postgresTime(std.math.minInt(i128)));
}
test "pgoutput rejects invalid messages and enforces caller byte limit" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidPgoutput, encode(a, .{ .insert = .{ .relation = 0, .values = &.{} } }, 100));
    try std.testing.expectError(error.InvalidPgoutput, encode(a, .{ .type = .{ .oid = 1, .namespace = "a\x00b", .name = "x" } }, 100));
    try std.testing.expectError(error.InvalidPgoutput, encode(a, .{ .commit = .{ .commit_lsn = @fromBackingInt(@intCast(2)), .end_lsn = @fromBackingInt(@intCast(1)), .commit_time = 0 } }, 100));
    try std.testing.expectError(error.InvalidPgoutput, encode(a, .{ .insert = .{ .relation = 1, .values = &.{.{ .text = "a\x00b" }} } }, 100));
    try std.testing.expectError(error.InvalidPgoutput, encode(a, .{ .insert = .{ .relation = 1, .values = &.{.{ .text = "\xff" }} } }, 100));
    try std.testing.expectError(error.InvalidPgoutput, encode(a, .{ .begin = .{ .final_lsn = @fromBackingInt(@intCast(0)), .commit_time = 0, .xid = 1 } }, 100));
    const m: Message = .{ .insert = .{ .relation = 1, .values = &.{.null} } };
    const complete = try encode(a, m, 100);
    defer a.free(complete);
    const exact = try encode(a, m, complete.len);
    defer a.free(exact);
    try std.testing.expectEqualSlices(u8, complete, exact);
    try std.testing.expectError(error.PgoutputMessageTooLarge, encode(a, m, complete.len - 1));
}
fn allocationProbe(a: A) !void {
    const bytes = try encode(a, .{ .relation = .{ .oid = 1, .namespace = "public", .name = &@as([1800]u8, @splat('x')), .identity = .default, .columns = &.{.{ .name = "value", .type_oid = 25 }} } }, max_message_bytes);
    a.free(bytes);
    const frame = try copyData(a, .{ .wal = .{ .start = @fromBackingInt(@intCast(1)), .end = @fromBackingInt(@intCast(2)), .server_time = 0, .payload = &@as([1600]u8, @splat('x')) } }, max_message_bytes);
    a.free(frame);
}
test "pgoutput allocation failures release every partial frame" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}

test "pgoutput v1 remaining message fixtures and old TOAST values" {
    const Fixture = struct { message: Message, expected: []const u8 };
    const fixtures = [_]Fixture{
        .{ .message = .{ .commit = .{ .commit_lsn = @fromBackingInt(@intCast(1)), .end_lsn = @fromBackingInt(@intCast(2)), .commit_time = 0 } }, .expected = "C\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00" },
        .{ .message = .{ .type = .{ .oid = 1, .namespace = "public", .name = "custom" } }, .expected = "Y\x00\x00\x00\x01public\x00custom\x00" },
        .{ .message = .{ .insert = .{ .relation = 1, .values = &.{ .null, .{ .text = "" } } } }, .expected = "I\x00\x00\x00\x01N\x00\x02nt\x00\x00\x00\x00" },
        .{ .message = .{ .update = .{ .relation = 1, .values = &.{.unchanged_toast} } }, .expected = "U\x00\x00\x00\x01N\x00\x01u" },
        .{ .message = .{ .delete = .{ .relation = 1, .old = .{ .key = &.{.{ .text = "1" }} } } }, .expected = "D\x00\x00\x00\x01K\x00\x01t\x00\x00\x00\x011" },
        .{ .message = .{ .delete = .{ .relation = 1, .old = .{ .full = &.{.unchanged_toast} } } }, .expected = "D\x00\x00\x00\x01O\x00\x01u" },
        .{ .message = .{ .truncate = .{ .relations = &.{ 1, 2 }, .cascade = true, .restart_identity = true } }, .expected = "T\x00\x00\x00\x02\x03\x00\x00\x00\x01\x00\x00\x00\x02" },
        .{ .message = .{ .origin = .{ .commit_lsn = @fromBackingInt(@intCast(1)), .name = "origin" } }, .expected = "O\x00\x00\x00\x00\x00\x00\x00\x01origin\x00" },
        .{ .message = .{ .logical_message = .{ .transactional = true, .lsn = @fromBackingInt(@intCast(1)), .prefix = "p", .content = "\x00\xff" } }, .expected = "M\x01\x00\x00\x00\x00\x00\x00\x00\x01p\x00\x00\x00\x00\x02\x00\xff" },
    };
    for (fixtures) |fixture| {
        const bytes = try encode(std.testing.allocator, fixture.message, 1024);
        defer std.testing.allocator.free(bytes);
        try std.testing.expectEqualSlices(u8, fixture.expected, bytes);
    }
}
