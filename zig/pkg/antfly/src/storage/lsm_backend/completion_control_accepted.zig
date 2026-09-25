// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One accepted but not yet applied control transition per retained owner.
//! This sidecar is a startup obligation, never restoration authority by itself.
const std = @import("std");
const record = @import("completion_control_record.zig");
const protocol = @import("../../common/completion_entry_protocol.zig");

pub const header_bytes = 164;
pub const max_bytes = header_bytes + protocol.max_wire_bytes;
pub const filenames = [_][]const u8{
    "completion-control-accepted-0.guard", "completion-control-accepted-1.guard",
    "completion-control-accepted-2.guard", "completion-control-accepted-3.guard",
};

pub fn hasAny(storage: @import("storage_io.zig").Storage, alloc: std.mem.Allocator, root: []const u8) !bool {
    for (filenames) |filename| {
        const path = try std.fs.path.join(alloc, &.{ root, filename });
        defer alloc.free(path);
        if (storage.fileSize(path)) |_| return true else |err| if (err != error.FileNotFound) return err;
    }
    return false;
}

pub const Accepted = struct {
    slot_index: u16,
    txn_id: [16]u8,
    begin: record.Receipt,
    transition: record.Receipt,
    envelope: []const u8,

    pub fn encode(self: Accepted, alloc: std.mem.Allocator) ![]u8 {
        const bytes = try alloc.alloc(u8, header_bytes + self.envelope.len);
        errdefer alloc.free(bytes);
        _ = try self.encodeInto(bytes);
        return bytes;
    }

    /// Encode into BEGIN-prepaid backing before writing the accepted sidecar.
    pub fn encodeInto(self: Accepted, bytes: []u8) ![]const u8 {
        if (self.slot_index >= record.max_owners or self.envelope.len == 0 or self.envelope.len > protocol.max_wire_bytes or
            bytes.len < header_bytes + self.envelope.len or self.transition.index <= self.begin.index or
            !std.mem.eql(u8, &protocol.payloadDigest(self.envelope), &self.transition.digest)) return error.InvalidCompletionSlot;
        const output = bytes[0 .. header_bytes + self.envelope.len];
        @memset(output[0..header_bytes], 0);
        @memcpy(output[0..8], "AFCTLAC1");
        std.mem.writeInt(u16, output[8..10], 1, .little);
        std.mem.writeInt(u16, output[10..12], self.slot_index, .little);
        @memcpy(output[16..32], &self.txn_id);
        @memcpy(output[32..80], &try self.begin.encode());
        @memcpy(output[80..128], &try self.transition.encode());
        std.mem.writeInt(u32, output[128..132], @intCast(self.envelope.len), .little);
        @memcpy(output[header_bytes..], self.envelope);
        @memcpy(output[132..164], &checksum(output));
        return output;
    }

    pub fn decode(bytes: []const u8) !Accepted {
        if (bytes.len <= header_bytes or bytes.len > max_bytes or !std.mem.eql(u8, bytes[0..8], "AFCTLAC1")) return error.InvalidCompletionSlot;
        if (std.mem.readInt(u16, bytes[8..10], .little) != 1) return error.UnsupportedCompletionSlotVersion;
        if (!std.mem.allEqual(u8, bytes[12..16], 0) or !std.mem.eql(u8, bytes[132..164], &checksum(bytes))) return error.CompletionSlotChecksumMismatch;
        const length = std.mem.readInt(u32, bytes[128..132], .little);
        if (@as(usize, length) != bytes.len - header_bytes) return error.InvalidCompletionSlot;
        const self: Accepted = .{
            .slot_index = std.mem.readInt(u16, bytes[10..12], .little),
            .txn_id = bytes[16..32].*,
            .begin = try record.Receipt.decode(bytes[32..80]),
            .transition = try record.Receipt.decode(bytes[80..128]),
            .envelope = bytes[header_bytes..],
        };
        if (self.slot_index >= record.max_owners or self.transition.index <= self.begin.index or
            !std.mem.eql(u8, &protocol.payloadDigest(self.envelope), &self.transition.digest)) return error.InvalidCompletionSlot;
        return self;
    }
};

fn checksum(bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-control-accepted-v1\x00");
    hash.update(bytes[0..132]);
    hash.update(bytes[header_bytes..]);
    return hash.finalResult();
}

test "workload admission completion compiler control accepted sidecar binds owner BEGIN and exact transition payload" {
    const alloc = std.testing.allocator;
    const envelope = protocol.magic ++ "control-decision";
    const expected: Accepted = .{
        .slot_index = 2,
        .txn_id = @splat(3),
        .begin = .{ .term = 5, .index = 7, .digest = @splat(11) },
        .transition = .{ .term = 5, .index = 8, .digest = protocol.payloadDigest(envelope) },
        .envelope = envelope,
    };
    const bytes = try expected.encode(alloc);
    defer alloc.free(bytes);
    const actual = try Accepted.decode(bytes);
    try std.testing.expectEqual(expected.slot_index, actual.slot_index);
    try std.testing.expectEqualDeep(expected.txn_id, actual.txn_id);
    try std.testing.expectEqualDeep(expected.begin, actual.begin);
    try std.testing.expectEqualDeep(expected.transition, actual.transition);
    try std.testing.expectEqualSlices(u8, expected.envelope, actual.envelope);
    for (0..bytes.len) |offset| {
        const damaged = try alloc.dupe(u8, bytes);
        defer alloc.free(damaged);
        damaged[offset] ^= 1;
        if (Accepted.decode(damaged)) |_| return error.TestExpectedError else |_| {}
    }
}
