// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Retained control-owner sidecar. The original accepted BEGIN envelope stays
//! available after its document cell drains and is reused. Local installation
//! identity and durable Raft reconciliation remain separate required proofs.
const std = @import("std");
const record_codec = @import("completion_control_record.zig");
const protocol = @import("../../common/completion_entry_protocol.zig");

pub const max_owners = record_codec.max_owners;
pub const max_bytes = record_codec.encoded_bytes + protocol.max_wire_bytes;
pub const filenames = [_][]const u8{
    "completion-control-0.guard", "completion-control-1.guard",
    "completion-control-2.guard", "completion-control-3.guard",
};
pub const pending_filenames = [_][]const u8{
    "completion-control-pending-0.guard", "completion-control-pending-1.guard",
    "completion-control-pending-2.guard", "completion-control-pending-3.guard",
};

pub const Guard = struct {
    record: record_codec.Record,
    envelope: []const u8,

    fn validate(self: Guard) !void {
        if (self.envelope.len == 0 or self.envelope.len > protocol.max_wire_bytes)
            return error.CompletionSlotTooLarge;
        if (!protocol.looksLike(self.envelope) or self.envelope.len < protocol.magic.len)
            return error.InvalidCompletionSlot;
        if (!std.mem.eql(u8, &protocol.payloadDigest(self.envelope), &self.record.begin.digest))
            return error.CompletionSlotChecksumMismatch;
    }

    pub fn encode(self: Guard, alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        const record = try self.record.encode();
        const bytes = try alloc.alloc(u8, record.len + self.envelope.len);
        @memcpy(bytes[0..record.len], &record);
        @memcpy(bytes[record.len..], self.envelope);
        return bytes;
    }

    /// The returned envelope borrows the complete sidecar. Its canonical entry
    /// and BEGIN semantics must also be checked before installing obligations.
    pub fn decode(bytes: []const u8) !Guard {
        if (bytes.len > max_bytes) return error.CompletionSlotTooLarge;
        if (bytes.len <= record_codec.encoded_bytes) return error.InvalidCompletionSlot;
        const self: Guard = .{
            .record = try record_codec.Record.decode(bytes[0..record_codec.encoded_bytes]),
            .envelope = bytes[record_codec.encoded_bytes..],
        };
        try self.validate();
        return self;
    }
};

pub const Owned = struct {
    alloc: std.mem.Allocator,
    bytes: []u8,
    guard: Guard,

    pub fn deinit(self: *Owned) void {
        self.alloc.free(self.bytes);
        self.* = undefined;
    }
};

/// Startup allocation, before advertising backing. Absence is not truncation
/// authority. Corruption or a different installation never becomes an empty
/// slot and must leave the database unavailable until reconciled.
pub fn load(alloc: std.mem.Allocator, storage: anytype, root: []const u8, index: usize, authority: record_codec.Authority) !?Owned {
    if (index >= max_owners) return error.InvalidCompletionSlot;
    const path = try std.fs.path.join(alloc, &.{ root, filenames[index] });
    defer alloc.free(path);
    const size = storage.fileSize(path) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    if (size > max_bytes) return error.CompletionSlotTooLarge;
    const bytes = try alloc.alloc(u8, @intCast(size));
    errdefer alloc.free(bytes);
    try storage.readFileRangeInto(alloc, path, 0, bytes);
    const guard = try Guard.decode(bytes);
    if (guard.record.slot_index != index) return error.InvalidCompletionSlot;
    try guard.record.verifyOwner(authority, guard.record.txn_id);
    return .{ .alloc = alloc, .bytes = bytes, .guard = guard };
}

test "workload admission completion compiler control guard retains BEGIN and immutable output ownership" {
    const alloc = std.testing.allocator;
    const envelope = protocol.magic ++ "canonical-BEGIN-fixture";
    const record: record_codec.Record = .{
        .authority = .{ .group_id = 7, .incarnation = @splat(9), .policy_digest = @splat(11), .schema_catalog_digest = @splat(13), .generation = 17 },
        .txn_id = @splat(19),
        .begin = .{ .term = 23, .index = 29, .digest = protocol.payloadDigest(envelope) },
        .participants = try record_codec.Participants.measure(&.{ "coordinator", "participant" }),
        .slot_index = 2,
        .output_run_id = 101,
    };
    const bytes = try (Guard{ .record = record, .envelope = envelope }).encode(alloc);
    defer alloc.free(bytes);
    const decoded = try Guard.decode(bytes);
    try std.testing.expectEqualDeep(record, decoded.record);
    try std.testing.expectEqualStrings(envelope, decoded.envelope);
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, Guard.decode(bytes));
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidCompletionSlot, Guard.decode(bytes[0..record_codec.encoded_bytes]));
    var wrong = record;
    wrong.begin.digest[0] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, (Guard{ .record = wrong, .envelope = envelope }).encode(std.testing.failing_allocator));
}
