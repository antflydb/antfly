// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Immutable native ownership record for BEGIN's future control obligations.
//! The applying replica creates this record with BEGIN, using its separately
//! validated installation and accepted log identity. A checksum detects damage;
//! it does not grant acceptance, restoration, or permission to retire an owner.
//! The participant bytes remain in the transaction's canonical metadata. This
//! record binds their exact encoding without duplicating a potentially large
//! list on every acknowledgement. The latest control receipt is a separate row.
const std = @import("std");

pub const owner_prefix = "\x00\x00__metadata__:completion_control_owner_v1:";
pub const receipt_prefix = "\x00\x00__metadata__:completion_control_receipt_v1:";
pub const progress_prefix = "\x00\x00__metadata__:completion_control_receipt_v2:";
pub const encoded_bytes = 256;
pub const receipt_bytes = 48;
pub const progress_bytes = 208;
pub const progress_bitmap_bits = 96;
pub const owner_key_bytes = owner_prefix.len + 16;
pub const receipt_key_bytes = receipt_prefix.len + 16;
pub const progress_key_bytes = progress_prefix.len + 16;
pub const max_owners = 4;
const magic = "AFCTLOW1";
const version: u16 = 1;

pub const Authority = struct {
    group_id: u64,
    incarnation: [16]u8,
    policy_digest: [32]u8,
    schema_catalog_digest: [32]u8,
    generation: u64,
};

pub const Receipt = struct {
    term: u64,
    index: u64,
    digest: [32]u8,

    fn validate(self: Receipt) !void {
        if (self.term == 0 or self.index == 0 or std.mem.allEqual(u8, &self.digest, 0))
            return error.InvalidCompletionSlot;
    }

    pub fn encode(self: Receipt) ![receipt_bytes]u8 {
        try self.validate();
        var bytes: [receipt_bytes]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], self.term, .little);
        std.mem.writeInt(u64, bytes[8..16], self.index, .little);
        @memcpy(bytes[16..48], &self.digest);
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != receipt_bytes) return error.InvalidCompletionSlot;
        const self: Receipt = .{
            .term = std.mem.readInt(u64, bytes[0..8], .little),
            .index = std.mem.readInt(u64, bytes[8..16], .little),
            .digest = bytes[16..48].*,
        };
        try self.validate();
        return self;
    }
};

/// The v1 receipt above is an identity-only row and keeps its exact ABI. A
/// control owner needs immutable BEGIN provenance after the Raft suffix is
/// compacted, plus a bound on the acknowledged set after later overwrites.
/// This v2 format is reserved in capacity now; writing it is disabled until
/// owner-backed apply and replay can atomically publish each transition.
pub const Progress = struct {
    pub const Phase = enum(u8) { begin = 0, decision = 1, acknowledgement = 2 };
    pub const Decision = enum(u8) { none = 0, committed = 1, aborted = 2 };

    txn_id: [16]u8,
    begin: Receipt,
    latest: Receipt,
    phase: Phase,
    decision: Decision,
    acknowledged: u32,
    resolved_digest: [32]u8,
    /// v3 encodes exact participant ordinals in the 12 bytes reserved by v2.
    /// An all-zero bitmap denotes a historical v2 receipt.
    ack_bitmap: [12]u8 = @splat(0),

    pub fn validate(self: Progress) !void {
        try self.begin.validate();
        try self.latest.validate();
        if (self.latest.index < self.begin.index) return error.InvalidCompletionSlot;
        switch (self.phase) {
            .begin => if (!std.meta.eql(self.latest, self.begin) or self.decision != .none or self.acknowledged != 0 or
                !std.mem.allEqual(u8, &self.resolved_digest, 0)) return error.InvalidCompletionSlot,
            .decision => if (self.latest.index == self.begin.index or self.decision == .none or self.acknowledged != 0 or
                !std.mem.allEqual(u8, &self.resolved_digest, 0)) return error.InvalidCompletionSlot,
            .acknowledgement => if (self.latest.index == self.begin.index or self.decision == .none or self.acknowledged == 0 or
                std.mem.allEqual(u8, &self.resolved_digest, 0)) return error.InvalidCompletionSlot,
        }
        if (!std.mem.allEqual(u8, &self.ack_bitmap, 0)) {
            if (self.phase != .acknowledgement or self.acknowledged > progress_bitmap_bits) return error.InvalidCompletionSlot;
            var bits: u32 = 0;
            for (self.ack_bitmap) |byte| bits += @as(u32, @popCount(byte));
            if (bits != self.acknowledged) return error.InvalidCompletionSlot;
        }
    }

    pub fn verifyOwner(self: Progress, owner: Record) !void {
        try self.validate();
        if (!std.mem.eql(u8, &self.txn_id, &owner.txn_id) or
            !std.meta.eql(self.begin, owner.begin) or
            self.acknowledged > owner.participants.count) return error.InvalidCompletionSlot;
        if (!std.mem.allEqual(u8, &self.ack_bitmap, 0)) {
            if (owner.participants.count > progress_bitmap_bits) return error.InvalidCompletionSlot;
            for (owner.participants.count..progress_bitmap_bits) |index| {
                const byte = self.ack_bitmap[index / 8];
                if (byte & (@as(u8, 1) << @intCast(index % 8)) != 0) return error.InvalidCompletionSlot;
            }
        }
    }

    pub fn encode(self: Progress) ![progress_bytes]u8 {
        try self.validate();
        var bytes: [progress_bytes]u8 = @splat(0);
        const v3 = !std.mem.allEqual(u8, &self.ack_bitmap, 0);
        @memcpy(bytes[0..8], if (v3) "AFCTLRP3" else "AFCTLRP2");
        std.mem.writeInt(u16, bytes[8..10], if (v3) 3 else 2, .little);
        bytes[10] = @intFromEnum(self.phase);
        bytes[11] = @intFromEnum(self.decision);
        @memcpy(bytes[16..32], &self.txn_id);
        @memcpy(bytes[32..80], &try self.begin.encode());
        @memcpy(bytes[80..128], &try self.latest.encode());
        std.mem.writeInt(u32, bytes[128..132], self.acknowledged, .little);
        @memcpy(bytes[132..144], &self.ack_bitmap);
        @memcpy(bytes[144..176], &self.resolved_digest);
        @memcpy(bytes[176..208], &progressChecksum(&bytes));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len != progress_bytes)
            return error.InvalidCompletionSlot;
        const v2 = std.mem.eql(u8, bytes[0..8], "AFCTLRP2");
        const v3 = std.mem.eql(u8, bytes[0..8], "AFCTLRP3");
        if (!v2 and !v3) return error.InvalidCompletionSlot;
        if (std.mem.readInt(u16, bytes[8..10], .little) != (if (v3) @as(u16, 3) else 2))
            return error.UnsupportedCompletionSlotVersion;
        if (!std.mem.allEqual(u8, bytes[12..16], 0) or
            (v2 and !std.mem.allEqual(u8, bytes[132..144], 0)) or
            (v3 and std.mem.allEqual(u8, bytes[132..144], 0))) return error.InvalidCompletionSlot;
        if (!std.mem.eql(u8, bytes[176..208], &progressChecksum(bytes)))
            return error.CompletionSlotChecksumMismatch;
        const phase: Phase = switch (bytes[10]) {
            0 => .begin,
            1 => .decision,
            2 => .acknowledgement,
            else => return error.InvalidCompletionSlot,
        };
        const decision: Decision = switch (bytes[11]) {
            0 => .none,
            1 => .committed,
            2 => .aborted,
            else => return error.InvalidCompletionSlot,
        };
        const self: Progress = .{
            .txn_id = bytes[16..32].*,
            .begin = try Receipt.decode(bytes[32..80]),
            .latest = try Receipt.decode(bytes[80..128]),
            .phase = phase,
            .decision = decision,
            .acknowledged = std.mem.readInt(u32, bytes[128..132], .little),
            .ack_bitmap = bytes[132..144].*,
            .resolved_digest = bytes[144..176].*,
        };
        try self.validate();
        return self;
    }
};

fn progressChecksum(bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-completion-control-receipt-v2\x00");
    hash.update(bytes[0..176]);
    return hash.finalResult();
}

pub const Participants = struct {
    digest: [32]u8,
    count: u32,
    /// u32 count followed by one u32 length and the bytes of each name.
    encoded_list_bytes: u32,

    pub fn measure(names: []const []const u8) !Participants {
        const count = std.math.cast(u32, names.len) orelse return error.TransactionTooLarge;
        var total: u32 = 4;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-completion-control-participants-v1\x00");
        var number: [4]u8 = undefined;
        std.mem.writeInt(u32, &number, count, .little);
        hash.update(&number);
        for (names) |name| {
            const length = std.math.cast(u32, name.len) orelse return error.TransactionTooLarge;
            total = std.math.add(u32, total, 4) catch return error.TransactionTooLarge;
            total = std.math.add(u32, total, length) catch return error.TransactionTooLarge;
            std.mem.writeInt(u32, &number, length, .little);
            hash.update(&number);
            hash.update(name);
        }
        return .{ .digest = hash.finalResult(), .count = count, .encoded_list_bytes = total };
    }

    pub fn fromEncodedList(bytes: []const u8) !Participants {
        // Validate complete framing before hashing; count and length alone do
        // not distinguish truncated names or a noncanonical trailing suffix.
        var iterator = try @import("../completion_control_budget.zig").ParticipantIterator.init(bytes);
        while (try iterator.next()) |_| {}
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-completion-control-participants-v1\x00");
        hash.update(bytes);
        return .{ .digest = hash.finalResult(), .count = iterator.count, .encoded_list_bytes = @intCast(bytes.len) };
    }

    fn validate(self: Participants) !void {
        const minimum = std.math.add(u64, 4, @as(u64, self.count) * 4) catch unreachable;
        if (self.encoded_list_bytes < minimum or (self.count == 0 and self.encoded_list_bytes != 4) or
            std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCompletionSlot;
    }
};

pub const Record = struct {
    authority: Authority,
    txn_id: [16]u8,
    begin: Receipt,
    participants: Participants,
    /// Native-local output ownership survives document cohort maintenance.
    slot_index: u16,
    output_run_id: u64,

    fn validate(self: Record) !void {
        if (self.authority.group_id == 0 or self.authority.generation == 0 or
            self.slot_index >= max_owners or self.output_run_id == 0)
            return error.InvalidCompletionSlot;
        try self.begin.validate();
        try self.participants.validate();
    }

    /// Called after decoding against trusted local installation identity. It
    /// deliberately includes generation and schema, not merely the group ID.
    pub fn verifyOwner(self: Record, authority: Authority, txn_id: [16]u8) !void {
        try self.validate();
        if (!std.meta.eql(self.authority, authority) or !std.mem.eql(u8, &self.txn_id, &txn_id))
            return error.InvalidCompletionSlot;
    }

    pub fn verifyParticipants(self: Record, names: []const []const u8) !void {
        if (!std.meta.eql(self.participants, try Participants.measure(names)))
            return error.InvalidCompletionSlot;
    }

    pub fn encode(self: Record) ![encoded_bytes]u8 {
        try self.validate();
        var bytes: [encoded_bytes]u8 = @splat(0);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u16, bytes[8..10], version, .little);
        std.mem.writeInt(u16, bytes[10..12], self.slot_index, .little);
        std.mem.writeInt(u64, bytes[16..24], self.authority.group_id, .little);
        @memcpy(bytes[24..40], &self.authority.incarnation);
        @memcpy(bytes[40..72], &self.authority.policy_digest);
        @memcpy(bytes[72..104], &self.authority.schema_catalog_digest);
        std.mem.writeInt(u64, bytes[104..112], self.authority.generation, .little);
        @memcpy(bytes[112..128], &self.txn_id);
        @memcpy(bytes[128..176], &try self.begin.encode());
        @memcpy(bytes[176..208], &self.participants.digest);
        std.mem.writeInt(u32, bytes[208..212], self.participants.count, .little);
        std.mem.writeInt(u32, bytes[212..216], self.participants.encoded_list_bytes, .little);
        std.mem.writeInt(u64, bytes[216..224], self.output_run_id, .little);
        @memcpy(bytes[224..256], &checksum(&bytes));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Record {
        if (bytes.len != encoded_bytes or !std.mem.eql(u8, bytes[0..8], magic))
            return error.InvalidCompletionSlot;
        if (std.mem.readInt(u16, bytes[8..10], .little) != version)
            return error.UnsupportedCompletionSlotVersion;
        if (!std.mem.allEqual(u8, bytes[12..16], 0))
            return error.InvalidCompletionSlot;
        if (!std.mem.eql(u8, bytes[224..256], &checksum(bytes)))
            return error.CompletionSlotChecksumMismatch;
        const self: Record = .{
            .authority = .{
                .group_id = std.mem.readInt(u64, bytes[16..24], .little),
                .incarnation = bytes[24..40].*,
                .policy_digest = bytes[40..72].*,
                .schema_catalog_digest = bytes[72..104].*,
                .generation = std.mem.readInt(u64, bytes[104..112], .little),
            },
            .txn_id = bytes[112..128].*,
            .begin = try Receipt.decode(bytes[128..176]),
            .participants = .{
                .digest = bytes[176..208].*,
                .count = std.mem.readInt(u32, bytes[208..212], .little),
                .encoded_list_bytes = std.mem.readInt(u32, bytes[212..216], .little),
            },
            .slot_index = std.mem.readInt(u16, bytes[10..12], .little),
            .output_run_id = std.mem.readInt(u64, bytes[216..224], .little),
        };
        try self.validate();
        return self;
    }
};

fn checksum(bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(bytes[0..224]);
    return hash.finalResult();
}

pub fn ownerKey(txn_id: [16]u8) [owner_key_bytes]u8 {
    return key(owner_prefix, txn_id);
}

pub fn receiptKey(txn_id: [16]u8) [receipt_key_bytes]u8 {
    return key(receipt_prefix, txn_id);
}

pub fn progressKey(txn_id: [16]u8) [progress_key_bytes]u8 {
    return key(progress_prefix, txn_id);
}

fn key(comptime prefix: []const u8, txn_id: [16]u8) [prefix.len + 16]u8 {
    var bytes: [prefix.len + 16]u8 = undefined;
    @memcpy(bytes[0..prefix.len], prefix);
    @memcpy(bytes[prefix.len..], &txn_id);
    return bytes;
}

fn fixture() !Record {
    return .{
        .authority = .{ .group_id = 7, .incarnation = @splat(9), .policy_digest = @splat(11), .schema_catalog_digest = @splat(13), .generation = 17 },
        .txn_id = @splat(19),
        .begin = .{ .term = 23, .index = 29, .digest = @splat(31) },
        .participants = try Participants.measure(&.{ "coordinator", "binary\x00\xffpeer" }),
        .slot_index = 2,
        .output_run_id = 101,
    };
}

test "workload admission completion compiler control record binds installation begin and exact participant encoding" {
    const expected = try fixture();
    var bytes = try expected.encode();
    const actual = try Record.decode(&bytes);
    try std.testing.expectEqualDeep(expected, actual);
    try std.testing.expectEqualSlices(u8, &bytes, &try actual.encode());
    @memset(&bytes, 0);
    try actual.verifyOwner(expected.authority, expected.txn_id);
    try actual.verifyParticipants(&.{ "coordinator", "binary\x00\xffpeer" });
    try std.testing.expectError(error.InvalidCompletionSlot, actual.verifyParticipants(&.{ "binary\x00\xffpeer", "coordinator" }));
    try std.testing.expectError(error.InvalidCompletionSlot, actual.verifyParticipants(&.{ "coordinator", "binary\x00\xffpee", "r" }));
    inline for (.{ "group_id", "incarnation", "policy_digest", "schema_catalog_digest", "generation" }) |field| {
        var wrong = expected.authority;
        if (@TypeOf(@field(wrong, field)) == u64) @field(wrong, field) += 1 else @field(wrong, field)[0] ^= 1;
        try std.testing.expectError(error.InvalidCompletionSlot, actual.verifyOwner(wrong, expected.txn_id));
    }
    try std.testing.expectError(error.InvalidCompletionSlot, actual.verifyOwner(expected.authority, @splat(20)));
    const receipt = try expected.begin.encode();
    try std.testing.expectEqualDeep(expected.begin, try Receipt.decode(&receipt));
    const owner_key = ownerKey(expected.txn_id);
    const receipt_key = receiptKey(expected.txn_id);
    try std.testing.expectEqualSlices(u8, &expected.txn_id, owner_key[owner_prefix.len..]);
    try std.testing.expectEqualSlices(u8, &expected.txn_id, receipt_key[receipt_prefix.len..]);
    try std.testing.expect(!std.mem.eql(u8, &owner_key, &receipt_key));
    const empty = try Participants.measure(&.{});
    try std.testing.expectEqual(@as(u32, 4), empty.encoded_list_bytes);
    try empty.validate();
}

test "workload admission completion compiler v2 progress retains begin and unique ACK frontier without changing v1 receipt" {
    const owner = try fixture();
    const v1 = try owner.begin.encode();
    try std.testing.expectEqual(@as(usize, 48), v1.len);
    try std.testing.expectEqualDeep(owner.begin, try Receipt.decode(&v1));
    const key_v1 = receiptKey(owner.txn_id);
    const key_v2 = progressKey(owner.txn_id);
    try std.testing.expect(!std.mem.eql(u8, &key_v1, &key_v2));
    var progress: Progress = .{
        .txn_id = owner.txn_id,
        .begin = owner.begin,
        .latest = owner.begin,
        .phase = .begin,
        .decision = .none,
        .acknowledged = 0,
        .resolved_digest = @splat(0),
    };
    for ([_]Progress.Phase{ .begin, .decision, .acknowledgement }) |phase| {
        progress.phase = phase;
        if (phase != .begin) progress.latest.index += 1;
        if (phase != .begin) progress.decision = .committed;
        if (phase == .acknowledgement) {
            progress.acknowledged = 1;
            progress.resolved_digest = @splat(0x71);
        }
        try progress.verifyOwner(owner);
        const encoded = try progress.encode();
        try std.testing.expectEqualDeep(progress, try Progress.decode(&encoded));
        for (0..progress_bytes) |offset| {
            var damaged = encoded;
            damaged[offset] ^= 1;
            if (Progress.decode(&damaged)) |_| return error.TestExpectedError else |_| {}
        }
        var other = progress;
        other.txn_id[0] ^= 1;
        try std.testing.expectError(error.InvalidCompletionSlot, other.verifyOwner(owner));
        other = progress;
        other.begin.digest[0] ^= 1;
        try std.testing.expectError(error.InvalidCompletionSlot, other.verifyOwner(owner));
    }
    progress.ack_bitmap[0] = 0b0000_0010;
    const v3 = try progress.encode();
    try std.testing.expectEqualStrings("AFCTLRP3", v3[0..8]);
    try std.testing.expectEqualDeep(progress, try Progress.decode(&v3));
    var invalid_bitmap = progress;
    invalid_bitmap.ack_bitmap[0] = 0b0000_0100;
    try std.testing.expectError(error.InvalidCompletionSlot, invalid_bitmap.verifyOwner(owner));
    invalid_bitmap.ack_bitmap[0] = 0b0000_0011;
    try std.testing.expectError(error.InvalidCompletionSlot, invalid_bitmap.encode());
    progress.ack_bitmap = @splat(0);
    progress.acknowledged = owner.participants.count + 1;
    try std.testing.expectError(error.InvalidCompletionSlot, progress.verifyOwner(owner));
    progress.acknowledged = 0;
    try std.testing.expectError(error.InvalidCompletionSlot, progress.encode());
}

test "workload admission completion compiler control record rejects corruption noncanonical framing and impossible identities" {
    const expected = try fixture();
    const original = try expected.encode();
    for (0..encoded_bytes) |offset| {
        var bytes = original;
        bytes[offset] ^= 1;
        if (Record.decode(&bytes)) |_| return error.TestExpectedError else |_| {}
    }
    try std.testing.expectError(error.InvalidCompletionSlot, Record.decode(original[0 .. original.len - 1]));
    for ([_]usize{ 12, 15 }) |offset| {
        var bytes = original;
        bytes[offset] = 1;
        @memcpy(bytes[224..256], &checksum(&bytes));
        try std.testing.expectError(error.InvalidCompletionSlot, Record.decode(&bytes));
    }
    var bytes = original;
    std.mem.writeInt(u32, bytes[212..216], 3, .little);
    @memcpy(bytes[224..256], &checksum(&bytes));
    try std.testing.expectError(error.InvalidCompletionSlot, Record.decode(&bytes));
    var invalid = expected;
    invalid.begin.index = 0;
    try std.testing.expectError(error.InvalidCompletionSlot, invalid.encode());
    invalid = expected;
    invalid.authority.generation = 0;
    try std.testing.expectError(error.InvalidCompletionSlot, invalid.encode());
    invalid = expected;
    invalid.slot_index = max_owners;
    try std.testing.expectError(error.InvalidCompletionSlot, invalid.encode());
    invalid = expected;
    invalid.output_run_id = 0;
    try std.testing.expectError(error.InvalidCompletionSlot, invalid.encode());
    var zero_receipt: [receipt_bytes]u8 = @splat(0);
    try std.testing.expectError(error.InvalidCompletionSlot, Receipt.decode(&zero_receipt));
}
