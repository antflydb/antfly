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

//! One HA terminal command over the exact immutable participant PREPARE already
//! installed by ALPP. Replay consumes retained intents, never a producer/VM.
//! The primary terminal batch owns both the real source cut and this outbox.
const std = @import("std");
const source = @import("../../api/replication_source_apply.zig");
const Position = @import("receipt_position.zig").Position;
pub const magic = "ALPT\x01\x00\x00\x00";
pub const max_command_bytes: usize = 32 * 1024;
// Native P admits at most64 table names of at most1024 bytes. A canonical
// participant adds a fixed framing/group suffix (bounded conservatively128).
pub const max_abort_roster_bytes: usize = 4 + 64 * (4 + 1024 + 128);
pub const max_bytes: usize = max_command_bytes + max_abort_roster_bytes + 16 + 32;
const header_bytes: usize = 16;
const digest_bytes: usize = 32;
pub const Action = enum { activate, commit, abort, acknowledge };
/// Fixed bounded selectors into the already retained participant roster. These
/// hashes do not grant ACK authority and never replace the immutable names.
pub const Acknowledgements = struct {
    count: u8,
    hashes: [64][32]u8 = @splat(@splat(0)),
    pub fn nameDigest(name: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/logical-terminal-ack-participant/v1\x00");
        hash.update(name);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn init(names: []const []const u8) !@This() {
        if (names.len == 0 or names.len > 64) return error.InvalidParticipant;
        var result: @This() = .{ .count = @intCast(names.len) };
        for (names, 0..) |name, index| result.hashes[index] = nameDigest(name);
        std.mem.sort([32]u8, result.hashes[0..names.len], {}, struct {
            fn less(_: void, left: [32]u8, right: [32]u8) bool {
                return std.mem.order(u8, &left, &right) == .lt;
            }
        }.less);
        var count: usize = 0;
        for (0..names.len) |index| {
            if (count != 0 and std.mem.eql(u8, &result.hashes[index], &result.hashes[count - 1])) continue;
            result.hashes[count] = result.hashes[index];
            count += 1;
        }
        result.count = @intCast(count);
        for (result.hashes[count..]) |*hash| hash.* = @splat(0);
        try result.validate();
        return result;
    }
    pub fn validate(self: @This()) !void {
        if (self.count == 0 or self.count > 64) return error.InvalidParticipant;
        for (self.hashes[0..self.count], 0..) |hash, index| {
            if (std.mem.allEqual(u8, &hash, 0) or (index != 0 and std.mem.order(u8, &self.hashes[index - 1], &hash) != .lt)) return error.InvalidParticipant;
        }
        for (self.hashes[self.count..]) |hash| if (!std.mem.allEqual(u8, &hash, 0)) return error.InvalidParticipant;
    }
    pub fn select(self: @This(), a: std.mem.Allocator, roster: []const []const u8) ![]const []const u8 {
        try self.validate();
        if (roster.len > 64) return error.InvalidParticipant;
        const selected = try a.alloc([]const u8, self.count);
        errdefer a.free(selected);
        var count: usize = 0;
        for (roster) |name| {
            const digest = nameDigest(name);
            for (self.hashes[0..self.count]) |expected| if (std.mem.eql(u8, &digest, &expected)) {
                if (count == selected.len) return error.InvalidParticipant;
                selected[count] = name;
                count += 1;
                break;
            };
        }
        if (count != selected.len) return error.InvalidParticipant;
        return selected;
    }
};
pub const Command = struct {
    seed: ?@import("native_standby_seed_contract.zig").Active = null,
    permit: ?source.Permit = null,
    capture: ?@import("../../system_catalog/logical_replication_publisher.zig").CaptureRef = null,
    action: Action,
    acknowledgements: ?Acknowledgements = null,
    source_ack: ?source.AckCompletion = null,
    /// Exact retained BEGIN role, only for an unprepared Source ABORT tail.
    abort_coordinator: bool = false,
    commit_version: u64,
    /// The image from the genuine accepted PREPARE or no-PREPARE ABORT. A
    /// standby retains this primary lineage; it cannot attest its copied root.
    physical_root: u128,
    position: Position,
    previous_sequence: u64,
    pub fn validate(self: @This()) !void {
        if (self.seed) |seed| {
            try seed.validate();
            if (seed.pending.physical_root_incarnation != self.physical_root or !seed.pending.namespace.eql(self.namespace()) or seed.pending.source_sequence > self.previous_sequence) return error.InvalidNativeSourceSeed;
        }
        if (self.abort_coordinator and (self.action != .abort or self.permit == null)) return error.InvalidLogicalOutbox;
        if (self.permit == null and self.capture == null) return error.InvalidLogicalOutbox;
        if (self.permit) |permit| {
            try permit.validate();
            if (permit.expected_image) |image| if (image.physical_root_incarnation != self.physical_root) return error.InvalidLogicalOutbox;
        }
        if (self.capture) |captured| {
            try captured.validate();
            if (captured.physical_root_incarnation != self.physical_root or !self.position.strictlyAfter(captured.position)) return error.InvalidLogicalOutbox;
            if (self.permit) |permit| if (!permit.decision_owner.eql(captured.owner) or !std.mem.eql(u8, &permit.reference.txn_id, &captured.transaction_id) or
                permit.member != captured.participant or permit.owner_group_id != captured.group_id or !permit.namespace.eql(captured.namespace)) return error.InvalidLogicalOutbox;
        }
        if (self.physical_root == 0 or self.position != .native or self.previous_sequence == std.math.maxInt(u64) or
            self.position.native.sequence != self.previous_sequence + 1) return error.InvalidLogicalOutbox;
        try self.position.requireNamespace(self.namespace());
        if (self.action == .commit and self.commit_version == 0) return error.InvalidLogicalOutbox;
        if (self.action == .acknowledge) {
            try (self.acknowledgements orelse return error.InvalidParticipant).validate();
            if (self.permit) |permit| {
                const completion = self.source_ack orelse return error.ReplicationCutoverAuthorityLost;
                try completion.validate();
                if (!std.meta.eql(completion.permit, permit)) return error.ReplicationCutoverAuthorityLost;
            } else if (self.source_ack != null) return error.ReplicationCutoverAuthorityLost;
        } else if (self.acknowledgements != null or self.source_ack != null) return error.InvalidParticipant;
    }
    pub fn namespace(self: @This()) @import("doc_identity_namespace.zig").Namespace {
        return if (self.permit) |permit| permit.namespace else self.capture.?.namespace;
    }
    pub fn owner(self: @This()) @import("../transaction_decision_owner.zig").External {
        return if (self.permit) |permit| permit.decision_owner else self.capture.?.owner;
    }
    pub fn transactionId(self: @This()) [16]u8 {
        return if (self.permit) |permit| permit.reference.txn_id else self.capture.?.transaction_id;
    }
    pub fn encode(self: @This(), a: std.mem.Allocator, maximum: usize) ![]u8 {
        return self.encodeWithAbortRoster(a, maximum, null);
    }
    pub fn encodeWithAbortRoster(self: @This(), a: std.mem.Allocator, maximum: usize, abort_roster: ?[]const u8) ![]u8 {
        try self.validate();
        if (self.abort_coordinator and abort_roster == null) return error.InvalidLogicalOutbox;
        if (abort_roster) |roster| {
            if (self.action != .abort or self.permit == null) return error.InvalidLogicalOutbox;
            try validateAbortRoster(roster);
        }
        const body = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(body);
        if (body.len > max_command_bytes) return error.TransactionTooLarge;
        var total = try std.math.add(usize, header_bytes + digest_bytes, body.len);
        total = try std.math.add(usize, total, if (abort_roster) |roster| roster.len else 0);
        if (total > @min(maximum, max_bytes)) return error.TransactionTooLarge;
        const bytes = try a.alloc(u8, total);
        errdefer a.free(bytes);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(body.len), .big);
        std.mem.writeInt(u32, bytes[12..16], @intCast(if (abort_roster) |roster| roster.len else 0), .big);
        @memcpy(bytes[header_bytes..][0..body.len], body);
        if (abort_roster) |roster| @memcpy(bytes[header_bytes + body.len ..][0..roster.len], roster);
        std.crypto.hash.Blake3.hash(bytes[0 .. total - digest_bytes], bytes[total - digest_bytes ..][0..digest_bytes], .{});
        return bytes;
    }
};

/// The existing participant-list encoding, copied as bytes from the accepting
/// transaction. It is not a PREPARE, an ACK or an independently chosen roster.
pub fn validateAbortRoster(bytes: []const u8) !void {
    if (bytes.len < 4 or bytes.len > max_abort_roster_bytes) return error.InvalidParticipant;
    const count = std.mem.readInt(u32, bytes[0..4], .little);
    if (count == 0 or count > 64) return error.InvalidParticipant;
    var offset: usize = 4;
    for (0..count) |_| {
        if (bytes.len - offset < 4) return error.InvalidParticipant;
        const length = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        if (length == 0 or length > 1024 + 128 or length > bytes.len - offset) return error.InvalidParticipant;
        offset += length;
    }
    if (offset != bytes.len) return error.InvalidParticipant;
}
pub const Frame = struct {
    command: Command,
    /// Borrows the input frame through synchronous terminal apply only.
    abort_roster: ?[]const u8,
};

pub fn isFrame(bytes: []const u8) bool {
    return bytes.len >= magic.len and std.mem.eql(u8, bytes[0..magic.len], magic);
}
pub fn decode(a: std.mem.Allocator, bytes: []const u8) !Command {
    return (try decodeFrame(a, bytes)).command;
}
pub fn decodeFrame(a: std.mem.Allocator, bytes: []const u8) !Frame {
    if (!isFrame(bytes) or bytes.len < header_bytes + digest_bytes or bytes.len > max_bytes) return error.InvalidLogicalOutbox;
    const length = std.mem.readInt(u32, bytes[8..12], .big);
    const roster_length = std.mem.readInt(u32, bytes[12..16], .big);
    if (length > max_command_bytes or roster_length > max_abort_roster_bytes or @as(usize, length) + roster_length != bytes.len - header_bytes - digest_bytes) return error.InvalidLogicalOutbox;
    var expected: [digest_bytes]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - digest_bytes], &expected, .{});
    if (!std.mem.eql(u8, &expected, bytes[bytes.len - digest_bytes ..])) return error.InvalidLogicalOutbox;
    const parsed = std.json.parseFromSlice(Command, a, bytes[header_bytes..][0..length], .{ .ignore_unknown_fields = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidLogicalOutbox,
    };
    defer parsed.deinit();
    try parsed.value.validate();
    const roster = if (roster_length == 0) null else bytes[header_bytes + length ..][0..roster_length];
    const canonical = try parsed.value.encodeWithAbortRoster(a, max_bytes, roster);
    defer a.free(canonical);
    if (!std.mem.eql(u8, canonical, bytes)) return error.InvalidLogicalOutbox;
    return .{ .command = parsed.value, .abort_roster = roster };
}

test "Source terminal HA frame retains original permission and rejects tampering and every allocation failure" {
    const Harness = struct {
        fn expectEncodeFailure(a: std.mem.Allocator, expected: anyerror, result: anyerror![]u8) !void {
            if (result) |bytes| {
                a.free(bytes);
                return error.TestExpectedError;
            } else |err| {
                if (err == error.OutOfMemory) return error.OutOfMemory;
                try std.testing.expectEqual(expected, err);
            }
        }
        fn run(a: std.mem.Allocator) !void {
            const reference: @import("../../api/replication_source_reference.zig").Reference = .{
                .owner = .{ .logical_subscription = .{ .database_id = 2, .subscription_id = 3 } },
                .birth = 1,
                .generation = 1,
                .source_digest = @splat(1),
                .event_digest = @splat(2),
                .intent_digest = @splat(3),
                .manifest_digest = @splat(4),
                .txn_id = @splat(5),
            };
            const permit: source.Permit = .{
                .semantic = .{ .metadata_group = 1, .metadata_incarnation = @splat('1'), .owner = reference.owner, .birth = reference.birth, .source_digest = reference.source_digest, .event_digest = reference.event_digest, .transaction_id = reference.txn_id },
                .reference = reference,
                .decision_owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(6), .admission_digest = @splat(7), .expires_at_unix_ns = 1000 },
                .member = 0,
                .owner_group_id = 9,
                .topology_epoch = 1,
                .namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 12 },
                .schema_version = 0,
                .schema_digest = @splat(8),
                .target_custody_digest = @splat(9),
                .expected_image = null,
                .previous_marker_digest = null,
                .pregrant_body_digest = @splat(10),
                .request_digest = @splat(11),
            };
            for ([_]Action{ .activate, .commit, .abort, .acknowledge }) |action| {
                const command: Command = .{ .permit = permit, .action = action, .acknowledgements = if (action == .acknowledge) try Acknowledgements.init(&.{"actual-owner"}) else null, .source_ack = if (action == .acknowledge) .{ .permit = permit, .participant_count = 1, .participant_ids_digest = try source.participantIdsDigest(&.{"actual-owner"}), .participant_id_digest = source.participantIdDigest("actual-owner"), .decision = .committed, .decision_version = 77, .terminal_receipts_digest = @splat(22) } else null, .commit_version = 77, .physical_root = 123, .position = .{ .native = .{ .namespace = permit.namespace, .sequence = 8 } }, .previous_sequence = 7 };
                const encoded = try command.encode(a, max_bytes);
                defer a.free(encoded);
                try std.testing.expectEqualDeep(command, try decode(a, encoded));
                try expectEncodeFailure(a, error.TransactionTooLarge, command.encode(a, encoded.len - 1));
                const changed = try a.dupe(u8, encoded);
                defer a.free(changed);
                changed[header_bytes] ^= 1;
                try std.testing.expectError(error.InvalidLogicalOutbox, decode(std.testing.allocator, changed));
                var wrong = command;
                wrong.previous_sequence = 8;
                try expectEncodeFailure(a, error.InvalidLogicalOutbox, wrong.encode(a, max_bytes));
                wrong = command;
                wrong.position.native.namespace.table_id += 1;
                try expectEncodeFailure(a, error.IdentityNamespaceMismatch, wrong.encode(a, max_bytes));
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}
