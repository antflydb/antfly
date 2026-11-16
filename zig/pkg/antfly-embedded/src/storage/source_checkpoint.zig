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

//! Source-global checkpoint intent for a qualified completion participant.
//! Binding is the immutable logical source scope, independent of upload attempts
//! and physical row shards. The caller routes one stable checkpoint authority
//! and commits the returned exact-value guard and metadata intent with effects.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const prefix = "\x00\x00__metadata__:source_checkpoint:";
pub const max_checkpoint_bytes = 1024;
const header_bytes = 136;
fn nonzero(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn equalOptional(left: ?[]const u8, right: ?[]const u8) bool {
    return if (left) |value| right != null and std.mem.eql(u8, value, right.?) else right == null;
}
pub const Command = struct {
    binding: Digest,
    purpose: Digest,
    sequence: u64,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    accepted_at_ns: u64,
    pub fn validate(self: Command) !void {
        if (!nonzero(self.binding) or !nonzero(self.purpose) or self.sequence == 0 or self.accepted_at_ns == 0 or self.accepted_at_ns == std.math.maxInt(u64) or self.checkpoint.len == 0 or self.checkpoint.len > max_checkpoint_bytes) return error.InvalidSourceAdmission;
        if (self.expected_checkpoint) |previous| if (previous.len == 0 or previous.len > max_checkpoint_bytes) return error.InvalidSourceAdmission;
        if ((self.sequence == 1) != (self.expected_checkpoint == null)) return error.SourceCheckpointConflict;
    }
};
pub fn isKey(bytes: []const u8) bool {
    return bytes.len == prefix.len + 32 and std.mem.startsWith(u8, bytes, prefix);
}
pub fn key(binding: Digest) [prefix.len + 32]u8 {
    var result: [prefix.len + 32]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &binding);
    return result;
}
fn checksum(bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-checkpoint-head:v1");
    hash.update(bytes[0..104]);
    hash.update(bytes[header_bytes..]);
    return hash.finalResult();
}
pub const Head = struct {
    binding: Digest,
    purpose: Digest,
    transaction_id: [16]u8,
    sequence: u64,
    checkpoint: []const u8,
    accepted_at_ns: u64,
    pub fn validate(self: Head) !void {
        if (!nonzero(self.binding) or !nonzero(self.purpose) or !nonzero(self.transaction_id) or self.sequence == 0 or self.accepted_at_ns == 0 or self.accepted_at_ns == std.math.maxInt(u64) or self.checkpoint.len == 0 or self.checkpoint.len > max_checkpoint_bytes) return error.SourceAdmissionCorrupt;
    }
    pub fn encode(self: Head, a: A) ![]u8 {
        try self.validate();
        const bytes = try a.alloc(u8, header_bytes + self.checkpoint.len);
        @memcpy(bytes[0..4], "SCP1");
        std.mem.writeInt(u64, bytes[4..12], self.sequence, .little);
        std.mem.writeInt(u64, bytes[12..20], self.accepted_at_ns, .little);
        @memcpy(bytes[20..52], &self.binding);
        @memcpy(bytes[52..84], &self.purpose);
        @memcpy(bytes[84..100], &self.transaction_id);
        std.mem.writeInt(u32, bytes[100..104], @intCast(self.checkpoint.len), .little);
        @memcpy(bytes[header_bytes..], self.checkpoint);
        @memcpy(bytes[104..136], &checksum(bytes));
        return bytes;
    }
    /// Checkpoint bytes borrow the verified retained record.
    pub fn decode(bytes: []const u8) !Head {
        if (bytes.len <= header_bytes or bytes.len > header_bytes + max_checkpoint_bytes or !std.mem.eql(u8, bytes[0..4], "SCP1") or std.mem.readInt(u32, bytes[100..104], .little) != bytes.len - header_bytes or !std.mem.eql(u8, bytes[104..136], &checksum(bytes))) return error.SourceAdmissionCorrupt;
        const result: Head = .{ .binding = bytes[20..52].*, .purpose = bytes[52..84].*, .transaction_id = bytes[84..100].*, .sequence = std.mem.readInt(u64, bytes[4..12], .little), .checkpoint = bytes[header_bytes..], .accepted_at_ns = std.mem.readInt(u64, bytes[12..20], .little) };
        try result.validate();
        return result;
    }
};
pub const Plan = struct { key: []const u8, before: ?[]const u8, value: []const u8 };
/// All returned bytes belong to an attempt arena. Nothing is written here.
/// The native transaction MUST predicate on before and resolve value atomically.
pub fn prepare(a: A, txn: anytype, transaction_id: [16]u8, source_purpose: Digest, command: Command) !Plan {
    try command.validate();
    if (!nonzero(transaction_id) or !std.mem.eql(u8, &command.purpose, &source_purpose)) return error.SourceTransactionIdentityConflict;
    const physical_key = try a.dupe(u8, &key(command.binding));
    const before = txn.get(physical_key) catch |err| switch (@as(anyerror, err)) {
        error.NotFound => null,
        else => return err,
    };
    const previous = if (before) |bytes| try Head.decode(bytes) else null;
    if (previous) |head| {
        if (!std.mem.eql(u8, &head.binding, &command.binding)) return error.SourceAdmissionCorrupt;
        if (head.sequence > command.sequence) return error.SourceReceiptExpired;
        if (head.sequence == command.sequence) {
            if (!std.mem.eql(u8, &head.purpose, &command.purpose) or !std.mem.eql(u8, &head.transaction_id, &transaction_id) or !std.mem.eql(u8, head.checkpoint, command.checkpoint) or head.accepted_at_ns != command.accepted_at_ns) return error.SourcePayloadConflict;
            return error.SourceAlreadyCommitted;
        }
        if (head.sequence == std.math.maxInt(u64) or head.sequence + 1 != command.sequence) return error.SourceCheckpointConflict;
    } else if (command.sequence != 1) return error.SourceCheckpointConflict;
    if (!equalOptional(command.expected_checkpoint, if (previous) |head| head.checkpoint else null)) return error.SourceCheckpointConflict;
    const next: Head = .{ .binding = command.binding, .purpose = command.purpose, .transaction_id = transaction_id, .sequence = command.sequence, .checkpoint = command.checkpoint, .accepted_at_ns = command.accepted_at_ns };
    return .{ .key = physical_key, .before = if (before) |bytes| try a.dupe(u8, bytes) else null, .value = try next.encode(a) };
}

test "source checkpoint preserves opaque cyclic positions and fences altered retries" {
    const a = std.testing.allocator;
    const Store = struct {
        value: ?[]const u8 = null,
        fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var store: Store = .{};
    var command: Command = .{ .binding = @splat(1), .purpose = @splat(2), .sequence = 1, .checkpoint = "opaque:z", .accepted_at_ns = 100 };
    const first = try prepare(arena.allocator(), &store, @splat(3), command.purpose, command);
    try std.testing.expect(first.before == null and store.value == null);
    store.value = first.value;
    try std.testing.expectError(error.SourceAlreadyCommitted, prepare(arena.allocator(), &store, @splat(3), command.purpose, command));
    var changed = command;
    changed.purpose[0] ^= 1;
    try std.testing.expectError(error.SourcePayloadConflict, prepare(arena.allocator(), &store, @splat(3), changed.purpose, changed));
    command.sequence = 2;
    command.expected_checkpoint = "opaque:z";
    command.checkpoint = "opaque:a";
    command.purpose = @splat(4);
    const second = try prepare(arena.allocator(), &store, @splat(5), command.purpose, command);
    try std.testing.expectEqualSlices(u8, first.value, second.before.?);
    store.value = second.value;
    command.sequence = 3;
    command.expected_checkpoint = "opaque:a";
    command.checkpoint = "opaque:z";
    command.purpose = @splat(6);
    const third = try prepare(arena.allocator(), &store, @splat(7), command.purpose, command);
    try std.testing.expectEqualStrings("opaque:z", (try Head.decode(third.value)).checkpoint);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, prepare(arena.allocator(), &store, @splat(7), @splat(8), command));
    const corrupt = try a.dupe(u8, third.value);
    defer a.free(corrupt);
    corrupt[corrupt.len - 1] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, Head.decode(corrupt));
    command.sequence = 1;
    command.expected_checkpoint = null;
    try std.testing.expectError(error.SourceReceiptExpired, prepare(arena.allocator(), &store, @splat(3), command.purpose, command));
}
