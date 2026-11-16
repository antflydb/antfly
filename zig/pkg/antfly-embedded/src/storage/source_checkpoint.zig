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
const receipt_header_bytes = 172;
pub const max_record_bytes = receipt_header_bytes + max_checkpoint_bytes;
pub const max_receipt_window: u32 = 65536;
const receipt_prefix = prefix ++ "receipt:";
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
    /// Zero preserves protocol-37 replay. Positive windows require protocol 38.
    receipt_window: u32 = 0,
    pub fn nativeJsonSkipField(self: Command, comptime name: []const u8) bool {
        if (comptime std.mem.eql(u8, name, "receipt_window")) return self.receipt_window == 0;
        return false;
    }
    pub fn jsonStringify(self: Command, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Command).@"struct".field_names) |name| {
            if (!self.nativeJsonSkipField(name)) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }
    pub fn digest(self: Command) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-checkpoint-command:v2");
        hash.update(&self.binding);
        hash.update(&self.purpose);
        for ([_]u64{ self.sequence, self.accepted_at_ns, self.receipt_window, self.checkpoint.len, @intFromBool(self.expected_checkpoint != null) }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .little);
            hash.update(&bytes);
        }
        hash.update(self.checkpoint);
        if (self.expected_checkpoint) |previous| hash.update(previous);
        return hash.finalResult();
    }
    pub fn validate(self: Command) !void {
        if (self.receipt_window > max_receipt_window) return error.InvalidSourceAdmission;
        if (!nonzero(self.binding) or !nonzero(self.purpose) or self.sequence == 0 or self.accepted_at_ns == 0 or self.accepted_at_ns == std.math.maxInt(u64) or self.checkpoint.len == 0 or self.checkpoint.len > max_checkpoint_bytes) return error.InvalidSourceAdmission;
        if (self.expected_checkpoint) |previous| if (previous.len == 0 or previous.len > max_checkpoint_bytes) return error.InvalidSourceAdmission;
        if ((self.sequence == 1) != (self.expected_checkpoint == null)) return error.SourceCheckpointConflict;
    }
};
pub fn isKey(bytes: []const u8) bool {
    return (bytes.len == prefix.len + 32 and std.mem.startsWith(u8, bytes, prefix)) or
        (bytes.len == receipt_prefix.len + 36 and std.mem.startsWith(u8, bytes, receipt_prefix));
}
pub fn key(binding: Digest) [prefix.len + 32]u8 {
    var result: [prefix.len + 32]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &binding);
    return result;
}
pub fn receiptKey(binding: Digest, sequence: u64, window: u32) ![receipt_prefix.len + 36]u8 {
    if (sequence == 0 or window == 0 or window > max_receipt_window) return error.InvalidSourceAdmission;
    var result: [receipt_prefix.len + 36]u8 = undefined;
    @memcpy(result[0..receipt_prefix.len], receipt_prefix);
    @memcpy(result[receipt_prefix.len..][0..32], &binding);
    std.mem.writeInt(u32, result[result.len - 4 ..][0..4], @intCast((sequence - 1) % window), .little);
    return result;
}
fn checksum(bytes: []const u8, receipts: bool) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(if (receipts) "antfly:source-checkpoint-head:v2" else "antfly:source-checkpoint-head:v1");
    const at: usize = if (receipts) 140 else 104;
    hash.update(bytes[0..at]);
    hash.update(bytes[at + 32 ..]);
    return hash.finalResult();
}
pub const Head = struct {
    binding: Digest,
    purpose: Digest,
    transaction_id: [16]u8,
    sequence: u64,
    checkpoint: []const u8,
    accepted_at_ns: u64,
    receipt_window: u32 = 0,
    request_digest: Digest = @splat(0),
    pub fn validate(self: Head) !void {
        if (!nonzero(self.binding) or !nonzero(self.purpose) or !nonzero(self.transaction_id) or self.sequence == 0 or self.accepted_at_ns == 0 or self.accepted_at_ns == std.math.maxInt(u64) or self.checkpoint.len == 0 or self.checkpoint.len > max_checkpoint_bytes or self.receipt_window > max_receipt_window or (self.receipt_window != 0) != nonzero(self.request_digest)) return error.SourceAdmissionCorrupt;
    }
    pub fn encode(self: Head, a: A) ![]u8 {
        try self.validate();
        const receipts = self.receipt_window != 0;
        const header: usize = if (receipts) receipt_header_bytes else header_bytes;
        const bytes = try a.alloc(u8, header + self.checkpoint.len);
        @memcpy(bytes[0..4], if (receipts) "SCP2" else "SCP1");
        std.mem.writeInt(u64, bytes[4..12], self.sequence, .little);
        std.mem.writeInt(u64, bytes[12..20], self.accepted_at_ns, .little);
        @memcpy(bytes[20..52], &self.binding);
        @memcpy(bytes[52..84], &self.purpose);
        @memcpy(bytes[84..100], &self.transaction_id);
        std.mem.writeInt(u32, bytes[100..104], @intCast(self.checkpoint.len), .little);
        if (receipts) {
            std.mem.writeInt(u32, bytes[104..108], self.receipt_window, .little);
            @memcpy(bytes[108..140], &self.request_digest);
        }
        @memcpy(bytes[header..], self.checkpoint);
        @memcpy(bytes[header - 32 ..][0..32], &checksum(bytes, receipts));
        return bytes;
    }
    /// Checkpoint bytes borrow the verified retained record.
    pub fn decode(bytes: []const u8) !Head {
        if (bytes.len < header_bytes) return error.SourceAdmissionCorrupt;
        const receipts = std.mem.eql(u8, bytes[0..4], "SCP2");
        if (!receipts and !std.mem.eql(u8, bytes[0..4], "SCP1")) return error.SourceAdmissionCorrupt;
        const header: usize = if (receipts) receipt_header_bytes else header_bytes;
        if (bytes.len <= header or bytes.len > header + max_checkpoint_bytes or std.mem.readInt(u32, bytes[100..104], .little) != bytes.len - header or !std.mem.eql(u8, bytes[header - 32 ..][0..32], &checksum(bytes, receipts))) return error.SourceAdmissionCorrupt;
        const result: Head = .{ .binding = bytes[20..52].*, .purpose = bytes[52..84].*, .transaction_id = bytes[84..100].*, .sequence = std.mem.readInt(u64, bytes[4..12], .little), .checkpoint = bytes[header..], .accepted_at_ns = std.mem.readInt(u64, bytes[12..20], .little), .receipt_window = if (receipts) std.mem.readInt(u32, bytes[104..108], .little) else 0, .request_digest = if (receipts) bytes[108..140].* else @splat(0) };
        if (receipts and result.receipt_window == 0) return error.SourceAdmissionCorrupt;
        try result.validate();
        return result;
    }
    pub fn requireRetry(self: Head, transaction_id: [16]u8, command: Command) !void {
        if (!std.mem.eql(u8, &self.binding, &command.binding) or self.sequence != command.sequence or self.receipt_window != command.receipt_window) return error.SourceAdmissionCorrupt;
        if (!std.mem.eql(u8, &self.purpose, &command.purpose) or !std.mem.eql(u8, &self.transaction_id, &transaction_id) or !std.mem.eql(u8, self.checkpoint, command.checkpoint) or self.accepted_at_ns != command.accepted_at_ns or (self.receipt_window != 0 and !std.mem.eql(u8, &self.request_digest, &command.digest()))) return error.SourcePayloadConflict;
    }
};
pub const Plan = struct { key: []const u8, before: ?[]const u8, value: []const u8, receipt_key: ?[]const u8 = null };
/// All returned bytes belong to an attempt arena. Nothing is written here.
/// The native transaction MUST predicate on before and resolve value atomically.
pub fn prepare(a: A, txn: anytype, transaction_id: [16]u8, source_purpose: Digest, command: Command) !Plan {
    try command.validate();
    if (!nonzero(transaction_id) or !std.mem.eql(u8, &command.purpose, &source_purpose)) return error.SourceTransactionIdentityConflict;
    const physical_key = try a.dupe(u8, &key(command.binding));
    const borrowed = txn.get(physical_key) catch |err| switch (@as(anyerror, err)) {
        error.NotFound => null,
        else => return err,
    };
    // Probe transactions may reuse scratch across lookups.
    const before = if (borrowed) |bytes| try a.dupe(u8, bytes) else null;
    const previous = if (before) |bytes| try Head.decode(bytes) else null;
    if (previous) |head| {
        if (!std.mem.eql(u8, &head.binding, &command.binding)) return error.SourceAdmissionCorrupt;
        if (head.receipt_window != command.receipt_window) return error.SourceCheckpointConflict;
        if (head.sequence >= command.sequence) {
            if (head.sequence == command.sequence) {
                try head.requireRetry(transaction_id, command);
            } else {
                if (command.receipt_window == 0 or head.sequence - command.sequence >= command.receipt_window) return error.SourceReceiptExpired;
                const retained_key = try receiptKey(command.binding, command.sequence, command.receipt_window);
                const raw = txn.get(&retained_key) catch |err| switch (@as(anyerror, err)) {
                    error.NotFound => return error.SourceAdmissionCorrupt,
                    else => return err,
                };
                try (try Head.decode(raw)).requireRetry(transaction_id, command);
            }
            return error.SourceAlreadyCommitted;
        }
        if (head.sequence == std.math.maxInt(u64) or head.sequence + 1 != command.sequence) return error.SourceCheckpointConflict;
    } else if (command.sequence != 1) return error.SourceCheckpointConflict;
    if (!equalOptional(command.expected_checkpoint, if (previous) |head| head.checkpoint else null)) return error.SourceCheckpointConflict;
    const next: Head = .{ .binding = command.binding, .purpose = command.purpose, .transaction_id = transaction_id, .sequence = command.sequence, .checkpoint = command.checkpoint, .accepted_at_ns = command.accepted_at_ns, .receipt_window = command.receipt_window, .request_digest = if (command.receipt_window != 0) command.digest() else @splat(0) };
    return .{ .key = physical_key, .before = before, .value = try next.encode(a), .receipt_key = if (command.receipt_window != 0) try a.dupe(u8, &try receiptKey(command.binding, command.sequence, command.receipt_window)) else null };
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

test "source checkpoint receipt ring preserves exact historical retries with bounded slots" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const Store = struct {
        values: std.StringHashMap([]const u8),
        fn get(self: *@This(), name: []const u8) ![]const u8 {
            return self.values.get(name) orelse error.NotFound;
        }
        fn commit(self: *@This(), plan: Plan) !void {
            try self.values.put(plan.key, plan.value);
            if (plan.receipt_key) |slot| try self.values.put(slot, plan.value);
        }
    };
    var store: Store = .{ .values = .init(scratch) };
    var commands: [5]Command = undefined;
    for (&commands, 0..) |*command, i| {
        command.* = .{ .binding = @splat(1), .purpose = @splat(@as(u8, @intCast(i + 2))), .sequence = i + 1, .checkpoint = if (i % 2 == 0) "opaque:z" else "opaque:a", .expected_checkpoint = if (i == 0) null else if (i % 2 == 0) "opaque:a" else "opaque:z", .accepted_at_ns = 100 + i, .receipt_window = 3 };
        const plan = try prepare(scratch, &store, @splat(@as(u8, @intCast(i + 20))), command.purpose, command.*);
        try std.testing.expectEqual(@min(i, 3) + @as(usize, @intFromBool(i != 0)), store.values.count());
        try std.testing.expect(isKey(plan.receipt_key.?));
        try store.commit(plan);
        try std.testing.expectEqual(@min(i + 1, 3) + @as(usize, 1), store.values.count());
    }
    for (2..5) |i| try std.testing.expectError(error.SourceAlreadyCommitted, prepare(scratch, &store, @splat(@as(u8, @intCast(i + 20))), commands[i].purpose, commands[i]));
    try std.testing.expectError(error.SourceReceiptExpired, prepare(scratch, &store, @splat(21), commands[1].purpose, commands[1]));
    var changed = commands[2];
    changed.expected_checkpoint = "different predecessor";
    try std.testing.expectError(error.SourcePayloadConflict, prepare(scratch, &store, @splat(22), changed.purpose, changed));
    changed = commands[2];
    changed.receipt_window = 4;
    try std.testing.expectError(error.SourceCheckpointConflict, prepare(scratch, &store, @splat(22), changed.purpose, changed));
    const slot = try receiptKey(commands[2].binding, commands[2].sequence, 3);
    _ = store.values.remove(&slot);
    try std.testing.expectError(error.SourceAdmissionCorrupt, prepare(scratch, &store, @splat(22), commands[2].purpose, commands[2]));
    var invalid = commands[0];
    invalid.receipt_window = max_receipt_window + 1;
    try std.testing.expectError(error.InvalidSourceAdmission, invalid.validate());
    invalid.receipt_window = 0;
    const legacy_json = try std.json.Stringify.valueAlloc(a, invalid, .{});
    defer a.free(legacy_json);
    try std.testing.expect(std.mem.indexOf(u8, legacy_json, "receipt_window") == null);
}
