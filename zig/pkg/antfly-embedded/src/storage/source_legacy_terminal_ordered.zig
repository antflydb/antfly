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

//! One bounded, checksummed original-terminal recovery outcome per root.
//! Superseded receipts prove neither successful recovery nor legacy drainage.
const std = @import("std");
pub const policy = @import("source_producer_policy.zig");
const scope = @import("source_scope.zig");
const A = std.mem.Allocator;
pub const receipt_key = "\x00\x00__metadata__:source_legacy_terminal:ordered_outcome_v1";
pub const max_record_bytes = 4096;
pub const Rejection = enum {
    invalid,
    pending,
    unresolved,
    conflict,
    not_found,
    scope_changed,
    pub fn fromError(err: anyerror) ?Rejection {
        return switch (err) {
            error.InvalidSourceAdmission => .invalid,
            error.SourcePendingWork => .pending,
            error.SourceOutcomeUnresolved => .unresolved,
            error.SourceTransactionIdentityConflict, error.DecisionConflict => .conflict,
            error.TxnNotFound => .not_found,
            error.HistoryResynchronizationRequired => .scope_changed,
            else => null,
        };
    }
    pub fn asError(self: Rejection) anyerror {
        return switch (self) {
            .invalid => error.InvalidSourceAdmission,
            .pending => error.SourcePendingWork,
            .unresolved => error.SourceOutcomeUnresolved,
            .conflict => error.SourceTransactionIdentityConflict,
            .not_found => error.TxnNotFound,
            .scope_changed => error.HistoryResynchronizationRequired,
        };
    }
};
pub const Decision = struct {
    kind: enum { committed, aborted },
    /// Original commit version or original ABORT finalization timestamp.
    timestamp: u64,
};
pub const Outcome = union(enum) { applied: Decision, rejected: Rejection, superseded };
const Record = struct { root: scope.Basis, entry: scope.Cut, command: scope.Digest, outcome: Outcome };
const Envelope = struct { record: Record, checksum: scope.Digest };
fn digest(a: A, domain: []const u8, value: anytype) !scope.Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(domain);
    hash.update(bytes);
    return hash.finalResult();
}
pub fn fingerprint(a: A, command: policy.LegacyTerminalCommand) !scope.Digest {
    try command.validate();
    return digest(a, "antfly:legacy-terminal-command:v1", command);
}
pub fn validateOutcome(outcome: Outcome) !void {
    if (outcome == .applied and outcome.applied.timestamp == 0) return error.SourceAdmissionCorrupt;
}
pub const max_outcome_bytes = 512;
pub fn encodeOutcome(a: A, outcome: Outcome) ![]u8 {
    try validateOutcome(outcome);
    const bytes = try std.json.Stringify.valueAlloc(a, outcome, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_outcome_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeOutcome(a: A, bytes: []const u8) !Outcome {
    if (bytes.len == 0 or bytes.len > max_outcome_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Outcome, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try validateOutcome(parsed.value);
    return parsed.value;
}
fn validateRecord(record: Record) !void {
    record.root.validate() catch return error.SourceAdmissionCorrupt;
    if (record.entry.term == 0 or record.entry.index == 0 or std.mem.allEqual(u8, &record.command, 0) or record.outcome == .superseded) return error.SourceAdmissionCorrupt;
    try validateOutcome(record.outcome);
}
/// Allocate before irreversible effects. The caller stages this exact value
/// together with resolved intents/history or deterministic rejection and marker.
pub fn encodeRecord(a: A, command: policy.LegacyTerminalCommand, entry: scope.Cut, outcome: Outcome) ![]u8 {
    const record: Record = .{ .root = command.proof.root, .entry = entry, .command = try fingerprint(a, command), .outcome = outcome };
    try validateRecord(record);
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .record = record, .checksum = try digest(a, "antfly:legacy-terminal-outcome:v1", record) }, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
/// Caller qualifies physical ownership and an actual applied cut in this read.
pub fn replay(a: A, txn: anytype, command: policy.LegacyTerminalCommand, entry: scope.Cut) !Outcome {
    try command.validate();
    if (entry.term == 0 or entry.index == 0) return error.InvalidSourceAdmission;
    const bytes = txn.get(receipt_key) catch |err| return if (err == error.NotFound) .superseded else err;
    if (bytes.len == 0 or bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const record = parsed.value.record;
    try validateRecord(record);
    if (!std.meta.eql(parsed.value.checksum, try digest(a, "antfly:legacy-terminal-outcome:v1", record))) return error.SourceAdmissionCorrupt;
    if (!std.meta.eql(record.root, command.proof.root)) return error.HistoryResynchronizationRequired;
    if (record.entry.index != entry.index) return .superseded;
    if (record.entry.term != entry.term or !std.meta.eql(record.command, try fingerprint(a, command))) return error.ConflictingOrderedApplyReceipt;
    return record.outcome;
}

const TestTxn = struct {
    value: ?[]const u8 = null,
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        if (!std.mem.eql(u8, key, receipt_key)) return error.NotFound;
        return self.value orelse error.NotFound;
    }
};
test "history control entry legacy terminal receipts bind exact commands and never turn supersession into ACK" {
    const a = std.testing.allocator;
    const owner: @import("transaction_completion.zig").RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    const command: policy.LegacyTerminalCommand = .{ .version = 1, .owner = owner, .proof = .{ .root = try scope.Basis.fromOwner(owner), .transaction_id = @splat(3), .record_digest = @splat(4) } };
    const cut: scope.Cut = .{ .term = 2, .index = 7 };
    var txn: TestTxn = .{};
    try std.testing.expect(try replay(a, &txn, command, cut) == .superseded);
    const bytes = try encodeRecord(a, command, cut, .{ .applied = .{ .kind = .committed, .timestamp = 200 } });
    defer a.free(bytes);
    txn.value = bytes;
    try std.testing.expectEqualDeep(@as(Outcome, .{ .applied = .{ .kind = .committed, .timestamp = 200 } }), try replay(a, &txn, command, cut));
    var wrong = command;
    wrong.proof.record_digest[0] ^= 1;
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, replay(a, &txn, wrong, cut));
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, replay(a, &txn, command, .{ .term = 1, .index = 7 }));
    try std.testing.expect(try replay(a, &txn, command, .{ .term = 2, .index = 8 }) == .superseded);
    const rejected = try encodeRecord(a, command, cut, .{ .rejected = .unresolved });
    defer a.free(rejected);
    txn.value = rejected;
    try std.testing.expectEqualDeep(@as(Outcome, .{ .rejected = .unresolved }), try replay(a, &txn, command, cut));
    const corrupt = try a.dupe(u8, rejected);
    defer a.free(corrupt);
    corrupt[corrupt.len - 3] ^= 1;
    txn.value = corrupt;
    try std.testing.expectError(error.SourceAdmissionCorrupt, replay(a, &txn, command, cut));
    var parsed = try std.json.parseFromSlice(std.json.Value, a, rejected, .{});
    defer parsed.deinit();
    parsed.value.object.getPtr("record").?.object.getPtr("entry").?.object.getPtr("index").?.* = .{ .integer = 8 };
    const tampered = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(tampered);
    txn.value = tampered;
    try std.testing.expectError(error.SourceAdmissionCorrupt, replay(a, &txn, command, cut));
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeRecord(a, command, cut, .superseded));
    try std.testing.expect(Rejection.fromError(error.OutOfMemory) == null);
    try std.testing.expect(Rejection.fromError(error.InvalidTxnRecord) == null);
}
