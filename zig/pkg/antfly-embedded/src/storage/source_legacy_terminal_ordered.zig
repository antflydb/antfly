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
/// Read-only original-command recovery. A supplied position is only a lower
/// read bound; it is not proof that the proposal originally committed.
pub const Query = struct {
    version: u8,
    owner: @import("transaction_completion.zig").RoutedOwner,
    command: policy.LegacyTerminalCommand,
    entry: scope.Cut,
    minimum_applied_index: u64,
    pub fn validate(self: Query) !void {
        try self.command.validate();
        if (self.version != 1 or !std.meta.eql(self.owner, self.command.owner) or self.entry.term == 0 or self.entry.index == 0 or self.minimum_applied_index < self.entry.index) return error.InvalidSourceAdmission;
    }
};
pub const Observation = struct {
    version: u8,
    owner: @import("transaction_completion.zig").RoutedOwner,
    command_digest: scope.Digest,
    entry: scope.Cut,
    cut: scope.Cut,
    outcome: Outcome,
    pub fn validate(self: Observation, a: A, query: Query) !void {
        try query.validate();
        if (self.version != 1 or !std.meta.eql(self.owner, query.owner) or !std.meta.eql(self.command_digest, try fingerprint(a, query.command)) or !std.meta.eql(self.entry, query.entry) or self.cut.term < query.entry.term or self.cut.index < query.minimum_applied_index or (self.cut.index == query.entry.index and self.cut.term != query.entry.term)) return error.SourceAdmissionCorrupt;
        try validateOutcome(self.outcome);
    }
};
pub const max_query_bytes = 12 * 1024;
pub const max_observation_bytes = 8192;
fn requireOwnerMaterial(value: std.json.Value) !void {
    if (value != .object) return error.InvalidSourceAdmission;
    for ([_][]const u8{ "version", "metadata_group_id", "metadata_incarnation", "namespace", "table_binding", "root_generation", "group_id", "topology_epoch" }) |name| if (!value.object.contains(name)) return error.InvalidSourceAdmission;
}
pub fn encodeQuery(a: A, query: Query) ![]u8 {
    try query.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, query, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_query_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn decodeQuery(a: A, bytes: []const u8) !Query {
    if (bytes.len == 0 or bytes.len > max_query_bytes) return error.InvalidSourceAdmission;
    var raw = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer raw.deinit();
    if (raw.value != .object) return error.InvalidSourceAdmission;
    try requireOwnerMaterial(raw.value.object.get("owner") orelse return error.InvalidSourceAdmission);
    const command_bytes = try std.json.Stringify.valueAlloc(a, raw.value.object.get("command") orelse return error.InvalidSourceAdmission, .{});
    defer a.free(command_bytes);
    _ = try policy.decodeLegacyTerminalCommand(a, command_bytes);
    var parsed = std.json.parseFromValue(Query, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeObservation(a: A, query: Query, observation: Observation) ![]u8 {
    try observation.validate(a, query);
    const bytes = try std.json.Stringify.valueAlloc(a, observation, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_observation_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeObservation(a: A, bytes: []const u8, query: Query) !Observation {
    try query.validate();
    if (bytes.len == 0 or bytes.len > max_observation_bytes) return error.SourceAdmissionCorrupt;
    var raw = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer raw.deinit();
    if (raw.value != .object) return error.SourceAdmissionCorrupt;
    requireOwnerMaterial(raw.value.object.get("owner") orelse return error.SourceAdmissionCorrupt) catch return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromValue(Observation, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(a, query);
    return parsed.value;
}
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

test "history control entry legacy recovery reads bind original command entry and qualified cut" {
    const a = std.testing.allocator;
    const owner: @import("transaction_completion.zig").RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    const query: Query = .{ .version = 1, .owner = owner, .command = .{ .version = 1, .owner = owner, .proof = .{ .root = try scope.Basis.fromOwner(owner), .transaction_id = @splat(3), .record_digest = @splat(4) } }, .entry = .{ .term = 2, .index = 7 }, .minimum_applied_index = 7 };
    const request = try encodeQuery(a, query);
    defer a.free(request);
    try std.testing.expectEqualDeep(query, try decodeQuery(a, request));
    const observation: Observation = .{ .version = 1, .owner = owner, .command_digest = try fingerprint(a, query.command), .entry = query.entry, .cut = .{ .term = 2, .index = 8 }, .outcome = .{ .applied = .{ .kind = .committed, .timestamp = 200 } } };
    const bytes = try encodeObservation(a, query, observation);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(observation, try decodeObservation(a, bytes, query));
    var future = query;
    future.minimum_applied_index = 9;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeObservation(a, bytes, future));
    var changed = query;
    changed.command.proof.record_digest[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeObservation(a, bytes, changed));
    changed = query;
    changed.entry.index = 6;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeObservation(a, bytes, changed));
    var forged = observation;
    forged.command_digest[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeObservation(a, query, forged));
    forged = observation;
    forged.cut.index = 6;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeObservation(a, query, forged));
    forged = observation;
    forged.cut = .{ .term = 3, .index = 7 };
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeObservation(a, query, forged));
    forged = observation;
    forged.outcome.applied.timestamp = 0;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeObservation(a, query, forged));
    var unknown = observation;
    unknown.outcome = .superseded;
    const unknown_bytes = try encodeObservation(a, query, unknown);
    defer a.free(unknown_bytes);
    try std.testing.expect((try decodeObservation(a, unknown_bytes, query)).outcome == .superseded);
    var raw = try std.json.parseFromSlice(std.json.Value, a, request, .{});
    defer raw.deinit();
    _ = raw.value.object.getPtr("owner").?.object.swapRemove("root_generation");
    const omitted = try std.json.Stringify.valueAlloc(a, raw.value, .{});
    defer a.free(omitted);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeQuery(a, omitted));
    const extra = try std.fmt.allocPrint(a, "{s},\"operation\":\"submit\"}}", .{request[0 .. request.len - 1]});
    defer a.free(extra);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeQuery(a, extra));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeObservation(a, duplicate, query));
}
