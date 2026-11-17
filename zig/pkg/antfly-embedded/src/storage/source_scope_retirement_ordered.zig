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

//! One bounded retirement outcome per physical root. Success commits with the
//! token floor and transaction resolution; deterministic rejection commits only
//! after the failed writer aborts. Both include the actual ordered marker.
//! Expiry conveys no transaction outcome.
const std = @import("std");
const scope = @import("source_scope.zig");
const retirement = @import("source_scope_retirement.zig");
const A = std.mem.Allocator;
pub const receipt_key = "\x00\x00__metadata__:source_scope:retirement_outcome_v2";
pub const max_record_bytes = 8192;
pub const Rejection = enum {
    invalid,
    scope_changed,
    conflict,
    expired,
    pending,
    backpressure,
    pub fn fromError(err: anyerror) ?Rejection {
        return switch (err) {
            error.InvalidSourceAdmission => .invalid,
            error.HistoryResynchronizationRequired, error.IdentityNamespaceMismatch => .scope_changed,
            error.DecisionConflict, error.SourceCheckpointConflict, error.SourceTransactionIdentityConflict => .conflict,
            error.SourceReceiptExpired => .expired,
            error.SourcePendingWork, error.IntegrityTopologyBusy => .pending,
            error.SourceAdmissionBackpressure => .backpressure,
            else => null,
        };
    }
};
pub const Recovery = union(enum) { applied: retirement.Reply, rejected: Rejection, superseded: void };
pub const Query = struct {
    command: retirement.Command,
    entry: scope.Cut,
    minimum_applied_index: u64,
    pub fn validate(self: Query) !void {
        _ = try self.command.proof();
        if (self.entry.term == 0 or self.entry.index == 0 or self.minimum_applied_index < self.entry.index) return error.InvalidSourceAdmission;
    }
};
// Decoded coordinator admissions contain owned participant/locator slices.
// Keep the parsed arena alive until the owner operation finishes.
pub const max_query_bytes = retirement.max_command_bytes + 1024;
pub fn decodeQuery(a: A, bytes: []const u8) !std.json.Parsed(Query) {
    if (bytes.len == 0 or bytes.len > max_query_bytes) return error.InvalidSourceAdmission;
    var parsed = std.json.parseFromSlice(Query, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
pub fn encodeQuery(a: A, query: Query) ![]u8 {
    try query.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, query, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_query_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
const Response = struct { version: u8 = 1, command: scope.Digest, entry: scope.Cut, outcome: Recovery };
fn validateOutcome(command: retirement.Command, entry: scope.Cut, outcome: Recovery) !void {
    const proof = try command.proof();
    if (entry.term == 0 or entry.index == 0) return error.InvalidSourceAdmission;
    if (outcome == .applied) {
        try outcome.applied.validate(command.intent);
        if (!std.meta.eql(outcome.applied.cut, entry) or !std.meta.eql(outcome.applied.coordinator_record_digest, proof.coordinator_record_digest)) return error.SourceAdmissionCorrupt;
    }
}
pub fn encodeOutcome(a: A, command: retirement.Command, entry: scope.Cut, outcome: Recovery) ![]u8 {
    try validateOutcome(command, entry, outcome);
    const bytes = try std.json.Stringify.valueAlloc(a, Response{ .command = try fingerprint(a, command), .entry = entry, .outcome = outcome }, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeOutcome(a: A, bytes: []const u8, command: retirement.Command, entry: scope.Cut) !Recovery {
    if (bytes.len == 0 or bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Response, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    if (parsed.value.version != 1 or !std.meta.eql(parsed.value.command, try fingerprint(a, command)) or !std.meta.eql(parsed.value.entry, entry)) return error.SourceAdmissionCorrupt;
    try validateOutcome(command, entry, parsed.value.outcome);
    return parsed.value.outcome;
}
const Record = struct { version: u8 = 2, root: scope.Basis, entry: scope.Cut, command: scope.Digest, outcome: Recovery };
const Envelope = struct { record: Record, checksum: scope.Digest };
fn checksum(a: A, record: Record) !scope.Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-scope-retirement-outcome:v2");
    hash.update(bytes);
    return hash.finalResult();
}
pub fn fingerprint(a: A, command: retirement.Command) !scope.Digest {
    const bytes = try retirement.encodeCommand(a, command);
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-scope-retirement-command:v1");
    hash.update(bytes);
    return hash.finalResult();
}
/// Validate the post-mutation scope in this same native writer. The caller must
/// already have validated or resolved the local retained transaction record.
pub fn resolvedReply(a: A, txn: anytype, command: retirement.Command, cut: scope.Cut, already_fenced: bool) !retirement.Reply {
    const proof = try command.proof();
    const header = try scope.Store.readHeader(a, txn, proof.root);
    const record = (try scope.Store.readRecord(a, txn, proof.slot.reference.domain)) orelse return error.SourceOutcomeUnresolved;
    if (!header.strict or header.domains == 0 or !std.meta.eql(record.spec.checkpoint_owner, proof.coordinator) or !std.meta.eql(record.spec.binding, proof.slot.reference.binding) or record.spec.generation != proof.slot.reference.generation or record.closed_through_token < proof.slot.reference.token) return error.SourceAdmissionCorrupt;
    if (record.slot) |slot| {
        if (slot.reference.token <= proof.slot.reference.token) return error.SourceAdmissionCorrupt;
    }
    const reply: retirement.Reply = .{ .version = 1, .request_digest = command.intent.digest(), .coordinator_record_digest = proof.coordinator_record_digest, .cut = cut, .closed_through_token = record.closed_through_token, .disposition = if (already_fenced) .already_fenced else .fenced };
    try reply.validate(command.intent);
    return reply;
}
pub fn stage(a: A, txn: anytype, command: retirement.Command, reply: retirement.Reply) !void {
    const expected = try resolvedReply(a, txn, command, reply.cut, reply.disposition == .already_fenced);
    if (!std.meta.eql(expected, reply)) return error.SourceAdmissionCorrupt;
    try stageOutcome(a, txn, command, reply.cut, .{ .applied = reply });
}
/// Called only after the failed mutation writer was aborted. Unexpected storage
/// errors and unresolved coordinator evidence cannot become durable rejection.
pub fn stageRejected(a: A, txn: anytype, command: retirement.Command, cut: scope.Cut, reason: Rejection) !void {
    try stageOutcome(a, txn, command, cut, .{ .rejected = reason });
}
fn validateRecord(record: Record) !void {
    try record.root.validate();
    if (record.version != 2 or record.entry.term == 0 or record.entry.index == 0 or std.mem.allEqual(u8, &record.command, 0) or record.outcome == .superseded) return error.SourceAdmissionCorrupt;
    if (record.outcome == .applied) {
        const reply = record.outcome.applied;
        if (reply.version != 1 or !std.meta.eql(reply.cut, record.entry) or reply.closed_through_token == 0 or std.mem.allEqual(u8, &reply.request_digest, 0) or std.mem.allEqual(u8, &reply.coordinator_record_digest, 0)) return error.SourceAdmissionCorrupt;
    }
}
fn stageOutcome(a: A, txn: anytype, command: retirement.Command, cut: scope.Cut, outcome: Recovery) !void {
    const proof = try command.proof();
    const record: Record = .{ .root = proof.root, .entry = cut, .command = try fingerprint(a, command), .outcome = outcome };
    try validateRecord(record);
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .record = record, .checksum = try checksum(a, record) }, .{});
    defer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    try txn.put(receipt_key, bytes);
}
/// Physical owner and applied-index qualification belong to the DB caller.
/// Re-read only this exact command/cut receipt, never today's mutable registry.
pub fn replay(a: A, txn: anytype, command: retirement.Command, cut: scope.Cut) !Recovery {
    const proof = try command.proof();
    if (cut.term == 0 or cut.index == 0) return error.InvalidSourceAdmission;
    const bytes = txn.get(receipt_key) catch |err| return if (err == error.NotFound) .superseded else err;
    if (bytes.len == 0 or bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const record = parsed.value.record;
    try validateRecord(record);
    if (!std.meta.eql(parsed.value.checksum, try checksum(a, record))) return error.SourceAdmissionCorrupt;
    if (!std.meta.eql(record.root, proof.root)) return error.HistoryResynchronizationRequired;
    if (record.entry.index != cut.index) return .superseded;
    if (record.entry.term != cut.term or !std.meta.eql(record.command, try fingerprint(a, command))) return error.ConflictingOrderedApplyReceipt;
    if (record.outcome == .applied) {
        try record.outcome.applied.validate(command.intent);
        if (!std.meta.eql(record.outcome.applied.coordinator_record_digest, proof.coordinator_record_digest)) return error.SourceAdmissionCorrupt;
    }
    return record.outcome;
}
