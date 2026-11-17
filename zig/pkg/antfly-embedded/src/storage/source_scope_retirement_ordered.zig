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

//! One bounded successful retirement receipt per physical root. It commits
//! after the token floor and transaction resolution, before the same batch's
//! actual ordered marker becomes durable. Expiry conveys no transaction outcome.
const std = @import("std");
const scope = @import("source_scope.zig");
const retirement = @import("source_scope_retirement.zig");
const A = std.mem.Allocator;
pub const receipt_key = "\x00\x00__metadata__:source_scope:retirement_outcome";
pub const max_record_bytes = 8192;
const Record = struct { root: scope.Basis, reply: retirement.Reply };
const Envelope = struct { record: Record, checksum: scope.Digest };
pub const Recovery = union(enum) { applied: retirement.Reply, superseded: void };
fn checksum(a: A, record: Record) !scope.Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-scope-retirement-outcome:v1");
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
    const record: Record = .{ .root = try scope.Basis.fromOwner(command.intent.owner), .reply = reply };
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .record = record, .checksum = try checksum(a, record) }, .{});
    defer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    try txn.put(receipt_key, bytes);
}
/// Requires independent physical owner and applied-index qualification. Do not
/// re-read the live scope: a newer attempt may have advanced it after this cut.
pub fn replay(a: A, txn: anytype, command: retirement.Command, cut: scope.Cut) !Recovery {
    const proof = try command.proof();
    if (cut.term == 0 or cut.index == 0) return error.InvalidSourceAdmission;
    const bytes = txn.get(receipt_key) catch |err| return if (err == error.NotFound) .superseded else err;
    if (bytes.len == 0 or bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const record = parsed.value.record;
    try record.root.validate();
    if (!std.meta.eql(parsed.value.checksum, try checksum(a, record))) return error.SourceAdmissionCorrupt;
    if (!std.meta.eql(record.root, proof.root)) return error.HistoryResynchronizationRequired;
    if (record.reply.cut.term == 0 or record.reply.cut.index == 0 or record.reply.version != 1 or std.mem.allEqual(u8, &record.reply.request_digest, 0) or std.mem.allEqual(u8, &record.reply.coordinator_record_digest, 0)) return error.SourceAdmissionCorrupt;
    if (record.reply.cut.index != cut.index) return .superseded;
    if (record.reply.cut.term != cut.term or !std.meta.eql(record.reply.request_digest, command.intent.digest()) or !std.meta.eql(record.reply.coordinator_record_digest, proof.coordinator_record_digest)) return error.ConflictingOrderedApplyReceipt;
    try record.reply.validate(command.intent);
    return .{ .applied = record.reply };
}
