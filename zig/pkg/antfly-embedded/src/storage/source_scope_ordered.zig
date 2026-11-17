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

//! One bounded ordered scope-control receipt per physical root. Superseded
//! receipts require a qualified live observation, never blind command replay.
const std = @import("std");
pub const control = @import("source_scope_control.zig");
const scope = control.scope;
const A = std.mem.Allocator;
pub const receipt_key = "\x00\x00__metadata__:source_scope:ordered_outcome";
pub const max_record_bytes = 12 * 1024;
pub const Rejection = enum {
    invalid,
    scope_changed,
    pending,
    conflict,
    backpressure,
    pub fn fromError(err: anyerror) ?Rejection {
        return switch (err) {
            error.InvalidSourceAdmission => .invalid,
            error.HistoryResynchronizationRequired, error.IdentityNamespaceMismatch => .scope_changed,
            error.SourcePendingWork, error.IntegrityTopologyBusy => .pending,
            error.SourceCheckpointConflict, error.SourceTransactionIdentityConflict => .conflict,
            error.SourceAdmissionBackpressure => .backpressure,
            else => null,
        };
    }
    pub fn asError(self: Rejection) anyerror {
        return switch (self) {
            .invalid => error.InvalidSourceAdmission,
            .scope_changed => error.HistoryResynchronizationRequired,
            .pending => error.SourcePendingWork,
            .conflict => error.SourceCheckpointConflict,
            .backpressure => error.SourceAdmissionBackpressure,
        };
    }
};
pub const Outcome = union(enum) { applied: control.Observation, rejected: Rejection, superseded: void };
pub const Recovery = struct {
    command: control.Command,
    entry: scope.Cut,
    minimum_applied_index: u64,
    pub fn validate(self: Recovery) !void {
        try self.command.validate();
        if (self.entry.term == 0 or self.entry.index == 0 or self.minimum_applied_index < self.entry.index) return error.InvalidSourceAdmission;
    }
};
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
pub fn fingerprint(a: A, command: control.Command) !scope.Digest {
    try command.validate();
    return digest(a, "antfly:source-scope-command:v1", command);
}
fn validateApplied(command: control.Command, entry: scope.Cut, observed: control.Observation) !void {
    try observed.validate(.{ .version = 1, .owner = command.intent.owner, .domain = command.intent.spec.domain(), .minimum_applied_index = entry.index });
    if (!std.meta.eql(observed.cut, entry) or !observed.header.strict or observed.header.metadata_floor < command.metadata_cut.index) return error.SourceAdmissionCorrupt;
    if (observed.record) |record| {
        if (!std.meta.eql(record.spec, command.intent.spec)) return error.SourceAdmissionCorrupt;
    }
    switch (command.intent.action) {
        .bind => if (observed.record == null or observed.record.?.phase != .active) return error.SourceAdmissionCorrupt,
        .close => if (observed.record != null and observed.record.?.phase == .active) return error.SourceAdmissionCorrupt,
        .drop => if (observed.record != null) return error.SourceAdmissionCorrupt,
    }
}
fn validateRecord(record: Record) !void {
    record.root.validate() catch return error.SourceAdmissionCorrupt;
    if (record.entry.term == 0 or record.entry.index == 0 or std.mem.allEqual(u8, &record.command, 0) or record.outcome == .superseded) return error.SourceAdmissionCorrupt;
    if (record.outcome == .applied) {
        const observed = record.outcome.applied;
        try observed.validate(.{ .version = 1, .owner = observed.owner, .domain = observed.domain, .minimum_applied_index = record.entry.index });
        if (!std.meta.eql(observed.header.root, record.root) or !std.meta.eql(observed.cut, record.entry)) return error.SourceAdmissionCorrupt;
    }
}
fn load(a: A, txn: anytype) !?Record {
    const bytes = txn.get(receipt_key) catch |err| return if (err == error.NotFound) null else err;
    if (bytes.len == 0 or bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Envelope, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const record = parsed.value.record;
    if (!std.meta.eql(parsed.value.checksum, try digest(a, "antfly:source-scope-outcome:v1", record))) return error.SourceAdmissionCorrupt;
    try validateRecord(record);
    return record;
}
/// Must share the scope mutation/rejection transaction and actual apply marker.
/// Unexpected storage errors and corruption are not converted to rejections.
pub fn stage(a: A, txn: anytype, command: control.Command, entry: scope.Cut, outcome: Outcome) !void {
    try command.validate();
    if (outcome == .applied) try validateApplied(command, entry, outcome.applied);
    const record: Record = .{ .root = try scope.Basis.fromOwner(command.intent.owner), .entry = entry, .command = try fingerprint(a, command), .outcome = outcome };
    try validateRecord(record);
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .record = record, .checksum = try digest(a, "antfly:source-scope-outcome:v1", record) }, .{});
    defer a.free(bytes);
    if (bytes.len > max_record_bytes) return error.SourceAdmissionCorrupt;
    try txn.put(receipt_key, bytes);
}
/// The native owner must qualify physical identity and actual applied index
/// before this read. Receipt expiry certifies neither success nor failure.
pub fn replay(a: A, txn: anytype, command: control.Command, entry: scope.Cut) !Outcome {
    try command.validate();
    if (entry.term == 0 or entry.index == 0) return error.InvalidSourceAdmission;
    const record = (try load(a, txn)) orelse return .superseded;
    if (!std.meta.eql(record.root, try scope.Basis.fromOwner(command.intent.owner))) return error.HistoryResynchronizationRequired;
    if (record.entry.index != entry.index) return .superseded;
    if (record.entry.term != entry.term or !std.meta.eql(record.command, try fingerprint(a, command))) return error.ConflictingOrderedApplyReceipt;
    if (record.outcome == .applied) try validateApplied(command, entry, record.outcome.applied);
    return record.outcome;
}

const TestTxn = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    pub fn deinit(self: *@This()) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
            std.testing.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit(std.testing.allocator);
    }
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.map.get(key) orelse error.NotFound;
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const a = std.testing.allocator;
        const k = try a.dupe(u8, key);
        errdefer a.free(k);
        const v = try a.dupe(u8, value);
        errdefer a.free(v);
        const slot = try self.map.getOrPut(a, k);
        if (slot.found_existing) {
            a.free(k);
            a.free(slot.value_ptr.*);
        }
        slot.value_ptr.* = v;
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        const entry = self.map.fetchRemove(key) orelse return error.NotFound;
        std.testing.allocator.free(entry.key);
        std.testing.allocator.free(entry.value);
    }
};

fn fixture() control.Command {
    const owner: @import("transaction_completion.zig").RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    return .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = scope.Basis.fromOwner(owner) catch unreachable }, .minimum_metadata_index = 10, .action = .bind }, .metadata_cut = .{ .group_id = 1, .incarnation = owner.metadata_incarnation, .index = 10 } };
}

test "ordered scope receipts are bounded exact and never invent expired outcomes" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    const command = fixture();
    const entry: scope.Cut = .{ .term = 1, .index = 20 };
    try (Recovery{ .command = command, .entry = entry, .minimum_applied_index = entry.index }).validate();
    try std.testing.expectError(error.InvalidSourceAdmission, (Recovery{ .command = command, .entry = entry, .minimum_applied_index = entry.index - 1 }).validate());
    try std.testing.expect(try replay(a, &txn, command, entry) == .superseded);
    try stage(a, &txn, command, entry, .{ .rejected = .pending });
    try std.testing.expectEqual(Rejection.pending, (try replay(a, &txn, command, entry)).rejected);
    var altered = command;
    altered.metadata_cut.index += 1;
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, replay(a, &txn, altered, entry));
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, replay(a, &txn, command, .{ .term = 2, .index = 20 }));
    try stage(a, &txn, altered, .{ .term = 2, .index = 21 }, .{ .rejected = .conflict });
    try std.testing.expect(try replay(a, &txn, command, entry) == .superseded);
    try std.testing.expectEqual(@as(u32, 1), txn.map.count());
    try std.testing.expectError(error.SourceAdmissionCorrupt, stage(a, &txn, command, entry, .superseded));
    try std.testing.expect(Rejection.fromError(error.SourceAdmissionCorrupt) == null);
    try std.testing.expect(Rejection.fromError(error.OutOfMemory) == null);
    const bytes = txn.map.getPtr(receipt_key).?.*;
    bytes[bytes.len - 2] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, replay(a, &txn, altered, .{ .term = 2, .index = 21 }));
}
test "ordered scope success binds the exact mutation and actual local cut" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    const command = fixture();
    const entry: scope.Cut = .{ .term = 1, .index = 20 };
    var observed: control.Observation = .{ .version = 1, .owner = command.intent.owner, .domain = command.intent.spec.domain(), .cut = entry, .header = .{ .root = try scope.Basis.fromOwner(command.intent.owner), .strict = true, .metadata_floor = command.metadata_cut.index, .domains = 1 }, .record = .{ .spec = command.intent.spec, .bound_metadata_index = command.metadata_cut.index } };
    try stage(a, &txn, command, entry, .{ .applied = observed });
    try std.testing.expectEqualDeep(observed, (try replay(a, &txn, command, entry)).applied);
    observed.cut.index += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, stage(a, &txn, command, entry, .{ .applied = observed }));
    observed.cut = entry;
    observed.record = null;
    try std.testing.expectError(error.SourceAdmissionCorrupt, stage(a, &txn, command, entry, .{ .applied = observed }));
    var close = command;
    close.intent.action = .close;
    try stage(a, &txn, close, entry, .{ .applied = observed });
    try std.testing.expectEqualDeep(observed, (try replay(a, &txn, close, entry)).applied);
    var wrong_root = close;
    wrong_root.intent.owner.root_generation += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, replay(a, &txn, wrong_root, entry));
}
