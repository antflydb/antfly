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

//! Coordinator-authorized local attempt retirement. Command observations must
//! originate in an independent qualified ReadIndex read, never an HTTP caller's
//! assertion. The trusted proposal boundary supplies that provenance; structural
//! validation here cannot manufacture it from a digest or absent record.
const std = @import("std");
const scope = @import("source_scope.zig");
const completion = @import("transaction_completion.zig");
const decision = @import("history_decision_wire.zig");
const A = std.mem.Allocator;
pub const max_intent_bytes = 8192;
pub const max_command_bytes = decision.max_reply_bytes + max_intent_bytes + 1024;
pub const max_reply_bytes = 4096;
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.meta.eql(a, b);
}
fn nonzero(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
pub const Intent = struct {
    version: u8,
    owner: completion.RoutedOwner,
    coordinator: decision.Query,
    reference: scope.Reference,
    source_purpose: scope.Digest,
    admission_digest: scope.Digest,
    pub fn validate(self: Intent) !void {
        try self.owner.validate();
        try self.coordinator.validate();
        try self.reference.validate();
        if (self.version != 1 or !nonzero(self.source_purpose) or !nonzero(self.admission_digest)) return error.InvalidSourceAdmission;
    }
    pub fn digest(self: Intent) scope.Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-scope-abort-intent:v1");
        hash.update(&.{self.version});
        hash.update(&self.owner.digest());
        hash.update(&self.coordinator.digest());
        const reference = self.reference.encode() catch return @splat(0);
        hash.update(&reference);
        hash.update(&self.source_purpose);
        hash.update(&self.admission_digest);
        return hash.finalResult();
    }
};
pub const Proof = struct {
    root: scope.Basis,
    coordinator: scope.Basis,
    slot: scope.Slot,
    coordinator_record_digest: scope.Digest,
};
pub const Command = struct {
    version: u8,
    intent: Intent,
    observation: decision.Reply,
    pub fn proof(self: Command) !Proof {
        try self.intent.validate();
        if (self.version != 1) return error.InvalidSourceAdmission;
        try self.observation.validate(self.intent.coordinator);
        const observed = self.observation.decision orelse return error.SourceOutcomeUnresolved;
        if (observed.record.status != .aborted) return error.SourceOutcomeUnresolved;
        if (!observed.record.intents_resolved or observed.record.source_scope == null or !same(observed.record.source_scope.?, self.intent.reference) or observed.record.source_purpose == null or !same(observed.record.source_purpose.?, self.intent.source_purpose) or !same(observed.admission.digest, self.intent.admission_digest)) return error.SourceTransactionIdentityConflict;
        const plan = observed.admission.admission;
        try plan.requireRoutedOwner(self.intent.owner);
        const coordinator = try plan.member(plan.coordinator_namespace);
        if (coordinator.roles & completion.Roles.checkpoint == 0) return error.InvalidSourceAdmission;
        return .{ .root = try scope.Basis.fromOwner(self.intent.owner), .coordinator = try scope.Basis.fromOwner(coordinator.routed_owner.?), .slot = .{ .reference = self.intent.reference, .transaction_id = plan.transaction_id, .source_purpose = self.intent.source_purpose, .admission_digest = observed.admission.digest }, .coordinator_record_digest = observed.record.digest };
    }
};
pub const Reply = struct {
    version: u8,
    request_digest: scope.Digest,
    coordinator_record_digest: scope.Digest,
    cut: scope.Cut,
    closed_through_token: u64,
    disposition: enum { fenced, already_fenced },
    pub fn validate(self: Reply, intent: Intent) !void {
        try intent.validate();
        if (self.version != 1 or !same(self.request_digest, intent.digest()) or !nonzero(self.coordinator_record_digest) or self.cut.term == 0 or self.cut.index == 0 or self.closed_through_token < intent.reference.token) return error.SourceAdmissionCorrupt;
    }
};
/// Missing local record must be established in this same write transaction.
/// The caller must abort on any error and append the ordered apply marker in
/// the same commit. This is a local fencing receipt, never a global outcome.
pub fn applyUnenlisted(a: A, txn: anytype, command: Command, applied: scope.Cut) !Reply {
    const proof = try command.proof();
    var record_key: [completion.record_prefix.len + 16]u8 = undefined;
    @memcpy(record_key[0..completion.record_prefix.len], completion.record_prefix);
    @memcpy(record_key[completion.record_prefix.len..], &proof.slot.transaction_id);
    const present = blk: {
        _ = txn.get(&record_key) catch |err| switch (@as(anyerror, err)) {
            error.NotFound => break :blk false,
            else => return err,
        };
        break :blk true;
    };
    if (present) return error.SourceOutcomeUnresolved;
    const result = try scope.Store.retireUnenlisted(a, txn, proof.root, proof.slot, proof.coordinator, applied);
    const reply: Reply = .{ .version = 1, .request_digest = command.intent.digest(), .coordinator_record_digest = proof.coordinator_record_digest, .cut = applied, .closed_through_token = result.floor, .disposition = if (result.changed) .fenced else .already_fenced };
    try reply.validate(command.intent);
    return reply;
}
pub fn decodeIntent(a: A, bytes: []const u8) !Intent {
    if (bytes.len == 0 or bytes.len > max_intent_bytes) return error.InvalidSourceAdmission;
    var parsed = std.json.parseFromSlice(Intent, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn decodeCommand(a: A, bytes: []const u8) !std.json.Parsed(Command) {
    if (bytes.len == 0 or bytes.len > max_command_bytes) return error.InvalidSourceAdmission;
    var parsed = std.json.parseFromSlice(Command, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    errdefer parsed.deinit();
    _ = try parsed.value.proof();
    return parsed;
}

pub fn encodeIntent(a: A, intent: Intent) ![]u8 {
    try intent.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, intent, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_intent_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn encodeCommand(a: A, command: Command) ![]u8 {
    _ = try command.proof();
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_command_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn decodeReply(a: A, bytes: []const u8, intent: Intent, coordinator_record_digest: scope.Digest) !Reply {
    if (bytes.len == 0 or bytes.len > max_reply_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(intent);
    if (!same(parsed.value.coordinator_record_digest, coordinator_record_digest)) return error.SourceAdmissionCorrupt;
    return parsed.value;
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

const Fixture = struct {
    coordinator: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 },
    owner: completion.RoutedOwner = undefined,
    members: [2]completion.Participant = undefined,
    fn init(self: *Fixture) void {
        self.owner = self.coordinator;
        self.owner.namespace = @splat(3);
        self.owner.group_id = 4;
        self.members = .{
            .{ .namespace = self.coordinator.namespace, .owner_fence = self.coordinator.digest(), .roles = completion.Roles.checkpoint, .input_digest = @splat(5), .locator = "checkpoint", .routed_owner = self.coordinator },
            .{ .namespace = self.owner.namespace, .owner_fence = self.owner.digest(), .roles = completion.Roles.effects, .input_digest = @splat(6), .locator = "rows", .routed_owner = self.owner },
        };
    }
    fn spec(self: *Fixture) !scope.Spec {
        return .{ .actor_table_id = 3, .source_id = @splat(7), .generation = 2, .binding = @splat(8), .checkpoint_owner = try scope.Basis.fromOwner(self.coordinator) };
    }
    fn command(self: *Fixture) !Command {
        const binding = try self.spec();
        const reference: scope.Reference = .{ .domain = binding.domain(), .binding = binding.binding, .generation = binding.generation, .token = 10, .sequence = 1 };
        const query: decision.Query = .{ .owner = self.coordinator, .transaction_id = @splat(9), .minimum_applied_index = 5 };
        const admission: completion.Admission = .{ .version = 3, .transaction_id = query.transaction_id, .admission_digest = @splat(11), .coordinator_namespace = self.coordinator.namespace, .coordinator_fence = self.coordinator.digest(), .participants = &self.members };
        return .{ .version = 1, .intent = .{ .version = 1, .owner = self.owner, .coordinator = query, .reference = reference, .source_purpose = @splat(12), .admission_digest = admission.digest() }, .observation = .{ .request_digest = query.digest(), .cut = .{ .term = 2, .index = 6 }, .decision = .{ .record = .{ .status = .aborted, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 0, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(13), .source_purpose = @splat(12), .source_scope = reference }, .admission = .{ .admission = admission, .digest = admission.digest() } } } };
    }
    fn bind(self: *Fixture, a: A, txn: anytype) !scope.Basis {
        const root = try scope.Basis.fromOwner(self.owner);
        _ = try scope.Store.bind(a, txn, root, try self.spec(), .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 10 });
        return root;
    }
};

test "source scope retirement fences ABORT before BEGIN without local outcome tombstones" {
    var fixture: Fixture = .{};
    fixture.init();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root = try fixture.bind(a, &txn);
    const command = try fixture.command();
    const proof = try command.proof();
    const count_before = txn.map.count();
    const reply = try applyUnenlisted(a, &txn, command, .{ .term = 3, .index = 20 });
    try reply.validate(command.intent);
    try std.testing.expectEqual(@TypeOf(reply.disposition).fenced, reply.disposition);
    try std.testing.expectEqual(count_before, txn.map.count());
    try std.testing.expectEqual(command.intent.reference.token, (try scope.Store.readRecord(a, &txn, command.intent.reference.domain)).?.closed_through_token);
    try std.testing.expectError(error.SourceCheckpointConflict, scope.Store.enter(a, &txn, root, proof.slot, proof.coordinator));
    const retry = try applyUnenlisted(a, &txn, command, .{ .term = 3, .index = 21 });
    try std.testing.expectEqual(@TypeOf(reply.disposition).already_fenced, retry.disposition);
    const reply_bytes = try std.json.Stringify.valueAlloc(a, reply, .{});
    try std.testing.expectEqualDeep(reply, try decodeReply(a, reply_bytes, command.intent, proof.coordinator_record_digest));
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, reply_bytes, command.intent, @splat(99)));
    var changed_reply = reply;
    changed_reply.request_digest[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, changed_reply.validate(command.intent));
}

test "source scope retirement requires positive original scoped abort and original owner membership" {
    var fixture: Fixture = .{};
    fixture.init();
    const original = try fixture.command();
    var changed = original;
    changed.observation.decision = null;
    try std.testing.expectError(error.SourceOutcomeUnresolved, changed.proof());
    changed = original;
    changed.observation.decision.?.record.status = .pending;
    changed.observation.decision.?.record.finalized_at_ns = 0;
    changed.observation.decision.?.record.intents_resolved = false;
    try std.testing.expectError(error.SourceOutcomeUnresolved, changed.proof());
    changed = original;
    changed.observation.decision.?.record.status = .committed;
    changed.observation.decision.?.record.commit_version = 30;
    try std.testing.expectError(error.SourceOutcomeUnresolved, changed.proof());
    changed = original;
    changed.intent.reference.token += 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, changed.proof());
    changed = original;
    changed.intent.source_purpose[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, changed.proof());
    changed = original;
    changed.intent.admission_digest[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, changed.proof());
    changed = original;
    changed.intent.owner.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, changed.proof());
    changed = original;
    changed.intent.owner.namespace = @splat(99);
    try std.testing.expectError(error.InvalidParticipant, changed.proof());
}

test "source scope retirement preserves a newer slot and refuses to skip unresolved older work" {
    var fixture: Fixture = .{};
    fixture.init();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    const root = try fixture.bind(a, &txn);
    const command = try fixture.command();
    const proof = try command.proof();
    var newer = proof.slot;
    newer.reference.token = 20;
    newer.reference.sequence = 2;
    newer.transaction_id = @splat(21);
    try scope.Store.enter(a, &txn, root, newer, proof.coordinator);
    _ = try applyUnenlisted(a, &txn, command, .{ .term = 3, .index = 20 });
    const record = (try scope.Store.readRecord(a, &txn, command.intent.reference.domain)).?;
    try std.testing.expectEqualDeep(newer, record.slot.?);
    try std.testing.expectEqual(@as(u64, 10), record.closed_through_token);
    // A higher future abort cannot erase this still-active original slot.
    var future = proof.slot;
    future.reference.token = 30;
    future.reference.sequence = 3;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, scope.Store.retireUnenlisted(a, &txn, root, future, proof.coordinator, .{ .term = 3, .index = 21 }));
}

test "source scope retirement cannot infer coordinator abort or discard an installed record" {
    var fixture: Fixture = .{};
    fixture.init();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    _ = try fixture.bind(a, &txn);
    const command = try fixture.command();
    var key: [completion.record_prefix.len + 16]u8 = undefined;
    @memcpy(key[0..completion.record_prefix.len], completion.record_prefix);
    @memcpy(key[completion.record_prefix.len..], &command.intent.coordinator.transaction_id);
    try txn.put(&key, "existing-native-transaction");
    try std.testing.expectError(error.SourceOutcomeUnresolved, applyUnenlisted(a, &txn, command, .{ .term = 3, .index = 20 }));
    try txn.delete(&key);
    const proof = try command.proof();
    var coordinator_record: scope.Record = .{ .spec = try fixture.spec(), .bound_metadata_index = 10 };
    try std.testing.expectError(error.SourceOutcomeUnresolved, coordinator_record.retireUnenlisted(proof.slot, proof.coordinator, proof.coordinator, .{ .term = 3, .index = 20 }));
}

test "source scope retirement command owns original observation and strict decoding rejects substitutions" {
    const a = std.testing.allocator;
    var fixture: Fixture = .{};
    fixture.init();
    const command = try fixture.command();
    const bytes = try encodeCommand(a, command);
    defer a.free(bytes);
    var owned = try decodeCommand(a, bytes);
    defer owned.deinit();
    @memset(bytes, 0);
    try std.testing.expectEqualDeep(try command.proof(), try owned.value.proof());
    const intent_bytes = try encodeIntent(a, command.intent);
    defer a.free(intent_bytes);
    try std.testing.expectEqualDeep(command.intent, try decodeIntent(a, intent_bytes));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{intent_bytes[0 .. intent_bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeIntent(a, duplicate));
}

test "source scope retirement rejects partial registry restore without its charged strict header" {
    var fixture: Fixture = .{};
    fixture.init();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    _ = try fixture.bind(a, &txn);
    const command = try fixture.command();
    try txn.delete(scope.prefix ++ "header");
    try std.testing.expectError(error.SourceAdmissionCorrupt, applyUnenlisted(a, &txn, command, .{ .term = 3, .index = 20 }));
    try std.testing.expectEqual(@as(u64, 0), (try scope.Store.readRecord(a, &txn, command.intent.reference.domain)).?.closed_through_token);
}
