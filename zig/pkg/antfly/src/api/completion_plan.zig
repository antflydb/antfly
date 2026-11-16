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

//! Owned input for the completion-aware coordinator. Freeze before the first
//! BEGIN. Retry from these bytes, never from current routing, schema or rows.
const std = @import("std");
const local = @import("antfly_local_sources");
const batch = local.api_internal_batch;
const types = local.storage_db_selected_root.db.types;
pub const completion = local.api_local_table_writes.table_write_source.history_resolution_wire.completion;
const locators = local.api_local_transaction_contract;
const frozen_storage = local.storage_source_plan_storage;
const A = std.mem.Allocator;

pub const Input = struct {
    owner: completion.RoutedOwner,
    locator: []const u8,
    roles: u8,
    prepare: types.BatchRequest,
};
pub const Participant = struct {
    owner: completion.RoutedOwner,
    locator: []const u8,
    /// Canonical complete private input. Owned for the coordinator's lifetime.
    prepare_bytes: []const u8,
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    admission: completion.Admission,
    admission_bytes: []const u8,
    participants: []const Participant,
    participant_ids: []const []const u8,
    begin_timestamp: u64,
    created_at_ns: u64,
    source_purpose: ?[32]u8,
    source_checkpoint: bool = false,
    source_checkpoint_receipts: bool = false,

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Owner capture and route activity leases belong to the caller. Structural
    /// validation here never creates authority or a lease from a digest.
    pub fn freeze(a: A, transaction_id: [16]u8, admission_digest: completion.Digest, coordinator: completion.RoutedOwner, inputs: []const Input, created_at_ns: u64) !Plan {
        if (created_at_ns == 0 or inputs.len == 0 or inputs.len > completion.max_participants) return error.InvalidTxnRecord;
        try coordinator.validate();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const participants = try owned.alloc(Participant, inputs.len);
        const members = try owned.alloc(completion.Participant, inputs.len);
        var begin_timestamp: u64 = 0;
        var source_purpose: ?[32]u8 = null;
        var checkpoint_count: usize = 0;
        var source_checkpoint = false;
        var source_checkpoint_receipts = false;
        var total_payload_bytes: usize = 0;
        var unique_locators: std.StringHashMapUnmanaged(void) = .empty;
        for (inputs, 0..) |input, i| {
            try input.owner.validate();
            const locator = locators.parseParticipantRef(input.locator) orelse return error.InvalidParticipant;
            if (locator.group_id != input.owner.group_id or locator.restore_staging_scope != null or locator.restore_staging_plan_id != null or input.locator.len > completion.max_locator_bytes) return error.InvalidParticipant;
            const mutation = input.prepare.transaction orelse return error.InvalidBatchRequest;
            if (mutation != .prepare) return error.InvalidBatchRequest;
            const vote = mutation.prepare;
            if (i == 0) source_purpose = vote.source_purpose else if (!std.meta.eql(source_purpose, vote.source_purpose)) return error.SourceTransactionIdentityConflict;
            if (input.roles & completion.Roles.checkpoint != 0) checkpoint_count += 1;
            if (vote.source_checkpoint) |checkpoint| {
                try checkpoint.validate();
                if (vote.source_purpose == null or !std.mem.eql(u8, &checkpoint.purpose, &vote.source_purpose.?) or input.roles & completion.Roles.checkpoint == 0) return error.InvalidSourceAdmission;
                source_checkpoint = true;
                source_checkpoint_receipts = checkpoint.receipt_window != 0;
            }
            const owner = vote.completion_owner orelse return error.InvalidBatchRequest;
            if (!std.mem.eql(u8, &vote.txn_id, &transaction_id)) return error.SourceTransactionIdentityConflict;
            if (!std.mem.eql(u8, &owner.digest(), &input.owner.digest()) or vote.topology_epoch != input.owner.topology_epoch) return error.TopologyChanged;
            // Normalizing either field after freezing changes the vote's hash.
            if (input.prepare.timestamp_ns == 0 or input.prepare.sync_level != .write or input.prepare.document_version_scheme != .revision_v1 or input.prepare.restore_staging_scope != null or input.prepare.restore_staging_plan_id != null) return error.InvalidBatchRequest;
            if (i == 0) begin_timestamp = input.prepare.timestamp_ns else if (begin_timestamp != input.prepare.timestamp_ns) return error.SourceTransactionIdentityConflict;
            if ((input.prepare.writes.len != 0 or input.prepare.deletes.len != 0 or input.prepare.transforms.len != 0 or input.prepare.graph_writes.len != 0 or input.prepare.graph_deletes.len != 0) and input.roles & completion.Roles.effects == 0) return error.InvalidTxnRecord;
            if ((input.prepare.predicates.len != 0 or input.prepare.range_guards.len != 0) and input.roles & completion.Roles.guards == 0) return error.InvalidTxnRecord;
            const bytes = try batch.completionPrepareInput(owned, input.prepare);
            total_payload_bytes = std.math.add(usize, total_payload_bytes, bytes.len) catch return error.HistoryTooLarge;
            if (bytes.len > completion.max_bytes or total_payload_bytes > 64 * 1024 * 1024) return error.HistoryTooLarge;
            const name = try owned.dupe(u8, input.locator);
            if ((try unique_locators.getOrPut(owned, name)).found_existing) return error.InvalidParticipant;
            participants[i] = .{ .owner = input.owner, .locator = name, .prepare_bytes = bytes };
            members[i] = .{ .namespace = input.owner.namespace, .owner_fence = input.owner.digest(), .roles = input.roles, .input_digest = completion.preparedInputDigest(input.owner.namespace, bytes), .locator = name, .routed_owner = input.owner };
        }
        if (source_purpose != null and checkpoint_count != 1) return error.InvalidSourceAdmission;
        // Admission order is canonical, independent of route enumeration or
        // fanout completion order. Keep the corresponding payloads together.
        for (1..participants.len) |i| {
            var j = i;
            while (j > 0 and std.mem.order(u8, &participants[j].owner.namespace, &participants[j - 1].owner.namespace) == .lt) : (j -= 1) {
                std.mem.swap(Participant, &participants[j], &participants[j - 1]);
                std.mem.swap(completion.Participant, &members[j], &members[j - 1]);
            }
        }
        const admission: completion.Admission = .{ .version = 3, .transaction_id = transaction_id, .admission_digest = admission_digest, .coordinator_namespace = coordinator.namespace, .coordinator_fence = coordinator.digest(), .participants = members };
        try admission.validate();
        try admission.requireRoutedOwner(coordinator);
        const encoded = try completion.encodeAdmission(owned, admission);
        _ = try admission.reservationBytes(encoded.len);
        const ids = try owned.alloc([]const u8, participants.len);
        // The admission uses namespace order. Existing transaction recovery
        // locators instead require coordinator first, then canonical followers.
        ids[0] = (try admission.member(coordinator.namespace)).locator;
        var follower: usize = 1;
        for (participants) |participant| {
            if (std.mem.eql(u8, &participant.owner.namespace, &coordinator.namespace)) continue;
            ids[follower] = participant.locator;
            follower += 1;
        }
        return .{ .arena = arena, .admission = admission, .admission_bytes = encoded, .participants = participants, .participant_ids = ids, .begin_timestamp = begin_timestamp, .created_at_ns = created_at_ns, .source_purpose = source_purpose, .source_checkpoint = source_checkpoint, .source_checkpoint_receipts = source_checkpoint_receipts };
    }

    /// Derive a retry identity from canonical namespace order and the complete
    /// frozen input, independent of route enumeration and admission wall time.
    pub fn freezeDerived(a: A, transaction_id: [16]u8, coordinator: completion.RoutedOwner, inputs: []const Input, created_at_ns: u64) !Plan {
        var plan = try freeze(a, transaction_id, @splat(1), coordinator, inputs, created_at_ns);
        errdefer plan.deinit();
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:ordinary-completion-input:v1");
        hash.update(&plan.admission.digest());
        plan.admission.admission_digest = hash.finalResult();
        plan.admission_bytes = try completion.encodeAdmission(plan.arena.allocator(), plan.admission);
        return plan;
    }

    /// Versioned artifact encoding for the exact pre-admission plan. Callers
    /// persist its digest/reference in source control before dispatch. This
    /// codec supplies no storage, lease, source ACK, or commit authority.
    pub fn encodeFrozen(self: *const Plan, a: A) ![]u8 {
        try self.admission.validate();
        if (self.admission.version != 3 or self.created_at_ns == 0 or self.begin_timestamp == 0 or self.participants.len != self.admission.participants.len or self.admission_bytes.len > completion.max_bytes) return error.InvalidTxnRecord;
        var parsed_admission = try completion.parseAdmission(a, self.admission_bytes);
        defer parsed_admission.deinit();
        if (!std.mem.eql(u8, &parsed_admission.value.digest, &self.admission.digest())) return error.SourcePayloadConflict;
        if (self.participant_ids.len != self.participants.len or !std.mem.eql(u8, self.participant_ids[0], (try self.admission.member(self.admission.coordinator_namespace)).locator)) return error.InvalidParticipant;
        var follower: usize = 1;
        var checkpoints: usize = 0;
        var has_checkpoint_command = false;
        var has_checkpoint_receipts = false;
        for (self.participants, 0..) |participant, index| {
            var request = try self.prepare(a, index);
            defer request.deinit(a);
            const mutation = request.req.transaction orelse return error.InvalidBatchRequest;
            if (mutation != .prepare) return error.InvalidBatchRequest;
            if (request.req.timestamp_ns != self.begin_timestamp or request.req.sync_level != .write or request.req.document_version_scheme != .revision_v1 or !std.meta.eql(mutation.prepare.source_purpose, self.source_purpose)) return error.SourcePayloadConflict;
            if (!std.mem.eql(u8, &mutation.prepare.txn_id, &self.admission.transaction_id) or mutation.prepare.completion_owner == null or !std.mem.eql(u8, &mutation.prepare.completion_owner.?.digest(), &participant.owner.digest()) or mutation.prepare.topology_epoch != participant.owner.topology_epoch) return error.SourceTransactionIdentityConflict;
            const member = try self.admission.member(participant.owner.namespace);
            if (member.roles & completion.Roles.checkpoint != 0) checkpoints += 1;
            if (mutation.prepare.source_checkpoint) |checkpoint| {
                try checkpoint.validate();
                if (self.source_purpose == null or !std.mem.eql(u8, &checkpoint.purpose, &self.source_purpose.?) or member.roles & completion.Roles.checkpoint == 0) return error.InvalidSourceAdmission;
                has_checkpoint_command = true;
                has_checkpoint_receipts = checkpoint.receipt_window != 0;
            }
            if (!std.mem.eql(u8, &participant.owner.namespace, &self.admission.coordinator_namespace)) {
                if (follower >= self.participant_ids.len or !std.mem.eql(u8, self.participant_ids[follower], participant.locator)) return error.InvalidParticipant;
                follower += 1;
            }
        }
        if (has_checkpoint_command != self.source_checkpoint or has_checkpoint_receipts != self.source_checkpoint_receipts) return error.SourcePayloadConflict;
        if (self.source_purpose != null and checkpoints != 1) return error.InvalidSourceAdmission;
        var size: usize = frozen_header_bytes + self.admission_bytes.len;
        var payload_bytes: usize = 0;
        for (self.participants, self.admission.participants) |participant, member| {
            try self.admission.requireRoutedOwner(participant.owner);
            if (!std.mem.eql(u8, &participant.owner.namespace, &member.namespace) or !std.mem.eql(u8, participant.locator, member.locator)) return error.InvalidParticipant;
            try self.admission.requirePreparedInput(participant.owner.namespace, participant.prepare_bytes);
            if (participant.prepare_bytes.len > completion.max_bytes) return error.HistoryTooLarge;
            payload_bytes = std.math.add(usize, payload_bytes, participant.prepare_bytes.len) catch return error.HistoryTooLarge;
            size = std.math.add(usize, size, 4 + participant.prepare_bytes.len) catch return error.HistoryTooLarge;
        }
        if (payload_bytes > max_frozen_payload_bytes or size > max_frozen_bytes) return error.HistoryTooLarge;
        const out = try a.alloc(u8, size);
        errdefer a.free(out);
        @memcpy(out[0..4], "AFP1");
        std.mem.writeInt(u32, out[4..8], @intCast(self.participants.len), .little);
        std.mem.writeInt(u64, out[8..16], self.begin_timestamp, .little);
        std.mem.writeInt(u64, out[16..24], self.created_at_ns, .little);
        std.mem.writeInt(u64, out[24..32], @intCast(self.admission_bytes.len), .little);
        @memcpy(out[frozen_header_bytes..][0..self.admission_bytes.len], self.admission_bytes);
        var cursor = frozen_header_bytes + self.admission_bytes.len;
        for (self.participants) |participant| {
            std.mem.writeInt(u32, out[cursor..][0..4], @intCast(participant.prepare_bytes.len), .little);
            cursor += 4;
            @memcpy(out[cursor..][0..participant.prepare_bytes.len], participant.prepare_bytes);
            cursor += participant.prepare_bytes.len;
        }
        @memcpy(out[32..64], &try frozenDigest(out));
        return out;
    }

    /// Restore solely from the retained artifact and trusted expected digest.
    /// Never consult current routing, rows, schema, or provider configuration.
    /// Version 1 requires its canonical private phase encoders to remain stable;
    /// incompatible encoders must retain a versioned reader instead of replanning.
    pub fn restoreFrozen(a: A, raw: []const u8, expected_digest: completion.Digest) !Plan {
        if (raw.len < frozen_header_bytes or raw.len > max_frozen_bytes or !std.mem.eql(u8, raw[0..4], "AFP1")) return error.InvalidTxnRecord;
        if (!std.mem.eql(u8, raw[32..64], &expected_digest) or !std.mem.eql(u8, &try frozenDigest(raw), &expected_digest)) return error.SourcePayloadConflict;
        const count = std.mem.readInt(u32, raw[4..8], .little);
        const begin_timestamp = std.mem.readInt(u64, raw[8..16], .little);
        const created_at_ns = std.mem.readInt(u64, raw[16..24], .little);
        const admission_len = std.mem.readInt(u64, raw[24..32], .little);
        if (count == 0 or count > completion.max_participants or begin_timestamp == 0 or created_at_ns == 0 or admission_len == 0 or admission_len > completion.max_bytes or admission_len > raw.len - frozen_header_bytes) return error.InvalidTxnRecord;
        const admission_bytes = raw[frozen_header_bytes..][0..@intCast(admission_len)];
        var parsed = try completion.parseAdmission(a, admission_bytes);
        defer parsed.deinit();
        const admission = parsed.value.admission;
        if (admission.version != 3 or admission.participants.len != count) return error.InvalidTxnRecord;
        const coordinator = try admission.recoveryOwner(admission.coordinator_namespace);
        const inputs = try a.alloc(Input, count);
        defer a.free(inputs);
        const requests = try a.alloc(batch.OwnedBatchRequest, count);
        defer a.free(requests);
        var initialized: usize = 0;
        defer for (requests[0..initialized]) |*request| request.deinit(a);
        var cursor = frozen_header_bytes + @as(usize, @intCast(admission_len));
        var payload_bytes: usize = 0;
        for (admission.participants, inputs, requests) |member, *input, *request| {
            if (raw.len - cursor < 4) return error.InvalidTxnRecord;
            const len = std.mem.readInt(u32, raw[cursor..][0..4], .little);
            cursor += 4;
            if (len == 0 or len > completion.max_bytes or len > raw.len - cursor) return error.InvalidTxnRecord;
            payload_bytes = std.math.add(usize, payload_bytes, len) catch return error.HistoryTooLarge;
            if (payload_bytes > max_frozen_payload_bytes) return error.HistoryTooLarge;
            const bytes = raw[cursor..][0..len];
            try admission.requirePreparedInput(member.namespace, bytes);
            request.* = try batch.parseInternalBatchRequest(a, bytes);
            initialized += 1;
            input.* = .{ .owner = try admission.recoveryOwner(member.namespace), .locator = member.locator, .roles = member.roles, .prepare = request.req };
            cursor += len;
        }
        if (cursor != raw.len) return error.InvalidTxnRecord;
        var restored = try freeze(a, admission.transaction_id, admission.admission_digest, coordinator, inputs, created_at_ns);
        errdefer restored.deinit();
        if (restored.begin_timestamp != begin_timestamp or !std.mem.eql(u8, restored.admission_bytes, admission_bytes)) return error.SourcePayloadConflict;
        cursor = frozen_header_bytes + @as(usize, @intCast(admission_len));
        for (restored.participants) |participant| {
            const len = std.mem.readInt(u32, raw[cursor..][0..4], .little);
            cursor += 4;
            if (!std.mem.eql(u8, participant.prepare_bytes, raw[cursor..][0..len])) return error.SourcePayloadConflict;
            cursor += len;
        }
        return restored;
    }

    /// The BEGIN request borrows this plan's admission and locator slices.
    /// Encode/submit it before destroying the plan; its timestamp never changes.
    pub fn begin(self: *const Plan, index: usize) !types.BatchRequest {
        if (index >= self.participants.len) return error.InvalidTxnRecord;
        const owner = self.participants[index].owner;
        try self.admission.requireRoutedOwner(owner);
        return .{ .sync_level = .write, .transaction = .{ .begin = .{ .txn_id = self.admission.transaction_id, .begin_timestamp = self.begin_timestamp, .created_at_ns = self.created_at_ns, .topology_epoch = owner.topology_epoch, .retain_terminal = true, .participants = self.participant_ids, .completion_admission = self.admission_bytes, .completion_owner = owner, .source_purpose = self.source_purpose, .source_checkpoint = self.source_checkpoint, .source_checkpoint_receipts = self.source_checkpoint_receipts } } };
    }

    /// Parsed retry input has independent ownership and can outlive this plan.
    pub fn prepare(self: *const Plan, a: A, index: usize) !batch.OwnedBatchRequest {
        if (index >= self.participants.len) return error.InvalidParticipant;
        const participant = self.participants[index];
        try self.admission.requireRoutedOwner(participant.owner);
        try self.admission.requirePreparedInput(participant.owner.namespace, participant.prepare_bytes);
        return batch.parseInternalBatchRequest(a, participant.prepare_bytes);
    }
};

pub const frozen_header_bytes = frozen_storage.frozen_header_bytes;
pub const max_frozen_payload_bytes = frozen_storage.max_participant_inputs_bytes;
pub const max_frozen_bytes: usize = frozen_header_bytes + completion.max_bytes + max_frozen_payload_bytes + 4 * completion.max_participants;

comptime {
    std.debug.assert(completion.max_bytes == frozen_storage.max_admission_bytes);
    std.debug.assert(completion.max_participants == frozen_storage.max_participants);
    std.debug.assert(max_frozen_bytes == frozen_storage.max_bytes);
}

/// Domain-separated artifact identity. The checksum slot is excluded; every
/// other byte, including lengths and admission timestamps, is authenticated.
pub fn frozenDigest(raw: []const u8) !completion.Digest {
    return frozen_storage.frozenDigest(raw);
}

test "completion frozen plan artifacts retain complete owned inputs and reject substituted recovery" {
    const a = std.testing.allocator;
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 9, .topology_epoch = 3 };
    var follower = owner;
    follower.namespace = @splat(3);
    follower.group_id = 10;
    const id: [16]u8 = @splat(4);
    const purpose: [32]u8 = @splat(5);
    const inputs = [_]Input{
        .{ .owner = owner, .locator = "table2:00000004:docs:9", .roles = completion.Roles.checkpoint, .prepare = try (types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_purpose = purpose }).completionBatch(id, 10, owner.topology_epoch) },
        .{ .owner = follower, .locator = "table2:00000004:docs:10", .roles = completion.Roles.effects, .prepare = try (types.TransactionIntentRequest{ .completion_owner = follower, .completion_source_purpose = purpose, .writes = &.{.{ .key = "row", .value = "{\"v\":1}" }} }).completionBatch(id, 10, follower.topology_epoch) },
    };
    var plan = try Plan.freeze(a, id, @splat(6), owner, &inputs, 11);
    defer plan.deinit();
    const raw = try plan.encodeFrozen(a);
    defer a.free(raw);
    const digest = try frozenDigest(raw);
    try std.testing.expectEqualSlices(u8, &digest, raw[32..64]);
    var reversed = try Plan.freeze(a, id, @splat(6), owner, &.{ inputs[1], inputs[0] }, 11);
    defer reversed.deinit();
    const reordered = try reversed.encodeFrozen(a);
    defer a.free(reordered);
    try std.testing.expectEqualSlices(u8, raw, reordered);
    var restored = try Plan.restoreFrozen(a, raw, digest);
    defer restored.deinit();
    try std.testing.expectEqual(plan.admission.digest(), restored.admission.digest());
    try std.testing.expectEqualDeep(plan.participant_ids, restored.participant_ids);
    try std.testing.expectEqual(purpose, restored.source_purpose.?);
    try std.testing.expectEqual(@as(u64, 10), restored.begin_timestamp);
    try std.testing.expectEqual(@as(u64, 11), restored.created_at_ns);

    const damaged = try a.dupe(u8, raw);
    defer a.free(damaged);
    damaged[0] ^= 1;
    try std.testing.expectError(error.InvalidTxnRecord, Plan.restoreFrozen(a, damaged, digest));
    @memcpy(damaged, raw);
    damaged[32] ^= 1;
    try std.testing.expectError(error.SourcePayloadConflict, Plan.restoreFrozen(a, damaged, digest));
    @memcpy(damaged, raw);
    std.mem.writeInt(u32, damaged[4..8], 0, .little);
    var changed_digest = try frozenDigest(damaged);
    @memcpy(damaged[32..64], &changed_digest);
    try std.testing.expectError(error.InvalidTxnRecord, Plan.restoreFrozen(a, damaged, changed_digest));
    @memcpy(damaged, raw);
    std.mem.writeInt(u64, damaged[24..32], std.math.maxInt(u64), .little);
    changed_digest = try frozenDigest(damaged);
    @memcpy(damaged[32..64], &changed_digest);
    try std.testing.expectError(error.InvalidTxnRecord, Plan.restoreFrozen(a, damaged, changed_digest));
    @memcpy(damaged, raw);
    std.mem.writeInt(u64, damaged[8..16], 9, .little);
    changed_digest = try frozenDigest(damaged);
    @memcpy(damaged[32..64], &changed_digest);
    try std.testing.expectError(error.SourcePayloadConflict, Plan.restoreFrozen(a, damaged, changed_digest));
    @memcpy(damaged, raw);
    const row = std.mem.indexOf(u8, damaged, "\"row\"") orelse return error.TestExpectedFrozenInput;
    damaged[row + 1] = 's';
    changed_digest = try frozenDigest(damaged);
    @memcpy(damaged[32..64], &changed_digest);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, Plan.restoreFrozen(a, damaged, changed_digest));
    @memcpy(damaged, raw);
    const truncated = damaged[0 .. damaged.len - 1];
    changed_digest = try frozenDigest(truncated);
    @memcpy(truncated[32..64], &changed_digest);
    try std.testing.expectError(error.InvalidTxnRecord, Plan.restoreFrozen(a, truncated, changed_digest));
    const extended = try a.alloc(u8, raw.len + 1);
    defer a.free(extended);
    @memcpy(extended[0..raw.len], raw);
    extended[raw.len] = 0;
    changed_digest = try frozenDigest(extended);
    @memcpy(extended[32..64], &changed_digest);
    try std.testing.expectError(error.InvalidTxnRecord, Plan.restoreFrozen(a, extended, changed_digest));

    plan.source_purpose = @splat(7);
    try std.testing.expectError(error.SourcePayloadConflict, plan.encodeFrozen(a));
    plan.source_purpose = purpose;
    const Failure = struct {
        fn encode(allocator: A, input: *const Plan) !void {
            const bytes = try input.encodeFrozen(allocator);
            defer allocator.free(bytes);
        }
        fn restore(allocator: A, bytes: []const u8, expected: completion.Digest) !void {
            var value = try Plan.restoreFrozen(allocator, bytes, expected);
            defer value.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.encode, .{&plan});
    try std.testing.checkAllAllocationFailures(a, Failure.restore, .{ raw, digest });
    @memset(raw, 0);
    var owned_request = try restored.prepare(a, 1);
    defer owned_request.deinit(a);
    try std.testing.expectEqualStrings("row", owned_request.req.writes[0].key);
    try std.testing.expectEqualStrings("{\"v\":1}", owned_request.req.writes[0].value);
}
