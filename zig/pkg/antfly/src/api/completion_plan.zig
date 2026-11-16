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
        var total_payload_bytes: usize = 0;
        var unique_locators: std.StringHashMapUnmanaged(void) = .empty;
        for (inputs, 0..) |input, i| {
            try input.owner.validate();
            const locator = locators.parseParticipantRef(input.locator) orelse return error.InvalidParticipant;
            if (locator.group_id != input.owner.group_id or locator.restore_staging_scope != null or locator.restore_staging_plan_id != null or input.locator.len > completion.max_locator_bytes) return error.InvalidParticipant;
            const mutation = input.prepare.transaction orelse return error.InvalidBatchRequest;
            if (mutation != .prepare) return error.InvalidBatchRequest;
            const vote = mutation.prepare;
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
            members[i] = .{ .namespace = input.owner.namespace, .owner_fence = input.owner.digest(), .roles = input.roles, .input_digest = completion.preparedInputDigest(input.owner.namespace, bytes), .locator = name };
        }
        // Admission order is canonical, independent of route enumeration or
        // fanout completion order. Keep the corresponding payloads together.
        for (1..participants.len) |i| {
            var j = i;
            while (j > 0 and std.mem.order(u8, &participants[j].owner.namespace, &participants[j - 1].owner.namespace) == .lt) : (j -= 1) {
                std.mem.swap(Participant, &participants[j], &participants[j - 1]);
                std.mem.swap(completion.Participant, &members[j], &members[j - 1]);
            }
        }
        const admission: completion.Admission = .{ .version = 2, .transaction_id = transaction_id, .admission_digest = admission_digest, .coordinator_namespace = coordinator.namespace, .coordinator_fence = coordinator.digest(), .participants = members };
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
        return .{ .arena = arena, .admission = admission, .admission_bytes = encoded, .participants = participants, .participant_ids = ids, .begin_timestamp = begin_timestamp, .created_at_ns = created_at_ns };
    }

    /// The BEGIN request borrows this plan's admission and locator slices.
    /// Encode/submit it before destroying the plan; its timestamp never changes.
    pub fn begin(self: *const Plan, index: usize) !types.BatchRequest {
        if (index >= self.participants.len) return error.InvalidTxnRecord;
        const owner = self.participants[index].owner;
        try self.admission.requireRoutedOwner(owner);
        return .{ .sync_level = .write, .transaction = .{ .begin = .{ .txn_id = self.admission.transaction_id, .begin_timestamp = self.begin_timestamp, .created_at_ns = self.created_at_ns, .topology_epoch = owner.topology_epoch, .retain_terminal = true, .participants = self.participant_ids, .completion_admission = self.admission_bytes, .completion_owner = owner } } };
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
