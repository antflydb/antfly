// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A signature proves possession of an actual physical owner's key. Native
//! acceptance independently rejoins enrollment, current owner/Boot or Raft
//! placement, exact pending publication/P, range and reader capability. These
//! DTOs never turn a presented signature or actor boolean into authorization.
const std = @import("std");
const local = @import("antfly_local_sources");
const capture = local.system_catalog_logical_replication_capture;
const publisher = local.system_catalog_logical_replication_publisher;
const attestation = @import("../metadata/store_root_attestation.zig");
const signing = local.storage_db_root_signing_identity;
const A = std.mem.Allocator;
pub const ObservationProof = attestation.ProofFor("antfly/logical-capture-observation/signature/v1\x00", "antfly/logical-capture-observation/accepted/v1\x00");
pub const GuardProof = attestation.ProofFor("antfly/logical-capture-owner/signature/v1\x00", "antfly/logical-capture-owner/accepted/v1\x00");
pub const CapacityProof = attestation.ProofFor("antfly/logical-capture-capacity/signature/v1\x00", "antfly/logical-capture-capacity/accepted/v1\x00");
pub const CapacityReceipt = struct {
    value: local.system_catalog_logical_replication_capacity.Observation,
    completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof,
    proof: CapacityProof,

    fn logicalDigest(self: @This(), a: A) ![32]u8 {
        return capture.canonicalDigest(a, "antfly/logical-capture-capacity-receipt/v1\x00", .{ .value = self.value, .completed = self.completed });
    }

    pub fn validate(self: @This(), a: A) !void {
        try self.value.validate();
        try requireImage(self.proof, self.value.namespace, self.value.physical_root_incarnation, self.value.position, self.value.owner.metadata_incarnation);
        if (self.proof.native_pin != null or self.value.position != .raft or self.completed.native != null or self.completed.incarnation == 0 or
            self.completed.term < self.value.position.raft.term or self.completed.applied_index < self.value.position.raft.index)
            return error.InvalidLogicalCaptureControl;
        try self.proof.verify(try self.logicalDigest(a));
    }

    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        var result = self;
        result.proof = try self.proof.sign(root, try self.logicalDigest(a));
        try result.validate(a);
        return result;
    }

    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        return .{ .group_id = self.value.owner_group_id, .namespace = self.value.namespace, .authority = .raft };
    }
};

pub const OutboxProof = attestation.ProofFor("antfly/logical-capture-outbox/signature/v1\x00", "antfly/logical-capture-outbox/accepted/v1\x00");
fn requireImage(proof: anytype, namespace: local.storage_db_doc_identity.Namespace, root: u128, position: local.storage_db_receipt_position.Position, incarnation: [32]u8) !void {
    try position.requireNamespace(namespace);
    if (!std.mem.eql(u8, &proof.identity.metadata_incarnation, &incarnation) or root == 0 or
        ((position == .native) != (proof.authority == .native))) return error.InvalidLogicalCaptureControl;
    if (proof.authority == .native) {
        const native: attestation.NativeView = .{
            .identity = proof.identity,
            .reporter_incarnation = proof.reporter_incarnation,
            .binding_digest = proof.authority.native,
            .native_pin = proof.native_pin,
        };
        try native.validate(incarnation);
    }
    if (proof.native_pin) |pin| {
        try pin.validate();
        if (pin.physical_root_incarnation != root or pin.table_id != namespace.table_id or pin.shard_id != namespace.shard_id or pin.range_id != namespace.range_id) return error.InvalidLogicalCaptureControl;
    }
}
pub const GuardReceipt = struct {
    value: capture.Receipt,
    proof: GuardProof,
    pub fn validate(self: @This(), control: capture.Control, a: A) !void {
        try self.value.require(control);
        try requireImage(self.proof, self.value.binding.namespace, self.value.physical_root_incarnation, self.value.position, self.value.binding.scope.realm.incarnation);
        try self.proof.verify(try self.value.digest(a));
    }
    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        var result = self;
        result.proof = try self.proof.sign(root, try self.value.digest(a));
        return result;
    }
    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        return .{ .group_id = self.value.binding.owner_group_id, .namespace = self.value.binding.namespace, .authority = if (self.proof.authority == .native) .native else .raft };
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        return self.proof.commitment(try self.value.digest(a));
    }
};
pub const OutboxReceipt = struct {
    reference: publisher.CaptureRef,
    proof: OutboxProof,
    pub fn logicalDigest(self: @This(), a: A) ![32]u8 {
        try self.reference.validate();
        return capture.canonicalDigest(a, "antfly/logical-capture-outbox-reference/v1\x00", self.reference);
    }
    pub fn validate(self: @This(), a: A) !void {
        try self.reference.validate();
        try requireImage(self.proof, self.reference.namespace, self.reference.physical_root_incarnation, self.reference.position, self.reference.owner.metadata_incarnation);
        try self.proof.verify(try self.logicalDigest(a));
    }
    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        var result = self;
        result.proof = try self.proof.sign(root, try self.logicalDigest(a));
        return result;
    }
    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        return .{ .group_id = self.reference.group_id, .namespace = self.reference.namespace, .authority = if (self.proof.authority == .native) .native else .raft };
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        return self.proof.commitment(try self.logicalDigest(a));
    }
};

pub const TerminalProof = attestation.ProofFor("antfly/logical-capture-terminal/signature/v1\x00", "antfly/logical-capture-terminal/accepted/v1\x00");

pub const TerminalReceipt = struct {
    value: publisher.OrderedTerminal,
    completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof,
    proof: TerminalProof,

    fn logicalDigest(self: @This(), a: A) ![32]u8 {
        return capture.canonicalDigest(a, "antfly/logical-capture-terminal-receipt/v1\x00", .{ .value = self.value, .completed = self.completed });
    }

    pub fn validate(self: @This(), a: A) !void {
        try self.value.validate();
        const reference = self.value.capture_ref;
        const cut = self.value.position;
        try requireImage(self.proof, reference.namespace, reference.physical_root_incarnation, .{ .raft = cut }, reference.owner.metadata_incarnation);
        if (self.proof.native_pin != null or self.completed.native != null or self.completed.incarnation == 0 or
            self.completed.term < cut.term or self.completed.applied_index < cut.index)
            return error.InvalidLogicalCaptureControl;
        try self.proof.verify(try self.logicalDigest(a));
    }

    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        var result = self;
        result.proof = try self.proof.sign(root, try self.logicalDigest(a));
        try result.validate(a);
        return result;
    }
    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        const reference = self.value.capture_ref;
        return .{ .group_id = reference.group_id, .namespace = reference.namespace, .authority = .raft };
    }
};

pub const AbortFenceProof = attestation.ProofFor("antfly/logical-capture-abort-fence/signature/v1\x00", "antfly/logical-capture-abort-fence/accepted/v1\x00");

pub const AbortFenceReceipt = struct {
    value: local.system_catalog_logical_replication_capacity.AbortAbsence,
    completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof,
    proof: AbortFenceProof,

    fn logicalDigest(self: @This(), a: A) ![32]u8 {
        return capture.canonicalDigest(a, "antfly/logical-capture-abort-fence/v1\x00", .{ .value = self.value, .completed = self.completed });
    }

    pub fn validate(self: @This(), a: A) !void {
        try self.value.validate();
        const cut = self.value.position;
        try requireImage(self.proof, self.value.namespace, self.value.physical_root_incarnation, .{ .raft = cut }, self.value.owner.metadata_incarnation);
        if (self.proof.native_pin != null or self.completed.native != null or self.completed.incarnation == 0 or
            self.completed.term < cut.term or self.completed.applied_index < cut.index)
            return error.InvalidLogicalCaptureControl;
        try self.proof.verify(try self.logicalDigest(a));
    }

    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        var result = self;
        result.proof = try self.proof.sign(root, try self.logicalDigest(a));
        try result.validate(a);
        return result;
    }

    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        return .{ .group_id = self.value.group_id, .namespace = self.value.namespace, .authority = .raft };
    }
};

pub const TerminalResult = union(enum) { captured: TerminalReceipt, absent: AbortFenceReceipt };

pub const CollectionProof = attestation.ProofFor("antfly/logical-collection/signature/v1\x00", "antfly/logical-collection/accepted/v1\x00");
pub const CollectionReceipt = struct {
    permission: publisher.GcPermission,
    value: publisher.GcObservation,
    completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof,
    proof: CollectionProof,

    fn logicalDigest(self: @This(), a: A) ![32]u8 {
        return capture.canonicalDigest(a, "antfly/logical-collection/v1\x00", .{ .permission_digest = try self.permission.digest(a), .value = self.value, .completed = self.completed });
    }

    pub fn validate(self: @This(), a: A) !void {
        try self.value.require(self.permission, a);
        const reference = self.permission.debt.reference;
        try requireImage(self.proof, reference.namespace, self.value.physical_root_incarnation, self.value.position, self.permission.realm.incarnation);
        if (self.proof.native_pin != null or self.completed.native != null or self.completed.incarnation == 0 or self.value.position != .raft or
            self.completed.term < self.value.position.raft.term or self.completed.applied_index < self.value.position.raft.index)
            return error.InvalidLogicalCaptureControl;
        try self.proof.verify(try self.logicalDigest(a));
    }

    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        var result = self;
        result.proof = try self.proof.sign(root, try self.logicalDigest(a));
        try result.validate(a);
        return result;
    }

    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        const ref = self.permission.debt.reference;
        return .{ .group_id = ref.group_id, .namespace = ref.namespace, .authority = .raft };
    }
};

/// Read-only AIC discovery. Null local data position is usable only with a
/// genuine independently joined completed Raft prefix and a pristine image.
/// This observation never substitutes for an actual ordered prepared receipt.
pub const ObservationReceipt = struct {
    value: capture.OwnerObservation,
    completed: ?local.storage_read_consistency.ReadSafetyBarrier.FrozenProof = null,
    proof: ObservationProof,
    pub fn logicalDigest(self: @This(), a: A) ![32]u8 {
        return capture.canonicalDigest(a, "antfly/logical-capture-observation/v1\x00", .{ .value = self.value, .completed = self.completed });
    }
    pub fn validate(self: @This(), request: capture.ObservationRequest, a: A) !void {
        try self.value.require(request);
        if (self.proof.native_pin != null or
            !std.mem.eql(u8, &self.proof.identity.metadata_incarnation, &request.scope.realm.incarnation)) return error.InvalidLogicalCaptureControl;
        if (self.proof.authority == .native) {
            // Permanent standalone source ownership has no SQL lifetime Boot.
            const view: attestation.NativeView = .{ .identity = self.proof.identity, .reporter_incarnation = self.proof.reporter_incarnation, .binding_digest = self.proof.authority.native };
            try view.validate(request.scope.realm.incarnation);
            if (self.completed != null) return error.InvalidLogicalCaptureControl;
            if (self.value.position) |position| {
                if (position != .native) return error.InvalidLogicalCaptureControl;
                try requireImage(self.proof, request.namespace, self.value.physical_root_incarnation, position, request.scope.realm.incarnation);
            }
            try self.proof.verify(try self.logicalDigest(a));
            return;
        }
        if (self.value.position) |position| {
            try requireImage(self.proof, request.namespace, self.value.physical_root_incarnation, position, request.scope.realm.incarnation);
        } else if (self.completed == null) return error.InvalidLogicalCaptureControl;
        if (self.completed) |prefix| {
            if (prefix.native != null or prefix.incarnation == 0 or prefix.term == 0 or prefix.applied_index == 0) return error.InvalidLogicalCaptureControl;
            if (self.value.position) |position| if (position != .raft or prefix.term < position.raft.term or prefix.applied_index < position.raft.index) return error.InvalidLogicalCaptureControl;
        }
        try self.proof.verify(try self.logicalDigest(a));
    }
    pub fn sign(self: @This(), root: signing.State, a: A) !@This() {
        try self.value.validate();
        var result = self;
        result.proof = try self.proof.sign(root, try self.logicalDigest(a));
        return result;
    }
    pub fn ownerScope(self: @This()) attestation.OwnerScope {
        return .{ .group_id = self.value.request.owner_group_id, .namespace = self.value.request.namespace, .authority = if (self.proof.authority == .native) .native else .raft };
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        return self.proof.commitment(try self.logicalDigest(a));
    }
};

comptime {
    if (@import("builtin").is_test) {
        _ = @import("../pgwire/logical_replication_commands.zig");
        _ = @import("../pgwire/logical_replication_frontend.zig");
        _ = @import("../pgwire/logical_replication_rows.zig");
    }
}

test "SQL logical replication capture receipts bind exact owner images and control actions" {
    const a = std.testing.allocator;
    const seed: [32]u8 = @splat(7);
    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: signing.State = .{ .root_incarnation = 9, .seed = seed, .public_key = key.public_key.toBytes() };
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const owner: @FieldType(ObservationProof, "identity") = .{
        .metadata_incarnation = incarnation,
        .node_id = 2,
        .store_id = 3,
        .root_incarnation = root.root_incarnation,
        .public_key = root.public_key,
    };
    const authority: @import("../metadata/standalone_native_owner.zig").Binding = .{
        .metadata_incarnation = incarnation,
        .node_id = owner.node_id,
        .store_id = owner.store_id,
        .root_incarnation = owner.root_incarnation,
    };
    const request: capture.ObservationRequest = .{
        .scope = .{ .realm = .{ .group_id = 7, .incarnation = incarnation, .database_id = 1 }, .subject = .{ .publication = .{ .id = 4, .generation = 1 } }, .operation_id = @splat(5), .change_digest = @splat(6) },
        .logical_relation_id = 15,
        .physical_table_id = 20,
        .owner_group_id = 30,
        .namespace = .{ .table_id = 20, .shard_id = 31, .range_id = 32 },
        .schema_version = 2,
        .schema_digest = @splat(10),
        .catalog_revision = 4,
        .previous = .{},
        .next = .{ .table = .{ .count = 1, .digest = @splat(11) } },
    };
    const P = @import("../system_catalog/prepared_transactions.zig");
    const reference: capture.DemandReference = .{ .realm = request.scope.realm, .guard_digest = @splat(13), .roster_digest = @splat(14), .pregrant_body_digest = @splat(15), .request_digest = @splat(16) };
    const participants = [_]P.Participant{.{
        .publication_capture = reference,
        .table_name = "physical:usage",
        .schema_version = 2,
        .route = .{ .metadata_group_id = 7, .metadata_incarnation = incarnation, .catalog_revision = 4, .table_id = 20, .topology_epoch = 1, .route = .{ .group_id = 30, .range_id = 32, .identity_namespace = .{ .table_id = 20, .shard_id = 31, .range_id = 32 } } },
    }};
    const record: P.Record = .{ .identity = .{
        .gid = "capacity_test",
        .database_id = 1,
        .principal = .bootstrap,
        .txn_id = @splat(21),
        .operation_id = @splat(22),
        .metadata_incarnation = incarnation,
        .begin_timestamp = 1,
        .expires_at_unix_ns = 1000,
        .plan_digest = try P.planDigest("[]"),
        .manifest_digest = try P.manifestDigest(a, &participants, incarnation),
    }, .participants = &participants };
    const decision_owner = try record.identity.owner(a);
    const capacity = try (CapacityReceipt{
        .value = .{ .owner = decision_owner, .transaction_id = record.identity.txn_id, .member = 0, .owner_group_id = 30, .topology_epoch = 1, .namespace = request.namespace, .physical_root_incarnation = 33, .position = .{ .raft = .{ .term = 3, .index = 9 } }, .reference = reference, .demand_digest = @splat(23), .before_digest = @splat(24), .max_payload_bytes = 1024 },
        .proof = .{ .identity = owner, .reporter_incarnation = 1, .authority = .{ .raft = 18 }, .root_generation = 1, .signature = undefined },
        .completed = .{ .incarnation = 17, .term = 3, .applied_index = 10 },
    }).sign(root, a);
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const accepted = try P.advance(region.allocator(), record, 1, decision_owner, .{ .publication_capacity = capacity });
    try std.testing.expectEqual(@as(u64, 0), accepted.votes);
    try std.testing.expectEqual(@as(u64, 1024), accepted.publication_capacities[0].max_payload_bytes);
    const encoded = try std.json.Stringify.valueAlloc(region.allocator(), accepted, .{});
    const retained = try std.json.parseFromSliceLeaky(P.Record, region.allocator(), encoded, .{});
    try retained.validate(region.allocator());
    try std.testing.expectEqual(accepted.revision, (try P.advance(region.allocator(), retained, accepted.revision, decision_owner, .{ .publication_capacity = capacity })).revision);
    try std.testing.expectError(error.CatalogPublicationProofPending, P.advance(region.allocator(), retained, retained.revision, decision_owner, .prepare));
    var other = capacity;
    other.value.max_payload_bytes += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, other.validate(a));
    other = try other.sign(root, a);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, P.advance(region.allocator(), retained, retained.revision, decision_owner, .{ .publication_capacity = other }));
    try std.testing.expectError(error.CatalogPublicationProofPending, P.advance(region.allocator(), retained, retained.revision, decision_owner, .{ .vote = 1 }));
    const outbox_receipt = try (OutboxReceipt{
        .reference = .{
            .owner = decision_owner,
            .transaction_id = record.identity.txn_id,
            .participant = 0,
            .group_id = 30,
            .namespace = request.namespace,
            .physical_root_incarnation = 33,
            .position = .{ .raft = .{ .term = 3, .index = 11 } },
            .roster_digest = reference.roster_digest,
            .guard_digest = reference.guard_digest,
            .pregrant_body_digest = reference.pregrant_body_digest,
            .changes_digest = @splat(25),
            .capacity_digest = try capacity.value.digest(a),
            .before_digest = capacity.value.before_digest,
            .change_count = 1,
            .encoded_bytes = 512,
        },
        .proof = .{ .identity = owner, .reporter_incarnation = 1, .authority = .{ .raft = 18 }, .root_generation = 1, .signature = undefined },
    }).sign(root, a);
    try std.testing.expectError(error.CatalogPublicationProofPending, P.advance(region.allocator(), record, 1, decision_owner, .{ .publication_vote = outbox_receipt }));
    const terminal = try (TerminalReceipt{
        .value = .{ .capture_ref = outbox_receipt.reference, .position = .{ .term = 4, .index = 12 }, .status = .committed, .commit_version = 30 },
        .completed = .{ .incarnation = 17, .term = 4, .applied_index = 12 },
        .proof = .{ .identity = owner, .reporter_incarnation = 1, .authority = .{ .raft = 18 }, .root_generation = 1, .signature = undefined },
    }).sign(root, a);
    try terminal.validate(a);
    var changed_terminal = terminal;
    changed_terminal.value.status = .aborted;
    try std.testing.expectError(error.InvalidGenerationPublication, changed_terminal.validate(a));
    changed_terminal = terminal;
    changed_terminal.completed.applied_index = 11;
    try std.testing.expectError(error.InvalidLogicalCaptureControl, changed_terminal.validate(a));
    var wrong_receipt = outbox_receipt;
    wrong_receipt.reference.encoded_bytes = 1025;
    wrong_receipt = try wrong_receipt.sign(root, a);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, P.advance(region.allocator(), retained, retained.revision, decision_owner, .{ .publication_vote = wrong_receipt }));
    const voted = try P.advance(region.allocator(), retained, retained.revision, decision_owner, .{ .publication_vote = outbox_receipt });
    try std.testing.expectEqual(@as(u64, 1), voted.votes);
    try std.testing.expectEqual(voted.revision, (try P.advance(region.allocator(), voted, voted.revision, decision_owner, .{ .publication_vote = outbox_receipt })).revision);
    const prepared_record = try P.advance(region.allocator(), voted, voted.revision, decision_owner, .prepare);
    try std.testing.expectEqual(P.Phase.prepared, prepared_record.phase);
    const scratch = region.allocator();
    const terminal_action: P.Action = .{ .publication_terminal = terminal };
    try std.testing.expectError(error.CatalogPublicationProofPending, P.advance(scratch, prepared_record, prepared_record.revision, decision_owner, terminal_action));
    const committed = try P.advance(scratch, prepared_record, prepared_record.revision, decision_owner, .{ .commit = .{ .version = 30 } });
    const adopted = try P.advance(scratch, committed, committed.revision, decision_owner, terminal_action);
    try std.testing.expectEqual(terminal.value, adopted.publication_terminals[0]);
    const Journal = @import("../system_catalog/replication/decision_journal.zig");
    const Txn = struct {
        alloc: A,
        state: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn put(self: *@This(), address: []const u8, value: []const u8) !void {
            try self.state.put(self.alloc, address, value);
        }
        pub fn delete(self: *@This(), address: []const u8) !void {
            if (!self.state.remove(address)) return error.NotFound;
        }
        pub fn get(self: *@This(), address: []const u8) ![]const u8 {
            return self.state.get(address) orelse error.NotFound;
        }
        fn apply(self: *@This(), alloc: A, writes: []const Journal.Write) !void {
            for (writes) |write| try self.state.put(alloc, write.key, write.value);
        }
    };
    var txn: Txn = .{ .alloc = scratch };
    const realm = request.scope.realm;
    const slot_request: publisher.CreateSlot = .{ .database_id = realm.database_id, .name = "integration", .snapshot = .nothing, .connection = @splat(1), .nonce = @splat(2) };
    const slot = try Journal.createSlot(scratch, &txn, realm, slot_request);
    var acquire: @FieldType(publisher.Control, "acquire") = .{
        .database_id = realm.database_id,
        .slot = slot.reference,
        .connection = slot_request.connection,
        .nonce = @splat(3),
        .observed_at_unix_ns = 10,
        .expires_at_unix_ns = 100,
    };
    const slot_lease = (try Journal.acquireSlot(scratch, &txn, realm, acquire)).lease.?;
    var feed: publisher.FeedRequest = .{ .lease = slot_lease, .observed_at_unix_ns = 11, .after = 1, .limit = 1, .publication_names = &.{"pub"} };
    const active_publications = [_]publisher.Publication{.{ .id = 4, .generation = 1, .digest = @splat(6) }};
    var feedback: publisher.Acknowledge = .{ .lease = slot_lease, .received = 5, .flushed = 5, .applied = 5, .observed_at_unix_ns = 12 };

    const reserved = try Journal.reserve(scratch, &txn, request.scope.realm, committed, 1234);
    try txn.apply(scratch, &reserved);
    try std.testing.expectEqual(.pending, try Journal.readNext(scratch, &txn, request.scope.realm, 1));
    try std.testing.expectError(error.CatalogPublicationProofPending, Journal.handoff(scratch, &txn, request.scope.realm, committed));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, Journal.reserve(scratch, &txn, request.scope.realm, committed, 1234));
    var later = adopted;
    later.identity.operation_id = @splat(44);
    later.identity.txn_id = @splat(45);
    var later_capacity = capacity.value;
    later_capacity.owner = try later.identity.owner(scratch);
    later_capacity.transaction_id = later.identity.txn_id;
    later.publication_capacities = &.{later_capacity};
    var later_terminal = terminal.value;
    later_terminal.capture_ref.owner = later_capacity.owner;
    later_terminal.capture_ref.transaction_id = later_capacity.transaction_id;
    later_terminal.capture_ref.capacity_digest = try later_capacity.digest(scratch);
    later.publication_captures = &.{later_terminal.capture_ref};
    later.publication_terminals = &.{later_terminal};
    const later_reserved = try Journal.reserve(scratch, &txn, request.scope.realm, later, 1235);
    try txn.apply(scratch, &later_reserved);
    const later_handoff = try Journal.handoff(scratch, &txn, request.scope.realm, later);
    try txn.apply(scratch, &later_handoff);
    try std.testing.expectEqual(.pending, try Journal.readNext(scratch, &txn, request.scope.realm, 1));

    try std.testing.expectError(error.SqlSnapshotCaptureChanged, Journal.snapshotPoint(scratch, &txn, realm));
    const probe: Journal.FeedProbe = .{ .realm = realm, .lease = slot_lease, .after = 1 };
    try std.testing.expect(!(try Journal.feedReadiness(scratch, &txn, probe, 11)).available);
    const waiting = try Journal.offerFeed(scratch, &txn, realm, feed, &active_publications);
    try std.testing.expectEqual(@as(usize, 0), waiting.entries.len);
    try std.testing.expectEqual(@as(u64, 1), waiting.offered_lsn);
    try std.testing.expectError(error.InvalidReplicationAcknowledgement, Journal.acknowledgeFeed(scratch, &txn, realm, feedback));

    const handoff = try Journal.handoff(scratch, &txn, request.scope.realm, adopted);
    try txn.apply(scratch, &handoff);
    try std.testing.expectEqual(@as(u64, 5), try Journal.snapshotPoint(scratch, &txn, realm));
    const ready = (try Journal.readNext(scratch, &txn, request.scope.realm, 1)).ready;
    try std.testing.expectEqual(@as(u64, 1234), ready.transaction.commit_time_unix_us);
    try std.testing.expectEqual(@as(u32, 3), ready.xid);
    const ready_later = (try Journal.readNext(scratch, &txn, request.scope.realm, ready.end_lsn)).ready;
    try std.testing.expectEqual(@as(u32, 4), ready_later.xid);
    try std.testing.expectEqual(later.identity.txn_id, ready_later.transaction.transaction_id);
    try std.testing.expectEqual(.caught_up, try Journal.readNext(scratch, &txn, request.scope.realm, ready_later.end_lsn));
    try std.testing.expect((try Journal.feedReadiness(scratch, &txn, probe, 11)).available);
    const first_offer: Journal.ControlRequest = .{ .realm = realm, .nonce = @splat(4), .observed_head = 0, .action = .{ .feed = feed } };
    const plan = try Journal.planControl(scratch, &txn, first_offer, &active_publications);
    try std.testing.expectError(error.CatalogPublicationProofPending, Journal.readOfferedPage(scratch, &txn, first_offer, 11));
    try plan.apply(&txn);
    const slot_view = try Journal.readSlotView(scratch, &txn, realm, slot_lease.slot.name);
    try std.testing.expectEqual(@as(u64, 1), slot_view.control_head);
    try std.testing.expect(slot_view.slot.?.reference.eql(slot_lease.slot));
    const missing_slot_view = try Journal.readSlotView(scratch, &txn, realm, "missing");
    try std.testing.expectEqual(slot_view.control_head, missing_slot_view.control_head);
    try std.testing.expect(missing_slot_view.slot == null);
    try std.testing.expectEqual(@as(usize, 0), (try Journal.planControl(scratch, &txn, first_offer, error.PublicationNotFound)).writes.len);
    var conflicting = first_offer;
    conflicting.action.feed.limit += 1;
    try std.testing.expectError(error.SequenceAdmissionConflict, Journal.planControl(scratch, &txn, conflicting, &active_publications));
    const first_page = try Journal.readOfferedPage(scratch, &txn, first_offer, 11);
    try Journal.verifyKey(scratch, &txn, realm.group_id, incarnation, incarnation ++ ":0000000000000001:slot_control_head", P);
    try std.testing.expectEqual(@as(usize, 1), first_page.entries.len);
    try std.testing.expectEqual(ready.transaction.transaction_id, first_page.entries[0].transaction.transaction_id);
    try std.testing.expectEqual(@as(u64, 3), first_page.offered_lsn);
    var capture_read: publisher.OutboxRead = .{ .realm = realm, .lease = slot_lease, .reference = outbox_receipt.reference, .table_name = participants[0].table_name };
    try std.testing.expect(try Journal.authorizeFeedCapture(scratch, &txn, capture_read, 12));
    capture_read.reference = later_terminal.capture_ref;
    try std.testing.expect(!try Journal.authorizeFeedCapture(scratch, &txn, capture_read, 12));
    feedback.received = 3;
    feedback.flushed = 3;
    feedback.applied = 3;
    _ = try Journal.acknowledgeFeed(scratch, &txn, realm, feedback);
    capture_read.reference = outbox_receipt.reference;
    try std.testing.expect(!try Journal.authorizeFeedCapture(scratch, &txn, capture_read, 12));
    feed.observed_at_unix_ns = 13;
    feed.after = 3;
    const second_page = try Journal.offerFeed(scratch, &txn, realm, feed, &active_publications);
    try std.testing.expectEqual(ready_later.transaction.transaction_id, second_page.entries[0].transaction.transaction_id);
    try std.testing.expectEqual(@as(u64, 5), second_page.offered_lsn);
    try Journal.verifyKey(scratch, &txn, realm.group_id, incarnation, incarnation ++ ":0000000000000001:slot/integration", P);

    acquire.observed_at_unix_ns = 100;
    acquire.expires_at_unix_ns = 200;
    acquire.connection = @splat(7);
    const successor = (try Journal.acquireSlot(scratch, &txn, realm, acquire)).lease.?;
    try std.testing.expect(successor.generation > slot_lease.generation);
    try std.testing.expectError(error.ReplicationSlotLeaseChanged, Journal.offerFeed(scratch, &txn, realm, feed, &active_publications));
    try std.testing.expectError(error.ReplicationSlotLeaseChanged, Journal.feedReadiness(scratch, &txn, probe, 101));
    try Journal.releaseSlot(scratch, &txn, realm, successor);
    try Journal.dropSlot(scratch, &txn, realm, .{ .database_id = realm.database_id, .slot = slot.reference });
    const recreated = try Journal.createSlot(scratch, &txn, realm, slot_request);
    try std.testing.expect(recreated.reference.birth > slot.reference.birth);
    const recreated_view = try Journal.readSlotView(scratch, &txn, realm, slot.reference.name);
    try std.testing.expect(recreated_view.slot.?.reference.eql(recreated.reference));
    var other_realm = realm;
    other_realm.database_id += 1;
    try std.testing.expect((try Journal.readSlotView(scratch, &txn, other_realm, slot.reference.name)).slot == null);
    for (0..64) |index| {
        const repeated: Journal.ControlRequest = .{
            .realm = realm,
            .nonce = @splat(@as(u8, @intCast(index)) + 16),
            .observed_head = @as(u64, @intCast(index)) + 1,
            .action = .{ .create_slot = slot_request },
        };
        const rejected = try Journal.planControl(scratch, &txn, repeated, &.{});
        try std.testing.expectEqual(Journal.ControlRefusal.ReplicationSlotAlreadyExists, rejected.result.refused);
        try rejected.apply(&txn);
    }
    try std.testing.expect((try Journal.readControlOutcome(scratch, &txn, first_offer)) == null);
    try std.testing.expectError(error.SequenceAdmissionExpired, Journal.planControl(scratch, &txn, first_offer, &active_publications));
    try std.testing.expectError(error.ReplicationSlotLeaseChanged, Journal.expireSlot(scratch, &txn, realm, .{ .lease = slot_lease, .expected_expiry = 100, .observed_at_unix_ns = 101 }));

    try Journal.verifyKey(scratch, &txn, request.scope.realm.group_id, incarnation, incarnation ++ ":0000000000000001:head", P);
    try std.testing.expect(txn.state.remove(reserved[2].key));
    try std.testing.expectError(error.InvalidCatalogRecord, Journal.verifyKey(scratch, &txn, request.scope.realm.group_id, incarnation, incarnation ++ ":0000000000000001:row/0000000000000003", P));
    const adopted_json = try std.json.Stringify.valueAlloc(scratch, adopted, .{});
    const cold = try std.json.parseFromSliceLeaky(P.Record, scratch, adopted_json, .{});
    try cold.validate(scratch);
    try std.testing.expectEqual(adopted.revision, (try P.advance(scratch, cold, cold.revision, decision_owner, terminal_action)).revision);

    const aborted = try P.advance(scratch, retained, retained.revision, decision_owner, .{ .abort = 30 });
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, P.advance(scratch, aborted, aborted.revision, decision_owner, terminal_action));
    changed_terminal = terminal;
    changed_terminal.value.status = .aborted;
    changed_terminal = try changed_terminal.sign(root, a);
    const adopted_abort = try P.advance(scratch, aborted, aborted.revision, decision_owner, .{ .publication_terminal = changed_terminal });
    try std.testing.expectEqual(@as(u64, 0), adopted_abort.votes);
    try std.testing.expectEqual(.aborted, adopted_abort.publication_terminals[0].status);
    var abort_txn: Txn = .{ .alloc = scratch };
    var debt: std.ArrayListUnmanaged(Journal.Write) = .empty;
    try Journal.handoffAbort(scratch, &abort_txn, request.scope.realm, adopted_abort, &debt);
    try std.testing.expectEqual(@as(usize, 1), debt.items.len);
    try abort_txn.apply(scratch, debt.items);
    var journal_prefix_buffer: [96]u8 = undefined;
    const journal_prefix = try Journal.groupPrefix(&journal_prefix_buffer, request.scope.realm.group_id);
    try Journal.verifyKey(scratch, &abort_txn, request.scope.realm.group_id, incarnation, debt.items[0].key[journal_prefix.len..], P);
    const cleanup: publisher.GcPermission = .{
        .realm = request.scope.realm,
        .action = .purge,
        .debt = .{
            .reference = adopted_abort.publication_terminals[0].capture_ref,
            .table_name = participants[0].table_name,
            .reason = .{ .aborted = 30 },
            .decision_version = 30,
        },
    };
    try std.testing.expect((try Journal.authorizeCollection(scratch, &abort_txn, request.scope.realm.group_id, incarnation, cleanup)) != null);
    var wrong_cleanup = cleanup;
    wrong_cleanup.debt.table_name = "different_table";
    try std.testing.expect((try Journal.authorizeCollection(scratch, &abort_txn, request.scope.realm.group_id, incarnation, wrong_cleanup)) == null);
    try std.testing.expect((try Journal.authorizeCollection(scratch, &abort_txn, request.scope.realm.group_id, @splat('1'), cleanup)) == null);
    const collected = try (CollectionReceipt{
        .permission = cleanup,
        .value = .{
            .action = .purge,
            .debt_digest = try cleanup.debt.digest(a),
            .reference_digest = try publisher.captureReferenceDigest(a, cleanup.debt.reference),
            .header_digest = @splat(99),
            .physical_root_incarnation = 33,
            .position = .{ .raft = .{ .term = 4, .index = 13 } },
            .payload_bytes = cleanup.debt.reference.encoded_bytes,
        },
        .completed = .{ .incarnation = 17, .term = 4, .applied_index = 13 },
        .proof = .{ .identity = owner, .reporter_incarnation = 1, .authority = .{ .raft = 18 }, .root_generation = 1, .signature = undefined },
    }).sign(root, a);
    const command = @import("../system_catalog/catalog_mutations.zig").publicationCollectionAckCommand(collected);
    const command_bytes = try std.json.Stringify.valueAlloc(scratch, command, .{});
    try std.testing.expect(@import("../metadata/schema_reader_wire.zig").requiresSqlPublicationCollection(command_bytes));
    try Journal.acceptCollection(scratch, &abort_txn, cleanup, collected.value);
    try Journal.acceptCollection(scratch, &abort_txn, cleanup, collected.value);
    try std.testing.expect((try Journal.authorizeCollection(scratch, &abort_txn, request.scope.realm.group_id, incarnation, cleanup)) == null);
    var clear = cleanup;
    clear.action = .clear_marker;
    clear.purged = collected.value;
    clear.purged_digest = try collected.value.digest(a);
    try std.testing.expect((try Journal.authorizeCollection(scratch, &abort_txn, request.scope.realm.group_id, incarnation, clear)) != null);
    var cleared = collected;
    cleared.permission = clear;
    cleared.value.action = .clear_marker;
    cleared.value.evidence = .absence;
    cleared.value.purged_digest = clear.purged_digest;
    cleared.value.position.raft.index += 1;
    cleared.completed.applied_index += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, cleared.validate(a));
    cleared = try cleared.sign(root, a);
    try Journal.acceptCollection(scratch, &abort_txn, clear, cleared.value);
    try Journal.acceptCollection(scratch, &abort_txn, clear, cleared.value);
    try std.testing.expect((try Journal.authorizeCollection(scratch, &abort_txn, request.scope.realm.group_id, incarnation, clear)) == null);

    const absent = try (AbortFenceReceipt{
        .value = .{
            .owner = decision_owner,
            .transaction_id = record.identity.txn_id,
            .member = 0,
            .group_id = 30,
            .namespace = request.namespace,
            .physical_root_incarnation = 33,
            .position = .{ .term = 4, .index = 12 },
            .capacity_digest = try capacity.value.digest(a),
            .evidence = .{ .expiry_floor = decision_owner.expires_at_unix_ns },
        },
        .completed = .{ .incarnation = 17, .term = 4, .applied_index = 12 },
        .proof = .{ .identity = owner, .reporter_incarnation = 1, .authority = .{ .raft = 18 }, .root_generation = 1, .signature = undefined },
    }).sign(root, a);
    try std.testing.expectError(error.CatalogPublicationProofPending, P.advance(scratch, committed, committed.revision, decision_owner, .{ .publication_abort_fence = absent }));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, P.advance(scratch, adopted_abort, adopted_abort.revision, decision_owner, .{ .publication_abort_fence = absent }));
    const fenced = try P.advance(scratch, aborted, aborted.revision, decision_owner, .{ .publication_abort_fence = absent });
    try std.testing.expectEqual(fenced.revision, (try P.advance(scratch, fenced, fenced.revision, decision_owner, .{ .publication_abort_fence = absent })).revision);
    var no_debt: std.ArrayListUnmanaged(Journal.Write) = .empty;
    try Journal.handoffAbort(scratch, &abort_txn, request.scope.realm, fenced, &no_debt);
    try std.testing.expectEqual(@as(usize, 0), no_debt.items.len);
    const unresolved_proof = try P.advance(scratch, aborted, aborted.revision, decision_owner, .{ .resolve = 1 });
    try std.testing.expectError(error.CatalogPublicationProofPending, P.advance(scratch, unresolved_proof, unresolved_proof.revision, decision_owner, .handoff));
    const resolved_proof = try P.advance(scratch, fenced, fenced.revision, decision_owner, .{ .resolve = 1 });
    try std.testing.expectEqual(P.Phase.retired, (try P.advance(scratch, resolved_proof, resolved_proof.revision, decision_owner, .handoff)).phase);

    const image: capture.OwnerObservation = .{
        .request = request,
        .schema_digest = request.schema_digest,
        .catalog_digest = @splat(12),
        .physical_root_incarnation = 33,
        .position = null,
        .has_user_data = false,
        .has_transaction_state = false,
    };
    const observation = try (ObservationReceipt{ .value = image, .proof = .{
        .identity = owner,
        .reporter_incarnation = 1,
        .authority = .{ .native = try authority.digest() },
        .root_generation = 1,
        .signature = undefined,
    } }).sign(root, a);
    try observation.validate(request, a);
    var different_request = request;
    different_request.catalog_revision += 1;
    try std.testing.expectError(error.LogicalCaptureControlChanged, observation.validate(different_request, a));
    var forged = observation;
    forged.value.catalog_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, forged.validate(request, a));
    forged = observation;
    forged.value.has_user_data = true;
    try std.testing.expectError(error.InvalidLogicalCaptureControl, forged.validate(request, a));

    var raft = observation;
    raft.proof.authority = .{ .raft = 18 };
    raft = try raft.sign(root, a);
    try std.testing.expectError(error.InvalidLogicalCaptureControl, raft.validate(request, a));
    raft.completed = .{ .incarnation = 17, .term = 3, .applied_index = 10 };
    raft = try raft.sign(root, a);
    try raft.validate(request, a);
    raft.value.position = .{ .raft = .{ .term = 3, .index = 11 } };
    raft = try raft.sign(root, a);
    try std.testing.expectError(error.InvalidLogicalCaptureControl, raft.validate(request, a));

    const control: capture.Control = .{ .binding = try observation.value.binding(), .action = .prepare };
    const receipt = try (GuardReceipt{ .value = .{
        .binding = control.binding,
        .action = .prepare,
        .physical_root_incarnation = image.physical_root_incarnation,
        .position = .{ .native = .{ .namespace = request.namespace, .sequence = 1 } },
    }, .proof = .{
        .identity = owner,
        .reporter_incarnation = 1,
        .authority = observation.proof.authority,
        .root_generation = 1,
        .signature = undefined,
    } }).sign(root, a);
    try receipt.validate(control, a);
    var foreign = receipt;
    foreign.proof.authority = .{ .native = @splat(42) };
    foreign = try foreign.sign(root, a);
    try std.testing.expectError(error.InvalidCatalogRecord, foreign.validate(control, a));
    var wrong_action = control;
    wrong_action.action = .activate;
    try std.testing.expectError(error.LogicalCaptureControlChanged, receipt.validate(wrong_action, a));
    var changed_position = receipt;
    changed_position.value.position.native.sequence += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, changed_position.validate(control, a));
}

test {
    _ = @import("sql/execution/snapshot_exports.zig");
}
