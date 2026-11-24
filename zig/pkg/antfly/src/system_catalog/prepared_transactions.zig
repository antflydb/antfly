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

//! Metadata-owned external transaction identities and monotone phase changes.
//! Native votes and immutable postimages remain in the existing data journals.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
pub const Principal = local.system_catalog_privileges.Principal;
pub const Owner = local.storage_transactions.ExternalDecisionOwner;
pub const max_participants = 64;
pub const max_plan_bytes = 8 * 1024 * 1024;
pub const max_named = local.storage_transactions.max_external_prepared_transactions;
pub const max_session_commits = 128;
pub const max_replication_transactions = 128;
pub const max_active = max_named + max_session_commits + max_replication_transactions;
pub const Kind = enum { named, session_commit, replication };
pub const ReplicationReference = local.api_replication_source_reference.Reference;
const ReplicationPermit = local.api_replication_source_apply.Permit;
const ReplicationAck = local.api_replication_source_apply.AckCompletion;
pub const max_bytes = 256 * 1024;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Actor = @import("authorization.zig").Actor;

pub fn validateGid(gid: []const u8) !void {
    if (gid.len >= 200 or !std.unicode.utf8ValidateSlice(gid) or std.mem.indexOfScalar(u8, gid, 0) != null) return error.InvalidPreparedTransactionId;
}

pub const Participant = struct {
    table_name: []const u8,
    route: local.metadata_catalog_route_contract.CatalogRouteFence,
    schema_version: ?u32 = null,
    sql_locks: ?local.storage_pessimistic_lock_participant.Admission = null,
    publication_capture: ?local.system_catalog_logical_replication_capture.DemandReference = null,

    pub fn jsonStringify(self: Participant, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Participant).@"struct".field_names) |field| {
            const omitted = if (comptime std.mem.eql(u8, field, "sql_locks")) self.sql_locks == null else if (comptime std.mem.eql(u8, field, "publication_capture")) self.publication_capture == null else false;
            if (!omitted) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn lockReference(self: Participant, identity: Identity, a: A) !?local.storage_pessimistic_lock_transaction.Ref {
        const admission = self.sql_locks orelse return null;
        const owner = try identity.owner(a);
        const result: local.storage_pessimistic_lock_transaction.Ref = .{ .admission = admission, .decision = .{ .operation = owner.operation_id, .admission = owner.admission_digest } };
        try result.validate(identity.txn_id, owner);
        return result;
    }

    pub fn validate(self: Participant, incarnation: [32]u8) !void {
        try self.route.validate();
        if (self.table_name.len == 0 or self.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(self.table_name) or std.mem.indexOfScalar(u8, self.table_name, 0) != null or
            self.route.table_id == 0 or self.route.route.group_id == 0 or self.route.topology_epoch == 0 or self.route.metadata_group_id == 0 or
            self.route.route.identity_namespace.table_id != self.route.table_id or
            self.route.metadata_incarnation == null or !std.mem.eql(u8, &self.route.metadata_incarnation.?, &incarnation)) return error.InvalidCatalogMutation;
        if (self.route.route.execution == .native_local and self.route.native_participant == null) return error.InvalidCatalogMutation;
        if (self.publication_capture) |reference| {
            try reference.validate();
            if (reference.realm.group_id != self.route.metadata_group_id or
                !std.mem.eql(u8, &reference.realm.incarnation, &incarnation))
                return error.InvalidCatalogMutation;
        }
        if (self.sql_locks) |admission| {
            try admission.validate();
            const expected = try (local.storage_pessimistic_lock_authority.Data.Spec{ .table_name = self.table_name, .route = self.route, .schema_version = self.schema_version }).binding(.{ .home = .metadata, .group = self.route.metadata_group_id, .incarnation = admission.binding.namespace.incarnation });
            if (admission.purpose != .resolution or !admission.binding.eql(expected)) return error.InvalidCatalogMutation;
        }
    }
};

pub const SessionRef = struct {
    owner_node_id: u64,
    connection_id: ?[32]u8 = null,
    /// The local sentinel is meaningful only in the standalone catalog realm.
    /// The containing identity independently binds its catalog incarnation.
    standalone: bool = false,
    pub fn jsonStringify(self: SessionRef, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(SessionRef).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "standalone")) or self.standalone) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: SessionRef) !void {
        if ((self.owner_node_id == 0) != self.standalone) return error.InvalidCatalogMutation;
        if (self.connection_id) |id| {
            for (id) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidCatalogMutation;
            if (std.mem.allEqual(u8, &id, '0')) return error.InvalidCatalogMutation;
        }
    }
    pub fn requireRealm(self: SessionRef, standalone: bool) !void {
        try self.validate();
        if (self.standalone != standalone) return error.CatalogGenerationChanged;
    }
};

/// Feature requirements compose: a notification or connection contract may
/// never weaken the decoder needed to own/release native lock custody.
fn decoderFloor(connection: bool, locks: bool, handoff: bool, notifications: bool) u16 {
    const protocol = @import("../metadata/topology_protocol.zig");
    var minimum = protocol.sql_prepared_transactions_version;
    if (connection) minimum = @max(minimum, protocol.sql_connection_transactions_version);
    if (locks) minimum = @max(minimum, protocol.sql_native_lock_custody_version);
    if (handoff) minimum = @max(minimum, protocol.sql_prepared_lock_handoff_version);
    if (notifications) minimum = @max(minimum, protocol.sql_notifications_version);
    return minimum;
}

/// The immutable staged cohort is part of the prepared decision identity.
pub const CatalogParticipant = struct {
    staging_id: [16]u8,
    plan_digest: [32]u8,
    intent_digest: [32]u8,

    pub fn validate(self: CatalogParticipant) !void {
        if (std.mem.allEqual(u8, &self.staging_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.intent_digest, 0)) return error.InvalidCatalogMutation;
    }
};

/// The accepting metadata writer obtains this binding from the real SQL
/// command before creating any hidden restore owners. Recovery uses the
/// immutable operation ID after named PREPARE detaches the source session.
pub const CatalogBinding = struct {
    fence: local.storage_pessimistic_lock_authority.CommandFence,
    transaction: [16]u8,
    operation: [16]u8,
    intent_digest: [32]u8,

    pub fn validate(self: CatalogBinding, group: u64) !void {
        try self.fence.validate();
        if (group != self.fence.group or std.mem.allEqual(u8, &self.transaction, 0) or
            std.mem.allEqual(u8, &self.operation, 0) or std.mem.allEqual(u8, &self.intent_digest, 0))
            return error.InvalidCatalogMutation;
    }

    pub fn requireBirth(self: CatalogBinding, a: A, txn: anytype, group: u64) !local.storage_pessimistic_lock_authority.Record {
        try self.validate(group);
        if (try readOperation(a, txn, group, self.operation) != null or try readReceipt(a, txn, group, self.operation) != null)
            return error.InvalidCatalogMutation;
        const locks = local.storage_pessimistic_lock_authority;
        const namespace: locks.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = self.fence.backend.incarnation };
        const frame = try locks.guard(a, txn, namespace, self.fence.backend, self.fence.command, self.fence);
        const transaction = frame.transaction orelse return error.SqlTransactionNotActive;
        if (!std.mem.eql(u8, &transaction.id, &self.transaction)) return error.ExternalTransactionDecisionChanged;
        return frame;
    }

    pub fn requireIdentity(self: CatalogBinding, a: A, group: u64, identity: Identity, participant: CatalogParticipant) !void {
        try self.validate(group);
        try participant.validate();
        const expected = identity.catalog_participant orelse return error.InvalidPreparedTransactionPhase;
        const scope = identity.lock_scope orelse return error.ExternalTransactionDecisionChanged;
        const session = identity.session orelse return error.ExternalTransactionDecisionChanged;
        if (!std.meta.eql(participant, expected) or !std.mem.eql(u8, &participant.intent_digest, &self.intent_digest) or
            !std.mem.eql(u8, &identity.txn_id, &self.transaction) or !std.mem.eql(u8, &identity.operation_id, &self.operation) or
            !std.mem.eql(u8, &identity.metadata_incarnation, &std.fmt.bytesToHex(self.fence.backend.incarnation, .lower)) or
            identity.database_id != self.fence.database or session.standalone != self.fence.standalone or
            !scope.backend.eql(self.fence.backend) or !std.meta.eql(scope.command, self.fence.command) or
            !std.mem.eql(u8, &self.fence.principal, &try @import("lock_authority.zig").principalDigest(a, identity.principal)))
            return error.ExternalTransactionDecisionChanged;
    }
};

pub const CatalogOutcome = enum { pending, published, canceled };

const history = local.api_local_table_writes.table_write_source.history_resolution_wire.completion;

pub const Identity = struct {
    history_completion: ?history.Admission = null,
    catalog_participant: ?CatalogParticipant = null,
    notification_plan: ?local.sql_ast.notifications.Plan = null,
    kind: Kind = .named,
    replication_source: ?ReplicationReference = null,
    lock_scope: ?@import("lock_authority.zig").Ref = null,
    commit_sync_level: local.storage_db_types.SyncLevel = .write,
    session: ?SessionRef = null,
    gid: []const u8,
    database_id: u64,
    native_service: ?local.system_catalog_domain.ServiceOwner = null,
    principal: Principal,
    txn_id: [16]u8,
    operation_id: [16]u8,
    metadata_incarnation: [32]u8,
    begin_timestamp: u64,
    expires_at_unix_ns: u64,
    plan_digest: [32]u8,
    manifest_digest: [32]u8,

    pub fn minimumDecoderVersion(self: Identity) u16 {
        const protocol = @import("../metadata/topology_protocol.zig");
        if (self.history_completion != null) return protocol.sql_external_completion_version;
        if (self.catalog_participant != null) return protocol.sql_catalog_completion_version;
        return decoderFloor(self.requiresConnectionDecoder(), self.lock_scope != null, self.lock_scope != null and self.kind == .named, self.notification_plan != null);
    }
    pub fn preparedLockLease(self: Identity, a: A, group: u64) !local.storage_pessimistic_lock_ledger.Lease {
        if (self.kind != .named or self.lock_scope == null or self.session == null) return error.InvalidCatalogMutation;
        var incarnation: [16]u8 = undefined;
        _ = try std.fmt.hexToBytes(&incarnation, &self.metadata_incarnation);
        const decision_owner = try self.owner(a);
        return local.storage_pessimistic_lock_authority.preparedLease(.{ .home = .metadata, .group = group, .incarnation = incarnation }, self.database_id, self.session.?.owner_node_id, .{ .operation = decision_owner.operation_id, .admission = decision_owner.admission_digest });
    }
    pub fn requiresConnectionDecoder(self: Identity) bool {
        return self.kind == .session_commit or (self.session != null and self.session.?.standalone);
    }

    pub fn validate(self: Identity) !void {
        if (self.history_completion) |admission| {
            if (self.kind == .replication or admission.version < 3 or
                !std.mem.eql(u8, &admission.transaction_id, &self.txn_id)) return error.InvalidCatalogMutation;
            try admission.validate();
            if (admission.participants.len > max_participants) return error.CatalogCommandTooLarge;
            for (admission.participants) |member| {
                const routed = member.routed_owner orelse return error.InvalidCatalogMutation;
                if (!std.mem.eql(u8, &routed.metadata_incarnation, &self.metadata_incarnation) or
                    member.roles & history.Roles.checkpoint != 0) return error.InvalidCatalogMutation;
            }
        }
        if (self.catalog_participant) |catalog| {
            try catalog.validate();
            if (self.kind == .replication or self.lock_scope == null or self.session == null or
                self.session.?.connection_id == null) return error.InvalidCatalogMutation;
        }
        if (self.kind == .replication) {
            const source = self.replication_source orelse return error.InvalidCatalogMutation;
            try source.validate();
            if (self.principal.kind != .bootstrap or self.gid.len != 0 or self.session != null or
                self.native_service != null or self.lock_scope != null or self.notification_plan != null or self.commit_sync_level != .write or
                !std.mem.eql(u8, &source.txn_id, &self.txn_id) or !std.mem.eql(u8, &self.txn_id, &self.operation_id) or
                !std.mem.eql(u8, &source.manifest_digest, &self.manifest_digest)) return error.InvalidCatalogMutation;
            if (source.owner == .logical_subscription and source.owner.logical_subscription.database_id != self.database_id) return error.InvalidCatalogMutation;
        } else if (self.replication_source != null) return error.InvalidCatalogMutation;
        if (self.notification_plan) |notifications| {
            try notifications.validate();
            if (self.kind != .session_commit or self.lock_scope == null or self.session == null or self.session.?.connection_id == null or !std.mem.eql(u8, &notifications.connection, &self.session.?.connection_id.?)) return error.InvalidCatalogMutation;
        }
        if (self.lock_scope) |scope| {
            try scope.backend.validate();
            try scope.command.validate();
            if (self.session == null or self.session.?.connection_id == null or !std.mem.eql(u8, &self.metadata_incarnation, &std.fmt.bytesToHex(scope.backend.incarnation, .lower))) return error.InvalidCatalogMutation;
        }
        if (self.session) |session| try session.validate();
        try validateGid(self.gid);
        if (self.kind == .session_commit and (self.gid.len != 0 or self.session == null or self.session.?.connection_id == null)) return error.InvalidCatalogMutation;
        if (self.kind == .session_commit and self.commit_sync_level == .propose) return error.InvalidCatalogMutation;
        if (self.kind == .named and self.commit_sync_level != .write) return error.InvalidCatalogMutation;
        try self.principal.validate();
        if (self.principal.kind == .public or self.database_id == 0 or self.begin_timestamp == 0 or
            std.mem.allEqual(u8, &self.txn_id, 0) or std.mem.allEqual(u8, &self.operation_id, 0) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or std.mem.allEqual(u8, &self.manifest_digest, 0)) return error.InvalidCatalogMutation;
        try (Owner{ .metadata_incarnation = self.metadata_incarnation, .operation_id = self.operation_id, .admission_digest = @splat(1), .expires_at_unix_ns = self.expires_at_unix_ns }).validate();
    }

    pub fn jsonStringify(self: Identity, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Identity).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "catalog_participant") or std.mem.eql(u8, field, "lock_scope") or std.mem.eql(u8, field, "notification_plan") or std.mem.eql(u8, field, "replication_source") or std.mem.eql(u8, field, "history_completion")) {
                if (@field(self, field) != null) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            } else if (comptime std.mem.eql(u8, field, "kind") or std.mem.eql(u8, field, "commit_sync_level")) {
                if (self.kind != .named) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            } else {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    fn validateHistoryCohort(self: Identity, participants: []const Participant) !void {
        const admission = self.history_completion orelse return;
        if (participants.len != admission.participants.len) return error.InvalidCatalogMutation;
        const coordinator = try admission.coordinatorOwner();
        if (participants[0].route.route.group_id != coordinator.group_id) return error.InvalidCatalogMutation;
        var matched: u64 = 0;
        for (admission.participants) |member| {
            const routed = member.routed_owner orelse return error.InvalidCatalogMutation;
            const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidCatalogMutation;
            if (locator.group_id != routed.group_id or locator.restore_staging_scope != null or locator.restore_staging_plan_id != null)
                return error.InvalidCatalogMutation;
            for (participants, 0..) |participant, index| {
                const fence = participant.route;
                if (fence.route.group_id != routed.group_id) continue;
                const ns = fence.route.identity_namespace;
                const namespace = local.storage_db_online_source_contract.namespaceBytes(.{ .table_id = ns.table_id, .shard_id = ns.shard_id, .range_id = ns.range_id });
                if (fence.route.execution != .raft or routed.metadata_group_id != fence.metadata_group_id or
                    routed.topology_epoch != fence.topology_epoch or !std.mem.eql(u8, &routed.namespace, &namespace) or
                    !std.mem.eql(u8, locator.table_name, participant.table_name)) return error.InvalidCatalogMutation;
                const bit = @as(u64, 1) << @as(u6, @intCast(index));
                if (matched & bit != 0) return error.InvalidCatalogMutation;
                matched |= bit;
                break;
            } else return error.InvalidCatalogMutation;
        }
    }

    pub fn owner(self: Identity, a: A) !Owner {
        try self.validate();
        return .{ .metadata_incarnation = self.metadata_incarnation, .operation_id = self.operation_id, .admission_digest = try digest(a, "antfly:sql-prepared-admission:v1\x00", self), .expires_at_unix_ns = self.expires_at_unix_ns };
    }
};

fn digest(a: A, domain: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(domain);
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn planDigest(bytes: []const u8) ![32]u8 {
    if (bytes.len == 0 or bytes.len > max_plan_bytes or !std.unicode.utf8ValidateSlice(bytes)) return error.CatalogCommandTooLarge;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:sql-prepared-execution-plan:v1\x00");
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn manifestDigest(a: A, manifest: []const Participant, incarnation: [32]u8) ![32]u8 {
    if (manifest.len > max_participants) return error.CatalogCommandTooLarge;
    for (manifest, 0..) |participant, index| {
        try participant.validate(incarnation);
        for (manifest[0..index]) |previous| if (previous.route.route.group_id == participant.route.route.group_id) return error.InvalidCatalogMutation;
    }
    return digest(a, "antfly:sql-prepared-participants:v1\x00", manifest);
}

pub const CommitProgress = enum { resolving, indexing, ready, repair_required };

pub const Phase = enum { preparing, locking, prepared, decided, active, resolved, retired };
pub const DecisionInput = struct {
    version: u64,
    sync_level: local.storage_db_types.SyncLevel = .write,
    commit_time_unix_us: ?u64 = null,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("version");
        try writer.write(self.version);
        try writer.objectField("sync_level");
        try writer.write(self.sync_level);
        if (self.commit_time_unix_us) |time| {
            try writer.objectField("commit_time_unix_us");
            try writer.write(time);
        }
        try writer.endObject();
    }
};
pub const Decision = struct { status: enum { committed, aborted }, version: u64, sync_level: local.storage_db_types.SyncLevel = .write };
pub const NativeRetirement = struct { member: u8, digest: [32]u8 };
const CaptureProofs = @import("../api/logical_replication_publisher_attestation.zig");
const CapacityReceipt = CaptureProofs.CapacityReceipt;
const CaptureRef = local.system_catalog_logical_replication_publisher.CaptureRef;
const CaptureCapacity = local.system_catalog_logical_replication_capacity.Observation;
const CaptureTerminal = local.system_catalog_logical_replication_publisher.OrderedTerminal;

pub const ReplicationEvent = struct {
    kind: enum { snapshot_begin, snapshot_page, initial_copy, transaction },
    source_point: u64,
    end_point: u64,
    offset: u64 = 0,
    next_offset: u64 = 0,
    changes_digest: [32]u8,
    snapshot_page: ?@import("replication/snapshot_chain.zig").Page = null,

    pub fn validate(self: @This()) !void {
        if (self.source_point == 0 or std.mem.allEqual(u8, &self.changes_digest, 0)) return error.InvalidReplicationCutoverIntent;
        if ((self.kind == .snapshot_page) != (self.snapshot_page != null)) return error.InvalidReplicationCutoverIntent;
        switch (self.kind) {
            .transaction => if (self.end_point <= self.source_point or self.offset != 0 or self.next_offset != 0) return error.InvalidReplicationCutoverIntent,
            .snapshot_begin => if (self.end_point != self.source_point or self.offset != 0 or self.next_offset != 0) return error.InvalidReplicationCutoverIntent,
            .snapshot_page => if (self.end_point != self.source_point or self.next_offset <= self.offset) return error.InvalidReplicationCutoverIntent,
            .initial_copy => if (self.end_point != self.source_point or self.next_offset < self.offset) return error.InvalidReplicationCutoverIntent,
        }
    }

    pub fn eventDigest(self: @This()) ![32]u8 {
        try self.validate();
        return @import("../metadata/replication_digest.zig").digest("antfly/subscription-event/v1\x00", self, max_bytes);
    }
};

pub const SnapshotBoundary = struct {
    pub const Custody = struct {
        purpose: local.api_replication_source_purpose.Purpose,
        ready: local.api_replication_source_purpose.Ready,
    };

    source_point: u64,
    total_rows: u64,
    capture_digest: [32]u8,
    page_chain: [32]u8 = @splat(0),
    custody: ?Custody = null,

    fn validateCustody(self: SnapshotBoundary, custody: Custody, group: u64, owner: @FieldType(ReplicationReference, "owner"), progress: ReplicationProgress) !void {
        const held = custody.purpose;
        const ready = custody.ready;
        try held.validate();
        try ready.validate();
        if (held.metadata_group != group or !held.owner.eql(owner) or held.birth != progress.birth or
            !std.mem.eql(u8, &held.source_digest, &progress.origin) or !held.upload.sameIdentity(ready.upload) or
            ready.source_point != self.source_point or ready.total_rows != self.total_rows or
            !std.mem.eql(u8, &ready.capture.content_digest, &self.capture_digest) or
            progress.snapshot_offset > self.total_rows or progress.snapshot_cut != self.source_point)
            return error.InvalidCatalogRecord;
    }

    pub fn requireEvent(self: SnapshotBoundary, event: ReplicationEvent, next_page: [32]u8) !void {
        const chain = @import("replication/snapshot_chain.zig");
        if (self.source_point == 0 or std.mem.allEqual(u8, &self.capture_digest, 0) or
            event.source_point != self.source_point or event.next_offset > self.total_rows)
            return error.ReplicationCutoverAuthorityLost;
        if (event.kind == .snapshot_begin) {
            if (self.custody == null or event.offset != 0 or event.next_offset != 0 or
                !std.mem.eql(u8, &event.changes_digest, &self.capture_digest) or
                !std.mem.eql(u8, &next_page, &self.page_chain))
                return error.ReplicationCutoverAuthorityLost;
            return;
        }
        if (event.kind == .initial_copy) {
            if (event.offset != self.total_rows or event.next_offset != self.total_rows or
                !std.mem.eql(u8, &event.changes_digest, &self.capture_digest) or !std.mem.eql(u8, &next_page, &chain.end))
                return error.ReplicationCutoverAuthorityLost;
            return;
        }
        const page = event.snapshot_page orelse return error.ReplicationCutoverAuthorityLost;
        const commitment = page.commitment() catch return error.ReplicationCutoverAuthorityLost;
        if (page.offset != event.offset or event.next_offset -| event.offset != page.count or
            !std.mem.eql(u8, &page.content_digest, &event.changes_digest) or !std.mem.eql(u8, &commitment, &next_page) or
            ((event.next_offset == self.total_rows) != std.mem.eql(u8, &page.next, &chain.end)))
            return error.ReplicationCutoverAuthorityLost;
    }
};

pub const ReplicationProgress = struct {
    birth: u64,
    origin: [32]u8,
    generation: u64 = 0,
    checkpoint: u64 = 0,
    snapshot_cut: u64 = 0,
    snapshot_offset: u64 = 0,
    snapshot_boundary: ?SnapshotBoundary = null,
    snapshot_next_page: [32]u8 = @splat(0),
    pending: ?ReplicationReference = null,
    last_acknowledged: ?ReplicationReference = null,
    ack_roster_digest: ?[32]u8 = null,

    pub fn initialCompletion(self: @This()) ?ReplicationEvent {
        const boundary = self.snapshot_boundary orelse return null;
        if (self.pending != null or self.checkpoint != 0 or boundary.custody == null) return null;
        const event: ReplicationEvent = .{
            .kind = .initial_copy,
            .source_point = self.snapshot_cut,
            .end_point = self.snapshot_cut,
            .offset = self.snapshot_offset,
            .next_offset = self.snapshot_offset,
            .changes_digest = boundary.capture_digest,
        };
        boundary.requireEvent(event, self.snapshot_next_page) catch return null;
        return event;
    }
};

pub const ReplicationTarget = struct {
    image: local.api_replication_source_apply.Image,
    marker: ?[32]u8,
};

pub fn replicationTargetDigest(group: u64, incarnation: [32]u8, owner: @FieldType(ReplicationReference, "owner"), birth: u64, namespace: @FieldType(ReplicationPermit, "namespace")) ![32]u8 {
    return @import("../metadata/replication_digest.zig").digest("antfly/subscription-target/v1\x00", .{
        .group = group,
        .incarnation = incarnation,
        .owner = owner,
        .birth = birth,
        .namespace = namespace,
    }, max_bytes);
}

fn replicationTargetKey(a: A, group: u64, target_digest: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{x}", .{ try prefix(a, group, "replication_target"), &target_digest });
}

pub fn readReplicationTarget(a: A, txn: anytype, group: u64, target_digest: [32]u8) !?ReplicationTarget {
    const raw = (try optional(txn, try replicationTargetKey(a, group, target_digest))) orelse return null;
    if (raw.len > 2048) return error.InvalidCatalogRecord;
    const target = try std.json.parseFromSliceLeaky(ReplicationTarget, a, raw, .{});
    try target.image.validate();
    if (!std.mem.eql(u8, &target.image.target_custody_digest, &target_digest)) return error.InvalidCatalogRecord;
    if (target.marker) |marker| if (std.mem.allEqual(u8, &marker, 0)) return error.InvalidCatalogRecord;
    return target;
}

pub fn readReplicationTargets(a: A, txn: anytype, group: u64, incarnation: [32]u8, owner: @FieldType(ReplicationReference, "owner"), birth: u64, namespaces: []const @FieldType(ReplicationPermit, "namespace")) ![]const ?ReplicationTarget {
    if (namespaces.len > max_participants) return error.InvalidReplicationCutoverIntent;
    const targets = try a.alloc(?ReplicationTarget, namespaces.len);
    for (targets, namespaces) |*target, namespace| {
        if (namespace.table_id == 0 or namespace.shard_id == 0 or namespace.range_id == 0)
            return error.InvalidReplicationCutoverIntent;
        target.* = try readReplicationTarget(a, txn, group, try replicationTargetDigest(group, incarnation, owner, birth, namespace));
    }
    return targets;
}

pub const ReplicationIntents = struct {
    upload_token: u64,
    members: []const @import("replication/payload.zig").Retained,

    pub fn require(self: ReplicationIntents, identity: Identity, group: u64, count: usize) !void {
        const source = identity.replication_source orelse return error.InvalidCatalogRecord;
        if (count == 0 or self.members.len != count) return error.InvalidCatalogRecord;
        const scope = try @import("replication/payload.zig").uploadScope(source, group, identity.metadata_incarnation, self.upload_token, source.txn_id);
        for (self.members) |member| {
            try member.validate();
            if (!std.meta.eql(scope, member.scope)) return error.InvalidReplicationPayloadAdmission;
        }
        if (!std.mem.eql(u8, &identity.plan_digest, &try self.digest())) return error.InvalidReplicationPayloadAdmission;
    }

    pub fn digest(self: ReplicationIntents) ![32]u8 {
        return @import("../metadata/replication_digest.zig").digest("antfly/subscription-intent-manifest/v1\x00", self, max_bytes);
    }
};

pub fn replicationIntentDigest(bodies: []const [32]u8) [32]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("antfly/subscription-intent-bodies/v1\x00");
    hasher.update(std.mem.sliceAsBytes(bodies));
    var result: [32]u8 = undefined;
    hasher.final(&result);
    return result;
}

pub const ReplicationDestination = struct {
    policy_epoch: local.api_replication_source_apply.PolicyEpoch = .{},
    table_id: u64,
    storage_name: []const u8,
    schema_version: u32,
    schema_digest: [32]u8,
};

pub const ReplicationWalAccess = struct {
    table_id: u64,
    catalog_revision: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    policy_epoch: local.api_replication_source_apply.PolicyEpoch,
    rights: u16,
};

pub const ReplicationRecovery = struct {
    progress: ReplicationProgress,
    transaction: ?Record = null,
    targets: []const ?ReplicationTarget = &.{},
    destination: ?ReplicationDestination = null,
    destination_credential: ?struct {
        actor: local.system_catalog_logical_replication_catalog.ExecutionActor,
        resource: []const u8,
    } = null,
};

/// Rejoin the retained decision from the same metadata read cut as its cursor.
pub fn readReplicationRecovery(a: A, txn: anytype, group: u64, owner: @FieldType(ReplicationReference, "owner"), birth: u64, origin: [32]u8) !?ReplicationRecovery {
    const progress = (try readReplicationProgress(a, txn, group, owner)) orelse return null;
    if (progress.birth != birth or !std.mem.eql(u8, &progress.origin, &origin)) return error.ReplicationCutoverAuthorityLost;
    const pending = progress.pending orelse return .{ .progress = progress };
    const record = (try readOperation(a, txn, group, pending.txn_id)) orelse return error.InvalidCatalogRecord;
    if (record.identity.kind != .replication or !record.identity.replication_source.?.eql(pending))
        return error.InvalidCatalogRecord;
    return .{ .progress = progress, .transaction = record };
}

fn replicationProgressKey(a: A, group: u64, owner: @FieldType(ReplicationReference, "owner")) ![]u8 {
    var buffer: [96]u8 = undefined;
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "replication_progress"), try owner.keyPart(&buffer) });
}

fn replicationPendingKey(a: A, group: u64, transaction: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "replication_pending"), &std.fmt.bytesToHex(transaction, .lower) });
}

pub fn nextReplicationRecovery(a: A, txn: anytype, group: u64, after: ?[16]u8) !?ReplicationReference {
    const family = try prefix(a, group, "replication_pending");
    const seek = if (after) |id| try replicationPendingKey(a, group, id) else family;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = (try cursor.seekAtOrAfter(seek)) orelse return null;
    if (after != null and std.mem.eql(u8, row.key, seek))
        row = (try cursor.next()) orelse return null;
    if (!std.mem.startsWith(u8, row.key, family)) return null;
    if (row.value.len > 8192) return error.InvalidCatalogRecord;
    const reference = try std.json.parseFromSliceLeaky(ReplicationReference, a, row.value, .{});
    try reference.validate();
    if (reference.owner != .logical_subscription or
        !std.mem.eql(u8, row.key, try replicationPendingKey(a, group, reference.txn_id))) return error.InvalidCatalogRecord;
    return reference;
}

pub fn readReplicationProgress(a: A, txn: anytype, group: u64, owner: @FieldType(ReplicationReference, "owner")) !?ReplicationProgress {
    const bytes = (try optional(txn, try replicationProgressKey(a, group, owner))) orelse return null;
    if (bytes.len > 16 * 1024) return error.InvalidCatalogRecord;
    const progress = try std.json.parseFromSliceLeaky(ReplicationProgress, a, bytes, .{});
    if (progress.birth == 0 or std.mem.allEqual(u8, &progress.origin, 0) or
        (progress.last_acknowledged == null) != (progress.ack_roster_digest == null)) return error.InvalidCatalogRecord;
    if (progress.snapshot_boundary) |boundary| {
        if (boundary.custody) |custody| try boundary.validateCustody(custody, group, owner, progress);
    }
    if (progress.pending) |reference| {
        try reference.validate();
        if (!reference.owner.eql(owner) or reference.birth != progress.birth or reference.generation != progress.generation or
            !std.mem.eql(u8, &reference.source_digest, &progress.origin)) return error.InvalidCatalogRecord;
    }
    if (progress.last_acknowledged) |reference| {
        try reference.validate();
        if (!reference.owner.eql(owner) or reference.birth != progress.birth or reference.generation > progress.generation or
            !std.mem.eql(u8, &reference.source_digest, &progress.origin)) return error.InvalidCatalogRecord;
    }
    return progress;
}

pub fn allowReplicationCheckpoint(a: A, txn: anytype, group: u64, reference: ReplicationReference, checkpoint: u64) !bool {
    try reference.validate();
    if (checkpoint == 0) return false;
    const progress = (try readReplicationProgress(a, txn, group, reference.owner)) orelse return false;
    return progress.checkpoint == checkpoint and progress.last_acknowledged != null and
        progress.last_acknowledged.?.eql(reference) and progress.ack_roster_digest != null;
}

pub const Record = struct {
    catalog_outcome: CatalogOutcome = .pending,
    identity: Identity,
    revision: u64 = 1,
    phase: Phase = .preparing,
    was_prepared: bool = false,
    participants: []const Participant,
    replication_event: ?ReplicationEvent = null,
    replication_intents: ?ReplicationIntents = null,
    replication_permits: []const ReplicationPermit = &.{},
    replication_ack: ?ReplicationAckGrant = null,
    votes: u64 = 0,
    native_lock_releases: u64 = 0,
    // Exact predecessor members drained under their generation handoff seal.
    catalog_retired: u64 = 0,
    activations: u64 = 0,
    resolutions: u64 = 0,
    visibility: u64 = 0,
    visibility_repairs: u64 = 0,
    locks_released: bool = false,
    decision: ?Decision = null,
    history_certificate: ?history.Digest = null,
    history_released: u64 = 0,
    /// Irreversible image retirement is independent of the global decision.
    /// These metadata-verified facts never assert a native row/intent ACK.
    native_retirements: []const NativeRetirement = &.{},
    publication_capacities: []const CaptureCapacity = &.{},
    publication_captures: []const CaptureRef = &.{},
    publication_terminals: []const CaptureTerminal = &.{},
    publication_abort_fences: []const local.system_catalog_logical_replication_capacity.AbortAbsence = &.{},

    pub fn nativeJsonSkipField(self: Record, comptime field: []const u8) bool {
        return (std.mem.eql(u8, field, "catalog_outcome") and self.identity.catalog_participant == null) or
            (std.mem.eql(u8, field, "catalog_retired") and self.catalog_retired == 0) or
            (std.mem.eql(u8, field, "history_certificate") and self.history_certificate == null) or
            (std.mem.eql(u8, field, "history_released") and self.history_released == 0) or
            (std.mem.eql(u8, field, "replication_event") and self.replication_event == null) or
            (std.mem.eql(u8, field, "replication_intents") and self.replication_intents == null) or
            (std.mem.eql(u8, field, "replication_ack") and self.replication_ack == null) or
            (std.mem.eql(u8, field, "replication_permits") and self.replication_permits.len == 0) or
            (std.mem.eql(u8, field, "native_retirements") and self.native_retirements.len == 0) or
            (std.mem.eql(u8, field, "publication_capacities") and self.publication_capacities.len == 0) or
            (std.mem.eql(u8, field, "publication_captures") and self.publication_captures.len == 0) or
            (std.mem.eql(u8, field, "publication_terminals") and self.publication_terminals.len == 0) or
            (std.mem.eql(u8, field, "publication_abort_fences") and self.publication_abort_fences.len == 0);
    }
    pub fn jsonStringify(self: Record, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Record).@"struct".field_names) |field| {
            if (!self.nativeJsonSkipField(field)) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    /// Join a Source permit to this exact native decision member. The caller
    /// separately verifies the signed store root and retained source custody.
    pub fn requireReplicationPermit(self: Record, a: A, group: u64, permit: local.api_replication_source_apply.Permit) !void {
        try permit.validate();
        const source = self.identity.replication_source orelse return error.ReplicationCutoverAuthorityLost;
        if (self.identity.kind != .replication or !source.eql(permit.reference) or
            group != permit.semantic.metadata_group or !(try self.identity.owner(a)).eql(permit.decision_owner) or
            permit.member >= self.participants.len) return error.ReplicationCutoverAuthorityLost;
        const participant = self.participants[permit.member];
        const route = participant.route;
        if (route.metadata_group_id != group or route.route.group_id != permit.owner_group_id or
            route.topology_epoch != permit.topology_epoch or !permit.namespace.eql(.{ .table_id = route.route.identity_namespace.table_id, .shard_id = route.route.identity_namespace.shard_id, .range_id = route.route.identity_namespace.range_id }) or
            participant.schema_version == null or participant.schema_version.? != permit.schema_version)
            return error.ReplicationCutoverAuthorityLost;
        if (participant.publication_capture) |publication| {
            if (!std.mem.eql(u8, &publication.pregrant_body_digest, &permit.pregrant_body_digest) or
                !std.mem.eql(u8, &publication.request_digest, &permit.request_digest)) return error.ReplicationCutoverAuthorityLost;
        }
    }

    pub fn requireRetainedReplicationPermit(self: Record, a: A, group: u64, permit: ReplicationPermit) !void {
        try self.requireReplicationPermit(a, group, permit);
        if (permit.member >= self.replication_permits.len or !std.meta.eql(self.replication_permits[permit.member], permit))
            return error.ReplicationCutoverAuthorityLost;
    }

    pub fn requireReplicationIntent(self: Record, a: A, group: u64, permit: local.api_replication_source_apply.Permit, request: local.storage_db_types.TransactionIntentRequest) !void {
        try self.requireRetainedReplicationPermit(a, group, permit);
        if ((permit.sql_rights != 0 or request.relational_integrity_generation_set != null) and try local.api_replication_source_apply.intentRights(request) != permit.sql_rights)
            return error.ReplicationCutoverAuthorityLost;
        const owner_value = request.external_decision_owner orelse return error.ReplicationCutoverAuthorityLost;
        if (!owner_value.eql(permit.decision_owner) or request.schema_version != permit.schema_version)
            return error.ReplicationCutoverAuthorityLost;
        const digests = try local.storage_db_logical_replication_request.digests(a, .{
            .namespace = permit.namespace,
            .owner_group_id = permit.owner_group_id,
            .transaction_id = permit.reference.txn_id,
            .member = permit.member,
            .topology_epoch = permit.topology_epoch,
        }, request);
        if (!std.mem.eql(u8, &digests.body, &permit.pregrant_body_digest) or !std.mem.eql(u8, &digests.request, &permit.request_digest))
            return error.ReplicationCutoverAuthorityLost;
    }

    pub fn nativeRetirementMask(self: Record) u64 {
        var mask_value: u64 = 0;
        for (self.native_retirements) |retired| mask_value |= @as(u64, 1) << @intCast(retired.member);
        return mask_value;
    }

    pub fn mask(self: Record) u64 {
        return if (self.participants.len == 64) std.math.maxInt(u64) else (@as(u64, 1) << @as(u6, @intCast(self.participants.len))) - 1;
    }

    pub fn hasPreparedLocks(self: Record) bool {
        return self.identity.kind == .named and self.identity.lock_scope != null;
    }
    pub fn visibilityMask(self: Record) u64 {
        return if (self.identity.kind == .session_commit and self.decision != null and self.decision.?.status == .committed and self.decision.?.sync_level != .write) self.mask() & ~self.catalog_retired else 0;
    }
    pub fn nativeLockMask(self: Record) u64 {
        var result: u64 = 0;
        for (self.participants, 0..) |part, index| if (part.sql_locks != null) {
            result |= @as(u64, 1) << @intCast(index);
        };
        return result;
    }
    pub fn pendingCatalogCompletion(self: Record) ?CatalogOutcome {
        if (self.identity.catalog_participant == null or self.catalog_outcome != .pending) return null;
        const decision = self.decision orelse return null;
        return switch (decision.status) {
            .committed => if (self.phase == .decided and self.activations == self.mask()) .published else null,
            .aborted => if (self.phase == .resolved) .canceled else null,
        };
    }

    pub fn commitProgress(self: Record) CommitProgress {
        if (self.phase == .decided or self.phase == .active or self.native_lock_releases != self.nativeLockMask() or
            (self.identity.catalog_participant != null and self.phase != .retired) or
            (self.identity.history_completion != null and (self.history_certificate == null or self.history_released != self.mask()))) return .resolving;
        if (self.visibility != self.visibilityMask()) return .indexing;
        return if (self.visibility_repairs != 0) .repair_required else .ready;
    }
    pub fn requirePublicationCapacity(self: Record, a: A, value: CaptureCapacity) !void {
        try value.validate();
        if (value.member >= self.participants.len or !value.owner.eql(try self.identity.owner(a)) or
            !std.mem.eql(u8, &value.transaction_id, &self.identity.txn_id)) return error.ExternalTransactionDecisionChanged;
        const participant = self.participants[value.member];
        const route = participant.route;
        const reference = participant.publication_capture orelse return error.ExternalTransactionDecisionChanged;
        const namespace = route.route.identity_namespace;
        if (!value.reference.eql(reference) or reference.realm.database_id != self.identity.database_id or
            route.route.execution != .raft or value.owner_group_id != route.route.group_id or value.topology_epoch != route.topology_epoch or
            value.namespace.table_id != namespace.table_id or value.namespace.range_id != namespace.range_id or value.namespace.shard_id != namespace.shard_id)
            return error.ExternalTransactionDecisionChanged;
    }

    pub fn publicationCapacity(self: Record, member: usize) ?CaptureCapacity {
        for (self.publication_capacities) |capacity| {
            if (capacity.member == member) return capacity;
        }
        return null;
    }

    pub fn requirePublicationCapture(self: Record, a: A, value: CaptureRef) !void {
        const capacity = self.publicationCapacity(value.participant) orelse return error.CatalogPublicationProofPending;
        try capacity.requireCapture(a, value);
    }

    pub fn requirePublicationTerminal(self: Record, a: A, terminal: CaptureTerminal) !void {
        try terminal.validate();
        try self.requirePublicationCapture(a, terminal.capture_ref);
        const decision = self.decision orelse return error.CatalogPublicationProofPending;
        if ((decision.status == .committed) != (terminal.status == .committed) or decision.version != terminal.commit_version)
            return error.ExternalTransactionDecisionChanged;
        for (self.publication_captures) |accepted| {
            if (accepted.participant != terminal.capture_ref.participant) continue;
            if (!accepted.sameLogical(terminal.capture_ref)) return error.ExternalTransactionDecisionChanged;
            return;
        }
        if (decision.status == .committed) return error.CatalogPublicationProofPending;
    }

    pub fn requirePublicationAbortFence(self: Record, a: A, fence: local.system_catalog_logical_replication_capacity.AbortAbsence) !void {
        if (self.decision == null or self.decision.?.status != .aborted) return error.CatalogPublicationProofPending;
        const grant = self.publicationCapacity(fence.member) orelse return error.CatalogPublicationProofPending;
        try fence.require(a, grant);
        if (self.votes & (@as(u64, 1) << @intCast(fence.member)) != 0) return error.ExternalTransactionDecisionChanged;
        for (self.publication_terminals) |terminal| {
            if (terminal.capture_ref.participant == fence.member) return error.ExternalTransactionDecisionChanged;
        }
    }

    pub fn publicationCaptureMask(self: Record) u64 {
        var result: u64 = 0;
        for (self.participants, 0..) |participant, index| if (participant.publication_capture != null) {
            result |= @as(u64, 1) << @intCast(index);
        };
        return result;
    }

    pub fn validate(self: Record, a: A) !void {
        try self.identity.validate();
        try self.identity.validateHistoryCohort(self.participants);
        if (self.history_released & ~self.mask() != 0 or (self.history_released != 0 and self.history_certificate == null))
            return error.InvalidCatalogRecord;
        if (self.history_certificate) |certificate| {
            if (self.identity.history_completion == null or std.mem.allEqual(u8, &certificate, 0) or
                (self.phase != .resolved and self.phase != .retired) or self.decision == null or self.decision.?.status != .committed)
                return error.InvalidCatalogRecord;
        }
        if (self.identity.kind == .replication) {
            const event = self.replication_event orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &try event.eventDigest(), &self.identity.replication_source.?.event_digest)) return error.InvalidCatalogRecord;
            if (self.replication_permits.len != self.participants.len or (self.replication_intents == null) != (self.participants.len == 0)) return error.InvalidCatalogRecord;
            if (self.replication_intents) |intents| try intents.require(self.identity, self.participants[0].route.metadata_group_id, self.participants.len);
            for (self.replication_permits, 0..) |permit, index| if (permit.member != index) return error.InvalidCatalogRecord;
            if (self.replication_ack) |grant| {
                if (self.participants.len == 0 or (self.phase != .resolved and self.phase != .retired) or
                    std.mem.allEqual(u8, &grant.participant_ids_digest, 0) or std.mem.allEqual(u8, &grant.terminal_receipts_digest, 0))
                    return error.InvalidCatalogRecord;
            }
        } else if (self.replication_event != null or self.replication_intents != null or self.replication_permits.len != 0 or self.replication_ack != null) return error.InvalidCatalogRecord;
        for (self.participants) |part| if (part.sql_locks) |admission| {
            const scope = self.identity.lock_scope orelse return error.InvalidCatalogRecord;
            if (!scope.backend.eql(admission.source.lease.backend) or !std.meta.eql(scope.command, admission.parent_command) or
                !std.mem.eql(u8, &self.identity.txn_id, &admission.transaction)) return error.InvalidCatalogRecord;
        };
        if (self.revision == 0 or !std.mem.eql(u8, &try manifestDigest(a, self.participants, self.identity.metadata_incarnation), &self.identity.manifest_digest)) return error.InvalidCatalogRecord;
        const all = self.mask();
        if (self.publication_capacities.len > self.participants.len) return error.InvalidCatalogRecord;
        var previous_capacity: ?u16 = null;
        for (self.publication_capacities) |capacity| {
            if (previous_capacity) |previous| if (capacity.member <= previous) return error.InvalidCatalogRecord;
            try self.requirePublicationCapacity(a, capacity);
            previous_capacity = capacity.member;
        }
        if (self.publication_captures.len > self.publication_capacities.len) return error.InvalidCatalogRecord;
        var captured: u64 = 0;
        for (self.publication_captures) |value| {
            try self.requirePublicationCapture(a, value);
            const bit = @as(u64, 1) << @intCast(value.participant);
            if (captured & bit != 0) return error.InvalidCatalogRecord;
            captured |= bit;
        }
        if (self.votes & self.publicationCaptureMask() != captured) return error.InvalidCatalogRecord;
        if (self.publication_terminals.len > self.publication_capacities.len) return error.InvalidCatalogRecord;
        var previous_terminal: ?u16 = null;
        for (self.publication_terminals) |terminal| {
            const member = terminal.capture_ref.participant;
            if (previous_terminal) |previous| if (member <= previous) return error.InvalidCatalogRecord;
            try self.requirePublicationTerminal(a, terminal);
            previous_terminal = member;
        }
        if (self.publication_abort_fences.len + self.publication_terminals.len > self.publication_capacities.len)
            return error.InvalidCatalogRecord;
        var fenced: u64 = 0;
        for (self.publication_abort_fences) |fence| {
            try self.requirePublicationAbortFence(a, fence);
            const bit = @as(u64, 1) << @intCast(fence.member);
            if (fenced & bit != 0) return error.InvalidCatalogRecord;
            fenced |= bit;
        }
        if (self.native_retirements.len > self.participants.len) return error.InvalidCatalogRecord;
        var retired_members: u64 = 0;
        for (self.native_retirements) |retired| {
            if (retired.member >= self.participants.len or std.mem.allEqual(u8, &retired.digest, 0)) return error.InvalidCatalogRecord;
            const part = self.participants[retired.member];
            if (part.route.route.execution != .native_local or part.route.native_participant == null) return error.InvalidCatalogRecord;
            const bit = @as(u64, 1) << @intCast(retired.member);
            if (retired_members & bit != 0) return error.InvalidCatalogRecord;
            retired_members |= bit;
        }
        if (self.catalog_retired != 0 and (self.identity.catalog_participant == null or self.catalog_outcome != .published or
            self.catalog_retired & ~self.nativeLockMask() != 0 or self.catalog_retired & ~self.resolutions != 0 or
            self.catalog_retired & ~self.native_lock_releases != 0)) return error.InvalidCatalogRecord;
        if (self.native_lock_releases & ~self.nativeLockMask() != 0 or
            (self.native_lock_releases & ~self.catalog_retired != 0 and self.phase != .resolved and self.phase != .retired) or
            (self.phase == .retired and self.native_lock_releases != self.nativeLockMask())) return error.InvalidCatalogRecord;
        if ((self.votes | self.activations | self.resolutions) & ~all != 0) return error.InvalidCatalogRecord;
        if ((self.visibility | self.visibility_repairs) & ~self.visibilityMask() != 0 or self.visibility_repairs & ~self.visibility != 0) return error.InvalidCatalogRecord;
        if (self.phase != .resolved and self.phase != .retired and self.visibility != 0) return error.InvalidCatalogRecord;
        if (self.phase == .retired and self.visibility != self.visibilityMask()) return error.InvalidCatalogRecord;
        if (self.locks_released and (!self.hasPreparedLocks() or (self.phase != .resolved and self.phase != .retired))) return error.InvalidCatalogRecord;
        if (self.phase == .retired and self.hasPreparedLocks() and !self.locks_released) return error.InvalidCatalogRecord;
        if (self.identity.catalog_participant == null) {
            if (self.catalog_outcome != .pending) return error.InvalidCatalogRecord;
        } else if (self.decision) |decision| {
            if (decision.status == .committed) {
                const expected: CatalogOutcome = if (self.phase == .decided) .pending else .published;
                if (self.catalog_outcome != expected) return error.InvalidCatalogRecord;
            } else if (self.catalog_outcome == .published or
                (self.catalog_outcome == .canceled and self.phase != .resolved and self.phase != .retired) or
                (self.phase == .retired and self.catalog_outcome != .canceled)) return error.InvalidCatalogRecord;
        } else if (self.catalog_outcome != .pending) return error.InvalidCatalogRecord;
        switch (self.phase) {
            .preparing, .locking, .prepared => {
                if (self.decision != null or self.activations != 0 or self.resolutions != 0) return error.InvalidCatalogRecord;
                if (self.phase == .prepared and (!self.was_prepared or self.votes != all)) return error.InvalidCatalogRecord;
                if (self.phase != .prepared and self.was_prepared) return error.InvalidCatalogRecord;
                if (self.phase == .locking and (!self.hasPreparedLocks() or self.votes != all)) return error.InvalidCatalogRecord;
            },
            .decided, .active, .resolved, .retired => {
                const decision = self.decision orelse return error.InvalidCatalogRecord;
                if (decision.version <= self.identity.begin_timestamp) return error.InvalidCatalogRecord;
                if (decision.status == .committed and (!self.was_prepared or self.votes != all)) return error.InvalidCatalogRecord;
                if (self.identity.kind == .session_commit and ((decision.status == .aborted and self.was_prepared) or (decision.status == .committed and decision.sync_level != self.identity.commit_sync_level))) return error.InvalidCatalogRecord;
                if (decision.status == .aborted and (self.activations != 0 or self.phase == .active)) return error.InvalidCatalogRecord;
                if (decision.status == .committed and self.phase != .decided and self.activations != all) return error.InvalidCatalogRecord;
                if ((self.phase == .resolved or self.phase == .retired) and self.resolutions != all) return error.InvalidCatalogRecord;
                if (self.phase == .decided) {
                    const awaiting_catalog = self.identity.catalog_participant != null and decision.status == .committed;
                    if (awaiting_catalog) {
                        if (self.resolutions != 0) return error.InvalidCatalogRecord;
                    } else if (all == 0 or self.resolutions == all or (decision.status == .committed and (self.resolutions != 0 or self.activations == all))) return error.InvalidCatalogRecord;
                }
                if (self.phase == .active and self.resolutions == all) return error.InvalidCatalogRecord;
            },
        }
    }
};

/// The metadata apply lane receipts the whole command before returning. These
/// transitions never contact native owners and never sample a process clock.
pub const ReplicationAdvance = union(enum) {
    vote: struct {
        receipt: @import("../api/replication_source_apply_attestation.zig").Receipt,
        publication: ?CaptureProofs.OutboxReceipt = null,
    },
    publication_capacity: CapacityReceipt,
    publication_terminal: CaptureProofs.TerminalReceipt,
    publication_abort_fence: CaptureProofs.AbortFenceReceipt,
    prepare,
    commit: DecisionInput,
    activate: @import("../api/replication_source_apply_attestation.zig").Receipt,
    abort: u64,
    resolve: @import("../api/replication_source_apply_attestation.zig").Receipt,
};

fn requireReplicationStage(a: A, group: u64, record: Record, receipt: @import("../api/replication_source_apply_attestation.zig").Receipt, stage: local.api_replication_source_apply.ApplyReceipt.Stage, verifier: anytype) !u64 {
    const permit = receipt.value.permit;
    try record.requireRetainedReplicationPermit(a, group, permit);
    try receipt.require(permit, stage, a);
    try verifier.requireTerminalRoot(receipt.ownerScope(), receipt.proof, receipt.value.position);
    return @as(u64, 1) << @intCast(permit.member);
}

fn verifiedReplicationAction(a: A, group: u64, record: Record, operation: ReplicationAdvance, verifier: anytype) !Action {
    const action: Action = switch (operation) {
        .vote => |vote| blk: {
            if (record.phase != .preparing) return error.InvalidPreparedTransactionPhase;
            const receipt = vote.receipt;
            const permit = receipt.value.permit;
            const member = try requireReplicationStage(a, group, record, receipt, .prepared, verifier);
            if (vote.publication) |capture| {
                if (capture.reference.participant != permit.member) return error.ExternalTransactionDecisionChanged;
                break :blk .{ .publication_vote = capture };
            }
            break :blk .{ .vote = member };
        },
        .publication_capacity => |receipt| .{ .publication_capacity = receipt },
        .publication_terminal => |receipt| .{ .publication_terminal = receipt },
        .publication_abort_fence => |receipt| .{ .publication_abort_fence = receipt },
        .prepare => .prepare,
        .commit => |decision| .{ .commit = decision },
        .abort => |version| .{ .abort = version },
        .activate => |receipt| .{ .activate = try requireReplicationStage(a, group, record, receipt, .activated, verifier) },
        .resolve => |receipt| blk: {
            const decision = record.decision orelse return error.InvalidPreparedTransactionPhase;
            const stage: local.api_replication_source_apply.ApplyReceipt.Stage = if (decision.status == .committed) .applied else .aborted;
            break :blk .{ .resolve = try requireReplicationStage(a, group, record, receipt, stage, verifier) };
        },
    };
    switch (action) {
        .publication_capacity, .publication_vote, .publication_terminal, .publication_abort_fence => try verifier.requirePublicationAction(a, record, action),
        else => {},
    }
    return action;
}

pub const Action = union(enum) {
    publication_capacity: CapacityReceipt,
    publication_vote: CaptureProofs.OutboxReceipt,
    publication_terminal: CaptureProofs.TerminalReceipt,
    publication_abort_fence: CaptureProofs.AbortFenceReceipt,
    vote: u64,
    prepare,
    lock_handoff,
    lock_release,
    lock_released,
    native_locks_released: u64,
    native_retired: []const NativeRetirement,
    commit: DecisionInput,
    abort: u64,
    activate: u64,
    catalog_complete: CatalogOutcome,
    resolve: u64,
    visibility: struct { complete: u64, repair: u64 = 0 },
    history_published: history.Digest,
    history_released: u64,
    handoff,
};

pub fn advance(a: A, record: Record, expected_revision: u64, owner: Owner, action: Action) !Record {
    try record.validate(a);
    if (!(try record.identity.owner(a)).eql(owner)) return error.ExternalTransactionDecisionChanged;
    if (record.revision != expected_revision) return error.CatalogGenerationChanged;
    var next = record;
    next.revision = if (action == .lock_handoff or action == .lock_release) record.revision else std.math.add(u64, record.revision, 1) catch return error.CatalogRevisionExhausted;
    const all = record.mask();
    switch (action) {
        .publication_capacity => |receipt| {
            if (record.phase != .preparing) return error.InvalidPreparedTransactionPhase;
            try receipt.validate(a);
            const value = receipt.value;
            try record.requirePublicationCapacity(a, value);
            var index: usize = 0;
            while (index < record.publication_capacities.len and record.publication_capacities[index].member < value.member) : (index += 1) {}
            if (index < record.publication_capacities.len and record.publication_capacities[index].member == value.member) {
                if (!std.meta.eql(record.publication_capacities[index], value)) return error.ExternalTransactionDecisionChanged;
                return record;
            }
            const capacities = try a.alloc(CaptureCapacity, record.publication_capacities.len + 1);
            @memcpy(capacities[0..index], record.publication_capacities[0..index]);
            capacities[index] = value;
            @memcpy(capacities[index + 1 ..], record.publication_capacities[index..]);
            next.publication_capacities = capacities;
        },
        .publication_vote => |receipt| {
            if (record.phase != .preparing) return error.InvalidPreparedTransactionPhase;
            try receipt.validate(a);
            const value = receipt.reference;
            try record.requirePublicationCapture(a, value);
            for (record.publication_captures) |previous| if (previous.participant == value.participant) {
                if (!previous.sameLogical(value)) return error.ExternalTransactionDecisionChanged;
                return record;
            };
            const captured = try a.alloc(CaptureRef, record.publication_captures.len + 1);
            @memcpy(captured[0..record.publication_captures.len], record.publication_captures);
            captured[captured.len - 1] = value;
            next.publication_captures = captured;
            next.votes |= @as(u64, 1) << @intCast(value.participant);
        },
        .publication_terminal => |receipt| {
            try receipt.validate(a);
            const terminal = receipt.value;
            try record.requirePublicationTerminal(a, terminal);
            const member = terminal.capture_ref.participant;
            var index: usize = 0;
            while (index < record.publication_terminals.len and record.publication_terminals[index].capture_ref.participant < member) : (index += 1) {}
            if (index < record.publication_terminals.len and record.publication_terminals[index].capture_ref.participant == member) {
                var previous = record.publication_terminals[index];
                if (!previous.capture_ref.sameLogical(terminal.capture_ref)) return error.ExternalTransactionDecisionChanged;
                previous.capture_ref = terminal.capture_ref;
                if (!std.meta.eql(previous, terminal)) return error.ExternalTransactionDecisionChanged;
                return record;
            }
            const terminals = try a.alloc(CaptureTerminal, record.publication_terminals.len + 1);
            @memcpy(terminals[0..index], record.publication_terminals[0..index]);
            terminals[index] = terminal;
            @memcpy(terminals[index + 1 ..], record.publication_terminals[index..]);
            next.publication_terminals = terminals;
        },
        .publication_abort_fence => |receipt| {
            try receipt.validate(a);
            try record.requirePublicationAbortFence(a, receipt.value);
            for (record.publication_abort_fences) |stored| {
                if (stored.member == receipt.value.member) return record;
            }
            const fences = try a.alloc(local.system_catalog_logical_replication_capacity.AbortAbsence, record.publication_abort_fences.len + 1);
            @memcpy(fences[0..record.publication_abort_fences.len], record.publication_abort_fences);
            fences[fences.len - 1] = receipt.value;
            next.publication_abort_fences = fences;
        },
        .vote => |members| {
            if (members & record.publicationCaptureMask() != 0) return error.CatalogPublicationProofPending;
            if (record.phase != .preparing or members == 0 or members & ~all != 0) return error.InvalidPreparedTransactionPhase;
            next.votes |= members;
        },
        .prepare => {
            if (record.phase != .preparing and record.phase != .locking) return error.InvalidPreparedTransactionPhase;
            // Capacity only reserves bytes; it cannot acknowledge a durable vote.
            if (record.publicationCaptureMask() != 0 and record.votes != all) return error.CatalogPublicationProofPending;
            next.votes = all;
            // The metadata home is a real lock participant, not a fabricated
            // table route. Its bounded native transfer must precede PREPARED.
            next.phase = if (record.hasPreparedLocks() and record.phase == .preparing) .locking else .prepared;
            next.was_prepared = next.phase == .prepared;
        },
        .lock_handoff => {
            if (record.phase != .locking) return error.InvalidPreparedTransactionPhase;
        },
        .lock_release, .lock_released => {
            if (record.phase != .resolved or !record.hasPreparedLocks() or record.locks_released) return error.InvalidPreparedTransactionPhase;
            if (action == .lock_released) next.locks_released = true;
        },
        .commit => |decision| {
            if (record.identity.notification_plan) |notifications| if (notifications.discard) return error.InvalidPreparedTransactionPhase;
            if (record.phase != .prepared or decision.version <= record.identity.begin_timestamp or (record.identity.kind == .session_commit and decision.sync_level != record.identity.commit_sync_level)) return error.InvalidPreparedTransactionPhase;
            next.decision = .{ .status = .committed, .version = decision.version, .sync_level = decision.sync_level };
            next.phase = if (all == 0 and record.identity.catalog_participant == null) .resolved else .decided;
        },
        .abort => |version| {
            if ((record.phase != .preparing and record.phase != .locking and record.phase != .prepared) or (record.identity.kind == .session_commit and record.phase == .prepared) or version <= record.identity.begin_timestamp) return error.InvalidPreparedTransactionPhase;
            next.decision = .{ .status = .aborted, .version = version };
            next.phase = if (all == 0) .resolved else .decided;
        },
        .native_retired => |claims| {
            if (claims.len == 0 or claims.len > record.participants.len or record.phase == .retired) return error.InvalidPreparedTransactionPhase;
            var merged: std.ArrayListUnmanaged(NativeRetirement) = .empty;
            errdefer merged.deinit(a);
            try merged.appendSlice(a, record.native_retirements);
            for (claims) |claim| {
                if (claim.member >= record.participants.len or record.participants[claim.member].route.route.execution != .native_local or
                    record.participants[claim.member].route.native_participant == null or std.mem.allEqual(u8, &claim.digest, 0)) return error.InvalidCatalogMutation;
                const existing: ?NativeRetirement = for (merged.items) |prior| {
                    if (prior.member == claim.member) break prior;
                } else null;
                if (existing) |prior| {
                    if (!std.mem.eql(u8, &prior.digest, &claim.digest)) return error.ExternalTransactionDecisionChanged;
                } else try merged.append(a, claim);
            }
            next.native_retirements = merged.items;
        },
        .activate => |members| {
            if ((record.phase != .decided and record.phase != .active) or record.decision.?.status != .committed or members == 0 or members & ~all != 0) return error.InvalidPreparedTransactionPhase;
            next.activations |= members;
            if (next.activations == all and record.identity.catalog_participant == null) next.phase = .active;
        },
        // The accepting writer must also apply the restore namespace delta.
        .catalog_complete => return error.CatalogPublicationProofPending,
        .resolve => |members| {
            if ((record.phase != .decided and record.phase != .active) or (record.decision.?.status == .committed and record.phase != .active) or members == 0 or members & ~all != 0) return error.InvalidPreparedTransactionPhase;
            next.resolutions |= members;
            if (next.resolutions == all) next.phase = .resolved;
        },
        .native_locks_released => |members| {
            if (record.phase != .resolved or members == 0 or members & ~record.nativeLockMask() != 0) return error.InvalidPreparedTransactionPhase;
            next.native_lock_releases |= members;
        },
        .visibility => |progress| {
            if (record.phase != .resolved or record.identity.kind != .session_commit or progress.complete == 0 or
                progress.complete & ~record.visibilityMask() != 0 or progress.repair & ~progress.complete != 0) return error.InvalidPreparedTransactionPhase;
            // An already attested successful barrier cannot become an index
            // failure belonging to some later mutation of the same table.
            if (progress.repair & record.visibility & ~record.visibility_repairs != 0) return error.ExternalTransactionDecisionChanged;
            next.visibility |= progress.complete;
            next.visibility_repairs |= progress.repair;
        },
        .history_published => |digest_value| {
            if (record.identity.history_completion == null or record.phase != .resolved or record.decision.?.status != .committed or std.mem.allEqual(u8, &digest_value, 0))
                return error.InvalidPreparedTransactionPhase;
            if (record.history_certificate) |prior| if (!std.mem.eql(u8, &prior, &digest_value))
                return error.ExternalTransactionDecisionChanged;
            next.history_certificate = digest_value;
        },
        .history_released => |members| {
            if (record.phase != .resolved or record.history_certificate == null or members == 0 or members & ~all != 0)
                return error.InvalidPreparedTransactionPhase;
            next.history_released |= members;
        },
        .handoff => {
            if (record.identity.history_completion != null and record.decision != null and record.decision.?.status == .committed and
                (record.history_certificate == null or record.history_released != all)) return error.CatalogPublicationProofPending;
            if (record.identity.catalog_participant != null and record.catalog_outcome == .pending) return error.InvalidPreparedTransactionPhase;
            if (record.publicationCaptureMask() != 0) {
                const decision = record.decision orelse return error.CatalogPublicationProofPending;
                const expected = if (decision.status == .committed) record.publication_captures.len else record.publication_capacities.len;
                if (expected != record.publication_terminals.len + record.publication_abort_fences.len)
                    return error.CatalogPublicationProofPending;
            }
            if (record.phase != .resolved or record.visibility != record.visibilityMask() or record.native_lock_releases != record.nativeLockMask() or (record.hasPreparedLocks() and !record.locks_released)) return error.InvalidPreparedTransactionPhase;
            next.phase = .retired;
        },
    }
    try next.validate(a);
    return next;
}

/// Only the native catalog writer may call this after verifying the exact
/// staged owner receipts in the transaction that publishes or cancels them.
pub fn completeCatalog(a: A, group: u64, record: Record, expected_revision: u64, owner: Owner, binding: CatalogBinding, participant: CatalogParticipant, outcome: CatalogOutcome) !Record {
    return completeCatalogWithRetirement(a, group, record, expected_revision, owner, binding, participant, outcome, 0);
}

fn completeCatalogWithRetirement(a: A, group: u64, record: Record, expected_revision: u64, owner: Owner, binding: CatalogBinding, participant: CatalogParticipant, outcome: CatalogOutcome, retired: u64) !Record {
    try record.validate(a);
    try binding.requireIdentity(a, group, record.identity, participant);
    if (!(try record.identity.owner(a)).eql(owner)) return error.ExternalTransactionDecisionChanged;
    if (record.revision != expected_revision) return error.CatalogGenerationChanged;
    if (record.catalog_outcome == outcome and outcome != .pending) return record;
    if (record.catalog_outcome != .pending) return error.InvalidPreparedTransactionPhase;
    const decision = record.decision orelse return error.InvalidPreparedTransactionPhase;
    var next = record;
    switch (outcome) {
        .pending => return error.InvalidPreparedTransactionPhase,
        .published => {
            if (decision.status != .committed or record.phase != .decided or record.activations != record.mask()) return error.InvalidPreparedTransactionPhase;
            next.catalog_retired = retired;
            next.resolutions |= retired;
            next.native_lock_releases |= retired;
            next.phase = if (next.resolutions == record.mask()) .resolved else .active;
        },
        .canceled => if (decision.status != .aborted or record.phase != .resolved) return error.InvalidPreparedTransactionPhase,
    }
    next.catalog_outcome = outcome;
    next.revision = std.math.add(u64, record.revision, 1) catch return error.CatalogRevisionExhausted;
    try next.validate(a);
    return next;
}

pub const ObservedCatalog = struct {
    job: @import("../metadata/restore_staging.zig").Job,
    progress: @import("../metadata/restore_staging.zig").Progress,
    binding: CatalogBinding,
};

pub fn observeCatalog(a: A, txn: anytype, group: u64, identity: Identity) !ObservedCatalog {
    const participant = identity.catalog_participant orelse return error.InvalidCatalogMutation;
    try participant.validate();
    const staging = @import("../metadata/restore_staging.zig");
    var job_key: [128]u8 = undefined;
    const encoded_job = (try optional(txn, try staging.jobKey(&job_key, group, participant.staging_id))) orelse return error.CatalogPublicationProofPending;
    if (encoded_job.len > staging.max_encoded_bytes) return error.InvalidCatalogRecord;
    const job = try std.json.parseFromSliceLeaky(staging.Job, a, encoded_job, .{});
    const binding = job.plan.catalog_binding orelse return error.InvalidCatalogMutation;
    if (!std.mem.eql(u8, &job.plan.id, &participant.staging_id) or
        !std.mem.eql(u8, &job.plan_digest, &participant.plan_digest) or
        !std.mem.eql(u8, &try job.plan.digest(a), &participant.plan_digest))
        return error.ExternalTransactionDecisionChanged;
    try binding.requireIdentity(a, group, identity, participant);
    var progress_key: [128]u8 = undefined;
    const encoded_progress = (try optional(txn, try staging.progressKey(&progress_key, group, participant.staging_id))) orelse return error.CatalogPublicationProofPending;
    if (encoded_progress.len > staging.max_encoded_bytes) return error.InvalidCatalogRecord;
    const progress = try std.json.parseFromSliceLeaky(staging.Progress, a, encoded_progress, .{});
    if (progress.revision < job.revision or progress.reservation_rejection != null) return error.InvalidCatalogRecord;
    return .{ .job = job, .progress = progress, .binding = binding };
}

/// Reads the live declaration and exact prepared seal in the metadata write
/// transaction. Projection skips the staged row payload and execution plan.
pub fn catalogReservationAuthority(a: A, txn: anytype, group: u64, reservation_plan: @import("../metadata/restore_staging.zig").Plan) !@import("../metadata/restore_staging.zig").Command.CatalogReservation {
    const staging = @import("../metadata/restore_staging.zig");
    const records = @import("../metadata/transaction_session_record.zig");
    const binding = reservation_plan.catalog_binding orelse return error.InvalidCatalogMutation;
    const frame = try binding.requireBirth(a, txn, group);
    var logical: [records.session_prefix.len + 32]u8 = undefined;
    @memcpy(logical[0..records.session_prefix.len], records.session_prefix);
    logical[records.session_prefix.len..].* = std.fmt.bytesToHex(binding.transaction, .lower);
    const key = try records.keyAlloc(a, group, &logical);
    defer a.free(key);
    const raw = (try optional(txn, key)) orelse return error.SqlTransactionNotActive;
    if (raw.len > records.max_bytes) return error.InvalidTransactionSessionRecord;
    const Pending = @import("../metadata/sql/relation_lifecycle/generation_intents.zig").Pending;
    const Header = struct {
        sql: ?struct { database: []const u8, catalog_generations: Pending = .{} } = null,
        external_preparation: ?[]const u8 = null,
    };
    const header = try std.json.parseFromSliceLeaky(Header, a, raw, .{ .ignore_unknown_fields = true });
    const session = header.sql orelse return error.InvalidTransactionSessionRecord;
    const seal_bytes = header.external_preparation orelse return error.InvalidTransactionSessionRecord;
    if (seal_bytes.len > max_bytes) return error.InvalidTransactionSessionRecord;
    const seal = try std.json.parseFromSliceLeaky(@import("../api/transactions.zig").ExternalPreparationSeal, a, seal_bytes, .{});
    const participant = seal.identity.catalog_participant orelse return error.InvalidCatalogMutation;
    try binding.requireIdentity(a, group, seal.identity, participant);
    const identity_session = seal.identity.session orelse return error.InvalidTransactionSessionRecord;
    if (!std.mem.eql(u8, &participant.staging_id, &reservation_plan.id) or identity_session.owner_node_id != frame.registration.lease.node or
        identity_session.connection_id == null or !std.mem.eql(u8, &identity_session.connection_id.?, &frame.registration.connection))
        return error.ExternalTransactionDecisionChanged;
    for (session.catalog_generations.entries.items) |entry| {
        if (!entry.birth.backend.eql(binding.fence.backend) or entry.birth.database != binding.fence.database or
            entry.birth.group != group or entry.birth.standalone != binding.fence.standalone or
            !std.mem.eql(u8, &entry.birth.principal, &binding.fence.principal) or
            !std.mem.eql(u8, &entry.transaction, &binding.transaction) or
            !std.mem.eql(u8, entry.target.database, session.database)) return error.InvalidCatalogMutation;
    }
    const original = try seal.admissionRequest();
    const journal: outcomes.Store(@TypeOf(txn.*)) = .{ .txn = txn, .group = group, .incarnation = binding.fence.backend.incarnation };
    if (try journal.prepare(a, original.admission, try commandDigest(a, original)) == .replay) return error.InvalidCatalogMutation;
    const pending = try a.create(Pending);
    pending.* = session.catalog_generations;
    return staging.Command.CatalogReservation{ .group = group, .pending = pending, .plan_digest = participant.plan_digest };
}

/// Read the reservation from the exact durable execution plan. The native
/// writer uses this before staging either half of catalog admission.
fn catalogAdmissionReservation(a: A, txn: anytype, group: u64, request: Mutation) !?[]const u8 {
    const identity = request.operation.admit.identity;
    const participant = identity.catalog_participant orelse return null;
    const stages = @import("../metadata/restore_staging.zig");
    var job_key: [128]u8 = undefined;
    if (try optional(txn, try stages.jobKey(&job_key, group, participant.staging_id)) != null) return null;
    const sessions = @import("../metadata/transaction_session_record.zig");
    var logical: [sessions.session_prefix.len + 32]u8 = undefined;
    @memcpy(logical[0..sessions.session_prefix.len], sessions.session_prefix);
    logical[sessions.session_prefix.len..].* = std.fmt.bytesToHex(identity.txn_id, .lower);
    const key = try sessions.keyAlloc(a, group, &logical);
    defer a.free(key);
    const raw = (try optional(txn, key)) orelse return error.CatalogPublicationProofPending;
    const Header = struct { execution_plan: ?[]const u8 = null, external_preparation: ?[]const u8 = null };
    const header = try std.json.parseFromSliceLeaky(Header, a, raw, .{ .ignore_unknown_fields = true });
    const encoded = header.execution_plan orelse return error.CatalogPublicationProofPending;
    const retained = header.external_preparation orelse return error.CatalogPublicationProofPending;
    const seal = try std.json.parseFromSliceLeaky(@import("../api/transactions.zig").ExternalPreparationSeal, a, retained, .{});
    if (!std.mem.eql(u8, &try commandDigest(a, request), &try commandDigest(a, try seal.admissionRequest())))
        return error.ExternalTransactionDecisionChanged;
    if (!std.mem.eql(u8, &try planDigest(encoded), &identity.plan_digest)) return error.ExternalTransactionDecisionChanged;
    const input = try std.json.parseFromSliceLeaky(struct { catalog: stages.Plan }, a, encoded, .{ .ignore_unknown_fields = true });
    const catalog = input.catalog;
    const binding = catalog.catalog_binding orelse return error.InvalidCatalogMutation;
    try binding.requireIdentity(a, group, identity, participant);
    if (!std.mem.eql(u8, &catalog.id, &participant.staging_id) or
        !std.mem.eql(u8, &try catalog.digest(a), &participant.plan_digest))
        return error.ExternalTransactionDecisionChanged;
    const command: stages.Command = .{ .action = .reserve, .id = catalog.id, .plan = catalog };
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    _ = stages.parseCatalogReservationTxn(a, bytes, txn, group) catch |err| switch (err) {
        error.OutOfMemory, error.Io => return err,
        else => return error.CatalogGenerationChanged,
    };
    return bytes;
}

fn requireCatalogAdmission(a: A, txn: anytype, group: u64, identity: Identity) !void {
    const observed = try observeCatalog(a, txn, group, identity);
    if (observed.progress.state != .importing and observed.progress.state != .validating) return error.CatalogPublicationProofPending;
    const frame = observed.binding.requireBirth(a, txn, group) catch |err| return if (err == error.SqlTransactionNotActive) error.CatalogGenerationChanged else @import("lock_authority.zig").semanticError(err);
    const session = identity.session orelse return error.InvalidCatalogMutation;
    if (session.owner_node_id != frame.registration.lease.node or session.connection_id == null or
        !std.mem.eql(u8, &session.connection_id.?, &frame.registration.connection))
        return error.ExternalTransactionDecisionChanged;
}

/// Only a retained SQL decision may authorize restore cutover or cleanup.
pub fn requireCatalogTransition(a: A, txn: anytype, group: u64, job: @import("../metadata/restore_staging.zig").Job, action: @import("../metadata/restore_staging.zig").Command.Action) !void {
    const binding = job.plan.catalog_binding orelse return error.InvalidCatalogMutation;
    const record = (try readOperation(a, txn, group, binding.operation)) orelse return error.CatalogPublicationProofPending;
    try binding.requireIdentity(a, group, record.identity, .{
        .staging_id = job.plan.id,
        .plan_digest = job.plan_digest,
        .intent_digest = binding.intent_digest,
    });
    try requireCatalogTransitionDecision(record, action);
}

fn requireCatalogTransitionDecision(record: Record, action: @import("../metadata/restore_staging.zig").Command.Action) !void {
    const expected: @FieldType(Decision, "status") = switch (action) {
        .catalog_owner_published => {
            if (record.catalog_outcome != .published or record.decision == null or record.decision.?.status != .committed)
                return error.InvalidPreparedTransactionPhase;
            return;
        },
        .imported, .validated, .source_handoff_checked => {
            if (record.phase != .preparing or record.decision != null or record.catalog_outcome != .pending)
                return error.InvalidPreparedTransactionPhase;
            return;
        },
        .begin_cutover, .old_fenced, .parent_fenced, .begin_activation, .parent_activated, .target_admissions_activated => .committed,
        .begin_cancel, .canceled => .aborted,
        else => return error.InvalidCatalogMutation,
    };
    const decision = record.decision orelse return error.CatalogPublicationProofPending;
    if (record.catalog_outcome != .pending or decision.status != expected) return error.InvalidPreparedTransactionPhase;
    switch (expected) {
        .committed => if (record.phase != .decided or record.activations != record.mask()) return error.CatalogPublicationProofPending,
        .aborted => if (record.phase != .resolved) return error.CatalogPublicationProofPending,
    }
}

fn requireCatalogPrepared(a: A, txn: anytype, group: u64, record: Record) !void {
    const observed = try observeCatalog(a, txn, group, record.identity);
    const staging = @import("../metadata/restore_staging.zig");
    var total: usize = 0;
    for (observed.job.plan.targets) |target| total += target.ranges.len;
    if (total == 0 or observed.progress.state != .validating or observed.progress.completed_owners != total)
        return error.CatalogPublicationProofPending;
    for (observed.job.plan.targets) |target| {
        if (target.generation_handoffs.len == 0) continue;
        const prior = target.replace orelse return error.InvalidCatalogRecord;
        for (prior.ranges) |range| {
            const handoff = (try staging.generationHandoffForOldRange(target, range)) orelse return error.InvalidCatalogRecord;
            const fence = for (prior.fences) |candidate| {
                if (candidate.owner_group_id == range.group_id) break candidate;
            } else return error.InvalidCatalogRecord;
            var buffer: [256]u8 = undefined;
            const key = try staging.receiptKey(&buffer, group, observed.job.plan.id, .validating, range.group_id);
            const actual = (try optional(txn, key)) orelse return error.CatalogPublicationProofPending;
            const expected = try staging.generationHandoffPreflightDigest(fence, observed.job.plan_digest, handoff);
            if (!std.mem.eql(u8, (try staging.ReceiptRecord.decode(actual)).payload, &expected)) return error.CatalogPublicationProofPending;
        }
    }
}

pub fn predecessorParticipant(record: Record, old: @import("../metadata/restore_staging.zig").Plan.CatalogPredecessor) !?usize {
    for (record.participants, 0..) |part, index| {
        if (part.route.route.group_id != old.range.group_id) continue;
        const namespace = part.route.route.identity_namespace;
        if (!std.mem.eql(u8, part.table_name, old.table.name) or part.route.table_id != old.table.table_id or
            namespace.table_id != old.fence.namespace.table_id or namespace.shard_id != old.fence.namespace.shard_id or
            namespace.range_id != old.fence.namespace.range_id or part.sql_locks == null or part.publication_capture != null or
            part.route.route.execution != .raft) return error.ExternalTransactionDecisionChanged;
        return index;
    }
    return null;
}

/// The metadata writer joins retained predecessor receipts to the immutable
/// manifest. A source count of one is safe only when that retained count is
/// this transaction, and no enrolled lock participant remains.
pub fn drainedCatalogMembers(a: A, txn: anytype, group: u64, cohort_plan: @import("../metadata/restore_staging.zig").Plan, record: Record, table: ?u64) !u64 {
    const staging = @import("../metadata/restore_staging.zig");
    if (record.pendingCatalogCompletion() != .published) return 0;
    var mask: u64 = 0;
    var index: usize = 0;
    while (try cohort_plan.catalogPredecessorAt(index)) |old| : (index += 1) {
        if (table) |id| if (old.table.table_id != id) continue;
        const member = (try predecessorParticipant(record, old)) orelse continue;
        if (try tableCount(a, txn, group, old.table.table_id) != 1) return error.CatalogPublicationProofPending;
        var key: [256]u8 = undefined;
        if (try optional(txn, try staging.receiptKey(&key, group, cohort_plan.id, .cutover, old.range.group_id)) == null)
            return error.CatalogPublicationProofPending;
        mask |= @as(u64, 1) << @intCast(member);
    }
    return mask;
}

/// Record the terminal cohort outcome in the accepting metadata transaction.
/// The caller must persist publication or cancellation progress in this same
/// transaction, after its native owner and namespace checks.
pub fn completeCatalogTxn(a: A, txn: anytype, group: u64, binding: CatalogBinding, participant: CatalogParticipant, expected_revision: u64, owner: Owner) !Record {
    try binding.validate(group);
    try participant.validate();
    const record = (try readOperation(a, txn, group, binding.operation)) orelse return error.SqlPreparedTransactionNotFound;
    try binding.requireIdentity(a, group, record.identity, participant);
    const observed = try observeCatalog(a, txn, group, record.identity);
    if (!std.meta.eql(observed.binding, binding)) return error.ExternalTransactionDecisionChanged;
    const job = observed.job;
    const progress = observed.progress;
    const outcome: CatalogOutcome = switch (progress.state) {
        .published => blk: {
            const phase = job.plan.catalogPublicationPhase() orelse return error.InvalidCatalogRecord;
            if (progress.completed_owners != phase.owners) return error.CatalogPublicationProofPending;
            break :blk .published;
        },
        .canceled => blk: {
            const owners = job.plan.cancellationOwnerCount();
            if (owners == 0 or progress.completed_owners != owners) return error.CatalogPublicationProofPending;
            break :blk .canceled;
        },
        else => return error.CatalogPublicationProofPending,
    };
    const retired = if (outcome == .published) try drainedCatalogMembers(a, txn, group, job.plan, record, null) else 0;
    const next = try completeCatalogWithRetirement(a, group, record, expected_revision, owner, binding, participant, outcome, retired);
    if (next.revision != record.revision) {
        const bytes = try std.json.Stringify.valueAlloc(a, next, .{});
        if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
        try txn.put(try operationKey(a, group, binding.operation), bytes);
    }
    return next;
}

/// Decision intent for an already validated immutable registry record. No
/// clock sampling, I/O, metadata mutation or participant work occurs here.
/// A caller must obtain a fresh decision version before applying the action.
pub fn recoveryDecision(record: Record, now_ns: u64) ?enum { abort, commit } {
    return switch (record.phase) {
        .preparing => if (now_ns >= record.identity.expires_at_unix_ns) .abort else null,
        .prepared => if (record.identity.kind == .session_commit) .commit else null,
        .locking, .decided, .active, .resolved, .retired => null,
    };
}

pub const AdmissionInput = struct { identity: Identity, participants: []const Participant, admitted_at_ns: u64 };

pub const Mutation = struct {
    /// Capability marker travels with advances after the manifest was frozen.
    notification_control: ?bool = null,
    admission: outcomes.Admission,
    incarnation: [16]u8,
    actor: Actor,
    native_admin: bool = false,
    standalone: bool = false,
    database_id: u64,
    operation: union(enum) {
        admit: AdmissionInput,
        /// Competes for the original admission nonce and canonical digest.
        /// The first durable representation wins; no new GID is reserved.
        cancel_admission: struct { original_actor: Actor, original: AdmissionInput },
        advance: struct { owner: Owner, expected_revision: u64, action: Action },
        replication_admit: struct {
            input: AdmissionInput,
            event: ReplicationEvent,
            recipe_digest: [32]u8,
            page_proof: ?@import("../metadata/replication_initialization_attestation.zig").Proof = null,
            initialization: ?struct {
                lease: local.system_catalog_logical_replication_catalog.SubscriptionWorkLease,
                attempt: [16]u8,
                snapshot: @import("../metadata/replication_initialization_attestation.zig").Snapshot,
            } = null,
            intents: ?ReplicationIntents = null,
            permits: []const ReplicationPermit,
            wal_access: ?[]const ReplicationWalAccess = null,
        },
        replication_advance: struct {
            owner: Owner,
            reference: ReplicationReference,
            expected_revision: u64,
            operation: ReplicationAdvance,
        },
        replication_checkpoint: struct {
            owner: Owner,
            reference: ReplicationReference,
            expected_revision: u64,
            receipts: []const @import("../api/replication_source_apply_attestation.zig").Acknowledged,
        },
        replication_ack_publish: struct {
            owner: Owner,
            reference: ReplicationReference,
            expected_revision: u64,
            receipts: []const @import("../api/replication_source_apply_attestation.zig").Receipt,
        },
        acknowledge_receipt: struct { owner: Owner, txn_id: [16]u8 },
    },

    pub fn jsonStringify(self: Mutation, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Mutation).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "notification_control")) or self.notification_control != null) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: Mutation) !void {
        if (self.notification_control) |active| if (!active) return error.InvalidCatalogMutation;
        try self.admission.validate();
        try self.actor.validate();
        if (self.native_admin and self.actor.current_role != null) return error.InvalidCatalogMutation;
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .replication_admit => |command| {
                if (self.actor.principal().kind != .bootstrap or self.native_admin or self.standalone or self.notification_control != null) return error.Forbidden;
                try command.input.identity.validate();
                try command.event.validate();
                const identity = command.input.identity;
                if (identity.kind != .replication or identity.database_id != self.database_id or
                    !std.mem.eql(u8, &identity.metadata_incarnation, &std.fmt.bytesToHex(self.incarnation, .lower)) or
                    command.input.participants.len > max_participants or command.permits.len != command.input.participants.len or
                    ((command.event.kind == .snapshot_begin or command.event.kind == .initial_copy) and command.permits.len != 0) or
                    (command.event.kind == .snapshot_begin) != (command.initialization != null) or
                    (command.event.kind == .snapshot_page) != (command.page_proof != null) or
                    (command.intents == null) != (command.input.participants.len == 0) or
                    (command.intents != null and command.intents.?.members.len != command.input.participants.len) or
                    std.mem.allEqual(u8, &command.recipe_digest, 0)) return error.InvalidCatalogMutation;
                if ((command.event.kind == .transaction) != (command.wal_access != null))
                    return error.InvalidCatalogMutation;
                var previous: u64 = 0;
                const wal_access = command.wal_access orelse &.{};
                if (wal_access.len > max_participants) return error.InvalidCatalogMutation;
                for (wal_access) |access| {
                    // A newly created relational table has layout version zero.
                    // The receiving writer verifies both its exact version and digest.
                    if (access.table_id <= previous or access.catalog_revision == 0 or
                        std.mem.allEqual(u8, &access.schema_digest, 0) or access.rights == 0)
                        return error.InvalidCatalogMutation;
                    previous = access.table_id;
                }
                if (command.initialization) |initial| {
                    const source = identity.replication_source.?;
                    if (!std.mem.eql(u8, &initial.attempt, &source.txn_id) or
                        identity.expires_at_unix_ns > initial.lease.expires_at_ns or
                        identity.expires_at_unix_ns > initial.snapshot.purpose.upload.expires_at_unix_ns)
                        return error.InvalidCatalogMutation;
                }
            },
            .replication_advance => |command| {
                if (self.actor.principal().kind != .bootstrap or self.native_admin or self.standalone or self.notification_control != null) return error.Forbidden;
                try command.owner.validate();
                try command.reference.validate();
                if (command.expected_revision == 0) return error.InvalidCatalogMutation;
            },
            .replication_checkpoint => |command| {
                if (self.actor.principal().kind != .bootstrap or self.native_admin or self.standalone or self.notification_control != null) return error.Forbidden;
                try command.owner.validate();
                try command.reference.validate();
                if (command.expected_revision == 0 or command.receipts.len > max_participants) return error.InvalidCatalogMutation;
            },
            .replication_ack_publish => |command| {
                if (self.actor.principal().kind != .bootstrap or self.native_admin or self.standalone or self.notification_control != null) return error.Forbidden;
                try command.owner.validate();
                try command.reference.validate();
                if (command.expected_revision == 0 or command.receipts.len == 0 or command.receipts.len > max_participants) return error.InvalidCatalogMutation;
            },
            .acknowledge_receipt => |receipt| {
                try receipt.owner.validate();
                if (self.actor.principal().kind != .bootstrap or std.mem.allEqual(u8, &receipt.txn_id, 0)) return error.Forbidden;
            },
            .admit => |request| try validateAdmission(self, request, self.actor),
            .cancel_admission => |cancel| {
                if (self.actor.principal().kind != .bootstrap or self.native_admin) return error.Forbidden;
                try validateAdmission(self, cancel.original, cancel.original_actor);
            },
            .advance => |request| {
                try request.owner.validate();
                if (request.expected_revision == 0) return error.InvalidCatalogMutation;
                if (request.action == .catalog_complete and request.action.catalog_complete == .pending) return error.InvalidCatalogMutation;
                switch (request.action) {
                    .commit, .abort => {},
                    // Only the trusted recovery/coordinator worker can attest
                    // native votes, activations, resolutions and handoffs.
                    else => if (self.actor.principal().kind != .bootstrap) return error.Forbidden,
                }
            },
        }
    }
};

fn validateAdmission(self: Mutation, request: AdmissionInput, actor: Actor) !void {
    try actor.validate();
    try request.identity.validate();
    if (request.identity.kind == .replication) return error.Forbidden;
    if ((request.identity.notification_plan != null) != (self.notification_control != null)) return error.InvalidCatalogMutation;
    if (request.identity.session) |session| try session.requireRealm(self.standalone);
    if (request.identity.database_id != self.database_id or !request.identity.principal.eql(actor.principal()) or
        !std.mem.eql(u8, &request.identity.operation_id, &self.admission.nonce) or
        !std.mem.eql(u8, &request.identity.metadata_incarnation, &std.fmt.bytesToHex(self.incarnation, .lower))) return error.InvalidCatalogMutation;
    try (Owner{ .metadata_incarnation = request.identity.metadata_incarnation, .operation_id = request.identity.operation_id, .admission_digest = @splat(1), .expires_at_unix_ns = request.identity.expires_at_unix_ns }).requireFreshVoteAt(request.admitted_at_ns);
    if (request.participants.len > max_participants) return error.CatalogCommandTooLarge;
    for (request.participants) |participant| try participant.validate(request.identity.metadata_incarnation);
    try request.identity.validateHistoryCohort(request.participants);
}

/// Cancellation is an alternative execution of the exact same admission, not
/// an unrelated mutation that could race a later original proposal. The
/// command envelope still requires trusted bootstrap authority to cancel.
pub fn admissionRequest(request: Mutation) Mutation {
    if (request.operation != .cancel_admission) return request;
    var original = request;
    original.actor = request.operation.cancel_admission.original_actor;
    original.operation = .{ .admit = request.operation.cancel_admission.original };
    return original;
}

pub fn commandDigest(a: A, request: Mutation) ![32]u8 {
    try request.validate();
    return digest(a, "antfly:sql-prepared-command:v1\x00", admissionRequest(request));
}
pub const ReceiptWorkRequest = struct {
    owner_node_id: ?u64 = null,
    after: ?[16]u8 = null,
    standalone: bool = false,
    pub fn jsonStringify(self: ReceiptWorkRequest, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(ReceiptWorkRequest).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "standalone")) or self.standalone) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: ReceiptWorkRequest) !void {
        if (self.standalone) {
            if (self.owner_node_id == null or self.owner_node_id.? != 0) return error.InvalidCatalogMutation;
        } else if (self.owner_node_id != null and self.owner_node_id.? == 0) return error.InvalidCatalogMutation;
    }
};

pub const ReplicationPhase = union(enum) {
    acknowledge: ReplicationAck,
    prepare,
    activate,
    resolve: struct { status: local.storage_db_types.TxnStatus, version: u64 },
};

pub const Read = union(enum) {
    gid: []const u8,
    operation: [16]u8,
    replication_checkpoint: struct { reference: ReplicationReference, checkpoint: u64 },
    replication_phase: struct { permit: ReplicationPermit, table_name: []const u8, phase: ReplicationPhase = .prepare },
    work: ?[16]u8,
    receipt: [16]u8,
    receipt_work: ReceiptWorkRequest,
    pub fn validate(self: Read) !void {
        switch (self) {
            .gid => |gid| try validateGid(gid),
            .operation => |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidCatalogMutation,
            .replication_checkpoint => |probe| {
                try probe.reference.validate();
                if (probe.checkpoint == 0) return error.InvalidCatalogMutation;
            },
            .replication_phase => |probe| {
                try probe.permit.validate();
                if (probe.table_name.len == 0 or probe.table_name.len > 1024) return error.InvalidCatalogMutation;
            },
            .work => {},
            .receipt => |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidCatalogMutation,
            .receipt_work => |work| try work.validate(),
        }
    }
};
pub const Snapshot = struct {
    standalone: bool = false,
    publication_capture_ready: bool = false,
    catalog_revision: u64 = 0,
    incarnation: [16]u8,
    head: u64,
    active: u64,
    records: []const Record,
    receipts: []const Receipt = &.{},
    outstanding_receipts: u64 = 0,
    next_after: ?[16]u8 = null,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "publication_capture_ready")) or self.publication_capture_ready) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
};
const publisher_journal = @import("replication/decision_journal.zig");
pub const Write = publisher_journal.Write;
pub const Plan = struct {
    writes: []const Write,
    deletes: []const []const u8,
    result: outcomes.Result,
    catalog_reservation: ?[]const u8 = null,

    /// The caller commits this together with its retained command receipt.
    pub fn apply(self: Plan, txn: anytype) !void {
        for (self.writes) |write| try txn.put(write.key, write.value);
        for (self.deletes) |key| try txn.delete(key);
    }
};

fn prefix(a: A, group: u64, family: []const u8) ![]u8 {
    if (group == 0) return error.InvalidCatalogMutation;
    return std.fmt.allocPrint(a, "\x00\x00__metadata__:sql_prepared:{d}:{s}:", .{ group, family });
}
pub fn operationKey(a: A, group: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "operation"), &std.fmt.bytesToHex(id, .lower) });
}
pub fn workKey(a: A, group: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "work"), &std.fmt.bytesToHex(id, .lower) });
}
fn gidKey(a: A, group: u64, gid: []const u8) ![]u8 {
    var bytes: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(gid, &bytes, .{});
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "gid"), &std.fmt.bytesToHex(bytes, .lower) });
}
fn countKey(a: A, group: u64, family: []const u8, id: u64) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{d}", .{ try prefix(a, group, family), id });
}
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) null else return err;
}
fn number(txn: anytype, key: []const u8) !u64 {
    const value = (try optional(txn, key)) orelse return 0;
    if (value.len != 8) return error.InvalidCatalogRecord;
    return std.mem.readInt(u64, value[0..8], .big);
}
pub fn activeCount(a: A, txn: anytype, group: u64) !u64 {
    _ = a;
    if (group == 0) return error.InvalidCatalogMutation;
    var buffer: [160]u8 = undefined;
    const key = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:sql_prepared:{d}:active:{d}", .{ group, 0 });
    const count = try number(txn, key);
    if (count > max_active) return error.InvalidCatalogRecord;
    return count;
}
pub fn databaseCount(a: A, txn: anytype, group: u64, id: u64) !u64 {
    _ = a;
    if (group == 0) return error.InvalidCatalogMutation;
    var buffer: [160]u8 = undefined;
    const key = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:sql_prepared:{d}:database:{d}", .{ group, id });
    const count = try number(txn, key);
    if (count > max_active) return error.InvalidCatalogRecord;
    return count;
}
pub fn tableCount(a: A, txn: anytype, group: u64, id: u64) !u64 {
    _ = a;
    if (group == 0) return error.InvalidCatalogMutation;
    var buffer: [160]u8 = undefined;
    const key = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:sql_prepared:{d}:table:{d}", .{ group, id });
    const count = try number(txn, key);
    if (count > max_active) return error.InvalidCatalogRecord;
    return count;
}
pub fn readOperation(a: A, txn: anytype, group: u64, id: [16]u8) !?Record {
    const bytes = (try optional(txn, try operationKey(a, group, id))) orelse return null;
    if (bytes.len > max_bytes) return error.InvalidCatalogRecord;
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always });
    try record.validate(a);
    if (!std.mem.eql(u8, &record.identity.operation_id, &id) or record.phase == .retired) return error.InvalidCatalogRecord;
    const work = try optional(txn, try workKey(a, group, id));
    const needs_sql_work = record.identity.kind != .replication and (record.phase != .prepared or record.identity.kind == .session_commit);
    if (needs_sql_work != (work != null) or (work != null and work.?.len != 0)) return error.InvalidCatalogRecord;
    return record;
}
/// Linearizable permission read used before the data owner proposes PREPARE.
pub fn allowReplicationPhase(a: A, txn: anytype, group: u64, incarnation: [32]u8, permit: ReplicationPermit, table_name: []const u8, now_ns: u64, phase: ReplicationPhase) !bool {
    try permit.validate();
    if (group != permit.semantic.metadata_group or !std.mem.eql(u8, &incarnation, &permit.semantic.metadata_incarnation)) return false;
    const record = (try readOperation(a, txn, group, permit.decision_owner.operation_id)) orelse return false;
    record.requireRetainedReplicationPermit(a, group, permit) catch |err| switch (err) {
        error.ReplicationCutoverAuthorityLost => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, record.participants[permit.member].table_name, table_name)) return false;
    switch (phase) {
        .acknowledge => |completion| {
            if (record.phase != .resolved or !std.meta.eql(permit, completion.permit)) return false;
            try completion.validate();
            const expected = (try replicationAckForMember(a, record, permit.member)) orelse return false;
            return std.meta.eql(expected, completion);
        },
        .prepare => if (record.phase != .preparing) return false,
        .activate => return record.decision != null and record.decision.?.status == .committed and
            (record.phase == .decided or record.phase == .active or record.phase == .resolved),
        .resolve => |resolution| {
            const decision = record.decision orelse return false;
            if (decision.version != resolution.version or resolution.status == .pending or (decision.status == .committed) != (resolution.status == .committed)) return false;
            return record.phase == .active or record.phase == .resolved or
                (record.phase == .decided and decision.status == .aborted);
        },
    }
    permit.decision_owner.requireFreshVoteAt(now_ns) catch |err| switch (err) {
        error.ExternalTransactionAdmissionExpired => return false,
        else => return err,
    };
    return true;
}

pub fn readGid(a: A, txn: anytype, group: u64, gid: []const u8) !?Record {
    try validateGid(gid);
    const bytes = (try optional(txn, try gidKey(a, group, gid))) orelse return null;
    if (bytes.len != 16) return error.InvalidCatalogRecord;
    const record = (try readOperation(a, txn, group, bytes[0..16].*)) orelse return error.InvalidCatalogRecord;
    if (record.identity.kind != .named or !std.mem.eql(u8, record.identity.gid, gid)) return error.InvalidCatalogRecord;
    return record;
}
fn changeCount(a: A, txn: anytype, writes: *std.ArrayListUnmanaged(Write), deletes: *std.ArrayListUnmanaged([]const u8), key: []const u8, adding: bool) !void {
    const old = try number(txn, key);
    if (old > max_active or (adding and old == max_active) or (!adding and old == 0)) return error.InvalidCatalogRecord;
    const value = if (adding) old + 1 else old - 1;
    if (value == 0) return deletes.append(a, key);
    const bytes = try a.alloc(u8, 8);
    std.mem.writeInt(u64, bytes[0..8], value, .big);
    try writes.append(a, .{ .key = key, .value = bytes });
}
fn kindFamily(kind: Kind) []const u8 {
    return switch (kind) {
        .named => "named_active",
        .session_commit => "session_active",
        .replication => "replication_active",
    };
}

fn kindMaximum(kind: Kind) u64 {
    return switch (kind) {
        .named => max_named,
        .session_commit => max_session_commits,
        .replication => max_replication_transactions,
    };
}

pub fn kindCount(a: A, txn: anytype, group: u64, kind: Kind) !u64 {
    const count = try number(txn, try countKey(a, group, kindFamily(kind), 0));
    if (count > kindMaximum(kind)) return error.InvalidCatalogRecord;
    return count;
}
fn reservations(a: A, txn: anytype, group: u64, record: Record, adding: bool, writes: *std.ArrayListUnmanaged(Write), deletes: *std.ArrayListUnmanaged([]const u8)) !void {
    try changeCount(a, txn, writes, deletes, try countKey(a, group, "active", 0), adding);
    try changeCount(a, txn, writes, deletes, try countKey(a, group, kindFamily(record.identity.kind), 0), adding);
    try changeCount(a, txn, writes, deletes, try countKey(a, group, "database", record.identity.database_id), adding);
    for (record.participants, 0..) |participant, index| {
        var seen = false;
        for (record.participants[0..index]) |previous| if (previous.route.table_id == participant.route.table_id) {
            seen = true;
            break;
        };
        if (!seen) try changeCount(a, txn, writes, deletes, try countKey(a, group, "table", participant.route.table_id), adding);
    }
}

/// Called by the native source writer after verifying the current enrollment
/// and the immutable apply plan in the same metadata transaction.
pub fn planReplicationAdmission(a: A, txn: anytype, group: u64, revision: u64, input: AdmissionInput, event: ReplicationEvent, permits: []const ReplicationPermit, intents: ?ReplicationIntents, snapshot: ?SnapshotBoundary) !Plan {
    try input.identity.validate();
    if (input.identity.kind != .replication) return error.InvalidCatalogMutation;
    try (try input.identity.owner(a)).requireFreshVoteAt(input.admitted_at_ns);
    if (input.participants.len > max_participants) return error.CatalogCommandTooLarge;
    for (input.participants) |participant| {
        try participant.validate(input.identity.metadata_incarnation);
        if (participant.route.metadata_group_id != group or participant.sql_locks != null) return error.InvalidCatalogMutation;
    }
    if (try readOperation(a, txn, group, input.identity.operation_id) != null or
        try readReceipt(a, txn, group, input.identity.operation_id) != null) return error.ExternalTransactionDecisionChanged;
    if (try activeCount(a, txn, group) >= max_active or
        try kindCount(a, txn, group, .replication) >= max_replication_transactions) return error.SqlPreparedTransactionCapacityExceeded;

    const reference = input.identity.replication_source.?;
    if (reference.owner != .logical_subscription) return error.InvalidCatalogMutation;
    if (permits.len != input.participants.len) return error.InvalidCatalogRecord;
    var bodies: [max_participants][32]u8 = undefined;
    for (permits, bodies[0..permits.len]) |permit, *body| body.* = permit.pregrant_body_digest;
    if (!std.mem.eql(u8, &reference.intent_digest, &replicationIntentDigest(bodies[0..permits.len])))
        return error.InvalidCatalogMutation;
    var progress = (try readReplicationProgress(a, txn, group, reference.owner)) orelse ReplicationProgress{ .birth = reference.birth, .origin = reference.source_digest };
    if (progress.birth != reference.birth or progress.pending != null or
        !std.mem.eql(u8, &progress.origin, &reference.source_digest) or
        reference.generation != (std.math.add(u64, progress.generation, 1) catch return error.CatalogGenerationExhausted))
        return error.ReplicationCutoverAuthorityLost;
    if (event.kind == .snapshot_begin and (snapshot == null or permits.len != 0))
        return error.ReplicationCutoverAuthorityLost;
    if (snapshot) |initial| {
        if (progress.generation != 0 or event.kind == .transaction) return error.ReplicationCutoverAuthorityLost;
        progress.snapshot_boundary = initial;
        progress.snapshot_next_page = initial.page_chain;
    }
    const boundary = progress.snapshot_boundary orelse return error.ReplicationCutoverAuthorityLost;
    if (event.kind != .transaction) try boundary.requireEvent(event, progress.snapshot_next_page);
    switch (event.kind) {
        .transaction => if (progress.checkpoint == 0 or event.source_point != progress.checkpoint) return error.ReplicationCutoverAuthorityLost,
        .snapshot_begin, .snapshot_page, .initial_copy => {
            if (progress.checkpoint != 0 or event.offset != progress.snapshot_offset or
                (progress.snapshot_cut != 0 and progress.snapshot_cut != event.source_point)) return error.ReplicationCutoverAuthorityLost;
            progress.snapshot_cut = event.source_point;
        },
    }
    progress.pending = reference;
    progress.generation = reference.generation;
    const record: Record = .{ .identity = input.identity, .participants = input.participants, .replication_event = event, .replication_intents = intents, .replication_permits = permits };
    try record.validate(a);
    for (permits) |permit| {
        try record.requireReplicationPermit(a, group, permit);
        const target_digest = try replicationTargetDigest(group, record.identity.metadata_incarnation, reference.owner, reference.birth, permit.namespace);
        if (!std.mem.eql(u8, &permit.target_custody_digest, &target_digest)) return error.ReplicationCutoverAuthorityLost;
        const target = try readReplicationTarget(a, txn, group, target_digest);
        const image = if (target) |value| value.image else null;
        const marker = if (target) |value| value.marker else null;
        if (!std.meta.eql(image, permit.expected_image) or !std.meta.eql(marker, permit.previous_marker_digest))
            return error.ReplicationCutoverAuthorityLost;
    }
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
    var writes: std.ArrayListUnmanaged(Write) = .empty;
    var deletes: std.ArrayListUnmanaged([]const u8) = .empty;
    try reservations(a, txn, group, record, true, &writes, &deletes);
    try writes.append(a, .{ .key = try replicationProgressKey(a, group, reference.owner), .value = try std.json.Stringify.valueAlloc(a, progress, .{}) });
    try writes.append(a, .{ .key = try replicationPendingKey(a, group, reference.txn_id), .value = try std.json.Stringify.valueAlloc(a, reference, .{}) });
    try writes.append(a, .{ .key = try operationKey(a, group, record.identity.operation_id), .value = bytes });
    return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = revision, .identity = null, .removed = 0 } };
}

pub const ReplicationAdvancePlan = struct { record: Record, plan: Plan };

/// Plan a source-owned phase transition. The native source writer supplies
/// participant proofs and commits this plan together with source state.
pub fn planReplicationAdvance(a: A, txn: anytype, group: u64, revision: u64, reference: ReplicationReference, owner: Owner, expected_revision: u64, action: Action) !ReplicationAdvancePlan {
    if (action == .handoff) return error.ReplicationCutoverAuthorityLost;
    return planReplicationAdvanceInternal(a, txn, group, revision, reference, owner, expected_revision, action);
}

fn planReplicationAdvanceInternal(a: A, txn: anytype, group: u64, revision: u64, reference: ReplicationReference, owner: Owner, expected_revision: u64, action: Action) !ReplicationAdvancePlan {
    try reference.validate();
    const previous = (try readOperation(a, txn, group, owner.operation_id)) orelse return error.SqlPreparedTransactionNotFound;
    return planReplicationAdvanceRecord(a, txn, group, revision, reference, owner, expected_revision, previous, action);
}

pub fn planVerifiedReplicationAdvance(a: A, txn: anytype, group: u64, revision: u64, reference: ReplicationReference, owner: Owner, expected_revision: u64, previous: Record, operation: ReplicationAdvance, verifier: anytype) !ReplicationAdvancePlan {
    if (previous.identity.kind != .replication or !previous.identity.replication_source.?.eql(reference) or !(try previous.identity.owner(a)).eql(owner))
        return error.ExternalTransactionDecisionChanged;
    if (previous.revision != expected_revision) return error.CatalogGenerationChanged;
    const action = try verifiedReplicationAction(a, group, previous, operation, verifier);
    return planReplicationAdvanceRecord(a, txn, group, revision, reference, owner, expected_revision, previous, action);
}

fn planReplicationAdvanceRecord(a: A, txn: anytype, group: u64, revision: u64, reference: ReplicationReference, owner: Owner, expected_revision: u64, previous: Record, action: Action) !ReplicationAdvancePlan {
    if (previous.identity.kind != .replication or !previous.identity.replication_source.?.eql(reference))
        return error.ExternalTransactionDecisionChanged;
    if (action == .prepare and previous.votes != previous.mask()) return error.InvalidPreparedTransactionPhase;
    if (action == .commit and action.commit.sync_level != previous.identity.commit_sync_level) return error.InvalidPreparedTransactionPhase;
    const next = try advance(a, previous, expected_revision, owner, action);
    var writes: std.ArrayListUnmanaged(Write) = .empty;
    var deletes: std.ArrayListUnmanaged([]const u8) = .empty;
    try appendPublicationAdvance(a, txn, group, previous, next, action, &writes);
    const key = try operationKey(a, group, next.identity.operation_id);
    if (next.phase == .retired) {
        try reservations(a, txn, group, next, false, &writes, &deletes);
        try deletes.append(a, key);
    } else {
        const bytes = try std.json.Stringify.valueAlloc(a, next, .{});
        if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
        try writes.append(a, .{ .key = key, .value = bytes });
    }
    return .{ .record = next, .plan = .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = revision, .identity = null, .removed = 0 } } };
}

pub const ReplicationAckGrant = struct {
    participant_ids_digest: [32]u8,
    terminal_receipts_digest: [32]u8,
};

fn replicationParticipantId(a: A, record: Record, member: usize) ![]u8 {
    const participant = record.participants[member];
    return local.api_local_transaction_contract.participantIdForGroup(a, participant.table_name, participant.route.route.group_id);
}

pub fn replicationAckForMember(a: A, record: Record, member: usize) !?ReplicationAck {
    const grant = record.replication_ack orelse return null;
    const decision = record.decision orelse return error.InvalidCatalogRecord;
    if (member >= record.replication_permits.len or member >= record.participants.len) return error.InvalidCatalogRecord;
    const id = try replicationParticipantId(a, record, member);
    defer a.free(id);
    return .{
        .permit = record.replication_permits[member],
        .participant_count = @intCast(record.participants.len),
        .participant_ids_digest = grant.participant_ids_digest,
        .participant_id_digest = local.api_replication_source_apply.participantIdDigest(id),
        .decision = if (decision.status == .committed) .committed else .aborted,
        .decision_version = decision.version,
        .terminal_receipts_digest = grant.terminal_receipts_digest,
    };
}

/// Build the ACK grants from native receipts validated against enrollment in
/// the same metadata transaction, then retain only their exact commitments.
pub fn planReplicationAcknowledgements(a: A, group: u64, revision: u64, reference: ReplicationReference, owner: Owner, expected_revision: u64, previous: Record, receipts: []const @import("../api/replication_source_apply_attestation.zig").Receipt, verifier: anytype) !struct { plan: Plan, completions: []const ReplicationAck } {
    var record = previous;
    if (record.identity.kind != .replication or !record.identity.replication_source.?.eql(reference) or !(try record.identity.owner(a)).eql(owner))
        return error.ExternalTransactionDecisionChanged;
    if (record.revision != expected_revision) return error.CatalogGenerationChanged;
    const decision = record.decision orelse return error.InvalidPreparedTransactionPhase;
    if (record.phase != .resolved or receipts.len != record.participants.len or receipts.len == 0)
        return error.InvalidPreparedTransactionPhase;
    if (record.publication_capacities.len != record.publication_terminals.len + record.publication_abort_fences.len)
        return error.CatalogPublicationProofPending;

    const ids = try a.alloc([]const u8, record.participants.len);
    for (ids, 0..) |*id, member| id.* = try replicationParticipantId(a, record, member);
    const completions = try @import("../api/replication_source_apply_attestation.zig").terminalCompletions(
        a,
        receipts,
        record.replication_permits,
        ids,
        if (decision.status == .committed) .committed else .aborted,
        decision.version,
        verifier,
    );
    for (record.replication_permits) |permit| try record.requireRetainedReplicationPermit(a, group, permit);
    const grant: ReplicationAckGrant = .{
        .participant_ids_digest = completions[0].participant_ids_digest,
        .terminal_receipts_digest = completions[0].terminal_receipts_digest,
    };
    const result: outcomes.Result = .{ .revision = revision, .identity = null, .removed = 0 };
    if (record.replication_ack) |stored| {
        if (!std.meta.eql(stored, grant)) return error.ExternalTransactionDecisionChanged;
        return .{ .completions = completions, .plan = .{ .writes = &.{}, .deletes = &.{}, .result = result } };
    }
    record.replication_ack = grant;
    record.revision = std.math.add(u64, record.revision, 1) catch return error.CatalogRevisionExhausted;
    try record.validate(a);
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
    const writes = try a.alloc(Write, 1);
    writes[0] = .{ .key = try operationKey(a, group, owner.operation_id), .value = bytes };
    return .{ .completions = completions, .plan = .{ .writes = writes, .deletes = &.{}, .result = result } };
}

/// Verify the complete post-ACK roster before upstream checkpoint publication.
pub fn verifyReplicationAcknowledged(a: A, decision: Record, receipts: []const @import("../api/replication_source_apply_attestation.zig").Acknowledged, verifier: anytype) ![32]u8 {
    if (decision.identity.kind != .replication or decision.phase != .resolved or receipts.len != decision.replication_permits.len or
        (receipts.len != 0 and decision.replication_ack == null)) return error.ReplicationCutoverAuthorityLost;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/source-ack-roster/v1\x00");
    hash.update(&decision.identity.operation_id);
    for (receipts, 0..) |receipt, member| {
        const expected = (try replicationAckForMember(a, decision, member)) orelse return error.ReplicationCutoverAuthorityLost;
        if (!std.meta.eql(receipt.value.completion, expected)) return error.ReplicationCutoverAuthorityLost;
        try receipt.validate(a);
        try verifier.requireAcknowledgedRoot(receipt.ownerScope(), receipt.proof, receipt.value.position);
        hash.update(&try receipt.proof.commitment(receipt.value.marker_digest));
    }
    var roster_commitment: [32]u8 = undefined;
    hash.final(&roster_commitment);
    return roster_commitment;
}

/// Atomically retire the prepared decision and publish the checkpoint proved
/// by its original admitted event and the complete physical ACK roster.
pub fn planReplicationCheckpoint(a: A, txn: anytype, group: u64, revision: u64, reference: ReplicationReference, owner: Owner, expected_revision: u64, receipts: []const @import("../api/replication_source_apply_attestation.zig").Acknowledged, verifier: anytype) !Plan {
    var progress = (try readReplicationProgress(a, txn, group, reference.owner)) orelse return error.ReplicationCutoverAuthorityLost;
    if (progress.birth != reference.birth or progress.pending == null or !progress.pending.?.eql(reference))
        return error.ReplicationCutoverAuthorityLost;
    const record = (try readOperation(a, txn, group, owner.operation_id)) orelse return error.SqlPreparedTransactionNotFound;
    const decision = record.decision orelse return error.InvalidPreparedTransactionPhase;
    const roster = try verifyReplicationAcknowledged(a, record, receipts, verifier);
    const event = record.replication_event orelse return error.InvalidCatalogRecord;
    if (decision.status == .committed) {
        switch (event.kind) {
            .snapshot_begin => {},
            .snapshot_page => {
                progress.snapshot_offset = event.next_offset;
                progress.snapshot_next_page = event.snapshot_page.?.next;
            },
            .initial_copy => {
                progress.snapshot_offset = event.next_offset;
                progress.checkpoint = event.end_point;
            },
            .transaction => progress.checkpoint = event.end_point,
        }
        progress.last_acknowledged = reference;
        progress.ack_roster_digest = roster;
    }
    var target_writes: std.ArrayListUnmanaged(Write) = .empty;
    for (receipts) |receipt| {
        const value = receipt.value;
        const permit = value.completion.permit;
        if (permit.expected_image) |expected| {
            if (!std.meta.eql(expected, value.image)) return error.ReplicationCutoverAuthorityLost;
        }
        const target: ReplicationTarget = .{
            .image = value.image,
            .marker = if (decision.status == .committed) value.terminal_marker_digest else permit.previous_marker_digest,
        };
        try target_writes.append(a, .{
            .key = try replicationTargetKey(a, group, permit.target_custody_digest),
            .value = try std.json.Stringify.valueAlloc(a, target, .{}),
        });
    }
    progress.pending = null;
    var handoff = (try planReplicationAdvanceInternal(a, txn, group, revision, reference, owner, expected_revision, .handoff)).plan;
    handoff.deletes = try std.mem.concat(a, []const u8, &.{ handoff.deletes, &.{try replicationPendingKey(a, group, reference.txn_id)} });
    const write = Write{ .key = try replicationProgressKey(a, group, reference.owner), .value = try std.json.Stringify.valueAlloc(a, progress, .{}) };
    handoff.writes = try std.mem.concat(a, Write, &.{ handoff.writes, target_writes.items, &.{write} });
    return handoff;
}

fn appendPublicationAdvance(a: A, txn: anytype, group: u64, previous: Record, next: Record, action: Action, writes: *std.ArrayListUnmanaged(Write)) !void {
    if (previous.publicationCaptureMask() != 0) {
        const realm: local.system_catalog_logical_replication_publisher.Realm = .{
            .group_id = group,
            .incarnation = previous.identity.metadata_incarnation,
            .database_id = previous.identity.database_id,
        };
        if (action == .commit) {
            const time = action.commit.commit_time_unix_us orelse return error.InvalidCatalogMutation;
            if (time == 0) return error.InvalidCatalogMutation;
            const journal_writes = try publisher_journal.reserve(a, txn, realm, next, time);
            try writes.appendSlice(a, &journal_writes);
        }
        if (next.phase == .retired) {
            if (previous.decision.?.status == .committed) {
                const adoption = try publisher_journal.handoff(a, txn, realm, previous);
                try writes.appendSlice(a, &adoption);
            } else {
                try publisher_journal.handoffAbort(a, txn, realm, previous, writes);
            }
        }
    } else if (action == .commit and action.commit.commit_time_unix_us != null) return error.InvalidCatalogMutation;
}

/// Arena-owned planning performs every rejection before the first native write.
/// Metadata catalog/topology validation runs in the caller's same transaction.
pub fn plan(a: A, txn: anytype, group: u64, catalog_revision: u64, request: Mutation) !Plan {
    return planInternal(a, txn, group, catalog_revision, request, null);
}

/// The prospective reservation has to be derived from a verified durable
/// session here. The accepting writer applies it only after all SQL planning.
pub fn planWithCatalogReservation(a: A, txn: anytype, group: u64, catalog_revision: u64, request: Mutation) !Plan {
    const reservation = if (request.operation == .admit)
        try catalogAdmissionReservation(a, txn, group, request)
    else
        null;
    var result = try planInternal(a, txn, group, catalog_revision, request, reservation);
    result.catalog_reservation = reservation;
    return result;
}

fn planInternal(a: A, txn: anytype, group: u64, catalog_revision: u64, request: Mutation, reservation: ?[]const u8) !Plan {
    try request.validate();
    var writes = std.ArrayListUnmanaged(Write).empty;
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    const record: Record = switch (request.operation) {
        .replication_admit, .replication_advance, .replication_ack_publish, .replication_checkpoint => return error.Forbidden,
        .cancel_admission => |cancel| {
            if (try readOperation(a, txn, group, cancel.original.identity.operation_id) != null or
                try readReceipt(a, txn, group, cancel.original.identity.operation_id) != null) return error.InvalidPreparedTransactionPhase;
            // The caller journals this rejection under the original admission
            // digest in the same transaction. A delayed BEGIN producer can
            // therefore never obtain a successful admission acknowledgement.
            return error.ExternalTransactionAdmissionExpired;
        },
        .acknowledge_receipt => |ack| {
            const receipt = (try readReceipt(a, txn, group, ack.owner.operation_id)) orelse return .{ .writes = &.{}, .deletes = &.{}, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0, .no_op = true } };
            if (!receipt.owner.eql(ack.owner) or !std.mem.eql(u8, &receipt.txn_id, &ack.txn_id)) return error.ExternalTransactionDecisionChanged;
            if (receipt.status == .pending or receipt.progress == .resolving or receipt.progress == .indexing) return error.InvalidPreparedTransactionPhase;
            if (try readOperation(a, txn, group, ack.owner.operation_id)) |pending| {

                // Delivery ACK is not proof that native participants released
                // terminal retention. Only the coordinator's canonical handoff
                // follows those actual ACKs and removes this operation in the
                // same writer. Keep the full manifest until that has happened.
                if (pending.identity.kind == .session_commit) return error.CatalogPublicationProofPending;
            }
            try changeReceiptCount(a, txn, group, false, &writes, &deletes);
            try deletes.append(a, try receiptKey(a, group, ack.owner.operation_id));
            try deletes.append(a, try receiptNodeKey(a, group, receipt.session.owner_node_id, ack.owner.operation_id));
            return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
        },
        .admit => |admission| blk: {
            if (admission.identity.kind == .named and try readGid(a, txn, group, admission.identity.gid) != null) return error.SqlPreparedTransactionAlreadyExists;
            if (try readOperation(a, txn, group, admission.identity.operation_id) != null) return error.ExternalTransactionDecisionChanged;
            if (try activeCount(a, txn, group) == max_active or try kindCount(a, txn, group, admission.identity.kind) == kindMaximum(admission.identity.kind)) return error.SqlPreparedTransactionCapacityExceeded;
            if (admission.identity.catalog_participant != null and reservation == null) try requireCatalogAdmission(a, txn, group, admission.identity);
            const candidate: Record = .{ .identity = admission.identity, .participants = admission.participants };
            try candidate.validate(a);
            try reservations(a, txn, group, candidate, true, &writes, &deletes);
            if (candidate.identity.session) |session| {
                if (try receiptCount(a, txn, group) == max_receipts) return error.SqlPreparedTransactionCapacityExceeded;
                if (try readReceipt(a, txn, group, candidate.identity.operation_id) != null) return error.ExternalTransactionDecisionChanged;
                try changeReceiptCount(a, txn, group, true, &writes, &deletes);
                try writeReceipt(a, group, .{ .owner = try candidate.identity.owner(a), .txn_id = candidate.identity.txn_id, .gid = candidate.identity.gid, .kind = candidate.identity.kind, .session = session, .lock_scope = candidate.identity.lock_scope, .status = .pending }, true, &writes);
            }
            if (admission.identity.kind == .named) try writes.append(a, .{ .key = try gidKey(a, group, admission.identity.gid), .value = try a.dupe(u8, &admission.identity.operation_id) });
            try writes.append(a, .{ .key = try workKey(a, group, admission.identity.operation_id), .value = "" });
            break :blk candidate;
        },
        .advance => |change| blk: {
            const previous = (try readOperation(a, txn, group, change.owner.operation_id)) orelse return error.SqlPreparedTransactionNotFound;
            if (previous.identity.kind == .replication) return error.Forbidden;
            if (previous.identity.kind == .named) {
                const indexed = (try readGid(a, txn, group, previous.identity.gid)) orelse return error.InvalidCatalogRecord;
                if (!std.mem.eql(u8, &indexed.identity.operation_id, &previous.identity.operation_id)) return error.InvalidCatalogRecord;
            }
            if (previous.identity.database_id != request.database_id) return error.SqlPreparedTransactionWrongDatabase;
            if (!request.native_admin and request.actor.principal().kind != .bootstrap and !previous.identity.principal.eql(request.actor.principal())) return error.Forbidden;
            if (change.action == .prepare and previous.identity.catalog_participant != null and
                (previous.phase == .preparing or previous.phase == .locking)) try requireCatalogPrepared(a, txn, group, previous);
            const next = try advance(a, previous, change.expected_revision, change.owner, change.action);
            try appendPublicationAdvance(a, txn, group, previous, next, change.action, &writes);
            if ((next.phase == .prepared and next.identity.kind == .named) or next.phase == .retired) {
                try deletes.append(a, try workKey(a, group, next.identity.operation_id));
            } else if (previous.phase == .prepared and previous.identity.kind == .named) {
                try writes.append(a, .{ .key = try workKey(a, group, next.identity.operation_id), .value = "" });
            }
            // Publication of PREPARED and its API handoff proof are one commit.
            // A later GID resolution cannot erase the original session result.
            const abort_settled = next.decision != null and next.decision.?.status == .aborted and
                (if (next.identity.catalog_participant != null) next.phase == .retired else previous.phase != .resolved and next.phase == .resolved);
            const named_handoff = next.identity.kind == .named and ((!previous.was_prepared and next.was_prepared) or (!next.was_prepared and abort_settled));
            const session_handoff = next.identity.kind == .session_commit and
                ((next.decision != null and next.decision.?.status == .committed and
                    (previous.decision == null or previous.commitProgress() != next.commitProgress())) or abort_settled);
            if (next.identity.session != null and (named_handoff or session_handoff)) {
                var receipt = (try readReceipt(a, txn, group, next.identity.operation_id)) orelse return error.InvalidCatalogRecord;
                if ((receipt.status != .pending and !(receipt.kind == .session_commit and receipt.status == .committed)) or !receipt.owner.eql(change.owner) or !std.mem.eql(u8, &receipt.txn_id, &next.identity.txn_id)) return error.InvalidCatalogRecord;
                if (receipt.kind != next.identity.kind) return error.InvalidCatalogRecord;
                receipt.status = if (next.identity.kind == .session_commit) (if (next.decision.?.status == .committed) .committed else .aborted) else if (next.was_prepared) .prepared else .aborted;
                if (next.identity.kind == .session_commit) {
                    receipt.decision = next.decision;
                    receipt.progress = if (next.decision.?.status == .committed) next.commitProgress() else .ready;
                }
                try writeReceipt(a, group, receipt, false, &writes);
            }
            if (next.phase == .retired) {
                try reservations(a, txn, group, next, false, &writes, &deletes);
                if (next.identity.kind == .named) try deletes.append(a, try gidKey(a, group, next.identity.gid));
                try deletes.append(a, try operationKey(a, group, next.identity.operation_id));
                return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
            }
            break :blk next;
        },
    };
    // Native lock job cursors own intermediate progress. The exact command
    // outcome still commits with each quantum, but its immutable participant
    // manifest is written only when the registry's logical phase changes.
    if (request.operation == .advance and (request.operation.advance.action == .lock_handoff or request.operation.advance.action == .lock_release))
        return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
    try writes.append(a, .{ .key = try operationKey(a, group, record.identity.operation_id), .value = bytes });
    return .{ .writes = writes.items, .deletes = deletes.items, .result = .{ .revision = catalog_revision, .identity = null, .removed = 0 } };
}

pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogMutation;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:sql_prepared:{d}:", .{group});
}
pub const Work = struct { records: []const Record, next_after: ?[16]u8 = null };
pub fn readWork(a: A, txn: anytype, group: u64, after: ?[16]u8) !Work {
    const range_prefix = try prefix(a, group, "work");
    const seek = if (after) |id| try workKey(a, group, id) else range_prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(seek);
    const records = try a.alloc(Record, 8);
    var used: usize = 0;
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, range_prefix)) break;
        if (after != null and std.mem.eql(u8, row.key, seek)) continue;
        if (used == records.len) return .{ .records = records, .next_after = records[used - 1].identity.operation_id };
        if (row.key.len != range_prefix.len + 32 or row.value.len != 0) return error.InvalidCatalogRecord;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, row.key[range_prefix.len..]) catch return error.InvalidCatalogRecord;
        const record = (try readOperation(a, txn, group, id)) orelse return error.InvalidCatalogRecord;
        if ((record.phase == .prepared and record.identity.kind == .named) or record.phase == .retired) return error.InvalidCatalogRecord;
        records[used] = record;
        used += 1;
    }
    return .{ .records = records[0..used] };
}

/// Slots are reserved before any native BEGIN. A lost API reply therefore
/// cannot consume unbounded metadata or make a durable PREPARE lose its proof.
/// Completion requires the receiving session store to confirm durable adoption
/// or canonical absence; wall-clock expiry never fabricates that acknowledgement.
pub const max_receipts = 4096;
pub const Receipt = struct {
    owner: Owner,
    txn_id: [16]u8,
    gid: []const u8,
    session: SessionRef,
    kind: Kind = .named,
    lock_scope: ?@import("lock_authority.zig").Ref = null,
    status: enum { pending, prepared, committed, aborted },
    /// Private COMMIT recovery must retain its exact version and visibility
    /// contract after the metadata job and native coordinator are retired.
    decision: ?Decision = null,
    progress: CommitProgress = .ready,
    pub fn minimumDecoderVersion(self: Receipt) u16 {
        return decoderFloor(self.kind == .session_commit or self.session.standalone, self.lock_scope != null, self.lock_scope != null and self.kind == .named, false);
    }
    pub fn jsonStringify(self: Receipt, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Receipt).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "lock_scope")) {
                if (self.lock_scope != null) {
                    try writer.objectField(field);
                    try writer.write(self.lock_scope);
                }
            } else if (!(comptime std.mem.eql(u8, field, "kind") or std.mem.eql(u8, field, "decision") or std.mem.eql(u8, field, "progress")) or self.kind == .session_commit) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: Receipt) !void {
        try self.owner.validate();
        if (self.lock_scope) |ref| {
            try ref.backend.validate();
            try ref.command.validate();
            if (self.session.connection_id == null or !std.mem.eql(u8, &self.owner.metadata_incarnation, &std.fmt.bytesToHex(ref.backend.incarnation, .lower))) return error.InvalidCatalogRecord;
        }
        try self.session.validate();
        try validateGid(self.gid);
        if ((self.kind == .named and self.status == .committed) or (self.kind == .session_commit and (self.status == .prepared or self.gid.len != 0 or self.session.connection_id == null))) return error.InvalidCatalogRecord;
        if (self.status != .committed and self.progress != .ready) return error.InvalidCatalogRecord;
        if (self.kind == .named and self.decision != null) return error.InvalidCatalogRecord;
        if (self.kind == .session_commit) {
            if ((self.status == .pending) != (self.decision == null)) return error.InvalidCatalogRecord;
            if (self.decision) |decision| {
                if (decision.sync_level == .write and (self.progress == .indexing or self.progress == .repair_required)) return error.InvalidCatalogRecord;
                if (decision.version == 0 or (self.status == .committed) != (decision.status == .committed) or (decision.status == .committed and decision.sync_level == .propose)) return error.InvalidCatalogRecord;
            }
        }
        if (std.mem.allEqual(u8, &self.txn_id, 0)) return error.InvalidCatalogRecord;
    }
};
pub fn receiptCount(a: A, txn: anytype, group: u64) !u64 {
    const count = try number(txn, try countKey(a, group, "receipt_active", 0));
    if (count > max_receipts) return error.InvalidCatalogRecord;
    return count;
}
fn receiptKey(a: A, group: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try prefix(a, group, "receipt"), &std.fmt.bytesToHex(id, .lower) });
}
fn receiptNodePrefix(a: A, group: u64, node: u64) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{d}:", .{ try prefix(a, group, "receipt_node"), node });
}
fn receiptNodeKey(a: A, group: u64, node: u64, id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try receiptNodePrefix(a, group, node), &std.fmt.bytesToHex(id, .lower) });
}
fn changeReceiptCount(a: A, txn: anytype, group: u64, adding: bool, writes: *std.ArrayListUnmanaged(Write), deletes: *std.ArrayListUnmanaged([]const u8)) !void {
    const key = try countKey(a, group, "receipt_active", 0);
    const old = try number(txn, key);
    if (old > max_receipts or (adding and old == max_receipts) or (!adding and old == 0)) return error.InvalidCatalogRecord;
    const next = if (adding) old + 1 else old - 1;
    if (next == 0) return deletes.append(a, key);
    const bytes = try a.alloc(u8, 8);
    std.mem.writeInt(u64, bytes[0..8], next, .big);
    try writes.append(a, .{ .key = key, .value = bytes });
}
fn writeReceipt(a: A, group: u64, receipt: Receipt, initial: bool, writes: *std.ArrayListUnmanaged(Write)) !void {
    try receipt.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    try writes.append(a, .{ .key = try receiptKey(a, group, receipt.owner.operation_id), .value = bytes });
    // This membership is immutable and readReceipt already verifies it on
    // transitions. Do not rewrite its LSM key for every completion update.
    if (initial) try writes.append(a, .{ .key = try receiptNodeKey(a, group, receipt.session.owner_node_id, receipt.owner.operation_id), .value = "1" });
}
pub fn readReceipt(a: A, txn: anytype, group: u64, id: [16]u8) !?Receipt {
    const bytes = (try optional(txn, try receiptKey(a, group, id))) orelse return null;
    if (bytes.len > 4096) return error.InvalidCatalogRecord;
    const receipt = try std.json.parseFromSliceLeaky(Receipt, a, bytes, .{ .allocate = .alloc_always });
    try receipt.validate();
    if (!std.mem.eql(u8, &receipt.owner.operation_id, &id)) return error.InvalidCatalogRecord;
    const node = (try optional(txn, try receiptNodeKey(a, group, receipt.session.owner_node_id, id))) orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, node, "1")) return error.InvalidCatalogRecord;
    return receipt;
}
pub const ReceiptWork = struct { receipts: []const Receipt, next_after: ?[16]u8 = null };
pub fn readReceiptWork(a: A, txn: anytype, group: u64, node: ?u64, after: ?[16]u8) !ReceiptWork {
    const range_prefix = if (node) |id| try receiptNodePrefix(a, group, id) else try prefix(a, group, "receipt");
    const seek = if (after) |id| if (node) |node_id| try receiptNodeKey(a, group, node_id, id) else try receiptKey(a, group, id) else range_prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(seek);
    const receipts = try a.alloc(Receipt, 8);
    var used: usize = 0;
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, range_prefix)) break;
        if (after != null and std.mem.eql(u8, row.key, seek)) continue;
        if (used == receipts.len) return .{ .receipts = receipts, .next_after = receipts[used - 1].owner.operation_id };
        if (row.key.len != range_prefix.len + 32) return error.InvalidCatalogRecord;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, row.key[range_prefix.len..]) catch return error.InvalidCatalogRecord;
        const receipt = (try readReceipt(a, txn, group, id)) orelse return error.InvalidCatalogRecord;
        if (node) |node_id| if (receipt.session.owner_node_id != node_id or !std.mem.eql(u8, row.value, "1")) return error.InvalidCatalogRecord;
        receipts[used] = receipt;
        used += 1;
    }
    return .{ .receipts = receipts[0..used] };
}

test "SQL prepared transaction session realms preserve clustered bytes and fence standalone sentinels" {
    const a = std.testing.allocator;
    const clustered: SessionRef = .{ .owner_node_id = 9 };
    const legacy = try std.json.Stringify.valueAlloc(a, clustered, .{});
    defer a.free(legacy);
    try std.testing.expectEqualStrings("{\"owner_node_id\":9,\"connection_id\":null}", legacy);
    try clustered.requireRealm(false);
    try std.testing.expectError(error.CatalogGenerationChanged, clustered.requireRealm(true));
    const standalone_ref: SessionRef = .{ .owner_node_id = 0, .connection_id = @splat('a'), .standalone = true };
    try standalone_ref.requireRealm(true);
    try std.testing.expectError(error.CatalogGenerationChanged, standalone_ref.requireRealm(false));
    try std.testing.expectError(error.InvalidCatalogMutation, (SessionRef{ .owner_node_id = 0 }).validate());
    try std.testing.expectError(error.InvalidCatalogMutation, (SessionRef{ .owner_node_id = 9, .standalone = true }).validate());
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const identity: Identity = .{ .kind = .session_commit, .session = standalone_ref, .gid = "", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(2), .operation_id = @splat(3), .metadata_incarnation = @splat('1'), .begin_timestamp = 10, .expires_at_unix_ns = 1000, .plan_digest = try planDigest("[]"), .manifest_digest = try manifestDigest(owned, &.{}, @splat('1')) };
    const request: Mutation = .{ .admission = .{ .nonce = identity.operation_id, .head = 0 }, .incarnation = @splat(0x11), .actor = .{ .session_user = .bootstrap, .credential = "" }, .standalone = true, .database_id = 1, .operation = .{ .admit = .{ .identity = identity, .participants = &.{}, .admitted_at_ns = 20 } } };
    try request.validate();
    var wrong = request;
    wrong.standalone = false;
    try std.testing.expectError(error.CatalogGenerationChanged, wrong.validate());
    wrong = request;
    wrong.operation.admit.identity.session = .{ .owner_node_id = 9, .connection_id = @splat('a') };
    try std.testing.expectError(error.CatalogGenerationChanged, wrong.validate());
    wrong.standalone = false;
    try wrong.validate();
    try std.testing.expect(!(try identity.owner(owned)).eql(try wrong.operation.admit.identity.owner(owned)));
    const encoded = try std.json.Stringify.valueAlloc(owned, request, .{});
    const decoded = try std.json.parseFromSliceLeaky(Mutation, owned, encoded, .{});
    try decoded.validate();
    try std.testing.expectEqualDeep(request, decoded);
}

test "SQL prepared transaction receipt work uses an explicit standalone realm without changing clustered selectors" {
    const a = std.testing.allocator;
    const clustered: Read = .{ .receipt_work = .{ .owner_node_id = 9 } };
    const encoded = try std.json.Stringify.valueAlloc(a, clustered, .{});
    defer a.free(encoded);
    try std.testing.expectEqualStrings("{\"receipt_work\":{\"owner_node_id\":9,\"after\":null}}", encoded);
    try clustered.validate();
    try (Read{ .receipt_work = .{ .owner_node_id = 0, .standalone = true } }).validate();
    try std.testing.expectError(error.InvalidCatalogMutation, (Read{ .receipt_work = .{ .owner_node_id = 0 } }).validate());
    try std.testing.expectError(error.InvalidCatalogMutation, (Read{ .receipt_work = .{ .standalone = true } }).validate());
    try std.testing.expectError(error.InvalidCatalogMutation, (Read{ .receipt_work = .{ .owner_node_id = 9, .standalone = true } }).validate());
}

test "prepared history requires a durable certificate before handoff" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const epoch: [32]u8 = @splat('1');
    const participants = [_]Participant{.{
        .table_name = "docs",
        .route = .{
            .metadata_group_id = 1,
            .metadata_incarnation = epoch,
            .catalog_revision = 2,
            .table_id = 3,
            .topology_epoch = 1,
            .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 3, .shard_id = 7, .range_id = 8 } },
        },
    }};
    const routed: history.RoutedOwner = .{
        .metadata_group_id = 1,
        .metadata_incarnation = epoch,
        .namespace = local.storage_db_online_source_contract.namespaceBytes(.{ .table_id = 3, .shard_id = 7, .range_id = 8 }),
        .table_binding = @splat(2),
        .root_generation = 0,
        .group_id = 7,
        .topology_epoch = 1,
    };
    const members = [_]history.Participant{.{
        .namespace = routed.namespace,
        .owner_fence = routed.digest(),
        .roles = history.Roles.effects,
        .locator = try local.api_local_transaction_contract.participantIdForGroup(a, "docs", 7),
        .input_digest = @splat(4),
        .routed_owner = routed,
    }};
    const identity: Identity = .{
        .gid = "prepared_history",
        .database_id = 1,
        .principal = .bootstrap,
        .txn_id = @splat(2),
        .operation_id = @splat(3),
        .metadata_incarnation = epoch,
        .begin_timestamp = 10,
        .expires_at_unix_ns = 1000,
        .plan_digest = try planDigest("[]"),
        .manifest_digest = try manifestDigest(a, &participants, epoch),
        .history_completion = .{
            .version = 3,
            .transaction_id = @splat(2),
            .admission_digest = @splat(6),
            .coordinator_namespace = routed.namespace,
            .coordinator_fence = routed.digest(),
            .participants = &members,
        },
    };
    const owner = try identity.owner(a);
    var record: Record = .{ .identity = identity, .participants = &participants, .phase = .prepared, .was_prepared = true, .votes = 1 };
    record = try advance(a, record, record.revision, owner, .{ .commit = .{ .version = 20 } });
    record = try advance(a, record, record.revision, owner, .{ .activate = 1 });
    record = try advance(a, record, record.revision, owner, .{ .resolve = 1 });
    try std.testing.expectEqual(CommitProgress.resolving, record.commitProgress());
    try std.testing.expectError(error.CatalogPublicationProofPending, advance(a, record, record.revision, owner, .handoff));
    const certificate: history.Digest = @splat(7);
    record = try advance(a, record, record.revision, owner, .{ .history_published = certificate });
    const encoded = try std.json.Stringify.valueAlloc(a, record, .{});
    record = try std.json.parseFromSliceLeaky(Record, a, encoded, .{});
    try std.testing.expectEqualDeep(certificate, record.history_certificate.?);
    try std.testing.expectEqual(CommitProgress.resolving, record.commitProgress());
    try std.testing.expectError(error.CatalogPublicationProofPending, advance(a, record, record.revision, owner, .handoff));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, advance(a, record, record.revision, owner, .{ .history_published = @splat(8) }));
    record = try advance(a, record, record.revision, owner, .{ .history_released = 1 });
    try std.testing.expectEqual(CommitProgress.ready, record.commitProgress());
    record = try advance(a, record, record.revision, owner, .handoff);
    try std.testing.expectEqual(Phase.retired, record.phase);
}

test "SQL prepared native retirement preserves undecided identity and separates loss facts from global commit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const epoch: [32]u8 = @splat('1');
    const route: local.metadata_catalog_route_contract.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = epoch, .catalog_revision = 2, .table_id = 3, .topology_epoch = 1, .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 3, .shard_id = 7, .range_id = 8 }, .execution = .{ .native_local = .{ .kind = .unlogged, .owner_node_id = 5, .owner_store_id = 6, .generation_instance = @splat(1), .data_generation = 2 } } }, .native_participant = .{ .table_id = 3, .shard_id = 7, .range_id = 8, .physical_root_incarnation = 9, .data_generation = 2 } };
    var permanent = route;
    permanent.table_id = 4;
    permanent.route.group_id = 10;
    permanent.route.range_id = 11;
    permanent.route.identity_namespace = .{ .table_id = 4, .shard_id = 10, .range_id = 11 };
    permanent.route.execution = .raft;
    permanent.native_participant = null;
    const parts = [_]Participant{ .{ .table_name = "u", .route = route }, .{ .table_name = "p", .route = permanent } };
    const identity: Identity = .{ .gid = "survives_crash", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(2), .operation_id = @splat(3), .metadata_incarnation = epoch, .begin_timestamp = 10, .expires_at_unix_ns = 1000, .plan_digest = try planDigest("[]"), .manifest_digest = try manifestDigest(a, &parts, epoch) };
    var record: Record = .{ .identity = identity, .participants = &parts, .phase = .prepared, .was_prepared = true, .votes = 3 };
    const owner = try identity.owner(a);
    try record.validate(a);
    const ordinary = try std.json.Stringify.valueAlloc(a, record, .{});
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "native_retirements") == null);
    try std.testing.expectError(error.InvalidCatalogMutation, advance(a, record, record.revision, owner, .{ .native_retired = &.{.{ .member = 1, .digest = @splat(4) }} }));
    record = try advance(a, record, record.revision, owner, .{ .native_retired = &.{.{ .member = 0, .digest = @splat(4) }} });
    try std.testing.expect(record.decision == null and record.phase == .prepared and record.activations == 0 and record.resolutions == 0);
    try std.testing.expectEqual(@as(u64, 1), record.nativeRetirementMask());
    try std.testing.expect(recoveryDecision(record, 2000) == null); // Named PREPARED never expires into ABORT.
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{});
    try record.validate(a);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, advance(a, record, record.revision, owner, .{ .native_retired = &.{.{ .member = 0, .digest = @splat(5) }} }));
    record = try advance(a, record, record.revision, owner, .{ .commit = .{ .version = 20 } });
    try std.testing.expect(record.decision.?.status == .committed and record.phase == .decided);
    record = try advance(a, record, record.revision, owner, .{ .activate = 3 });
    record = try advance(a, record, record.revision, owner, .{ .resolve = 3 });
    record = try advance(a, record, record.revision, owner, .handoff);
    try std.testing.expect(record.phase == .retired and record.decision.?.status == .committed and record.nativeRetirementMask() == 1);
}

test "subscription native transaction admission has separate recovery custody" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const participants: []const Participant = &.{};
    const manifest = try manifestDigest(a, participants, incarnation);
    const id: [16]u8 = @splat(5);
    const event: ReplicationEvent = .{ .kind = .initial_copy, .source_point = 11, .end_point = 11, .changes_digest = @splat(7) };
    const source: ReplicationReference = .{
        .owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } },
        .birth = 3,
        .generation = 1,
        .source_digest = @splat(6),
        .event_digest = try event.eventDigest(),
        .intent_digest = replicationIntentDigest(&.{}),
        .manifest_digest = manifest,
        .txn_id = id,
    };
    const identity: Identity = .{
        .kind = .replication,
        .replication_source = source,
        .gid = "",
        .database_id = 1,
        .principal = .bootstrap,
        .txn_id = id,
        .operation_id = id,
        .metadata_incarnation = incarnation,
        .begin_timestamp = 10,
        .expires_at_unix_ns = 100,
        .plan_digest = try planDigest("[]"),
        .manifest_digest = manifest,
    };
    const admission: AdmissionInput = .{ .identity = identity, .participants = participants, .admitted_at_ns = 20 };
    var bytes: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&bytes, &incarnation);
    const request: Mutation = .{
        .admission = .{ .nonce = id, .head = 0 },
        .incarnation = bytes,
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .database_id = 1,
        .operation = .{ .admit = admission },
    };
    try std.testing.expectError(error.Forbidden, request.validate());
    var wrong = identity;
    wrong.replication_source.?.txn_id[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogMutation, wrong.validate());
    wrong = identity;
    wrong.replication_source.?.event_digest[0] ^= 1;
    try std.testing.expect(!(try identity.owner(a)).eql(try wrong.owner(a)));

    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-prepare", .{temporary.sub_path}, 0);
    {
        var store = try local.storage_docstore.DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const planned = try planReplicationAdmission(a, &txn, 1, 1, admission, event, &.{}, null, .{ .source_point = event.source_point, .total_rows = 0, .capture_digest = @splat(7) });
        try planned.apply(&txn);
        const admitted = (try readOperation(a, &txn, 1, id)).?;
        try std.testing.expectEqualDeep(source, admitted.identity.replication_source.?);
        try std.testing.expectEqualDeep(source, (try nextReplicationRecovery(a, &txn, 1, null)).?);
        try std.testing.expect((try nextReplicationRecovery(a, &txn, 1, id)) == null);
        try std.testing.expectEqual(@as(u64, 1), try kindCount(a, &txn, 1, .replication));
        try std.testing.expectEqual(@as(u64, 0), try kindCount(a, &txn, 1, .session_commit));
        try std.testing.expect((try optional(&txn, try workKey(a, 1, id))) == null);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, planReplicationAdmission(a, &txn, 1, 1, admission, event, &.{}, null, .{ .source_point = event.source_point, .total_rows = 0, .capture_digest = @splat(7) }));
        try txn.commit();
    }
    var store = try local.storage_docstore.DocStore.open(std.testing.allocator, path, .{});
    defer store.close();
    const Verifier = struct {
        pub fn requireAcknowledgedRoot(_: @This(), _: anytype, _: anytype, _: anytype) !void {
            return error.TestUnexpectedResult;
        }
    };
    const owner = try identity.owner(a);
    {
        var abort_recovery = try store.beginWriteTxn();
        defer abort_recovery.abort();
        const live = (try readOperation(a, &abort_recovery, 1, id)).?;
        const aborted = try planReplicationAdvance(a, &abort_recovery, 1, 1, source, owner, live.revision, .{ .abort = 40 });
        try aborted.plan.apply(&abort_recovery);
        const handoff = try planReplicationCheckpoint(a, &abort_recovery, 1, 1, source, owner, aborted.record.revision, &.{}, Verifier{});
        try handoff.apply(&abort_recovery);
        const progress = (try readReplicationProgress(a, &abort_recovery, 1, source.owner)).?;
        try std.testing.expect(progress.pending == null and progress.last_acknowledged == null);
        try std.testing.expectEqual(@as(u64, 0), progress.checkpoint);
        try std.testing.expect(!try allowReplicationCheckpoint(a, &abort_recovery, 1, source, event.end_point));
        try std.testing.expect((try readOperation(a, &abort_recovery, 1, id)) == null);
    }
    var recovery = try store.beginWriteTxn();
    defer recovery.abort();
    const live = (try readOperation(a, &recovery, 1, id)).?;
    try std.testing.expectEqualDeep(source, (try nextReplicationRecovery(a, &recovery, 1, null)).?);
    try std.testing.expectEqual(.preparing, live.phase);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, planReplicationAdvance(a, &recovery, 1, 1, wrong.replication_source.?, owner, live.revision, .prepare));
    const prepared = try planReplicationAdvance(a, &recovery, 1, 1, source, owner, live.revision, .prepare);
    try prepared.plan.apply(&recovery);
    const commit: Action = .{ .commit = .{ .version = 40 } };
    try std.testing.expectError(error.CatalogGenerationChanged, planReplicationAdvance(a, &recovery, 1, 1, source, owner, live.revision, commit));
    const decided = try planReplicationAdvance(a, &recovery, 1, 1, source, owner, prepared.record.revision, commit);
    try decided.plan.apply(&recovery);
    try std.testing.expectEqual(.resolved, decided.record.phase);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planReplicationAdvance(a, &recovery, 1, 1, source, owner, decided.record.revision, .handoff));
    const retired = try planReplicationCheckpoint(a, &recovery, 1, 1, source, owner, decided.record.revision, &.{}, Verifier{});
    try retired.apply(&recovery);
    try std.testing.expect((try nextReplicationRecovery(a, &recovery, 1, null)) == null);
    const verified = (try readReplicationProgress(a, &recovery, 1, source.owner)).?;
    try std.testing.expectEqual(event.end_point, verified.checkpoint);
    try std.testing.expect((try readOperation(a, &recovery, 1, id)) == null);
    try std.testing.expectEqual(@as(u64, 0), try kindCount(a, &recovery, 1, .replication));
    var wal = admission;
    wal.identity.txn_id = @splat(8);
    wal.identity.operation_id = wal.identity.txn_id;
    wal.identity.replication_source.?.txn_id = wal.identity.txn_id;
    wal.identity.replication_source.?.generation = 2;
    const checkpoint = verified.checkpoint;
    for ([_]u64{ checkpoint - 1, checkpoint + 1, checkpoint }) |start| {
        const selected: ReplicationEvent = .{
            .kind = .transaction,
            .source_point = start,
            .end_point = checkpoint + 2,
            .changes_digest = @splat(9),
        };
        wal.identity.replication_source.?.event_digest = try selected.eventDigest();
        const wal_plan = planReplicationAdmission(a, &recovery, 1, 1, wal, selected, &.{}, null, null);
        if (start == checkpoint) {
            try (try wal_plan).apply(&recovery);
        } else try std.testing.expectError(error.ReplicationCutoverAuthorityLost, wal_plan);
    }
    const pending = (try readReplicationProgress(a, &recovery, 1, source.owner)).?;
    try std.testing.expectEqual(checkpoint, pending.checkpoint);
    try std.testing.expect(pending.pending.?.eql(wal.identity.replication_source.?));
}

test "subscription native transaction admission rejoins exact participant permits" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const namespace: @FieldType(local.api_replication_source_apply.Permit, "namespace") = .{ .table_id = 3, .shard_id = 7, .range_id = 8 };
    const part: Participant = .{
        .table_name = "records",
        .schema_version = 2,
        .route = .{
            .metadata_group_id = 1,
            .metadata_incarnation = incarnation,
            .catalog_revision = 2,
            .table_id = 3,
            .topology_epoch = 5,
            .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = namespace.table_id, .shard_id = namespace.shard_id, .range_id = namespace.range_id }, .execution = .raft },
        },
    };
    const event: ReplicationEvent = .{ .kind = .initial_copy, .source_point = 11, .end_point = 11, .changes_digest = @splat(7) };
    var intent: local.storage_db_types.TransactionIntentRequest = .{
        .schema_version = 2,
        .writes = &.{.{ .key = "\xffrow", .value = "{\"v\":1}" }},
        .predicates = &.{.{ .key = "\xffrow", .expected_version = 0, .unique_absence = true, .row_policy_requirement = .insert }},
    };
    const payload = @import("replication/payload.zig");
    var artifact_directory = try local.common_test_directory.TestDirectory.init("subscription-intent-recovery");
    defer artifact_directory.cleanup();
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(std.testing.allocator, artifact_directory.path());
    defer artifact_fs.deinit();
    var artifacts = artifact_fs.artifactStore();
    const admission = try @import("../api/replication_subscription_admission.zig").seal(a, &artifacts, .{
        .metadata_group = 1,
        .incarnation = incarnation,
        .owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } },
        .birth = 3,
        .generation = 1,
        .origin = @splat(6),
        .txn_id = @splat(9),
        .recipe_digest = @splat(7),
        .upload_token = 1,
        .begin_timestamp = 10,
        .admitted_at_ns = 20,
        .expires_at_ns = 100,
        .event = event,
    }, &.{.{ .participant = part, .intent = intent, .schema_digest = @splat(10) }}, .none);
    const identity = admission.input.identity;
    const source = identity.replication_source.?;
    const permit = admission.permits[0];
    const intents = admission.intents;
    const upload_scope = try payload.uploadScope(source, 1, incarnation, 1, source.txn_id);
    intent.external_decision_owner = permit.decision_owner;
    const record: Record = .{ .identity = identity, .participants = &.{part}, .replication_event = event, .replication_intents = intents, .replication_permits = &.{permit} };
    try record.validate(a);
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-permit", .{temporary.sub_path}, 0);
    {
        var store = try local.storage_docstore.DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const input = admission.input;
        try std.testing.expectError(error.InvalidCatalogRecord, planReplicationAdmission(a, &txn, 1, 1, input, event, &.{}, intents, .{ .source_point = event.source_point, .total_rows = 0, .capture_digest = @splat(7) }));
        var changed = permit;
        changed.pregrant_body_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidCatalogMutation, planReplicationAdmission(a, &txn, 1, 1, input, event, &.{changed}, intents, .{ .source_point = event.source_point, .total_rows = 0, .capture_digest = @splat(7) }));
        changed = permit;
        changed.expected_image = .{ .physical_root_incarnation = 123, .target_custody_digest = permit.target_custody_digest };
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planReplicationAdmission(a, &txn, 1, 1, input, event, &.{changed}, intents, .{ .source_point = event.source_point, .total_rows = 0, .capture_digest = @splat(7) }));
        const plan_value = try planReplicationAdmission(a, &txn, 1, 1, input, event, &.{permit}, intents, .{ .source_point = event.source_point, .total_rows = 0, .capture_digest = @splat(7) });
        try plan_value.apply(&txn);
        try txn.commit();
    }
    var store = try local.storage_docstore.DocStore.open(std.testing.allocator, path, .{});
    defer store.close();
    var txn = try store.beginWriteTxn();
    defer txn.abort();
    const retained = (try readOperation(a, &txn, 1, identity.operation_id)).?;
    const frozen = retained.replication_intents.?;
    const decoded_input = try payload.readIntent(a, &artifacts, frozen.members[0], upload_scope, permit, .none);
    defer decoded_input.deinit();
    var replay_input = decoded_input.value.intent;
    replay_input.external_decision_owner = permit.decision_owner;
    replay_input.replication_source_permit = permit;
    try retained.requireReplicationIntent(a, 1, permit, replay_input);
    try std.testing.expectEqualStrings("\xffrow", replay_input.writes[0].key);
    try std.testing.expectEqual(.revision_v1, replay_input.document_version_scheme);
    try std.testing.expectEqual(.revision_v1, replay_input.asBatch(source.txn_id, permit.topology_epoch).admittedDocumentVersionScheme(true));
    intent.document_version_scheme = .revision_v1;
    try std.testing.expect(try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 21, .prepare));
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 2, incarnation, permit, "records", 21, .prepare));
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, .prepare));
    try retained.requireReplicationIntent(a, 1, permit, intent);
    var forged = permit;
    forged.target_custody_digest[0] ^= 1;
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, forged, "records", 21, .prepare));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, retained.requireRetainedReplicationPermit(a, 1, forged));
    try record.requireReplicationIntent(a, 1, permit, intent);
    const batch_codec = local.api_internal_batch;
    var batch = intent.asBatch(source.txn_id, permit.topology_epoch);
    batch.transaction.?.prepare.replication_source_permit = permit;
    const encoded = try batch_codec.encodeBatchRequest(a, batch);
    var decoded = try batch_codec.parseInternalBatchRequest(a, encoded);
    defer decoded.deinit(a);
    try std.testing.expectEqualDeep(permit, decoded.req.transaction.?.prepare.replication_source_permit.?);
    try retained.requireReplicationIntent(a, 1, permit, try local.storage_db_types.TransactionIntentRequest.fromBatch(decoded.req));
    {
        var activation = (local.storage_db_types.TransactionIntentRequest{
            .external_activation = true,
            .external_decision_owner = permit.decision_owner,
            .replication_source_permit = permit,
        }).asBatch(source.txn_id, permit.topology_epoch);
        const wire = try batch_codec.encodeBatchRequest(a, activation);
        var restored = try batch_codec.parseInternalBatchRequest(a, wire);
        defer restored.deinit(a);
        try std.testing.expectEqualDeep(activation, restored.req);
        activation.writes = intent.writes;
        try std.testing.expectError(error.InvalidBatchRequest, batch_codec.encodeBatchRequest(a, activation));
    }
    {
        var resolve: local.storage_db_types.BatchRequest = .{ .timestamp_ns = 40, .transaction = .{ .resolve = .{
            .txn_id = source.txn_id,
            .status = .committed,
            .commit_version = 30,
            .external_decision_owner = permit.decision_owner,
            .replication_source_permit = permit,
        } } };
        const wire = try batch_codec.encodeBatchRequest(a, resolve);
        var restored = try batch_codec.parseInternalBatchRequest(a, wire);
        defer restored.deinit(a);
        try std.testing.expectEqualDeep(resolve, restored.req);
        resolve.transaction.?.resolve.txn_id[0] ^= 1;
        try std.testing.expectError(error.InvalidBatchRequest, batch_codec.encodeBatchRequest(a, resolve));
    }
    batch.transaction.?.prepare.topology_epoch += 1;
    try std.testing.expectError(error.InvalidBatchRequest, batch_codec.encodeBatchRequest(a, batch));
    intent.writes = &.{.{ .key = "row", .value = "{\"v\":2}" }};
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, record.requireReplicationIntent(a, 1, permit, intent));
    try record.requireReplicationPermit(a, 1, permit);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, record.requireReplicationPermit(a, 2, permit));
    var wrong = permit;
    wrong.namespace.range_id += 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, record.requireReplicationPermit(a, 1, wrong));
    wrong = permit;
    wrong.schema_version += 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, record.requireReplicationPermit(a, 1, wrong));
    wrong = permit;
    wrong.reference.generation += 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, record.requireReplicationPermit(a, 1, wrong));
    const terminal: ReplicationPhase = .{ .resolve = .{ .status = .committed, .version = 30 } };
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 21, .activate));
    const attestation = @import("../api/replication_source_apply_attestation.zig");
    const root = @import("../metadata/store_root_attestation.zig");
    const Verifier = struct {
        allowed: bool,
        pub fn requirePublicationAction(_: @This(), _: A, _: Record, _: Action) !void {
            return error.ReplicationCutoverAuthorityLost;
        }
        pub fn requireTerminalRoot(self: @This(), scope: root.OwnerScope, proof: attestation.Proof, position: @FieldType(local.api_replication_source_apply.ApplyReceipt, "position")) !void {
            if (!self.allowed or scope.group_id != 7 or scope.authority != .raft or proof.authority.raft != 3 or position.raft.index != 40)
                return error.ReplicationCutoverAuthorityLost;
        }
        pub fn requireAcknowledgedRoot(self: @This(), scope: root.OwnerScope, proof: attestation.AckProof, position: @FieldType(local.api_replication_source_apply.ApplyReceipt, "position")) !void {
            if (!self.allowed or scope.group_id != 7 or scope.authority != .raft or proof.authority.raft != 3 or position.raft.index != 41)
                return error.ReplicationCutoverAuthorityLost;
        }
    };
    const seed: [32]u8 = @splat(20);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const receipt = try (attestation.Receipt{
        .value = .{ .stage = .applied, .permit = permit, .image = .{
            .physical_root_incarnation = 123,
            .target_custody_digest = permit.target_custody_digest,
        }, .position = .{ .raft = .{ .term = 1, .index = 40 } }, .marker_digest = @splat(17) },
        .proof = .{
            .identity = .{ .metadata_incarnation = incarnation, .node_id = 1, .store_id = 2, .root_incarnation = 3, .public_key = pair.public_key.toBytes() },
            .reporter_incarnation = 1,
            .root_generation = 1,
            .authority = .{ .raft = 3 },
            .signature = undefined,
        },
    }).sign(.{ .root_incarnation = 3, .seed = seed, .public_key = pair.public_key.toBytes() }, a);
    var prepared_receipt = receipt;
    prepared_receipt.value.stage = .prepared;
    prepared_receipt = try prepared_receipt.sign(.{ .root_incarnation = 3, .seed = seed, .public_key = pair.public_key.toBytes() }, a);
    const owner = permit.decision_owner;
    const verifier: Verifier = .{ .allowed = true };
    const vote: ReplicationAdvance = .{ .vote = .{ .receipt = prepared_receipt } };
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, retained.revision, retained, .{ .vote = .{ .receipt = receipt } }, verifier));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, retained.revision, retained, vote, Verifier{ .allowed = false }));
    try std.testing.expectError(error.InvalidPreparedTransactionPhase, planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, retained.revision, retained, .prepare, verifier));
    const voted = try planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, retained.revision, retained, vote, verifier);
    try voted.plan.apply(&txn);
    const actions: []const ReplicationAdvance = &.{ .prepare, .{ .commit = .{ .version = 30 } } };
    var current = voted.record;
    for (actions) |action| {
        const planned = try planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, current.revision, current, action, verifier);
        try planned.plan.apply(&txn);
        current = planned.record;
    }
    try std.testing.expect(try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, .activate));
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 21, terminal));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, current.revision, current, .{ .activate = prepared_receipt }, verifier));
    try std.testing.expectError(error.InvalidPreparedTransactionPhase, planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, current.revision, current, .{ .resolve = receipt }, verifier));
    var activation_receipt = prepared_receipt;
    activation_receipt.value.stage = .activated;
    activation_receipt = try activation_receipt.sign(.{ .root_incarnation = 3, .seed = seed, .public_key = pair.public_key.toBytes() }, a);
    const activated = try planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, current.revision, current, .{ .activate = activation_receipt }, verifier);
    try activated.plan.apply(&txn);
    try std.testing.expect(try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, terminal));
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, .{ .resolve = .{ .status = .committed, .version = 31 } }));
    const resolved = try planVerifiedReplicationAdvance(a, &txn, 1, 1, source, owner, activated.record.revision, activated.record, .{ .resolve = receipt }, verifier);
    try resolved.plan.apply(&txn);
    const ids: []const []const u8 = &.{try replicationParticipantId(a, resolved.record, 0)};
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planReplicationAcknowledgements(a, 1, 1, source, permit.decision_owner, resolved.record.revision, resolved.record, &.{receipt}, Verifier{ .allowed = false }));
    const ack_plan = try planReplicationAcknowledgements(a, 1, 1, source, permit.decision_owner, resolved.record.revision, resolved.record, &.{receipt}, Verifier{ .allowed = true });
    var completion = ack_plan.completions[0];
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, .{ .acknowledge = completion }));
    try ack_plan.plan.apply(&txn);
    try txn.commit();
    txn = try store.beginWriteTxn();
    const recovered = (try readReplicationRecovery(a, &txn, 1, source.owner, source.birth, source.source_digest)).?;
    try std.testing.expectEqualDeep(source, recovered.progress.pending.?);
    try std.testing.expectEqualDeep(completion, (try replicationAckForMember(a, recovered.transaction.?, 0)).?);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readReplicationRecovery(a, &txn, 1, source.owner, source.birth + 1, source.source_digest));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readReplicationRecovery(a, &txn, 1, source.owner, source.birth, @splat(99)));
    try std.testing.expect(try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, .{ .acknowledge = completion }));
    var ack_command: local.storage_db_types.BatchRequest = .{ .timestamp_ns = 41, .transaction = .{ .acknowledge_many = .{
        .txn_id = source.txn_id,
        .external_decision_owner = permit.decision_owner,
        .participants = ids,
        .replication_source_ack = completion,
    } } };
    const ack_wire = try batch_codec.encodeBatchRequest(a, ack_command);
    var ack_decoded = try batch_codec.parseInternalBatchRequest(a, ack_wire);
    defer ack_decoded.deinit(a);
    try std.testing.expectEqualDeep(ack_command, ack_decoded.req);
    ack_command.transaction.?.acknowledge_many.participants = &.{"another-owner"};
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, batch_codec.encodeBatchRequest(a, ack_command));
    const physical = local.storage_db_replication_source_apply_store;
    const prepared = try physical.stagePrepared(a, &txn, .{ .permit = permit, .physical = .{
        .physical_root_incarnation = 123,
        .namespace = permit.namespace,
        .schema_version = permit.schema_version,
        .schema_digest = permit.schema_digest,
    } }, .{ .raft = .{ .term = 1, .index = 39 } }, source.txn_id, 1, &.{});
    _ = try physical.stageApplied(a, &txn, permit, 123, prepared.intent_revision, .{ .raft = .{ .term = 1, .index = 40 } });
    const ack = try physical.stageAcknowledged(a, &txn, completion, 123, .{ .raft = .{ .term = 1, .index = 41 } });
    const signer: local.storage_db_root_signing_identity.State = .{ .root_incarnation = 3, .seed = seed, .public_key = pair.public_key.toBytes() };
    const signed = try (attestation.Acknowledged{ .value = ack, .proof = .{
        .identity = receipt.proof.identity,
        .reporter_incarnation = 1,
        .root_generation = 1,
        .authority = .{ .raft = 3 },
        .signature = undefined,
    } }).sign(signer, a);
    const acked_record = (try readOperation(a, &txn, 1, identity.operation_id)).?;
    _ = try verifyReplicationAcknowledged(a, acked_record, &.{signed}, Verifier{ .allowed = true });
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, verifyReplicationAcknowledged(a, acked_record, &.{}, Verifier{ .allowed = true }));
    var forged_ack = signed;
    forged_ack.proof.signature = receipt.proof.signature;
    try std.testing.expectError(error.InvalidGenerationPublication, verifyReplicationAcknowledged(a, acked_record, &.{forged_ack}, Verifier{ .allowed = true }));
    completion.terminal_receipts_digest[0] ^= 1;
    try std.testing.expect(!try allowReplicationPhase(a, &txn, 1, incarnation, permit, "records", 100, .{ .acknowledge = completion }));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, planReplicationCheckpoint(a, &txn, 1, 1, source, permit.decision_owner, acked_record.revision, &.{}, Verifier{ .allowed = true }));
    const checkpoint_plan = try planReplicationCheckpoint(a, &txn, 1, 1, source, permit.decision_owner, acked_record.revision, &.{signed}, Verifier{ .allowed = true });
    try checkpoint_plan.apply(&txn);
    try txn.commit();
    txn = try store.beginWriteTxn();
    const target = (try readReplicationTarget(a, &txn, 1, permit.target_custody_digest)).?;
    try std.testing.expectEqualDeep(ack.image, target.image);
    try std.testing.expectEqual(ack.terminal_marker_digest, target.marker.?);
    const progress = (try readReplicationProgress(a, &txn, 1, source.owner)).?;
    try std.testing.expect(try allowReplicationCheckpoint(a, &txn, 1, source, event.end_point));
    try std.testing.expect(!try allowReplicationCheckpoint(a, &txn, 1, source, event.end_point + 1));
    var stale_reference = source;
    stale_reference.birth += 1;
    try std.testing.expect(!try allowReplicationCheckpoint(a, &txn, 1, stale_reference, event.end_point));
    try std.testing.expectEqual(event.end_point, progress.checkpoint);
    try std.testing.expect(progress.pending == null);
    try std.testing.expect(progress.last_acknowledged.?.eql(source));
    try std.testing.expect((try readOperation(a, &txn, 1, identity.operation_id)) == null);
}

test "subscription native transaction admission bounds the final checkpoint to the signed snapshot" {
    const chain = @import("replication/snapshot_chain.zig");
    const second: chain.Page = .{ .relation = 7, .offset = 1, .count = 1, .content_digest = @splat(6), .next = chain.end };
    const first: chain.Page = .{ .relation = 7, .offset = 0, .count = 1, .content_digest = @splat(5), .next = try second.commitment() };
    const head = try first.commitment();
    const boundary: SnapshotBoundary = .{ .source_point = 10, .total_rows = 2, .capture_digest = @splat(4), .page_chain = head };
    const event: ReplicationEvent = .{ .kind = .snapshot_page, .source_point = 10, .end_point = 10, .next_offset = 1, .changes_digest = first.content_digest, .snapshot_page = first };
    try boundary.requireEvent(event, head);
    var forged = event;
    forged.snapshot_page.?.relation = 8;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, boundary.requireEvent(forged, head));
    forged = event;
    forged.changes_digest[0] ^= 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, boundary.requireEvent(forged, head));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, boundary.requireEvent(event, first.next));
    const finish: ReplicationEvent = .{ .kind = .initial_copy, .source_point = 10, .end_point = 10, .offset = 2, .next_offset = 2, .changes_digest = @splat(4) };
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, boundary.requireEvent(finish, first.next));
    try boundary.requireEvent(finish, chain.end);
}

test "SQL prepared transaction catalog participant fences publication resolution and retirement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const epoch: [32]u8 = @splat('1');
    const catalog: CatalogParticipant = .{ .staging_id = @splat(4), .plan_digest = @splat(5), .intent_digest = @splat(6) };
    const row: Participant = .{ .table_name = "rows", .route = .{
        .metadata_group_id = 1,
        .metadata_incarnation = epoch,
        .catalog_revision = 2,
        .table_id = 3,
        .topology_epoch = 1,
        .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 3, .shard_id = 7, .range_id = 8 }, .execution = .raft },
    } };
    for ([_][]const Participant{ &.{}, &.{row} }) |participants| {
        var identity: Identity = .{
            .catalog_participant = catalog,
            .kind = .session_commit,
            .session = .{ .owner_node_id = 9, .connection_id = @splat('a') },
            .lock_scope = .{
                .backend = .{ .incarnation = @splat(0x11), .id = @splat(8), .fence = 1 },
                .command = .{ .sequence = 1, .nonce = @splat(9), .executor = @splat(10) },
            },
            .gid = "",
            .database_id = 1,
            .principal = .bootstrap,
            .txn_id = @splat(2),
            .operation_id = @splat(3),
            .metadata_incarnation = epoch,
            .begin_timestamp = 10,
            .expires_at_unix_ns = 1000,
            .plan_digest = try planDigest("[]"),
            .manifest_digest = try manifestDigest(a, participants, epoch),
        };
        const binding: CatalogBinding = .{
            .fence = .{
                .group = 1,
                .backend = identity.lock_scope.?.backend,
                .command = identity.lock_scope.?.command,
                .database = identity.database_id,
                .principal = try @import("lock_authority.zig").principalDigest(a, identity.principal),
                .standalone = false,
            },
            .transaction = identity.txn_id,
            .operation = identity.operation_id,
            .intent_digest = catalog.intent_digest,
        };
        const owner = try identity.owner(a);
        const admission: Mutation = .{
            .admission = .{ .nonce = identity.operation_id, .head = 0 },
            .incarnation = @splat(0x11),
            .actor = .{ .session_user = .bootstrap, .credential = "" },
            .database_id = 1,
            .operation = .{ .admit = .{ .identity = identity, .participants = participants, .admitted_at_ns = 20 } },
        };
        try admission.validate();
        const fresh: Record = .{ .identity = identity, .participants = participants };
        try std.testing.expectError(error.CatalogPublicationProofPending, requireCatalogTransitionDecision(fresh, .begin_cancel));
        try requireCatalogTransitionDecision(fresh, .imported);
        try requireCatalogTransitionDecision(fresh, .validated);
        try requireCatalogTransitionDecision(fresh, .source_handoff_checked);

        try std.testing.expectError(error.InvalidPreparedTransactionPhase, completeCatalog(a, 1, fresh, fresh.revision, owner, binding, catalog, .published));
        var committed = try advance(a, fresh, fresh.revision, owner, .prepare);
        committed = try advance(a, committed, committed.revision, owner, .{ .commit = .{ .version = 20 } });
        try std.testing.expectEqual(Phase.decided, committed.phase);
        if (participants.len != 0) {
            try std.testing.expectError(error.InvalidPreparedTransactionPhase, completeCatalog(a, 1, committed, committed.revision, owner, binding, catalog, .published));
            committed = try advance(a, committed, committed.revision, owner, .{ .activate = 1 });
            try std.testing.expectError(error.InvalidPreparedTransactionPhase, advance(a, committed, committed.revision, owner, .{ .resolve = 1 }));
        }
        const encoded = try std.json.Stringify.valueAlloc(a, committed, .{});
        committed = try std.json.parseFromSliceLeaky(Record, a, encoded, .{});
        try committed.validate(a);
        try std.testing.expectEqual(Phase.decided, committed.phase);
        try std.testing.expectError(error.InvalidPreparedTransactionPhase, advance(a, committed, committed.revision, owner, .handoff));
        try std.testing.expectError(error.InvalidPreparedTransactionPhase, completeCatalog(a, 1, committed, committed.revision, owner, binding, catalog, .canceled));
        try requireCatalogTransitionDecision(committed, .begin_cutover);
        try std.testing.expectError(error.InvalidPreparedTransactionPhase, requireCatalogTransitionDecision(committed, .imported));
        try std.testing.expectError(error.InvalidPreparedTransactionPhase, requireCatalogTransitionDecision(committed, .begin_cancel));

        var wrong = catalog;
        wrong.plan_digest = @splat(11);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, completeCatalog(a, 1, committed, committed.revision, owner, binding, wrong, .published));
        var forged = committed;
        forged.catalog_outcome = .published;
        try std.testing.expectError(error.InvalidCatalogRecord, forged.validate(a));
        committed = try completeCatalog(a, 1, committed, committed.revision, owner, binding, catalog, .published);
        if (participants.len != 0) {
            try std.testing.expectEqual(Phase.active, committed.phase);
            committed = try advance(a, committed, committed.revision, owner, .{ .resolve = 1 });
        }
        try std.testing.expectEqual(Phase.resolved, committed.phase);
        const replayed = try completeCatalog(a, 1, committed, committed.revision, owner, binding, catalog, .published);
        try std.testing.expectEqual(committed.revision, replayed.revision);
        try std.testing.expectEqual(Phase.retired, (try advance(a, committed, committed.revision, owner, .handoff)).phase);

        var aborted = try advance(a, fresh, fresh.revision, owner, .{ .abort = 21 });
        if (participants.len != 0) aborted = try advance(a, aborted, aborted.revision, owner, .{ .resolve = 1 });
        try std.testing.expectError(error.InvalidPreparedTransactionPhase, advance(a, aborted, aborted.revision, owner, .handoff));
        try std.testing.expectEqual(CommitProgress.resolving, aborted.commitProgress());
        try requireCatalogTransitionDecision(aborted, .begin_cancel);
        try std.testing.expectError(error.InvalidPreparedTransactionPhase, requireCatalogTransitionDecision(aborted, .begin_cutover));

        aborted = try completeCatalog(a, 1, aborted, aborted.revision, owner, binding, catalog, .canceled);
        try std.testing.expectEqual(Phase.retired, (try advance(a, aborted, aborted.revision, owner, .handoff)).phase);

        if (participants.len == 0) {
            var named = identity;
            named.kind = .named;
            named.gid = "catalog-generation";
            const named_owner = try named.owner(a);
            var held: Record = .{ .identity = named, .participants = &.{} };
            held = try advance(a, held, held.revision, named_owner, .prepare);
            try std.testing.expectEqual(Phase.locking, held.phase);
            held = try advance(a, held, held.revision, named_owner, .prepare);
            held = try advance(a, held, held.revision, named_owner, .{ .commit = .{ .version = 22 } });
            try std.testing.expectEqual(Phase.decided, held.phase);
            held = try completeCatalog(a, 1, held, held.revision, named_owner, binding, catalog, .published);
            try std.testing.expectError(error.InvalidPreparedTransactionPhase, advance(a, held, held.revision, named_owner, .handoff));
            held = try advance(a, held, held.revision, named_owner, .lock_released);
            try std.testing.expectEqual(Phase.retired, (try advance(a, held, held.revision, named_owner, .handoff)).phase);
        }
        identity.catalog_participant = null;
        const ordinary = try std.json.Stringify.valueAlloc(a, Record{ .identity = identity, .participants = participants }, .{});
        try std.testing.expect(std.mem.indexOf(u8, ordinary, "catalog_participant") == null);
        try std.testing.expect(std.mem.indexOf(u8, ordinary, "catalog_outcome") == null);
        try std.testing.expect(!(try identity.owner(a)).eql(owner));
    }
}

test "SQL prepared transaction catalog writer rejoins exact durable cohort and rolls back incomplete publication" {
    const staging = @import("../metadata/restore_staging.zig");
    const backing = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const group: u64 = 1;
    const epoch: [32]u8 = @splat('1');
    const binding: CatalogBinding = .{
        .fence = .{
            .group = group,
            .backend = .{ .incarnation = @splat(0x11), .id = @splat(8), .fence = 1 },
            .command = .{ .sequence = 1, .nonce = @splat(9), .executor = @splat(10) },
            .database = 1,
            .principal = try @import("lock_authority.zig").principalDigest(a, .bootstrap),
            .standalone = false,
        },
        .transaction = @splat(2),
        .operation = @splat(3),
        .intent_digest = @splat(6),
    };
    const cohort: staging.Plan = .{
        .id = @splat(4),
        .expected_incarnation = epoch,
        .cohort_digest = @splat(5),
        .catalog_binding = binding,
        .targets = &.{.{ .source_table_id = 0, .table = .{ .table_id = 18, .name = "private" }, .ranges = &.{.{ .table_id = 18, .group_id = 8, .start_key = "" }} }},
    };
    try std.testing.expectError(error.InvalidRestoreStaging, cohort.validate(a));
    const participant: CatalogParticipant = .{ .staging_id = cohort.id, .plan_digest = try cohort.digest(a), .intent_digest = binding.intent_digest };
    const identity: Identity = .{
        .catalog_participant = participant,
        .kind = .session_commit,
        .session = .{ .owner_node_id = 9, .connection_id = @splat('a') },
        .lock_scope = .{ .backend = binding.fence.backend, .command = binding.fence.command },
        .gid = "",
        .database_id = binding.fence.database,
        .principal = .bootstrap,
        .txn_id = binding.transaction,
        .operation_id = binding.operation,
        .metadata_incarnation = epoch,
        .begin_timestamp = 10,
        .expires_at_unix_ns = 1000,
        .plan_digest = try planDigest("[]"),
        .manifest_digest = try manifestDigest(a, &.{}, epoch),
    };
    const owner = try identity.owner(a);
    var record: Record = .{ .identity = identity, .participants = &.{} };
    record = try advance(a, record, record.revision, owner, .prepare);
    record = try advance(a, record, record.revision, owner, .{ .commit = .{ .version = 22 } });
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/catalog-cohort-terminal", .{tmp.sub_path});
    const Store = @import("../metadata/storage/raft_apply_store.zig").RaftApplyStore;
    var job_buf: [128]u8 = undefined;
    const job_key = try staging.jobKey(&job_buf, group, cohort.id);
    var progress_buf: [128]u8 = undefined;
    const progress_key = try staging.progressKey(&progress_buf, group, cohort.id);
    const job: staging.Job = .{ .plan = cohort, .plan_digest = participant.plan_digest };
    const progress: staging.Progress = .{ .state = .published, .revision = 2, .completed_owners = 1 };
    const published = try std.json.Stringify.valueAlloc(a, progress, .{});
    {
        var store = try Store.init(backing, .{ .root_dir = path });
        defer store.deinit();
        try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = epoch });
        const controller = @import("../metadata/restore_coordinator_control.zig");
        try std.testing.expectError(error.CatalogPublicationProofPending, controller.observeCatalogReservation(&store, a, group, participant));
        const reserve: staging.Command = .{ .id = cohort.id, .action = .reserve, .plan = cohort };
        const body = try std.json.Stringify.valueAlloc(a, reserve, .{});
        const metadata = @import("../metadata/storage/raft_apply_store.zig");
        const wire = try metadata.encodeTransitionCommand(a, .{ .reserve_sql_catalog_restore = body });
        const decoded = (try metadata.decodeTransitionCommand(a, wire)).?;
        try std.testing.expectEqualStrings(body, decoded.reserve_sql_catalog_restore);
        const extra = try std.mem.concat(a, u8, &.{ wire, "x" });
        try std.testing.expectError(error.InvalidMetadataTransitionEncoding, metadata.decodeTransitionCommand(a, extra));
        try store.applyStandaloneCommand(group, decoded);
        const Service = struct {
            store: *Store,
            metadata_group_id: u64,
            proposed: usize = 0,
            applied: bool = false,

            pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: metadata.TransitionCommand) !usize {
                try self.store.applyStandaloneCommand(self.metadata_group_id, command);
                self.proposed += 1;
                return self.proposed;
            }
            pub fn waitForTransitionAppliedWithContext(self: *@This(), receipt: usize, _: local.api_operation.RequestContext) !void {
                try std.testing.expectEqual(self.proposed, receipt);
                self.applied = true;
            }
            pub fn ensureLinearizableReadWithContext(self: *@This(), _: local.api_operation.RequestContext) !void {
                try std.testing.expect(self.applied);
            }
            pub fn projectedStore(self: *@This()) ?*Store {
                return self.store;
            }
        };
        var svc: Service = .{ .store = &store, .metadata_group_id = group };
        var wrong_participant = participant;
        wrong_participant.plan_digest = @splat(0x30);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, controller.reserveCatalog(&svc, a, cohort, wrong_participant, .{}));
        try std.testing.expectEqual(@as(usize, 0), svc.proposed);
        try std.testing.expectError(error.CatalogPublicationProofPending, controller.reserveCatalog(&svc, a, cohort, participant, .{}));
        try std.testing.expectEqual(@as(usize, 1), svc.proposed);
        var txn = try store.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(null, try optional(&txn, job_key));
        try std.testing.expectError(error.CatalogPublicationProofPending, requireCatalogAdmission(a, &txn, group, identity));
        try txn.put(job_key, try std.json.Stringify.valueAlloc(a, job, .{}));
        try txn.put(progress_key, try std.json.Stringify.valueAlloc(a, staging.Progress{}, .{}));
        try std.testing.expectError(error.CatalogGenerationChanged, requireCatalogAdmission(a, &txn, group, identity));
        const locks = @import("lock_authority.zig");
        const realm: locks.scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = binding.fence.backend.incarnation };
        const actor: Actor = .{ .session_user = .bootstrap, .credential = "" };
        const authority: locks.Context = .{ .a = a, .actor = actor, .database = binding.fence.database, .standalone = false };
        const registration: locks.scopes.Registration = .{
            .group = group,
            .lease = .{ .backend = binding.fence.backend, .database = binding.fence.database, .node = 9, .principal = binding.fence.principal, .expires_at_ms = 1000 },
            .connection = identity.session.?.connection_id.?,
            .nonce = @splat(17),
            .standalone = false,
        };
        try locks.scopes.register(a, &txn, realm, registration, authority);
        _ = try locks.scopes.begin(a, &txn, realm, binding.fence.backend, binding.fence.command, authority);
        try std.testing.expectError(error.CatalogGenerationChanged, requireCatalogAdmission(a, &txn, group, identity));
        try locks.scopes.bindTransaction(a, &txn, realm, binding.fence.backend, binding.fence.command, binding.transaction, authority);
        var wrong_session = identity;
        wrong_session.session.?.connection_id = @splat('b');
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, requireCatalogAdmission(a, &txn, group, wrong_session));
        {
            const generations = @import("../metadata/sql/relation_lifecycle/generation_intents.zig");
            const txns = @import("../api/transactions.zig");
            var entry = try generations.testingEntry(a, binding.transaction, 1, 0);
            entry.birth = binding.fence;
            var definition = try entry.prepared(a);
            definition.metadata_incarnation = epoch;
            definition.child.schema_json = try local.sql_ddl_runtime.createSchemaAlloc(a, .{
                .table = .{ .table = entry.target.table },
                .columns = &.{.{ .name = "val", .type = .string, .nullable = true }},
            });
            const ranges = try a.dupe(local.common_topology_records.RangeRecord, definition.child_ranges);
            for (ranges) |*range| {
                range.range_id = range.group_id;
                range.doc_identity_range_id = range.group_id;
                range.doc_identity_shard_id = range.group_id;
            }
            definition.child_ranges = ranges;
            entry.preparation = try std.json.Stringify.valueAlloc(a, definition, .{});
            var session: txns.Session = .{
                .txn_id = binding.transaction,
                .owner_node_id = 9,
                .connection_id = identity.session.?.connection_id,
                .begin_timestamp = identity.begin_timestamp,
                .last_touched_timestamp = identity.begin_timestamp,
                .sync_level = .write,
                .sql = .{ .database = try a.dupe(u8, "app"), .namespace = try a.dupe(u8, "public"), .mode = .read_write, .isolation = .read_committed },
            };
            defer session.deinit(a);
            try session.sql.?.catalog_generations.append(a, entry);
            var scoped = binding;
            scoped.intent_digest = session.sql.?.catalog_generations.digest();
            const Cohort = @import("../api/sql/copy/generation_rows.zig").Cohort;
            var prepared = try Cohort.init(a, &session, try staging.idForAttempt(42, 1), scoped, .{
                .capture_id = @splat(1),
                .definition = @splat(2),
                .source_definition = @splat(3),
                .target_schema = @splat(4),
                .principal_instance = @splat(5),
                .catalog_revision = 7,
                .write_version = 9,
                .expires_at_ms = 100,
            });
            defer prepared.deinit();
            const catalog_wire = try txns.encodeCatalogExecutionPlan(a, .{ .tables = &.{}, .catalog = prepared.plan });
            var proposed = identity;
            proposed.catalog_participant = prepared.participant;
            proposed.plan_digest = try planDigest(catalog_wire);
            const seal: txns.ExternalPreparationSeal = .{
                .admission_head = 0,
                .admitted_at_ns = 20,
                .standalone = false,
                .identity = proposed,
                .actor = actor,
                .participants = &.{},
            };
            const request = try seal.admissionRequest();
            const sessions = @import("../metadata/transaction_session_record.zig");
            const logical = try std.mem.concat(a, u8, &.{ sessions.session_prefix, &std.fmt.bytesToHex(binding.transaction, .lower) });
            const session_key = try sessions.keyAlloc(a, group, logical);
            try txn.put(session_key, try std.json.Stringify.valueAlloc(a, .{
                .sql = session.sql,
                .execution_plan = catalog_wire,
                .external_preparation = try std.json.Stringify.valueAlloc(a, seal, .{}),
            }, .{}));
            const planned = try planWithCatalogReservation(a, &txn, group, 0, request);
            const reservation = try staging.parseCatalogReservationTxn(a, planned.catalog_reservation.?, &txn, group);
            try std.testing.expectEqual(prepared.participant.plan_digest, try reservation.plan.?.digest(a));
            try std.testing.expectError(error.CatalogPublicationProofPending, observeCatalog(a, &txn, group, proposed));
            try std.testing.expectEqual(null, try readOperation(a, &txn, group, scoped.operation));
            var changed = request;
            changed.operation.admit.admitted_at_ns += 1;
            try std.testing.expectError(error.ExternalTransactionDecisionChanged, planWithCatalogReservation(a, &txn, group, 0, changed));
            try txn.delete(session_key);
        }

        const admission: Mutation = .{
            .admission = .{ .nonce = binding.operation, .head = 0 },
            .incarnation = realm.incarnation,
            .actor = actor,
            .database_id = binding.fence.database,
            .operation = .{ .admit = .{ .identity = identity, .participants = &.{}, .admitted_at_ns = 20 } },
        };

        try std.testing.expectError(error.SqlTransactionNotActive, catalogReservationAuthority(a, &txn, group, cohort));
        const sessions = @import("../metadata/transaction_session_record.zig");
        const logical = try std.mem.concat(a, u8, &.{ sessions.session_prefix, &std.fmt.bytesToHex(binding.transaction, .lower) });
        const session_key = try sessions.keyAlloc(a, group, logical);
        var seal: @import("../api/transactions.zig").ExternalPreparationSeal = .{
            .admission_head = 0,
            .admitted_at_ns = 20,
            .standalone = false,
            .identity = wrong_session,
            .actor = actor,
            .participants = &.{},
        };
        try txn.put(session_key, try std.json.Stringify.valueAlloc(a, .{
            .sql = .{ .database = "app" },
            .external_preparation = try std.json.Stringify.valueAlloc(a, seal, .{}),
        }, .{}));
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, catalogReservationAuthority(a, &txn, group, cohort));
        seal.identity = identity;
        try txn.put(session_key, try std.json.Stringify.valueAlloc(a, .{
            .sql = .{ .database = "app" },
            .external_preparation = try std.json.Stringify.valueAlloc(a, seal, .{}),
        }, .{}));
        const reservation = try catalogReservationAuthority(a, &txn, group, cohort);
        try std.testing.expectEqualDeep(participant.plan_digest, reservation.plan_digest);
        try std.testing.expectError(error.CatalogPublicationProofPending, requireCatalogTransition(a, &txn, group, job, .imported));
        try (try plan(a, &txn, group, 0, admission)).apply(&txn);
        try requireCatalogTransition(a, &txn, group, job, .imported);
        try requireCatalogTransition(a, &txn, group, job, .validated);
        try requireCatalogTransition(a, &txn, group, job, .source_handoff_checked);
        var changed_job = job;
        changed_job.plan_digest[0] ^= 1;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, requireCatalogTransition(a, &txn, group, changed_job, .imported));
        const prepare_request: Mutation = .{
            .admission = .{ .nonce = @splat(18), .head = 0 },
            .incarnation = realm.incarnation,
            .actor = actor,
            .database_id = binding.fence.database,
            .operation = .{ .advance = .{ .owner = owner, .expected_revision = 1, .action = .prepare } },
        };
        try std.testing.expectError(error.CatalogPublicationProofPending, plan(a, &txn, group, 0, prepare_request));
        try txn.put(progress_key, try std.json.Stringify.valueAlloc(a, staging.Progress{ .state = .validating }, .{}));
        try std.testing.expectError(error.CatalogPublicationProofPending, plan(a, &txn, group, 0, prepare_request));
        try txn.put(progress_key, try std.json.Stringify.valueAlloc(a, staging.Progress{ .state = .validating, .completed_owners = 1 }, .{}));
        try (try plan(a, &txn, group, 0, prepare_request)).apply(&txn);
        var commit_request = prepare_request;
        commit_request.operation.advance.expected_revision = 2;
        commit_request.operation.advance.action = .{ .commit = .{ .version = 22 } };
        try (try plan(a, &txn, group, 0, commit_request)).apply(&txn);
        try std.testing.expectEqualDeep(record, (try readOperation(a, &txn, group, binding.operation)).?);
        try txn.delete(progress_key);
        try txn.commit();
        {
            var writer = try store.store.beginWriteTxn();
            defer writer.abort();
            try std.testing.expectError(error.InvalidCatalogMutation, binding.requireBirth(a, &writer, group));
            try std.testing.expectError(error.CatalogPublicationProofPending, completeCatalogTxn(a, &writer, group, binding, participant, record.revision, owner));
            var incomplete = progress;
            incomplete.completed_owners = 0;
            try writer.put(progress_key, try std.json.Stringify.valueAlloc(a, incomplete, .{}));
            try std.testing.expectError(error.CatalogPublicationProofPending, completeCatalogTxn(a, &writer, group, binding, participant, record.revision, owner));
            try writer.put(progress_key, published);
            var wrong = binding;
            wrong.fence.command.nonce = @splat(12);
            try std.testing.expectError(error.ExternalTransactionDecisionChanged, completeCatalogTxn(a, &writer, group, wrong, participant, record.revision, owner));
            _ = try completeCatalogTxn(a, &writer, group, binding, participant, record.revision, owner);
            // Simulate writer failure before committing the paired effects.
        }
    }
    {
        var store = try Store.init(backing, .{ .root_dir = path });
        defer store.deinit();
        var writer = try store.store.beginWriteTxn();
        errdefer writer.abort();
        try std.testing.expectEqual(CatalogOutcome.pending, (try readOperation(a, &writer, group, binding.operation)).?.catalog_outcome);
        try std.testing.expectError(error.NotFound, writer.get(progress_key));
        try writer.put(progress_key, published);
        const next = try completeCatalogTxn(a, &writer, group, binding, participant, record.revision, owner);
        try std.testing.expectEqual(Phase.resolved, next.phase);
        try writer.commit();
    }
    var reopened = try Store.init(backing, .{ .root_dir = path });
    defer reopened.deinit();
    const controller = @import("../metadata/restore_coordinator_control.zig");
    var durable = try controller.observeCatalogReservation(&reopened, backing, group, participant);
    defer durable.deinit();
    try std.testing.expectEqual(staging.State.published, durable.value.state);
    var different_plan = participant;
    different_plan.plan_digest = @splat(0x40);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, controller.observeCatalogReservation(&reopened, backing, group, different_plan));
    var read = try reopened.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualStrings(published, try read.get(progress_key));
    try std.testing.expectEqual(CatalogOutcome.published, (try readOperation(a, &read, group, binding.operation)).?.catalog_outcome);
    const receipt = (try readReceipt(a, &read, group, binding.operation)).?;
    try std.testing.expectEqual(CommitProgress.resolving, receipt.progress);
}
