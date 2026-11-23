// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared logical publisher contracts. These are data, never caller authority:
//! the native writer rejoins actual catalog, enrollment and P decision owners.
const std = @import("std");
const catalog = @import("logical_replication_catalog.zig");
const options = @import("logical_replication.zig");
const decision = @import("../storage/transaction_decision_owner.zig");
const position = @import("../storage/db/receipt_position.zig");
const namespace = @import("../storage/db/doc_identity_namespace.zig");
pub const Digest = [32]u8;
pub const SourceReference = @import("../api/replication_source_reference.zig").Reference;
pub const Publication = catalog.Stamp;
pub const max_relations = options.max_members;
pub const max_columns = 1600;
pub const max_publications = options.max_members;
pub const max_capture_participants = 64;
pub const max_feed_page = 32;
/// The same authored bounded metadata object ceiling as replication catalog
/// definitions; per-relation limits never multiply into an unbounded reply.
pub const max_roster_bytes = catalog.max_record_bytes;
pub const decoder_version = 184;
pub const data_version = 13;

pub const Realm = struct {
    group_id: u64,
    incarnation: [32]u8,
    database_id: u64,
    pub fn validate(self: @This()) !void {
        if (self.group_id == 0 or self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, '0')) return error.InvalidLogicalPublisher;
        for (self.incarnation) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidLogicalPublisher;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }
};
pub const PublisherIdentity = struct {
    realm: Realm,
    /// Issued once by the actual native publisher, never a hash of a username,
    /// a fabricated PostgreSQL backend or the current transport connection.
    system_identifier: u64,
    timeline: u32,
    database_oid: u32,
    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        if (self.system_identifier == 0 or self.timeline == 0 or self.database_oid == 0) return error.InvalidLogicalPublisher;
    }
};
pub const ReplicaIdentity = enum { default_key, full, nothing, index };
pub const Column = struct {
    attnum: u16,
    name: []const u8,
    type_oid: u32,
    type_modifier: i32 = -1,
    key: bool = false,
    generated: bool = false,
    pub fn validate(self: @This()) !void {
        if (self.attnum == 0 or self.attnum > max_columns or self.type_oid == 0) return error.InvalidLogicalPublisher;
        try identifier(self.name);
    }
};
pub const RelationDescription = struct {
    logical_relation_id: u64,
    physical_table_id: u64,
    database_id: u64,
    namespace_id: u64,
    /// Canonical catalog OID mapping, never a truncated logical/physical u64.
    relation_oid: u32,
    namespace: []const u8,
    name: []const u8,
    schema_version: u32,
    schema_digest: Digest,
    replica_identity: ReplicaIdentity,
    columns: []const Column,
    pub fn validate(self: @This()) !void {
        if (self.logical_relation_id == 0 or self.physical_table_id == 0 or self.database_id == 0 or self.namespace_id == 0 or self.relation_oid == 0 or zero(self.schema_digest) or self.columns.len == 0 or self.columns.len > max_columns) return error.InvalidLogicalPublisher;
        try identifier(self.namespace);
        try identifier(self.name);
        var previous: u16 = 0;
        var has_key = false;
        for (self.columns) |column| {
            try column.validate();
            if (column.attnum <= previous) return error.InvalidLogicalPublisher;
            previous = column.attnum;
            has_key = has_key or column.key;
        }
        if (self.replica_identity == .index and !has_key) return error.InvalidLogicalPublisher;
    }
};
pub const Membership = struct { publication: Publication, parameters: options.PublicationOptions };
pub const PublishedRelation = struct {
    relation: RelationDescription,
    memberships: []const Membership,
    pub fn validate(self: @This()) !void {
        try self.relation.validate();
        if (self.memberships.len == 0 or self.memberships.len > max_publications) return error.InvalidLogicalPublisher;
        var previous: u64 = 0;
        for (self.memberships) |member| {
            if (member.publication.id <= previous or member.publication.generation == 0 or zero(member.publication.digest)) return error.InvalidLogicalPublisher;
            previous = member.publication.id;
        }
    }
};
pub const RosterRequest = struct {
    database_id: u64,
    physical_table_ids: []const u64,
    expected_revision: ?u64 = null,
    pub fn validate(self: @This()) !void {
        if (self.database_id == 0 or self.physical_table_ids.len > max_relations) return error.InvalidLogicalPublisher;
        for (self.physical_table_ids) |table| if (table == 0) return error.InvalidLogicalPublisher;
    }
};
pub const Roster = struct {
    realm: Realm,
    catalog_revision: u64,
    /// Same order/cardinality as requested actual physical tables. Null means
    /// no publication membership, not missing catalog/ownership authority.
    relations: []const ?PublishedRelation,
    pub fn validate(self: @This(), request: RosterRequest) !void {
        try request.validate();
        try self.realm.validate();
        if (self.realm.database_id != request.database_id or self.relations.len != request.physical_table_ids.len or self.relations.len > max_relations or (request.expected_revision != null and self.catalog_revision != request.expected_revision.?)) return error.LogicalPublisherCatalogChanged;
        var budget: RosterBudget = .{};
        for (self.relations, request.physical_table_ids) |maybe, table_id| {
            try budget.add(maybe);
            if (table_id == 0) return error.InvalidLogicalPublisher;
            if (maybe) |relation| {
                try relation.validate();
                if (self.catalog_revision == 0) return error.LogicalPublisherCatalogChanged;
                if (relation.relation.physical_table_id != table_id or relation.relation.database_id != request.database_id) return error.LogicalPublisherCatalogChanged;
            }
        }
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_roster_bytes) return error.LogicalPublisherRosterTooLarge;
    }
};
/// Native roster builders use this before retaining each next relation. JSON
/// counting uses the actual escaping/number codec and allocates no reply body.
/// The final Roster.validate additionally counts the complete outer envelope.
pub const RosterBudget = struct {
    encoded_relations: usize = 0,
    count: usize = 0,
    pub fn add(self: *@This(), relation: ?PublishedRelation) !void {
        if (self.count == max_relations) return error.LogicalPublisherRosterTooLarge;
        if (relation) |actual| try actual.validate();
        var size = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(relation, .{}, &size.writer);
        const bytes = size.fullCount();
        const comma: usize = @intFromBool(self.count != 0);
        if (bytes > max_roster_bytes - self.encoded_relations or comma > max_roster_bytes - self.encoded_relations - bytes) return error.LogicalPublisherRosterTooLarge;
        self.encoded_relations += bytes + comma;
        self.count += 1;
    }
};
/// Durable evidence written in the same ordered batch as the row decision.
pub const OrderedTerminal = struct {
    capture_ref: CaptureRef,
    position: @FieldType(position.Position, "raft"),
    status: enum { committed, aborted },
    commit_version: u64,

    pub fn validate(self: @This()) !void {
        try self.capture_ref.validate();
        const terminal_position: position.Position = .{ .raft = self.position };
        try terminal_position.validate();
        if (self.capture_ref.position != .raft or !terminal_position.strictlyAfter(self.capture_ref.position) or
            (self.status == .committed and self.commit_version == 0)) return error.InvalidLogicalOutbox;
    }
};

pub const CaptureRef = struct {
    owner: decision.External,
    transaction_id: [16]u8,
    participant: u16,
    group_id: u64,
    namespace: namespace.Namespace,
    /// Actual image at local durable preparation. A different Raft replica
    /// may retain the same replicated effect; native image identity is exact.
    physical_root_incarnation: u128,
    position: position.Position,
    roster_digest: Digest,
    guard_digest: Digest,
    pregrant_body_digest: Digest,
    changes_digest: Digest,
    capacity_digest: Digest,
    before_digest: Digest,
    change_count: u64,
    encoded_bytes: u64,
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        try self.position.requireNamespace(self.namespace);
        if (std.mem.allEqual(u8, &self.transaction_id, 0) or self.participant >= max_capture_participants or self.group_id == 0 or self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or self.physical_root_incarnation == 0 or zero(self.roster_digest) or zero(self.capacity_digest) or zero(self.before_digest) or zero(self.guard_digest) or zero(self.pregrant_body_digest) or zero(self.changes_digest) or self.encoded_bytes == 0) return error.InvalidLogicalPublisher;
    }
    /// This comparison does not grant a relocation. A read/re-attestation must
    /// additionally verify the actual current Raft placement and applied cut.
    /// A native Root replacement can never satisfy an old image reference.
    pub fn sameLogical(self: @This(), other: @This()) bool {
        self.validate() catch return false;
        other.validate() catch return false;
        var left = self;
        if (left.position == .raft and other.position == .raft)
            left.physical_root_incarnation = other.physical_root_incarnation;
        return std.meta.eql(left, other);
    }
};
/// Retained in the publisher journal after committed history is collected or
/// a genuine durable ABORT releases a captured prepare. No caller can mint it.
pub const GcDebt = struct {
    reference: CaptureRef,
    table_name: []const u8,
    reason: union(enum) { journal: u64, aborted: u64 },
    decision_version: u64,
    pub fn validate(self: @This()) !void {
        try self.reference.validate();
        const version = switch (self.reason) {
            .journal, .aborted => |value| value,
        };
        if (version == 0 or self.decision_version == 0 or (self.reason == .aborted and self.reason.aborted != self.decision_version) or self.table_name.len == 0 or self.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(self.table_name) or std.mem.indexOfScalar(u8, self.table_name, 0) != null) return error.InvalidLogicalPublisher;
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate();
        return @import("logical_replication_capture.zig").canonicalDigest(a, "antfly/logical-publisher-gc-debt/v1\x00", self);
    }
};
pub const GcAction = enum { purge, clear_marker };
pub const GcPermission = struct {
    realm: Realm,
    debt: GcDebt,
    action: GcAction,
    /// Exact logical observation already accepted by the native writer, not
    /// a client claim that a purge happened. Only clear_marker carries it.
    purged_digest: ?Digest = null,
    purged: ?GcObservation = null,
    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        try self.debt.validate();
        if (!std.mem.eql(u8, &self.realm.incarnation, &self.debt.reference.owner.metadata_incarnation) or
            (self.action == .clear_marker) != (self.purged_digest != null) or (self.action == .clear_marker) != (self.purged != null)) return error.InvalidLogicalPublisher;
        if (self.purged) |value| {
            try value.validate();
            try value.position.requireNamespace(self.debt.reference.namespace);
            if (value.action != .purge or value.evidence != .effect or (value.position == .native) != (self.debt.reference.position == .native)) return error.InvalidLogicalPublisher;
        }
        if (self.purged_digest) |value| if (zero(value)) return error.InvalidLogicalPublisher;
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate();
        return @import("logical_replication_capture.zig").canonicalDigest(a, "antfly/logical-publisher-gc-permission/v1\x00", self);
    }
};
pub const GcObservation = struct {
    action: GcAction,
    /// Absence is a signed read at a genuine later source cut, after metadata
    /// accepted the actual purge. It never claims a new mutation position.
    evidence: enum { effect, absence } = .effect,
    debt_digest: Digest,
    reference_digest: Digest,
    header_digest: Digest,
    physical_root_incarnation: u128,
    position: position.Position,
    payload_bytes: u64,
    purged_digest: ?Digest = null,
    pub fn validate(self: @This()) !void {
        try self.position.validate();
        if ((self.evidence == .absence and self.action != .clear_marker) or zero(self.debt_digest) or zero(self.reference_digest) or zero(self.header_digest) or self.physical_root_incarnation == 0 or self.payload_bytes == 0 or
            (self.action == .clear_marker) != (self.purged_digest != null)) return error.InvalidLogicalOutbox;
        if (self.purged_digest) |value| if (zero(value)) return error.InvalidLogicalOutbox;
    }
    pub fn require(self: @This(), permission: GcPermission, a: std.mem.Allocator) !void {
        try self.validate();
        try permission.validate();
        try self.position.requireNamespace(permission.debt.reference.namespace);
        if ((self.position == .native) != (permission.debt.reference.position == .native)) return error.InvalidLogicalOutbox;
        if (!self.position.strictlyAfter(permission.debt.reference.position)) return error.InvalidLogicalOutbox;
        if (self.action != permission.action or !std.mem.eql(u8, &self.debt_digest, &try permission.debt.digest(a)) or
            !std.mem.eql(u8, &self.reference_digest, &try captureReferenceDigest(a, permission.debt.reference)) or
            self.payload_bytes != permission.debt.reference.encoded_bytes or !std.meta.eql(self.purged_digest, permission.purged_digest)) return error.InvalidLogicalOutbox;
        if (permission.debt.reference.position == .native and self.physical_root_incarnation != permission.debt.reference.physical_root_incarnation) return error.InvalidLogicalOutbox;
        if (permission.purged) |prior| {
            if (!std.mem.eql(u8, &try prior.digest(a), &permission.purged_digest.?) or !std.mem.eql(u8, &self.header_digest, &prior.header_digest) or
                !self.position.strictlyAfter(prior.position)) return error.InvalidLogicalOutbox;
        }
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate();
        return @import("logical_replication_capture.zig").canonicalDigest(a, "antfly/logical-publisher-gc-observation/v1\x00", self);
    }
};
pub fn captureReferenceDigest(a: std.mem.Allocator, reference: CaptureRef) !Digest {
    try reference.validate();
    return @import("logical_replication_capture.zig").canonicalDigest(a, "antfly/logical-publisher-capture-reference/v1\x00", reference);
}

pub const Origin = struct { name: []const u8, position: u64 };
pub const Transaction = struct {
    owner: decision.External,
    transaction_id: [16]u8,
    realm: Realm,
    commit_version: u64,
    commit_time_unix_us: u64,
    origin: ?Origin = null,
    /// Native Source admission provenance is retained even when the provider
    /// has no PostgreSQL-numeric origin point. origin=none must exclude it.
    source: ?SourceReference = null,
    /// Captures are in frozen participant order. Each captured mutation has
    /// its original whole-plan ordinal; delivery merges those ordered streams.
    captures: []const CaptureRef,
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        try self.realm.validate();
        if (!std.mem.eql(u8, &self.owner.metadata_incarnation, &self.realm.incarnation) or
            std.mem.allEqual(u8, &self.transaction_id, 0) or self.commit_version == 0 or self.commit_time_unix_us == 0 or
            self.captures.len == 0 or self.captures.len > max_capture_participants) return error.InvalidLogicalPublisher;
        if (self.source) |reference| {
            try reference.validate();
            if (!std.mem.eql(u8, &reference.txn_id, &self.transaction_id)) return error.InvalidLogicalPublisher;
        }
        if (self.origin) |origin| {
            try identifier(origin.name);
            if (origin.position == 0) return error.InvalidLogicalPublisher;
        }
        var previous: ?u16 = null;
        for (self.captures) |capture| {
            try capture.validate();
            if (!capture.owner.eql(self.owner) or !std.mem.eql(u8, &capture.transaction_id, &self.transaction_id) or
                (previous != null and capture.participant <= previous.?)) return error.InvalidLogicalPublisher;
            previous = capture.participant;
        }
    }
};
pub const SlotRef = struct {
    database_id: u64,
    name: []const u8,
    birth: u64,
    pub fn validate(self: @This()) !void {
        try validateSlotName(self.name);
        if (self.database_id == 0 or self.birth == 0) return error.InvalidLogicalPublisher;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return self.database_id == other.database_id and self.birth == other.birth and std.mem.eql(u8, self.name, other.name);
    }
};
pub const Lease = struct {
    slot: SlotRef,
    generation: u64,
    connection: [32]u8,
    nonce: [16]u8,
    pub fn validate(self: @This()) !void {
        try self.slot.validate();
        if (self.generation == 0 or std.mem.allEqual(u8, &self.connection, 0) or std.mem.allEqual(u8, &self.nonce, 0)) return error.InvalidLogicalPublisher;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return self.slot.eql(other.slot) and self.generation == other.generation and std.mem.eql(u8, &self.connection, &other.connection) and std.mem.eql(u8, &self.nonce, &other.nonce);
    }
};
pub const SnapshotMode = enum { export_snapshot, use_snapshot, nothing };
pub const SnapshotPoint = struct { lsn: u64, catalog_revision: u64 };
pub const CreateSlot = struct {
    database_id: u64,
    name: []const u8,
    temporary: bool = false,
    snapshot: SnapshotMode = .export_snapshot,
    connection: [32]u8,
    nonce: [16]u8,
    expected_cut: ?SnapshotPoint = null,
};
pub const Acknowledge = struct { lease: Lease, received: u64, flushed: u64, applied: u64, observed_at_unix_ns: u64 };
pub const FeedRequest = struct { lease: Lease, observed_at_unix_ns: u64, after: u64, limit: u16 = max_feed_page, publication_names: []const []const u8, protocol_version: u32 = 1, origin: options.Origin = .any };
pub const ParticipantCaptureRequest = struct {
    physical_table_id: u64,
    owner_group_id: u64,
    namespace: namespace.Namespace,
    pregrant_body_digest: Digest,
    request_digest: Digest,
};
pub const ParticipantCapture = struct {
    reference: @import("logical_replication_capture.zig").DemandReference,
    roster: PublishedRelation,
};
pub const OutboxRead = struct {
    realm: Realm,
    lease: Lease,
    reference: CaptureRef,
    /// Frozen physical transport name from the real P participant, not the
    /// current SQL alias (which may have been renamed/dropped after COMMIT).
    table_name: []const u8,
    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        try self.lease.validate();
        try self.reference.validate();
        if (self.lease.slot.database_id != self.realm.database_id or !std.mem.eql(u8, &self.realm.incarnation, &self.reference.owner.metadata_incarnation) or
            self.table_name.len == 0 or self.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(self.table_name) or std.mem.indexOfScalar(u8, self.table_name, 0) != null) return error.InvalidLogicalPublisher;
    }
};
pub const Read = union(enum) {
    outbox_permission: OutboxRead,
    control_head: struct { database_id: u64 },
    gc_work: struct { after_database: ?u64 = null, max_databases: u8 = 8 },
    gc_permission: GcPermission,
    capacity_grant: @import("logical_replication_capture.zig").Demand,
    capacity_abort_permission: @import("logical_replication_capacity.zig").AbortPermission,
    /// Native receiver derives authoritative absence/installed aggregate in
    /// one snapshot. Zero hashes can prove absence only, never a required grant.
    participant: ParticipantCaptureRequest,
    /// Independent actual pending-operation permission before physical owner
    /// observation/proposal. Presented Control is not its own authorization.
    capture_permission: @import("logical_replication_capture.zig").Request,
    roster: RosterRequest,
    identity: struct { database_id: u64 },
    head: struct { database_id: u64 },
    slots: struct { database_id: u64, after: ?[]const u8 = null, limit: u16 = max_feed_page },
};
pub const Control = union(enum) {
    finish_subscription_disable: struct { item: catalog.SubscriptionWork, checkpoint: ?u64 = null },
    finish_subscription_drop: struct { item: catalog.SubscriptionWork, checkpoint: u64 },
    claim_subscription: struct { item: catalog.SubscriptionWork, worker: [16]u8, observed_at_unix_ns: u64, expires_at_unix_ns: u64 },
    transition_subscription_export: struct { lease: catalog.SubscriptionWorkLease, action: enum { fence, retire, cancel_unexported }, observed_at_unix_ns: u64 },
    bind_subscription_export: struct { lease: catalog.SubscriptionWorkLease, attempt: [16]u8, origin: Digest, receipt: catalog.SubscriptionExport, observed_at_unix_ns: u64 },
    bind_subscription_origin: struct { lease: catalog.SubscriptionWorkLease, attempt: [16]u8, origin: Digest, observed_at_unix_ns: u64 },
    start_subscription: struct {
        lease: catalog.SubscriptionWorkLease,
        attempt: [16]u8,
        observed_at_unix_ns: u64,
        provider_preflight_identity: ?Digest = null,

        pub fn jsonStringify(self: @This(), writer: anytype) !void {
            try writer.beginObject();
            inline for (@typeInfo(@This()).@"struct".field_names) |field| {
                if ((comptime !std.mem.eql(u8, field, "provider_preflight_identity")) or self.provider_preflight_identity != null) {
                    try writer.objectField(field);
                    try writer.write(@field(self, field));
                }
            }
            try writer.endObject();
        }
    },
    initialize: u64,
    /// Feed advances the exact lease offered boundary and therefore is an
    /// authored native control, not a query whose clock mutates slot state.
    feed: FeedRequest,
    create_slot: CreateSlot,
    drop_slot: struct { database_id: u64, slot: SlotRef, wait: bool = false },
    acquire: struct { database_id: u64, slot: SlotRef, connection: [32]u8, nonce: [16]u8, observed_at_unix_ns: u64, expires_at_unix_ns: u64 },
    release: Lease,
    acknowledge: Acknowledge,
    expire: struct { lease: Lease, expected_expiry: u64, observed_at_unix_ns: u64 },
};
pub const Request = union(enum) { read: Read, control: Control };

/// A cursor is allocated by the native decision writer, in the very same
/// transaction as COMMIT. It is not a Data Raft index or upstream PostgreSQL LSN.
pub const JournalEntry = struct {
    /// Native-issued decoding XID; never truncation/hash of the P txn UUID.
    xid: u32,
    begin_lsn: u64,
    end_lsn: u64,
    transaction: Transaction,
    routes: []const struct { participant: u16, table_name: []const u8 },
    pub fn validate(self: @This()) !void {
        try self.transaction.validate();
        if (self.routes.len != self.transaction.captures.len) return error.InvalidLogicalPublisher;
        for (self.routes, self.transaction.captures) |route, reference| {
            if (route.participant != reference.participant or route.table_name.len == 0 or route.table_name.len > 1024 or !std.unicode.utf8ValidateSlice(route.table_name) or std.mem.indexOfScalar(u8, route.table_name, 0) != null) return error.InvalidLogicalPublisher;
        }
        if (self.xid < 3 or self.begin_lsn == 0 or self.end_lsn <= self.begin_lsn) return error.InvalidLogicalPublisher;
    }
    pub fn commitLsn(self: @This()) u64 {
        return self.end_lsn - 1;
    }
};
pub const Slot = struct {
    reference: SlotRef,
    temporary_connection: ?[32]u8 = null,
    restart_lsn: u64,
    confirmed_flush_lsn: u64,
    applied_lsn: u64,
    /// Maximum complete journal boundary handed to this exact lease. Reading
    /// a head/keepalive does not grant permission to acknowledge unseen data.
    offered_lsn: u64,
    lease: ?Lease = null,
    lease_highwater: u64 = 0,
    lease_observed_at_unix_ns: u64 = 0,
    lease_expires_at_unix_ns: u64 = 0,
    pub fn validate(self: @This()) !void {
        try self.reference.validate();
        if (self.restart_lsn == 0 or self.restart_lsn != self.confirmed_flush_lsn or
            self.applied_lsn > self.confirmed_flush_lsn or self.confirmed_flush_lsn > self.offered_lsn) return error.InvalidLogicalPublisher;
        if (self.temporary_connection) |connection| if (std.mem.allEqual(u8, &connection, 0)) return error.InvalidLogicalPublisher;
        if (self.lease) |lease| {
            try lease.validate();
            if (!lease.slot.eql(self.reference) or lease.generation != self.lease_highwater or
                self.lease_observed_at_unix_ns == 0 or self.lease_expires_at_unix_ns <= self.lease_observed_at_unix_ns or
                (self.temporary_connection != null and !std.mem.eql(u8, &self.temporary_connection.?, &lease.connection))) return error.InvalidLogicalPublisher;
        } else if (self.lease_observed_at_unix_ns != 0 or self.lease_expires_at_unix_ns != 0) return error.InvalidLogicalPublisher;
    }
};
pub const SlotPage = struct { slots: []const Slot, next_after: ?[]const u8 = null };
pub const FeedPage = struct {
    /// Actual canonical publication IDs resolved by the accepting native
    /// control. Names alone never retarget historic captured memberships.
    publications: []const Publication = &.{},
    realm: Realm,
    lease: Lease,
    head_lsn: u64,
    /// The writer records this complete scanned boundary before exposing the
    /// page. Filtering may omit rows, never alter the acknowledged boundary.
    offered_lsn: u64,
    entries: []const JournalEntry,
};
pub const CreatedSlot = struct {
    slot: Slot,
    consistent_point: u64,
    /// Set only by the owner retaining a genuine SQL snapshot cut. A native
    /// custody/drain lease or exported PG token from another provider is not it.
    snapshot_name: ?[]const u8 = null,
};
pub const Observation = union(enum) {
    outbox_permission: ?OutboxRead,
    control_head: struct { realm: Realm, head: u64, database_name: []const u8 },
    gc_work: GcWorkPage,
    gc_permission: ?GcPermission,
    capacity_grant: ?@import("logical_replication_capture.zig").CapacityGrant,
    capacity_abort_permission: ?@import("logical_replication_capacity.zig").AbortPermission,
    capacity_reconciled: Digest,
    collected: struct { debt_digest: Digest, action: GcAction },
    participant: ?ParticipantCapture,
    capture_permission: ?@import("logical_replication_capture.zig").Request,
    roster: Roster,
    identity: PublisherIdentity,
    head: struct { realm: Realm, end_lsn: u64 },
    slots: SlotPage,
    slot: ?Slot,
    created: CreatedSlot,
    feed: FeedPage,
    released: void,
};
pub fn validateSlotName(name: []const u8) !void {
    if (name.len == 0 or name.len > 63) return error.InvalidReplicationSlotName;
    for (name) |byte| if (!(byte >= 'a' and byte <= 'z') and !std.ascii.isDigit(byte) and byte != '_') return error.InvalidReplicationSlotName;
}
fn zero(digest: Digest) bool {
    return std.mem.allEqual(u8, &digest, 0);
}
fn identifier(name: []const u8) !void {
    if (name.len == 0 or name.len > 1024 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidLogicalPublisher;
}

test "logical publisher aggregate roster budget counts actual escaping before retaining another relation" {
    const a = std.testing.allocator;
    const members = try a.alloc(Membership, max_publications);
    defer a.free(members);
    for (members, 0..) |*member, index| member.* = .{ .publication = .{ .id = index + 1, .generation = 1, .digest = @splat(1) }, .parameters = .{} };
    const actual: PublishedRelation = .{ .relation = .{ .logical_relation_id = 1, .physical_table_id = 2, .database_id = 3, .namespace_id = 4, .relation_oid = 5, .namespace = "public", .name = "quoted\"relation", .schema_version = 1, .schema_digest = @splat(1), .replica_identity = .default_key, .columns = &.{.{ .attnum = 1, .name = "id", .type_oid = 20, .key = true }} }, .memberships = members };
    var budget: RosterBudget = .{};
    try budget.add(actual);
    const first = budget.encoded_relations;
    var counter = std.Io.Writer.Discarding.init(&.{});
    try std.json.Stringify.value(actual, .{}, &counter.writer);
    try std.testing.expectEqual(counter.fullCount(), first);
    var refused = false;
    for (0..max_relations) |_| {
        const count = budget.count;
        budget.add(actual) catch |err| {
            try std.testing.expectEqual(error.LogicalPublisherRosterTooLarge, err);
            try std.testing.expectEqual(count, budget.count);
            refused = true;
            break;
        };
    }
    try std.testing.expect(refused);
    try std.testing.expect(budget.encoded_relations <= max_roster_bytes);
}

/// Fixed observation returned through the checked physical owner boundary.
pub const GcNativeOutcome = struct {
    observation: ?GcObservation,
    position: position.Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
};

/// A private ordered control cannot smuggle rows, transaction decisions or
/// another control. Every new BatchRequest field is classified by equality to
/// its actual default, preserving explicit row-version languages separately.
pub fn validateCollectionBatch(request: anytype) !void {
    const permission = request.logical_publication_gc orelse return;
    try permission.validate();
    const defaults: @TypeOf(request) = .{};
    inline for (@typeInfo(@TypeOf(request)).@"struct".field_names) |name| {
        if (comptime std.mem.eql(u8, name, "logical_publication_gc")) continue;
        if (!@import("../storage/control_field_defaults.zig").isolatedFieldMatchesDefault(name, @field(request, name), @field(defaults, name))) return error.InvalidBatchRequest;
    }
}

pub const GcWork = struct {
    database_id: u64,
    database_name: []const u8,
    permission: ?GcPermission = null,
    abort_permission: ?@import("logical_replication_capacity.zig").AbortPermission = null,
    collect: bool = false,
};
pub const GcWorkPage = struct { work: ?GcWork, next_after: ?u64 = null };

test "logical publisher collection absence requires exact accepted purge and a true later same-image cut" {
    const a = std.testing.allocator;
    const realm: Realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 1 };
    const ref: CaptureRef = .{
        .owner = .{ .metadata_incarnation = realm.incarnation, .operation_id = @splat(1), .admission_digest = @splat(2), .expires_at_unix_ns = 100 },
        .transaction_id = @splat(3),
        .participant = 0,
        .group_id = 4,
        .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 },
        .physical_root_incarnation = 9,
        .position = .{ .native = .{ .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .sequence = 5 } },
        .roster_digest = @splat(4),
        .guard_digest = @splat(5),
        .pregrant_body_digest = @splat(6),
        .changes_digest = @splat(7),
        .capacity_digest = @splat(8),
        .before_digest = @splat(9),
        .change_count = 1,
        .encoded_bytes = 99,
    };
    const debt: GcDebt = .{ .reference = ref, .table_name = "physical:retained", .reason = .{ .journal = 20 }, .decision_version = 12 };
    const purge: GcObservation = .{ .action = .purge, .debt_digest = try debt.digest(a), .reference_digest = try captureReferenceDigest(a, ref), .header_digest = @splat(8), .physical_root_incarnation = 9, .position = .{ .native = .{ .namespace = ref.namespace, .sequence = 6 } }, .payload_bytes = ref.encoded_bytes };
    const permission: GcPermission = .{ .realm = realm, .debt = debt, .action = .clear_marker, .purged = purge, .purged_digest = try purge.digest(a) };
    const absent: GcObservation = .{ .action = .clear_marker, .evidence = .absence, .debt_digest = purge.debt_digest, .reference_digest = purge.reference_digest, .header_digest = purge.header_digest, .physical_root_incarnation = 9, .position = .{ .native = .{ .namespace = ref.namespace, .sequence = 7 } }, .payload_bytes = ref.encoded_bytes, .purged_digest = permission.purged_digest };
    try absent.require(permission, a);
    var changed = absent;
    changed.position = purge.position;
    try std.testing.expectError(error.InvalidLogicalOutbox, changed.require(permission, a));
    changed = absent;
    changed.physical_root_incarnation += 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, changed.require(permission, a));
    changed = absent;
    changed.header_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, changed.require(permission, a));
    changed = absent;
    changed.purged_digest.?[0] ^= 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, changed.require(permission, a));
    changed = absent;
    changed.payload_bytes += 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, changed.require(permission, a));
    var altered = permission;
    altered.debt.reference.participant += 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, absent.require(altered, a));
    altered = permission;
    altered.debt.reference.owner.operation_id[0] ^= 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, absent.require(altered, a));
}
