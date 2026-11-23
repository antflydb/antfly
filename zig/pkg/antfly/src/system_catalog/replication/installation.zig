// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Capture custody belongs to its existing catalog publication operation. The
//! active point index is only an address: every use rejoins this full immutable
//! body, its signed owner receipts and current authoritative catalog heads.
const std = @import("std");
const local = @import("antfly_local_sources");
const capture = local.system_catalog_logical_replication_capture;
const publisher = local.system_catalog_logical_replication_publisher;
const catalog = local.system_catalog_logical_replication_catalog;
const proofs = @import("../../api/logical_replication_publisher_attestation.zig");
const A = std.mem.Allocator;
pub const max_bytes = catalog.max_record_bytes;
pub const Owner = union(enum) {
    publication: u64,
    relation: u64,

    pub fn id(self: @This()) u64 {
        return switch (self) {
            inline else => |value| value,
        };
    }

    pub fn validate(self: @This()) !void {
        if (self.id() == 0 or self.id() > local.system_catalog_domain.max_relation_oid) return error.InvalidLogicalCaptureControl;
    }
};
pub const Phase = enum { preparing, prepared, published, active, canceling, canceled };
pub const Reference = struct {
    owner: Owner,
    operation_id: [16]u8,
    plan_digest: [32]u8,
    binding_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        if (zero(self.operation_id) or zero(self.plan_digest) or zero(self.binding_digest)) return error.InvalidLogicalCaptureControl;
    }
};
pub const Member = struct {
    binding: capture.Binding,
    observed: proofs.ObservationReceipt,
    /// Frozen post-publication roster. Nil means genuine aggregate removal,
    /// never unknown membership or an unmaterialized relation identity.
    roster: ?publisher.PublishedRelation,
    previous: ?Reference = null,
    pub fn validate(self: @This(), a: A) !void {
        try self.binding.validate();
        try self.observed.validate(self.observed.value.request, a);
        if (!(try self.observed.value.binding()).eql(self.binding)) return error.LogicalCaptureControlChanged;
        if (self.previous) |prior| try prior.validate();
        if (self.binding.next.empty() != (self.roster == null)) return error.InvalidLogicalCaptureControl;
        if (self.roster) |roster| {
            try roster.validate();
            const relation = roster.relation;
            if (relation.physical_table_id != self.binding.physical_table_id or relation.logical_relation_id != self.binding.logical_relation_id or
                relation.database_id != self.binding.scope.realm.database_id or relation.schema_version != self.binding.schema_version or
                !std.mem.eql(u8, &relation.schema_digest, &self.binding.schema_digest)) return error.LogicalCaptureControlChanged;
        }
    }
};
/// Initial native admission freezes only metadata-derived facts. The owner
/// observation supplies actual retained AIC bytes afterwards; caller hashes or
/// a fresh catalog compilation can never complete this record.
pub const DraftMember = struct {
    request: capture.ObservationRequest,
    roster: ?publisher.PublishedRelation,
    previous: ?Reference = null,
    pub fn validate(self: @This()) !void {
        try self.request.validate();
        if (self.previous) |prior| try prior.validate();
        if (self.request.next.empty() != (self.roster == null)) return error.InvalidLogicalCaptureControl;
        if (self.roster) |roster| {
            try roster.validate();
            const relation = roster.relation;
            if (relation.physical_table_id != self.request.physical_table_id or
                relation.logical_relation_id != self.request.logical_relation_id or
                relation.database_id != self.request.scope.realm.database_id or
                relation.schema_version != self.request.schema_version or
                !std.mem.eql(u8, &relation.schema_digest, &self.request.schema_digest)) return error.LogicalCaptureControlChanged;
        }
    }
};
pub const Draft = struct {
    scope: capture.Scope,
    previous: ?catalog.Publication,
    desired: ?catalog.Publication,
    members: []const DraftMember,
    pub fn owner(self: @This()) Owner {
        return switch (self.scope.subject) {
            .publication => |value| .{ .publication = value.id },
            .table_change => |value| .{ .relation = value.logical_relation_id },
        };
    }
    pub fn validate(self: @This(), a: A) !void {
        try self.scope.validate();
        try self.owner().validate();
        if (self.members.len > publisher.max_relations) return error.InvalidLogicalCaptureControl;
        var prior_table: u64 = 0;
        var prior_group: u64 = 0;
        for (self.members) |member| {
            try member.validate();
            const request = member.request;
            if (!request.scope.eql(self.scope) or request.physical_table_id < prior_table or
                (request.physical_table_id == prior_table and request.owner_group_id <= prior_group)) return error.InvalidLogicalCaptureControl;
            prior_table = request.physical_table_id;
            prior_group = request.owner_group_id;
        }
        // Publication identity/options and generation are independent of AIC.
        // Validate these through the same final-plan structural rules using
        // a bounded empty member roster, without inventing an owner receipt.
        if (self.owner() == .publication) {
            const shell: Plan = .{ .scope = self.scope, .previous = self.previous, .desired = self.desired, .members = &.{} };
            try shell.validate(a);
        } else if (self.previous != null or self.desired != null or self.members.len == 0) return error.InvalidLogicalCaptureControl;
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate(a);
        return capture.canonicalDigest(a, "antfly/logical-capture-draft/v1\x00", self);
    }
    /// The accepting writer verifies every observation's CURRENT enrolled
    /// owner/completed-prefix cut before invoking this exact roster join.
    pub fn bind(self: @This(), a: A, observations: []const proofs.ObservationReceipt) !Plan {
        try self.validate(a);
        if (observations.len != self.members.len) return error.LogicalCaptureReceiptPending;
        const members = try a.alloc(Member, self.members.len);
        for (self.members, members, observations) |draft_member, *member, observed| {
            try observed.validate(draft_member.request, a);
            member.* = .{ .binding = try observed.value.binding(), .observed = observed, .roster = draft_member.roster, .previous = draft_member.previous };
        }
        const plan: Plan = .{ .scope = self.scope, .previous = self.previous, .desired = self.desired, .members = members };
        try plan.validate(a);
        return plan;
    }
};

pub const Plan = struct {
    scope: capture.Scope,
    previous: ?catalog.Publication,
    desired: ?catalog.Publication,
    members: []const Member,
    pub fn owner(self: @This()) Owner {
        return switch (self.scope.subject) {
            .publication => |value| .{ .publication = value.id },
            .table_change => |value| .{ .relation = value.logical_relation_id },
        };
    }
    pub fn validate(self: @This(), a: A) !void {
        try self.scope.validate();
        const actual_owner = self.owner();
        try actual_owner.validate();
        const id = actual_owner.id();
        if (self.members.len > publisher.max_relations) return error.InvalidLogicalCaptureControl;
        if (actual_owner == .publication) {
            if (self.previous == null and self.desired == null) return error.InvalidLogicalCaptureControl;
        } else if (self.previous != null or self.desired != null or self.members.len == 0) return error.InvalidLogicalCaptureControl;
        for ([_]?catalog.Publication{ self.previous, self.desired }) |maybe| if (maybe) |record| {
            try record.validate(a);
            if (!record.active or record.capture_operation != null or record.id != id or record.database_id != self.scope.realm.database_id) return error.InvalidLogicalCaptureControl;
        };
        if (self.scope.subject == .publication) {
            const generation = if (self.desired) |next| next.generation else std.math.add(u64, self.previous.?.generation, 1) catch return error.InvalidLogicalCaptureControl;
            if (self.scope.subject.publication.generation != generation) return error.InvalidLogicalCaptureControl;
        }
        if (self.previous) |before| if (self.desired) |after| {
            const generation = std.math.add(u64, before.generation, 1) catch return error.InvalidLogicalCaptureControl;
            if (after.generation != generation or
                !std.mem.eql(u8, before.name, after.name) or !sameOwner(before.owner, after.owner)) return error.InvalidLogicalCaptureControl;
        };
        var previous_table: u64 = 0;
        var previous_group: u64 = 0;
        for (self.members) |member| {
            try member.validate(a);
            const binding = member.binding;
            if (!binding.scope.eql(self.scope) or binding.physical_table_id < previous_table or
                (binding.physical_table_id == previous_table and binding.owner_group_id <= previous_group)) return error.InvalidLogicalCaptureControl;
            previous_table = binding.physical_table_id;
            previous_group = binding.owner_group_id;
        }
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate(a);
        return capture.canonicalDigest(a, "antfly/logical-publication-installation/v1\x00", self);
    }
    pub fn reference(self: @This(), a: A, index: usize) !Reference {
        if (index >= self.members.len) return error.InvalidLogicalCaptureControl;
        return .{ .owner = self.owner(), .operation_id = self.scope.operation_id, .plan_digest = try self.digest(a), .binding_digest = try self.members[index].binding.digest(a) };
    }
};
pub const Record = struct {
    version: u16 = 1,
    plan: Plan,
    plan_digest: [32]u8,
    draft_digest: ?[32]u8 = null,
    revision: u64,
    phase: Phase,
    prepared: []const proofs.GuardReceipt = &.{},
    activated: []const proofs.GuardReceipt = &.{},
    canceled: []const proofs.GuardReceipt = &.{},
    pub fn init(a: A, plan: Plan) !@This() {
        return .{ .plan = plan, .plan_digest = try plan.digest(a), .revision = 1, .phase = if (plan.members.len == 0) .prepared else .preparing };
    }
    pub fn validate(self: @This(), a: A) !void {
        if (self.version != 1 or self.revision == 0 or !std.mem.eql(u8, &try self.plan.digest(a), &self.plan_digest)) return error.InvalidLogicalCaptureControl;
        try self.validateReceipts(a, self.prepared, .prepare);
        try self.validateReceipts(a, self.activated, .activate);
        try self.validateReceipts(a, self.canceled, .cancel);
        if ((self.phase == .prepared or self.phase == .published or self.phase == .active) and self.prepared.len != self.plan.members.len) return error.InvalidLogicalCaptureControl;
        if (self.activated.len != 0 and self.phase != .published and self.phase != .active) return error.InvalidLogicalCaptureControl;
        if ((self.phase == .active and self.activated.len != self.plan.members.len) or
            (self.phase == .canceled and self.canceled.len != self.plan.members.len) or
            (self.canceled.len != 0 and self.phase != .canceling and self.phase != .canceled)) return error.InvalidLogicalCaptureControl;
    }
    fn order(left: capture.Binding, right: capture.Binding) std.math.Order {
        return switch (std.math.order(left.physical_table_id, right.physical_table_id)) {
            .eq => std.math.order(left.owner_group_id, right.owner_group_id),
            else => |result| result,
        };
    }
    fn lowerBound(values: anytype, binding: capture.Binding, comptime bindingOf: fn (@TypeOf(values[0])) capture.Binding) usize {
        var low: usize = 0;
        var high = values.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (order(bindingOf(values[middle]), binding) == .lt) low = middle + 1 else high = middle;
        }
        return low;
    }
    fn memberBinding(member: Member) capture.Binding {
        return member.binding;
    }
    fn receiptBinding(receipt: proofs.GuardReceipt) capture.Binding {
        return receipt.value.binding;
    }
    fn memberIndex(self: @This(), binding: capture.Binding) !usize {
        const index = lowerBound(self.plan.members, binding, memberBinding);
        if (index == self.plan.members.len or !self.plan.members[index].binding.eql(binding)) return error.LogicalCaptureControlChanged;
        return index;
    }
    fn validateReceipts(self: @This(), a: A, receipts: []const proofs.GuardReceipt, action: capture.Action) !void {
        if (receipts.len > self.plan.members.len) return error.InvalidLogicalCaptureControl;
        var previous: ?capture.Binding = null;
        for (receipts) |receipt| {
            const binding = receipt.value.binding;
            if (previous) |prior| if (order(prior, binding) != .lt) return error.InvalidLogicalCaptureControl;
            const index = try self.memberIndex(binding);
            try receipt.validate(.{ .binding = binding, .action = action }, a);
            try self.requireReceiptCut(index, receipt);
            previous = binding;
        }
    }
    fn requireReceiptCut(self: @This(), index: usize, receipt: proofs.GuardReceipt) !void {
        const observed = self.plan.members[index].observed;
        if ((receipt.value.position == .native) != (observed.proof.authority == .native) or
            receipt.value.physical_root_incarnation != observed.value.physical_root_incarnation)
            return error.LogicalCaptureControlChanged;
        if (observed.value.position) |before| try requireNotBefore(receipt.value.position, before);
        if (observed.completed) |prefix| {
            if (receipt.value.position != .raft) return error.LogicalCaptureControlChanged;
            if (receipt.value.position.raft.term < prefix.term or receipt.value.position.raft.index < prefix.applied_index) return error.LogicalCaptureControlChanged;
        }
        if (receipt.value.action == .prepare) return;
        const prepared_index = lowerBound(self.prepared, receipt.value.binding, receiptBinding);
        if (prepared_index < self.prepared.len and self.prepared[prepared_index].value.binding.eql(receipt.value.binding)) {
            const before = self.prepared[prepared_index].value.position;
            try requireNotBefore(receipt.value.position, before);
            if (std.meta.eql(receipt.value.position, before)) return error.LogicalCaptureControlChanged;
        } else if (receipt.value.action == .activate) return error.LogicalCaptureControlChanged;
    }
    /// Native caller first rejoins CURRENT enrollment/placement/source-cut and
    /// callback capability. The signature-only model never authorizes receipt
    /// admission. Null physical PREPARE is pending and cannot call this method.
    pub fn accept(self: @This(), a: A, receipt: proofs.GuardReceipt) !@This() {
        try self.validate(a);
        const member_index = try self.memberIndex(receipt.value.binding);
        try self.requireReceiptCut(member_index, receipt);
        const action = receipt.value.action;
        try receipt.validate(.{ .binding = receipt.value.binding, .action = action }, a);
        const values = switch (action) {
            .prepare => self.prepared,
            .activate => self.activated,
            .cancel => self.canceled,
        };
        const insertion = lowerBound(values, receipt.value.binding, receiptBinding);
        if (insertion < values.len and values[insertion].value.binding.eql(receipt.value.binding)) {
            if (!std.mem.eql(u8, &try values[insertion].digest(a), &try receipt.digest(a))) return error.LogicalCaptureControlChanged;
            return self;
        }
        switch (action) {
            .prepare => if (self.phase != .preparing) return error.LogicalCaptureControlChanged,
            .activate => if (self.phase != .published) return error.LogicalCaptureControlChanged,
            .cancel => if (self.phase != .canceling) return error.LogicalCaptureControlChanged,
        }
        const next_values = try a.alloc(proofs.GuardReceipt, values.len + 1);
        @memcpy(next_values[0..insertion], values[0..insertion]);
        next_values[insertion] = receipt;
        @memcpy(next_values[insertion + 1 ..], values[insertion..]);
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.InvalidLogicalCaptureControl;
        switch (action) {
            .prepare => {
                next.prepared = next_values;
                if (next_values.len == self.plan.members.len) next.phase = .prepared;
            },
            .activate => {
                next.activated = next_values;
                if (next_values.len == self.plan.members.len) next.phase = .active;
            },
            .cancel => {
                next.canceled = next_values;
                if (next_values.len == self.plan.members.len) next.phase = .canceled;
            },
        }
        try next.validate(a);
        return next;
    }
    /// The actual native writer must check old P table counts AND genuine
    /// prepared owner receipts before atomically publishing target heads here.
    pub fn published(self: @This(), a: A) !@This() {
        try self.validate(a);
        if (self.phase == .published or self.phase == .active) return self;
        if (self.phase != .prepared) return error.LogicalCaptureControlChanged;
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.InvalidLogicalCaptureControl;
        next.phase = if (self.plan.members.len == 0) .active else .published;
        try next.validate(a);
        return next;
    }
    pub fn cancel(self: @This(), a: A) !@This() {
        try self.validate(a);
        if (self.phase == .canceling or self.phase == .canceled) return self;
        // Once target heads are published the irreversible activation finishes;
        // it cannot restore an old policy under already-admitted new writers.
        if (self.phase == .published or self.phase == .active) return error.LogicalCaptureControlChanged;
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.InvalidLogicalCaptureControl;
        next.phase = if (self.plan.members.len == 0) .canceled else .canceling;
        try next.validate(a);
        return next;
    }
};
fn zero(value: anytype) bool {
    return std.mem.allEqual(u8, &value, 0);
}

fn sameOwner(left: @FieldType(catalog.Publication, "owner"), right: @FieldType(catalog.Publication, "owner")) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .auth_disabled => true,
        .user, .role => |principal| blk: {
            const other = switch (right) {
                .user, .role => |value| value,
                .auth_disabled => unreachable,
            };
            break :blk std.mem.eql(u8, principal.name, other.name) and std.mem.eql(u8, &principal.instance_id, &other.instance_id);
        },
    };
}

fn requireNotBefore(current: local.storage_db_receipt_position.Position, before: local.storage_db_receipt_position.Position) !void {
    try current.validate();
    try before.validate();
    const ordered = switch (current) {
        .raft => |cut| before == .raft and cut.term >= before.raft.term and cut.index >= before.raft.index,
        .native => |cut| before == .native and cut.namespace.eql(before.native.namespace) and cut.sequence >= before.native.sequence,
    };
    if (!ordered) return error.LogicalCaptureControlChanged;
}

/// A work address is only a scheduler cursor. Every mutation rejoins the
/// authoritative journal and uses the caller's native metadata transaction.
pub const Address = struct {
    owner: Owner,
    operation: [16]u8,

    fn from(scope: capture.Scope, owner: Owner) Address {
        return .{ .owner = owner, .operation = scope.operation_id };
    }
};

pub const Journal = union(enum) {
    observing: Draft,
    abandoned: Draft,
    bound: Record,

    pub fn scope(self: Journal) capture.Scope {
        return switch (self) {
            .observing, .abandoned => |draft| draft.scope,
            .bound => |record| record.plan.scope,
        };
    }

    fn address(self: Journal) Address {
        return Address.from(self.scope(), switch (self) {
            .observing, .abandoned => |draft| draft.owner(),
            .bound => |record| record.plan.owner(),
        });
    }

    fn validate(self: Journal, a: A) !void {
        switch (self) {
            .observing, .abandoned => |draft| try draft.validate(a),
            .bound => |record| {
                try record.validate(a);
                if (record.draft_digest == null or zero(record.draft_digest.?)) return error.InvalidLogicalCaptureControl;
            },
        }
    }
};

pub const Advance = union(enum) {
    bind: []const proofs.ObservationReceipt,
    receipt: proofs.GuardReceipt,
    publish,
    cancel,
};

pub const Command = struct {
    address: Address,
    incarnation: [16]u8,
    expected_revision: u64,
    action: Advance,

    pub fn validate(self: Command) !void {
        try self.address.owner.validate();
        if (zero(self.incarnation) or zero(self.address.operation) or
            (self.expected_revision == 0) != (self.action == .bind)) return error.InvalidLogicalCaptureControl;
        if (self.action == .bind and self.action.bind.len > publisher.max_relations) return error.CatalogCommandTooLarge;
    }
};

pub fn operationKey(buffer: []u8, group: u64, address: Address) ![]const u8 {
    return addressKey(buffer, group, "operation", address);
}

pub fn workKey(buffer: []u8, group: u64, address: Address) ![]const u8 {
    return addressKey(buffer, group, "work", address);
}

fn workPrefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidLogicalCaptureControl;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-capture:work:", .{group});
}

fn addressKey(buffer: []u8, group: u64, kind: []const u8, address: Address) ![]const u8 {
    try address.owner.validate();
    if (group == 0 or zero(address.operation)) return error.InvalidLogicalCaptureControl;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-capture:{s}:{s}:{x:0>16}:{s}", .{
        group, kind, @tagName(address.owner), address.owner.id(), std.fmt.bytesToHex(address.operation, .lower),
    });
}

pub fn requireUnpublished(txn: anytype, group: u64, plan: Plan) !void {
    var buffer: [256]u8 = undefined;
    const key = try addressKey(&buffer, group, "published", Address.from(plan.scope, plan.owner()));
    if ((try get(txn, key)) != null) return error.LogicalCaptureControlChanged;
}

pub fn putPublishedRevision(txn: anytype, group: u64, plan: Plan, revision: u64) !void {
    var buffer: [256]u8 = undefined;
    const key = try addressKey(&buffer, group, "published", Address.from(plan.scope, plan.owner()));
    if (revision == 0) return error.InvalidLogicalCaptureControl;
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, revision, .big);
    try txn.put(key, &encoded);
}

pub fn publishedRevision(txn: anytype, group: u64, address: Address) !u64 {
    var buffer: [256]u8 = undefined;
    const bytes = (try get(txn, try addressKey(&buffer, group, "published", address))) orelse return error.LogicalCaptureControlChanged;
    if (bytes.len != 8) return error.InvalidLogicalCaptureControl;
    return std.mem.readInt(u64, bytes[0..8], .big);
}

fn ownerKey(buffer: []u8, group: u64, owner: Owner) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-capture:owner-fence:{s}:{x:0>16}", .{ group, @tagName(owner), owner.id() });
}

fn databaseKey(buffer: []u8, group: u64, database: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-capture:database-fence:{x:0>16}", .{ group, database });
}

fn coversDatabase(previous: ?catalog.Publication, desired: ?catalog.Publication) bool {
    return (previous != null and previous.?.all_tables) or (desired != null and desired.?.all_tables);
}

fn memberKey(buffer: []u8, group: u64, table: u64, owner: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-capture:member-fence:{x:0>16}:{x:0>16}", .{ group, table, owner });
}

fn installedKey(buffer: []u8, group: u64, table: u64, owner: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-capture:installed:{x:0>16}:{x:0>16}", .{ group, table, owner });
}

pub const LookupCache = std.AutoHashMapUnmanaged(Address, Record);

const InstalledMember = struct { reference: Reference, member: Member };
fn lookupInstalled(a: A, txn: anytype, group: u64, table: u64, owner: u64, cache: *LookupCache) !?InstalledMember {
    var buffer: [256]u8 = undefined;
    const raw = (try get(txn, try installedKey(&buffer, group, table, owner))) orelse return null;
    if (raw.len > 1024) return error.InvalidLogicalCaptureControl;
    const value = try std.json.parseFromSliceLeaky(Reference, a, raw, .{});
    try value.validate();
    const address: Address = .{ .owner = value.owner, .operation = value.operation_id };
    const entry = try cache.getOrPut(a, address);
    if (!entry.found_existing) {
        const journal = (try read(a, txn, group, address)) orelse return error.LogicalCaptureControlChanged;
        if (journal != .bound or journal.bound.phase != .active) return error.LogicalCaptureControlChanged;
        entry.value_ptr.* = journal.bound;
    }
    const record = entry.value_ptr.*;
    if (!std.mem.eql(u8, &record.plan_digest, &value.plan_digest)) return error.LogicalCaptureControlChanged;
    var low: usize = 0;
    var high = record.plan.members.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const binding = record.plan.members[middle].binding;
        if (binding.physical_table_id < table or (binding.physical_table_id == table and binding.owner_group_id < owner)) low = middle + 1 else high = middle;
    }
    if (low == record.plan.members.len) return error.LogicalCaptureControlChanged;
    const member = record.plan.members[low];
    if (member.binding.physical_table_id != table or member.binding.owner_group_id != owner or member.binding.next.empty() or
        !std.mem.eql(u8, &try member.binding.digest(a), &value.binding_digest)) return error.LogicalCaptureControlChanged;
    return .{ .reference = value, .member = member };
}

pub fn installed(a: A, txn: anytype, group: u64, request: capture.ObservationRequest, cache: *LookupCache) !?Reference {
    const found = (try lookupInstalled(a, txn, group, request.physical_table_id, request.owner_group_id, cache)) orelse {
        if (!request.previous.empty()) return error.LogicalCaptureControlChanged;
        return null;
    };
    const binding = found.member.binding;
    if (!binding.scope.realm.eql(request.scope.realm) or binding.logical_relation_id != request.logical_relation_id or
        !binding.namespace.eql(request.namespace) or !binding.next.eql(request.previous)) return error.LogicalCaptureControlChanged;
    return found.reference;
}

fn get(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

pub fn read(a: A, reader: anytype, group: u64, address: Address) !?Journal {
    var buffer: [256]u8 = undefined;
    const bytes = (try get(reader, try operationKey(&buffer, group, address))) orelse return null;
    if (bytes.len > max_bytes) return error.InvalidLogicalCaptureControl;
    const journal = try std.json.parseFromSliceLeaky(Journal, a, bytes, .{ .allocate = .alloc_always });
    try journal.validate(a);
    if (journal.scope().realm.group_id != group or !std.meta.eql(journal.address(), address)) return error.LogicalCaptureControlChanged;
    return journal;
}

fn put(a: A, txn: anytype, group: u64, journal: Journal) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, journal, .{});
    defer a.free(bytes);
    if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
    var buffer: [256]u8 = undefined;
    try txn.put(try operationKey(&buffer, group, journal.address()), bytes);
}

/// The caller computes the draft from one authoritative catalog cut. Claim
/// the operation and every overlapping physical owner before issuing work.
pub fn begin(a: A, txn: anytype, group: u64, draft: Draft) !Address {
    try draft.validate(a);
    if (draft.scope.realm.group_id != group) return error.LogicalCaptureControlChanged;
    const address = Address.from(draft.scope, draft.owner());
    if (try read(a, txn, group, address)) |previous| {
        const previous_digest = switch (previous) {
            .observing, .abandoned => |stored| try stored.digest(a),
            .bound => |record| record.draft_digest.?,
        };
        if (!std.mem.eql(u8, &try draft.digest(a), &previous_digest)) return error.LogicalCaptureControlChanged;
        return address;
    }
    try requireUnclaimed(txn, group, draft);
    var buffer: [256]u8 = undefined;
    try put(a, txn, group, .{ .observing = draft });
    try txn.put(try ownerKey(&buffer, group, address.owner), &address.operation);
    if (coversDatabase(draft.previous, draft.desired)) try txn.put(try databaseKey(&buffer, group, draft.scope.realm.database_id), &address.operation);
    for (draft.members) |member| try txn.put(try memberKey(&buffer, group, member.request.physical_table_id, member.request.owner_group_id), &address.operation);
    try txn.put(try workKey(&buffer, group, address), &address.operation);
    return address;
}

pub fn requireUnclaimed(txn: anytype, group: u64, draft: Draft) !void {
    var buffer: [256]u8 = undefined;
    if (try get(txn, try ownerKey(&buffer, group, draft.owner())) != null or
        try get(txn, try databaseKey(&buffer, group, draft.scope.realm.database_id)) != null)
        return error.LogicalCaptureControlChanged;
    for (draft.members) |member| {
        if (try get(txn, try memberKey(&buffer, group, member.request.physical_table_id, member.request.owner_group_id)) != null)
            return error.LogicalCaptureControlChanged;
    }
}

fn requireFence(txn: anytype, key: []const u8, operation: [16]u8) !void {
    const current = (try get(txn, key)) orelse return error.LogicalCaptureControlChanged;
    if (!std.mem.eql(u8, current, &operation)) return error.LogicalCaptureControlChanged;
}

/// A draft with no accepted observations has issued no physical prepare.
pub fn abandonObservation(a: A, txn: anytype, group: u64, address: Address) !void {
    const journal = (try read(a, txn, group, address)) orelse return error.LogicalCaptureControlChanged;
    if (journal == .abandoned) return;
    if (journal != .observing) return error.LogicalCaptureControlChanged;
    var buffer: [256]u8 = undefined;
    const draft = journal.observing;
    try requireFence(txn, try ownerKey(&buffer, group, address.owner), address.operation);
    if (coversDatabase(draft.previous, draft.desired)) try requireFence(txn, try databaseKey(&buffer, group, draft.scope.realm.database_id), address.operation);
    try requireFence(txn, try workKey(&buffer, group, address), address.operation);
    for (draft.members) |member| try requireFence(txn, try memberKey(&buffer, group, member.request.physical_table_id, member.request.owner_group_id), address.operation);
    try put(a, txn, group, .{ .abandoned = draft });
    if (coversDatabase(draft.previous, draft.desired)) try txn.delete(try databaseKey(&buffer, group, draft.scope.realm.database_id));
    for (draft.members) |member| try txn.delete(try memberKey(&buffer, group, member.request.physical_table_id, member.request.owner_group_id));
    try txn.delete(try ownerKey(&buffer, group, address.owner));
    try txn.delete(try workKey(&buffer, group, address));
}

/// The verifier must rejoin enrolled owners and the frozen catalog cut in
/// this transaction. Publication of the new catalog heads and the irreversible
/// state transition share the same transaction. Errors require caller abort.
pub fn advance(a: A, txn: anytype, group: u64, address: Address, expected: u64, action: Advance, verifier: anytype) !Record {
    const journal = (try read(a, txn, group, address)) orelse return error.LogicalCaptureControlChanged;
    var buffer: [256]u8 = undefined;
    if (journal == .abandoned) return error.LogicalCaptureControlChanged;
    if (journal == .observing) {
        if (expected != 0 or action != .bind) return error.LogicalCaptureControlChanged;
        try requireFence(txn, try ownerKey(&buffer, group, address.owner), address.operation);
        try requireFence(txn, try workKey(&buffer, group, address), address.operation);
        const draft = journal.observing;
        if (coversDatabase(draft.previous, draft.desired)) try requireFence(txn, try databaseKey(&buffer, group, draft.scope.realm.database_id), address.operation);
        if (action.bind.len != draft.members.len) return error.LogicalCaptureReceiptPending;
        for (draft.members, action.bind) |member, observation| {
            try requireFence(txn, try memberKey(&buffer, group, member.request.physical_table_id, member.request.owner_group_id), address.operation);
            try verifier.observe(txn, member.request, observation);
        }
        var next = try Record.init(a, try draft.bind(a, action.bind));
        next.draft_digest = try draft.digest(a);
        try put(a, txn, group, .{ .bound = next });
        return next;
    }
    const current = journal.bound;
    if (expected != current.revision or action == .bind) return error.LogicalCaptureControlChanged;
    const next = switch (action) {
        .bind => unreachable,
        .receipt => |receipt| try current.accept(a, receipt),
        .publish => try current.published(a),
        .cancel => try current.cancel(a),
    };
    if (next.revision == current.revision) return current;
    const all = coversDatabase(next.plan.previous, next.plan.desired);
    if (all) try requireFence(txn, try databaseKey(&buffer, group, next.plan.scope.realm.database_id), address.operation);
    try requireFence(txn, try ownerKey(&buffer, group, address.owner), address.operation);
    try requireFence(txn, try workKey(&buffer, group, address), address.operation);
    for (next.plan.members) |member| try requireFence(txn, try memberKey(&buffer, group, member.binding.physical_table_id, member.binding.owner_group_id), address.operation);
    switch (action) {
        .receipt => |receipt| try verifier.receipt(txn, current.plan.members[try current.memberIndex(receipt.value.binding)], receipt),
        .publish => try verifier.publish(txn, current),
        .cancel => {},
        .bind => unreachable,
    }
    try put(a, txn, group, .{ .bound = next });
    if (next.phase == .active or next.phase == .canceled) {
        if (all) try txn.delete(try databaseKey(&buffer, group, next.plan.scope.realm.database_id));
        if (next.phase == .active) for (next.plan.members) |member| {
            const binding = member.binding;
            const key = try installedKey(&buffer, group, binding.physical_table_id, binding.owner_group_id);
            if (binding.next.empty()) {
                try txn.delete(key);
            } else {
                const reference: Reference = .{ .owner = address.owner, .operation_id = address.operation, .plan_digest = next.plan_digest, .binding_digest = try binding.digest(a) };
                const encoded = try std.json.Stringify.valueAlloc(a, reference, .{});
                defer a.free(encoded);
                try txn.put(key, encoded);
            }
        };
        for (next.plan.members) |member| try txn.delete(try memberKey(&buffer, group, member.binding.physical_table_id, member.binding.owner_group_id));
        try txn.delete(try ownerKey(&buffer, group, address.owner));
        try txn.delete(try workKey(&buffer, group, address));
    }
    return next;
}

pub fn projectOutcome(a: A, txn: anytype, group: u64, pending: @import("../sequence_outcomes.zig").Pending, digest: [32]u8) !@import("../sequence_outcomes.zig").Outcome {
    const work = pending.work.publication;
    if (work.group != group) return error.InvalidSequenceOutcome;
    const address: Address = .{ .owner = .{ .publication = pending.identity.id }, .operation = work.operation };
    const journal = (try read(a, txn, group, address)) orelse return error.InvalidSequenceOutcome;
    const scope = journal.scope();
    if (!std.mem.eql(u8, &scope.realm.incarnation, &std.fmt.bytesToHex(work.incarnation, .lower)) or
        !std.mem.eql(u8, &scope.change_digest, &digest) or
        scope.subject.publication.generation != pending.identity.generation) return error.InvalidSequenceOutcome;
    if (journal == .abandoned or (journal == .bound and journal.bound.phase == .canceled))
        return .{ .rejected = .TableTransitionActive };
    if (journal != .bound or journal.bound.phase != .active) return .{ .pending = pending };
    const revision = try publishedRevision(txn, group, address);
    const meta = try @import("../storage.zig").readMeta(a, txn, group);
    if (revision <= pending.revision or revision > meta.revision) return error.InvalidSequenceOutcome;
    const dropped = journal.bound.plan.desired == null;
    return .{ .success = .{ .revision = revision, .identity = if (dropped) null else pending.identity, .removed = @intFromBool(dropped) } };
}

pub const DemandQuery = struct {
    route: local.metadata_catalog_route_contract.CatalogRouteFence,
    pregrant_body_digest: [32]u8,
    request_digest: [32]u8,
};

pub const ResolvedDemand = struct {
    reference: capture.DemandReference,
    roster: publisher.PublishedRelation,
};

pub fn requireSettled(txn: anytype, group: u64, database: u64, table: u64, owner: u64) !void {
    var key: [256]u8 = undefined;
    if (try get(txn, try databaseKey(&key, group, database)) != null or
        try get(txn, try memberKey(&key, group, table, owner)) != null)
        return error.LogicalCapturePublicationPending;
}

pub const DemandRoute = local.metadata_catalog_route_contract.CatalogRouteFence;
pub const DemandBinding = struct {
    realm: publisher.Realm,
    guard_digest: [32]u8,
    roster_digest: [32]u8,
    roster: publisher.PublishedRelation,

    pub fn bind(self: @This(), body: [32]u8, request: [32]u8) !ResolvedDemand {
        const reference: capture.DemandReference = .{
            .realm = self.realm,
            .guard_digest = self.guard_digest,
            .roster_digest = self.roster_digest,
            .pregrant_body_digest = body,
            .request_digest = request,
        };
        try reference.validate();
        return .{ .reference = reference, .roster = self.roster };
    }
};

pub fn resolveDemand(a: A, txn: anytype, realm: publisher.Realm, table: local.system_catalog_domain.Resource, database: local.system_catalog_domain.Resource, description: ?publisher.RelationDescription, query: DemandQuery) !?ResolvedDemand {
    const binding = (try resolveDemandBinding(a, txn, realm, table, database, description, query.route)) orelse return null;
    return try binding.bind(query.pregrant_body_digest, query.request_digest);
}

pub fn resolveDemandBinding(a: A, txn: anytype, realm: publisher.Realm, table: local.system_catalog_domain.Resource, database: local.system_catalog_domain.Resource, description: ?publisher.RelationDescription, route: DemandRoute) !?DemandBinding {
    try realm.validate();
    const owner = route.route.group_id;
    const group = realm.group_id;
    try requireSettled(txn, group, database.id, table.id, owner);
    const heads: capture.Heads = .{ .table = table.sql_publications, .database = database.sql_all_publications };
    var cache: LookupCache = .empty;
    defer cache.deinit(a);
    const installed_member = (try lookupInstalled(a, txn, group, table.id, owner, &cache)) orelse {
        if (!heads.empty()) return error.LogicalCaptureControlChanged;
        return null;
    };
    const current = description orelse return error.LogicalCaptureControlChanged;
    const binding = installed_member.member.binding;
    if (heads.empty() or !binding.next.eql(heads) or !binding.scope.realm.eql(realm) or
        table.sql_relation_id != binding.logical_relation_id or
        binding.namespace.table_id != route.route.identity_namespace.table_id or
        binding.namespace.shard_id != route.route.identity_namespace.shard_id or
        binding.namespace.range_id != route.route.identity_namespace.range_id or binding.schema_version != current.schema_version or
        !std.mem.eql(u8, &binding.schema_digest, &current.schema_digest))
        return error.LogicalCaptureControlChanged;
    var roster = installed_member.member.roster orelse return error.InvalidLogicalCaptureControl;
    roster.relation.namespace = current.namespace;
    roster.relation.name = current.name;
    if (!std.mem.eql(u8, &try capture.canonicalDigest(a, "antfly/capture-description/v1\x00", roster.relation), &try capture.canonicalDigest(a, "antfly/capture-description/v1\x00", current))) return error.LogicalCaptureControlChanged;
    return .{
        .realm = realm,
        .guard_digest = installed_member.reference.binding_digest,
        .roster_digest = try capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", roster),
        .roster = roster,
    };
}

pub fn requireDatabaseSettled(txn: anytype, group: u64, database: u64) !void {
    var key: [256]u8 = undefined;
    if (try get(txn, try databaseKey(&key, group, database)) != null) return error.LogicalCapturePublicationPending;
}

const slot_journal = @import("decision_journal.zig");
pub const PublisherView = struct {
    realm: publisher.Realm,
    control_head: u64,
};

pub const SlotRead = union(enum) {
    database: []const u8,
    identity: publisher.Realm,
    snapshot_point: publisher.Realm,
    subscription_lease: struct { realm: publisher.Realm, subscription_id: u64 },
    subscription_apply: struct {
        realm: publisher.Realm,
        subscription_id: u64,
        birth: u64,
        origin: publisher.Digest,
        targets: []const local.storage_db_doc_identity_namespace.Namespace = &.{},
        destination: ?struct { namespace: []const u8, relation: []const u8, rights: u16 } = null,
    },
    subscription_cleanup_definition: struct { realm: publisher.Realm, item: @import("../../metadata/replication_subscription_work.zig").Item, disabling: bool = false },
    subscription_wal_definition: struct {
        realm: publisher.Realm,
        reference: local.api_replication_source_reference.Reference,
        checkpoint: u64,
    },
    subscription_capture_definition: struct { realm: publisher.Realm, lease: @import("../../metadata/replication_subscription_work.zig").Lease, attempt: ?[16]u8 = null },
    subscription_permission: struct {
        realm: publisher.Realm,
        lease: @import("antfly_local_sources").system_catalog_logical_replication_catalog.SubscriptionWorkLease,
        attempt: [16]u8,
        origin: ?publisher.Digest = null,
    },
    ready: slot_journal.FeedProbe,
    check_publications: struct { realm: publisher.Realm, names: []const []const u8 },
    head: publisher.Realm,
    lookup: struct { realm: publisher.Realm, name: []const u8 },
    outcome: slot_journal.ControlRequest,
    page: slot_journal.ControlRequest,
};

pub const DdlQuery = struct { database: []const u8, name: []const u8 };
pub const DdlView = struct {
    revision: u64,
    incarnation: [16]u8,
    database_id: u64,
    head: u64,
    definition: ?@import("logical_replication.zig").Publication,
};

pub const SubscriptionApplyWork = struct {
    realm: publisher.Realm,
    source: @import("antfly_local_sources").api_replication_source_reference.Reference,
};

pub const WorkRequest = struct {
    subscription: ?struct { after: ?u64 = null, recovery: ?struct { after: ?[16]u8 = null } = null } = null,
    ddl: ?DdlQuery = null,
    slot: ?SlotRead = null,
    collection: ?@import("decision_journal.zig").CollectionRead = null,
    feed_capture: ?publisher.OutboxRead = null,
    after: ?Address = null,
    permission: ?capture.Request = null,
    demand: ?DemandQuery = null,
    demand_routes: ?[]const DemandRoute = null,

    pub fn jsonStringify(self: WorkRequest, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(WorkRequest).@"struct".field_names) |field| {
            if ((!std.mem.eql(u8, field, "collection") or self.collection != null) and
                (!std.mem.eql(u8, field, "feed_capture") or self.feed_capture != null) and
                (!std.mem.eql(u8, field, "slot") or self.slot != null))
            {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
};

pub fn permission(a: A, txn: anytype, group: u64, incarnation: [32]u8, request: capture.Request) !bool {
    const target = switch (request) {
        .observe => |value| value,
        .apply => |value| capture.ObservationRequest.fromBinding(value.binding),
    };
    try target.validate();
    if (request == .apply) try request.apply.validate();
    if (target.scope.realm.group_id != group or !std.mem.eql(u8, &target.scope.realm.incarnation, &incarnation)) return false;
    const owner: Owner = switch (target.scope.subject) {
        .publication => |value| .{ .publication = value.id },
        .table_change => |value| .{ .relation = value.logical_relation_id },
    };
    const address = Address.from(target.scope, owner);
    const journal = (try read(a, txn, group, address)) orelse return false;
    if (!journal.scope().eql(target.scope)) return false;
    const matches = switch (request) {
        .observe => journal == .observing and found: {
            for (journal.observing.members) |member| if (member.request.eql(target)) break :found true;
            break :found false;
        },
        .apply => journal == .bound and found: {
            const record = journal.bound;
            const phase_allowed = switch (request.apply.action) {
                .prepare => record.phase == .preparing or record.phase == .prepared,
                .activate => record.phase == .published,
                .cancel => record.phase == .canceling,
            };
            if (!phase_allowed) break :found false;
            _ = record.memberIndex(request.apply.binding) catch break :found false;
            break :found true;
        },
    };
    if (!matches) return false;
    var key: [256]u8 = undefined;
    try requireFence(txn, try ownerKey(&key, group, address.owner), address.operation);
    try requireFence(txn, try memberKey(&key, group, target.physical_table_id, target.owner_group_id), address.operation);
    try requireFence(txn, try workKey(&key, group, address), address.operation);
    return true;
}
pub const max_work_bytes = max_bytes + publisher.max_relations * 512;

pub const ObservationTask = struct { table: []const u8, request: capture.ObservationRequest };
pub const ControlTask = struct { table: []const u8, request: capture.Control };
pub const Work = struct {
    address: Address,
    incarnation: [32]u8,
    revision: u64,
    action: union(enum) {
        observe: []const ObservationTask,
        apply: ControlTask,
        publish,
    },
};

fn parseWorkAddress(key: []const u8, operation: []const u8) !Address {
    if (operation.len != 16) return error.InvalidLogicalCaptureControl;
    var parts = std.mem.splitScalar(u8, key, ':');
    const tag = parts.next() orelse return error.InvalidLogicalCaptureControl;
    const hex_id = parts.next() orelse return error.InvalidLogicalCaptureControl;
    const id = std.fmt.parseInt(u64, hex_id, 16) catch return error.InvalidLogicalCaptureControl;
    const owner: Owner = if (std.mem.eql(u8, tag, "publication")) .{ .publication = id } else if (std.mem.eql(u8, tag, "relation")) .{ .relation = id } else return error.InvalidLogicalCaptureControl;
    return .{ .owner = owner, .operation = operation[0..16].* };
}

/// One keyset seek and at most one authoritative journal per pull. Work is a
/// hint tied to an exact incarnation/revision and grants no owner permission.
pub fn nextWork(a: A, txn: anytype, group: u64, incarnation: [32]u8, after: ?Address, host: anytype) !?Work {
    var prefix_buffer: [128]u8 = undefined;
    const prefix = try workPrefix(&prefix_buffer, group);
    var start_buffer: [256]u8 = undefined;
    const start = if (after) |address| try workKey(&start_buffer, group, address) else prefix;
    const address = blk: {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(start);
        if (entry) |row| if (after != null and std.mem.eql(u8, row.key, start)) {
            entry = try cursor.next();
        };
        const row = entry orelse return null;
        if (!std.mem.startsWith(u8, row.key, prefix)) return null;
        const address = try parseWorkAddress(row.key[prefix.len..], row.value);
        var canonical: [256]u8 = undefined;
        if (!std.mem.eql(u8, row.key, try workKey(&canonical, group, address))) return error.InvalidLogicalCaptureControl;
        break :blk address;
    };
    const journal = (try read(a, txn, group, address)) orelse return error.LogicalCaptureControlChanged;
    if (!std.mem.eql(u8, &journal.scope().realm.incarnation, &incarnation)) return error.LogicalCaptureControlChanged;
    var buffer: [256]u8 = undefined;
    try requireFence(txn, try ownerKey(&buffer, group, address.owner), address.operation);
    switch (journal) {
        .abandoned => return error.LogicalCaptureControlChanged,
        .observing => |draft| {
            const items = try a.alloc(ObservationTask, draft.members.len);
            for (draft.members, items) |member, *item| {
                const request = member.request;
                try requireFence(txn, try memberKey(&buffer, group, request.physical_table_id, request.owner_group_id), address.operation);
                item.* = .{ .request = request, .table = try host.tableName(a, request.physical_table_id) };
            }
            return .{ .address = address, .incarnation = incarnation, .revision = 0, .action = .{ .observe = items } };
        },
        .bound => |record| {
            const pending: struct { receipts: []const proofs.GuardReceipt, action: capture.Action } = switch (record.phase) {
                .preparing => .{ .receipts = record.prepared, .action = .prepare },
                .prepared => return .{ .address = address, .incarnation = incarnation, .revision = record.revision, .action = .publish },
                .published => .{ .receipts = record.activated, .action = .activate },
                .canceling => .{ .receipts = record.canceled, .action = .cancel },
                .active, .canceled => return error.LogicalCaptureControlChanged,
            };
            // Both streams are canonical, so the acknowledged prefix can be
            // skipped without a map or scanning the remainder of the cohort.
            for (record.plan.members, 0..) |member, i| {
                const binding = member.binding;
                if (i < pending.receipts.len and pending.receipts[i].value.binding.eql(binding)) continue;
                try requireFence(txn, try memberKey(&buffer, group, binding.physical_table_id, binding.owner_group_id), address.operation);
                return .{ .address = address, .incarnation = incarnation, .revision = record.revision, .action = .{ .apply = .{
                    .request = .{ .binding = binding, .action = pending.action },
                    .table = try host.tableName(a, binding.physical_table_id),
                } } };
            }
            return error.LogicalCaptureControlChanged;
        },
    }
}

test "SQL logical replication installation journals before work and publishes in the same metadata transaction" {
    const backing = std.testing.allocator;
    var memory = std.heap.ArenaAllocator.init(backing);
    defer memory.deinit();
    const a = memory.allocator();
    const store = local.storage_docstore.DocStore;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/capture-installation", .{tmp.sub_path}, 0);
    const realm: publisher.Realm = .{ .group_id = 7, .incarnation = "0123456789abcdef0123456789abcdef".*, .database_id = 1 };
    const draft: Draft = .{
        .scope = .{ .realm = realm, .subject = .{ .publication = .{ .id = 5, .generation = 1 } }, .operation_id = @splat(2), .change_digest = @splat(3) },
        .previous = null,
        .desired = .{ .id = 5, .generation = 1, .database_id = 1, .name = "empty_pub", .owner = .auth_disabled, .all_tables = false, .tables = &.{}, .parameters = .{} },
        .members = &.{},
    };
    const Routing = struct {
        fn tableName(_: @This(), _: A, _: u64) ![]const u8 {
            return error.UnexpectedOwner;
        }
    };
    const Verifier = struct {
        fn observe(_: @This(), _: *store.Txn, _: capture.ObservationRequest, _: proofs.ObservationReceipt) !void {
            return error.UnexpectedOwner;
        }
        fn receipt(_: @This(), _: *store.Txn, _: Member, _: proofs.GuardReceipt) !void {
            return error.UnexpectedOwner;
        }
        fn publish(_: @This(), txn: *store.Txn, record: Record) !void {
            try std.testing.expectEqual(Phase.prepared, record.phase);
            const storage = @import("../storage.zig");
            const writable = std.testing.allocator;
            const publication = record.plan.desired.?;
            const definition = try std.json.Stringify.valueAlloc(writable, publication, .{});
            defer writable.free(definition);
            var resources = [_]local.system_catalog_domain.Resource{.{
                .kind = .publication,
                .id = publication.id,
                .parent_id = publication.database_id,
                .name = publication.name,
                .source_json = definition,
            }};
            const group = record.plan.scope.realm.group_id;
            const meta = try storage.readMeta(writable, txn, group);
            try storage.applyDelta(writable, txn, group, .{
                .upserts = &resources,
                .removes = &.{},
                .next_id = publication.id + 1,
            }, meta, record.plan_digest);
        }

        fn visible(scratch: A, txn: *store.Txn) !bool {
            const storage = @import("../storage.zig");
            const view: storage.View = .{ .alloc = scratch, .txn = txn, .group_id = 7, .meta = try storage.readMeta(scratch, txn, 7) };
            return (try view.lookup(.publication, 1, "empty_pub")) != null;
        }
    };
    var db = try store.open(backing, path.ptr, .{});
    var open = true;
    defer if (open) db.close();
    const address = blk: {
        var txn = try db.beginWriteTxn();
        errdefer txn.abort();
        const address = try begin(a, &txn, 7, draft);
        try std.testing.expectEqual(address, try begin(a, &txn, 7, draft));
        var conflicting = draft;
        conflicting.scope.operation_id = @splat(4);
        try std.testing.expectError(error.LogicalCaptureControlChanged, begin(a, &txn, 7, conflicting));
        try std.testing.expectError(error.LogicalCaptureControlChanged, advance(a, &txn, 7, address, 0, .publish, Verifier{}));
        try txn.commit();
        break :blk address;
    };
    try db.sync(true);
    db.close();
    open = false;
    db = try store.open(backing, path.ptr, .{});
    open = true;
    {
        var txn = try db.beginWriteTxn();
        errdefer txn.abort();
        const observed = (try nextWork(a, &txn, 7, realm.incarnation, null, Routing{})).?;
        try std.testing.expectEqual(address, observed.address);
        try std.testing.expectEqual(@as(u64, 0), observed.revision);
        try std.testing.expectEqual(@as(usize, 0), observed.action.observe.len);
        try std.testing.expect((try nextWork(a, &txn, 7, realm.incarnation, address, Routing{})) == null);
        try std.testing.expectError(error.LogicalCaptureControlChanged, nextWork(a, &txn, 7, @splat('2'), null, Routing{}));
        const prepared = try advance(a, &txn, 7, address, 0, .{ .bind = &.{} }, Verifier{});
        try std.testing.expectEqual(Phase.prepared, prepared.phase);
        const work = (try nextWork(a, &txn, 7, realm.incarnation, null, Routing{})).?;
        try std.testing.expectEqual(.publish, std.meta.activeTag(work.action));
        try std.testing.expectEqual(prepared.revision, work.revision);
        try txn.commit();
    }
    {
        var txn = try db.beginWriteTxn();
        defer txn.abort();
        const active = try advance(a, &txn, 7, address, 1, .publish, Verifier{});
        try std.testing.expectEqual(Phase.active, active.phase);
        try std.testing.expect(try Verifier.visible(a, &txn));
    }
    {
        var txn = try db.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(!try Verifier.visible(a, &txn));
        const prepared = (try read(a, &txn, 7, address)).?.bound;
        try std.testing.expectEqual(Phase.prepared, prepared.phase);
        const active = try advance(a, &txn, 7, address, prepared.revision, .publish, Verifier{});
        try std.testing.expectEqual(Phase.active, active.phase);
        try std.testing.expectError(error.LogicalCaptureControlChanged, advance(a, &txn, 7, address, active.revision, .cancel, Verifier{}));
        var buffer: [256]u8 = undefined;
        try std.testing.expectError(error.NotFound, txn.get(try workKey(&buffer, 7, address)));
        try txn.commit();
    }
    {
        var txn = try db.beginWriteTxn();
        defer txn.abort();
        const active = (try read(a, &txn, 7, address)).?.bound;
        try std.testing.expectEqual(Phase.active, active.phase);
        try std.testing.expect(try Verifier.visible(a, &txn));
        const replayed = try begin(a, &txn, 7, draft);
        try std.testing.expectEqual(address, replayed);
        var later = draft;
        later.scope.operation_id = @splat(4);
        const pending = try begin(a, &txn, 7, later);
        try abandonObservation(a, &txn, 7, pending);
        try abandonObservation(a, &txn, 7, pending);
        try std.testing.expectEqual(.abandoned, std.meta.activeTag((try read(a, &txn, 7, pending)).?));
        try std.testing.expectError(error.LogicalCaptureControlChanged, advance(a, &txn, 7, pending, 0, .{ .bind = &.{} }, Verifier{}));
        later.scope.operation_id = @splat(5);
        const later_address = try begin(a, &txn, 7, later);
        try std.testing.expectEqual(later_address, (try nextWork(a, &txn, 7, realm.incarnation, pending, Routing{})).?.address);
        var buffer: [256]u8 = undefined;
        try txn.put(try workKey(&buffer, 7, pending), &pending.operation);
        try std.testing.expectError(error.LogicalCaptureControlChanged, nextWork(a, &txn, 7, realm.incarnation, address, Routing{}));
    }
}

test "SQL logical replication installation waits for every prepared owner and fences activation positions" {
    var memory = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer memory.deinit();
    const a = memory.allocator();
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{
        .root_incarnation = 44,
        .seed = seed,
        .public_key = pair.public_key.toBytes(),
    };
    const realm: publisher.Realm = .{ .group_id = 7, .incarnation = "0123456789abcdef0123456789abcdef".*, .database_id = 1 };
    const scope: capture.Scope = .{
        .realm = realm,
        .subject = .{ .publication = .{ .id = 5, .generation = 2 } },
        .operation_id = @splat(2),
        .change_digest = @splat(3),
    };
    const identity: @FieldType(proofs.ObservationProof, "identity") = .{
        .metadata_incarnation = realm.incarnation,
        .node_id = 2,
        .store_id = 3,
        .root_incarnation = root.root_incarnation,
        .public_key = root.public_key,
    };
    const roster: publisher.PublishedRelation = .{
        .relation = .{ .logical_relation_id = 20, .physical_table_id = 21, .database_id = 1, .namespace_id = 2, .relation_oid = 20, .namespace = "public", .name = "usage", .schema_version = 2, .schema_digest = @splat(11), .replica_identity = .default_key, .columns = &.{.{ .attnum = 1, .name = "id", .type_oid = 20, .key = true }} },
        .memberships = &.{.{ .publication = .{ .id = 5, .generation = 2, .digest = @splat(15) }, .parameters = .{} }},
    };
    var members: [2]DraftMember = undefined;
    var observations: [2]proofs.ObservationReceipt = undefined;
    var prepared: [2]proofs.GuardReceipt = undefined;
    for (&members, &observations, &prepared, 0..) |*member, *observed, *receipt, i| {
        const request: capture.ObservationRequest = .{
            .scope = scope,
            .logical_relation_id = 20,
            .physical_table_id = 21,
            .owner_group_id = 30 + i,
            .namespace = .{ .table_id = 21, .shard_id = 1, .range_id = 40 + i },
            .schema_version = 2,
            .schema_digest = @splat(11),
            .catalog_revision = 22,
            .previous = .{},
            .next = .{ .table = .{ .count = 1, .digest = @splat(12) } },
        };
        member.* = .{ .request = request, .roster = roster };
        observed.* = try (proofs.ObservationReceipt{
            .value = .{ .request = request, .schema_digest = request.schema_digest, .catalog_digest = @splat(13), .physical_root_incarnation = 55 + i, .has_user_data = true, .has_transaction_state = false, .position = .{ .raft = .{ .term = 1, .index = 8 } } },
            .completed = .{ .incarnation = 93, .term = 1, .applied_index = 9 },
            .proof = .{ .identity = identity, .reporter_incarnation = 1, .authority = .{ .raft = 1 }, .root_generation = 1, .signature = undefined },
        }).sign(root, a);
        receipt.* = try (proofs.GuardReceipt{
            .value = .{ .binding = try observed.value.binding(), .action = .prepare, .physical_root_incarnation = 55 + i, .position = .{ .raft = .{ .term = 1, .index = 10 } } },
            .proof = .{ .identity = identity, .reporter_incarnation = 1, .authority = .{ .raft = 1 }, .root_generation = 1, .signature = undefined },
        }).sign(root, a);
    }
    const draft: Draft = .{
        .scope = scope,
        .members = &members,
        .previous = null,
        .desired = .{ .id = 5, .generation = 2, .database_id = 1, .name = "active_pub", .owner = .auth_disabled, .all_tables = false, .tables = &.{.{ .id = 20, .namespace_id = 2, .definition_digest = @splat(14) }}, .parameters = .{} },
    };
    var record = try Record.init(a, try draft.bind(a, &observations));
    try std.testing.expectError(error.LogicalCaptureControlChanged, record.published(a));
    record = try record.accept(a, prepared[1]);
    try std.testing.expectEqual(Phase.preparing, record.phase);
    try std.testing.expectEqual(record.revision, (try record.accept(a, prepared[1])).revision);
    try std.testing.expectError(error.LogicalCaptureControlChanged, record.published(a));
    var foreign = prepared[0];
    foreign.value.physical_root_incarnation += 1;
    foreign = try foreign.sign(root, a);
    try std.testing.expectError(error.LogicalCaptureControlChanged, record.accept(a, foreign));
    record = try record.accept(a, prepared[0]);
    try std.testing.expectEqual(Phase.prepared, record.phase);
    try std.testing.expectEqual(@as(u64, 30), record.prepared[0].value.binding.owner_group_id);
    record = try record.published(a);
    try std.testing.expectError(error.LogicalCaptureControlChanged, record.cancel(a));
    for (prepared) |value| {
        var active = value;
        active.value.action = .activate;
        active = try active.sign(root, a);
        try std.testing.expectError(error.LogicalCaptureControlChanged, record.accept(a, active));
        active.value.position.raft.index += 1;
        active = try active.sign(root, a);
        record = try record.accept(a, active);
    }
    try std.testing.expectEqual(Phase.active, record.phase);

    const Store = local.storage_docstore.DocStore;
    const Host = struct {
        pub fn tableName(_: @This(), _: A, id: u64) ![]const u8 {
            try std.testing.expectEqual(@as(u64, 21), id);
            return "physical:usage";
        }
        pub fn observe(_: @This(), _: *Store.Txn, request: capture.ObservationRequest, value: proofs.ObservationReceipt) !void {
            try value.validate(request, std.testing.allocator);
        }
        pub fn receipt(_: @This(), _: *Store.Txn, member: Member, value: proofs.GuardReceipt) !void {
            try value.validate(.{ .binding = member.binding, .action = value.value.action }, std.testing.allocator);
        }
        pub fn publish(_: @This(), _: *Store.Txn, current: Record) !void {
            try std.testing.expectEqual(Phase.prepared, current.phase);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/capture-two-owners", .{tmp.sub_path}, 0);
    var storage = try Store.open(std.testing.allocator, path.ptr, .{});
    defer storage.close();
    var txn = try storage.beginWriteTxn();
    defer txn.abort();
    const addr = try begin(a, &txn, realm.group_id, draft);
    const original = (try nextWork(a, &txn, realm.group_id, realm.incarnation, null, Host{})).?;
    const observe_request: capture.Request = .{ .observe = members[0].request };
    const prepare_request: capture.Request = .{ .apply = .{ .binding = prepared[0].value.binding, .action = .prepare } };
    var forged = observe_request;
    forged.observe.namespace.range_id += 1;
    try std.testing.expect(try permission(a, &txn, realm.group_id, realm.incarnation, observe_request));
    try std.testing.expect(!try permission(a, &txn, realm.group_id, realm.incarnation, forged));
    try std.testing.expect(!try permission(a, &txn, realm.group_id, realm.incarnation, prepare_request));
    try std.testing.expectEqual(@as(usize, 2), original.action.observe.len);
    try std.testing.expectEqualStrings("physical:usage", original.action.observe[1].table);
    var step = try advance(a, &txn, realm.group_id, addr, original.revision, .{ .bind = &observations }, Host{});
    try std.testing.expect(!try permission(a, &txn, realm.group_id, realm.incarnation, observe_request));
    try std.testing.expect(try permission(a, &txn, realm.group_id, realm.incarnation, prepare_request));
    var wrong_binding = prepare_request;
    wrong_binding.apply.binding.catalog_digest[0] ^= 1;
    try std.testing.expect(!try permission(a, &txn, realm.group_id, realm.incarnation, wrong_binding));
    step = try advance(a, &txn, realm.group_id, addr, step.revision, .{ .receipt = prepared[1] }, Host{});
    const outstanding = (try nextWork(a, &txn, realm.group_id, realm.incarnation, null, Host{})).?;
    try std.testing.expectEqual(capture.Action.prepare, outstanding.action.apply.request.action);
    try std.testing.expectEqual(@as(u64, 30), outstanding.action.apply.request.binding.owner_group_id);
    step = try advance(a, &txn, realm.group_id, addr, outstanding.revision, .{ .receipt = prepared[0] }, Host{});
    step = try advance(a, &txn, realm.group_id, addr, step.revision, .publish, Host{});
    const activation = (try nextWork(a, &txn, realm.group_id, realm.incarnation, null, Host{})).?;
    try std.testing.expectEqual(capture.Action.activate, activation.action.apply.request.action);
    try std.testing.expectEqual(step.revision, activation.revision);
    try std.testing.expect(!try permission(a, &txn, realm.group_id, realm.incarnation, prepare_request));
    try std.testing.expect(try permission(a, &txn, realm.group_id, realm.incarnation, .{ .apply = activation.action.apply.request }));
    const table: local.system_catalog_domain.Resource = .{ .kind = .table, .id = 21, .parent_id = 2, .name = "usage", .sql_relation_id = 20, .sql_publications = members[0].request.next.table };
    const database: local.system_catalog_domain.Resource = .{ .kind = .database, .id = 1, .parent_id = 0, .name = "default" };
    var query: DemandQuery = .{
        .route = .{
            .metadata_group_id = realm.group_id,
            .metadata_incarnation = realm.incarnation,
            .catalog_revision = 22,
            .table_id = 21,
            .topology_epoch = 1,
            .route = .{ .group_id = 30, .range_id = 40, .identity_namespace = .{ .table_id = 21, .shard_id = 1, .range_id = 40 } },
        },
        .pregrant_body_digest = @splat(17),
        .request_digest = @splat(18),
    };
    try std.testing.expectError(error.LogicalCapturePublicationPending, resolveDemand(a, &txn, realm, table, database, roster.relation, query));
    for (prepared, 0..) |value, i| {
        var active = value;
        active.value.action = .activate;
        active.value.position.raft.index += 1;
        active = try active.sign(root, a);
        step = try advance(a, &txn, realm.group_id, addr, step.revision, .{ .receipt = active }, Host{});
        if (i == 0) try std.testing.expectError(error.LogicalCapturePublicationPending, resolveDemand(a, &txn, realm, table, database, roster.relation, query));
    }
    const demand = (try resolveDemand(a, &txn, realm, table, database, roster.relation, query)).?;
    try std.testing.expectEqual(try prepared[0].value.binding.digest(a), demand.reference.guard_digest);
    try std.testing.expectEqual(query.pregrant_body_digest, demand.reference.pregrant_body_digest);
    try std.testing.expectEqual(query.request_digest, demand.reference.request_digest);
    var stripped = table;
    stripped.sql_publications = null;
    try std.testing.expectError(error.LogicalCaptureControlChanged, resolveDemand(a, &txn, realm, stripped, database, null, query));
    query.route.route.identity_namespace.range_id += 1;
    try std.testing.expectError(error.LogicalCaptureControlChanged, resolveDemand(a, &txn, realm, table, database, roster.relation, query));
}
