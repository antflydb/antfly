// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Logical publication capture installation, independent of SQL row policy.
//! These immutable values describe work. Only the actual metadata accepting
//! writer may derive a request, and only the actual physical owner can receipt
//! it. A field or digest in a caller body never installs a capture guard.
const std = @import("std");
const publisher = @import("logical_replication_publisher.zig");
const domain = @import("domain.zig");
const identity = @import("../storage/db/doc_identity_namespace.zig");
const positions = @import("../storage/db/receipt_position.zig");
pub const Digest = [32]u8;
pub const Heads = struct {
    table: ?domain.PublicationMembership = null,
    database: ?domain.PublicationMembership = null,
    pub fn validate(self: @This()) !void {
        if (self.table) |head| try head.validate();
        if (self.database) |head| try head.validate();
    }
    pub fn empty(self: @This()) bool {
        return self.table == null and self.database == null;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }
};
pub const Subject = union(enum) {
    publication: struct { id: u64, generation: u64 },
    /// Genuine schema/topology command identity covers both ALL TABLES birth
    /// and later schema/image transitions. Maintenance copies are not inserts.
    table_change: struct {
        logical_relation_id: u64,
        generation: u64,
        kind: enum { create, alter, drop, truncate, image_replacement },
    },
};
pub const Scope = struct {
    realm: publisher.Realm,
    subject: Subject,
    operation_id: [16]u8,
    change_digest: Digest,
    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        if (std.mem.allEqual(u8, &self.operation_id, 0) or zero(self.change_digest)) return error.InvalidLogicalCaptureControl;
        switch (self.subject) {
            .publication => |value| if (value.id == 0 or value.generation == 0) return error.InvalidLogicalCaptureControl,
            .table_change => |value| if (value.logical_relation_id == 0 or value.logical_relation_id > domain.max_relation_oid or value.generation == 0) return error.InvalidLogicalCaptureControl,
        }
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }
};
fn requireTarget(self: anytype) !void {
    try self.scope.validate();
    try self.previous.validate();
    try self.next.validate();
    if (self.logical_relation_id == 0 or self.logical_relation_id > domain.max_relation_oid or self.physical_table_id == 0 or
        self.owner_group_id == 0 or self.namespace.table_id != self.physical_table_id or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
        zero(self.schema_digest) or self.catalog_revision == 0) return error.InvalidLogicalCaptureControl;
    if (self.scope.subject == .table_change) {
        const change = self.scope.subject.table_change;
        if (change.logical_relation_id != self.logical_relation_id or (change.kind == .create and !self.previous.empty())) return error.InvalidLogicalCaptureControl;
    }
}

pub const Binding = struct {
    scope: Scope,
    logical_relation_id: u64,
    physical_table_id: u64,
    owner_group_id: u64,
    namespace: identity.Namespace,
    schema_version: u32,
    schema_digest: Digest,
    catalog_digest: Digest,
    /// Actual authority projection cut, not wall time or a truncated global
    /// metadata index. The native writer supplies/rejoins this exact revision.
    catalog_revision: u64,
    previous: Heads,
    next: Heads,
    pub fn validate(self: @This()) !void {
        try requireTarget(self);
        if (zero(self.catalog_digest)) return error.InvalidLogicalCaptureControl;
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate();
        return canonicalDigest(a, "antfly/logical-capture-binding/v1\x00", self);
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }
};
/// Native-derived target before the physical owner reports its actual AIC.
/// No guessed fresh catalog or synthetic initialization generation is needed.
pub const ObservationRequest = struct {
    scope: Scope,
    logical_relation_id: u64,
    physical_table_id: u64,
    owner_group_id: u64,
    namespace: identity.Namespace,
    schema_version: u32,
    schema_digest: Digest,
    /// Actual authority projection cut, not wall time or a truncated global
    /// metadata index. The native writer supplies/rejoins this exact revision.
    catalog_revision: u64,
    previous: Heads,
    next: Heads,
    pub fn fromBinding(binding: Binding) @This() {
        var result: @This() = undefined;
        inline for (@typeInfo(@This()).@"struct".field_names) |field| @field(result, field) = @field(binding, field);
        return result;
    }
    pub fn validate(self: @This()) !void {
        try requireTarget(self);
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }
    pub fn bind(self: @This(), catalog_digest: Digest) !Binding {
        try self.validate();
        var result: Binding = undefined;
        inline for (@typeInfo(Binding).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "catalog_digest")) result.catalog_digest = catalog_digest else @field(result, field) = @field(self, field);
        }
        try result.validate();
        return result;
    }
};
pub const OwnerObservation = struct {
    request: ObservationRequest,
    schema_digest: Digest,
    catalog_digest: Digest,
    physical_root_incarnation: u128,
    position: ?positions.Position,
    has_user_data: bool,
    has_transaction_state: bool,
    pub fn validate(self: @This()) !void {
        try self.request.validate();
        if (self.physical_root_incarnation == 0 or zero(self.catalog_digest) or
            !std.mem.eql(u8, &self.schema_digest, &self.request.schema_digest)) return error.InvalidLogicalCaptureControl;
        if (self.position) |value| {
            try value.requireNamespace(self.request.namespace);
        } else if (self.has_user_data or self.has_transaction_state) return error.InvalidLogicalCaptureControl;
    }
    pub fn binding(self: @This()) !Binding {
        try self.validate();
        return self.request.bind(self.catalog_digest);
    }
    pub fn require(self: @This(), request: ObservationRequest) !void {
        try self.validate();
        try request.validate();
        if (!self.request.eql(request)) return error.LogicalCaptureControlChanged;
    }
};
/// One closed owner-control surface: observe cannot install a guard or issue
/// a prepared receipt; apply is an actual metadata-admitted ordered command.
pub const Request = union(enum) { observe: ObservationRequest, apply: Control };

pub const Phase = enum { preparing, prepared, active, canceled };
pub const Action = enum { prepare, activate, cancel };
pub const Control = struct {
    binding: Binding,
    action: Action,
    pub fn validate(self: @This()) !void {
        try self.binding.validate();
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate();
        return canonicalDigest(a, "antfly/logical-capture-control/v1\x00", self);
    }
};
pub const Receipt = struct {
    binding: Binding,
    action: Action,
    /// A real native/ordered apply clock value, not an admission revision.
    position: positions.Position,
    physical_root_incarnation: u128,
    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        try self.position.requireNamespace(self.binding.namespace);
        if (self.physical_root_incarnation == 0) return error.InvalidLogicalCaptureControl;
    }
    pub fn require(self: @This(), control: Control) !void {
        try self.validate();
        try control.validate();
        if (self.action != control.action or !self.binding.eql(control.binding)) return error.LogicalCaptureControlChanged;
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate();
        return canonicalDigest(a, "antfly/logical-capture-owner-receipt/v1\x00", self);
    }
};
/// Native control and its durable mirror disposition are reported separately.
pub const NativeOutcome = struct {
    receipt: ?Receipt,
    position: positions.Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
};

/// Physical durable guard contains only bounded fixed ownership facts. The
/// complete former membership survives prepare/cancel; an activation cannot
/// clear another publication's aggregate, nor replace an unrelated operation.
pub const State = struct {
    binding: Binding,
    /// Actual formerly effective guard, retained by the physical accepting
    /// prepare. Heads alone cannot reconstruct its original scope/digest.
    /// This is flat (no previous State chain) and absent when no policy exists.
    previous_binding: ?Binding = null,
    phase: Phase,
    position: positions.Position,
    physical_root_incarnation: u128,
    pub fn validate(self: @This()) !void {
        try (Receipt{ .binding = self.binding, .action = switch (self.phase) {
            .active => .activate,
            .canceled => .cancel,
            .preparing, .prepared => .prepare,
        }, .position = self.position, .physical_root_incarnation = self.physical_root_incarnation }).validate();
        if (self.phase == .active) {
            if (self.previous_binding != null) return error.InvalidLogicalCaptureControl;
        } else if (self.previous_binding) |previous| {
            try previous.validate();
            if (previous.next.empty() or !previous.next.eql(self.binding.previous) or
                !previous.scope.realm.eql(self.binding.scope.realm) or previous.logical_relation_id != self.binding.logical_relation_id or
                previous.physical_table_id != self.binding.physical_table_id or previous.owner_group_id != self.binding.owner_group_id or
                !previous.namespace.eql(self.binding.namespace)) return error.InvalidLogicalCaptureControl;
        } else if (!self.binding.previous.empty()) return error.InvalidLogicalCaptureControl;
    }
    /// New work stays fenced while preparing. Cancellation restores the EXACT
    /// originally effective binding, not a new digest with old membership heads.
    /// An empty active tombstone grants no capture and cannot grow history.
    pub fn effectiveBinding(self: @This()) !?Binding {
        try self.validate();
        return switch (self.phase) {
            .active => if (self.binding.next.empty()) null else self.binding,
            .canceled => self.previous_binding,
            .preparing, .prepared => error.LogicalCapturePublicationPending,
        };
    }
    pub fn requiresCapture(self: @This()) !bool {
        return try self.effectiveBinding() != null;
    }
};
/// Frozen per-participant intent identity. The accepting metadata writer
/// derives this from actual publication heads/roster and the immutable plan;
/// Data independently rejoins it before constructing any durable capture.
pub const DemandReference = struct {
    realm: publisher.Realm,
    guard_digest: Digest,
    roster_digest: Digest,
    pregrant_body_digest: Digest,
    request_digest: Digest,
    pub fn validate(self: @This()) !void {
        try self.realm.validate();
        if (zero(self.guard_digest) or zero(self.roster_digest) or zero(self.pregrant_body_digest) or zero(self.request_digest)) return error.InvalidLogicalCaptureControl;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return std.meta.eql(self, other);
    }
};
/// Native-issued capacity, after P admission and BEFORE physical capture.
/// This post-admission grant is not part of the pregrant intent/body digest.
pub const CapacityGrant = struct {
    bound_digest: Digest,
    before_digest: Digest,
    max_payload_bytes: u64,
    pub fn validate(self: @This()) !void {
        if (zero(self.bound_digest) or zero(self.before_digest) or self.max_payload_bytes == 0 or self.max_payload_bytes > @import("../storage/db/table_catalog.zig").default_transaction_admission_bytes) return error.InvalidLogicalCaptureControl;
    }
};
pub const Demand = struct {
    capacity: ?CapacityGrant = null,
    owner: @import("../storage/transaction_decision_owner.zig").External,
    transaction_id: [16]u8,
    member: u16,
    owner_group_id: u64,
    topology_epoch: u64,
    reference: DemandReference,
    roster: publisher.PublishedRelation,
    pub fn validate(self: @This(), a: std.mem.Allocator) !void {
        try self.owner.validate();
        try self.reference.validate();
        if (self.capacity) |grant| try grant.validate();
        if (self.owner_group_id == 0 or self.topology_epoch == 0 or self.member >= publisher.max_capture_participants or std.mem.allEqual(u8, &self.transaction_id, 0) or
            !std.mem.eql(u8, &self.owner.metadata_incarnation, &self.reference.realm.incarnation) or
            self.reference.realm.database_id != self.roster.relation.database_id) return error.InvalidLogicalCaptureControl;
        var budget: publisher.RosterBudget = .{};
        try budget.add(self.roster);
        const actual = try canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", self.roster);
        if (!std.mem.eql(u8, &actual, &self.reference.roster_digest)) return error.LogicalCaptureControlChanged;
    }
    pub fn preCapacityDigest(self: @This(), a: std.mem.Allocator) !Digest {
        var original = self;
        original.capacity = null;
        return original.digest(a);
    }
    pub fn digest(self: @This(), a: std.mem.Allocator) !Digest {
        try self.validate(a);
        return canonicalDigest(a, "antfly/logical-capture-demand/v1\x00", self);
    }
};

fn zero(bytes: Digest) bool {
    return std.mem.allEqual(u8, &bytes, 0);
}
pub fn canonicalDigest(a: std.mem.Allocator, comptime tag: []const u8, value: anytype) !Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(tag);
    hash.update(bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

/// Isolated future37 ordered control. Exhaustive comparison against semantic
/// defaults means no mixed row effect/transaction/unknown private field can be
/// omitted by its control-only envelope. This is shape, never authorization.
pub fn validateControlBatch(request: anytype) !void {
    const control = request.logical_publication_control orelse return;
    try control.validate();
    const defaults: @TypeOf(request) = .{};
    inline for (@typeInfo(@TypeOf(request)).@"struct".field_names) |name| {
        if (comptime std.mem.eql(u8, name, "logical_publication_control")) continue;
        if (!@import("../storage/control_field_defaults.zig").isolatedFieldMatchesDefault(name, @field(request, name), @field(defaults, name))) return error.InvalidBatchRequest;
    }
}

test "logical capture canceled operation restores exact prior binding without recursive history" {
    const a = std.testing.allocator;
    const first: Binding = .{ .scope = .{ .realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 2 }, .subject = .{ .publication = .{ .id = 10, .generation = 1 } }, .operation_id = @splat(1), .change_digest = @splat(2) }, .logical_relation_id = 40, .physical_table_id = 50, .owner_group_id = 9, .namespace = .{ .table_id = 50, .shard_id = 1, .range_id = 2 }, .schema_version = 1, .schema_digest = @splat(3), .catalog_digest = @splat(4), .catalog_revision = 5, .previous = .{}, .next = .{ .table = .{ .count = 1, .digest = @splat(6) } } };
    var second = first;
    second.scope.operation_id = @splat(7);
    second.scope.subject.publication.generation += 1;
    second.catalog_revision += 1;
    second.previous = first.next;
    second.next.table = .{ .count = 2, .digest = @splat(8) };
    const restored: State = .{ .binding = second, .previous_binding = first, .phase = .canceled, .position = .{ .native = .{ .namespace = first.namespace, .sequence = 3 } }, .physical_root_incarnation = 9 };
    const effective = (try restored.effectiveBinding()).?;
    try std.testing.expect(effective.eql(first));
    try std.testing.expectEqualSlices(u8, &try first.digest(a), &try effective.digest(a));
    try std.testing.expect(!std.mem.eql(u8, &try second.digest(a), &try effective.digest(a)));
    try std.testing.expect(try restored.requiresCapture());
    const encoded = try std.json.Stringify.valueAlloc(a, restored, .{});
    defer a.free(encoded);
    var decoded = try std.json.parseFromSlice(State, a, encoded, .{ .ignore_unknown_fields = false });
    defer decoded.deinit();
    try std.testing.expect((try decoded.value.effectiveBinding()).?.eql(first));
    var pending = restored;
    pending.phase = .preparing;
    try std.testing.expectError(error.LogicalCapturePublicationPending, pending.effectiveBinding());
    var missing = restored;
    missing.previous_binding = null;
    try std.testing.expectError(error.InvalidLogicalCaptureControl, missing.validate());
    var wrong = restored;
    wrong.previous_binding.?.namespace.range_id += 1;
    try std.testing.expectError(error.InvalidLogicalCaptureControl, wrong.validate());
    var active = restored;
    active.phase = .active;
    active.previous_binding = null;
    try std.testing.expect((try active.effectiveBinding()).?.eql(second));
    active.binding.next = .{};
    try std.testing.expect((try active.effectiveBinding()) == null);
    var creation = restored;
    creation.binding.previous = .{};
    creation.previous_binding = null;
    try std.testing.expect((try creation.effectiveBinding()) == null);
}
