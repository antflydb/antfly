// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Database-owned logical replication definitions and lifecycle intents.
//! These records contain no provider callback, SQL connection or fabricated
//! checkpoint. Initialization/cleanup completion is owned by the native Source
//! journal; a local desired-state change never pretends those effects finished.
const std = @import("std");
const options = @import("logical_replication.zig");
const owner_mod = @import("owners.zig");
const domain = @import("domain.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const max_record_bytes = 4 * 1024 * 1024;
pub const max_connection_bytes = 64 * 1024;
pub const ObjectKind = enum { publication, subscription };

pub const Table = struct {
    /// Stable logical relation identity. Each publication read rejoins its
    /// current physical/schema binding; an ordinary table generation change
    /// does not silently drop publication membership.
    id: u64,
    namespace_id: u64,
    definition_digest: Digest,
    pub fn validate(self: Table) !void {
        if (self.id == 0 or self.id > domain.max_relation_oid or self.namespace_id == 0 or std.mem.allEqual(u8, &self.definition_digest, 0)) return error.InvalidLogicalReplicationRecord;
    }
};
pub const Stamp = struct { id: u64, generation: u64, digest: Digest };
pub const Publication = struct {
    version: u16 = 1,
    id: u64,
    generation: u64,
    database_id: u64,
    name: []const u8,
    owner: owner_mod.Owner,
    all_tables: bool,
    tables: []const Table,
    parameters: options.PublicationOptions,
    /// A native-owned pending operation retains the name while its actual
    /// receiving owners drain/install capture. It is never capture authority.
    /// CREATE has no active definition until prepared receipts are published;
    /// ALTER keeps the previous active definition while this intent is held.
    capture_operation: ?Operation = null,
    active: bool = true,

    pub fn validate(self: Publication, a: A) !void {
        try header(self.version, self.id, self.generation, self.database_id, self.name, self.owner);
        if (self.capture_operation) |operation| try operation.validate();
        if (!self.active and self.capture_operation == null) return error.InvalidLogicalReplicationRecord;
        if (self.tables.len > options.max_members or (self.all_tables and self.tables.len != 0)) return error.InvalidLogicalReplicationRecord;
        var ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer ids.deinit(a);
        for (self.tables) |table| {
            try table.validate();
            if ((try ids.getOrPut(a, table.id)).found_existing) return error.InvalidLogicalReplicationRecord;
        }
    }
    pub fn stamp(self: Publication, a: A) !Stamp {
        try self.validate(a);
        return .{ .id = self.id, .generation = self.generation, .digest = try digest(a, "antfly:logical-publication-definition:v1\x00", .{
            .version = self.version,
            .id = self.id,
            .generation = self.generation,
            .database_id = self.database_id,
            .name = self.name,
            .owner = self.owner,
            .all_tables = self.all_tables,
            .tables = self.tables,
            .parameters = self.parameters,
        }) };
    }
};

/// Real catalog owner for one replication slot and one whole upstream
/// transaction across a publication relation roster. It is not a table ID.
pub const SubscriptionWork = struct {
    owner: @import("logical_replication_origin.zig").Owner,
    generation: u64,
    recipe_digest: [32]u8,
    operation: Operation,
    phase: enum { initialize, drain, drop },
};

pub const SubscriptionExport = struct {
    provider_identity: Digest,
    checkpoint: u64,
    retiring: bool = false,

    pub fn validate(self: @This()) !void {
        if (self.checkpoint == 0 or std.mem.allEqual(u8, &self.provider_identity, 0))
            return error.InvalidCatalogRecord;
    }
};

pub const SubscriptionWorkLease = struct {
    item: SubscriptionWork,
    worker: [16]u8,
    generation: u64,
    expires_at_ns: u64,
    provider_attempt: ?[16]u8 = null,
    provider_preflight_identity: ?Digest = null,
    provider_origin: ?Digest = null,
    provider_export: ?SubscriptionExport = null,

    pub fn jsonStringify(self: SubscriptionWorkLease, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(SubscriptionWorkLease).@"struct".field_names) |field| {
            if (((comptime !std.mem.eql(u8, field, "provider_preflight_identity")) or self.provider_preflight_identity != null) and
                ((comptime !std.mem.eql(u8, field, "provider_export")) or self.provider_export != null))
            {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: SubscriptionWorkLease) !void {
        try self.item.owner.validate();
        try self.item.operation.validate();
        if (self.item.owner != .logical_subscription or self.item.generation == 0 or self.generation == 0 or
            self.expires_at_ns == 0 or std.mem.allEqual(u8, &self.item.recipe_digest, 0) or
            std.mem.allEqual(u8, &self.worker, 0)) return error.InvalidCatalogRecord;
        if (self.provider_attempt) |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidCatalogRecord;
        if (self.provider_preflight_identity) |identity| if (self.provider_attempt == null or std.mem.allEqual(u8, &identity, 0)) return error.InvalidCatalogRecord;
        if (self.provider_origin) |origin_digest| if (self.provider_attempt == null or std.mem.allEqual(u8, &origin_digest, 0)) return error.InvalidCatalogRecord;
        if (self.provider_export) |receipt| {
            try receipt.validate();
            if (self.provider_origin == null) return error.InvalidCatalogRecord;
            if (self.provider_preflight_identity) |identity| if (!std.mem.eql(u8, &identity, &receipt.provider_identity)) return error.InvalidCatalogRecord;
        }
    }
};

pub const SubscriptionIdentity = struct {
    database_id: u64,
    subscription_id: u64,
    pub fn validate(self: @This()) !void {
        if (self.database_id == 0 or self.subscription_id == 0 or self.subscription_id > domain.max_relation_oid) return error.InvalidLogicalReplicationRecord;
    }
    pub fn eql(self: @This(), other: @This()) bool {
        return self.database_id == other.database_id and self.subscription_id == other.subscription_id;
    }
};
pub const SourceReference = struct {
    /// Native-assigned immutable birth, independent of event/transaction count.
    birth: u64,
    origin_digest: Digest,
    pub fn validate(self: @This()) !void {
        if (self.birth == 0 or std.mem.allEqual(u8, &self.origin_digest, 0)) return error.InvalidLogicalReplicationRecord;
    }
};
pub const Phase = enum { disconnected, awaiting_refresh, initializing, enabled, disabled, draining, dropping, dropped, failed };
pub const DrainReason = enum { disable, drop, reconfigure };
pub const Operation = struct {
    id: [16]u8,
    body_digest: Digest,
    pub fn validate(self: @This()) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.body_digest, 0)) return error.InvalidLogicalReplicationRecord;
    }
};
/// Captured from the authenticated DDL actor by the native catalog writer.
/// This is private credential identity, not a password/DSN or service actor.
/// New work must revalidate the actual user/key instance and role membership;
/// an already decided P never consults it during phase two.
pub const ExecutionActor = struct {
    session_user: @import("privileges.zig").Principal,
    current_role: ?@import("privileges.zig").Principal = null,
    credential: []const u8,
    pub fn validate(self: @This()) !void {
        try self.session_user.validate();
        if (self.session_user.kind != .bootstrap and self.session_user.kind != .user) return error.InvalidLogicalReplicationRecord;
        if (self.current_role) |role| {
            try role.validate();
            if (role.kind != .role) return error.InvalidLogicalReplicationRecord;
        }
        if (self.credential.len > 512 or !std.unicode.utf8ValidateSlice(self.credential) or std.mem.indexOfScalar(u8, self.credential, 0) != null or
            (self.session_user.kind != .bootstrap and self.credential.len == 0)) return error.InvalidLogicalReplicationRecord;
    }
};
pub const Subscription = struct {
    execution_actor: ?ExecutionActor = null,
    version: u16 = 1,
    id: u64,
    generation: u64,
    database_id: u64,
    name: []const u8,
    owner: owner_mod.Owner,
    /// Literal libpq conninfo is private catalog data, never public table
    /// query-definition data or an unauthenticated progress diagnostic.
    connection: []const u8,
    publications: []const []const u8,
    parameters: options.SubscriptionOptions,
    phase: Phase,
    desired_enabled: bool,
    source: ?SourceReference = null,
    operation: ?Operation = null,
    drain_reason: ?DrainReason = null,

    pub fn validate(self: Subscription, a: A) !void {
        try header(self.version, self.id, self.generation, self.database_id, self.name, self.owner);
        try self.parameters.validate();
        if (self.execution_actor) |actor| try actor.validate();
        if (self.connection.len == 0 or self.connection.len > max_connection_bytes or std.mem.indexOfScalar(u8, self.connection, 0) != null or
            self.publications.len == 0 or self.publications.len > options.max_members or self.desired_enabled != self.parameters.enabled or
            ((self.phase == .draining) != (self.drain_reason != null))) return error.InvalidLogicalReplicationRecord;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        for (self.publications) |name| {
            try domain.validateName(name);
            if ((try names.getOrPut(a, name)).found_existing) return error.InvalidLogicalReplicationRecord;
        }
        if (self.source) |source| try source.validate();
        if (self.operation) |operation| try operation.validate();
        switch (self.phase) {
            .disconnected => if (self.parameters.connect or self.desired_enabled or self.source != null or self.operation != null) return error.InvalidLogicalReplicationRecord,
            .awaiting_refresh => if (!self.desired_enabled or self.source != null or self.operation != null) return error.InvalidLogicalReplicationRecord,
            .initializing => if (self.operation == null) return error.InvalidLogicalReplicationRecord,
            .enabled => if (!self.desired_enabled or self.source == null or self.operation != null) return error.InvalidLogicalReplicationRecord,
            .disabled => if (self.desired_enabled or self.operation != null) return error.InvalidLogicalReplicationRecord,
            .draining, .dropping => if (self.operation == null) return error.InvalidLogicalReplicationRecord,
            .dropped => if (self.source != null or self.operation != null or self.desired_enabled) return error.InvalidLogicalReplicationRecord,
            .failed => {},
        }
    }
    pub fn jsonStringify(self: Subscription, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Subscription).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "execution_actor")) {
                if (self.execution_actor) |actor| {
                    try writer.objectField(field);
                    try writer.write(actor);
                }
            } else {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn identity(self: Subscription) SubscriptionIdentity {
        return .{ .database_id = self.database_id, .subscription_id = self.id };
    }
    pub fn requireOwner(self: Subscription, principal: @import("privileges.zig").Principal) !void {
        if (principal.kind == .bootstrap) return;
        const owner = switch (self.owner) {
            .user => |owner| if (principal.kind == .user) owner else return error.Forbidden,
            .role => |owner| if (principal.kind == .role) owner else return error.Forbidden,
            .auth_disabled => return error.Forbidden,
        };
        if (!std.mem.eql(u8, &owner.instance_id, &principal.instance)) return error.Forbidden;
    }
    pub fn stamp(self: Subscription, a: A) !Stamp {
        try self.validate(a);
        return .{ .id = self.id, .generation = self.generation, .digest = try digest(a, "antfly:logical-subscription-definition:v1\x00", self) };
    }

    /// Operational enable/drain state is not a new upstream origin. The
    /// native Source writer separately checks desired_enabled at admission;
    /// the immutable recipe binds provider, owner and replica-apply semantics.
    pub fn recipeDigest(self: Subscription, a: A) !Digest {
        try self.validate(a);
        return digest(a, "antfly:logical-subscription-recipe:v1\x00", .{
            .id = self.id,
            .database_id = self.database_id,
            .owner = self.owner,
            .execution_actor = self.execution_actor,
            .connection = self.connection,
            .publications = self.publications,
            .slot_name = self.parameters.slot_name,
            .binary = self.parameters.binary,
            .two_phase = self.parameters.two_phase,
            .origin = self.parameters.origin,
            .streaming = self.parameters.streaming,
            .synchronous_commit = self.parameters.synchronous_commit,
            .password_required = self.parameters.password_required,
            .run_as_owner = self.parameters.run_as_owner,
            .failover = self.parameters.failover,
            .copy_data = self.parameters.copy_data,
        });
    }

    /// Admission stops atomically with this desired-state intent. Actual
    /// admitted P work retains its original source and must finish normally.
    /// This transition does not release a slot or acknowledge any data work.
    pub fn requestEnabled(self: Subscription, a: A, enabled: bool, operation: Operation) !Subscription {
        try self.validate(a);
        try operation.validate();
        if (self.phase == .draining or self.phase == .dropping or self.phase == .dropped or self.phase == .failed) return error.LogicalReplicationLifecyclePending;
        if (self.desired_enabled == enabled) return self;
        var next = self;
        next.generation = std.math.add(u64, self.generation, 1) catch return error.CatalogGenerationExhausted;
        next.parameters.enabled = enabled;
        next.desired_enabled = enabled;
        next.operation = operation;
        if (enabled) {
            // ENABLE does not refresh a connect=false definition's absent
            // subscription relation roster or implicitly request an initial
            // copy. A real REFRESH/slot handshake must bind that roster first.
            next.phase = if (self.phase == .disconnected) .awaiting_refresh else .initializing;
            if (next.phase == .awaiting_refresh) next.operation = null;
        } else {
            next.phase = .draining;
            next.drain_reason = .disable;
        }
        try next.validate(a);
        return next;
    }

    pub fn requestDrop(self: Subscription, a: A, operation: Operation) !Subscription {
        try self.validate(a);
        try operation.validate();
        if (self.phase == .dropping or self.phase == .dropped or self.phase == .draining) return error.LogicalReplicationLifecyclePending;
        var next = self;
        next.generation = std.math.add(u64, self.generation, 1) catch return error.CatalogGenerationExhausted;
        next.desired_enabled = false;
        next.parameters.enabled = false;
        next.operation = operation;
        // A configured named slot still requires actual publisher cleanup,
        // even when connect=false avoided all creation-time provider work.
        // Only the native owner may establish the empty slot-NONE case.
        next.phase = if (self.source == null) .dropping else .draining;
        next.drain_reason = if (next.phase == .draining) .drop else null;
        try next.validate(a);
        return next;
    }
};

fn header(version: u16, id: u64, generation: u64, database: u64, name: []const u8, owner: owner_mod.Owner) !void {
    if (version != 1 or id == 0 or id > domain.max_relation_oid or generation == 0 or database == 0) return error.InvalidLogicalReplicationRecord;
    try domain.validateName(name);
    switch (owner) {
        .auth_disabled => {},
        .user, .role => |principal| {
            if (std.mem.allEqual(u8, &principal.instance_id, 0)) return error.InvalidLogicalReplicationRecord;
            try domain.validateName(principal.name);
        },
    }
}
fn digest(a: A, comptime purpose: []const u8, value: anytype) !Digest {
    const encoded = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(encoded);
    if (encoded.len > max_record_bytes) return error.InvalidLogicalReplicationRecord;
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update(purpose);
    hasher.update(encoded);
    var result: Digest = undefined;
    hasher.final(&result);
    return result;
}

/// The ordinary catalog resource is the sole object/name/ID directory. The
/// typed definition is its private payload, never public query-definition data.
pub fn decodePublication(a: A, resource: domain.Resource) !Publication {
    if (resource.kind != .publication or resource.source_json.len > max_record_bytes) return error.InvalidLogicalReplicationRecord;
    const record = try std.json.parseFromSliceLeaky(Publication, a, resource.source_json, .{ .allocate = .alloc_always });
    try record.validate(a);
    try requireResource(resource, record.id, record.database_id, record.name);
    return record;
}
pub fn decodeSubscription(a: A, resource: domain.Resource) !Subscription {
    if (resource.kind != .subscription or resource.source_json.len > max_record_bytes) return error.InvalidLogicalReplicationRecord;
    const record = try std.json.parseFromSliceLeaky(Subscription, a, resource.source_json, .{ .allocate = .alloc_always });
    try record.validate(a);
    try requireResource(resource, record.id, record.database_id, record.name);
    return record;
}
fn requireResource(resource: domain.Resource, id: u64, database: u64, name: []const u8) !void {
    if (resource.id != id or resource.parent_id != database or !std.mem.eql(u8, resource.name, name) or
        resource.storage_name.len != 0 or resource.tablespace_id != 0 or resource.native_service != null or
        resource.sql_relation_id != null or resource.sql_temporary_owner != null or resource.sql_owner != null or resource.sql_publications != null or resource.sql_all_publications != null or
        resource.graph_tables.len != 0 or resource.graph_indexes.len != 0 or resource.signature != null or
        !std.mem.eql(u8, resource.location_json, "null")) return error.InvalidLogicalReplicationRecord;
}

pub fn sameOwner(left: owner_mod.Owner, right: owner_mod.Owner) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .auth_disabled => true,
        .user => |principal| std.mem.eql(u8, &principal.instance_id, &right.user.instance_id),
        .role => |principal| std.mem.eql(u8, &principal.instance_id, &right.role.instance_id),
    };
}
