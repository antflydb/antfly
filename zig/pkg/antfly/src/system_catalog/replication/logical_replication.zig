// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native logical-replication catalog planning. Definitions live in ordinary
//! catalog Resources and use its allocator, parent scopes and atomic Delta.
//! This planner never connects a provider or acknowledges Source/P work.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const records = local.system_catalog_logical_replication_catalog;
const parameters = local.system_catalog_logical_replication;
const storage = @import("../storage.zig");
const A = std.mem.Allocator;
pub const outcomes = @import("../sequence_outcomes.zig");
pub const Outcome = outcomes.Outcome;
pub const Publication = records.Publication;
pub const Subscription = records.Subscription;
pub const Stamp = records.Stamp;
pub const Operation = records.Operation;
pub const Table = records.Table;
pub const Owner = @FieldType(Publication, "owner");

/// SQL creation intent. Provider work starts only after the native catalog
/// commits the definition and an initialization worker claims it.
pub const SubscriptionCreateIntent = struct {
    owner: Owner,
    execution_actor: records.ExecutionActor,
    connection: []const u8,
    publications: []const []const u8,
    options: parameters.SubscriptionOptions,

    pub fn decode(a: A, mutation: domain.Mutation) !SubscriptionCreateIntent {
        const ordinary = domain.Mutation{ .action = .create, .kind = .subscription, .name = mutation.name, .database = mutation.database, .source_json = mutation.source_json, .expected_parent_id = mutation.expected_parent_id };
        const catalog = @import("../catalog_mutations.zig");
        if (!std.mem.eql(u8, &try catalog.canonicalDigest("subscription-envelope", ordinary), &try catalog.canonicalDigest("subscription-envelope", mutation))) return error.InvalidCatalogMutation;
        if (mutation.source_json.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
        const intent = try std.json.parseFromSliceLeaky(SubscriptionCreateIntent, a, mutation.source_json, .{});
        try intent.options.validateCreation();
        if (intent.options.connect and (!intent.options.enabled or !intent.options.create_slot or
            intent.options.slot_name != .name or intent.options.binary or intent.options.two_phase or
            intent.options.failover or intent.options.disable_on_error or intent.options.origin != .any or
            intent.options.synchronous_commit != .off))
            return error.UnsupportedSqlExecution;
        try intent.execution_actor.validate();
        if (intent.options.connect and !intent.options.password_required and (intent.owner != .auth_disabled or intent.execution_actor.session_user.kind != .bootstrap or intent.execution_actor.current_role != null))
            return error.Forbidden;
        return intent;
    }
};

test "SQL logical replication native catalog keeps disconnected password options without authorizing a connection" {
    const a = std.testing.allocator;
    var intent: SubscriptionCreateIntent = .{
        .owner = .{ .user = .{ .name = "subscriber", .instance_id = @splat(7) } },
        .execution_actor = .{ .session_user = .{ .kind = .user, .instance = @splat(7) }, .credential = "basic:subscriber" },
        .connection = "host=localhost dbname=usage",
        .publications = &.{"usage_pub"},
        .options = .{ .connect = false, .create_slot = false, .enabled = false, .copy_data = false, .password_required = false, .slot_name = .{ .name = "usage_sub" } },
    };
    var mutation: domain.Mutation = .{ .kind = .subscription, .action = .create, .name = "usage_sub" };
    mutation.source_json = try std.json.Stringify.valueAlloc(a, intent, .{});
    defer a.free(mutation.source_json);
    var scratch: std.heap.ArenaAllocator = .init(a);
    defer scratch.deinit();
    const accepted = try SubscriptionCreateIntent.decode(scratch.allocator(), mutation);
    try std.testing.expect(!accepted.options.password_required);
    intent.options = .{ .slot_name = .{ .name = "usage_sub" }, .password_required = false };
    var connected = mutation;
    connected.source_json = try std.json.Stringify.valueAlloc(a, intent, .{});
    defer a.free(connected.source_json);
    try std.testing.expectError(error.Forbidden, SubscriptionCreateIntent.decode(scratch.allocator(), connected));
}

pub const SubscriptionChangeIntent = struct {
    actor: records.ExecutionActor,
    enabled: ?bool = null,
    if_exists: bool = false,

    pub fn decode(a: A, mutation: domain.Mutation) !SubscriptionChangeIntent {
        const ordinary = domain.Mutation{ .action = mutation.action, .kind = .subscription, .name = mutation.name, .database = mutation.database, .source_json = mutation.source_json, .expected_parent_id = mutation.expected_parent_id };
        const catalog = @import("../catalog_mutations.zig");
        if (!std.mem.eql(u8, &try catalog.canonicalDigest("subscription-envelope", ordinary), &try catalog.canonicalDigest("subscription-envelope", mutation))) return error.InvalidCatalogMutation;
        if (mutation.source_json.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
        const intent = try std.json.parseFromSliceLeaky(SubscriptionChangeIntent, a, mutation.source_json, .{});
        try intent.actor.validate();
        switch (mutation.action) {
            .alter => if (intent.enabled == null or intent.if_exists) return error.InvalidCatalogMutation,
            .drop => if (intent.enabled != null) return error.InvalidCatalogMutation,
            else => return error.InvalidCatalogMutation,
        }
        return intent;
    }
};

pub fn subscriptionRequiresApply(a: A, mutation: domain.Mutation) !bool {
    if (mutation.action == .create) return (try SubscriptionCreateIntent.decode(a, mutation)).options.connect;
    _ = try SubscriptionChangeIntent.decode(a, mutation);
    return true;
}

pub const Request = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    database: []const u8,
    database_id: u64,
    name: []const u8,
    expected: ?Stamp = null,
    operation: union(enum) {
        create_publication: struct { owner: Owner, all_tables: bool, tables: []const domain.RelationMutationGuard, options: parameters.PublicationOptions },
        alter_publication: struct {
            mode: enum { add, set, drop, options },
            tables: []const domain.RelationMutationGuard = &.{},
            options: ?parameters.PublicationOptions = null,
        },
        drop_publication: struct { if_exists: bool },
        create_subscription: struct { owner: Owner, execution_actor: records.ExecutionActor, connection: []const u8, publications: []const []const u8, options: parameters.SubscriptionOptions, intent: Operation },
        subscription_enabled: struct { enabled: bool, intent: Operation },
        drop_subscription: struct { if_exists: bool, intent: Operation },
    },
    pub fn kind(self: @This()) domain.Kind {
        return switch (self.operation) {
            .create_publication, .alter_publication, .drop_publication => .publication,
            .create_subscription, .subscription_enabled, .drop_subscription => .subscription,
        };
    }
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        if (std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidLogicalReplicationRecord;
        try domain.validateName(self.database);
        try domain.validateName(self.name);
        if (self.database_id == 0) return error.InvalidLogicalReplicationRecord;
        if (self.expected) |stamp| if (stamp.id == 0 or stamp.generation == 0 or std.mem.allEqual(u8, &stamp.digest, 0)) return error.InvalidLogicalReplicationRecord;
        switch (self.operation) {
            .create_publication => |value| {
                if (self.expected != null or (value.all_tables and value.tables.len != 0)) return error.InvalidLogicalReplicationRecord;
                try validateGuards(self.incarnation, value.tables);
            },
            .alter_publication => |value| {
                if (self.expected == null or (value.mode == .options) != (value.options != null) or
                    (value.mode == .options and value.tables.len != 0)) return error.InvalidLogicalReplicationRecord;
                try validateGuards(self.incarnation, value.tables);
            },
            .create_subscription => |value| {
                if (self.expected != null) return error.InvalidLogicalReplicationRecord;
                try value.options.validateCreation();
                try value.execution_actor.validate();
                try value.intent.validate();
            },
            .subscription_enabled => |value| {
                if (self.expected == null) return error.InvalidLogicalReplicationRecord;
                try value.intent.validate();
            },
            .drop_subscription => |value| try value.intent.validate(),
            .drop_publication => {},
        }
    }
};
fn validateGuards(incarnation: [16]u8, guards: []const domain.RelationMutationGuard) !void {
    if (guards.len > parameters.max_members) return error.InvalidLogicalReplicationRecord;
    for (guards) |guard| {
        try guard.validate();
        if (guard.owner.kind != .table or guard.owner.phase != .active or guard.hasDefinitionIntent() or
            !std.mem.eql(u8, &incarnation, &guard.incarnation) or !std.mem.eql(u8, guard.target.name, guard.logical_table))
            return error.InvalidLogicalReplicationRecord;
    }
}

pub const Result = struct {
    revision: u64,
    identity: ?Stamp,
    no_op: bool = false,
    /// SQL admission may succeed while real provider work remains pending.
    /// This describes native desired state; it is never an effect receipt.
    subscription_phase: ?records.Phase = null,
    removed: usize = 0,
    pub fn retained(self: @This()) outcomes.Result {
        return .{ .revision = self.revision, .identity = if (self.identity) |identity| .{ .id = identity.id, .generation = identity.generation } else null, .removed = self.removed, .no_op = self.no_op };
    }
};
pub const Plan = struct { delta: domain.Delta, result: Result };

/// Call only after the actual receiving writer has validated every member's
/// ordinary RelationMutationGuard against its same metadata transaction.
/// Logical binding lookup below additionally prevents a name/schema mix-up.
pub fn plan(a: A, view: storage.View, request: Request) !Plan {
    try request.validate();
    const database = (try view.lookup(.database, 0, request.database)) orelse return error.DatabaseNotFound;
    if (database.id != request.database_id) return error.CatalogGenerationChanged;
    const kind = request.kind();
    const previous = try view.lookup(kind, database.id, request.name);
    var next_id = view.meta.next_id;
    var upserts: []domain.Resource = &.{};
    var removes: []domain.Resource = &.{};
    var phase: ?records.Phase = null;
    var identity: ?Stamp = null;
    switch (request.operation) {
        .create_publication => |value| {
            if (previous != null) return error.CatalogAlreadyExists;
            const id = try allocate(&next_id);
            const record: Publication = .{ .id = id, .generation = 1, .database_id = database.id, .name = request.name, .owner = value.owner, .all_tables = value.all_tables, .tables = try members(a, view, database.id, value.tables), .parameters = value.options };
            identity = try record.stamp(a);
            upserts = try a.dupe(domain.Resource, &.{try resource(a, record, .publication)});
        },
        .alter_publication => |value| {
            const actual = previous orelse return error.CatalogNotFound;
            var record = try records.decodePublication(a, actual);
            try requireStamp(try record.stamp(a), request.expected.?);
            if (record.all_tables and value.mode != .options) return error.InvalidLogicalReplicationRecord;
            if (value.options) |options| record.parameters = options;
            if (value.mode != .options) {
                const captured = try members(a, view, database.id, value.tables);
                record.tables = try changeMembers(a, record.tables, captured, value.mode);
            }
            record.generation = std.math.add(u64, record.generation, 1) catch return error.CatalogGenerationExhausted;
            identity = try record.stamp(a);
            upserts = try a.dupe(domain.Resource, &.{try resource(a, record, .publication)});
        },
        .drop_publication => |value| {
            const actual = previous orelse {
                if (request.expected != null or !value.if_exists) return error.CatalogNotFound;
                return unchanged(view, null, null);
            };
            const record = try records.decodePublication(a, actual);
            if (request.expected) |expected| try requireStamp(try record.stamp(a), expected) else return error.CatalogGenerationChanged;
            removes = try a.dupe(domain.Resource, &.{actual});
        },
        .create_subscription => |value| {
            if (previous != null) return error.CatalogAlreadyExists;
            const id = try allocate(&next_id);
            const record: Subscription = .{ .id = id, .generation = 1, .database_id = database.id, .name = request.name, .owner = value.owner, .execution_actor = value.execution_actor, .connection = value.connection, .publications = value.publications, .parameters = value.options, .desired_enabled = value.options.enabled, .phase = if (value.options.connect) .initializing else .disconnected, .operation = if (value.options.connect) value.intent else null };
            identity = try record.stamp(a);
            phase = record.phase;
            upserts = try a.dupe(domain.Resource, &.{try resource(a, record, .subscription)});
        },
        .subscription_enabled => |value| {
            const actual = previous orelse return error.CatalogNotFound;
            const record = try records.decodeSubscription(a, actual);
            try requireStamp(try record.stamp(a), request.expected.?);
            var next = try record.requestEnabled(a, value.enabled, value.intent);
            if (next.generation == record.generation) return unchanged(view, try record.stamp(a), record.phase);
            if (value.enabled and record.phase == .disabled) if (record.source) |source| {
                const registry = @import("../prepared_transactions.zig");
                const owner: @FieldType(registry.ReplicationReference, "owner") = .{ .logical_subscription = .{ .database_id = record.database_id, .subscription_id = record.id } };
                const progress = (try registry.readReplicationProgress(a, view.txn, view.group_id, owner)) orelse return error.InvalidCatalogRecord;
                if (progress.birth != source.birth or !std.mem.eql(u8, &progress.origin, &source.origin_digest) or progress.pending != null)
                    return error.LogicalReplicationLifecyclePending;
                if (progress.checkpoint != 0) {
                    if (progress.last_acknowledged == null) return error.InvalidCatalogRecord;
                    next.phase = .enabled;
                    next.operation = null;
                }
            };
            identity = try next.stamp(a);
            phase = next.phase;
            upserts = try a.dupe(domain.Resource, &.{try resource(a, next, .subscription)});
        },
        .drop_subscription => |value| {
            const actual = previous orelse {
                if (request.expected != null or !value.if_exists) return error.CatalogNotFound;
                return unchanged(view, null, null);
            };
            const record = try records.decodeSubscription(a, actual);
            if (request.expected) |expected| try requireStamp(try record.stamp(a), expected) else return error.CatalogGenerationChanged;
            if ((record.phase == .disconnected and record.parameters.slot_name == .none) or
                try unstartedSubscription(a, view, request, record))
            {
                // The writer can retire the definition when no provider
                // attempt was admitted and it has no externally owned slot.
                removes = try a.dupe(domain.Resource, &.{actual});
            } else {
                // Named slots and connected origins retain their custody
                // until the provider and admitted transactions drain.
                const next = try record.requestDrop(a, value.intent);
                identity = try next.stamp(a);
                phase = next.phase;
                upserts = try a.dupe(domain.Resource, &.{try resource(a, next, .subscription)});
            }
        },
    }
    return .{ .delta = .{ .upserts = upserts, .removes = removes, .next_id = next_id }, .result = .{ .revision = std.math.add(u64, view.meta.revision, 1) catch return error.CatalogRevisionExhausted, .identity = identity, .subscription_phase = phase, .removed = removes.len } };
}
fn unstartedSubscription(a: A, view: storage.View, request: Request, record: Subscription) !bool {
    if ((record.phase != .initializing and record.phase != .disabled) or record.source != null or !record.parameters.create_slot)
        return false;
    const work = @import("../../metadata/replication_subscription_work.zig");
    const incarnation = std.fmt.bytesToHex(request.incarnation, .lower);
    const lease = try work.observe(a, view.txn, view.group_id, incarnation, .{
        .logical_subscription = .{ .database_id = record.database_id, .subscription_id = record.id },
    });
    return if (lease) |held| held.provider_attempt == null else true;
}

fn unchanged(view: storage.View, identity: ?Stamp, phase: ?records.Phase) Plan {
    return .{ .delta = .{ .upserts = &.{}, .removes = &.{}, .next_id = view.meta.next_id }, .result = .{ .revision = view.meta.revision, .identity = identity, .no_op = true, .subscription_phase = phase } };
}
fn allocate(next: *u64) !u64 {
    const id = next.*;
    if (id == 0 or id > domain.max_relation_oid) return error.CatalogIdExhausted;
    next.* = std.math.add(u64, id, 1) catch return error.CatalogIdExhausted;
    return id;
}
fn requireStamp(actual: Stamp, expected: Stamp) !void {
    if (actual.id != expected.id or actual.generation != expected.generation or !std.mem.eql(u8, &actual.digest, &expected.digest)) return error.CatalogGenerationChanged;
}
fn resource(a: A, record: anytype, kind: domain.Kind) !domain.Resource {
    try record.validate(a);
    return .{ .kind = kind, .id = record.id, .parent_id = record.database_id, .name = record.name, .source_json = try std.json.Stringify.valueAlloc(a, record, .{}) };
}
fn members(a: A, view: storage.View, database: u64, guards: []const domain.RelationMutationGuard) ![]const Table {
    const result = try a.alloc(Table, guards.len);
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen.deinit(a);
    try seen.ensureTotalCapacity(a, @intCast(guards.len));
    for (guards, result) |guard, *out| {
        const namespace = try view.namespaceFor(guard.target.database, guard.target.namespace);
        if (namespace.parent_id != database) return error.InvalidLogicalReplicationRecord;
        const actual = (try view.lookup(.table, namespace.id, guard.target.name)) orelse return error.CatalogNotFound;
        if (actual.id != guard.owner.table_id or actual.sql_temporary_owner != null) return error.CatalogGenerationChanged;
        if ((try seen.getOrPut(a, actual.id)).found_existing) return error.InvalidLogicalReplicationRecord;
        out.* = .{ .id = actual.sql_relation_id orelse return error.CatalogSourceUntracked, .namespace_id = namespace.id, .definition_digest = guard.owner.schema_digest };
    }
    return result;
}
fn changeMembers(a: A, before: []const Table, changes: []const Table, mode: anytype) ![]const Table {
    if (mode == .set) return a.dupe(Table, changes);
    if (mode == .add and changes.len > parameters.max_members - before.len) return error.InvalidLogicalReplicationRecord;
    var selected: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer selected.deinit(a);
    try selected.ensureTotalCapacity(a, @intCast(changes.len));
    for (changes) |member| selected.putAssumeCapacity(member.id, {});
    var result: std.ArrayList(Table) = .empty;
    errdefer result.deinit(a);
    try result.ensureTotalCapacity(a, if (mode == .add) before.len + changes.len else before.len);
    for (before) |member| {
        if (selected.remove(member.id)) {
            if (mode == .add) return error.CatalogAlreadyExists;
        } else result.appendAssumeCapacity(member);
    }
    if (mode == .drop) {
        if (selected.count() != 0) return error.CatalogNotFound;
    } else result.appendSliceAssumeCapacity(changes);
    return result.toOwnedSlice(a);
}

pub const SnapshotRequest = struct {
    database: []const u8,
    name: []const u8,
    kind: records.ObjectKind,
    pub fn validate(self: @This()) !void {
        try domain.validateName(self.database);
        try domain.validateName(self.name);
    }
};
pub const Snapshot = struct {
    revision: u64,
    head: u64,
    incarnation: [16]u8,
    database_id: u64,
    definition: ?union(enum) { publication: Publication, subscription: Subscription },
    pub fn validate(self: @This(), a: A, request: SnapshotRequest) !void {
        try request.validate();
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        if (self.definition) |definition| switch (definition) {
            inline else => |record, tag| {
                if (!std.mem.eql(u8, @tagName(tag), @tagName(request.kind))) return error.InvalidSqlBackendResponse;
                try record.validate(a);
                if (record.database_id != self.database_id or !std.mem.eql(u8, record.name, request.name)) return error.InvalidSqlBackendResponse;
            },
        };
    }
};
pub fn snapshot(a: A, view: storage.View, request: SnapshotRequest, head: u64, incarnation: [16]u8) !Snapshot {
    try request.validate();
    const database = (try view.lookup(.database, 0, request.database)) orelse return error.DatabaseNotFound;
    const kind: domain.Kind = switch (request.kind) {
        .publication => .publication,
        .subscription => .subscription,
    };
    const row = try view.lookup(kind, database.id, request.name);
    const result: Snapshot = .{
        .revision = view.meta.revision,
        .head = head,
        .incarnation = incarnation,
        .database_id = database.id,
        .definition = if (row) |resource_record| switch (request.kind) {
            .publication => .{ .publication = try records.decodePublication(a, resource_record) },
            .subscription => .{ .subscription = try records.decodeSubscription(a, resource_record) },
        } else null,
    };
    try result.validate(a, request);
    return result;
}

pub const OutcomeRequest = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    database: []const u8,
    database_id: u64,
    name: []const u8,
    kind: records.ObjectKind,
    digest: [32]u8,
    operation: local.sql_catalog.CatalogOperationReceipt.Operation,
    pub fn validate(self: @This()) !void {
        try self.admission.validate();
        try domain.validateName(self.database);
        try domain.validateName(self.name);
        if (self.database_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidCatalogMutation;
    }
};
pub fn observation(a: A, request: Request) !OutcomeRequest {
    try request.validate();
    return .{ .admission = request.admission, .incarnation = request.incarnation, .database = request.database, .database_id = request.database_id, .name = request.name, .kind = if (request.kind() == .publication) .publication else .subscription, .digest = try digest(a, "antfly:sql-logical-replication-command:v1\x00", request), .operation = switch (commandAction(request)) {
        .create => .create,
        .drop => .drop,
        else => .alter,
    } };
}
pub fn boundDigest(a: A, request: OutcomeRequest) ![32]u8 {
    try request.validate();
    return digest(a, "antfly:sql-logical-replication-outcome:v1\x00", request);
}
fn digest(a: A, label: []const u8, value: anytype) ![32]u8 {
    const encoded = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(encoded);
    if (encoded.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    var state = std.crypto.hash.Blake3.init(.{});
    state.update(label);
    state.update(encoded);
    var result: [32]u8 = undefined;
    state.final(&result);
    return result;
}
pub fn commandAction(request: Request) domain.Action {
    return switch (request.operation) {
        .create_publication, .create_subscription => .create,
        .drop_publication, .drop_subscription => .drop,
        else => .alter,
    };
}

test "SQL logical replication native catalog uses actual resources and retains disconnected slot custody across cold reopen" {
    // This is a real metadata DocStore/Delta fixture. It establishes no provider,
    // worker, physical admission or slot cleanup receipt.
    const backing = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/logical-replication-catalog", .{tmp.sub_path}, 0);
    var store = try local.storage_docstore.DocStore.open(backing, path.ptr, .{});
    var open = true;
    defer if (open) store.close();
    const subscription: Request = .{
        .admission = .{ .nonce = @splat(1), .head = 0 },
        .incarnation = @splat(2),
        .database = "default",
        .database_id = 1,
        .name = "usage_sub",
        .operation = .{ .create_subscription = .{
            .execution_actor = .{ .session_user = .bootstrap, .credential = "" },
            .owner = .auth_disabled,
            .connection = "host=localhost dbname=usage",
            .publications = &.{ "usage_pub", "usage_audit_pub" },
            .options = .{ .connect = false, .create_slot = false, .enabled = false, .copy_data = false, .binary = true, .slot_name = .{ .name = "usage_slot" } },
            .intent = .{ .id = @splat(3), .body_digest = @splat(4) },
        } },
    };
    var original: Stamp = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const meta = try storage.readMeta(a, &txn, 1);
        const view: storage.View = .{ .alloc = a, .txn = &txn, .group_id = 1, .meta = meta };
        const prepared = try plan(a, view, subscription);
        try std.testing.expectEqual(@as(u64, 3), prepared.result.identity.?.id);
        try std.testing.expectEqual(records.Phase.disconnected, prepared.result.subscription_phase.?);
        const value = try records.decodeSubscription(a, prepared.delta.upserts[0]);
        try std.testing.expect(value.operation == null and value.source == null);
        try std.testing.expectEqualStrings("usage_slot", value.parameters.slot_name.name);
        const recovery_json = try std.json.Stringify.valueAlloc(a, try observation(a, subscription), .{});
        try std.testing.expect(std.mem.indexOf(u8, recovery_json, value.connection) == null);
        original = prepared.result.identity.?;
        try storage.applyDelta(a, &txn, 1, prepared.delta, meta, try boundDigest(a, try observation(a, subscription)));
        try txn.commit();
    }
    try store.sync(true);
    store.close();
    open = false;
    store = try local.storage_docstore.DocStore.open(backing, path.ptr, .{});
    open = true;
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var meta = try storage.readMeta(a, &txn, 1);
        var view: storage.View = .{ .alloc = a, .txn = &txn, .group_id = 1, .meta = meta };
        const found = (try view.lookup(.subscription, 1, "usage_sub")).?;
        const retained = try records.decodeSubscription(a, found);
        try requireStamp(try retained.stamp(a), original);
        try std.testing.expectEqual(@as(usize, 2), retained.publications.len);
        const cut = try snapshot(a, view, .{ .database = "default", .name = "usage_sub", .kind = .subscription }, 0, subscription.incarnation);
        try requireStamp(try cut.definition.?.subscription.stamp(a), original);
        try std.testing.expectError(error.InvalidCatalogMutation, view.read(.{ .kind = .subscription }));
        try std.testing.expectError(error.DatabaseNotEmpty, view.requireNoLogicalReplication(1));
        var database_removal = [_]domain.Resource{(try view.lookup(.database, 0, "default")).?};
        try std.testing.expectError(error.DatabaseNotEmpty, storage.applyDelta(a, &txn, 1, .{
            .upserts = &.{},
            .removes = &database_removal,
            .next_id = meta.next_id,
        }, meta, @splat(9)));
        try std.testing.expectError(error.CatalogReplicationAdmissionRequired, domain.plan(a, .{}, .{ .action = .drop, .kind = .subscription, .name = "usage_sub" }, &.{}));
        try std.testing.expectError(error.CatalogAlreadyExists, plan(a, view, subscription));
        var enable = subscription;
        enable.expected = original;
        enable.operation = .{ .subscription_enabled = .{ .enabled = true, .intent = .{ .id = @splat(5), .body_digest = @splat(6) } } };
        const enabled = try plan(a, view, enable);
        const awaiting = try records.decodeSubscription(a, enabled.delta.upserts[0]);
        try std.testing.expectEqual(records.Phase.awaiting_refresh, awaiting.phase);
        try std.testing.expect(awaiting.source == null and awaiting.operation == null);
        try storage.applyDelta(a, &txn, 1, enabled.delta, meta, try boundDigest(a, try observation(a, enable)));
        meta = try storage.readMeta(a, &txn, 1);
        view.meta = meta;
        try std.testing.expectError(error.CatalogGenerationChanged, plan(a, view, enable));
        var drop = subscription;
        drop.expected = enabled.result.identity.?;
        drop.operation = .{ .drop_subscription = .{ .if_exists = false, .intent = .{ .id = @splat(7), .body_digest = @splat(8) } } };
        const dropping = try plan(a, view, drop);
        try std.testing.expectEqual(@as(usize, 0), dropping.delta.removes.len);
        const held = try records.decodeSubscription(a, dropping.delta.upserts[0]);
        try std.testing.expectEqual(records.Phase.dropping, held.phase);
        try std.testing.expectEqualStrings("usage_slot", held.parameters.slot_name.name);
        try std.testing.expect(held.operation != null and held.source == null);
        var local_only = subscription;
        local_only.name = "slotless_sub";
        local_only.operation.create_subscription.options.slot_name = .none;
        const created_slotless = try plan(a, view, local_only);
        try storage.applyDelta(a, &txn, 1, created_slotless.delta, meta, try boundDigest(a, try observation(a, local_only)));
        meta = try storage.readMeta(a, &txn, 1);
        view.meta = meta;
        drop.name = local_only.name;
        drop.expected = created_slotless.result.identity;
        const dropped_slotless = try plan(a, view, drop);
        try std.testing.expectEqual(@as(usize, 1), dropped_slotless.delta.removes.len);
        try std.testing.expectEqual(@as(usize, 0), dropped_slotless.delta.upserts.len);
        try storage.applyDelta(a, &txn, 1, dropped_slotless.delta, meta, try boundDigest(a, try observation(a, drop)));
        view.meta = try storage.readMeta(a, &txn, 1);
        drop.expected = null;
        drop.operation.drop_subscription.if_exists = true;
        try std.testing.expect((try plan(a, view, drop)).result.no_op);
        var connected = subscription;
        connected.name = "unstarted_sub";
        connected.operation.create_subscription.options = .{ .slot_name = .{ .name = "unstarted_slot" } };
        const created_connected = try plan(a, view, connected);
        try storage.applyDelta(a, &txn, 1, created_connected.delta, view.meta, try boundDigest(a, try observation(a, connected)));
        view.meta = try storage.readMeta(a, &txn, 1);
        drop.name = connected.name;
        drop.expected = created_connected.result.identity;
        try std.testing.expectEqual(@as(usize, 1), (try plan(a, view, drop)).delta.removes.len);
        const work = @import("../../metadata/replication_subscription_work.zig");
        const incarnation = std.fmt.bytesToHex(connected.incarnation, .lower);
        const pending = (try work.read(a, &txn, 1, incarnation, null)).capture.?.item;
        const lease = try work.claim(a, &txn, 1, incarnation, pending, @splat(19), 1, 10);
        try std.testing.expectEqual(@as(usize, 1), (try plan(a, view, drop)).delta.removes.len);
        _ = try work.start(a, &txn, 1, incarnation, lease, @splat(20), @as([32]u8, @splat(21)), 2);
        const pending_drop = try plan(a, view, drop);
        try std.testing.expectEqual(@as(usize, 0), pending_drop.delta.removes.len);
        try std.testing.expectEqual(records.Phase.dropping, pending_drop.result.subscription_phase.?);

        var foreign = found;
        foreign.parent_id = 9;
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, records.decodeSubscription(a, foreign));
        foreign = found;
        foreign.storage_name = "not-a-logical-catalog-object";
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, records.decodeSubscription(a, foreign));
    }
}

test "SQL logical replication native catalog keeps publication generations and conditional drop atomic" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const backing = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/publication-catalog", .{tmp.sub_path}, 0);
    var store = try local.storage_docstore.DocStore.open(backing, path.ptr, .{});
    defer store.close();
    var txn = try store.beginWriteTxn();
    var committed = false;
    defer if (!committed) txn.abort();
    var view: storage.View = .{ .txn = &txn, .alloc = a, .group_id = 1, .meta = try storage.readMeta(a, &txn, 1) };
    var request: Request = .{
        .admission = .{ .nonce = @splat(1), .head = 0 },
        .incarnation = @splat(2),
        .database = "default",
        .database_id = 1,
        .name = "all_usage",
        .operation = .{ .create_publication = .{ .owner = .auth_disabled, .all_tables = true, .tables = &.{}, .options = .{} } },
    };
    const created = try plan(a, view, request);
    try storage.applyDelta(a, &txn, 1, created.delta, view.meta, try boundDigest(a, try observation(a, request)));
    view.meta = try storage.readMeta(a, &txn, 1);
    const cut = try snapshot(a, view, .{ .database = "default", .name = "all_usage", .kind = .publication }, 0, request.incarnation);
    try std.testing.expect(cut.definition.?.publication.all_tables);
    request.database_id = 42;
    try std.testing.expectError(error.CatalogGenerationChanged, plan(a, view, request));
    request.database_id = 1;
    request.expected = created.result.identity;
    request.admission.nonce = @splat(3);
    request.operation = .{ .alter_publication = .{ .mode = .options, .options = .{ .publish_via_partition_root = true } } };
    const changed = try plan(a, view, request);
    try std.testing.expectEqual(created.result.identity.?.id, changed.result.identity.?.id);
    try std.testing.expectEqual(@as(u64, 2), changed.result.identity.?.generation);
    try storage.applyDelta(a, &txn, 1, changed.delta, view.meta, try boundDigest(a, try observation(a, request)));
    view.meta = try storage.readMeta(a, &txn, 1);
    try std.testing.expectError(error.CatalogGenerationChanged, plan(a, view, request));
    request.expected = changed.result.identity;
    request.admission.nonce = @splat(4);
    request.operation = .{ .drop_publication = .{ .if_exists = true } };
    const dropped = try plan(a, view, request);
    try storage.applyDelta(a, &txn, 1, dropped.delta, view.meta, try boundDigest(a, try observation(a, request)));
    view.meta = try storage.readMeta(a, &txn, 1);
    request.expected = null;
    const absent = try plan(a, view, request);
    try std.testing.expect(absent.result.no_op);
    try std.testing.expectEqual(view.meta.revision, absent.result.revision);
    try txn.commit();
    committed = true;
}
