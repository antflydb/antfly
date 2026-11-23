// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded discovery of subscription lifecycle work from canonical catalog
//! records. Provider effects must rejoin the exact owner and generation on
//! the native writer before admission.
const std = @import("std");
const local = @import("antfly_local_sources");
const storage = @import("../system_catalog/storage.zig");
const catalog = local.system_catalog_logical_replication_catalog;
const registry = @import("../system_catalog/prepared_transactions.zig");
const A = std.mem.Allocator;
pub const Item = catalog.SubscriptionWork;
pub const SnapshotWork = struct {
    group: u64,
    item: Item,
    source: catalog.SourceReference,
    generation: u64,
    event: ?registry.ReplicationEvent,
};
pub const WalWork = struct {
    group: u64,
    owner: @FieldType(Item, "owner"),
    source: catalog.SourceReference,
    recipe_digest: [32]u8,
};
pub const Page = struct {
    incarnation: [32]u8,
    items: []const Item,
    next_after: ?u64,
    snapshot: ?SnapshotWork = null,
    wal: ?WalWork = null,
    capture: ?struct { group: u64, item: Item } = null,
    disable: ?struct { group: u64, item: Item, cleanup: bool = false } = null,
    drop: ?struct { group: u64, item: Item } = null,
};

/// At most eight physical canonical records and four MiB decoded per turn.
/// IDs are a keyset cursor in the existing decimal-key order, not numerical
/// pagination. Disabled/deleted definitions never hide retained drain work.
pub fn read(a: A, txn: anytype, group: u64, incarnation: [32]u8, after: ?u64) !Page {
    if (group == 0 or (after != null and after.? == 0)) return error.InvalidReplicationCutoverIntent;
    const prefix = try std.fmt.allocPrint(a, "\x00\x00__metadata__:system_catalog:{d}:record:subscription:", .{group});
    const seek = if (after) |id| try storage.recordKeyAlloc(a, group, .subscription, id) else prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(seek);
    var items: std.ArrayList(Item) = .empty;
    var scanned: usize = 0;
    var bytes: usize = 0;
    var last: ?u64 = null;
    var snapshot: ?SnapshotWork = null;
    var wal: ?WalWork = null;
    var capture: @FieldType(Page, "capture") = null;
    var disable: @FieldType(Page, "disable") = null;
    var drop: @FieldType(Page, "drop") = null;
    var record_arena: std.heap.ArenaAllocator = .init(a);
    defer record_arena.deinit();
    while (entry) |row| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, prefix)) break;
        if (after != null and std.mem.eql(u8, row.key, seek)) continue;
        if (row.value.len > catalog.max_record_bytes) return error.InvalidCatalogRecord;
        if (scanned == 8 or row.value.len > catalog.max_record_bytes - bytes) return .{ .incarnation = incarnation, .items = try items.toOwnedSlice(a), .next_after = last, .snapshot = snapshot, .capture = capture, .wal = wal, .disable = disable, .drop = drop };
        _ = record_arena.reset(.retain_capacity);
        const scratch = record_arena.allocator();
        const resource = std.json.parseFromSliceLeaky(local.system_catalog_domain.Resource, scratch, row.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogRecord;
        if (resource.kind != .subscription or resource.id == 0 or !std.mem.eql(u8, row.key, try storage.recordKeyAlloc(scratch, group, .subscription, resource.id))) return error.InvalidCatalogRecord;
        const value = catalog.decodeSubscription(scratch, resource) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogRecord;
        scanned += 1;
        bytes += row.value.len;
        last = resource.id;
        if (wal == null and value.phase == .enabled and value.desired_enabled) {
            const source = value.source orelse return error.InvalidCatalogRecord;
            const owner: @FieldType(Item, "owner") = .{ .logical_subscription = .{ .database_id = value.database_id, .subscription_id = value.id } };
            const progress = (try registry.readReplicationProgress(scratch, txn, group, owner)) orelse return error.InvalidCatalogRecord;
            if (progress.birth != source.birth or !std.mem.eql(u8, &progress.origin, &source.origin_digest))
                return error.InvalidCatalogRecord;
            if (progress.pending == null and progress.checkpoint != 0 and progress.last_acknowledged != null)
                wal = .{ .group = group, .owner = owner, .source = source, .recipe_digest = try value.recipeDigest(scratch) };
        }
        if (try itemFor(scratch, value)) |item| {
            try items.append(a, item);
            // The lifecycle worker advances past the selected ID. Choosing
            // later work by phase would permanently skip earlier records
            // when the later provider keeps failing on every wrap.
            if (snapshot != null or capture != null or disable != null or drop != null) continue;
            if (item.phase == .drain and value.drain_reason == .disable) {
                if (try disableReady(scratch, txn, group, incarnation, item, value)) {
                    disable = .{ .group = group, .item = item };
                } else if (value.source == null and (try abandonedSource(scratch, txn, group, incarnation, item)) != null) {
                    disable = .{ .group = group, .item = item, .cleanup = true };
                }
            } else if (item.phase == .drain and value.drain_reason == .drop and value.source != null and
                try disableReady(scratch, txn, group, incarnation, item, value))
            {
                drop = .{ .group = group, .item = item };
            } else if (item.phase == .drop and value.source == null and
                (try abandonedSource(scratch, txn, group, incarnation, item)) != null)
            {
                drop = .{ .group = group, .item = item };
            } else if (item.phase == .initialize) {
                if (value.source) |source| {
                    const progress = (try registry.readReplicationProgress(scratch, txn, group, item.owner)) orelse return error.InvalidCatalogRecord;
                    if (progress.birth != source.birth or !std.mem.eql(u8, &progress.origin, &source.origin_digest))
                        return error.InvalidCatalogRecord;
                    if (progress.pending == null and progress.checkpoint == 0 and progress.snapshot_boundary != null and progress.snapshot_boundary.?.custody != null) {
                        snapshot = .{ .group = group, .item = item, .source = source, .generation = progress.generation, .event = progress.initialCompletion() };
                    }
                } else {
                    capture = .{ .group = group, .item = item };
                }
            }
        }
    }
    return .{ .incarnation = incarnation, .items = try items.toOwnedSlice(a), .next_after = null, .snapshot = snapshot, .capture = capture, .wal = wal, .disable = disable, .drop = drop };
}

/// Bind the native owner's runtime schema, rather than hashing the public
/// definition whose layout includes logical-only metadata.
pub fn bindDestination(a: A, table: local.metadata_local_catalog.TableRecord) !registry.ReplicationDestination {
    if (table.sql_persistence != null or table.sql_materialized_view_id != null or table.schema_json.len == 0)
        return error.UnsupportedSqlExecution;
    var prepared = try local.schema_mod.PreparedSchemaDefinition.init(a, table.schema_json);
    defer prepared.deinit(a);
    if (prepared.runtime_schema.storage_mode != .relational or prepared.runtime_schema.external_base_source != null or prepared.public_schema.sql_partition != null)
        return error.UnsupportedSqlExecution;
    const encoded = try local.storage_schema.serializeSchema(a, prepared.runtime_schema);
    defer a.free(encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(encoded, &digest, .{});
    return .{ .table_id = table.table_id, .storage_name = table.name, .schema_version = prepared.runtime_schema.version, .schema_digest = digest };
}

const max_lease_ns = 60 * std.time.ns_per_s;
pub const Lease = catalog.SubscriptionWorkLease;

fn leaseKey(a: A, group: u64, incarnation: [32]u8, owner: @FieldType(Item, "owner")) ![]const u8 {
    const identity = owner.logical_subscription;
    if (identity.subscription_id == 0) return error.InvalidReplicationCutoverIntent;
    const journal = @import("../system_catalog/replication/decision_journal.zig");
    return journal.key(a, .{ .group_id = group, .incarnation = incarnation, .database_id = identity.database_id }, try std.fmt.allocPrint(a, "subscription_lease/{x:0>16}", .{identity.subscription_id}));
}

fn readLease(a: A, txn: anytype, key: []const u8) !?Lease {
    const bytes = txn.get(key) catch |err| return if (err == error.NotFound) null else err;
    if (bytes.len > 4096) return error.InvalidCatalogRecord;
    const result = try std.json.parseFromSliceLeaky(Lease, a, bytes, .{});
    try result.validate();
    return result;
}

pub fn requireDatabase(item: Item, database_id: u64) !void {
    if (item.owner != .logical_subscription or item.owner.logical_subscription.database_id != database_id)
        return error.ReplicationCutoverAuthorityLost;
}

pub fn removeCompletedDropLease(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item) !void {
    _ = try cleanupDefinition(a, txn, group, incarnation, item, false);
    const key = try leaseKey(a, group, incarnation, item.owner);
    if (try readLease(a, txn, key) != null) try txn.delete(key);
}

pub fn removeUnstartedLease(a: A, txn: anytype, group: u64, incarnation: [32]u8, owner: @FieldType(Item, "owner")) !void {
    const key = try leaseKey(a, group, incarnation, owner);
    const lease = (try readLease(a, txn, key)) orelse return;
    if (!lease.item.owner.eql(owner)) return error.InvalidCatalogRecord;
    if (lease.provider_attempt != null) return error.LogicalReplicationLifecyclePending;
    try txn.delete(key);
}

pub fn observe(a: A, txn: anytype, group: u64, incarnation: [32]u8, owner: @FieldType(Item, "owner")) !?Lease {
    try owner.validate();
    if (owner != .logical_subscription) return error.InvalidReplicationCutoverIntent;
    const value = (try readLease(a, txn, try leaseKey(a, group, incarnation, owner))) orelse return null;
    if (!value.item.owner.eql(owner)) return error.InvalidCatalogRecord;
    return value;
}

fn writeLease(a: A, txn: anytype, key: []const u8, value: Lease) !void {
    try value.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    try txn.put(key, bytes);
}

fn readCurrent(a: A, txn: anytype, group: u64, item: Item) !catalog.Subscription {
    if (item.owner != .logical_subscription) return error.InvalidReplicationCutoverIntent;
    const id = item.owner.logical_subscription.subscription_id;
    const key = try storage.recordKeyAlloc(a, group, .subscription, id);
    const bytes = txn.get(key) catch |err| return if (err == error.NotFound) error.ReplicationCutoverAuthorityLost else err;
    if (bytes.len > catalog.max_record_bytes) return error.InvalidCatalogRecord;
    const resource = try std.json.parseFromSliceLeaky(local.system_catalog_domain.Resource, a, bytes, .{});
    if (resource.id != id or resource.kind != .subscription) return error.InvalidCatalogRecord;
    const value = try catalog.decodeSubscription(a, resource);
    const current = (try itemFor(a, value)) orelse return error.ReplicationCutoverAuthorityLost;
    if (!std.meta.eql(item, current)) return error.ReplicationCutoverAuthorityLost;
    return value;
}

fn itemFor(a: A, value: catalog.Subscription) !?Item {
    const phase: @FieldType(Item, "phase") = switch (value.phase) {
        .initializing => .initialize,
        .draining => .drain,
        .dropping => .drop,
        else => return null,
    };
    return .{
        .owner = .{ .logical_subscription = .{ .database_id = value.database_id, .subscription_id = value.id } },
        .generation = value.generation,
        .recipe_digest = try value.recipeDigest(a),
        .operation = value.operation orelse return error.InvalidCatalogRecord,
        .phase = phase,
    };
}

pub fn claim(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item, worker: [16]u8, now_ns: u64, expires_at_ns: u64) !Lease {
    _ = try readCurrent(a, txn, group, item);
    if (now_ns == 0 or expires_at_ns <= now_ns or expires_at_ns - now_ns > max_lease_ns or
        std.mem.allEqual(u8, &worker, 0)) return error.InvalidReplicationCutoverIntent;
    const key = try leaseKey(a, group, incarnation, item.owner);
    const prior = try readLease(a, txn, key);
    var attempt: ?[16]u8 = null;
    var provider_preflight_identity: ?catalog.Digest = null;
    var origin: ?catalog.Digest = null;
    var provider_export: ?catalog.SubscriptionExport = null;
    if (prior) |existing| {
        if (!existing.item.owner.eql(item.owner)) return error.InvalidCatalogRecord;
        const same_item = std.meta.eql(existing.item, item);
        if (existing.provider_attempt != null and !same_item) return error.LogicalReplicationLifecyclePending;
        if (existing.expires_at_ns > now_ns) {
            if (!same_item or !std.mem.eql(u8, &existing.worker, &worker)) return error.ReplicationCutoverAuthorityLost;
            if (existing.expires_at_ns >= expires_at_ns) return existing;
            var renewed = existing;
            renewed.expires_at_ns = expires_at_ns;
            try writeLease(a, txn, key, renewed);
            return renewed;
        }
        if (same_item) {
            attempt = existing.provider_attempt;
            provider_preflight_identity = existing.provider_preflight_identity;
            origin = existing.provider_origin;
            provider_export = existing.provider_export;
        }
    }
    const next: Lease = .{
        .item = item,
        .worker = worker,
        .generation = std.math.add(u64, if (prior) |existing| existing.generation else 0, 1) catch return error.CatalogGenerationExhausted,
        .expires_at_ns = expires_at_ns,
        .provider_attempt = attempt,
        .provider_preflight_identity = provider_preflight_identity,
        .provider_origin = origin,
        .provider_export = provider_export,
    };
    try writeLease(a, txn, key, next);
    return next;
}

fn liveLease(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, now_ns: u64) !Lease {
    try lease.validate();
    _ = try readCurrent(a, txn, group, lease.item);
    return matchLease(a, txn, group, incarnation, lease, now_ns);
}

fn matchLease(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, now_ns: u64) !Lease {
    if (now_ns == 0) return error.InvalidReplicationCutoverIntent;
    const current = (try observe(a, txn, group, incarnation, lease.item.owner)) orelse return error.ReplicationCutoverAuthorityLost;
    if (!std.meta.eql(current.item, lease.item) or current.generation != lease.generation or
        !std.mem.eql(u8, &current.worker, &lease.worker) or now_ns >= current.expires_at_ns)
        return error.ReplicationCutoverAuthorityLost;
    return current;
}

fn requireAttempt(lease: Lease, attempt: [16]u8) !void {
    const actual = lease.provider_attempt orelse return error.ReplicationCutoverAuthorityLost;
    if (!std.mem.eql(u8, &actual, &attempt)) return error.ReplicationCutoverAuthorityLost;
}

pub fn authorize(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, attempt: [16]u8, origin: ?catalog.Digest, now_ns: u64) !bool {
    const current = liveLease(a, txn, group, incarnation, lease, now_ns) catch |err| switch (err) {
        error.ReplicationCutoverAuthorityLost => return false,
        else => return err,
    };
    requireAttempt(current, attempt) catch return false;
    if (lease.provider_export != null and !std.meta.eql(lease.provider_export, current.provider_export)) return false;
    if (origin) |digest| {
        const actual = current.provider_origin orelse return false;
        return std.mem.eql(u8, &actual, &digest);
    }
    return true;
}

pub const CleanupSource = union(enum) {
    committed: registry.SnapshotBoundary,
    exported: catalog.SubscriptionExport,
    unexported: catalog.Digest,
};

pub const CleanupDefinition = struct {
    actor: catalog.ExecutionActor,
    connection: []const u8,
    slot_name: []const u8,
    source: CleanupSource,
    checkpoint: u64,
};

pub fn cleanupDefinition(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item, disabling: bool) !CleanupDefinition {
    const value = try readCurrent(a, txn, group, item);
    if (disabling) {
        if (value.phase != .draining or value.drain_reason != .disable or value.source != null) return error.ReplicationCutoverAuthorityLost;
    } else if (value.phase != .dropping and (value.phase != .draining or value.drain_reason != .drop))
        return error.ReplicationCutoverAuthorityLost;
    if (value.parameters.slot_name != .name) return error.ReplicationCutoverAuthorityLost;
    const actor = value.execution_actor orelse return error.Forbidden;
    const source = value.source orelse {
        const abandoned = (try abandonedSource(a, txn, group, incarnation, item)) orelse return error.ReplicationCutoverAuthorityLost;
        return .{
            .actor = actor,
            .connection = value.connection,
            .slot_name = value.parameters.slot_name.name,
            .source = abandoned,
            .checkpoint = if (abandoned == .exported) abandoned.exported.checkpoint else 0,
        };
    };
    const progress = (try registry.readReplicationProgress(a, txn, group, item.owner)) orelse
        return error.ReplicationCutoverAuthorityLost;
    if (progress.birth != source.birth or !std.mem.eql(u8, &progress.origin, &source.origin_digest))
        return error.ReplicationCutoverAuthorityLost;
    if (progress.pending != null) return error.LogicalReplicationLifecyclePending;
    const boundary = progress.snapshot_boundary orelse return error.ReplicationCutoverAuthorityLost;
    if (boundary.custody == null)
        return error.ReplicationCutoverAuthorityLost;
    return .{ .actor = actor, .connection = value.connection, .slot_name = value.parameters.slot_name.name, .source = .{ .committed = boundary }, .checkpoint = @max(progress.checkpoint, boundary.source_point) };
}

fn abandonedSource(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item) !?CleanupSource {
    const lease = (try observe(a, txn, group, incarnation, item.owner)) orelse return null;
    if (lease.item.phase != .initialize or lease.item.generation >= item.generation or
        !std.mem.eql(u8, &lease.item.recipe_digest, &item.recipe_digest))
        return error.ReplicationCutoverAuthorityLost;
    if (lease.provider_export) |receipt| return .{ .exported = receipt };
    if (lease.provider_origin != null) return null;
    if (lease.provider_preflight_identity) |provider| return .{ .unexported = provider };
    return null;
}

pub const CaptureDefinition = struct {
    actor: ?catalog.ExecutionActor = null,
    connection: []const u8,
    publications: []const []const u8,
    options: local.system_catalog_logical_replication.SubscriptionOptions,
};

/// Credentials cross only the private worker boundary after the metadata
/// writer has authorized the worker's claim. Reading the recipe alone
/// leaves the provider attempt unstarted.
pub fn captureDefinition(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, attempt: ?[16]u8, now_ns: u64) !CaptureDefinition {
    try lease.validate();
    const value = try readCurrent(a, txn, group, lease.item);
    if (value.phase != .initializing or !value.desired_enabled or value.source != null)
        return error.ReplicationCutoverAuthorityLost;
    const current = try matchLease(a, txn, group, incarnation, lease, now_ns);
    if (!std.meta.eql(current.provider_attempt, attempt) or !std.meta.eql(lease.provider_attempt, attempt))
        return error.ReplicationCutoverAuthorityLost;
    _ = try executionPrincipal(a, txn, group, value.execution_actor orelse return error.Forbidden);
    return .{ .connection = value.connection, .publications = value.publications, .options = value.parameters, .actor = value.execution_actor };
}

/// The persistent recipe stores a role instance, so dropping and reusing a
/// role name cannot authorize new subscription work.
pub fn executionPrincipal(a: A, txn: anytype, group: u64, actor: catalog.ExecutionActor) !local.system_catalog_privileges.Principal {
    try actor.validate();
    const selected = actor.current_role orelse return actor.session_user;
    const auth = @import("../system_catalog/authorization_storage.zig");
    if (try auth.roleByPrincipal(a, txn, group, selected) == null) return error.Forbidden;
    if (actor.session_user.kind == .bootstrap) return selected;
    const reachable = try auth.closure(a, txn, group, actor.session_user, .set);
    defer a.free(reachable);
    for (reachable) |role| if (role.principal().eql(selected)) return selected;
    return error.Forbidden;
}

pub fn destinationPolicyEpoch(a: A, txn: anytype, group: u64, table_id: u64) !local.api_replication_source_apply.PolicyEpoch {
    const stamp = (try storage.loadPolicyPublicationStamp(a, txn, group, table_id)) orelse return .{};
    return .{ .generation = stamp.generation, .catalog_epoch = stamp.catalog_epoch };
}

pub fn requireDestinationRights(a: A, txn: anytype, group: u64, principal: local.system_catalog_privileges.Principal, run_as_owner: bool, table_id: u64, rights: u16) !void {
    return requireDestinationRightsAtEpoch(a, txn, group, principal, run_as_owner, table_id, rights, null);
}

pub fn requireDestinationRightsAtEpoch(a: A, txn: anytype, group: u64, principal: local.system_catalog_privileges.Principal, run_as_owner: bool, table_id: u64, rights: u16, expected: ?local.api_replication_source_apply.PolicyEpoch) !void {
    const auth = @import("../system_catalog/authorization_storage.zig");
    const acl = local.system_catalog_privileges;
    const mutations = acl.Access.bit(.insert) | acl.Access.bit(.update) | acl.Access.bit(.delete);
    if (rights & mutations == 0 or rights & ~(mutations | acl.Access.bit(.select)) != 0)
        return error.Forbidden;
    const stamp = try storage.loadPolicyPublicationStamp(a, txn, group, table_id);
    if (expected) |epoch| {
        const current: @TypeOf(epoch) = if (stamp) |policy|
            .{ .generation = policy.generation, .catalog_epoch = policy.catalog_epoch }
        else
            .{};
        if (!std.meta.eql(current, epoch)) return error.RowPolicyCatalogChanged;
    }
    if (stamp) |policy| switch (policy.phase) {
        .active, .disabled => {},
        else => return error.RowPolicyCatalogChanged,
    };
    if (principal.kind == .bootstrap) return;
    const record = (try auth.read(a, txn, group, .{ .table = table_id })) orelse return error.Forbidden;
    var effective = principal;
    if (!run_as_owner and !principal.eql(record.owner)) {
        const candidates = try auth.closure(a, txn, group, principal, .set);
        defer a.free(candidates);
        for (candidates) |role| {
            if (role.principal().eql(record.owner)) {
                effective = record.owner;
                break;
            }
        } else return error.Forbidden;
    }
    if (stamp) |policy| {
        if (policy.phase == .active and !effective.eql(record.owner)) return error.Forbidden;
    }
    const inherited = try auth.closure(a, txn, group, effective, .inherit);
    defer a.free(inherited);
    const principals = try a.alloc(acl.Principal, inherited.len + 1);
    defer a.free(principals);
    principals[0] = effective;
    for (principals[1..], inherited) |*key, role| key.* = role.principal();
    var resolver = try acl.AccessResolver.init(a, principals);
    defer resolver.deinit(a);
    const access = try resolver.captureValidated(a, record.object());
    if (access.rights & rights != rights) return error.Forbidden;
}

pub fn establishSource(a: A, txn: anytype, group: u64, incarnation: [32]u8, value: catalog.Subscription, lease: Lease, attempt: [16]u8, source: catalog.SourceReference, now_ns: u64) !catalog.Subscription {
    try source.validate();
    if (value.phase != .initializing or value.source != null or !value.desired_enabled or
        source.birth != value.generation or lease.item.phase != .initialize or
        !lease.item.owner.eql(.{ .logical_subscription = .{ .database_id = value.database_id, .subscription_id = value.id } }))
        return error.ReplicationCutoverAuthorityLost;
    if (lease.provider_export) |receipt| if (receipt.retiring) return error.ReplicationCutoverAuthorityLost;
    if (!try authorize(a, txn, group, incarnation, lease, attempt, source.origin_digest, now_ns))
        return error.ReplicationCutoverAuthorityLost;
    var next = value;
    next.source = source;
    try next.validate(a);
    return next;
}

pub fn completeInitialCopy(a: A, value: catalog.Subscription, source: catalog.SourceReference) !?catalog.Subscription {
    const current = value.source orelse return error.ReplicationCutoverAuthorityLost;
    if (!std.meta.eql(current, source)) return error.ReplicationCutoverAuthorityLost;
    if (value.phase == .draining) return null;
    if (value.phase != .initializing or !value.desired_enabled) return error.ReplicationCutoverAuthorityLost;
    var next = value;
    next.phase = .enabled;
    next.operation = null;
    next.generation = std.math.add(u64, value.generation, 1) catch return error.CatalogGenerationExhausted;
    try next.validate(a);
    return next;
}

/// Finalize a disable only after the exact source has no admitted transaction.
/// A provider attempt without established source custody needs reconciliation
/// before the worker can retire this generation.
pub fn finishDisable(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item) !void {
    var value = try readCurrent(a, txn, group, item);
    if (value.phase != .draining or value.drain_reason != .disable)
        return error.ReplicationCutoverAuthorityLost;
    if (!try disableReady(a, txn, group, incarnation, item, value)) return error.LogicalReplicationLifecyclePending;
    if (value.source == null) try removeUnstartedLease(a, txn, group, incarnation, item.owner);
    value.phase = .disabled;
    value.operation = null;
    value.drain_reason = null;
    value.generation = std.math.add(u64, value.generation, 1) catch return error.CatalogGenerationExhausted;
    try value.validate(a);
    const key = try storage.recordKeyAlloc(a, group, .subscription, value.id);
    var resource = try std.json.parseFromSliceLeaky(local.system_catalog_domain.Resource, a, try txn.get(key), .{});
    resource.source_json = try std.json.Stringify.valueAlloc(a, value, .{});
    try txn.put(key, try std.json.Stringify.valueAlloc(a, resource, .{}));
}

pub fn finishDisableExport(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item, checkpoint: u64) !void {
    const definition = try cleanupDefinition(a, txn, group, incarnation, item, true);
    if (definition.checkpoint != checkpoint) return error.ReplicationCutoverAuthorityLost;
    try txn.delete(try leaseKey(a, group, incarnation, item.owner));
    try finishDisable(a, txn, group, incarnation, item);
}

fn disableReady(a: A, txn: anytype, group: u64, incarnation: [32]u8, item: Item, value: catalog.Subscription) !bool {
    if (value.source) |source| {
        const progress = (try registry.readReplicationProgress(a, txn, group, item.owner)) orelse return error.InvalidCatalogRecord;
        if (progress.birth != source.birth or !std.mem.eql(u8, &progress.origin, &source.origin_digest))
            return error.ReplicationCutoverAuthorityLost;
        if (progress.pending != null) return false;
    } else if (try readLease(a, txn, try leaseKey(a, group, incarnation, item.owner))) |lease| {
        if (lease.provider_attempt != null) return false;
    }
    return true;
}

pub fn transitionExport(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, action: @FieldType(@FieldType(local.system_catalog_logical_replication_publisher.Control, "transition_subscription_export"), "action"), now_ns: u64) !Lease {
    if (action != .cancel_unexported) {
        const value = try readCurrent(a, txn, group, lease.item);
        if (value.source != null or value.phase != .initializing) return error.ReplicationCutoverAuthorityLost;
    }
    var current = try matchLease(a, txn, group, incarnation, lease, now_ns);
    if (!std.meta.eql(current.provider_export, lease.provider_export) or
        !std.meta.eql(current.provider_attempt, lease.provider_attempt) or !std.meta.eql(current.provider_origin, lease.provider_origin))
        return error.ReplicationCutoverAuthorityLost;
    current.generation = std.math.add(u64, current.generation, 1) catch return error.CatalogGenerationExhausted;
    switch (action) {
        .fence => {
            if (current.provider_export == null) return error.ReplicationCutoverAuthorityLost;
            current.provider_export.?.retiring = true;
        },
        .retire => {
            if (current.provider_export == null or !current.provider_export.?.retiring) return error.ReplicationCutoverAuthorityLost;
            current.provider_attempt = null;
            current.provider_preflight_identity = null;
            current.provider_origin = null;
            current.provider_export = null;
        },
        .cancel_unexported => {
            if (current.provider_attempt == null or current.provider_origin != null or current.provider_export != null)
                return error.ReplicationCutoverAuthorityLost;
            current.provider_attempt = null;
            current.provider_preflight_identity = null;
        },
    }
    try writeLease(a, txn, try leaseKey(a, group, incarnation, lease.item.owner), current);
    return current;
}

pub fn bindExport(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, attempt: [16]u8, origin: catalog.Digest, receipt: catalog.SubscriptionExport, now_ns: u64) !Lease {
    receipt.validate() catch return error.InvalidReplicationCutoverIntent;
    if (receipt.retiring or std.mem.allEqual(u8, &origin, 0)) return error.InvalidReplicationCutoverIntent;
    var current = try liveLease(a, txn, group, incarnation, lease, now_ns);
    try requireAttempt(current, attempt);
    if (current.provider_preflight_identity) |identity| if (!std.mem.eql(u8, &identity, &receipt.provider_identity))
        return error.ForeignProviderIdentityMismatch;
    if (current.provider_origin) |prior| {
        if (!std.mem.eql(u8, &prior, &origin)) return error.ForeignProviderIdentityMismatch;
    }
    if (current.provider_export) |prior| {
        if (!std.meta.eql(prior, receipt)) return error.ForeignProviderIdentityMismatch;
        return current;
    }
    current.provider_origin = origin;
    current.provider_export = receipt;
    try writeLease(a, txn, try leaseKey(a, group, incarnation, current.item.owner), current);
    return current;
}

pub fn bindOrigin(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, attempt: [16]u8, origin: catalog.Digest, now_ns: u64) !Lease {
    var current = try liveLease(a, txn, group, incarnation, lease, now_ns);
    try requireAttempt(current, attempt);
    if (std.mem.allEqual(u8, &origin, 0)) return error.InvalidReplicationCutoverIntent;
    if (current.provider_origin) |actual| {
        if (!std.mem.eql(u8, &actual, &origin)) return error.ForeignProviderIdentityMismatch;
        return current;
    }
    current.provider_origin = origin;
    try writeLease(a, txn, try leaseKey(a, group, incarnation, lease.item.owner), current);
    return current;
}

/// Persist the first provider attempt before the callback. A replay or lease
/// takeover must reconcile the original attempt against the provider.
pub fn start(a: A, txn: anytype, group: u64, incarnation: [32]u8, lease: Lease, attempt: [16]u8, identity: ?catalog.Digest, now_ns: u64) !Lease {
    if (std.mem.allEqual(u8, &attempt, 0)) return error.InvalidReplicationCutoverIntent;
    var current = try liveLease(a, txn, group, incarnation, lease, now_ns);
    if (current.provider_attempt) |original| {
        if (!std.mem.eql(u8, &original, &attempt)) return error.ReplicationCutoverAuthorityLost;
        if (!std.meta.eql(current.provider_preflight_identity, identity)) return error.ForeignProviderIdentityMismatch;
        return current;
    }
    current.provider_attempt = attempt;
    current.provider_preflight_identity = identity;
    try writeLease(a, txn, try leaseKey(a, group, incarnation, lease.item.owner), current);
    return current;
}

test "SQL logical replication subscription work pages stable owner identities" {
    const DocStore = local.storage_docstore.DocStore;
    const replication = @import("../system_catalog/replication/logical_replication.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/subscription-bootstrap", .{temporary.sub_path}, 0);
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    var catalog_incarnation: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&catalog_incarnation, &incarnation);
    const lease = admit: {
        var store = try DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            for (0..9) |index| {
                const request: replication.Request = .{ .admission = .{ .nonce = @splat(@as(u8, @intCast(index + 1))), .head = 0 }, .incarnation = catalog_incarnation, .database = "default", .database_id = 1, .name = try std.fmt.allocPrint(a, "sub_{d}", .{index}), .operation = .{ .create_subscription = .{ .owner = .auth_disabled, .execution_actor = .{ .session_user = .bootstrap, .credential = "" }, .connection = "host=actual-definition dbname=source", .publications = &.{"pub"}, .options = .{ .slot_name = .{ .name = try std.fmt.allocPrint(a, "slot_{d}", .{index}) } }, .intent = .{ .id = @splat(7), .body_digest = @splat(8) } } } };
                const meta = try storage.readMeta(a, &txn, 1);
                const planned = try replication.plan(a, .{ .alloc = a, .txn = &txn, .group_id = 1, .meta = meta }, request);
                try storage.applyDelta(a, &txn, 1, planned.delta, meta, try replication.boundDigest(a, try replication.observation(a, request)));
            }
            try txn.commit();
        }
        const item = read_work: {
            var snapshot = try store.beginReadTxn();
            defer snapshot.abort();
            const first = try read(a, &snapshot, 1, incarnation, null);
            try std.testing.expectEqual(@as(usize, 8), first.items.len);
            try std.testing.expect(first.next_after != null);
            for (first.items) |item| {
                try std.testing.expectEqual(.initialize, item.phase);
                try std.testing.expectEqual(@as(u64, 1), item.generation);
            }
            const second = try read(a, &snapshot, 1, incarnation, first.next_after);
            try std.testing.expectEqual(@as(usize, 1), second.items.len);
            try std.testing.expectEqual(@as(?u64, null), second.next_after);
            for (first.items) |item| try std.testing.expect(!item.owner.eql(second.items[0].owner));
            const encoded = try std.json.Stringify.valueAlloc(a, first, .{});
            try std.testing.expect(std.mem.indexOf(u8, encoded, "host=actual-definition") == null);
            break :read_work first.items[0];
        };
        var writer = try store.beginWriteTxn();
        errdefer writer.abort();
        const journal = @import("../system_catalog/replication/decision_journal.zig");
        const realm: local.system_catalog_logical_replication_publisher.Realm = .{ .group_id = 1, .incarnation = incarnation, .database_id = item.owner.logical_subscription.database_id };
        const claim_request: journal.ControlRequest = .{ .realm = realm, .nonce = @splat(2), .observed_head = 0, .action = .{ .claim_subscription = .{
            .item = item,
            .worker = @splat(4),
            .observed_at_unix_ns = 10,
            .expires_at_unix_ns = 100,
        } } };
        const claimed = try journal.planControl(a, &writer, claim_request, &.{});
        try claimed.apply(&writer);
        const lease = claimed.result.subscription_lease;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, claim(a, &writer, 1, incarnation, item, @splat(5), 11, 100));
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, captureDefinition(a, &writer, 1, incarnation, lease, @splat(6), 12));
        try std.testing.expectEqualStrings("host=actual-definition dbname=source", (try captureDefinition(a, &writer, 1, incarnation, lease, null, 12)).connection);
        const start_request: journal.ControlRequest = .{ .realm = realm, .nonce = @splat(3), .observed_head = 1, .action = .{ .start_subscription = .{
            .lease = lease,
            .attempt = @splat(6),
            .provider_preflight_identity = @splat(22),
            .observed_at_unix_ns = 12,
        } } };
        const started = try journal.planControl(a, &writer, start_request, &.{});
        try started.apply(&writer);
        const replay = try journal.planControl(a, &writer, start_request, &.{});
        try std.testing.expectEqual(@as(usize, 0), replay.writes.len);
        try std.testing.expectEqual(@as(?[16]u8, @splat(6)), started.result.subscription_lease.provider_attempt);
        try std.testing.expectEqualDeep(started.result.subscription_lease, replay.result.subscription_lease);
        var conflicting_start = start_request;
        conflicting_start.nonce = @splat(11);
        conflicting_start.observed_head = 2;
        conflicting_start.action.start_subscription.attempt = @splat(7);
        const refusal = try journal.planControl(a, &writer, conflicting_start, &.{});
        try std.testing.expectEqual(.ReplicationCutoverAuthorityLost, refusal.result.refused);
        try refusal.apply(&writer);
        try std.testing.expectEqualDeep(started.result.subscription_lease, (try observe(a, &writer, 1, incarnation, item.owner)).?);
        try std.testing.expect(try authorize(a, &writer, 1, incarnation, lease, @splat(6), null, 13));
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, captureDefinition(a, &writer, 1, incarnation, lease, null, 13));
        const definition = try captureDefinition(a, &writer, 1, incarnation, started.result.subscription_lease, @splat(6), 13);
        try std.testing.expectEqualStrings("host=actual-definition dbname=source", definition.connection);
        try std.testing.expectEqualStrings("pub", definition.publications[0]);
        try std.testing.expect(definition.options.copy_data);
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, captureDefinition(a, &writer, 1, incarnation, lease, @splat(7), 13));
        try std.testing.expect(!try authorize(a, &writer, 1, incarnation, lease, @splat(6), @splat(9), 13));
        const bind_request: journal.ControlRequest = .{ .realm = realm, .nonce = @splat(8), .observed_head = 3, .action = .{ .bind_subscription_origin = .{
            .lease = lease,
            .attempt = @splat(6),
            .origin = @splat(9),
            .observed_at_unix_ns = 13,
        } } };
        const bound = try journal.planControl(a, &writer, bind_request, &.{});
        try bound.apply(&writer);
        try std.testing.expect(try authorize(a, &writer, 1, incarnation, lease, @splat(6), @splat(9), 14));
        try std.testing.expectError(error.ForeignProviderIdentityMismatch, bindOrigin(a, &writer, 1, incarnation, lease, @splat(6), @splat(10), 14));
        const exported: catalog.SubscriptionExport = .{ .provider_identity = @splat(22), .checkpoint = 1000 };
        const export_request: journal.ControlRequest = .{ .realm = realm, .nonce = @splat(21), .observed_head = 4, .action = .{ .bind_subscription_export = .{
            .lease = bound.result.subscription_lease,
            .attempt = @splat(6),
            .origin = @splat(9),
            .receipt = exported,
            .observed_at_unix_ns = 14,
        } } };
        const export_plan = try journal.planControl(a, &writer, export_request, &.{});
        try export_plan.apply(&writer);
        try std.testing.expectEqualDeep(exported, export_plan.result.subscription_lease.provider_export.?);
        var conflicting = exported;
        conflicting.checkpoint += 1;
        try std.testing.expectError(error.ForeignProviderIdentityMismatch, bindExport(a, &writer, 1, incarnation, export_plan.result.subscription_lease, @splat(6), @splat(9), conflicting, 15));

        try writer.commit();
        break :admit lease;
    };
    const item = lease.item;
    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();
    var recovery = try store.beginWriteTxn();
    defer recovery.abort();
    var stale = item;
    stale.generation += 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, claim(a, &recovery, 1, incarnation, stale, @splat(5), 101, 200));
    const successor = try claim(a, &recovery, 1, incarnation, item, @splat(5), 101, 200);
    try std.testing.expectEqual(lease.generation + 1, successor.generation);
    try std.testing.expectEqual(@as(?[16]u8, @splat(6)), successor.provider_attempt);
    try std.testing.expectEqual(@as(?catalog.Digest, @splat(9)), successor.provider_origin);
    try std.testing.expectEqual(@as(u64, 1000), successor.provider_export.?.checkpoint);
    try std.testing.expect(!try authorize(a, &recovery, 1, incarnation, lease, @splat(6), @splat(9), 101));
    try std.testing.expect(try authorize(a, &recovery, 1, incarnation, successor, @splat(6), @splat(9), 101));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, start(a, &recovery, 1, incarnation, lease, @splat(6), null, 102));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, start(a, &recovery, 1, incarnation, successor, @splat(7), null, 102));
    try std.testing.expectEqualDeep(successor, try start(a, &recovery, 1, incarnation, successor, @splat(6), @splat(22), 102));
    const resource = (try storage.getById(a, &recovery, 1, .subscription, item.owner.logical_subscription.subscription_id)).?;
    const current = try catalog.decodeSubscription(a, resource.value);
    const source: catalog.SourceReference = .{ .birth = current.generation, .origin_digest = @splat(9) };
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, establishSource(a, &recovery, 1, incarnation, current, lease, @splat(6), source, 102));
    var substituted = source;
    substituted.origin_digest[0] ^= 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, establishSource(a, &recovery, 1, incarnation, current, successor, @splat(6), substituted, 102));
    try std.testing.expectEqual(@as(?catalog.Digest, @splat(22)), successor.provider_preflight_identity);
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, start(a, &recovery, 1, incarnation, successor, @splat(6), @splat(23), 102));
    const joined = try establishSource(a, &recovery, 1, incarnation, current, successor, @splat(6), source, 102);
    try std.testing.expectEqual(.initializing, joined.phase);
    try std.testing.expectEqualDeep(source, joined.source.?);
    const live = (try completeInitialCopy(a, joined, source)).?;
    try std.testing.expectEqual(.enabled, live.phase);
    try std.testing.expectEqual(joined.generation + 1, live.generation);
    try std.testing.expect(live.operation == null);
    const fenced = try transitionExport(a, &recovery, 1, incarnation, successor, .fence, 103);
    try std.testing.expect(!try authorize(a, &recovery, 1, incarnation, successor, @splat(6), @splat(9), 103));
    try std.testing.expect(try authorize(a, &recovery, 1, incarnation, fenced, @splat(6), @splat(9), 103));
    try std.testing.expect(fenced.provider_export.?.retiring);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, establishSource(a, &recovery, 1, incarnation, current, fenced, @splat(6), source, 103));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, transitionExport(a, &recovery, 1, incarnation, successor, .retire, 104));
    const retired_export = try transitionExport(a, &recovery, 1, incarnation, fenced, .retire, 104);
    try std.testing.expect(retired_export.generation > fenced.generation);
    try std.testing.expect(retired_export.provider_attempt == null and retired_export.provider_origin == null and retired_export.provider_export == null);
    const refused = try start(a, &recovery, 1, incarnation, retired_export, @splat(17), @splat(25), 104);
    {
        const draining = try current.requestEnabled(a, false, .{ .id = @splat(30), .body_digest = @splat(31) });
        var draining_resource = resource.value;
        draining_resource.source_json = try std.json.Stringify.valueAlloc(a, draining, .{});
        try storage.writeResource(a, &recovery, 1, draining_resource);
        const pending = (try read(a, &recovery, 1, incarnation, null)).disable.?;
        try std.testing.expect(pending.cleanup);
        const definition = try cleanupDefinition(a, &recovery, 1, incarnation, pending.item, true);
        try std.testing.expectEqual(@as(u64, 0), definition.checkpoint);
        try std.testing.expectEqual(@as(catalog.Digest, @splat(25)), definition.source.unexported);
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, finishDisableExport(a, &recovery, 1, incarnation, pending.item, 1));
        try storage.writeResource(a, &recovery, 1, resource.value);
    }
    const canceled = try transitionExport(a, &recovery, 1, incarnation, refused, .cancel_unexported, 104);
    try std.testing.expect(canceled.provider_attempt == null and canceled.generation > refused.generation);
    try std.testing.expect(!try authorize(a, &recovery, 1, incarnation, refused, @splat(17), null, 104));
    const restarted = try start(a, &recovery, 1, incarnation, canceled, @splat(18), null, 104);
    try std.testing.expectEqual(@as(?[16]u8, @splat(18)), restarted.provider_attempt);
    const proven: catalog.SubscriptionExport = .{ .provider_identity = @splat(25), .checkpoint = 2000 };
    const rebound = try bindExport(a, &recovery, 1, incarnation, restarted, @splat(18), @splat(24), proven, 105);
    try std.testing.expectEqual(@as(?catalog.Digest, @splat(24)), rebound.provider_origin);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, transitionExport(a, &recovery, 1, incarnation, rebound, .cancel_unexported, 105));
    try std.testing.expectEqualDeep(rebound, try bindExport(a, &recovery, 1, incarnation, restarted, @splat(18), @splat(24), proven, 105));
    const requested_drop = try current.requestDrop(a, .{ .id = @splat(26), .body_digest = @splat(27) });
    var dropping_resource = resource.value;
    dropping_resource.source_json = try std.json.Stringify.valueAlloc(a, requested_drop, .{});
    try storage.writeResource(a, &recovery, 1, dropping_resource);
    const drop_page = try read(a, &recovery, 1, incarnation, null);
    const drop_work = drop_page.drop.?.item;
    try std.testing.expect(drop_page.capture == null);
    const next_page = try read(a, &recovery, 1, incarnation, drop_work.owner.logical_subscription.subscription_id);
    try std.testing.expect(next_page.drop == null);
    try std.testing.expect(next_page.capture != null);
    const next_item = next_page.capture.?.item;
    var later = (try storage.getById(a, &recovery, 1, .subscription, next_item.owner.logical_subscription.subscription_id)).?.value;
    const later_definition = try catalog.decodeSubscription(a, later);
    const later_disabled = try later_definition.requestEnabled(a, false, .{ .id = @splat(28), .body_digest = @splat(29) });
    later.source_json = try std.json.Stringify.valueAlloc(a, later_disabled, .{});
    try storage.writeResource(a, &recovery, 1, later);
    const ordered_page = try read(a, &recovery, 1, incarnation, null);
    try std.testing.expect(ordered_page.drop != null and ordered_page.disable == null);
    const following = try read(a, &recovery, 1, incarnation, drop_work.owner.logical_subscription.subscription_id);
    try std.testing.expectEqualDeep((try itemFor(a, later_disabled)).?, following.disable.?.item);
    later.source_json = try std.json.Stringify.valueAlloc(a, later_definition, .{});
    try storage.writeResource(a, &recovery, 1, later);
    const drop_definition = try cleanupDefinition(a, &recovery, 1, incarnation, drop_work, false);
    try std.testing.expectEqualDeep(proven, drop_definition.source.exported);
    try std.testing.expectEqual(@as(u64, 2000), drop_definition.checkpoint);

    const disabling = try joined.requestEnabled(a, false, .{ .id = @splat(14), .body_digest = @splat(15) });
    try std.testing.expect((try completeInitialCopy(a, disabling, source)) == null);
    var updated = resource.value;
    var unbound = disabling;
    unbound.source = null;
    updated.source_json = try std.json.Stringify.valueAlloc(a, unbound, .{});
    try storage.writeResource(a, &recovery, 1, updated);
    const drain = (try itemFor(a, unbound)).?;
    const cleanup_work = (try read(a, &recovery, 1, incarnation, null)).disable.?;
    try std.testing.expect(cleanup_work.cleanup);
    try std.testing.expectEqualDeep(drain, cleanup_work.item);
    try std.testing.expectError(error.LogicalReplicationLifecyclePending, finishDisable(a, &recovery, 1, incarnation, drain));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, finishDisableExport(a, &recovery, 1, incarnation, drain, proven.checkpoint + 1));
    try finishDisableExport(a, &recovery, 1, incarnation, drain, proven.checkpoint);
    try std.testing.expect((try observe(a, &recovery, 1, incarnation, drain.owner)) == null);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, finishDisable(a, &recovery, 1, incarnation, drain));
    const retired = (try storage.getById(a, &recovery, 1, .subscription, updated.id)).?;
    const disabled = try catalog.decodeSubscription(a, retired.value);
    try std.testing.expectEqual(.disabled, disabled.phase);
    try std.testing.expectEqual(unbound.generation + 1, disabled.generation);
    const no_source_drop = try disabled.requestDrop(a, .{ .id = @splat(16), .body_digest = @splat(17) });
    updated.source_json = try std.json.Stringify.valueAlloc(a, no_source_drop, .{});
    try storage.writeResource(a, &recovery, 1, updated);
    try std.testing.expect((try read(a, &recovery, 1, incarnation, null)).drop == null);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, cleanupDefinition(a, &recovery, 1, incarnation, (try itemFor(a, no_source_drop)).?, false));
}

test "SQL logical replication subscription work binds the canonical native schema and rejects non-native targets" {
    const a = std.testing.allocator;
    var table: local.metadata_local_catalog.TableRecord = .{
        .table_id = 19,
        .name = "physical-destination",
        .schema_json = "{\"version\":7,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}}}}",
    };
    const binding = try bindDestination(a, table);
    try std.testing.expectEqual(@as(u64, 19), binding.table_id);
    try std.testing.expectEqualStrings(table.name, binding.storage_name);
    try std.testing.expectEqual(@as(u32, 7), binding.schema_version);
    var definition = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer definition.deinit(a);
    const native = try local.schema_mod.deriveRuntimeTableSchema(a, definition);
    defer local.storage_schema.freeSchema(a, native);
    const encoded = try local.storage_schema.serializeSchema(a, native);
    defer a.free(encoded);
    try std.testing.expectEqual(local.storage_db_relational_schema_owner_contract.hash(encoded), binding.schema_digest);
    table.sql_materialized_view_id = 4;
    try std.testing.expectError(error.UnsupportedSqlExecution, bindDestination(a, table));
    table.sql_materialized_view_id = null;
    table.schema_json = "{\"version\":7,\"storage_mode\":\"document\"}";
    try std.testing.expectError(error.UnsupportedSqlExecution, bindDestination(a, table));
}

test "SQL logical replication subscription work revalidates SET ROLE membership before new effects" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var dir = try local.common_test_directory.TestDirectory.init("subscription-role-fence");
    defer dir.cleanup();
    const path = try a.dupeSentinel(u8, dir.path(), 0);
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    var txn = try store.beginWriteTxn();
    defer txn.abort();
    const domain = local.system_catalog_domain;
    const privileges = local.system_catalog_privileges;
    const user: privileges.Principal = .{ .kind = .user, .instance = @splat(1) };
    const role: privileges.Role = .{ .id = 100, .instance = @splat(2), .name = "subscription_runner", .creator = .bootstrap };
    const actor: catalog.ExecutionActor = .{ .session_user = user, .current_role = role.principal(), .credential = "basic:subscriber" };
    const membership: privileges.Record = .{
        .scope = .{ .role = role.id },
        .generation = 1,
        .owner = .bootstrap,
        .grants = &.{.{ .grantor = .bootstrap, .grantee = user, .privilege = .membership, .inherit = false }},
    };
    try storage.importState(a, &txn, 1, .{
        .revision = 1,
        .next_id = 101,
        .resources = &.{ domain.default_database, domain.default_namespace, .{
            .kind = .role,
            .id = role.id,
            .name = role.name,
            .source_json = try std.json.Stringify.valueAlloc(a, role, .{}),
        } },
        .privileges = &.{membership},
    });
    try std.testing.expectEqual(role.principal(), try executionPrincipal(a, &txn, 1, actor));
    const acl = @import("../system_catalog/authorization_storage.zig");
    try acl.write(a, &txn, 1, .{
        .scope = .{ .table = 77 },
        .generation = 1,
        .owner = role.principal(),
        .grants = &.{.{ .grantor = role.principal(), .grantee = role.principal(), .privilege = .insert }},
    });
    try requireDestinationRights(a, &txn, 1, user, false, 77, privileges.Access.bit(.insert));
    try std.testing.expectError(error.Forbidden, requireDestinationRights(a, &txn, 1, user, true, 77, privileges.Access.bit(.insert)));
    try std.testing.expectError(error.Forbidden, requireDestinationRights(a, &txn, 1, user, false, 77, privileges.Access.bit(.delete)));
    const mixed = privileges.Access.bit(.insert) | privileges.Access.bit(.delete);
    try std.testing.expectError(error.Forbidden, requireDestinationRights(a, &txn, 1, user, false, 77, mixed));
    try acl.write(a, &txn, 1, .{
        .scope = .{ .table = 77 },
        .generation = 2,
        .owner = role.principal(),
        .grants = &.{
            .{ .grantor = role.principal(), .grantee = role.principal(), .privilege = .insert },
            .{ .grantor = role.principal(), .grantee = user, .privilege = .insert },
        },
    });
    const insert = privileges.Access.bit(.insert);
    try requireDestinationRights(a, &txn, 1, user, true, 77, insert);
    const planned_epoch = try destinationPolicyEpoch(a, &txn, 1, 77);
    try requireDestinationRightsAtEpoch(a, &txn, 1, user, true, 77, insert, planned_epoch);
    try acl.write(a, &txn, 1, .{
        .scope = .{ .table = 77 },
        .generation = 3,
        .owner = role.principal(),
        .grants = &.{.{ .grantor = role.principal(), .grantee = role.principal(), .privilege = .insert }},
    });
    try std.testing.expectError(error.Forbidden, requireDestinationRightsAtEpoch(a, &txn, 1, user, true, 77, insert, planned_epoch));
    const key = "\x00\x00__metadata__:system_catalog:1:policy-publication-stamp:77";
    var stamp: local.system_catalog_policies.PublicationStamp = .{
        .table_id = 77,
        .schema_version = 1,
        .generation = 2,
        .catalog_epoch = 2,
        .phase = .active,
    };
    try txn.put(key, try std.json.Stringify.valueAlloc(a, stamp, .{}));
    try std.testing.expectError(error.Forbidden, requireDestinationRights(a, &txn, 1, user, true, 77, insert));
    try requireDestinationRights(a, &txn, 1, user, false, 77, insert);
    try std.testing.expectError(error.RowPolicyCatalogChanged, requireDestinationRightsAtEpoch(a, &txn, 1, user, false, 77, insert, planned_epoch));
    const active_epoch = try destinationPolicyEpoch(a, &txn, 1, 77);
    try requireDestinationRightsAtEpoch(a, &txn, 1, user, false, 77, insert, active_epoch);
    stamp.phase = .pending_disable;
    try txn.put(key, try std.json.Stringify.valueAlloc(a, stamp, .{}));
    try std.testing.expectError(error.RowPolicyCatalogChanged, requireDestinationRights(a, &txn, 1, .bootstrap, true, 77, insert));
    var revoked = membership;
    revoked.generation += 1;
    revoked.grants = &.{};
    try @import("../system_catalog/authorization_storage.zig").write(a, &txn, 1, revoked);
    try std.testing.expectError(error.Forbidden, executionPrincipal(a, &txn, 1, actor));
}
