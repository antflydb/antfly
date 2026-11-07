// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Irreversible namespace retirement or an empty-generation job retires an image.
//! It cannot decide an external transaction. Decision recovery rejoins both
//! independent durable facts before acknowledging effects of that old image.
const std = @import("std");
const local = @import("antfly_local_sources");
const registry = @import("../system_catalog/prepared_transactions.zig");
const catalog = @import("table_catalog.zig");
const loss = @import("../metadata/sql_relation_recovery.zig");
const Context = local.api_operation.RequestContext;
const Route = local.metadata_catalog_route_contract.CatalogRouteFence;
const A = std.mem.Allocator;

pub fn retiredGeneration(a: A, source: catalog.CatalogSource, route: Route, context: Context) !?loss.RetiredGeneration {
    try context.ensureActive();
    try route.validate();
    if (route.route.execution != .native_local) return null;
    const pin = route.native_participant orelse return error.SqlRelationRecoveryIncomplete;
    const incarnation = route.metadata_incarnation orelse return error.CatalogGenerationChanged;
    const observed = try source.sqlRelationBinding(a, .{ .lost_generation = .{ .table_id = route.table_id, .physical_pin = pin } }, context.deadline_ns);
    const receipt = observed.lost_generation orelse return null;
    try receipt.requireCaptured(route.metadata_group_id, incarnation, route.route.group_id, pin);
    if (!receipt.beforeBinding().binding.descriptor.eql(route.route.execution.native_local)) return error.ExternalTransactionDecisionChanged;
    try context.ensureActive();
    return receipt;
}

/// Read through the existing metadata decision home. A loss receipt proves no
/// row/intent can remain in oldPin; it supplies neither COMMIT nor ABORT.
/// The caller's native route still must be a member of this exact manifest.
pub fn terminalDecision(a: A, source: anytype, context: Context, table: []const u8, route: Route, txn_id: [16]u8, owner: registry.Owner) !registry.Decision {
    return terminalDecisionMode(a, source, context, table, route, txn_id, owner, null);
}

/// Only retention ACKs may use this completed canonical cut after native GC.
/// It never supplies a missing vote, primary resolution or lock-release ACK.
pub fn requireCompletedRetention(a: A, source: anytype, context: Context, table: []const u8, route: Route, txn_id: [16]u8, owner: registry.Owner, members: []const []const u8) !void {
    _ = try terminalDecisionMode(a, source, context, table, route, txn_id, owner, members);
}

fn terminalDecisionMode(a: A, source: anytype, context: Context, table: []const u8, route: Route, txn_id: [16]u8, owner: registry.Owner, completed_members: ?[]const []const u8) !registry.Decision {
    try owner.validate();
    const expected = route.metadata_incarnation orelse return error.ExternalTransactionDecisionChanged;
    if (!std.mem.eql(u8, &expected, &owner.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
    var private = context;
    private.setting_admin = true;
    private.sql_command_fence = null;
    private.sql_completion = null;
    private.sql_completion_submission = null;
    try private.ensureActive();
    const bytes = try source.systemCatalog(a, private, .{ .sql_prepared_read = .{ .operation = owner.operation_id } });
    defer a.free(bytes);
    if (bytes.len > registry.max_bytes + 4096) return error.InvalidSqlBackendResponse;
    var parsed = try std.json.parseFromSlice(registry.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const snapshot = parsed.value;
    if (snapshot.records.len > 1 or snapshot.receipts.len > 1 or !std.mem.eql(u8, &std.fmt.bytesToHex(snapshot.incarnation, .lower), &owner.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
    for (snapshot.records) |record| {
        try record.validate(a);
        if (record.identity.session) |session| try session.requireRealm(snapshot.standalone);
        if (!(try record.identity.owner(a)).eql(owner) or !std.mem.eql(u8, &record.identity.txn_id, &txn_id)) return error.ExternalTransactionDecisionChanged;
        var member_index: ?usize = null;
        for (record.participants, 0..) |part, index| {
            if (!std.mem.eql(u8, part.table_name, table)) continue;
            if (try sameRoute(a, part.route, route)) {
                member_index = index;
                break;
            }
        }
        if (member_index == null) return error.ExternalTransactionDecisionChanged;
        if (completed_members) |members| {
            if ((record.phase != .resolved and record.phase != .retired) or record.resolutions != record.mask() or
                record.native_lock_releases != record.nativeLockMask() or record.visibility != record.visibilityMask() or
                (record.hasPreparedLocks() and !record.locks_released)) return error.CommitDecisionUnknown;
            if (members.len == 0 or members.len > record.participants.len) return error.InvalidParticipant;
            // Only the coordinator owns the global roster. Every native peer
            // retains its local singleton, even when a foreign ID is enlisted
            // elsewhere in this exact completed metadata transaction.
            if (member_index.? != 0) {
                if (members.len != 1) return error.InvalidParticipant;
                const local_id = try local.api_local_transaction_contract.participantIdForGroupScoped(a, table, route.route.group_id, null, null);
                defer a.free(local_id);
                if (!std.mem.eql(u8, members[0], local_id)) return error.InvalidParticipant;
            }
            for (members, 0..) |id, i| {
                for (members[0..i]) |previous| if (std.mem.eql(u8, id, previous)) return error.InvalidParticipant;
                var enlisted = false;
                for (record.participants) |part| {
                    const actual = try local.api_local_transaction_contract.participantIdForGroupScoped(a, part.table_name, part.route.route.group_id, null, null);
                    defer a.free(actual);
                    if (std.mem.eql(u8, id, actual)) {
                        enlisted = true;
                        break;
                    }
                }
                if (!enlisted) return error.InvalidParticipant;
            }
        }
        return record.decision orelse error.CommitDecisionUnknown;
    }
    // A receipt omits the manifest. Completion recovery requires its exact
    // retained member set, not a free-standing owner for an unrelated route.
    if (completed_members != null) return error.CommitDecisionUnknown;
    // A finalized receipt exists only after the complete participant ACK
    // handoff. Repeating an ACK has no native effect; the exact owner/txn and
    // terminal decision remain available without reopening the retired image.
    const receipt_bytes = try source.systemCatalog(a, private, .{ .sql_prepared_read = .{ .receipt = owner.operation_id } });
    defer a.free(receipt_bytes);
    if (receipt_bytes.len > registry.max_bytes + 4096) return error.InvalidSqlBackendResponse;
    var final = try std.json.parseFromSlice(registry.Snapshot, a, receipt_bytes, .{ .allocate = .alloc_always });
    defer final.deinit();
    if (final.value.records.len != 0 or final.value.receipts.len > 1 or !std.mem.eql(u8, &std.fmt.bytesToHex(final.value.incarnation, .lower), &owner.metadata_incarnation)) return error.ExternalTransactionDecisionChanged;
    for (final.value.receipts) |receipt| {
        try receipt.validate();
        try receipt.session.requireRealm(final.value.standalone);
        if (!receipt.owner.eql(owner) or !std.mem.eql(u8, &receipt.txn_id, &txn_id)) return error.ExternalTransactionDecisionChanged;
        return receipt.decision orelse error.CommitDecisionUnknown;
    }
    return error.CommitDecisionUnknown;
}

fn sameRoute(a: A, left: Route, right: Route) !bool {
    const before = try std.json.Stringify.valueAlloc(a, left, .{});
    defer a.free(before);
    const after = try std.json.Stringify.valueAlloc(a, right, .{});
    defer a.free(after);
    return std.mem.eql(u8, before, after);
}

test "SQL relation lifetime loss retirement rejoins exact terminal owner manifest and refuses undecided work" {
    const a = std.testing.allocator;
    const incarnation: [16]u8 = @splat(1);
    const epoch = std.fmt.bytesToHex(incarnation, .lower);
    const route: Route = .{ .metadata_group_id = 1, .metadata_incarnation = epoch, .catalog_revision = 2, .table_id = 3, .topology_epoch = 1, .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 3, .shard_id = 7, .range_id = 8 }, .execution = .{ .native_local = .{ .kind = .unlogged, .owner_node_id = 5, .owner_store_id = 6, .generation_instance = @splat(12), .data_generation = 1 } } }, .native_participant = .{ .physical_root_incarnation = 13, .table_id = 3, .shard_id = 7, .range_id = 8, .data_generation = 1 } };
    const manifest = [_]registry.Participant{.{ .table_name = "table:native", .route = route }};
    const identity: registry.Identity = .{ .gid = "native-recovery", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(2), .operation_id = @splat(3), .metadata_incarnation = epoch, .begin_timestamp = 10, .expires_at_unix_ns = 1000, .plan_digest = @splat(4), .manifest_digest = try registry.manifestDigest(a, &manifest, epoch) };
    const Source = struct {
        record: registry.Record,
        pub fn systemCatalog(self: *@This(), allocator: A, _: Context, _: anytype) ![]u8 {
            return std.json.Stringify.valueAlloc(allocator, registry.Snapshot{ .incarnation = incarnation, .head = 1, .active = 1, .records = &.{self.record} }, .{});
        }
    };
    var source: Source = .{ .record = .{ .identity = identity, .participants = &manifest, .phase = .decided, .decision = .{ .status = .aborted, .version = 20 } } };
    const owner = try identity.owner(a);
    const result = try terminalDecision(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, owner);
    try std.testing.expectEqual(registry.Decision{ .status = .aborted, .version = 20 }, result);
    var wrong = owner;
    wrong.admission_digest[0] ^= 1;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, terminalDecision(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, wrong));
    var replaced = route;
    replaced.native_participant.?.physical_root_incarnation += 1;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, terminalDecision(a, &source, .{}, manifest[0].table_name, replaced, identity.txn_id, owner));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, terminalDecision(a, &source, .{}, "other-table", route, identity.txn_id, owner));
    const member = try local.api_local_transaction_contract.participantIdForGroupScoped(a, manifest[0].table_name, route.route.group_id, null, null);
    defer a.free(member);
    // A decision alone cannot release terminal retention after native GC.
    try std.testing.expectError(error.CommitDecisionUnknown, requireCompletedRetention(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, owner, &.{member}));
    source.record.phase = .resolved;
    source.record.resolutions = source.record.mask();
    try requireCompletedRetention(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, owner, &.{member});
    try std.testing.expectError(error.InvalidParticipant, requireCompletedRetention(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, owner, &.{"foreign"}));
    try std.testing.expectError(error.InvalidParticipant, requireCompletedRetention(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, owner, &.{}));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, requireCompletedRetention(a, &source, .{}, manifest[0].table_name, replaced, identity.txn_id, owner, &.{member}));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, requireCompletedRetention(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, wrong, &.{member}));
    source.record.resolutions = 0;
    source.record.phase = .preparing;
    source.record.decision = null;
    try std.testing.expectError(error.CommitDecisionUnknown, terminalDecision(a, &source, .{}, manifest[0].table_name, route, identity.txn_id, owner));
}
