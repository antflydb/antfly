// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Recovery authority is a canonical irreversible metadata decision, never a
//! fabricated SQL principal. Ordinary empty-generation jobs retain their
//! original credential checks.
const std = @import("std");
const staging = @import("../metadata/restore_staging.zig");
const boots = @import("../metadata/sql_relation_boots.zig");
const operation = @import("antfly_local_sources").api_operation;

pub fn requiresNativeRecovery(plan: staging.Plan) bool {
    return for (plan.targets) |target| {
        if (target.native_reset != null) break true;
    } else false;
}

/// Re-read the immutable native job and every source-loss checkpoint. A local
/// worker JSON or marker string cannot grant authority after reservation failed
/// or when a caller substituted a different healthy target/closure.
pub fn requireAdmitted(server: anytype, a: std.mem.Allocator, plan: staging.Plan, context: operation.RequestContext) !void {
    if (!requiresNativeRecovery(plan)) return error.InvalidSqlRelationPersistence;
    try plan.validate(a);
    const digest = try plan.digest(a);
    const encoded = (try server.source.getRestoreStaging(a, plan.id, context)) orelse return error.SqlRelationRecoveryIncomplete;
    defer a.free(encoded);
    var stored = try std.json.parseFromSlice(staging.Job, a, encoded, .{});
    defer stored.deinit();
    if (!std.mem.eql(u8, &stored.value.plan_digest, &digest) or !std.mem.eql(u8, &try stored.value.plan.digest(a), &digest)) return error.RestoreStagingScopeChanged;
    var internal = context;
    internal.setting_admin = true;
    for (plan.targets) |target| if (target.native_reset) |guard| {
        const pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(guard.before.binding);
        const bytes = try server.source.systemCatalog(a, internal, .{ .sql_relation_boot_read = .{ .lost_generation = .{ .table_id = pin.table_id, .physical_pin = pin } } });
        defer a.free(bytes);
        var observed = try std.json.parseFromSlice(boots.Observation, a, bytes, .{});
        defer observed.deinit();
        const actual = observed.value.lost_generation orelse return error.SqlRelationRecoveryIncomplete;
        try actual.requireCaptured(guard.before.metadata_group_id, guard.before.identity.metadata_incarnation, guard.source_group_id, pin);
        // Both an in-progress loss fence and a terminal publication bind the
        // exact same admitted job; neither relies on the successor PID still
        // being alive when a deferred worker retries.
        const address: struct { id: [16]u8, digest: [32]u8 } = switch (actual) {
            .unlogged_loss_fenced => |loss| .{ .id = loss.plan_id, .digest = loss.plan_digest },
            .unlogged_reset => |loss| .{ .id = loss.plan_id, .digest = loss.plan_digest },
            .temporary_scope => return error.InvalidSqlRelationPersistence,
        };
        if (!std.mem.eql(u8, &address.id, &plan.id) or !std.mem.eql(u8, &address.digest, &digest)) return error.RestoreStagingScopeChanged;
    };
}
