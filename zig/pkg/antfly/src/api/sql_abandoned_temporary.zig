// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Metadata-leader backstop for ended API owners whose local outbox was lost.
//! Existing scope jobs still own every actual table/participant retirement.
const std = @import("std");
const local = @import("antfly_local_sources");
const boots = @import("../metadata/sql_relation_boots.zig");
const wire = @import("../metadata/scope_retirement_wire.zig");
const Server = @import("http_server.zig").ApiHttpServer;

pub fn step(server: *Server, input: local.api_operation.RequestContext) !void {
    const group = server.cfg.prepared_transaction_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return;
    } else if (server.cfg.deployment_mode != .standalone) return;
    const now = @import("antfly_platform").time.monotonicNs();
    const previous = server.temporary_cleanup_poll_after_ns.load(.acquire);
    if (previous > now or server.temporary_cleanup_poll_after_ns.cmpxchgStrong(previous, now +| 100 * std.time.ns_per_ms, .acq_rel, .acquire) != null) return;
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const after = server.temporary_cleanup_cursor.load(.monotonic);
    var context = try input.platformDeadline();
    context.setting_admin = true;
    context.schema_validation_authority = true;
    context.fk_generation_publication_authority = true;
    const bytes = server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .temporary_abandonment = .{ .after = if (after == 0) null else after } } }) catch |err| return switch (err) {
        // The backstop is not activated by a request-local flag. Older active
        // memberships keep their ordinary retirement/session behavior.
        error.MixedMetadataVersions, error.SqlRelationCapabilityUnavailable, error.TableTopologyProtocolUpgradeRequired => {},
        else => err,
    };
    const work = try std.json.parseFromSliceLeaky(boots.AbandonedTemporaryPage, a, bytes, .{});
    if (work.next) |next| if (next <= after) return error.InvalidSqlBackendResponse;
    defer server.temporary_cleanup_cursor.store(work.next orelse 0, .monotonic);
    const selected = work.target orelse return;
    const reference = selected.namespace.reference;
    try reference.validate();
    try selected.guard.validate();
    if (work.next == null or work.next.? != reference.namespace_id or !std.meta.eql(selected.guard.namespace, reference)) return error.InvalidSqlBackendResponse;
    var name: [72]u8 = undefined;
    const prepared_bytes = server.source.systemCatalog(a, context, .{ .scope_retirement_prepare = .{
        .kind = .namespace,
        .database = selected.namespace.database,
        .name = try reference.owner.namespace(&name),
        .creator = .bootstrap,
        .cascade = true,
        .sql_temporary_owner = reference.owner,
    } }) catch |err| return switch (err) {
        error.NamespaceNotFound, error.DatabaseNotFound, error.CatalogGenerationChanged, error.CatalogPublicationProofPending, error.ScopeRetirementChanged, error.TableTransitionActive => {},
        else => err,
    };
    const manifest = try std.json.parseFromSliceLeaky(local.system_catalog_domain.scope_retirement.Manifest, a, prepared_bytes, .{});
    const namespace = manifest.scope.namespace orelse return error.InvalidScopeRetirement;
    if (manifest.metadata_group_id != group or manifest.scope.kind != .namespace or manifest.scope.database.id != reference.database_id or
        namespace.id != reference.namespace_id or namespace.sql_temporary_owner == null or !namespace.sql_temporary_owner.?.eql(reference.owner) or
        manifest.creator.kind != .bootstrap or !manifest.cascade or manifest.force) return error.InvalidScopeRetirement;
    // Exact source and manifest cuts select one retry-stable admission. Once
    // accepted, namespace membership makes later scans rejoin the existing job.
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/abandoned-temporary-admission/v1\x00");
    hash.update(&selected.guard.digest);
    hash.update(prepared_bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const request: wire.Request = .{ .admission = .{ .nonce = digest[0..16].*, .head = manifest.catalog_revision }, .operation = .{ .reserve = manifest }, .temporary_abandonment = selected.guard };
    const reply = server.source.systemCatalog(a, context, .{ .scope_retirement_submit = request }) catch |err| return switch (err) {
        // No mutation is blindly replayed. The next metadata scan rejoins exact
        // scope membership after either a lost reply or catalog/source race.
        error.MetadataMutationOutcomeUnknown, error.MetadataReplicationPending, error.SequenceAdmissionConflict, error.CatalogGenerationChanged, error.CatalogPublicationProofPending, error.ScopeRetirementChanged => {},
        else => err,
    };
    const observed = try std.json.parseFromSliceLeaky(wire.Observation, a, reply, .{});
    const outcome = observed.outcome orelse return error.InvalidScopeRetirement;
    if (!std.mem.eql(u8, &outcome.query.nonce, &request.admission.nonce)) return error.InvalidScopeRetirement;
    try observed.requireFor(outcome.query);
    if (outcome.result == .rejected) return switch (outcome.result.rejected) {
        .CatalogGenerationChanged, .CatalogPublicationProofPending, .CatalogOccupancyNotReady, .ScopeRetirementChanged, .TableTransitionActive, .NamespaceNotFound, .DatabaseNotFound => {},
        else => outcome.result.rejected.toError(),
    };
}
