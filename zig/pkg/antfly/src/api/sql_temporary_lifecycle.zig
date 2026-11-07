// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TEMP cleanup is an ordinary namespace retirement job. The existing source
//! outbox retains a nonce before delivery, so response loss never creates a
//! second DROP or requires storing schemas/rows in connection metadata.
const std = @import("std");
const local = @import("antfly_local_sources");
const producer = @import("sql_command_producer.zig");
const namespaces = @import("../system_catalog/temporary_namespace_index.zig");
const wire = @import("../metadata/scope_retirement_wire.zig");
const http = @import("http_server.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;

fn page(server: *http.ApiHttpServer, a: A, context: Context, owner: producer.TemporaryOwner, after: ?u64) !namespaces.NamedPage {
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .temporary_namespaces = .{ .connection = owner }, .after = after, .limit = 1 } });
    defer a.free(bytes);
    const result = try std.json.parseFromSliceLeaky(namespaces.NamedPage, a, bytes, .{ .allocate = .alloc_always });
    if (result.entries.len > 1 or (result.entries.len == 0 and result.next != null)) return error.InvalidSqlBackendResponse;
    for (result.entries) |target| {
        try target.reference.validate();
        try local.system_catalog_domain.validateName(target.database);
        if (!owner.eql(target.reference.owner) or (after != null and target.reference.namespace_id <= after.?)) return error.InvalidSqlBackendResponse;
        if (result.next) |next| if (next != target.reference.namespace_id) return error.InvalidSqlBackendResponse;
    }
    return result;
}

fn progress(prior: producer.TemporaryRetirement, job: ?producer.TemporaryJob, after: ?u64) producer.TemporaryProgress {
    var next = prior;
    next.job = job;
    next.after = after;
    return .{ .advanced = next };
}
fn nonce(server: *http.ApiHttpServer) ![16]u8 {
    const io = server.sharedApiIo() orelse return error.MissingBackendRuntimeIo;
    var result: [16]u8 = undefined;
    try io.randomSecure(&result);
    if (std.mem.allEqual(u8, &result, 0)) return error.InvalidScopeRetirement;
    return result;
}

pub fn advance(server: *http.ApiHttpServer, input: Context, backing: A, source: producer.Record) !producer.TemporaryProgress {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    try source.validate(a);
    const pending = source.temporary_retirement orelse return error.InvalidSqlConnection;
    if (source.phase != .retired and (source.phase != .finishing or source.recovery or source.control_effect != null or source.pending == null or source.pending.?.operation != .finish)) return error.SqlConnectionBusy;
    var context = try input.platformDeadline();
    context.setting_admin = true;
    context.schema_validation_authority = true;
    context.fk_generation_publication_authority = true;
    context.sql_completion = null;
    context.sql_completion_submission = null;
    context.sql_relation_owner = pending.owner;
    var fence = if (source.phase == .finishing) source.fence() else null;
    context.sql_command_fence = if (fence) |*value| value else null;

    const job = pending.job orelse {
        const selected = try page(server, a, context, pending.owner, pending.after);
        if (selected.entries.len == 0) return .complete;
        const target = selected.entries[0].reference;
        if (target.database_id != source.registration.lease.database) return error.InvalidSqlTemporaryNamespace;
        // This postimage must commit in the source writer before any reserve
        // request is sent. Even a crash immediately after submission can rejoin
        // the native outcome by nonce and exact immutable namespace identity.
        return progress(pending, .{ .nonce = try nonce(server), .database_id = target.database_id, .namespace_id = target.namespace_id }, pending.after);
    };
    const probe: wire.TemporaryProbe = .{ .owner = pending.owner, .database_id = job.database_id, .namespace_id = job.namespace_id, .nonce = job.nonce };
    const observed_bytes = try server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .temporary_retirement = probe } });
    defer a.free(observed_bytes);
    const observed = try std.json.parseFromSliceLeaky(?wire.Observation, a, observed_bytes, .{});
    if (observed) |value| {
        const outcome = value.outcome orelse return error.InvalidSqlBackendResponse;
        try value.requireFor(outcome.query);
        const query = outcome.query;
        if (query.lookup.group != source.registration.group or query.lookup.reference.kind != .namespace or query.lookup.reference.database != job.database_id or query.lookup.reference.root != job.namespace_id or
            !std.mem.eql(u8, &query.nonce, &job.nonce) or !std.mem.eql(u8, &query.lookup.reference.id, &job.nonce)) return error.InvalidSqlBackendResponse;
        if (outcome.result == .rejected) return switch (outcome.result.rejected) {
            .CatalogGenerationChanged, .MetadataIncarnationMismatch, .CatalogPublicationProofPending, .CatalogOccupancyNotReady, .TableTransitionActive, .DatabaseNotFound, .NamespaceNotFound, .ScopeRetirementChanged => progress(pending, null, pending.after),
            else => outcome.result.rejected.toError(),
        };
        const head = value.current orelse return error.InvalidSqlBackendResponse;
        try head.validate();
        const owner = head.temporary_owner orelse return error.InvalidSqlTemporaryNamespace;
        if (!owner.eql(pending.owner)) return error.SqlTemporaryRelationNotOwned;
        if (head.active()) try @import("scope_retirement_coordinator.zig").stepRetainedTemporary(server, head, context);
        return switch (head.progress.phase) {
            .published => progress(pending, null, job.namespace_id),
            .canceled => progress(pending, null, pending.after),
            .retiring_tables => blk: {
                // The metadata issuer advances the ancestor; the data owner
                // normally advances physical children on session maintenance.
                // That lane is joined during shutdown, so this same durable
                // source must continue its exact admitted child quantum.
                const scopes = @import("../metadata/scope_retirement_ledger.zig");
                const bytes = try server.source.systemCatalog(a, context, .{ .scope_retirement_work = head.sequence - 1 });
                const current = (try std.json.parseFromSliceLeaky(?scopes.Work, a, bytes, .{})) orelse break :blk .pending;
                try current.head.validate();
                if (current.head.lookup.group != head.lookup.group or current.head.sequence < head.sequence) return error.InvalidScopeRetirement;
                // Publication can remove this queue item between the exact
                // probe and feed read. Reobserve its retained outcome; a later
                // scope never grants permission to drive that other child.
                if (current.head.sequence > head.sequence) break :blk .pending;
                if (!std.meta.eql(current.head.lookup, head.lookup) or current.head.temporary_owner == null or !current.head.temporary_owner.?.eql(pending.owner)) return error.InvalidScopeRetirement;
                if (current.head.progress.phase != .retiring_tables) break :blk .pending;
                const child = current.child orelse return error.InvalidScopeRetirement;
                if (!std.meta.eql(child.lookup, head.lookup)) return error.InvalidScopeRetirement;
                try @import("table_retirement_coordinator.zig").stepScopeChild(server, child, context);
                break :blk .pending;
            },
            else => .pending,
        };
    }

    const selected = try page(server, a, context, pending.owner, job.namespace_id - 1);
    if (selected.entries.len == 0 or selected.entries[0].reference.namespace_id != job.namespace_id) return progress(pending, null, job.namespace_id);
    const target = selected.entries[0];
    if (target.reference.database_id != job.database_id) return error.InvalidSqlTemporaryNamespace;
    var buffer: [72]u8 = undefined;
    const name = try pending.owner.namespace(&buffer);
    const body = server.source.systemCatalog(a, context, .{ .scope_retirement_prepare = .{
        .kind = .namespace,
        .database = target.database,
        .name = name,
        .creator = source.actor.session_user,
        .cascade = true,
        .sql_temporary_owner = pending.owner,
    } }) catch |err| switch (err) {
        error.DatabaseNotFound, error.NamespaceNotFound, error.CatalogGenerationChanged, error.CatalogPublicationProofPending, error.TableTransitionActive => return .pending,
        else => return err,
    };
    defer a.free(body);
    const manifest = try std.json.parseFromSliceLeaky(local.system_catalog_domain.scope_retirement.Manifest, a, body, .{});
    const captured = manifest.scope.namespace orelse return error.InvalidSqlBackendResponse;
    if (manifest.metadata_group_id != source.registration.group or manifest.scope.kind != .namespace or manifest.scope.database.id != job.database_id or captured.id != job.namespace_id or
        captured.sql_temporary_owner == null or !captured.sql_temporary_owner.?.eql(pending.owner) or !manifest.creator.eql(source.actor.session_user) or !manifest.cascade or manifest.force) return error.InvalidSqlBackendResponse;
    var survivors = try manifest.prepareSurvivors(a);
    defer survivors.deinit();
    if (survivors.changed != 0) return error.CatalogPublicationProofPending;
    const request: wire.Request = .{ .admission = .{ .nonce = job.nonce, .head = manifest.catalog_revision, .command_fence = if (context.sql_command_fence) |value| value.* else null }, .operation = .{ .reserve = manifest } };
    const reply = server.source.systemCatalog(a, context, .{ .scope_retirement_submit = request }) catch |err| switch (err) {
        // A different body for this nonce may already have its exact outcome.
        // Reobserve that outcome; neither conflict nor a lost reply authorizes
        // forgetting this nonce or reserving a replacement job.
        error.SequenceAdmissionConflict, error.MetadataMutationOutcomeUnknown, error.MetadataReplicationPending, error.CatalogGenerationChanged, error.CatalogPublicationProofPending, error.OutOfMemory => return .pending,
        else => return err,
    };
    a.free(reply);
    return .pending;
}

/// One bounded recovery quantum for a public TEMP image lost at native restart.
/// This is metadata retirement, not a SQL session or an attempt to reopen the
/// old physical image. Only the canonical checkpoint permits Boot to advance.
pub fn retireLostNativeEntry(server: *http.ApiHttpServer, backing: A, current_boot: @import("../metadata/sql_relation_boots.zig").Record, entry: @import("../metadata/sql_relation_lifetimes.zig").Entry, input: Context) !void {
    const boots = @import("../metadata/sql_relation_boots.zig");
    try current_boot.validate();
    try entry.validate();
    if (entry.descriptor.kind != .temporary or entry.descriptor.owner == null or entry.descriptor.owner_node_id != current_boot.node_id or
        std.mem.eql(u8, &entry.descriptor.generation_instance, &current_boot.instance)) return error.InvalidSqlRelationPersistence;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    var context = try input.platformDeadline();
    context.setting_admin = true;
    context.schema_validation_authority = true;
    context.fk_generation_publication_authority = true;
    context.sql_completion = null;
    context.sql_completion_submission = null;
    context.sql_command_fence = null;
    const selected_bytes = try server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .lost_temporary = .{ .entry = entry, .boot = current_boot } } });
    const selected = try std.json.parseFromSliceLeaky(boots.LostTemporaryTarget, a, selected_bytes, .{});
    if (selected.retired) {
        if (selected.namespace != null or selected.held != null) return error.InvalidSqlBackendResponse;
        return;
    }
    var request: wire.Request = undefined;
    if (selected.held) |held| {
        try held.head.validate();
        if (selected.namespace != null or !held.head.active() or held.ordinal >= held.head.lookup.reference.tables) return error.InvalidSqlBackendResponse;
        request = .{ .admission = .{ .nonce = try nonce(server), .head = 0 }, .operation = .{ .source_lost = .{
            .lookup = held.head.lookup,
            .expected_revision = held.head.progress.revision,
            .ordinal = held.ordinal,
        } } };
    } else {
        const target = selected.namespace orelse return error.InvalidSqlBackendResponse;
        try target.reference.validate();
        const owner = entry.descriptor.owner.?;
        if (!owner.eql(target.reference.owner)) return error.SqlTemporaryRelationNotOwned;
        var name: [72]u8 = undefined;
        const manifest_bytes = try server.source.systemCatalog(a, context, .{ .scope_retirement_prepare = .{
            .kind = .namespace,
            .database = target.database,
            .name = try owner.namespace(&name),
            .creator = .bootstrap,
            .cascade = true,
            .sql_temporary_owner = owner,
        } });
        const manifest = try std.json.parseFromSliceLeaky(local.system_catalog_domain.scope_retirement.Manifest, a, manifest_bytes, .{});
        const captured = manifest.scope.namespace orelse return error.InvalidSqlBackendResponse;
        if (manifest.scope.kind != .namespace or captured.id != target.reference.namespace_id or manifest.scope.database.id != target.reference.database_id or
            captured.sql_temporary_owner == null or !captured.sql_temporary_owner.?.eql(owner) or manifest.creator.kind != .bootstrap or !manifest.cascade or manifest.force) return error.InvalidSqlBackendResponse;
        // Exact body and Boot identity select a retry-stable nonce. A changed
        // catalog cut may retry a rejected reservation, while an accepted one
        // is rejoined through its immutable table membership above.
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/lost-temporary-scope-admission/v1\x00");
        hash.update(&current_boot.instance);
        hash.update(manifest_bytes);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        request = .{ .admission = .{ .nonce = digest[0..16].*, .head = manifest.catalog_revision }, .operation = .{ .reserve_lost_temporary = manifest } };
    }
    const reply = try server.source.systemCatalog(a, context, .{ .scope_retirement_submit = request });
    const observed = try std.json.parseFromSliceLeaky(wire.Observation, a, reply, .{});
    if (observed.outcome) |outcome| {
        if (!std.mem.eql(u8, &outcome.query.nonce, &request.admission.nonce)) return error.InvalidSqlBackendResponse;
        try observed.requireFor(outcome.query);
        if (outcome.result == .rejected) return switch (outcome.result.rejected) {
            .ScopeRetirementChanged, .CatalogGenerationChanged, .CatalogPublicationProofPending, .TableTransitionActive => error.SqlRelationRecoveryIncomplete,
            else => outcome.result.rejected.toError(),
        };
    }
    // Even an accepted command is not a substitute for the canonical exact
    // Entry/Boot check at the start of the next recovery quantum.
    return error.SqlRelationRecoveryIncomplete;
}
