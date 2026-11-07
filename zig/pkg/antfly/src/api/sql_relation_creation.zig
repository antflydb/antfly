// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Capture one native lifetime before ordinary table creation. Metadata must
//! rejoin this same boot and bind placement in its atomic create transaction.
const std = @import("std");
const local = @import("antfly_local_sources");
const lifetimes = @import("sql_relation_lifetimes.zig");
const boots = @import("../metadata/sql_relation_boots.zig");
const directory = @import("../metadata/sql_relation_lifetimes.zig");
const domain = local.system_catalog_domain;
const A = std.mem.Allocator;
pub const Capture = struct { target: domain.Target, descriptor: ?lifetimes.Descriptor };

pub fn prepare(server: anytype, a: A, context: local.api_operation.RequestContext, target: domain.Target, kind: lifetimes.persistence.Kind, on_commit: lifetimes.persistence.OnCommit, explicitly_qualified: bool) !Capture {
    try context.ensureActive();
    if (kind == .permanent) {
        if (on_commit != .preserve_rows) return error.InvalidSqlRelationPersistence;
        return .{ .target = target, .descriptor = null };
    }
    const owner = if (kind == .temporary) context.sql_relation_owner orelse return error.SqlTemporaryRelationNotOwned else null;
    if (owner) |value| {
        try value.validate();
        try @import("sql_preparation_authority_keys.zig").requireConfiguration(server.cfg);
    }
    const bound = if (owner) |value| try lifetimes.bindTemporaryTarget(a, target, explicitly_qualified, value) else target;
    errdefer if (owner != null) a.free(bound.namespace);
    var private = context;
    private.setting_admin = true;
    // A connection's TEMP relations share one native host, preserving local
    // FK ownership. The issuer and native host remain distinct identities.
    var previous: ?lifetimes.Descriptor = null;
    if (owner) |value| {
        const bytes = try server.source.systemCatalog(a, private, .{ .sql_relation_lifetime_page = .{ .selection = .{ .connection = value }, .limit = 1 } });
        defer a.free(bytes);
        var page = try std.json.parseFromSlice(directory.Page, a, bytes, .{});
        defer page.deinit();
        if (page.value.entries.len > 1 or (page.value.entries.len == 0 and page.value.next != null)) return error.InvalidSqlBackendResponse;
        if (page.value.entries.len != 0) {
            const entry = page.value.entries[0];
            try entry.validate();
            try entry.descriptor.requireConnectionOwner(value);
            if (entry.descriptor.kind != .temporary) return error.InvalidSqlBackendResponse;
            previous = entry.descriptor;
        }
    }
    var query: boots.Query = .{
        .node = if (previous) |value| value.owner_node_id else null,
        .preferred_node = if (previous != null) null else if (owner) |value| if (value.issuer_node_id != 0) value.issuer_node_id else null else null,
    };
    while (true) {
        try context.ensureActive();
        const bytes = try server.source.systemCatalog(a, private, .{ .sql_relation_boot_read = query });
        defer a.free(bytes);
        var observed = try std.json.parseFromSlice(boots.Observation, a, bytes, .{});
        defer observed.deinit();
        if (observed.value.record == null) {
            if (query.node != null or observed.value.after == null) return error.SqlRelationOwnerUnavailable;
            const after = observed.value.after.?;
            if (query.after) |prior| if (after <= prior) return error.InvalidSqlBackendResponse;
            query.after = after;
            query.preferred_node = null;
            continue;
        }
        const boot = try observed.value.requireReady();
        if (previous) |value| {
            if (boot.node_id != value.owner_node_id or boot.identity.store_id != value.owner_store_id or
                !std.mem.eql(u8, &boot.instance, &value.generation_instance)) return error.SqlRelationOwnerChanged;
        }
        return .{ .target = bound, .descriptor = try lifetimes.descriptor(kind, on_commit, boot.node_id, boot.identity.store_id, boot.instance, 1, owner) };
    }
}
