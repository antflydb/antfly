// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Session namespace publication uses the ordinary immutable catalog journal.
//! No user-selected namespace or external token grants TEMP ownership.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const lifetimes = @import("sql_relation_lifetimes.zig");
const ordinary = @import("sql_ordinary_catalog.zig");
const http = @import("http_server.zig");
const catalog = local.sql_catalog;

fn exists(server: *http.ApiHttpServer, context: local.api_operation.RequestContext, a: std.mem.Allocator, database: []const u8, name: []const u8, owner: lifetimes.Owner) !bool {
    const bytes = server.source.systemCatalog(a, context, .{ .read = .{ .kind = .namespace, .database = database, .name = name } }) catch |err| switch (err) {
        // A selected catalog read reports absence as an error. The first
        // TEMP declaration must then publish its private namespace through
        // the existing journal; missing databases and authority errors remain
        // failures rather than being interpreted as an empty namespace.
        error.CatalogNotFound, error.NotFound => return false,
        else => return err,
    };
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.State, a, bytes, .{});
    defer parsed.deinit();
    var found = false;
    for (parsed.value.resources) |resource| if (resource.kind == .namespace and std.mem.eql(u8, resource.name, name)) {
        if (found) return error.InvalidSqlBackendResponse;
        const captured = resource.sql_temporary_owner orelse return error.SqlTemporaryRelationNotOwned;
        if (!captured.eql(owner)) return error.SqlTemporaryRelationNotOwned;
        try @import("sql_preparation_authority_keys.zig").requireConfiguration(server.cfg);
        found = true;
    };
    return found;
}

pub fn ensure(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: local.api_operation.RequestContext, allocators: catalog.DdlAllocators, database: []const u8, owner: lifetimes.Owner) !void {
    if (context.sql_command_fence == null) return error.SqlConnectionBusy;
    const captured = context.sql_relation_owner orelse return error.SqlTemporaryRelationNotOwned;
    if (!captured.eql(owner)) return error.SqlTemporaryRelationNotOwned;
    const store = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
    try @import("sql_command_producer.zig").markTemporaryOwner(store, owner, context.sql_command_fence.?.*, database, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    var buffer: [72]u8 = undefined;
    const name = try owner.namespace(&buffer);
    // This operation is independently idempotent and row-free. A failed table
    // declaration may retain its private empty session namespace.
    const request: domain.Request = .{ .mutation = .{ .kind = .namespace, .action = .create, .database = database, .name = name, .sql_temporary_owner = owner } };
    var private = context;
    private.setting_admin = true;
    if (try exists(server, private, allocators.scratch, database, name, owner)) return;
    var result = ordinary.submit(server, identity, private, allocators, request) catch |err| {
        if (err == error.CatalogAlreadyExists and try exists(server, private, allocators.scratch, database, name, owner)) return;
        return err;
    };
    defer result.deinit(allocators.result);
    const receipt = result.receipt orelse return error.MetadataMutationOutcomeUnknown;
    if (receipt.state != .committed or result.unknown) return error.MetadataMutationOutcomeUnknown;
}
