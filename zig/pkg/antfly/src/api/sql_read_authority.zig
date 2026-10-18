// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Request-owned read authority, separate from SQL session identity. Stored
//! views attach this only after catalog/owner admission. No wire decoder or
//! prepared-plan cache may construct or retain a Context.
const std = @import("std");
const http = @import("http_server.zig");
const catalog = @import("antfly_local_sources").sql_catalog;
const domain = @import("antfly_local_sources").system_catalog_domain;
const manager = @import("../usermgr/mod.zig").UserManager;

pub const Grant = struct {
    identity: http.AuthenticatedIdentity,
    instance: [16]u8,
    logical_view: []const u8,
};
pub const Context = struct {
    effective: http.AuthenticatedIdentity,
    instance: [16]u8,
    grants: []const Grant,
    delegation: ?manager.ViewDelegation,
};

/// Clone only the authorization capsule into a cursor-owned bounded arena.
/// Never retain a binder's arena after a stream has detached its operators.
pub fn clone(a: std.mem.Allocator, value: *const Context) !*const Context {
    const owned = try a.create(Context);
    const grants = try a.alloc(Grant, value.grants.len);
    for (value.grants, grants) |grant, *out| out.* = .{
        .identity = try cloneIdentity(a, grant.identity),
        .instance = grant.instance,
        .logical_view = try a.dupe(u8, grant.logical_view),
    };
    owned.* = .{ .effective = try cloneIdentity(a, value.effective), .instance = value.instance, .grants = grants, .delegation = null };
    return owned;
}

fn cloneIdentity(a: std.mem.Allocator, value: http.AuthenticatedIdentity) !http.AuthenticatedIdentity {
    var result = (try http.cloneCatalogIdentity(a, value)).?;
    result.user_instance_id = value.user_instance_id;
    return result;
}

fn refreshed(a: std.mem.Allocator, value: http.AuthenticatedIdentity) !http.AuthenticatedIdentity {
    const live = value.live_user_manager orelse return error.Forbidden;
    var user = try live.getUser(value.username);
    defer user.deinit(live.alloc);
    if (value.user_instance_id) |expected| if (!std.mem.eql(u8, &expected, &user.instance_id)) return error.Forbidden;
    const filters = try live.durableCredentialRowFilters(value.credential_principal);
    defer http.freeRowFilters(live.alloc, filters);
    const roles = try live.getRolesForUser(value.username);
    defer {
        for (roles) |role| live.alloc.free(role);
        live.alloc.free(roles);
    }
    var result = try http.cloneAuthenticatedIdentity(a, value.username, value.credential_principal, value.permissions, filters, user.metadata_json, roles);
    result.live_user_manager = live;
    result.user_instance_id = user.instance_id;
    return result;
}

fn sameFilter(left: ?[]const u8, right: ?[]const u8) bool {
    return if (left) |bytes| if (right) |other| std.mem.eql(u8, bytes, other) else false else right == null;
}

/// Buffered/detached pages must recheck grants and output-resource filters,
/// even for constant views with no physical scan and no cursor.next callback.
pub fn validateGrants(a: std.mem.Allocator, value: *const Context) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    try checkInstance(value.effective, value.instance);
    if (value.grants.len == 0 or value.grants.len > 64) return error.Forbidden;
    for (value.grants) |grant| {
        try checkInstance(grant.identity, grant.instance);
        if (!try http.tablePermissionCurrentlyAllowed(grant.identity, grant.logical_view, .read)) return error.Forbidden;
        const fresh = try refreshed(scratch, grant.identity);
        if (!sameFilter(try http.resolveEffectiveRowFilterJson(scratch, grant.identity, grant.logical_view), try http.resolveEffectiveRowFilterJson(scratch, fresh, grant.logical_view))) return error.Forbidden;
    }
}

pub fn validateFilters(a: std.mem.Allocator, table: catalog.Table, caller: http.AuthenticatedIdentity, admitted_filter: ?[]const u8) !void {
    try check(table);
    const value = context(table) orelse return error.Forbidden;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var fresh_context = value.*;
    fresh_context.effective = try refreshed(scratch, value.effective);
    var fresh_table = table;
    fresh_table.read_authority = &fresh_context;
    if (!sameFilter(admitted_filter, try filter(scratch, fresh_table, caller))) return error.Forbidden;
}

pub fn context(table: catalog.Table) ?*const Context {
    return if (table.read_authority) |raw| @ptrCast(@alignCast(raw)) else null;
}

pub const ProofKey = struct {
    table_id: u64,
    schema_version: u32,
    revision: u64,
    authority: ?*const anyopaque,

    pub fn from(table: catalog.Table) !ProofKey {
        return .{
            .table_id = table.id,
            .schema_version = table.schema_version,
            .revision = (table.scope orelse return error.InvalidSqlBackendResponse).revision,
            .authority = table.read_authority,
        };
    }
};

fn checkInstance(identity: http.AuthenticatedIdentity, instance: [16]u8) !void {
    const live = identity.live_user_manager orelse return error.Forbidden;
    var user = live.getUser(identity.username) catch |err| switch (err) {
        error.UserNotFound => return error.Forbidden,
        else => return err,
    };
    defer user.deinit(live.alloc);
    if (!std.mem.eql(u8, &user.instance_id, &instance)) return error.Forbidden;
}

pub fn check(table: catalog.Table) !void {
    const authority = context(table) orelse return;
    if (authority.grants.len == 0 or authority.grants.len > 64) return error.Forbidden;
    for (authority.grants) |grant| {
        try checkInstance(grant.identity, grant.instance);
        if (!try http.tablePermissionCurrentlyAllowed(grant.identity, grant.logical_view, .read)) return error.Forbidden;
    }
    try checkInstance(authority.effective, authority.instance);
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    var arena = std.heap.ArenaAllocator.init(authority.effective.live_user_manager.?.alloc);
    defer arena.deinit();
    const logical = try (domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(arena.allocator());
    if (!try http.tablePermissionCurrentlyAllowed(authority.effective, logical, .read)) return error.Forbidden;
}

/// Legacy Antfly row filters are additional restrictions, not PostgreSQL RLS
/// role selection. Owner delegation must not discard the caller's tenant/API
/// key filter. Resolve each $auth reference against its own original identity.
pub fn filter(a: std.mem.Allocator, table: catalog.Table, caller: ?http.AuthenticatedIdentity) !?[]u8 {
    const authority = context(table) orelse return http.resolveEffectiveRowFilterJson(a, caller, table.physical_name);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const temporary = scratch.allocator();
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    const logical = try (domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(temporary);
    // Logical resource matching is essential: the caller need not have a
    // direct base-table grant/alias when reading through an owner view.
    const left = try http.resolveEffectiveRowFilterJson(temporary, caller, logical);
    const right = try http.resolveEffectiveRowFilterJson(temporary, authority.effective, logical);
    if (left == null) return if (right) |bytes| try a.dupe(u8, bytes) else null;
    if (right == null) return try a.dupe(u8, left.?);
    if (std.mem.eql(u8, left.?, right.?)) return try a.dupe(u8, left.?);
    const first = try std.json.parseFromSliceLeaky(std.json.Value, temporary, left.?, .{});
    const second = try std.json.parseFromSliceLeaky(std.json.Value, temporary, right.?, .{});
    return try std.json.Stringify.valueAlloc(a, .{ .conjuncts = [_]std.json.Value{ first, second } }, .{});
}

test "SQL read policy proof cache separates source epochs and view authorities" {
    const a = std.testing.allocator;
    var first: u8 = 1;
    var second: u8 = 2;
    var table: catalog.Table = .{ .id = 7, .schema_version = 1, .physical_name = "rows", .columns = &.{}, .scope = .{ .database = "default", .namespace = "public", .name = "rows", .revision = 3 } };
    var entries: std.AutoHashMapUnmanaged(ProofKey, void) = .empty;
    defer entries.deinit(a);
    try entries.put(a, try ProofKey.from(table), {});
    table.read_authority = &first;
    try entries.put(a, try ProofKey.from(table), {});
    table.read_authority = &second;
    try entries.put(a, try ProofKey.from(table), {});
    table.schema_version = 2;
    try entries.put(a, try ProofKey.from(table), {});
    table.scope.?.revision = 4;
    try entries.put(a, try ProofKey.from(table), {});
    try std.testing.expectEqual(@as(usize, 5), entries.count());
}

test "SQL owner view legacy filters intersect owner and caller with original auth identities" {
    const a = std.testing.allocator;
    const usermgr = @import("../usermgr/mod.zig");
    var caller_filters = [_]usermgr.RowFilterEntry{.{ .table = @constCast("*"), .filter = @constCast("{\"term\":{\"tenant\":{\"$auth\":\"username\"}}}") }};
    var owner_filters = [_]usermgr.RowFilterEntry{.{ .table = @constCast("*"), .filter = @constCast("{\"term\":{\"owner\":{\"$auth\":\"username\"}}}") }};
    const caller: http.AuthenticatedIdentity = .{ .username = @constCast("caller"), .row_filter = &caller_filters };
    const owner: http.AuthenticatedIdentity = .{ .username = @constCast("owner"), .row_filter = &owner_filters };
    const authority: Context = .{ .effective = owner, .instance = @splat(1), .grants = &.{}, .delegation = null };
    const table: catalog.Table = .{
        .id = 7,
        .physical_name = "table:7",
        .schema_version = 1,
        .columns = &.{},
        .read_authority = &authority,
        .scope = .{ .database = "default", .namespace = "public", .name = "rows", .revision = 1 },
    };
    const bytes = (try filter(a, table, caller)).?;
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const conjuncts = parsed.value.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), conjuncts.len);
    try std.testing.expectEqualStrings("caller", conjuncts[0].object.get("term").?.object.get("tenant").?.string);
    try std.testing.expectEqualStrings("owner", conjuncts[1].object.get("term").?.object.get("owner").?.string);
    try std.testing.expectError(error.Forbidden, check(table));
}
