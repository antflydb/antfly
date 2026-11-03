// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compile an authorization matrix once and submit one atomic catalog decision.
const std = @import("std");
const local = @import("antfly_local_sources");
const auth = @import("../system_catalog/authorization.zig");
const graph = auth.records;
const domain = local.system_catalog_domain;
const ast = local.sql_ast;
const catalog = local.sql_catalog;
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const recovery = @import("sql_authorization_recovery.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;

fn read(root: *execution.Adapter, a: A, lookup: auth.Read) !auth.Snapshot {
    try root.context.ensureActive();
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_authorization_read = lookup });
    const cut = try std.json.parseFromSliceLeaky(auth.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(a, lookup);
    return cut;
}
fn userPrincipal(root: *execution.Adapter, name: []const u8) !?graph.Principal {
    const manager = root.server.cfg.user_manager orelse return null;
    const instance = manager.currentUserInstance(name) catch return null;
    return .{ .kind = .user, .instance = instance };
}
fn resolvePrincipal(root: *execution.Adapter, names: []const []const u8, roles: []const ?auth.Role, name: []const u8) !graph.Principal {
    for (names, roles) |candidate, role| if (std.mem.eql(u8, candidate, name)) {
        if (role) |value| return value.principal();
        return (try userPrincipal(root, name)) orelse error.SqlRoleNotFound;
    };
    return error.InvalidSqlBackendResponse;
}
fn nativeRoleAdmin(root: *execution.Adapter, name: []const u8) !bool {
    return http.resourcePermissionCurrentlyAllowed(root.identity.*, .user, name, .admin);
}
fn credentialAdministration(root: *execution.Adapter, resource_type: @import("../usermgr/mod.zig").ResourceType, resource: []const u8) !void {
    if (!root.server.cfg.auth_enabled) return;
    const identity = root.identity.* orelse return error.Forbidden;
    const manager = identity.live_user_manager orelse return error.Forbidden;
    const instance = identity.user_instance_id orelse return error.Forbidden;
    if (!try manager.sqlCredentialCatalogPermits(identity.username, instance, identity.credential_principal, resource_type, resource)) return error.Forbidden;
}
fn appendScope(a: A, scopes: *std.ArrayList(auth.Scope), administrators: *std.ArrayList(bool), scope: auth.Scope, administrator: bool) !void {
    for (scopes.items) |prior| if (scope.eql(prior)) return;
    if (scopes.items.len >= auth.max_targets) return error.SqlProgramLimitExceeded;
    try scopes.append(a, scope);
    try administrators.append(a, administrator);
}
fn target(root: *execution.Adapter, name: ast.Name) domain.Target {
    return .{ .database = name.database orelse root.database, .namespace = name.namespace orelse root.namespace, .table = name.table };
}

const BoundTable = struct { logical: domain.Target, table: domain.ResolvedTable };
fn resolveTables(root: *execution.Adapter, a: A, names: []const ast.Name, revision: u64) ![]const BoundTable {
    var targets: std.ArrayList(domain.Target) = .empty;
    const starts = try a.alloc(usize, names.len + 1);
    for (names, 0..) |name, i| {
        starts[i] = targets.items.len;
        const path = root.ddl_search_path;
        if (name.namespace == null and name.database == null and path != null) {
            if (path.?.len == 0) return error.TableNotFound;
            for (path.?.entries[0..path.?.len]) |*namespace| {
                if (targets.items.len >= auth.max_targets) return error.SqlProgramLimitExceeded;
                var logical = target(root, name);
                logical.namespace = namespace.slice();
                try targets.append(a, logical);
            }
        } else {
            if (targets.items.len >= auth.max_targets) return error.SqlProgramLimitExceeded;
            try targets.append(a, target(root, name));
        }
    }
    starts[names.len] = targets.items.len;
    const lookup: domain.ResolveMany = .{ .targets = targets.items, .expected_revision = revision };
    const bytes = try root.server.source.systemCatalog(a, root.context, .{ .resolve_many = lookup });
    const cut = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    try cut.validateRelations(lookup);
    if (cut.revision != revision or cut.tables.len != targets.items.len) return error.InvalidSqlBackendResponse;
    const result = try a.alloc(BoundTable, names.len);
    for (result, 0..) |*out, i| {
        out.* = for (cut.tables[starts[i]..starts[i + 1]], targets.items[starts[i]..starts[i + 1]]) |found, logical| {
            if (found) |table| break .{ .logical = logical, .table = table };
        } else return error.TableNotFound;
    }
    return result;
}

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    const intent = ddl.authorization orelse return error.InvalidSqlSyntax;
    if (!root.server.source.vtable.supports_sql_authorization or root.active_transaction != null) return error.UnsupportedSqlExecution;
    try root.context.ensureActive();
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const actor = try recovery.actor(root.server, root.identity.*);
    var names: std.ArrayList([]const u8) = .empty;
    var names_seen: std.StringHashMapUnmanaged(void) = .empty;
    for (intent.roles) |name| if (!(try names_seen.getOrPut(a, name)).found_existing) try names.append(a, name);
    for (intent.default_owners) |name| if (!(try names_seen.getOrPut(a, name)).found_existing) try names.append(a, name);
    for (intent.grantees) |grantee| if (grantee == .role and !(try names_seen.getOrPut(a, grantee.role)).found_existing) try names.append(a, grantee.role);
    const namespaces = try a.alloc(auth.Read.Namespace, intent.schemas.len);
    for (intent.schemas, namespaces) |schema, *out| {
        if (schema.database != null) return error.InvalidSqlSyntax;
        out.* = .{ .database = schema.namespace orelse root.database, .name = schema.table };
    }
    // Names and membership may exceed one bounded point-read envelope. Every
    // part still resolves at the same revision; no names are resolved at apply.
    const roles = try a.alloc(?auth.Role, names.items.len);
    const first = try read(root, a, .{ .namespaces = namespaces, .role_names = names.items[0..@min(names.items.len, auth.max_targets)], .principal = actor.session_user });
    @memcpy(roles[0..first.roles.len], first.roles);
    var position = first.roles.len;
    while (position < names.items.len) {
        const end = @min(names.items.len, position + auth.max_targets);
        const part = try read(root, a, .{ .role_names = names.items[position..end], .principal = actor.session_user, .expected_revision = first.revision });
        @memcpy(roles[position..end], part.roles);
        position = end;
    }
    var request: auth.Mutation = .{ .admission = .{ .nonce = undefined, .head = first.head, .command_fence = if (root.context.sql_command_fence) |fence| fence.* else null }, .incarnation = first.incarnation, .actor = actor, .label = switch (intent.operation) {
        .create_role, .drop_role => ddl.name.table,
        .grant => "grant",
        .revoke => "revoke",
    }, .operation = undefined };
    try root.server.sqlPlanCacheIo().randomSecure(&request.admission.nonce);
    switch (intent.operation) {
        .create_role => {
            if (intent.roles.len != 1) return error.UnsupportedSqlShape;
            request.native_role_admin = try nativeRoleAdmin(root, ddl.name.table);
            if (try userPrincipal(root, ddl.name.table) != null) return error.SqlDuplicateRole;
            request.operation = .{ .create_role = .{ .name = ddl.name.table } };
        },
        .drop_role => {
            if (intent.roles.len != 1) return error.UnsupportedSqlShape;
            request.native_role_admin = try nativeRoleAdmin(root, ddl.name.table);
            request.operation = .{ .drop_role = .{ .name = ddl.name.table, .expected = roles[0], .if_exists = ddl.conditional } };
            if (roles[0] == null and !ddl.conditional) return error.SqlRoleNotFound;
        },
        .grant, .revoke => {
            const grantees = try a.alloc(graph.Principal, intent.grantees.len);
            var recipient_count: usize = 0;
            for (intent.grantees) |grantee| {
                const principal_value: graph.Principal = switch (grantee) {
                    .public => .public,
                    .role => |name| try resolvePrincipal(root, names.items, roles, name),
                };
                const duplicate = for (grantees[0..recipient_count]) |prior| {
                    if (prior.eql(principal_value)) break true;
                } else false;
                if (!duplicate) {
                    grantees[recipient_count] = principal_value;
                    recipient_count += 1;
                }
            }
            var scopes: std.ArrayList(auth.Scope) = .empty;
            var administrators: std.ArrayList(bool) = .empty;
            switch (intent.scope) {
                .roles => for (intent.roles) |name| {
                    const found = for (names.items, roles) |candidate, role| {
                        if (std.mem.eql(u8, candidate, name)) break role orelse return error.SqlRoleNotFound;
                    } else return error.SqlRoleNotFound;
                    try credentialAdministration(root, .user, name);
                    try appendScope(a, &scopes, &administrators, .{ .role = found.id }, try nativeRoleAdmin(root, name));
                },
                .tables => {
                    const tables = try resolveTables(root, a, intent.tables, first.revision);
                    for (tables) |bound| {
                        const table = bound.table;
                        try table.requireAccess(root.context.native_service);
                        const resource = try bound.logical.resourceNameAlloc(a);
                        try credentialAdministration(root, .table, resource);
                        try appendScope(a, &scopes, &administrators, .{ .table = table.table_id }, try http.tablePermissionCurrentlyAllowed(root.identity.*, resource, .admin));
                    }
                },
                .all_tables => for (namespaces) |namespace| {
                    var listing_request: domain.TableList = .{ .database = namespace.database, .namespace = namespace.name, .limit = 128, .revision = first.revision };
                    while (true) {
                        try root.context.ensureActive();
                        const bytes = try root.server.source.systemCatalog(a, root.context, .{ .list_tables = listing_request });
                        const listing = try std.json.parseFromSliceLeaky(@import("../system_catalog/projection.zig").TableListing, a, bytes, .{ .allocate = .alloc_always });
                        if (listing.revision != first.revision) return error.CatalogGenerationChanged;
                        const page_targets = try a.alloc(domain.Target, listing.entries.len);
                        for (listing.entries, page_targets) |entry, *out| out.* = .{ .database = namespace.database, .namespace = namespace.name, .table = entry.name };
                        const page_lookup: domain.ResolveMany = .{ .targets = page_targets, .expected_revision = first.revision };
                        const page_bytes = try root.server.source.systemCatalog(a, root.context, .{ .resolve_many = page_lookup });
                        const page = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, page_bytes, .{ .allocate = .alloc_always });
                        if (page.revision != first.revision or page.tables.len != page_targets.len) return error.CatalogGenerationChanged;
                        for (listing.entries, page.tables) |entry, resolved| {
                            const table = resolved orelse return error.CatalogGenerationChanged;
                            if (table.table_id != entry.table.table_id) return error.CatalogGenerationChanged;
                            try table.requireAccess(root.context.native_service);
                            const resource = try (domain.Target{ .database = namespace.database, .namespace = namespace.name, .table = entry.name }).resourceNameAlloc(a);
                            try credentialAdministration(root, .table, resource);
                            try appendScope(a, &scopes, &administrators, .{ .table = entry.table.table_id }, try http.tablePermissionCurrentlyAllowed(root.identity.*, resource, .admin));
                        }
                        listing_request.legacy_membership = listing.legacy_membership;
                        listing_request.after = listing.next_after orelse break;
                        listing_request.after_table_id = listing.next_table_id;
                    }
                },
                .defaults => {
                    const owners = try a.alloc(graph.Principal, @max(intent.default_owners.len, 1));
                    if (intent.default_owners.len == 0) owners[0] = actor.session_user else for (intent.default_owners, owners) |name, *owner| owner.* = try resolvePrincipal(root, names.items, roles, name);
                    const administrator = try nativeRoleAdmin(root, "*");
                    for (owners, 0..) |owner, owner_index| {
                        if (first.namespace_ids.len == 0) {
                            const owner_name = if (intent.default_owners.len != 0) intent.default_owners[owner_index] else if (root.identity.*) |identity| identity.username else "*";
                            try credentialAdministration(root, .user, owner_name);
                            try appendScope(a, &scopes, &administrators, .{ .defaults = .{ .owner = owner, .namespace_id = 0 } }, administrator);
                        } else for (first.namespace_ids, namespaces) |namespace_id, namespace| {
                            const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(a, .{ .kind = .namespace, .database = namespace.database, .name = namespace.name });
                            try credentialAdministration(root, .namespace, resource);
                            try appendScope(a, &scopes, &administrators, .{ .defaults = .{ .owner = owner, .namespace_id = namespace_id } }, administrator);
                        }
                    }
                },
            }
            if (scopes.items.len == 0) return .{};
            const cut = try read(root, a, .{ .scopes = scopes.items, .principal = actor.session_user, .expected_revision = first.revision });
            const bindings = try a.alloc(auth.Binding, scopes.items.len);
            for (scopes.items, cut.objects, bindings) |scope, object, *binding| binding.* = .{ .scope = scope, .generation = if (object) |value| value.generation else 0 };
            const privileges: []const graph.Privilege = if (intent.scope == .roles) &.{.membership} else if (intent.all_privileges) &.{ .select, .insert, .update, .delete, .truncate, .references, .trigger, .maintain } else intent.privileges;
            request.operation = .{ .change = .{ .targets = bindings, .native_administrators = administrators.items, .grantees = grantees[0..recipient_count], .privileges = privileges, .revoke = intent.operation == .revoke, .option = intent.option, .option_only = intent.option_only, .cascade = intent.cascade } };
        },
    }
    try request.validate();
    const observation = try auth.observation(a, request);
    var receipt = try recovery.newReceipt(allocators.result, a, observation, first.revision);
    errdefer receipt.deinit(allocators.result);
    var private = root.context;
    private.setting_admin = true;
    try root.context.ensureActive();
    const bytes = root.server.source.systemCatalog(a, private, .{ .sql_authorization_mutate = request }) catch null;
    var outcome = if (bytes) |body| recovery.decodeOutcome(a, body, observation, first.revision) else null;
    if (outcome == null) outcome = recovery.observe(root.server, root.identity.*, root.context, a, observation, first.revision);
    if (outcome) |known| {
        _ = try known.result();
        common.complete(&receipt, known);
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}
