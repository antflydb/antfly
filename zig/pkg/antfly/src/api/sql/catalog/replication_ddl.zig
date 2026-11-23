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

//! Authenticated SQL admission for native replication definitions.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("../../http_server.zig");
const ordinary = @import("ordinary_catalog.zig");
const replication = @import("../../../system_catalog/replication/logical_replication.zig");
const Adapter = @import("../execution/execution.zig").Adapter;
const Context = local.api_operation.RequestContext;
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const ast = local.sql_ast;
const parameters = local.system_catalog_logical_replication;
const installation = @import("../../../system_catalog/replication/installation.zig");
const native = @import("../../../system_catalog/catalog_mutations.zig");

/// Resolve bounded pages of name candidates at one catalog/relation epoch.
/// No table schema decoding or per-table metadata round trip is needed here;
/// the native writer revalidates every actual guard before admitting a delta.
fn members(root: *Adapter, a: std.mem.Allocator, names: []const ast.Name, cut: installation.DdlView) ![]const domain.RelationMutationGuard {
    if (names.len > parameters.max_members) return error.SqlLimitExceeded;
    const Range = struct { start: usize, end: usize };
    const ranges = try a.alloc(Range, names.len);
    var targets: std.ArrayList(domain.RelationTarget) = .empty;
    for (names, ranges) |name, *range| {
        try root.context.ensureActive();
        if (name.database) |database| if (!std.mem.eql(u8, database, root.database)) return error.UnsupportedSqlShape;
        const qualified = name.namespace != null or name.database != null;
        const count = if (!qualified and root.ddl_search_path != null) root.ddl_search_path.?.len else 1;
        range.start = targets.items.len;
        for (0..count) |index| {
            const namespace = if (!qualified and root.ddl_search_path != null) root.ddl_search_path.?.entries[index].slice() else name.namespace orelse root.namespace;
            const target: domain.Target = .{ .database = root.database, .namespace = namespace, .table = name.table };
            if (!try @import("catalog_auth.zig").namespaceUsage(root.identity.*, a, target)) {
                if (qualified) return error.Forbidden;
                continue;
            }
            try targets.append(a, .{ .database = root.database, .namespace = namespace, .name = name.table });
        }
        range.end = targets.items.len;
        if (range.start == range.end) return error.TableNotFound;
    }
    const resolved = try a.alloc(?domain.ResolvedRelation, targets.items.len);
    var epoch: @FieldType(domain.ResolvedMany, "relation_epoch") = null;
    var offset: usize = 0;
    while (offset < targets.items.len) {
        try root.context.ensureActive();
        const end = @min(offset + 128, targets.items.len);
        const query: domain.ResolveMany = .{ .relations = targets.items[offset..end], .expected_revision = cut.revision, .expected_relation_epoch = epoch };
        const bytes = try root.server.source.systemCatalog(a, root.context, .{ .resolve_many = query });
        const found = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        try found.validateRelations(query);
        if (found.revision != cut.revision) return error.CatalogGenerationChanged;
        epoch = found.relation_epoch;
        @memcpy(resolved[offset..end], found.relations);
        offset = end;
    }
    const guards = try a.alloc(domain.RelationMutationGuard, names.len);
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (ranges, guards) |range, *guard| {
        const selected = for (resolved[range.start..range.end], range.start..) |candidate, index| {
            if (candidate) |value| break .{ .index = index, .value = value };
        } else return error.TableNotFound;
        const target = targets.items[selected.index];
        const resource = try (domain.Target{ .database = target.database, .namespace = target.namespace, .table = selected.value.logical_table }).resourceNameAlloc(a);
        if (!try http.resourcePermissionCurrentlyAllowed(root.identity.*, .table, resource, .admin)) return error.Forbidden;
        if (selected.value.owner.kind != .table) return error.SqlWrongObjectType;
        if ((try seen.getOrPut(a, selected.value.owner.table_id)).found_existing) return error.InvalidLogicalReplicationRecord;
        guard.* = .{ .target = target, .logical_table = selected.value.logical_table, .owner = selected.value.owner, .incarnation = epoch.?.incarnation };
    }
    return guards;
}

pub fn databaseId(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: std.mem.Allocator, database: []const u8, name: []const u8) !u64 {
    try ordinary.authorize(server, identity, a, .{
        .kind = .subscription,
        .action = .create,
        .database = database,
        .namespace = domain.default_namespace_name,
        .name = name,
        .table_id = 0,
    });
    const bytes = try server.source.systemCatalog(a, context, .{ .read = .{ .kind = .database, .name = database } });
    const state = try std.json.parseFromSliceLeaky(domain.State, a, bytes, .{});
    return (state.find(.database, 0, database) orelse return error.DatabaseNotFound).id;
}

pub fn execute(root: *Adapter, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    try root.context.ensureActive();
    const syntax = ddl.replication orelse return error.InvalidSqlSyntax;
    try syntax.validateClassification(ddl);
    if (root.active_transaction != null) return error.SqlDdlInsideTransaction;
    if (ddl.name.database != null or ddl.name.namespace != null) return error.InvalidSqlSyntax;
    if (ddl.kind == .publication) return executePublication(root, allocators, ddl);
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const id = try databaseId(root.server, root.identity.*, root.context, a, root.database, ddl.name.table);
    if (root.ddl_guard) |guard| {
        const bound = try guard.objects(a);
        if (bound.parent_id != id) return error.CatalogGenerationChanged;
    }
    const actor = try @import("../recovery/authorization_recovery.zig").actor(root.server, root.identity.*);
    const role = if (root.sql_authority) |authority| if (authority.principal.kind == .role) authority.principal else null else null;
    const execution_actor: local.system_catalog_logical_replication_catalog.ExecutionActor = .{
        .session_user = actor.session_user,
        .credential = actor.credential,
        .current_role = role,
    };
    const action: domain.Action = switch (syntax) {
        .create_subscription => .create,
        .set_subscription_enabled => .alter,
        .drop_subscription => .drop,
        else => return error.UnsupportedSqlExecution,
    };
    const intent = switch (syntax) {
        .create_subscription => |creation| blk: {
            const options = try parameters.subscriptionOptions(ddl.name.table, creation.options);
            if (options.connect and root.server.cfg.deployment_mode == .standalone)
                return error.UnsupportedSqlExecution;
            break :blk try std.json.Stringify.valueAlloc(a, replication.SubscriptionCreateIntent{
                .owner = try publicationOwner(root),
                .execution_actor = execution_actor,
                .connection = creation.connection,
                .publications = creation.publications,
                .options = options,
            }, .{});
        },
        .set_subscription_enabled => |enabled| try std.json.Stringify.valueAlloc(a, replication.SubscriptionChangeIntent{
            .actor = execution_actor,
            .enabled = enabled,
        }, .{}),
        .drop_subscription => try std.json.Stringify.valueAlloc(a, replication.SubscriptionChangeIntent{
            .actor = execution_actor,
            .if_exists = ddl.conditional,
        }, .{}),
        else => unreachable,
    };
    var reply = ordinary.submit(root.server, root.identity.*, root.context, allocators, .{ .mutation = .{
        .kind = .subscription,
        .action = action,
        .name = ddl.name.table,
        .database = root.database,
        .expected_parent_id = id,
        .source_json = intent,
    } }) catch |err| switch (err) {
        error.CatalogAlreadyExists => return error.SqlDuplicateSubscription,
        else => return err,
    };
    defer reply.deinit(allocators.result);
    return .{ .mutation_outcome = if (reply.unknown) null else .committed, .catalog_receipt = reply.takeReceipt() };
}
fn executePublication(root: *Adapter, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const syntax = ddl.replication.?;
    const action: domain.Action = switch (syntax) {
        .create_publication => .create,
        .alter_publication => .alter,
        .drop_publication => .drop,
        else => return error.InvalidSqlSyntax,
    };
    try ordinary.authorize(root.server, root.identity.*, a, .{
        .kind = .publication,
        .action = action,
        .database = root.database,
        .namespace = domain.default_namespace_name,
        .name = ddl.name.table,
        .table_id = 0,
    });
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_publication_capture_work = .{ .ddl = .{ .database = root.database, .name = ddl.name.table } } });
    const cut = try std.json.parseFromSliceLeaky(installation.DdlView, a, bytes, .{ .allocate = .alloc_always });
    if (root.ddl_guard) |guard| {
        const bound = try guard.objects(a);
        if (bound.parent_id != cut.database_id) return error.CatalogGenerationChanged;
    }
    if (action == .create and cut.definition != null) return error.SqlDuplicatePublication;
    if (cut.definition == null and !(action == .create or (action == .drop and ddl.conditional))) return error.PublicationNotFound;
    if (syntax == .create_publication and syntax.create_publication.all_tables and
        !try http.resourcePermissionCurrentlyAllowed(root.identity.*, .user, "*", .admin)) return error.Forbidden;
    const expected = if (cut.definition) |definition| try definition.stamp(a) else null;
    const io = root.server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const request: replication.Request = .{
        .admission = .{ .head = cut.head, .nonce = nonce, .command_fence = if (root.context.sql_command_fence) |fence| fence.* else null },
        .incarnation = cut.incarnation,
        .database = root.database,
        .database_id = cut.database_id,
        .name = ddl.name.table,
        .expected = expected,
        .operation = switch (syntax) {
            .create_publication => |value| .{ .create_publication = .{
                .owner = try publicationOwner(root),
                .all_tables = value.all_tables,
                .tables = try members(root, a, value.tables, cut),
                .options = try parameters.publicationOptions(value.options, .{}),
            } },
            .alter_publication => |value| .{ .alter_publication = switch (value) {
                .options => |options| .{ .mode = .options, .options = try parameters.publicationOptions(options, cut.definition.?.parameters) },
                inline .add_tables, .set_tables, .drop_tables => |tables, tag| .{
                    .mode = switch (tag) {
                        .add_tables => .add,
                        .set_tables => .set,
                        .drop_tables => .drop,
                        else => unreachable,
                    },
                    .tables = try members(root, a, tables, cut),
                },
            } },
            .drop_publication => .{ .drop_publication = .{ .if_exists = ddl.conditional } },
            else => unreachable,
        },
    };
    const command = native.publicationCommand(cut.revision, request);
    const observation = try native.observation(a, command);
    var receipt = try ordinary.newReceipt(allocators.result, a, observation);
    errdefer receipt.deinit(allocators.result);
    const response = root.server.source.systemCatalog(a, private, .{ .sql_catalog_mutate = command }) catch null;
    const known = if (response) |value| ordinary.decodeOutcome(a, value, observation) else null;
    if (known orelse ordinary.observe(root.server, root.identity.*, root.context, a, observation)) |outcome| {
        if (outcome == .rejected) _ = outcome.result() catch |err| switch (err) {
            error.CatalogAlreadyExists => return error.SqlDuplicatePublication,
            error.CatalogNotFound => return error.PublicationNotFound,
            else => return err,
        };
        @import("../recovery/catalog_recovery.zig").complete(&receipt, outcome);
        return .{ .mutation_outcome = if (outcome == .success) .committed else null, .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}

fn publicationOwner(root: *Adapter) !replication.Owner {
    if (root.sql_authority) |authority| if (authority.principal.kind == .role) {
        for (authority.roles) |role| {
            if (role.principal().eql(authority.principal)) return .{ .role = .{ .name = role.name, .instance_id = role.instance } };
        }
        return error.Forbidden;
    };
    return @import("view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled);
}
