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

//! Public SQL intent producer for the existing transactional catalog lane.
//! Definition lookup grants no source SELECT authority and never opens rows.
const std = @import("std");
const sources = @import("antfly_local_sources");
const catalog = sources.sql_catalog;
const ast = sources.sql_ast;
const domain = sources.system_catalog_domain;
const definitions = sources.sql_view_definition;
const views = @import("../system_catalog/views.zig");
const http = @import("http_server.zig");
const operation = sources.api_operation;
const Path = @import("../pgwire/search_path.zig").Path;
const A = std.mem.Allocator;

const namespaceUsage = @import("sql_catalog_auth.zig").namespaceUsage;

fn owner(identity: ?http.AuthenticatedIdentity, auth_enabled: bool) !definitions.Owner {
    const actor = identity orelse return if (auth_enabled) error.Forbidden else .auth_disabled;
    if (actor.is_internal_service) return error.RowPolicyAuthenticationRequired;
    const manager = actor.live_user_manager orelse return error.RowPolicyAuthenticationRequired;
    var user = try manager.getUser(actor.username);
    defer user.deinit(manager.alloc);
    if (actor.user_instance_id) |instance| if (!std.mem.eql(u8, &instance, &user.instance_id)) return error.Forbidden;
    return .{ .user = .{ .name = actor.username, .instance_id = user.instance_id } };
}

const Loader = struct {
    server: *http.ApiHttpServer,
    identity: ?http.AuthenticatedIdentity,
    context: operation.RequestContext,
    database: []const u8,
    namespace: []const u8,
    revision: u64,
    path: ?*const Path,

    fn checkpoint(raw: *anyopaque) !void {
        const self: *Loader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
    }
    fn resolve(raw: *anyopaque, a: A, name: ast.Name) !definitions.ResolvedSource {
        const self: *Loader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        var targets: [Path.max_namespaces]domain.Target = undefined;
        var count: usize = 0;
        if (name.namespace == null and name.database == null and self.path != null) {
            for (self.path.?.entries[0..self.path.?.len]) |*entry| {
                const target: domain.Target = .{ .database = self.database, .namespace = entry.slice(), .table = name.table };
                if (!try namespaceUsage(self.identity, a, target)) continue;
                targets[count] = target;
                count += 1;
            }
        } else {
            const target: domain.Target = .{ .database = name.database orelse self.database, .namespace = name.namespace orelse self.namespace, .table = name.table };
            if (!try namespaceUsage(self.identity, a, target)) return error.Forbidden;
            targets[0] = target;
            count = 1;
        }
        if (count == 0) return error.TableNotFound;
        const lookups = try a.alloc(domain.SqlSourceLookup, count);
        for (targets[0..count], lookups) |target, *output| output.* = .{ .name = target };
        const bytes = try self.server.source.systemCatalog(a, self.context, .{ .resolve_many = .{
            .sql_sources = lookups,
            .include_query_definitions = true,
            .expected_revision = self.revision,
        } });
        const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        if (!snapshot.sql_sources_resolved or snapshot.sql_sources.len != count or snapshot.revision != self.revision) return error.CatalogGenerationChanged;
        const selected = for (snapshot.sql_sources) |source| {
            if (source) |value| break value;
        } else return error.TableNotFound;
        const scope: catalog.Table.Scope = .{
            .database = selected.target.database,
            .namespace = selected.target.namespace,
            .name = selected.target.table,
            .revision = snapshot.revision,
        };
        return switch (selected.value) {
            .table => |value| table: {
                const definition = value.binding.query_definition orelse return error.InvalidSqlBackendResponse;
                if (value.binding.table_id == 0 or definition.table_id != value.binding.table_id) return error.InvalidSqlBackendResponse;
                var table = try self.server.sql_schema_cache.resolve(self.server.sqlPlanCacheIo(), a, definition.schema_json, value.binding.table_id, value.binding.name);
                table.scope = scope;
                var digest: [32]u8 = undefined;
                std.crypto.hash.Blake3.hash(definition.schema_json, &digest, .{});
                break :table .{ .table = table, .schema_digest = digest };
            },
            .view => |resource| view: {
                const parsed = try views.Record.decode(a, resource.source_json);
                const record = parsed.value;
                if (record.id != resource.id) return error.InvalidSqlViewRecord;
                const columns = try a.alloc(catalog.Column, record.definition.columns.len);
                for (record.definition.columns, columns) |column, *output| output.* = .{
                    .name = column.name,
                    .path = column.name,
                    .type = column.type,
                    .element_type = column.element_type,
                    .numeric_modifier = column.numeric_modifier,
                };
                break :view .{
                    .kind = .view,
                    .schema_digest = record.attributes.epoch.digest,
                    .table = .{ .id = record.id, .physical_name = "", .schema_version = record.generation, .scope = scope, .columns = columns, .virtual_columns_override = &.{} },
                };
            },
        };
    }
};

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl, path: ?*const Path) !catalog.DdlOutcome {
    try context.ensureActive();
    if (!server.source.vtable.supports_sql_views or !server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const target: domain.Target = .{ .database = ddl.name.database orelse database, .namespace = ddl.name.namespace orelse namespace, .table = ddl.name.table };
    try target.validate();
    const logical = try target.resourceNameAlloc(a);
    if (!try http.tablePermissionCurrentlyAllowed(identity, logical, .admin)) return error.Forbidden;
    if (ddl.new_name) |name| {
        var destination = target;
        destination.table = name;
        try destination.validate();
        const resource = try destination.resourceNameAlloc(a);
        if (!try http.tablePermissionCurrentlyAllowed(identity, resource, .admin)) return error.Forbidden;
    }
    const bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = .{
        .sql_sources = &.{.{ .name = target }},
        .include_query_definitions = true,
    } });
    const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    if (!snapshot.sql_sources_resolved or snapshot.sql_sources.len != 1) return error.UnsupportedSqlExecution;
    const existing = snapshot.sql_sources[0];
    var expected: ?views.Identity = null;
    if (existing) |source| {
        if (source.value != .view) return if (ddl.action == .create and !(if (ddl.view_definition) |intent| intent.replace else false)) error.CatalogAlreadyExists else error.SqlWrongObjectType;
        const parsed = try views.Record.decode(a, source.value.view.source_json);
        if (parsed.value.id != source.value.view.id) return error.InvalidSqlViewRecord;
        expected = .{ .id = parsed.value.id, .generation = parsed.value.generation };
    }
    const change: views.Mutation = switch (ddl.action) {
        .create => create: {
            const intent = ddl.view_definition orelse return error.InvalidSqlSyntax;
            if (existing != null and !intent.replace) return error.CatalogAlreadyExists;
            var loader: Loader = .{ .server = server, .identity = identity, .context = context, .database = database, .namespace = namespace, .revision = snapshot.revision, .path = path };
            const Adapter = @import("sql_execution.zig").Adapter;
            var admitted = identity;
            var adapter: Adapter = .{ .server = server, .identity = &admitted, .context = context, .database = database, .namespace = namespace };
            var settings: ?sources.sql_setting_catalog.View = null;
            defer if (settings) |*value| value.deinit();
            if (identity != null) {
                const capture = adapter.settingCapture().?;
                settings = try sources.sql_setting_catalog.View.capture(a, capture.owner, capture.scope, capture.overlay);
                try settings.?.pinSqlUser(capture.sql_user);
            }
            var definition = try definitions.prepareDdl(a, .{ .ptr = &loader, .resolve = Loader.resolve, .checkpoint = Loader.checkpoint }, ddl, .{ .owner = try owner(identity, server.cfg.auth_enabled), .settings = if (settings) |*value| value else null });
            defer definition.deinit();
            // The command owner borrows definition until serialization below.
            // Its owned JSON survives the preparation's arena teardown.
            const encoded = try definition.value.encode(a);
            const retained = try std.json.parseFromSliceLeaky(definitions.Definition, a, encoded, .{ .allocate = .alloc_always });
            break :create .{ .target = target, .operation = .{ .publish = .{ .definition = retained, .replace = intent.replace, .expected = expected } } };
        },
        .drop => drop: {
            if (existing == null) {
                if (ddl.conditional) return .{};
                return error.CatalogNotFound;
            }
            break :drop .{ .target = target, .operation = .{ .drop = .{ .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } } };
        },
        .rename => rename: {
            if (expected == null) {
                if (ddl.conditional) return .{};
                return error.CatalogNotFound;
            }
            break :rename .{ .target = target, .operation = .{ .rename = .{ .expected = expected.?, .name = ddl.new_name orelse return error.InvalidSqlSyntax } } };
        },
        else => return error.UnsupportedSqlShape,
    };
    var private = context;
    private.setting_admin = true;
    const result_bytes = try server.source.systemCatalog(a, private, .{ .sql_view_mutate = .{ .expected_revision = snapshot.revision, .mutation = change } });
    const result = try std.json.parseFromSliceLeaky(views.MutationResult, a, result_bytes, .{});
    if (result.revision != snapshot.revision + 1 or result.identity == null) return error.MetadataMutationOutcomeUnknown;
    return .{};
}
