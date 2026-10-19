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

//! Statement-owned expansion over durable source IDs. This provider changes
//! resolver authority, never the adapter's caller/session identity.
const std = @import("std");
const sources = @import("antfly_local_sources");
const catalog = sources.sql_catalog;
const ast = sources.sql_ast;
const definitions = sources.sql_view_definition;
const identities = sources.system_catalog_column_identities;
const domain = sources.system_catalog_domain;
const views = @import("../system_catalog/views.zig");
const http = @import("http_server.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const authority_api = @import("sql_read_authority.zig");
const A = std.mem.Allocator;

fn sameName(left: ast.Name, right: ast.Name) bool {
    return std.mem.eql(u8, left.table, right.table) and
        std.mem.eql(u8, left.namespace orelse "", right.namespace orelse "") and
        std.mem.eql(u8, left.database orelse "", right.database orelse "");
}

pub fn lookup(root: *Adapter, a: A, target: domain.Target) !domain.ResolvedMany {
    const bytes = try root.server.source.systemCatalog(a, root.context, .{ .resolve_many = .{
        .sql_sources = &.{.{ .name = target }},
        .expected_revision = root.revision,
        .include_query_definitions = true,
    } });
    const result = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    if (!result.sql_sources_resolved or result.sql_sources.len != 1) return error.UnsupportedSqlExecution;
    if (root.revision) |revision| if (result.revision != revision) return error.CatalogGenerationChanged;
    root.revision = result.revision;
    return result;
}

fn liveInstance(identity: http.AuthenticatedIdentity) ![16]u8 {
    const manager = identity.live_user_manager orelse return error.RowPolicyAuthenticationRequired;
    var user = manager.getUser(identity.username) catch |err| switch (err) {
        error.UserNotFound => return error.Forbidden,
        else => return err,
    };
    defer user.deinit(manager.alloc);
    if (identity.user_instance_id) |expected| if (!std.mem.eql(u8, &expected, &user.instance_id)) return error.Forbidden;
    return user.instance_id;
}

fn captureIdentity(a: A, value: http.AuthenticatedIdentity) !http.AuthenticatedIdentity {
    // Catalog alias projection mutates the adapter's original identity. A
    // by-value copy would retain alias/filter arrays freed by later binds.
    var captured = try http.cloneAuthenticatedIdentity(a, value.username, value.credential_principal, value.permissions, value.row_filter, value.metadata_json, value.roles);
    captured.live_user_manager = value.live_user_manager;
    captured.user_instance_id = value.user_instance_id;
    captured.is_internal_service = value.is_internal_service;
    return captured;
}

fn ownerIdentity(a: A, caller: http.AuthenticatedIdentity, principal: definitions.Principal) !http.AuthenticatedIdentity {
    const manager = caller.live_user_manager orelse return error.RowPolicyAuthenticationRequired;
    var user = manager.getUser(principal.name) catch |err| switch (err) {
        error.UserNotFound => return error.Forbidden,
        else => return err,
    };
    defer user.deinit(manager.alloc);
    if (!std.mem.eql(u8, &user.instance_id, &principal.instance_id)) return error.Forbidden;
    const permissions = try manager.getPermissionsForUser(principal.name);
    defer {
        for (permissions) |*value| value.deinit(manager.alloc);
        manager.alloc.free(permissions);
    }
    const filters = try manager.getRowFilters(principal.name);
    defer {
        for (filters) |*value| value.deinit(manager.alloc);
        manager.alloc.free(filters);
    }
    const roles = try manager.getRolesForUser(principal.name);
    defer {
        for (roles) |value| manager.alloc.free(value);
        manager.alloc.free(roles);
    }
    const credential = try std.fmt.allocPrint(a, "basic:{s}", .{principal.name});
    var identity = try http.cloneAuthenticatedIdentity(a, user.username, credential, permissions, filters, user.metadata_json, roles);
    identity.live_user_manager = manager;
    identity.user_instance_id = principal.instance_id;
    return identity;
}

fn readAuthority(root: *Adapter, a: A, target: domain.Target, record: views.Record, inherited: ?*const authority_api.Context) !?*const authority_api.Context {
    if (!root.server.cfg.auth_enabled and root.identity.* == null) {
        if (record.definition.owner != .auth_disabled) return error.Forbidden;
        return null;
    }
    const caller = root.identity.* orelse return error.Forbidden;
    const incoming = if (inherited) |value| value.effective else try captureIdentity(a, caller);
    const logical = try target.resourceNameAlloc(a);
    if (!try http.tablePermissionCurrentlyAllowed(incoming, logical, .read)) return error.Forbidden;
    const incoming_instance = try liveInstance(incoming);
    const prior = if (inherited) |value| value.grants else &.{};
    if (prior.len >= 64) return error.SqlProgramLimitExceeded;
    const grants = try a.alloc(authority_api.Grant, prior.len + 1);
    @memcpy(grants[0..prior.len], prior);
    grants[prior.len] = .{ .identity = incoming, .instance = incoming_instance, .logical_view = logical };
    // PostgreSQL security_invoker restores the original query caller even
    // inside a non-invoker view. It does not inherit the outer view owner.
    const effective = if (record.definition.security == .invoker) try captureIdentity(a, caller) else switch (record.definition.owner) {
        .user => |principal| try ownerIdentity(a, caller, principal),
        .auth_disabled, .role => return error.RowPolicyAuthenticationRequired,
    };
    const instance = try liveInstance(effective);
    const result = try a.create(authority_api.Context);
    result.* = .{ .effective = effective, .instance = instance, .grants = grants, .delegation = null };
    {
        const Manager = @import("../usermgr/mod.zig").UserManager;
        const intermediate = try a.alloc(Manager.ViewDelegation.Intermediate, grants.len - 1);
        for (grants[1..], intermediate) |grant, *output| output.* = .{
            .principal = grant.identity.username,
            .instance = grant.instance,
            .logical_view = grant.logical_view,
        };
        result.delegation = .{
            .logical_view = grants[0].logical_view,
            .owner = effective.username,
            .owner_instance = instance,
            .caller_instance = try liveInstance(caller),
            .intermediate = intermediate,
            .invoker = record.definition.security == .invoker,
        };
    }
    if (root.collect_view_authorities) |collected| try collected.append(a, result);
    return result;
}

const Context = struct {
    root: *Adapter,
    record: views.Record,
    resolved: []const ?domain.SqlSource,
    authority: ?*const authority_api.Context,

    fn backend(self: *Context) catalog.Backend {
        return .{ .ptr = self, .binding_scope = @intFromPtr(self), .vtable = &.{
            .resolve_read = resolveRead,
            .resolve = resolve,
            .scan = scan,
            .mutate = mutate,
            .checkpoint = checkpoint,
        } };
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Context = @ptrCast(@alignCast(raw));
        try self.root.context.ensureActive();
    }
    fn resolve(raw: *anyopaque, a: A, name: ast.Name, action: catalog.Action) !catalog.Table {
        if (action != .read) return error.Forbidden;
        return switch (try resolveRead(raw, a, name)) {
            .table => |table| table,
            .view => error.UnsupportedSqlShape,
        };
    }
    fn resolveRead(raw: *anyopaque, a: A, name: ast.Name) !catalog.ReadRelation {
        const self: *Context = @ptrCast(@alignCast(raw));
        try self.root.context.ensureActive();
        for (self.record.definition.sources, 0..) |source, index| {
            if (!sameName(source.reference, name)) continue;
            const current = self.resolved[index] orelse return error.CatalogGenerationChanged;
            switch (current.value) {
                .table => |value| {
                    if (source.kind != .table or value.binding.table_id != source.table_id) return error.CatalogGenerationChanged;
                    const bytes = value.column_identities_json orelse return error.InvalidSqlViewRecord;
                    var state = try identities.State.decode(a, bytes);
                    defer state.deinit();
                    try self.record.requireSource(index, state.value, false);
                    const definition = value.binding.query_definition orelse return error.InvalidSqlBackendResponse;
                    var digest: [32]u8 = undefined;
                    std.crypto.hash.Blake3.hash(definition.schema_json, &digest, .{});
                    if (!std.mem.eql(u8, &digest, &state.value.epoch.digest)) return error.InvalidSqlViewRecord;
                    var table = try self.root.bindViewSource(a, current.target, value.binding, self.root.revision.?);
                    if (table.schema_version != state.value.epoch.version) return error.InvalidSqlViewRecord;
                    const Item = struct { ordinal: u32, column: catalog.Column };
                    var columns: std.ArrayList(Item) = .empty;
                    for (source.columns, self.record.source_columns[index]) |captured, id| {
                        const attribute = try state.value.requireDependency(id, captured.signature);
                        if (captured.virtual) continue;
                        var column = try table.column(attribute.name);
                        column.name = captured.name;
                        try columns.append(a, .{ .ordinal = captured.ordinal, .column = column });
                    }
                    std.mem.sort(Item, columns.items, {}, struct {
                        fn less(_: void, left: Item, right: Item) bool {
                            return left.ordinal < right.ordinal;
                        }
                    }.less);
                    const frozen = try a.alloc(catalog.Column, columns.items.len);
                    for (columns.items, frozen) |column, *output| output.* = column.column;
                    table.columns = frozen;
                    table.binding_scope = .{ .database = source.database, .namespace = source.namespace, .name = source.name, .revision = self.root.revision.? };
                    table.read_authority = self.authority;
                    try authority_api.check(table);
                    return .{ .table = table };
                },
                .view => |resource| {
                    if (source.kind != .view or resource.id != source.table_id) return error.CatalogGenerationChanged;
                    var parsed = try views.Record.decode(a, resource.source_json);
                    defer parsed.deinit();
                    try self.record.requireSource(index, parsed.value.attributes, false);
                    var nested = try prepare(self.root, a, current.target, resource, self.authority);
                    if (source.columns.len != 0) {
                        const Item = struct { captured: u32, exposure: catalog.ViewExposure };
                        const selected = try a.alloc(Item, source.columns.len);
                        for (source.columns, self.record.source_columns[index], selected) |column, id, *output| {
                            if (column.virtual) return error.InvalidSqlViewRecord;
                            const ordinal = for (parsed.value.attributes.columns, 0..) |attribute, position| {
                                if (attribute.id == id) break position;
                            } else return error.CatalogGenerationChanged;
                            output.* = .{ .captured = column.ordinal, .exposure = .{ .ordinal = ordinal, .name = column.name } };
                        }
                        std.mem.sort(Item, selected, {}, struct {
                            fn less(_: void, left: Item, right: Item) bool {
                                return left.captured < right.captured;
                            }
                        }.less);
                        const exposure = try a.alloc(catalog.ViewExposure, selected.len);
                        for (selected, exposure) |item, *output| output.* = item.exposure;
                        nested.view.exposure = exposure;
                    }
                    return nested;
                },
            }
        }
        // A stored body cannot resolve arbitrary new names or escape into a
        // caller CTE/search_path when its captured source manifest is missing.
        return error.InvalidSqlViewRecord;
    }
    fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.InvalidSqlBackendResponse;
    }
    fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.Forbidden;
    }
};

pub fn prepare(root: *Adapter, a: A, target: domain.Target, resource: domain.Resource, inherited: ?*const authority_api.Context) !catalog.ReadRelation {
    _ = root.revision orelse return error.InvalidSqlBackendResponse;
    const parsed = try views.Record.decode(a, resource.source_json);
    // The resolver allocator is the statement region; the parsed definition
    // and compiled body remain owned by that region through cursor teardown.
    const record = parsed.value;
    if (resource.kind != .view or resource.id != record.id) return error.InvalidSqlViewRecord;
    const authority = try readAuthority(root, a, target, record, inherited);
    const Binding = @import("sql_prepared.zig").Binding;
    const manifest = Binding.fromView(target, record.id, record.generation, record.attributes.epoch.digest, root.revision.?);
    if (root.prepared_bindings) |bindings| try Binding.verifyView(bindings, manifest);
    if (root.collect_prepared_bindings) |bindings| {
        const existing = for (bindings.items) |entry| {
            if (entry.kind != .view or entry.id != manifest.id) continue;
            try Binding.verifyView(&.{entry}, manifest);
            break true;
        } else false;
        if (!existing) {
            if (bindings.items.len >= 64) return error.SqlProgramLimitExceeded;
            try bindings.append(a, manifest);
        }
    }
    const lookups = try a.alloc(domain.SqlSourceLookup, record.definition.sources.len);
    for (record.definition.sources, lookups) |source, *output| output.* = switch (source.kind) {
        .table => .{ .table_id = source.table_id },
        .view => .{ .view_id = source.table_id },
    };
    var resolved: []const ?domain.SqlSource = &.{};
    if (lookups.len != 0) {
        const bytes = try root.server.source.systemCatalog(a, root.context, .{ .resolve_many = .{
            .sql_sources = lookups,
            .expected_revision = root.revision,
            .include_query_definitions = true,
        } });
        const snapshot = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        if (!snapshot.sql_sources_resolved or snapshot.sql_sources.len != lookups.len or snapshot.revision != root.revision.?) return error.CatalogGenerationChanged;
        resolved = snapshot.sql_sources;
    }
    const compiled = try a.create(sources.sql_compiler.Compiled);
    compiled.* = try sources.sql_compiler.compile(a, record.definition.query, .{});
    if (compiled.statement != .select or compiled.parameter_count != 0) return error.InvalidSqlViewRecord;
    const context = try a.create(Context);
    context.* = .{ .root = root, .record = record, .resolved = resolved, .authority = authority };
    var backend = context.backend();
    if (compiled.uses_current_setting) {
        const capture = root.settingCapture().?;
        const settings = try a.create(sources.sql_setting_catalog.View);
        settings.* = try sources.sql_setting_catalog.View.capture(a, capture.owner, capture.scope, capture.overlay);
        try settings.pinSqlUser(capture.sql_user);
        backend.settings_view = settings;
    }
    const columns = try a.alloc(catalog.Column, record.definition.columns.len);
    for (record.definition.columns, columns) |column, *output| output.* = .{
        .name = column.name,
        .path = column.name,
        .type = column.type,
        .element_type = column.element_type,
        .numeric_modifier = column.numeric_modifier,
    };
    const logical = try target.resourceNameAlloc(a);
    const incoming = if (inherited) |value| value.effective else root.identity.*;
    const row_filter = try @import("sql_view_filter.zig").prepare(a, root.context, root.identity.*, incoming, logical, columns);
    return .{ .view = .{ .id = record.id, .generation = record.generation, .query = &compiled.statement.select, .columns = columns, .backend = backend, .security_barrier = record.definition.security_barrier, .row_filter = row_filter } };
}

test "SQL view public resolver executes a stored definition without a fabricated physical table" {
    const Fixture = struct {
        record: domain.Resource,
        tagged_reads: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn call(raw: *anyopaque, a: A, _: sources.api_operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (input != .resolve_many) return error.TestUnexpectedCatalogCall;
            const request = input.resolve_many;
            if (request.sql_sources.len == 0) return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 1, .tables = &.{null} }, .{});
            self.tagged_reads += 1;
            try std.testing.expectEqual(@as(usize, 1), request.sql_sources.len);
            const found = switch (request.sql_sources[0]) {
                .name => |name| std.mem.eql(u8, name.table, self.record.name),
                .view_id => |id| id == self.record.id,
                .table_id => false,
            };
            const target: domain.Target = .{ .table = self.record.name };
            return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{
                .revision = 1,
                .tables = &.{},
                .sql_sources_resolved = true,
                .sql_sources = &.{if (found) .{ .target = target, .value = .{ .view = self.record } } else null},
            }, .{});
        }
    };
    const a = std.testing.allocator;
    const definition: definitions.Definition = .{
        .format = 1,
        .query = "SELECT 7::BIGINT AS id",
        .owner = .auth_disabled,
        .security = .invoker,
        .security_barrier = true,
        .catalog_revision = null,
        .setting_epoch = null,
        .columns = &.{.{ .name = "id", .type = .integer, .element_type = .int64 }},
        .sources = &.{},
    };
    var prepared = try views.prepare(a, 3, definition, null, &.{});
    defer prepared.deinit();
    const record = try prepared.value.encode(a);
    defer a.free(record);
    var fixture: Fixture = .{ .record = .{ .kind = .view, .id = 3, .parent_id = 2, .name = "stored", .source_json = record } };
    var server = http.ApiHttpServer.init(a, .{}, .{ .ptr = &fixture, .vtable = &.{
        .status = Fixture.status,
        .system_catalog = Fixture.call,
        .supports_query_definitions = true,
        .supports_sql_views = true,
    } }, null, null);
    defer server.deinit();
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    for ([_][]const u8{ "SELECT id FROM stored", "SELECT stored.id FROM stored", "SELECT count(*) FROM stored" }) |sql| {
        var compiled = try sources.sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try adapter.execute(a, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings(if (std.mem.indexOf(u8, sql, "count") != null) "1" else "7", result.output.rows[0][0].string);
    }
    try std.testing.expectEqual(@as(usize, 3), fixture.tagged_reads);
    const manifest = @import("sql_prepared.zig").Binding.fromView(.{ .table = "stored" }, 3, prepared.value.generation, prepared.value.attributes.epoch.digest, 1);
    adapter.prepared_bindings = &.{manifest};
    fixture.record.name = "renamed";
    var compiled = try sources.sql_compiler.compile(a, "SELECT stored.id FROM stored", .{});
    defer compiled.deinit();
    {
        var result = try adapter.execute(a, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqualStrings("7", result.output.rows[0][0].string);
    }
    // A replacement under the original spelling cannot satisfy the captured
    // ID, even if it has exactly the same public output shape.
    fixture.record.id = 4;
    fixture.record.name = "stored";
    try std.testing.expectError(error.CatalogGenerationChanged, adapter.execute(a, &compiled, &.{}, .{}, null));
}
