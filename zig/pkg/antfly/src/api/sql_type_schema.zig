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

//! Catalog-owned named type binding shared by public schema admission. Only
//! physical column annotations create dependencies; user JSON values do not.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const types = @import("../system_catalog/named_types.zig");
const auth = @import("sql_catalog_auth.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;

fn mayReferenceTypes(bytes: []const u8) bool {
    // Escaped JSON keys must take the full parser path. The fast path is only
    // a safe negative test, never a parser or an authorization decision.
    return std.mem.indexOf(u8, bytes, "x-antfly-sql-type-id") != null or
        std.mem.indexOf(u8, bytes, "named_types") != null or
        std.mem.indexOf(u8, bytes, "sql_enum") != null or
        std.mem.indexOf(u8, bytes, "\\u") != null;
}

fn bindValue(server: *http.ApiHttpServer, a: A, identity: ?http.AuthenticatedIdentity, context: Context, root: *std.json.Value) !void {
    try context.ensureActive();
    const ids = try local.schema_mod.namedTypeReferencesAlloc(a, root.*);
    // A public readonly directory cannot introduce dependencies or retain
    // stale snapshots after the last referencing column is removed.
    if (ids.len == 0) {
        _ = root.object.orderedRemove("named_types");
        return;
    }
    const records = try recordsForIdsLeaky(server, a, identity, context, ids);
    try context.ensureActive();
    try local.sql_catalog.named_layouts.canonicalizeSchemaLeaky(a, root, records);
}

pub fn recordsForIdsLeaky(server: *http.ApiHttpServer, a: A, identity: ?http.AuthenticatedIdentity, context: Context, ids: []const u64) ![]const types.records.Record {
    try context.ensureActive();
    if (ids.len == 0) return &.{};
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    if (!server.source.vtable.supports_sql_types) return error.MetadataCapabilityUnavailable;
    const request: types.BindingsRequest = .{ .ids = ids };
    var private = context;
    private.setting_admin = true;
    const bytes = try server.source.systemCatalog(a, private, .{ .sql_type_bindings = request });
    defer a.free(bytes);
    const cut = try std.json.parseFromSliceLeaky(types.BindingsSnapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(a, request);
    const records = try a.alloc(types.records.Record, cut.bindings.len);
    for (cut.bindings, records) |entry, *record| {
        if (!try auth.namespaceUsage(identity, a, entry.target)) return error.Forbidden;
        record.* = entry.record;
    }
    try context.ensureActive();
    return records;
}

/// Rebind declarative identities through current catalog authority. No process
/// pointer is accepted from durable JSON or a client-declared PostgreSQL OID.
/// Empty builtin-only inputs keep their allocation-free negative path.
pub fn parameterDescriptorsLeaky(server: *http.ApiHttpServer, a: A, identity: ?http.AuthenticatedIdentity, context: Context, input: []const local.sql_scalar.Type, oids: []const u32) ![]const local.sql_scalar.Type {
    if (oids.len != 0 and oids.len != input.len) return error.InvalidSqlParameters;
    var ids: std.ArrayList(u64) = .empty;
    for (input, 0..) |descriptor, ordinal| {
        const id: ?u64 = if (descriptor.domainView()) |view| view.identity.?.id else if (descriptor.domainReference()) |reference| reference.id else if (descriptor.enum_view) |view| view.record.identity.id else if (descriptor.enum_reference) |reference| reference.id else if (oids.len != 0 and oids[ordinal] >= 16386) candidate: {
            if (oids[ordinal] % 2 != 0) return error.UnsupportedParameterType;
            break :candidate (oids[ordinal] - 16384) / 2;
        } else null;
        if (id) |value| try ids.append(a, value);
    }
    if (ids.items.len == 0) return input;
    std.mem.sort(u64, ids.items, {}, std.sort.asc(u64));
    var unique: usize = 0;
    for (ids.items) |id| {
        if (unique == 0 or ids.items[unique - 1] != id) {
            ids.items[unique] = id;
            unique += 1;
        }
    }
    ids.items.len = unique;
    const records = try recordsForIdsLeaky(server, a, identity, context, ids.items);
    const domains = try a.create(local.sql_catalog.domain_values.Registry);
    domains.* = try local.sql_catalog.domain_values.Registry.init(a, records);
    const result = try a.dupe(local.sql_scalar.Type, input);
    for (result, 0..) |*descriptor, ordinal| {
        const reference: ?local.sql_scalar.EnumReference = if (descriptor.domainView()) |view| local.sql_scalar.referenceOf(view) else if (descriptor.domainReference()) |value| .{ .id = value.id, .origin_id = value.origin_id, .generation = value.generation } else if (descriptor.enum_reference) |value| value.* else if (descriptor.enum_view) |view| .{ .id = view.record.identity.id, .origin_id = view.record.identity.origin_id, .generation = view.record.identity.generation } else null;
        const id = if (reference) |value| value.id else if (oids.len != 0 and oids[ordinal] >= 16386) @as(u64, (oids[ordinal] - 16384) / 2) else continue;
        const entry = domains.byId(id) orelse return error.SqlTypeNotFound;
        if (entry.record.definition == .domain) {
            const view = &entry.executable.?.domain;
            const current = view.identity.?;
            if (reference) |expected| if (expected.generation != current.generation or !std.mem.eql(u8, &expected.origin_id, &current.origin_id)) return error.CatalogGenerationChanged;
            if (oids.len != 0 and oids[ordinal] != 0 and oids[ordinal] != current.oid) return error.InvalidSqlParameters;
            const resolved = try local.sql_scalar.domainType(view);
            // The catalog owns the physical layout; client metadata cannot
            // substitute a different base decoder under a valid domain ID.
            if (reference != null and (descriptor.kind != resolved.kind or descriptor.element_type != resolved.element_type or !local.sql_scalar.NumericModifier.eql(descriptor.numeric_modifier, resolved.numeric_modifier))) return error.InvalidSqlParameters;
            descriptor.* = resolved;
            continue;
        }
        if (descriptor.domainView() != null or descriptor.domainReference() != null) return error.CatalogGenerationChanged;
        const view = &entry.executable.?.enumeration;
        if (reference) |expected| if (!std.mem.eql(u8, &expected.origin_id, &view.record.identity.origin_id) or view.record.identity.generation < expected.generation) return error.CatalogGenerationChanged;
        if (oids.len != 0 and oids[ordinal] != 0 and oids[ordinal] != view.record.identity.oid) return error.InvalidSqlParameters;
        descriptor.kind = .enumeration;
        descriptor.enum_view = view;
        descriptor.enum_reference = null;
        try local.sql_scalar.validateParameterType(descriptor.*);
    }
    return result;
}

/// Returns null without allocation for ordinary schemas. Replacement bytes
/// belong to the caller; every intermediate and catalog snapshot is scoped to
/// one admission region and cannot escape into row execution.
pub fn bindSchemaAlloc(server: *http.ApiHttpServer, a: A, identity: ?http.AuthenticatedIdentity, context: Context, schema: []const u8) !?[]u8 {
    if (!mayReferenceTypes(schema)) return null;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, schema, .{ .parse_numbers = false });
    try bindValue(server, scratch, identity, context, &root);
    return try std.json.Stringify.valueAlloc(a, root, .{});
}

/// Bind before the ordinary public create parser, so callers supply only type
/// IDs, never authority over a definition. Preserve exact JSON numeric tokens.
pub fn bindCreateBodyAlloc(server: *http.ApiHttpServer, a: A, identity: ?http.AuthenticatedIdentity, context: Context, body: []const u8) !?[]u8 {
    if (!mayReferenceTypes(body)) return null;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, body, .{ .parse_numbers = false });
    if (root != .object) return error.InvalidCreateTableRequest;
    const schema = root.object.getPtr("schema") orelse return null;
    if (schema.* == .null) return null;
    try bindValue(server, scratch, identity, context, schema);
    return try std.json.Stringify.valueAlloc(a, root, .{});
}

/// PATCH must bind the complete postimage, including retained columns, against
/// the same authoritative predecessor that will guard later publication.
pub fn bindUpdateAlloc(server: *http.ApiHttpServer, a: A, identity: ?http.AuthenticatedIdentity, context: Context, before: []const u8, body: []const u8, mode: @import("tables.zig").SchemaMutationMode) ![]u8 {
    const merged = if (mode == .merge_patch) try @import("tables.zig").mergeSchemaPatchDocumentAlloc(a, before, body) else null;
    defer if (merged) |bytes| a.free(bytes);
    const proposed = merged orelse body;
    const bound = try bindSchemaAlloc(server, a, identity, context, proposed);
    defer if (bound) |bytes| a.free(bytes);
    return @import("table_contract.zig").parseSchemaUpdateRequest(a, bound orelse proposed);
}

test "SQL named enum schema ingress authorizes one catalog cut and preserves ordinary escaped schemas" {
    const a = std.testing.allocator;
    const users = @import("../usermgr/mod.zig");
    const State = struct {
        record: types.records.Record,
        namespace: []const u8 = "public",
        calls: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
        fn catalog(raw: *anyopaque, alloc: A, context: Context, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(context.setting_admin);
            if (call != .sql_type_bindings) return error.UnexpectedTypeSchemaCall;
            try std.testing.expectEqualSlices(u64, &.{7}, call.sql_type_bindings.ids);
            self.calls += 1;
            return std.json.Stringify.valueAlloc(alloc, types.BindingsSnapshot{
                .revision = 1,
                .next_id = 8,
                .incarnation = @splat(9),
                .bindings = &.{.{ .target = .{ .namespace = self.namespace, .table = "state" }, .namespace_id = 2, .record = self.record }},
            }, .{});
        }
    };
    var labels = try types.records.Owned.create(a, &.{ "z", "a", "" });
    defer labels.deinit();
    var state: State = .{ .record = .{ .identity = try types.records.Identity.init(7, 2, 1, @splat(3)), .definition = labels.definition, .owner = .auth_disabled } };
    var server = http.ApiHttpServer.init(a, .{ .auth_enabled = true }, .{ .ptr = &state, .vtable = &.{ .status = State.status, .system_catalog = State.catalog, .supports_sql_types = true } }, null, null);
    defer server.deinit();
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expect(try bindSchemaAlloc(&server, denied.allocator(), null, .{}, "{}") == null);
    try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
    const ordinary = (try bindSchemaAlloc(&server, a, null, .{}, "{\"description\":\"sql_enum\\u0020literal\",\"named_types\":{\"forged\":true}} ")).?;
    defer a.free(ordinary);
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "named_types") == null);
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "sql_enum literal") != null);
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    const input =
        \\{"storage_mode":"relational","default_type":"row","named_types":{"forged":true},"document_schemas":{"row":{"schema":{"type":"object","additionalProperties":false,"properties":{"a":{"type":"sql_enum","x-antfly-sql-type-id":7},"b":{"type":"sql_enum","x-antfly-sql-type-id":7}}}}}}
    ;
    try std.testing.expectError(error.Forbidden, bindSchemaAlloc(&server, a, null, .{}, input));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
    var permission = try users.Permission.initOwned(a, .namespace, "default.public", .read);
    defer permission.deinit(a);
    var grants = [_]users.Permission{permission};
    const actor: http.AuthenticatedIdentity = .{ .username = @constCast("reader"), .permissions = &grants };
    const prepared = (try bindSchemaAlloc(&server, a, actor, .{}, input)).?;
    defer a.free(prepared);
    var compiled = try local.schema_mod.PreparedSchemaDefinition.init(a, prepared);
    defer compiled.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), compiled.runtime_schema.named_types.len);
    try std.testing.expectEqual(@as(usize, 2), compiled.runtime_schema.relational_columns.len);
    try std.testing.expectEqualDeep(state.record.identity, compiled.runtime_schema.named_types[0].identity);
    // Both columns resolved through one read cut and one shared executable view.
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    // Durable descriptors retain only witnesses. Reopening must authorize the
    // current catalog and rebuild one shared executable directory per type.
    {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const arena = region.allocator();
        const raw = try std.json.Stringify.valueAlloc(arena, .{local.sql_scalar.Type{ .kind = .enumeration, .enum_reference = &.{ .id = state.record.identity.id, .origin_id = state.record.identity.origin_id, .generation = state.record.identity.generation } }}, .{});
        const descriptors = try std.json.parseFromSliceLeaky([]const local.sql_scalar.Type, arena, raw, .{});
        try std.testing.expect(descriptors[0].enum_view == null);
        try std.testing.expectError(error.Forbidden, parameterDescriptorsLeaky(&server, arena, null, .{}, descriptors, &.{}));
        const rebound = try parameterDescriptorsLeaky(&server, arena, actor, .{}, descriptors, &.{});
        try std.testing.expect(rebound[0].enum_reference == null);
        try std.testing.expectEqualStrings("z", (try rebound[0].enum_view.?.input("z")).text);
        const declared = try parameterDescriptorsLeaky(&server, arena, actor, .{}, &.{ .{}, .{} }, &.{ state.record.identity.oid, state.record.identity.oid });
        try std.testing.expect(declared[0].enum_view == declared[1].enum_view);
        const forged: local.sql_scalar.EnumReference = .{ .id = state.record.identity.id, .origin_id = @splat(4), .generation = state.record.identity.generation };
        try std.testing.expectError(error.CatalogGenerationChanged, parameterDescriptorsLeaky(&server, arena, actor, .{}, &.{.{ .kind = .enumeration, .enum_reference = &forged }}, &.{}));
        state.namespace = "renamed";
        try std.testing.expectError(error.Forbidden, parameterDescriptorsLeaky(&server, arena, actor, .{}, descriptors, &.{}));
        state.namespace = "public";
    }
    state.namespace = "renamed";
    try std.testing.expectError(error.Forbidden, bindSchemaAlloc(&server, a, actor, .{}, input));
    state.namespace = "public";
    const create_body = try std.fmt.allocPrint(a, "{{\"schema\":{s},\"indexes\":{{}}}}", .{input});
    defer a.free(create_body);
    const bound_body = (try bindCreateBodyAlloc(&server, a, actor, .{}, create_body)).?;
    defer a.free(bound_body);
    var create = try @import("table_contract.zig").parseCreateTableRequest(a, bound_body);
    defer create.deinit(a);
    try std.testing.expect(create.schema_json != null);
    {
        const original = state.record.definition;
        defer state.record.definition = original;
        state.record.definition = .{ .domain = .{ .base = .{ .builtin = .text }, .not_null = true } };
        const domain_input = try std.mem.replaceOwned(u8, a, input, "sql_enum", "text");
        defer a.free(domain_input);
        const domain_schema = (try bindSchemaAlloc(&server, a, actor, .{}, domain_input)).?;
        defer a.free(domain_schema);
        var domain_validator = try local.schema_mod.CompiledTableValidator.init(a, domain_schema);
        defer domain_validator.deinit(a);
        try std.testing.expect(domain_validator.execution.domains != null);
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const arena = region.allocator();
        const declared = try parameterDescriptorsLeaky(&server, arena, actor, .{}, &.{ .{}, .{} }, &.{ state.record.identity.oid, state.record.identity.oid });
        try std.testing.expect(declared[0].domainView() != null);
        try std.testing.expect(declared[0].domainView() == declared[1].domainView());
        try std.testing.expectError(error.SqlNotNullViolation, local.sql_parameter_frame.Frame.prepare(a, declared, &.{ .{ .datum = .{} }, .{ .datum = .{} } }, .{}));
        const bytes = try std.json.Stringify.valueAlloc(arena, declared, .{});
        const references = try std.json.parseFromSliceLeaky([]const local.sql_scalar.Type, arena, bytes, .{});
        try std.testing.expect(references[0].domainView() == null);
        try std.testing.expect(references[0].domainReference() != null);
        const rebound = try parameterDescriptorsLeaky(&server, arena, actor, .{}, references, &.{});
        try std.testing.expectEqual(state.record.identity.id, rebound[0].domainView().?.identity.?.id);
        state.record.identity.generation += 1;
        defer state.record.identity.generation -= 1;
        try std.testing.expectError(error.CatalogGenerationChanged, parameterDescriptorsLeaky(&server, arena, actor, .{}, references, &.{}));
    }
    const Check = struct {
        fn run(alloc: A, api: *http.ApiHttpServer, identity: http.AuthenticatedIdentity, schema: []const u8) !void {
            const result = (try bindSchemaAlloc(api, alloc, identity, .{}, schema)).?;
            defer alloc.free(result);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ &server, actor, input });
}

/// One request-owned catalog cut binds all declared columns. The directory and
/// executable views are deduplicated by immutable identity; ordinary builtin
/// declarations never allocate or make a type lookup.
pub fn bindDeclaredColumns(raw: *anyopaque, a: A, create: local.sql_ast.CreateTable) !local.sql_ast.CreateTable {
    const execution = @import("sql_execution.zig");
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    try root.context.ensureActive();
    if (!root.server.source.vtable.supports_sql_types) return error.UnsupportedSqlExecution;
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    var names: std.ArrayList([]const local.system_catalog_domain.Target) = .empty;
    const ordinals = try a.alloc(?usize, create.columns.len);
    @memset(ordinals, null);
    var keys: std.StringHashMapUnmanaged(usize) = .empty;
    for (create.columns, ordinals) |column, *ordinal| if (column.named_type) |intent| {
        const path = @import("sql_routine_execution.zig").targets(root, a, intent) catch |err| switch (err) {
            error.SqlUndefinedFunction => return error.SqlNamedTypeNotFound,
            else => return err,
        };
        const key = try std.json.Stringify.valueAlloc(a, path, .{});
        const slot = try keys.getOrPut(a, key);
        if (!slot.found_existing) {
            slot.value_ptr.* = names.items.len;
            try names.append(a, path);
        }
        ordinal.* = slot.value_ptr.*;
    };
    if (names.items.len == 0) return create;
    const request: types.BindingsRequest = .{ .names = names.items, .expected_revision = root.revision };
    try request.validate();
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_type_bindings = request });
    const cut = try std.json.parseFromSliceLeaky(types.BindingsSnapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(a, request);
    root.revision = cut.revision;
    const records = try a.alloc(types.records.Record, cut.directory.len);
    for (cut.directory, records) |entry, *record| {
        if (!try auth.namespaceUsage(root.identity.*, a, entry.target)) return error.Forbidden;
        record.* = entry.record;
    }
    const registry = try a.create(local.sql_catalog.domain_values.Registry);
    registry.* = try local.sql_catalog.domain_values.Registry.init(a, records);
    var result = create;
    const columns = try a.dupe(local.sql_ast.Column, create.columns);
    for (columns, ordinals) |*column, ordinal| if (ordinal) |position| {
        const index = cut.named[position] orelse return error.SqlNamedTypeNotFound;
        const entry = registry.byId(cut.directory[index].record.identity.id) orelse return error.InvalidSqlBackendResponse;
        switch (entry.executable.?) {
            .enumeration => |*view| column.enum_view = view,
            .domain => |*view| {
                const physical = try local.sql_scalar.domainType(view);
                column.domain_view = view;
                column.type = physical.kind orelse return error.InvalidSqlBackendResponse;
                column.element_type = physical.element_type;
                column.numeric_modifier = physical.numeric_modifier;
                column.enum_view = physical.enum_view;
            },
        }
    };
    result.columns = columns;
    return result;
}

/// Type lookup is preparation work, shared by the statement invocation. Prepared
/// resources carry a durable identity witness; subsequent execution never
/// resolves the old spelling to a new object or trusts client-supplied OIDs.
pub fn bindCast(raw: *anyopaque, a: A, intent: local.sql_ast.Name) !local.sql_scalar.Type {
    const root: *@import("sql_execution.zig").Adapter = @ptrCast(@alignCast(raw));
    try root.context.ensureActive();
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    if (builtinNamedType(intent)) |builtin| return builtin;
    const lookup = try std.json.Stringify.valueAlloc(a, intent, .{});
    var pinned: ?@import("sql_prepared.zig").Binding = null;
    if (root.prepared_bindings) |bindings| {
        for (bindings) |binding| {
            if (binding.kind != .named_type) continue;
            const witness = binding.named_type orelse return error.InvalidSqlBackendResponse;
            if (!std.mem.eql(u8, witness.lookup, lookup)) continue;
            if (pinned != null) return error.InvalidSqlBackendResponse;
            pinned = binding;
        }
        if (pinned == null) return error.CatalogGenerationChanged;
    }
    const request: types.SnapshotRequest = if (pinned) |binding| .{
        .targets = &.{},
        .reference = .{ .id = binding.id, .namespace_id = binding.named_type.?.namespace_id },
        .expected_revision = root.revision,
    } else .{
        .targets = @import("sql_routine_execution.zig").targets(root, a, intent) catch |err| switch (err) {
            error.SqlUndefinedFunction => return error.SqlNamedTypeNotFound,
            else => return err,
        },
        .expected_revision = root.revision,
    };
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_type_snapshot = request });
    const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(a, request);
    const found = cut.found orelse return error.SqlNamedTypeNotFound;
    const scope = cut.scopes[found.scope];
    if (!try auth.namespaceUsage(root.identity.*, a, scope.target)) return error.Forbidden;
    if (pinned) |binding| {
        const witness = binding.named_type.?;
        if (binding.id != found.record.identity.id or binding.schema_version > found.record.identity.generation or
            !std.mem.eql(u8, &witness.incarnation, &cut.incarnation) or
            !std.mem.eql(u8, &witness.origin_id, &found.record.identity.origin_id)) return error.CatalogGenerationChanged;
    }
    root.revision = cut.revision;
    if (root.collect_prepared_bindings) |bindings| try bindings.append(a, .{
        .kind = .named_type,
        .named_type = .{ .lookup = lookup, .incarnation = cut.incarnation, .namespace_id = found.record.identity.namespace_id, .origin_id = found.record.identity.origin_id },
        .database = scope.target.database,
        .namespace = scope.target.namespace,
        .name = scope.target.table,
        .physical_name = "",
        .id = found.record.identity.id,
        .schema_version = found.record.identity.generation,
        .revision = cut.revision,
    });
    if (found.record.definition == .domain) {
        if (pinned) |binding| if (binding.schema_version != found.record.identity.generation) return error.CatalogGenerationChanged;
        const registry = try a.create(local.sql_catalog.domain_values.Registry);
        registry.* = try local.sql_catalog.domain_values.Registry.init(a, &.{found.record});
        return local.sql_scalar.domainType(&registry.byId(found.record.identity.id).?.executable.?.domain);
    }
    const view = try a.create(local.sql_catalog.enum_values.View);
    view.* = try local.sql_catalog.enum_values.View.init(a, found.record);
    return .{ .kind = .enumeration, .enum_view = view };
}

/// Quoting selects PostgreSQL's actual catalog name, not SQL alias syntax.
/// Unquoted aliases continue through the parser's ordinary builtin path.
fn builtinNamedType(intent: local.sql_ast.Name) ?local.sql_scalar.Type {
    if (intent.database != null or (intent.namespace != null and !std.mem.eql(u8, intent.namespace.?, "pg_catalog"))) return null;
    const scalar = local.sql_scalar;
    const pairs = .{
        .{ "text", scalar.Type{ .kind = .string, .element_type = .text } },
        .{ "uuid", scalar.Type{ .kind = .uuid, .element_type = .uuid } },
        .{ "int2", scalar.Type{ .kind = .integer, .element_type = .int16 } },
        .{ "int4", scalar.Type{ .kind = .integer, .element_type = .int32 } },
        .{ "int8", scalar.Type{ .kind = .integer, .element_type = .int64 } },
        .{ "float4", scalar.Type{ .kind = .number, .element_type = .float32 } },
        .{ "float8", scalar.Type{ .kind = .number, .element_type = .float64 } },
        .{ "numeric", scalar.Type{ .kind = .number, .element_type = .numeric } },
        .{ "bool", scalar.Type{ .kind = .boolean, .element_type = .boolean } },
        .{ "jsonb", scalar.Type{ .kind = .json, .element_type = .jsonb } },
        .{ "json", scalar.Type{ .kind = .json } },
        .{ "date", scalar.Type{ .kind = .datetime, .temporal_kind = .date } },
        .{ "timestamp", scalar.Type{ .kind = .datetime, .temporal_kind = .timestamp } },
        .{ "timestamptz", scalar.Type{ .kind = .datetime, .temporal_kind = .timestamp_tz } },
        .{ "time", scalar.Type{ .kind = .datetime, .temporal_kind = .time } },
        .{ "timetz", scalar.Type{ .kind = .datetime, .temporal_kind = .time_tz } },
        .{ "interval", scalar.Type{ .kind = .datetime, .temporal_kind = .interval } },
        .{ "numrange", scalar.Type{ .kind = .numrange } },
        .{ "daterange", scalar.Type{ .kind = .daterange } },
        .{ "tsrange", scalar.Type{ .kind = .tsrange } },
        .{ "tstzrange", scalar.Type{ .kind = .tstzrange } },
    };
    inline for (pairs) |pair| if (std.mem.eql(u8, intent.table, pair[0])) return pair[1];
    return null;
}
