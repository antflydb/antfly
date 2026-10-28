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
    try local.sql_catalog.named_layouts.canonicalizeSchemaLeaky(a, root, records);
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
    const Check = struct {
        fn run(alloc: A, api: *http.ApiHttpServer, identity: http.AuthenticatedIdentity, schema: []const u8) !void {
            const result = (try bindSchemaAlloc(api, alloc, identity, .{}, schema)).?;
            defer alloc.free(result);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ &server, actor, input });
}
