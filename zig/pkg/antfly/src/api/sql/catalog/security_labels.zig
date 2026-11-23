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

const std = @import("std");
const local = @import("antfly_local_sources");
const ast = local.sql_ast;
const catalog = local.sql_catalog;
const native = @import("../../../system_catalog/security_labels.zig");
const http = @import("../../http_server.zig");
const Context = local.api_operation.RequestContext;
const SearchPath = @import("../../../pgwire/search_path.zig").Path;

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl, path: ?*const SearchPath) !catalog.DdlOutcome {
    try context.ensureActive();
    if (ddl.action != .security_label or ddl.conditional or ddl.cascade) return error.InvalidSqlSyntax;
    const syntax = ddl.security_label orelse return error.InvalidSqlSyntax;
    // PG resolves the provider before target lookup and ownership. The trusted
    // compiled registry supplies executable policy; SQL packages cannot add it.
    if (!std.mem.eql(u8, syntax.provider orelse "", "selinux") or context.sql_selinux_ingress == null) {
        _ = try native.labels.nativeRegistry().select(syntax.provider);
    }
    const class: native.labels.Class = switch (ddl.kind) {
        .table => .table,
        .namespace => .namespace,
        .procedure => .procedure,
        else => return error.UnsupportedSqlExecution,
    };
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const signature = if (ddl.routine_signature) |types| signature: {
        const arguments = try a.alloc(catalog.routines.records.Argument, types.len);
        for (types, arguments) |kind, *argument| argument.* = .{ .type = try catalog.routines.storageType(kind) };
        const definition: catalog.routines.records.Definition = .{ .arguments = arguments, .result = .{ .scalar = .text }, .body = "SELECT NULL", .owner = .auth_disabled };
        break :signature try definition.signatureAlloc(a);
    } else null;
    var read_revision: ?u64 = null;
    var expected_table_id: ?u64 = null;
    var target: local.system_catalog_domain.Target = .{ .database = ddl.name.database orelse database, .namespace = ddl.name.namespace orelse namespace, .table = ddl.name.table };
    // Use the same captured search path and authorized identity selection as
    // ordinary routines/tables. A read-only snapshot cannot confer mutation.
    if (class != .namespace and ddl.name.namespace == null and ddl.name.database == null) if (path) |captured| {
        if (captured.len == 0) return if (class == .procedure) error.SqlUndefinedProcedure else error.TableNotFound;
        const scopes = try a.alloc(local.system_catalog_domain.Target, captured.len);
        for (scopes, 0..) |*scope, i| scope.* = .{ .database = database, .namespace = captured.entries[i].slice(), .table = ddl.name.table };
        var private = context;
        private.setting_admin = true;
        private.sql_selinux_ingress = null;
        if (class == .procedure) {
            const bytes = try server.source.systemCatalog(a, private, .{ .sql_routine_snapshot = .{ .targets = scopes, .signature = signature } });
            const snapshot = try std.json.parseFromSliceLeaky(@import("../../../system_catalog/routines.zig").Snapshot, a, bytes, .{});
            try snapshot.validate(.{ .targets = scopes, .signature = signature });
            read_revision = snapshot.revision;
            target = snapshot.scopes[(snapshot.found orelse return error.SqlUndefinedProcedure).scope].target;
        } else {
            const relations = try a.alloc(local.system_catalog_domain.RelationTarget, scopes.len);
            for (scopes, relations) |scope, *relation| relation.* = .{ .database = scope.database, .namespace = scope.namespace, .name = scope.table };
            const bytes = try server.source.systemCatalog(a, private, .{ .resolve_many = .{ .relations = relations } });
            const resolved = try std.json.parseFromSliceLeaky(local.system_catalog_domain.ResolvedMany, a, bytes, .{});
            try resolved.validateRelations(.{ .relations = relations });
            read_revision = resolved.revision;
            var selected = false;
            for (resolved.relations, 0..) |found, i| if (found != null) {
                target = scopes[i];
                expected_table_id = found.?.owner.table_id;
                selected = true;
                break;
            };
            if (!selected) return error.TableNotFound;
        }
    };
    if (class == .table and read_revision == null) {
        var private = context;
        private.setting_admin = true;
        private.sql_selinux_ingress = null;
        const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .database = target.database, .namespace = target.namespace, .name = target.table }} };
        const bytes = try server.source.systemCatalog(a, private, .{ .resolve_many = lookup });
        const resolved = try std.json.parseFromSliceLeaky(local.system_catalog_domain.ResolvedMany, a, bytes, .{});
        try resolved.validateRelations(lookup);
        const found = resolved.relations[0] orelse return error.TableNotFound;
        read_revision = resolved.revision;
        expected_table_id = found.owner.table_id;
    }
    const actor = try @import("../recovery/authorization_recovery.zig").actor(server, identity);
    const intent: native.Intent = .{ .target = target, .class = class, .signature = signature, .read_revision = read_revision, .expected_table_id = expected_table_id, .provider = syntax.provider, .label = syntax.label, .actor = actor, .native_administrator = true };
    // The existing native scoped-admin authority includes API-key attenuation,
    // revocation and owner override. It is repeated after preparation and on
    // receipt recovery; the receipt itself grants no mutation authority.
    try @import("ordinary_catalog.zig").authorize(server, identity, a, .{ .kind = intent.mutation().kind, .action = .alter, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = 0 });
    var private = context;
    private.setting_admin = true;
    var reply = try @import("ordinary_catalog.zig").submitSecurityLabel(server, identity, private, allocators, intent);
    defer reply.deinit(allocators.result);
    return .{ .mutation_outcome = if (reply.unknown) null else .committed, .catalog_receipt = reply.takeReceipt() };
}
