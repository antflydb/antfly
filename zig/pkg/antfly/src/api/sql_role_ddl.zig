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

//! SQL role defaults patch the existing authoritative setting catalog. They
//! do not mutate a connection overlay or broaden its policy authority.
const std = @import("std");
const sources = @import("antfly_local_sources");
const http = @import("http_server.zig");
const catalog = sources.sql_catalog;
const settings = sources.system_catalog_settings;

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: sources.api_operation.RequestContext, allocators: catalog.DdlAllocators, ddl: sources.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    try context.ensureActive();
    if (ddl.kind != .role or ddl.action != .role_default) return error.UnsupportedSqlExecution;
    const change = ddl.role_setting orelse return error.InvalidSqlSyntax;
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    // Changing durable defaults is an administrator operation, not the
    // session-writable SET capability. Policy-sensitive defaults remain
    // capability-guarded by the authoritative catalog mutation.
    if (!try http.resourcePermissionCurrentlyAllowed(identity, .user, ddl.name.table, .admin)) return error.Forbidden;
    // Native control settings have their own typed protocol state. Never
    // accept a catalog-only default which that execution control ignores.
    if (!std.ascii.eqlIgnoreCase(change.name, "statement_timeout") and
        (std.mem.indexOfScalar(u8, change.name, '.') == null or std.ascii.startsWithIgnoreCase(change.name, "antfly."))) return error.UnsupportedSqlExecution;
    const manager = server.cfg.user_manager orelse return error.UnsupportedSqlExecution;
    const instance = manager.currentUserInstance(ddl.name.table) catch return error.SqlRoleNotFound;
    const value: ?settings.Value = if (change.value) |literal| switch (literal) {
        .string => |bytes| if (std.ascii.eqlIgnoreCase(change.name, "statement_timeout")) .{ .integer = @import("../pgwire/session_commands.zig").parseTimeoutText(allocators.scratch, bytes) catch |err| switch (err) {
            error.InvalidParameter, error.InvalidSqlSyntax => return error.InvalidSettingValue,
            else => return err,
        } } else .{ .string = bytes },
        .integer => |number| .{ .integer = number },
        .boolean => |boolean| .{ .boolean = boolean },
        else => return error.InvalidSettingValue,
    } else null;
    if (value) |configured| if (std.ascii.eqlIgnoreCase(change.name, "statement_timeout")) {
        if (configured != .integer or configured.integer < 0 or configured.integer > std.math.maxInt(i32)) return error.InvalidSettingValue;
    };
    var private = context;
    private.setting_admin = true;
    const response = server.source.systemCatalog(allocators.scratch, private, .{ .setting_mutate = .{ .role_default = .{ .name = change.name, .principal = ddl.name.table, .principal_instance = instance, .database = change.database orelse "", .value = value } } }) catch |err| {
        if (err == error.MetadataMutationOutcomeUnknown) return error.SqlMutationOutcomeUnknown;
        return err;
    };
    allocators.scratch.free(response);
    return .{ .mutation_outcome = .committed };
}
