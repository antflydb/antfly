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

//! SQL role and database defaults patch the existing authoritative setting catalog. They
//! do not mutate a connection overlay or broaden its policy authority.
const std = @import("std");
const sources = @import("antfly_local_sources");
const http = @import("http_server.zig");
const catalog = sources.sql_catalog;
const settings = sources.system_catalog_settings;

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: sources.api_operation.RequestContext, allocators: catalog.DdlAllocators, ddl: sources.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    try context.ensureActive();
    if ((ddl.kind != .role and ddl.kind != .database) or ddl.action != .setting_default) return error.UnsupportedSqlExecution;
    const change = ddl.setting_default orelse return error.InvalidSqlSyntax;
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    // Changing durable defaults is an administrator operation, not the
    // session-writable SET capability. Policy-sensitive defaults remain
    // capability-guarded by the authoritative catalog mutation.
    if (!try http.resourcePermissionCurrentlyAllowed(identity, if (ddl.kind == .database) .database else .user, ddl.name.table, .admin)) return error.Forbidden;
    // Native control settings have their own typed protocol state. Never
    // accept a catalog-only default which that execution control ignores.
    if (!(ddl.kind == .database and change.all) and !std.ascii.eqlIgnoreCase(change.name, "timezone") and !std.ascii.eqlIgnoreCase(change.name, "statement_timeout") and
        (std.mem.indexOfScalar(u8, change.name, '.') == null or std.ascii.startsWithIgnoreCase(change.name, "antfly."))) return error.UnsupportedSqlExecution;
    var owned_literal: ?[]u8 = null;
    defer if (owned_literal) |bytes| allocators.scratch.free(bytes);
    const value: ?settings.Value = if (change.value) |literal| configured: {
        const text = switch (literal) {
            .string, .numeric => |bytes| bytes,
            .integer => |number| formatted: {
                owned_literal = try std.fmt.allocPrint(allocators.scratch, "{d}", .{number});
                break :formatted owned_literal.?;
            },
            .boolean => |boolean| if (boolean) "true" else "false",
            else => return error.InvalidSettingValue,
        };
        if (std.ascii.eqlIgnoreCase(change.name, "statement_timeout")) {
            const timeout = @import("../pgwire/session_commands.zig").parseTimeoutText(allocators.scratch, text) catch |err| switch (err) {
                error.InvalidParameter, error.InvalidSqlSyntax => return error.InvalidSettingValue,
                else => return err,
            };
            if (timeout > std.math.maxInt(i32)) return error.InvalidSettingValue;
            break :configured .{ .integer = timeout };
        }
        break :configured .{ .string = if (std.ascii.eqlIgnoreCase(change.name, "timezone") and std.ascii.eqlIgnoreCase(text, "UTC")) "UTC" else text };
    } else null;
    var private = context;
    private.setting_admin = true;
    const fence: ?settings.CommandFence = if (context.sql_command_fence) |bound| bound.* else null;
    const mutation: settings.Request = if (ddl.kind == .database)
        .{ .database_default = .{ .name = change.name, .all = change.all, .database = ddl.name.table, .value = value, .command_fence = fence } }
    else role: {
        const manager = server.cfg.user_manager orelse return error.UnsupportedSqlExecution;
        const instance = manager.currentUserInstance(ddl.name.table) catch return error.SqlRoleNotFound;
        break :role .{ .role_default = .{ .name = change.name, .principal = ddl.name.table, .principal_instance = instance, .database = change.database orelse "", .value = value, .command_fence = fence } };
    };
    const response = server.source.systemCatalog(allocators.scratch, private, .{ .setting_mutate = mutation }) catch |err| {
        if (err == error.MetadataMutationOutcomeUnknown) return error.SqlMutationOutcomeUnknown;
        return err;
    };
    allocators.scratch.free(response);
    return .{ .mutation_outcome = .committed };
}
