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

//! Definition-only namespace usage shared by SQL catalog producers. This
//! grants neither source SELECT nor row-policy bypass.
const std = @import("std");
const http = @import("http_server.zig");
const domain = @import("antfly_local_sources").system_catalog_domain;

pub fn authorizeTarget(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, a: std.mem.Allocator, target: domain.Target) !void {
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    const resource = try target.resourceNameAlloc(a);
    if (!try http.tablePermissionCurrentlyAllowed(identity, resource, .admin)) return error.Forbidden;
}

pub fn namespaceUsage(identity: ?http.AuthenticatedIdentity, a: std.mem.Allocator, target: domain.Target) !bool {
    const actor = identity orelse return true;
    const table = try target.resourceNameAlloc(a);
    if (try http.tablePermissionCurrentlyAllowed(actor, table, .read) or
        try http.tablePermissionCurrentlyAllowed(actor, table, .write)) return true;
    const namespace = try @import("../system_catalog/routes.zig").resourceNameAlloc(a, .{ .kind = .namespace, .database = target.database, .name = target.namespace });
    if (!http.permissionsAllow(actor.permissions, .namespace, namespace, .read)) return false;
    const manager = actor.live_user_manager orelse return true;
    if (std.mem.startsWith(u8, actor.credential_principal, "api-key:")) {
        const current = manager.effectiveApiKeyPermissions(actor.credential_principal["api-key:".len..]) catch |err| switch (err) {
            error.ApiKeyNotFound, error.ApiKeyExpired, error.UserNotFound => return false,
            else => return err,
        };
        defer {
            for (current) |*permission| permission.deinit(manager.alloc);
            manager.alloc.free(current);
        }
        return http.permissionsAllow(current, .namespace, namespace, .read);
    }
    return manager.permissionCurrentlyAllowed(actor.username, .namespace, namespace, .read);
}
