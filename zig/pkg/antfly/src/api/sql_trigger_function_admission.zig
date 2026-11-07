// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! The same destination function-use admission applies to CREATE TRIGGER and
//! staged restoration. An archived realm/name is never execution authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const routines = @import("../system_catalog/routines.zig");

pub fn authorize(a: std.mem.Allocator, auth_enabled: bool, identity: ?http.AuthenticatedIdentity, record: routines.records.Record, target: local.system_catalog_domain.Target) !void {
    if (record.kind != .function or !record.definition.returns_trigger or record.definition.arguments.len != 0) return error.InvalidSqlTriggerDefinition;
    if (auth_enabled and (identity == null or record.definition.owner == .auth_disabled)) return error.Forbidden;
    if (!try @import("sql_catalog_auth.zig").namespaceUsage(identity, a, target)) return error.Forbidden;
}
