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

//! Immutable serving-policy evaluation for unpublished SQL session images.
//! This uses the same typed policy evaluator as physical owners; it does not
//! reconstruct a policy as an Antfly filter or trust roles from request JSON.
const std = @import("std");
const local = @import("antfly_local_sources");
const policies = local.system_catalog_policies;
const settings = local.sql_setting_catalog;
const catalog = local.sql_catalog;
const authority = @import("../usermgr/mod.zig").row_policy_authority;
const http = @import("http_server.zig");
const operation = local.api_operation;

pub const Evaluation = struct {
    alloc: std.mem.Allocator,
    table: catalog.Table,
    principal: std.json.Parsed(authority.Payload),
    settings: settings.View,
    policies: policies.View,
    context: operation.RequestContext,
    cells: []local.sql_scalar.Datum,
    evaluators: [5]?policies.Evaluator = @splat(null),
    scratch: std.heap.ArenaAllocator,

    pub fn init(alloc: std.mem.Allocator, server: *http.ApiHttpServer, context: operation.RequestContext, table: catalog.Table, caller: []const u8, token: []const u8, overlay: []const settings.OverlayEntry) !*Evaluation {
        try context.ensureActive();
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        var authenticated = context;
        authenticated.setting_read_principal = caller;
        const bytes = try server.source.systemCatalog(alloc, authenticated, .{ .policy_snapshot = .{ .table_id = table.id, .principal = caller, .database = scope.database } });
        defer alloc.free(bytes);
        const Header = struct { schema_digest: [32]u8, policy_generation: u64, catalog_epoch: u64 };
        var header = try std.json.parseFromSlice(Header, alloc, bytes, .{ .ignore_unknown_fields = true });
        defer header.deinit();
        const self = try alloc.create(Evaluation);
        errdefer alloc.destroy(self);
        self.alloc = alloc;
        self.context = context;
        self.table = table;
        self.principal = try authority.verify(alloc, server.cfg.trusted_principal_secret orelse return error.RowPolicyAuthenticationRequired, server.cfg.trusted_principal_issuer orelse return error.RowPolicyAuthenticationRequired, .{
            .table_id = table.id,
            .table = table.physical_name,
            .database = scope.database,
            .policy_generation = header.value.policy_generation,
            .catalog_epoch = header.value.catalog_epoch,
        }, @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)), token);
        errdefer self.principal.deinit();
        if (!std.mem.eql(u8, self.principal.value.principal, caller)) return error.RowPolicyAuthenticationRequired;
        if (self.principal.value.purpose != table.row_policy_purpose) return error.RowPolicyAuthenticationRequired;
        const setting_principal = self.principal.value.setting_principal orelse caller;
        authenticated.setting_read_principal = setting_principal;
        var owner: SettingOwner = .{ .server = server, .context = authenticated };
        self.settings = try settings.View.capture(alloc, .{ .ptr = &owner, .load = SettingOwner.load }, .{ .principal = setting_principal, .database = scope.database }, overlay);
        errdefer self.settings.deinit();
        try self.settings.pinSqlUser(caller);
        var policy_owner: PolicyOwner = .{ .bytes = bytes, .principal = setting_principal };
        self.policies = try policies.View.capture(alloc, .{ .ptr = &policy_owner, .load = PolicyOwner.load }, table, header.value.schema_digest, &self.settings);
        errdefer self.policies.deinit();
        self.cells = try alloc.alloc(local.sql_scalar.Datum, table.columns.len);
        @memset(self.cells, .{});
        self.evaluators = @splat(null);
        self.scratch = .init(alloc);
        return self;
    }

    pub fn deinit(self: *Evaluation) void {
        for (&self.evaluators) |*value| if (value.*) |*evaluator| evaluator.deinit();
        self.scratch.deinit();
        self.alloc.free(self.cells);
        self.policies.deinit();
        self.settings.deinit();
        self.principal.deinit();
        self.alloc.destroy(self);
    }

    pub fn permits(self: *Evaluation, action: policies.Evaluator.Action, key: []const u8, value: std.json.Value, json_null_fields: []const []const u8) !bool {
        try self.context.ensureActive();
        const index = @backingInt(action);
        if (self.evaluators[index] == null)
            self.evaluators[index] = try policies.Evaluator.initForAuthenticatedPrincipal(self.alloc, &self.policies, &self.settings, action, self.principal.value.policy_principal orelse self.principal.value.principal, self.principal.value.roles);
        const evaluator = &self.evaluators[index].?;
        _ = self.scratch.reset(.retain_capacity);
        const a = self.scratch.allocator();
        // Ordinals are schema-bound, but each command reads only its own
        // dependency set. Reuse the directory and clear just populated slots;
        // never zero/allocate a wide schema for every staged row.
        const cells = self.cells;
        defer for (evaluator.required_columns) |ordinal| {
            cells[ordinal] = .{};
        };
        const row: catalog.Row = .{ .id = key, .version = 0, .value = value };
        for (evaluator.required_columns) |ordinal| {
            const column = self.table.columns[ordinal];
            var cell = try row.cell(column.path);
            if (column.type == .json and cell.value == .null) for (json_null_fields) |field| {
                if (std.mem.eql(u8, field, column.path)) {
                    cell.sql_null = false;
                    break;
                }
            };
            cells[ordinal] = try local.sql_document_row.declaredCell(a, column, cell);
        }
        return evaluator.permitsWithScratch(a, cells);
    }

    pub fn permitsScan(self: *Evaluation, key: []const u8, value: std.json.Value, null_fields: []const []const u8) !bool {
        const purpose = self.principal.value.purpose;
        const action: policies.Evaluator.Action = switch (purpose) {
            .ordinary => .select,
            .update, .update_select => .update_old,
            .delete, .delete_select => .delete,
            else => return error.RowPolicyAuthenticationRequired,
        };
        if (!try self.permits(action, key, value, null_fields)) return false;
        return if (purpose.requiresSelectPermission()) self.permits(.select, key, value, null_fields) else true;
    }

    pub fn overlayPolicy(self: *Evaluation) @import("sql_session_overlay.zig").Policy {
        return .{ .ptr = self, .permits = struct {
            fn run(ptr: *anyopaque, key: []const u8, value: std.json.Value, null_fields: []const []const u8) anyerror!bool {
                const evaluation: *Evaluation = @ptrCast(@alignCast(ptr));
                return evaluation.permitsScan(key, value, null_fields);
            }
        }.run };
    }
};

const SettingOwner = struct {
    server: *http.ApiHttpServer,
    context: operation.RequestContext,
    fn load(ptr: *anyopaque, alloc: std.mem.Allocator, scope: settings.Scope) !settings.RawSnapshot {
        const self: *SettingOwner = @ptrCast(@alignCast(ptr));
        const bytes = try self.server.source.systemCatalog(alloc, self.context, .{ .setting_snapshot = scope });
        defer alloc.free(bytes);
        const snapshot = try std.json.parseFromSliceLeaky(settings.RawSnapshot, alloc, bytes, .{ .allocate = .alloc_always });
        return http.authorizeSqlRoleDefaults(self.server, alloc, snapshot);
    }
};

const PolicyOwner = struct {
    bytes: []const u8,
    principal: []const u8,
    fn load(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !policies.Snapshot {
        const self: *PolicyOwner = @ptrCast(@alignCast(ptr));
        var value = try std.json.parseFromSliceLeaky(policies.Snapshot, alloc, self.bytes, .{ .allocate = .alloc_always });
        value.principal = try alloc.dupe(u8, self.principal);
        return value;
    }
};
