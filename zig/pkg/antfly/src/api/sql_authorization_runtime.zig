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

//! Statement-owned SQL rights layered over native credential caps and filters.
const std = @import("std");
const local = @import("antfly_local_sources");
const sql = local.system_catalog_privileges;
const catalog = local.sql_catalog;
const http = @import("http_server.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const A = std.mem.Allocator;

pub fn principal(root: *Adapter) !?sql.Principal {
    if (!root.server.cfg.auth_enabled or !root.server.source.vtable.supports_sql_authorization) return null;
    const identity = root.identity.* orelse return null;
    if (identity.is_internal_service) return null;
    const manager = identity.live_user_manager orelse return null;
    const instance = identity.user_instance_id orelse return null;
    if (!try manager.sqlCredentialPermits(identity.username, instance, identity.credential_principal, "", 0)) return error.Forbidden;
    return .{ .kind = .user, .instance = instance };
}

pub fn admittedNativeRights(identity: ?http.AuthenticatedIdentity, logical: []const u8) u16 {
    const user = identity orelse return 255;
    if (http.permissionsAllow(user.permissions, .table, logical, .admin)) return 255;
    var mask: u16 = 0;
    if (http.permissionsAllow(user.permissions, .table, logical, .read)) mask |= sql.Access.bit(.select);
    if (http.permissionsAllow(user.permissions, .table, logical, .write)) mask |= sql.Access.bit(.insert) | sql.Access.bit(.update) | sql.Access.bit(.delete);
    return mask;
}

pub fn required(table: catalog.Table) u16 {
    if (table.sql_required_privileges != 0) return table.sql_required_privileges;
    return switch (table.row_policy_purpose) {
        .update => sql.Access.bit(.update),
        .update_select => sql.Access.bit(.update) | sql.Access.bit(.select),
        .delete => sql.Access.bit(.delete),
        .delete_select => sql.Access.bit(.delete) | sql.Access.bit(.select),
        else => sql.Access.bit(.select),
    };
}

pub fn authorize(root: *Adapter, a: A, table: catalog.Table) !void {
    if (table.read_authority != null) return @import("sql_read_authority.zig").check(table);
    const captured = table.sql_access orelse return;
    const bound = root.sql_authority orelse return error.InvalidSqlBackendResponse;
    const identity = root.identity.* orelse return error.Forbidden;
    const manager = identity.live_user_manager orelse return error.Forbidden;
    const instance = identity.user_instance_id orelse return error.Forbidden;
    if (!bound.principal.eql(.{ .kind = .user, .instance = instance })) return error.Forbidden;
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    const logical = try (local.system_catalog_domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(a);
    defer a.free(logical);
    const needed = required(table);
    var native: u16 = 0;
    if (table.sql_native_rights != 0) {
        if (try http.tablePermissionCurrentlyAllowed(identity, logical, .admin)) {
            native = table.sql_native_rights;
        } else {
            if (needed & sql.Access.bit(.select) != 0 and try http.tablePermissionCurrentlyAllowed(identity, logical, .read)) native |= sql.Access.bit(.select);
            const writes = sql.Access.bit(.insert) | sql.Access.bit(.update) | sql.Access.bit(.delete);
            if (needed & writes != 0 and try http.tablePermissionCurrentlyAllowed(identity, logical, .write)) native |= writes;
            native &= table.sql_native_rights;
        }
    }
    if ((native | captured.rights) & needed != needed) return error.Forbidden;
    if (!try manager.sqlCredentialPermits(identity.username, instance, identity.credential_principal, logical, needed)) return error.Forbidden;
}

pub fn policyProof(root: *Adapter, a: A, table: catalog.Table) !?[]u8 {
    return policyProofForPurpose(root, a, table, table.row_policy_purpose);
}

fn policyProofForPurpose(root: *Adapter, a: A, table: catalog.Table, purpose: @import("../usermgr/mod.zig").row_policy_authority.Purpose) !?[]u8 {
    try authorize(root, a, table);
    const bound = root.sql_authority orelse return error.InvalidSqlBackendResponse;
    const identity = if (root.identity.*) |*value| value else return error.Forbidden;
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    const logical = try (local.system_catalog_domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(a);
    defer a.free(logical);
    return root.server.rowPolicySqlProof(a, identity, root.context, table.id, table.physical_name, scope.database, table.schema_version, purpose, .{ .logical_table = logical, .grant = .{ .principal_instance = bound.principal.instance, .required = required(table), .roles = bound.roles } });
}

/// Stack-owned capability borrowed only by synchronous native preparation.
/// Integrity expansion retains the ingress checks for other participants.
pub const WriteScope = struct {
    root: *Adapter,
    table: catalog.Table,
    logical: []const u8,
    allocator: A,
    integrity_reads: @import("sql_policy_integrity_reads.zig").Cache = undefined,
    integrity_initialized: bool = false,

    pub fn deinit(self: *WriteScope) void {
        if (self.integrity_initialized) self.integrity_reads.deinit();
        self.allocator.free(self.logical);
    }
    pub fn init(root: *Adapter, a: A, table: catalog.Table) !WriteScope {
        if (required(table) & (sql.Access.bit(.insert) | sql.Access.bit(.update) | sql.Access.bit(.delete)) == 0) return error.Forbidden;
        try authorize(root, a, table);
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        return .{ .root = root, .table = table, .allocator = a, .logical = try (local.system_catalog_domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(a) };
    }
    pub fn context(self: *WriteScope) local.api_operation.RequestContext {
        var request = self.root.context;
        request.table_write_authorization = .{ .ptr = self, .allows = allows };
        request.row_policy_proof_provider = .{ .ptr = self, .sign = sign };
        if (!self.integrity_initialized) {
            self.integrity_reads = .init(self.root.server, request);
            self.integrity_reads.known = .{ .ptr = self, .resolve = integrityBinding };
            self.integrity_initialized = true;
        }
        return self.integrity_reads.overlay(request);
    }
    fn integrityBinding(raw: *const anyopaque, physical: []const u8) !?@import("sql_policy_integrity_reads.zig").Cache.Binding {
        const self: *const WriteScope = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, physical, self.table.physical_name)) return null;
        return .{ .table_id = self.table.id, .database = (self.table.scope orelse return error.InvalidSqlBackendResponse).database, .schema_version = self.table.schema_version };
    }
    fn allows(raw: *const anyopaque, logical: []const u8) bool {
        const self: *const WriteScope = @ptrCast(@alignCast(raw));
        if (std.mem.eql(u8, self.logical, logical)) {
            authorize(self.root, self.allocator, self.table) catch return false;
            return true;
        }
        const prior = self.root.context.table_write_authorization orelse return false;
        return prior.allows(prior.ptr, logical);
    }
    fn sign(raw: *const anyopaque, a: A, table_id: u64, physical: []const u8, database: []const u8, schema_version: ?u32, purpose: @import("../usermgr/mod.zig").row_policy_authority.Purpose) !local.api_operation.RowPolicyProofProvider.Result {
        const self: *const WriteScope = @ptrCast(@alignCast(raw));
        if (table_id != self.table.id or !std.mem.eql(u8, physical, self.table.physical_name)) {
            if (self.root.context.row_policy_proof_provider) |prior| return prior.sign(prior.ptr, a, table_id, physical, database, schema_version, purpose);
            return .unhandled;
        }
        const scope = self.table.scope orelse return error.InvalidSqlBackendResponse;
        if (!std.mem.eql(u8, database, scope.database) or (schema_version != null and schema_version.? != self.table.schema_version)) return error.RowPolicyCatalogChanged;
        return .{ .handled = try policyProofForPurpose(self.root, a, self.table, if (purpose == .ordinary) .sql_write else purpose) };
    }
};

fn filterWitness(a: A, identity: ?http.AuthenticatedIdentity, logical: []const u8) ![32]u8 {
    const resolved = try http.resolveEffectiveRowFilterJson(a, identity, logical);
    defer if (resolved) |bytes| a.free(bytes);
    return @import("../usermgr/mod.zig").row_policy_authority.filterDigest(resolved);
}

pub fn transactionIntent(root: *Adapter, table: catalog.Table) !?@import("transactions.zig").TableCommitRequest.SqlAuthorization {
    if (table.sql_access == null or table.read_authority != null) return null;
    const bound = root.sql_authority orelse return error.InvalidSqlBackendResponse;
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    const a = root.server.alloc;
    const logical = try (local.system_catalog_domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(a);
    defer a.free(logical);
    const result: @import("transactions.zig").TableCommitRequest.SqlAuthorization = .{
        .row_filter_digest = try filterWitness(a, root.identity.*, logical),
        .principal_instance = bound.principal.instance,
        .table_id = table.id,
        .required = required(table),
        .native_rights = table.sql_native_rights,
    };
    try result.validate();
    return result;
}

pub fn retainObservedIntent(root: *Adapter, observed: *@import("transactions.zig").OwnedTransactionCommitRequest, table: catalog.Table) !void {
    if (table.sql_access == null or table.read_authority != null) return;
    const bound = root.sql_authority orelse return error.InvalidSqlBackendResponse;
    for (observed.tables) |*entry| if (std.mem.eql(u8, observed.physicalName(entry.table_name), table.physical_name)) {
        if (entry.sql_authorization) |*current| {
            // The request's identity is immutable. Reuse this table's filter
            // witness across scan pages instead of parsing/hashing it again.
            var next = current.*;
            next.principal_instance = bound.principal.instance;
            next.table_id = table.id;
            next.required = required(table);
            next.native_rights = table.sql_native_rights;
            try current.merge(next);
        } else entry.sql_authorization = try transactionIntent(root, table);
        return;
    };
    return error.InvalidSqlBackendResponse;
}

/// One current catalog cut supplies all staged SQL participants. Only private
/// statement intent can request SQL rights; public transaction bodies retain
/// their native read/write requirements.
pub const TransactionScope = struct {
    const Entry = struct { logical: []const u8, table: catalog.Table, intent: @import("transactions.zig").TableCommitRequest.SqlAuthorization };
    server: *http.ApiHttpServer,
    identity: ?http.AuthenticatedIdentity,
    base: local.api_operation.RequestContext,
    allocator: A,
    authority: sql.Authority,
    entries: []const Entry,
    integrity_reads: @import("sql_policy_integrity_reads.zig").Cache = undefined,
    integrity_initialized: bool = false,

    pub fn deinit(self: *TransactionScope) void {
        if (self.integrity_initialized) self.integrity_reads.deinit();
    }
    pub fn init(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, request_context: local.api_operation.RequestContext, a: A, request: @import("transactions.zig").OwnedTransactionCommitRequest) !?TransactionScope {
        const domain = local.system_catalog_domain;
        var count: usize = 0;
        for (request.tables) |table| if (table.sql_authorization != null) {
            count += 1;
        };
        if (count == 0) return null;
        const actor = identity orelse return error.Forbidden;
        if (!server.source.vtable.supports_sql_authorization) return error.UnsupportedSqlExecution;
        const instance = actor.user_instance_id orelse return error.Forbidden;
        const principal_value: sql.Principal = .{ .kind = .user, .instance = instance };
        const entries = try a.alloc(Entry, count);
        var captured: ?sql.Authority = null;
        var revision: ?u64 = null;
        var targets: std.ArrayList(domain.Target) = .empty;
        var staged: std.ArrayList(@import("transactions.zig").TableCommitRequest) = .empty;
        for (request.tables) |table| if (table.sql_authorization) |intent| {
            try intent.validate();
            if (!std.mem.eql(u8, &intent.principal_instance, &instance)) return error.Forbidden;
            const current_filter = try filterWitness(a, identity, table.table_name);
            if (!std.mem.eql(u8, &current_filter, &intent.row_filter_digest)) return error.Forbidden;
            try targets.append(a, try domain.Target.parse(table.table_name));
            try staged.append(a, table);
        };
        var offset: usize = 0;
        while (offset < count) {
            try request_context.ensureActive();
            const end = @min(count, offset + 256);
            const lookup: domain.ResolveMany = .{ .targets = targets.items[offset..end], .sql_principal = principal_value, .expected_revision = revision };
            var private = request_context;
            private.setting_admin = true;
            const bytes = try server.source.systemCatalog(a, private, .{ .resolve_many = lookup });
            const cut = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
            try cut.validateAuthority(a, lookup);
            try cut.validateRelations(lookup);
            if (cut.tables.len != end - offset) return error.InvalidSqlBackendResponse;
            revision = cut.revision;
            if (captured == null) captured = cut.sql_authority orelse return error.InvalidSqlBackendResponse;
            for (cut.tables, staged.items[offset..end], targets.items[offset..end], entries[offset..end]) |found, old, target, *entry| {
                const table = found orelse return error.CatalogGenerationChanged;
                const intent = old.sql_authorization.?;
                try table.requireAccess(request_context.native_service);
                if ((old.batch.writes.len != 0 or old.batch.deletes.len != 0 or old.batch.transforms.len != 0) and intent.required & (sql.Access.bit(.insert) | sql.Access.bit(.update) | sql.Access.bit(.delete)) == 0) return error.InvalidTransactionSessionRecord;
                if (table.table_id != intent.table_id or !std.mem.eql(u8, table.name, request.physicalName(old.table_name))) return error.CatalogGenerationChanged;
                entry.* = .{ .logical = old.table_name, .intent = intent, .table = .{
                    .id = table.table_id,
                    .physical_name = table.name,
                    .schema_version = old.relational_schema_version orelse old.schema_version orelse return error.InvalidTransactionSessionRecord,
                    .columns = &.{},
                    .scope = .{ .database = target.database, .namespace = target.namespace, .name = target.table, .revision = cut.revision },
                    .sql_access = table.sql_access,
                    .sql_native_rights = intent.native_rights,
                    .sql_required_privileges = intent.required,
                } };
            }
            offset = end;
        }
        const result: TransactionScope = .{ .server = server, .identity = identity, .base = request_context, .allocator = a, .authority = captured orelse return error.InvalidSqlBackendResponse, .entries = entries };
        for (result.entries) |entry| try result.authorizeEntry(entry);
        return result;
    }

    fn root(self: *const TransactionScope, identity: *?http.AuthenticatedIdentity) Adapter {
        return .{ .server = self.server, .identity = identity, .context = self.base, .sql_authority = self.authority };
    }
    fn authorizeEntry(self: *const TransactionScope, entry: Entry) !void {
        var identity = self.identity;
        var adapter = self.root(&identity);
        try authorize(&adapter, self.allocator, entry.table);
        if (entry.intent.native_read and !try http.tablePermissionCurrentlyAllowed(identity, entry.logical, .read)) return error.Forbidden;
        if (entry.intent.native_write and !try http.tablePermissionCurrentlyAllowed(identity, entry.logical, .write)) return error.Forbidden;
    }
    pub fn handles(self: *const TransactionScope, table: @import("transactions.zig").TableCommitRequest) bool {
        const intent = table.sql_authorization orelse return false;
        for (self.entries) |entry| if (std.mem.eql(u8, entry.logical, table.table_name)) {
            return std.meta.eql(entry.intent, intent);
        };
        return false;
    }
    pub fn context(self: *TransactionScope) local.api_operation.RequestContext {
        var request = self.base;
        request.table_write_authorization = .{ .ptr = self, .allows = allows };
        request.row_policy_proof_provider = .{ .ptr = self, .sign = sign };
        if (!self.integrity_initialized) {
            self.integrity_reads = .init(self.server, request);
            self.integrity_reads.known = .{ .ptr = self, .resolve = integrityBinding };
            self.integrity_initialized = true;
        }
        return self.integrity_reads.overlay(request);
    }
    fn integrityBinding(raw: *const anyopaque, physical: []const u8) !?@import("sql_policy_integrity_reads.zig").Cache.Binding {
        const self: *const TransactionScope = @ptrCast(@alignCast(raw));
        for (self.entries) |entry| if (std.mem.eql(u8, physical, entry.table.physical_name)) {
            return .{ .table_id = entry.table.id, .database = (entry.table.scope orelse return error.InvalidSqlBackendResponse).database, .schema_version = entry.table.schema_version };
        };
        return null;
    }
    fn allows(raw: *const anyopaque, logical: []const u8) bool {
        const self: *const TransactionScope = @ptrCast(@alignCast(raw));
        for (self.entries) |entry| if (std.mem.eql(u8, entry.logical, logical)) {
            if (entry.intent.required & (sql.Access.bit(.insert) | sql.Access.bit(.update) | sql.Access.bit(.delete)) == 0) return false;
            self.authorizeEntry(entry) catch return false;
            return true;
        };
        const prior = self.base.table_write_authorization orelse return false;
        return prior.allows(prior.ptr, logical);
    }
    fn sign(raw: *const anyopaque, a: A, table_id: u64, physical: []const u8, database: []const u8, schema_version: ?u32, purpose: @import("../usermgr/mod.zig").row_policy_authority.Purpose) !local.api_operation.RowPolicyProofProvider.Result {
        const self: *const TransactionScope = @ptrCast(@alignCast(raw));
        for (self.entries) |entry| if (entry.table.id == table_id and std.mem.eql(u8, physical, entry.table.physical_name)) {
            if (!std.mem.eql(u8, database, entry.table.scope.?.database) or (schema_version != null and schema_version.? != entry.table.schema_version)) return error.RowPolicyCatalogChanged;
            try self.authorizeEntry(entry);
            var identity = self.identity;
            var adapter = self.root(&identity);
            return .{ .handled = try policyProofForPurpose(&adapter, a, entry.table, if (purpose == .ordinary) .sql_write else purpose) };
        };
        if (self.base.row_policy_proof_provider) |prior| return prior.sign(prior.ptr, a, table_id, physical, database, schema_version, purpose);
        return .unhandled;
    }
};

test "SQL privileges commit cuts fence revocation credential caps incarnations and physical identity without native grants" {
    const users = @import("../usermgr/mod.zig");
    const sessions = @import("transactions.zig");
    const domain = local.system_catalog_domain;
    const Fake = struct {
        rights: u16 = sql.Access.bit(.insert),
        id: u64 = 9,
        calls: usize = 0,
        fn resolve(raw: *anyopaque, a: A, request: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .policy_publication_status) return a.dupe(u8, "null");
            if (call != .resolve_many) return error.UnexpectedCatalogCall;
            try std.testing.expect(request.setting_admin);
            const lookup = call.resolve_many;
            const actor = lookup.sql_principal orelse return error.UnexpectedCatalogCall;
            try std.testing.expectEqual(@as(usize, 1), lookup.targets.len);
            try std.testing.expectEqualStrings("docs", lookup.targets[0].table);
            self.calls += 1;
            return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 7, .tables = &.{.{ .table_id = self.id, .name = "table:9", .sql_access = .{ .rights = self.rights, .grant_options = 0 } }}, .sql_authority = .{ .principal = actor, .roles = &.{} } }, .{});
        }
    };
    const a = std.testing.allocator;
    var store = users.MemoryStore.init(a);
    defer store.deinit();
    var casbin = @import("antfly_casbin").MemoryAdapter.init(a);
    defer casbin.deinit();
    var manager = try users.UserManager.init(a, store.iface(), try users.initDefaultEnforcer(a, casbin.iface()));
    defer manager.deinit();
    var permission = try users.Permission.initOwned(a, .table, "docs", .read);
    defer permission.deinit(a);
    var user = try manager.createUser("writer", "secret", &.{permission});
    defer user.deinit(a);
    var key = try manager.createApiKey("writer", "reader-cap", &.{permission}, &.{}, null);
    defer key.deinit(a);
    const key_principal = try std.fmt.allocPrint(a, "api-key:{s}", .{key.key.key_id});
    defer a.free(key_principal);
    try manager.removePermissionFromUser("writer", "docs", .table);
    var identity = try http.cloneAuthenticatedIdentity(a, "writer", "basic:writer", &.{}, &.{}, "{}", &.{});
    defer identity.deinit(a);
    identity.live_user_manager = &manager;
    identity.user_instance_id = user.instance_id;
    var fake: Fake = .{};
    var server = http.ApiHttpServer.init(a, .{ .auth_enabled = true, .user_manager = &manager }, .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.resolve, .supports_sql_authorization = true } }, null, null);
    defer server.deinit();
    var tables = [_]sessions.TableCommitRequest{.{ .table_name = @constCast("docs"), .schema_version = 3, .sql_authorization = .{ .row_filter_digest = @splat(0), .principal_instance = user.instance_id, .table_id = 9, .required = sql.Access.bit(.insert), .native_rights = 0 } }};
    var bindings = [_]sessions.CatalogBinding{.{ .logical = "docs", .physical = "table:9" }};
    const request: sessions.OwnedTransactionCommitRequest = .{ .tables = &tables, .catalog_bindings = .{ .items = &bindings, .capacity = bindings.len, .pointer_stability = .{} } };
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    var admitted = (try TransactionScope.init(&server, identity, .{}, scratch, request)).?;
    defer admitted.deinit();
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expect(admitted.handles(tables[0]));
    const context = admitted.context();
    const writes = context.table_write_authorization.?;
    try std.testing.expect(writes.allows(writes.ptr, "docs"));
    try std.testing.expect(!writes.allows(writes.ptr, "other"));
    try std.testing.expect(!try http.tablePermissionCurrentlyAllowed(identity, "docs", .write));
    const provider = context.row_policy_proof_provider.?;
    try std.testing.expectEqual(.unhandled, try provider.sign(provider.ptr, scratch, 10, "table:10", "default", 3, .sql_write));
    try std.testing.expectError(error.RowPolicyCatalogChanged, provider.sign(provider.ptr, scratch, 9, "table:9", "default", 4, .sql_write));
    try std.testing.expect((try provider.sign(provider.ptr, scratch, 9, "table:9", "default", 3, .ordinary)).handled == null);
    fake.rights = 0;
    try std.testing.expectError(error.Forbidden, TransactionScope.init(&server, identity, .{}, scratch, request));
    fake.rights = sql.Access.bit(.insert);
    tables[0].sql_authorization.?.native_write = true;
    try std.testing.expectError(error.Forbidden, TransactionScope.init(&server, identity, .{}, scratch, request));
    tables[0].sql_authorization.?.native_write = false;
    fake.id = 10;
    try std.testing.expectError(error.CatalogGenerationChanged, TransactionScope.init(&server, identity, .{}, scratch, request));
    fake.id = 9;
    var capped = identity;
    capped.credential_principal = key_principal;
    try std.testing.expectError(error.Forbidden, TransactionScope.init(&server, capped, .{}, scratch, request));
    fake.rights = sql.Access.bit(.select);
    tables[0].sql_authorization.?.required = sql.Access.bit(.select);
    var read = (try TransactionScope.init(&server, capped, .{}, scratch, request)).?;
    defer read.deinit();
    const read_writes = read.context().table_write_authorization.?;
    try std.testing.expect(!read_writes.allows(read_writes.ptr, "docs"));
    try manager.deleteApiKey("writer", key.key.key_id);
    try std.testing.expectError(error.Forbidden, TransactionScope.init(&server, capped, .{}, scratch, request));
    tables[0].sql_authorization.?.principal_instance = @splat(123);
    try std.testing.expectError(error.Forbidden, TransactionScope.init(&server, identity, .{}, scratch, request));
}
