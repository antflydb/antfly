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

//! PostgreSQL originals exercised through HTTP, native owners, and cold reopen.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("http_server.zig");
const transport = @import("../raft/transport/http_common.zig");
const wire = @import("antfly_metadata_openapi").types;
const graph = local.system_catalog_privileges;
const domain = local.system_catalog_domain;
const IdentityReference = struct {
    after_update_ids: []const i64,
    after_update_counter: i64,
    before_restart_ids: []const i64,
    before_restart_counter: i64,
    recovered_counter: i64,
    after_restart_ids: []const i64,
    after_restart_counter: i64,
};
const Entry = struct { id: []const u8, sql: []const u8, command_tag: ?[]const u8 = null, sqlstate: ?[]const u8 = null, postimage: std.json.Value };

fn send(ctx: *fixture.Context, headers: []const transport.RequestHeader, statement: []const u8) !transport.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = headers;
    return client.execute(&authenticated, statement, .write_admission);
}
fn ok(ctx: *fixture.Context, statement: []const u8) !void {
    var response = try send(ctx, ctx.admin_headers, statement);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("authorization SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
}
fn denied(ctx: *fixture.Context, headers: []const transport.RequestHeader, statement: []const u8, code: []const u8) !void {
    var response = try send(ctx, headers, statement);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("authorization expected denial SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status != 200);
    var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
}
fn readerRows(ctx: *fixture.Context, statement: []const u8, expected: []const i64) !void {
    return rowsWithHeaders(ctx, ctx.reader_headers, statement, expected);
}
fn rowsWithHeaders(ctx: *fixture.Context, headers: []const transport.RequestHeader, statement: []const u8, expected: []const i64) !void {
    var response = try send(ctx, headers, statement);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("authorization read SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    for (expected, parsed.value.rows) |value, row| {
        const actual = switch (row[0]) {
            .integer => |number| number,
            .string, .number_string => |number| try std.fmt.parseInt(i64, number, 10),
            else => return error.InvalidSqlBackendResponse,
        };
        try std.testing.expectEqual(value, actual);
    }
}

fn grantedWrite(ctx: *fixture.Context, headers: []const transport.RequestHeader, statement: []const u8) !void {
    var response = try send(ctx, headers, statement);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("identity SQL grant status={d} SQL={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
}

fn identityCounter(ctx: *fixture.Context) !i64 {
    const sequences = @import("../system_catalog/sequence_catalog.zig");
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const request: sequences.SnapshotRequest = .{ .targets = &.{.{ .table = "identity_acl_probe_id_seq" }}, .include_counters = true };
    const bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_sequence_snapshot = request });
    defer ctx.alloc.free(bytes);
    var cut = try std.json.parseFromSlice(sequences.Snapshot, ctx.alloc, bytes, .{});
    defer cut.deinit();
    try cut.value.validate(request);
    return (cut.value.entries[0].counter orelse return error.TestMissingIdentityCounter).last_value;
}

fn identityGrantBeforeRestart(ctx: *fixture.Context, headers: []const transport.RequestHeader, reference: IdentityReference) !void {
    // Match the oracle's public-schema USAGE precondition. This grants no
    // native table read/write or sequence authority to the test credential.
    const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
    var namespace = try @import("../usermgr/mod.zig").Permission.initOwned(manager.alloc, .namespace, "default.public", .read);
    defer namespace.deinit(manager.alloc);
    try manager.addPermissionToUser("pk-reader", namespace);
    try ok(ctx, "CREATE ROLE identity_writer");
    try ok(ctx, "CREATE TABLE identity_acl_probe (id BIGINT GENERATED ALWAYS AS IDENTITY, amount BIGINT)");
    const table_id = (try fixture.waitServingSchema(ctx, "identity_acl_probe", 0)).table_id;
    try ok(ctx, "GRANT INSERT, UPDATE ON identity_acl_probe TO identity_writer");
    try ok(ctx, "GRANT identity_writer TO \"pk-reader\"");
    // The credential has no native table grants and no SQL SELECT or sequence
    // grant. Implicit identity needs the actual INSERT/UPDATE grant only.
    try grantedWrite(ctx, headers, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1");
    try grantedWrite(ctx, headers, "UPDATE identity_acl_probe SET id=DEFAULT RETURNING 1");
    try std.testing.expectEqual(reference.after_update_counter, try identityCounter(ctx));
    try rowsWithHeaders(ctx, ctx.admin_headers, "SELECT id FROM identity_acl_probe ORDER BY id", reference.after_update_ids);
    try denied(ctx, headers, "SELECT nextval('identity_acl_probe_id_seq')", "42501");
    try denied(ctx, headers, "SELECT id FROM identity_acl_probe", "42501");
    // Native RLS also filters administrative observers. Authorize the
    // independent observer explicitly; the mutation credential keeps only
    // INSERT/UPDATE and still cannot SELECT the stored rows.
    try ok(ctx, "GRANT identity_writer TO \"pk-rewriter\"");
    try ok(ctx, "CREATE POLICY identity_acl_write ON identity_acl_probe TO identity_writer USING (amount=10) WITH CHECK (amount=10)");
    try ok(ctx, "ALTER TABLE identity_acl_probe ENABLE ROW LEVEL SECURITY");
    _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, table_id, .active, 0);
    try denied(ctx, headers, "INSERT INTO identity_acl_probe(amount) VALUES(20) RETURNING 1", "42501");
    try grantedWrite(ctx, headers, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1");
    try rowsWithHeaders(ctx, ctx.admin_headers, "SELECT id FROM identity_acl_probe ORDER BY id", reference.before_restart_ids);
    try std.testing.expectEqual(reference.before_restart_counter, try identityCounter(ctx));
    const before = try identityCounter(ctx);
    try ok(ctx, "REVOKE INSERT, UPDATE ON identity_acl_probe FROM identity_writer");
    try denied(ctx, headers, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1", "42501");
    try denied(ctx, headers, "UPDATE identity_acl_probe SET id=DEFAULT RETURNING 1", "42501");
    try std.testing.expectEqual(before, try identityCounter(ctx));
    try ok(ctx, "GRANT INSERT, UPDATE ON identity_acl_probe TO identity_writer");
}

fn identityGrantAfterRestart(ctx: *fixture.Context, headers: []const transport.RequestHeader, reference: IdentityReference) !void {
    try std.testing.expectEqual(reference.recovered_counter, try identityCounter(ctx));
    try grantedWrite(ctx, headers, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1");
    try rowsWithHeaders(ctx, ctx.admin_headers, "SELECT id FROM identity_acl_probe ORDER BY id", reference.after_restart_ids);
    try denied(ctx, headers, "SELECT nextval('identity_acl_probe_id_seq')", "42501");
    try ok(ctx, "REVOKE identity_writer FROM \"pk-reader\"");
    try denied(ctx, headers, "INSERT INTO identity_acl_probe(amount) VALUES(10) RETURNING 1", "42501");
    try denied(ctx, headers, "UPDATE identity_acl_probe SET id=DEFAULT RETURNING 1", "42501");
    try std.testing.expectEqual(reference.after_restart_counter, try identityCounter(ctx));
    try ok(ctx, "ALTER TABLE identity_acl_probe DISABLE ROW LEVEL SECURITY");
    const table_id = (try fixture.waitServingSchema(ctx, "identity_acl_probe", 0)).table_id;
    _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, table_id, .disabled, 0);
    try ok(ctx, "DROP POLICY identity_acl_write ON identity_acl_probe");
    try dropTable(ctx, "public", "identity_acl_probe");
    try ok(ctx, "REVOKE identity_writer FROM \"pk-rewriter\"");
    try ok(ctx, "DROP ROLE identity_writer");
}

fn dropTable(ctx: *fixture.Context, namespace: []const u8, name: []const u8) !void {
    const statement = try std.fmt.allocPrint(ctx.alloc, "DROP TABLE {s}.{s}", .{ namespace, name });
    defer ctx.alloc.free(statement);
    var response = try send(ctx, ctx.admin_headers, statement);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) std.debug.print("authorization cleanup SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings(if (response.status == 200) "DROP TABLE" else "DDL PENDING", result.value.command_tag);
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const platform = @import("antfly_platform");
    const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve = .{ .database = "default", .namespace = namespace, .table = name } });
        defer ctx.alloc.free(bytes);
        var resolved = try std.json.parseFromSlice(?domain.ResolvedTable, ctx.alloc, bytes, .{});
        defer resolved.deinit();
        if (resolved.value == null) return;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlDropRetirementTimeout;
}

fn grantOnPath(ctx: *fixture.Context) !void {
    try ok(ctx, "CREATE SCHEMA acl_scope");
    try ok(ctx, "CREATE TABLE public.path_rows (id BIGINT PRIMARY KEY)");
    try ok(ctx, "CREATE TABLE acl_scope.path_rows (id BIGINT PRIMARY KEY)");
    try ok(ctx, "INSERT INTO acl_scope.path_rows VALUES(9)");
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
    defer opened.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), opened.status);
    var connection = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
    defer connection.deinit();
    const id = connection.value.connection_id;
    const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
    defer ctx.alloc.free(suffix);
    defer {
        var closed = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, suffix, .DELETE, null, 15_000) catch null;
        if (closed) |*response| response.deinit(ctx.alloc);
    }
    for ([_][]const u8{ "SET search_path TO acl_scope, public", "GRANT SELECT ON path_rows TO app_writer" }) |statement| {
        var response = try client.executeConnected(&authenticated, null, id, statement, .never);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("authorization path SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    try readerRows(ctx, "SELECT id FROM acl_scope.path_rows", &.{9});
    try denied(ctx, ctx.reader_headers, "SELECT id FROM public.path_rows", "42501");
    try dropTable(ctx, "acl_scope", "path_rows");
    try dropTable(ctx, "public", "path_rows");
    try ok(ctx, "DROP SCHEMA acl_scope");
}

fn roleName(roles: []const graph.Role, principal: graph.Principal) ?[]const u8 {
    if (principal.kind != .role) return null;
    for (roles) |role| if (role.principal().eql(principal)) return role.name;
    return null;
}

fn revokedSession(ctx: *fixture.Context, headers: []const transport.RequestHeader, filter_change: bool) !void {
    var authenticated = ctx.*;
    authenticated.headers = headers;
    var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
    defer opened.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), opened.status);
    var connection = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
    defer connection.deinit();
    const id = connection.value.connection_id;
    const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
    defer ctx.alloc.free(suffix);
    defer {
        var closed = fixture.requestWithTimeout(ctx.alloc, ctx.transport, headers, ctx.base, suffix, .DELETE, null, 15_000) catch null;
        if (closed) |*response| response.deinit(ctx.alloc);
    }
    for ([_][]const u8{ "BEGIN", "INSERT INTO usage_records VALUES(3,10)" }) |statement| {
        var response = try client.executeConnected(&authenticated, null, id, statement, .never);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("authorization session SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
    if (filter_change) try manager.setRowFilter("pk-reader", "usage_records", "{\"term\":{\"amount\":20}}") else try ok(ctx, "REVOKE INSERT ON usage_records FROM app_writer");
    var commit = try client.executeConnected(&authenticated, null, id, "COMMIT", .never);
    defer commit.deinit(ctx.alloc);
    if (commit.status == 200) std.debug.print("authorization revoked commit body={s}\n", .{commit.body});
    try std.testing.expect(commit.status != 200);
    var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, commit.body, .{ .ignore_unknown_fields = true });
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings("42501", diagnostic.value.code);
    var rollback = try client.executeConnected(&authenticated, null, id, "ROLLBACK", .never);
    defer rollback.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), rollback.status);
    if (filter_change) try manager.setRowFilter("pk-reader", "usage_records", "{\"term\":{\"amount\":10}}") else try ok(ctx, "GRANT INSERT ON usage_records TO app_writer");
    try readerRows(ctx, "SELECT id FROM usage_records ORDER BY id", &.{1});
}
fn targetRoleName(roles: []const graph.Role, id: u64) ?[]const u8 {
    for (roles) |role| if (role.id == id) return role.name;
    return null;
}
fn selectedMember(name: []const u8) bool {
    return std.mem.eql(u8, name, "app_reader") or std.mem.eql(u8, name, "app_writer");
}
fn postimage(ctx: *fixture.Context, table_id: u64, expected: std.json.Value) !void {
    var region = std.heap.ArenaAllocator.init(ctx.alloc);
    defer region.deinit();
    const a = region.allocator();
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(a, .{}, .snapshot);
    const state = try std.json.parseFromSliceLeaky(domain.State, a, bytes, .{ .allocate = .alloc_always });
    var index = try domain.StateIndex.init(a, state);
    defer index.deinit(a);
    var roles: std.ArrayList(graph.Role) = .empty;
    for (state.resources) |resource| if (resource.kind == .role) try roles.append(a, try std.json.parseFromSliceLeaky(graph.Role, a, resource.source_json, .{ .allocate = .alloc_always }));
    try std.testing.expectEqual(expected.object.get("writer_exists").?.bool, index.find(.role, 0, "app_writer") != null);
    const memberships = expected.object.get("memberships").?.array.items;
    const table_grants = expected.object.get("table_grants").?.array.items;
    const defaults = expected.object.get("defaults").?.array.items;
    var member_count: usize = 0;
    var table_count: usize = 0;
    var default_count: usize = 0;
    for (state.privileges) |record| for (record.grants) |grant| {
        const recipient = roleName(roles.items, grant.grantee) orelse continue;
        switch (record.scope) {
            .role => |id| {
                const target = targetRoleName(roles.items, id) orelse return error.InvalidCatalogRecord;
                if ((!std.mem.eql(u8, target, "readonly") and !std.mem.eql(u8, target, "reporting")) or !selectedMember(recipient)) continue;
                const matched = for (memberships) |row| {
                    const values = row.array.items;
                    if (std.mem.eql(u8, target, values[0].string) and std.mem.eql(u8, recipient, values[1].string) and grant.option == values[2].bool and grant.inherit == values[3].bool and grant.set == values[4].bool) break true;
                } else false;
                try std.testing.expect(matched);
                member_count += 1;
            },
            .table => |id| {
                if (id != table_id or !std.mem.eql(u8, recipient, "app_writer")) continue;
                const matched = for (table_grants) |row| {
                    const values = row.array.items;
                    if (std.mem.eql(u8, recipient, values[0].string) and std.ascii.eqlIgnoreCase(@tagName(grant.privilege), values[1].string) and grant.option == values[2].bool) break true;
                } else false;
                try std.testing.expect(matched);
                table_count += 1;
            },
            .defaults => |scope| {
                const owner = roleName(roles.items, scope.owner) orelse continue;
                if (!std.mem.eql(u8, owner, "app_owner") or scope.namespace_id != domain.default_namespace_id or (!std.mem.eql(u8, recipient, "readonly") and !std.mem.eql(u8, recipient, "app_writer"))) continue;
                const matched = for (defaults) |row| {
                    const values = row.array.items;
                    if (std.mem.eql(u8, owner, values[0].string) and std.mem.eql(u8, "public", values[1].string) and std.mem.eql(u8, recipient, values[2].string) and std.ascii.eqlIgnoreCase(@tagName(grant.privilege), values[3].string) and grant.option == values[4].bool) break true;
                } else false;
                try std.testing.expect(matched);
                default_count += 1;
            },
        }
    };
    try std.testing.expectEqual(memberships.len, member_count);
    try std.testing.expectEqual(table_grants.len, table_count);
    try std.testing.expectEqual(defaults.len, default_count);
}

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    entries: []const Entry,
    identity_defaults: IdentityReference,
    credentials: client.ApiKeyHeaders = .{},
    table_id: u64 = 0,
    writer_authorization: [192]u8 = undefined,
    writer_headers: [1]transport.RequestHeader = undefined,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.user, users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
        try manager.setRowFilter("pk-reader", "usage_records", "{\"term\":{\"amount\":10}}");
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
        var administrator = try manager.createApiKey("pk-rewriter", "authorization-admin", &.{}, &.{}, null);
        defer administrator.deinit(manager.alloc);
        const administrator_header = try std.fmt.bufPrint(&self.credentials.authorization[0], "ApiKey {s}", .{administrator.encoded});
        self.credentials.admin[0] = .{ .name = "authorization", .value = administrator_header };
        var writer = try manager.createApiKey("pk-reader", "authorization-writer", &.{}, &.{}, null);
        defer writer.deinit(manager.alloc);
        self.writer_headers[0] = .{ .name = "authorization", .value = try std.fmt.bufPrint(&self.writer_authorization, "ApiKey {s}", .{writer.encoded}) };
        try ok(ctx, "CREATE ROLE app_owner");
        try ok(ctx, "CREATE ROLE readonly");
        try ok(ctx, "CREATE ROLE reporting");
        try ok(ctx, "CREATE ROLE app_reader");
        try ok(ctx, "CREATE TABLE usage_records (id BIGINT PRIMARY KEY,amount BIGINT)");
        self.table_id = (try fixture.waitServingSchema(ctx, "usage_records", 0)).table_id;
        try ok(ctx, "INSERT INTO usage_records VALUES(1,10),(2,20)");
        // A policy is a dependency even when its role has no data grants,
        // memberships, default grants, or ownership references.
        try ok(ctx, "CREATE ROLE policy_only");
        try ok(ctx, "CREATE POLICY draft_role ON usage_records TO policy_only USING (true)");
        try denied(ctx, ctx.admin_headers, "DROP ROLE policy_only", "2BP01");
        try ok(ctx, "DROP POLICY draft_role ON usage_records");
        try ok(ctx, "DROP ROLE policy_only");
        try ok(ctx, "CREATE ROLE \"PUBLIC\"");
        try ok(ctx, "CREATE POLICY quoted_role ON usage_records TO \"PUBLIC\" USING (true)");
        try denied(ctx, ctx.admin_headers, "DROP ROLE \"PUBLIC\"", "2BP01");
        try ok(ctx, "DROP POLICY quoted_role ON usage_records");
        try ok(ctx, "DROP ROLE \"PUBLIC\"");
        try manager.removePermissionFromUser("pk-reader", "*", .table);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (self.entries) |entry| {
            try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
            errdefer std.debug.print("authorization original failed id={s} SQL={s}\n", .{ entry.id, entry.sql });
            if (entry.sqlstate) |code| {
                try denied(ctx, ctx.admin_headers, entry.sql, code);
                try postimage(ctx, self.table_id, entry.postimage);
                try ok(ctx, "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public REVOKE ALL PRIVILEGES ON TABLES FROM app_writer CASCADE");
                continue;
            }
            var response = try send(ctx, ctx.admin_headers, entry.sql);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("authorization original status={d} body={s}\n", .{ response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(entry.command_tag.?, parsed.value.command_tag);
            try postimage(ctx, self.table_id, entry.postimage);
            if (std.mem.eql(u8, entry.id, "sql-0332")) {
                try ok(ctx, "GRANT app_writer TO \"pk-reader\"");
                try readerRows(ctx, "SELECT id FROM usage_records ORDER BY id", &.{1});
                try denied(ctx, ctx.reader_headers, "INSERT INTO usage_records VALUES(3,10) RETURNING 1", "42501");
                try denied(ctx, &self.writer_headers, "UPDATE usage_records SET amount=10", "42501");
                try denied(ctx, &self.writer_headers, "DELETE FROM usage_records", "42501");
                try denied(ctx, &self.writer_headers, "INSERT INTO usage_records VALUES(3,20)", "42501");
                var inserted = try send(ctx, &self.writer_headers, "INSERT INTO usage_records VALUES(3,10) RETURNING 1");
                defer inserted.deinit(ctx.alloc);
                if (inserted.status != 200) std.debug.print("authorization insert status={d} body={s}\n", .{ inserted.status, inserted.body });
                try std.testing.expectEqual(@as(u16, 200), inserted.status);
                try readerRows(ctx, "SELECT id FROM usage_records ORDER BY id", &.{ 1, 3 });
                try ok(ctx, "DELETE FROM usage_records WHERE id=3");
                try revokedSession(ctx, &self.writer_headers, false);
                try revokedSession(ctx, &self.writer_headers, true);
                try grantOnPath(ctx);
                try denied(ctx, &self.writer_headers, "ANALYZE usage_records", "42501");
                try ok(ctx, "GRANT MAINTAIN ON usage_records TO app_writer");
                try denied(ctx, ctx.reader_headers, "ANALYZE usage_records", "42501");
                var maintained = try send(ctx, &self.writer_headers, "ANALYZE usage_records");
                defer maintained.deinit(ctx.alloc);
                if (maintained.status != 200) std.debug.print("authorization MAINTAIN status={d} body={s}\n", .{ maintained.status, maintained.body });
                try std.testing.expectEqual(@as(u16, 200), maintained.status);
                try ok(ctx, "REVOKE MAINTAIN ON usage_records FROM app_writer");
                // Defaults affect the actual native CREATE transaction and
                // retire with its table, independently of later default edits.
                try ok(ctx, "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO app_writer");
                try ok(ctx, "CREATE TABLE default_probe(id BIGINT PRIMARY KEY)");
                _ = try fixture.waitServingSchema(ctx, "default_probe", 0);
                try ok(ctx, "INSERT INTO default_probe VALUES(7)");
                try readerRows(ctx, "SELECT id FROM default_probe", &.{7});
                try ok(ctx, "ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM app_writer");
                try readerRows(ctx, "SELECT id FROM default_probe", &.{7});
                try dropTable(ctx, "public", "default_probe");
                try ok(ctx, "CREATE POLICY role_read ON usage_records FOR SELECT TO app_writer USING (true)");
                try ok(ctx, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY");
                const policy_wait = @import("sql_policy_lifecycle_integration_test.zig");
                const generation = try policy_wait.waitPublication(ctx, self.table_id, .active, 0);
                try readerRows(ctx, "SELECT id FROM usage_records ORDER BY id", &.{1});
                try ok(ctx, "ALTER TABLE usage_records DISABLE ROW LEVEL SECURITY");
                _ = try policy_wait.waitPublication(ctx, self.table_id, .disabled, generation);
                try ok(ctx, "DROP POLICY role_read ON usage_records");
            }
            if (std.mem.eql(u8, entry.id, "sql-0333")) try denied(ctx, &self.writer_headers, "INSERT INTO usage_records VALUES(3,10)", "42501");
            if (std.mem.eql(u8, entry.id, "sql-0341")) try denied(ctx, ctx.reader_headers, "SELECT id FROM usage_records", "42501");
        }
        try ok(ctx, "CREATE ROLE insert_only");
        try ok(ctx, "GRANT INSERT ON usage_records TO insert_only");
        try ok(ctx, "GRANT insert_only TO \"pk-reader\"");
        try denied(ctx, &self.writer_headers, "INSERT INTO usage_records VALUES(3,10) RETURNING *", "42501");
        var literal_returning = try send(ctx, &self.writer_headers, "INSERT INTO usage_records VALUES(3,10) RETURNING 1");
        defer literal_returning.deinit(ctx.alloc);
        if (literal_returning.status != 200) std.debug.print("authorization INSERT-only literal RETURNING status={d} body={s}\n", .{ literal_returning.status, literal_returning.body });
        try std.testing.expectEqual(@as(u16, 200), literal_returning.status);
        try ok(ctx, "DELETE FROM usage_records WHERE id=3");
        try ok(ctx, "REVOKE INSERT ON usage_records FROM insert_only");
        try ok(ctx, "DROP ROLE insert_only");
        try identityGrantBeforeRestart(ctx, &self.writer_headers, self.identity_defaults);
        try ok(ctx, "CREATE ROLE cold_policy");
        try ok(ctx, "CREATE POLICY retained_role ON usage_records TO cold_policy USING (true)");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try identityGrantAfterRestart(ctx, &self.writer_headers, self.identity_defaults);
        try denied(ctx, ctx.admin_headers, "DROP ROLE cold_policy", "2BP01");
        try ok(ctx, "DROP POLICY retained_role ON usage_records");
        try ok(ctx, "DROP ROLE cold_policy");
        try postimage(ctx, self.table_id, self.entries[self.entries.len - 1].postimage);
        try denied(ctx, ctx.reader_headers, "SELECT id FROM usage_records", "42501");
        try ok(ctx, "CREATE ROLE app_writer");
        try denied(ctx, ctx.reader_headers, "SELECT id FROM usage_records", "42501");
        try ok(ctx, "DROP ROLE app_writer");
    }
};

test "mounted SQL original authorization preserves PostgreSQL matrices credential caps filtered mutations and cold revocation" {
    var reference = try std.json.parseFromSlice(struct { cases: []const Entry, identity_defaults: IdentityReference }, std.testing.allocator, local.sql_parity_fixtures.authorization_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    const originals = [_][]const u8{ "sql-0342", "sql-0332", "sql-0333", "sql-0334", "sql-0335", "sql-0336", "sql-0337", "sql-0338", "sql-0339", "sql-0340", "sql-0341", "sql-0350", "sql-0350" };
    try std.testing.expectEqual(originals.len, reference.value.cases.len);
    for (originals, reference.value.cases) |id, entry| try std.testing.expectEqualStrings(id, entry.id);
    try fixture.runWithState(Campaign, .{ .entries = reference.value.cases, .identity_defaults = reference.value.identity_defaults });
}
