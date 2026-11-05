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

//! Original database DROP contracts through authenticated public SQL, native
//! session custody, real owner barriers and complete metadata/data restart.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const transport = @import("../raft/transport/http_common.zig");
const Wire = @import("antfly_metadata_openapi").types;
const time = @import("antfly_platform").time;

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    retained: std.mem.Allocator,
    ordinary_token: ?[]const u8 = null,
    force_token: ?[]const u8 = null,
    idle: [32]u8 = undefined,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn send(ctx: *fixture.Context, database: []const u8, connection: ?[]const u8, statement: []const u8) !transport.HttpResponse {
        const body = if (connection) |id|
            try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .connection_id = id }, .{})
        else
            try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .database = database }, .{});
        defer ctx.alloc.free(body);
        // Connection handles belong to the public API that issued them. This
        // campaign tests database retirement, not cross-entrypoint discovery.
        var scoped = ctx.*;
        scoped.base = ctx.metadata_uri;
        return client.executeBody(&scoped, connection, body, .write_admission);
    }
    fn command(ctx: *fixture.Context, database: []const u8, sql: []const u8) !void {
        var response = try send(ctx, database, null, sql);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("database retirement setup sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    fn reject(ctx: *fixture.Context, database: []const u8, connection: ?[]const u8, sql: []const u8, code: []const u8) !void {
        var response = try send(ctx, database, connection, sql);
        defer response.deinit(ctx.alloc);
        if (response.status == 200 or response.status == 202) return error.ExpectedSqlRejection;
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(code, diagnostic.value.code);
    }
    fn open(ctx: *fixture.Context, database: []const u8) ![32]u8 {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .database = database }, .{});
        defer ctx.alloc.free(body);
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/sql/connections", .POST, body, 10000);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("database connection open status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.connection_id.len != 32) return error.InvalidSqlConnection;
        return parsed.value.connection_id[0..32].*;
    }
    fn observe(ctx: *fixture.Context, token: []const u8) !bool {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = token }, .{});
        defer ctx.alloc.free(body);
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/sql/operations/status", .POST, body, 10000);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("database retirement recovery status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(struct { state: []const u8, catalog_revision: ?[]const u8 = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (std.mem.eql(u8, parsed.value.state, "rejected")) return error.DatabaseDropRejected;
        if (!std.mem.eql(u8, parsed.value.state, "committed")) return false;
        _ = try std.fmt.parseInt(u64, parsed.value.catalog_revision orelse return error.MissingCatalogRevision, 10);
        return true;
    }
    fn await(ctx: *fixture.Context, token: []const u8) !void {
        const deadline = time.monotonicNs() +| 90 * std.time.ns_per_s;
        while (time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            if (try observe(ctx, token)) return;
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const work = source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .schema_validation_authority = true }, .{ .scope_retirement_work = 0 }) catch |err| {
            std.debug.print("database retirement timeout: work read failed {s}\n", .{@errorName(err)});
            return error.DatabaseDropCompletionTimeout;
        };
        defer ctx.alloc.free(work);
        std.debug.print("database retirement timeout: active work={s}\n", .{work});
        return error.DatabaseDropCompletionTimeout;
    }
    fn drop(self: *@This(), ctx: *fixture.Context, sql: []const u8) ![]const u8 {
        var response = try send(ctx, "default", null, sql);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("database retirement sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        var parsed = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const result = parsed.value.catalog_receipt orelse return error.MissingDatabaseDropReceipt;
        try std.testing.expectEqualStrings(if (result.state == .committed) "DROP DATABASE" else "DDL PENDING", parsed.value.command_tag);
        return self.retained.dupe(u8, result.recovery_token);
    }
    fn createPopulated(ctx: *fixture.Context) !void {
        try command(ctx, "default", "CREATE DATABASE tenant_ops");
        try command(ctx, "tenant_ops", "CREATE TABLE durable(id BIGINT PRIMARY KEY)");
        try command(ctx, "tenant_ops", "INSERT INTO durable VALUES (9)");
        try command(ctx, "tenant_ops", "CREATE VIEW display AS SELECT id FROM durable");
        try command(ctx, "tenant_ops", "CREATE SEQUENCE counter");
        try command(ctx, "tenant_ops", "CREATE TABLE borrower(id BIGSERIAL)");
        try command(ctx, "tenant_ops", "INSERT INTO borrower DEFAULT VALUES");
        try command(ctx, "tenant_ops", "CREATE TABLE empty_docs () WITH (antfly.storage_mode='document')");
        try command(ctx, "tenant_ops", "CREATE TABLE documents () WITH (antfly.storage_mode='document',antfly.default_type='doc') DOCUMENT SCHEMA doc AS JSON '{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"keyword\"}}}'");
        try command(ctx, "tenant_ops", "INSERT INTO documents(_id,title) VALUES ('doc:one','ready')");
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        try createPopulated(ctx);
        const control = try open(ctx, "default");
        var begin = try send(ctx, "default", &control, "BEGIN");
        defer begin.deinit(ctx.alloc);
        if (begin.status != 200) std.debug.print("database retirement BEGIN status={d} body={s}\n", .{ begin.status, begin.body });
        try std.testing.expectEqual(@as(u16, 200), begin.status);
        try reject(ctx, "default", &control, (try corpus.get("sql-0108")).sql, "25001");
        var rollback = try send(ctx, "default", &control, "ROLLBACK");
        defer rollback.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), rollback.status);
        const prepared = try open(ctx, "tenant_ops");
        inline for (.{ "BEGIN", "UPDATE durable SET id=10 WHERE id=9", "PREPARE TRANSACTION 'retirement_prepared'" }) |sql| {
            var response = try send(ctx, "tenant_ops", &prepared, sql);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("database retirement prepared SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
        }
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{prepared});
        defer ctx.alloc.free(path);
        var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, path, .DELETE, "", 10000);
        defer closed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), closed.status);
        try reject(ctx, "default", null, (try corpus.get("sql-0109")).sql, "55006");
        try command(ctx, "tenant_ops", "ROLLBACK PREPARED 'retirement_prepared'");
        self.ordinary_token = try self.drop(ctx, (try corpus.get("sql-0108")).sql);
        try await(ctx, self.ordinary_token.?);
        // A replacement database has a new immutable ID; the previous receipt
        // must remain committed and must never operate on this replacement.
        try createPopulated(ctx);
        self.idle = try open(ctx, "tenant_ops");
        try reject(ctx, "default", null, (try corpus.get("sql-0108")).sql, "55006");
        try reject(ctx, "tenant_ops", &self.idle, (try corpus.get("sql-0109")).sql, "55006");
        // Persist an admitted FORCE while the normal supervisor is paused,
        // then lose every volatile coordinator by reopening all native roots.
        const driver = @import("http_server.zig").ApiHttpServer.FkGenerationPublicationTestDriver;
        try driver.pauseBackground(ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi, ctx.io);
        if (ctx.data.http_server) |*api| try driver.pauseBackground(api, ctx.io);
        self.force_token = try self.drop(ctx, (try corpus.get("sql-0109")).sql);
        try std.testing.expect(!try observe(ctx, self.force_token.?));
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try await(ctx, self.force_token.?);
        try std.testing.expect(try observe(ctx, self.ordinary_token.?));
        var old = try send(ctx, "tenant_ops", &self.idle, "SELECT 1");
        defer old.deinit(ctx.alloc);
        try std.testing.expect(old.status != 200);
        try command(ctx, "default", "DROP DATABASE IF EXISTS tenant_ops WITH (FORCE)");
        try command(ctx, "default", "CREATE DATABASE tenant_ops");
        const connection = try open(ctx, "tenant_ops");
        var fresh = try send(ctx, "tenant_ops", &connection, "SELECT 1");
        defer fresh.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), fresh.status);
        var stale = try send(ctx, "tenant_ops", &self.idle, "SELECT 1");
        defer stale.deinit(ctx.alloc);
        try std.testing.expect(stale.status != 200);
        try std.testing.expect(try observe(ctx, self.force_token.?));
        const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
        const snapshot = try store.snapshotBuilder().buildSnapshot(ctx.alloc, local.common_group_ids.main_metadata_group_id);
        defer ctx.alloc.free(snapshot);
    }
};

test "mounted SQL scope retirement capture completes original database DROP and FORCE with idle custody and cold recovery" {
    // Exact original sql-0108 and sql-0109 execute against populated mixed
    // document/relational databases, then recover the same native decisions.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try fixture.runWithState(Campaign, .{ .retained = arena.allocator() });
}
