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

//! Original role-default DDL through authenticated SQL, metadata consensus,
//! independent old/new connections and cold catalog/session-store recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_catalog_owner_test_fixture.zig");
const http = @import("http_server.zig");
const users = @import("../usermgr/mod.zig");
const transport = @import("../raft/transport/http_common.zig");

const Entry = struct {
    id: []const u8,
    command_tag: ?[]const u8,
    sqlstate: ?[]const u8,
    before: []const [2][]const u8,
    after: []const [2][]const u8,
    old_login_value: ?[]const u8,
    new_login_value: ?[]const u8,
};

fn send(ctx: *fixture.Context, headers: []const transport.RequestHeader, statement: []const u8, connection: ?*const [32]u8) !transport.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .connection_id = if (connection) |id| @as([]const u8, id) else null }, .{ .emit_null_optional_fields = false });
    defer ctx.alloc.free(body);
    return fixture.request(ctx.alloc, ctx.transport, headers, ctx.base, "/db/v1/sql", .POST, body);
}

fn command(ctx: *fixture.Context, statement: []const u8, sqlstate: ?[]const u8) !void {
    var response = try send(ctx, ctx.admin_headers, statement, null);
    defer response.deinit(ctx.alloc);
    if (sqlstate) |code| {
        try std.testing.expectEqual(@as(u16, 400), response.status);
        var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings(code, parsed.value.code);
    } else {
        if (response.status != 200) std.debug.print("role default SQL failed statement={s} status={} body={s}\n", .{ statement, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(struct { command_tag: []const u8, rows_affected: u64 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer result.deinit();
        const tag = if (std.mem.startsWith(u8, statement, "ALTER ROLE")) "ALTER ROLE" else if (std.mem.startsWith(u8, statement, "ALTER DATABASE")) "ALTER DATABASE" else if (std.mem.startsWith(u8, statement, "DROP DATABASE")) "DROP DATABASE" else "CREATE DATABASE";
        try std.testing.expectEqualStrings(tag, result.value.command_tag);
        try std.testing.expectEqual(@as(u64, 0), result.value.rows_affected);
    }
}

fn open(ctx: *fixture.Context, headers: []const transport.RequestHeader) ![32]u8 {
    return openDatabase(ctx, headers, "appdb");
}

fn openDatabase(ctx: *fixture.Context, headers: []const transport.RequestHeader, database: []const u8) ![32]u8 {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .database = database }, .{});
    defer ctx.alloc.free(body);
    var response = try fixture.request(ctx.alloc, ctx.transport, headers, ctx.base, "/db/v1/sql/connections", .POST, body);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("role default connection failed status={} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 32), parsed.value.connection_id.len);
    return parsed.value.connection_id[0..32].*;
}

fn close(ctx: *fixture.Context, headers: []const transport.RequestHeader, id: *const [32]u8) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
    defer ctx.alloc.free(path);
    var response = try fixture.request(ctx.alloc, ctx.transport, headers, ctx.base, path, .DELETE, null);
    defer response.deinit(ctx.alloc);
    try std.testing.expect(response.status == 200 or response.status == 204);
}

fn expectValue(ctx: *fixture.Context, headers: []const transport.RequestHeader, id: *const [32]u8, name: []const u8, expected: ?[]const u8) !void {
    if (std.mem.eql(u8, name, "statement_timeout")) {
        // A correctly enforced 1ms budget may cancel the observing query
        // itself. Inspect the real persisted connection contract instead of
        // disabling that budget or requiring a wall-clock timing accident.
        const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingCatalogApi;
        const store = api.txn_sessions.durable orelse return error.MissingSqlStore;
        var connection = try @import("sql_connections.zig").load(store, ctx.alloc, id.*, "app_writer", api.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        defer connection.deinit();
        try std.testing.expectEqual(@as(u32, if (std.mem.eql(u8, expected.?, "0")) 0 else 1), connection.value.statement_timeout_ms orelse 0);
        return;
    }
    return expectQueryValue(ctx, headers, id, name, expected);
}

fn expectQueryValue(ctx: *fixture.Context, headers: []const transport.RequestHeader, id: *const [32]u8, name: []const u8, expected: ?[]const u8) !void {
    const statement = try std.fmt.allocPrint(ctx.alloc, "SELECT current_setting('{s}') AS value", .{name});
    defer ctx.alloc.free(statement);
    var response = try send(ctx, headers, statement, id);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("role default observation failed status={} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), result.value.rows[0].len);
    try std.testing.expect(expected != null);
    try std.testing.expectEqualStrings(expected.?, result.value.rows[0][0].string);
}

/// Lose only the client acknowledgement, after the real HTTP owner has
/// returned a committed response. Never replay an ambiguous mutation.
const LostReply = struct {
    inner: transport.RequestExecutor,
    reached: bool = false,

    fn execute(raw: *anyopaque, allocator: std.mem.Allocator, request: transport.HttpRequest) !transport.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var response = try self.inner.execute(allocator, request);
        defer response.deinit(allocator);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        self.reached = true;
        return error.ConnectionResetByPeer;
    }
};

const ConcurrentChange = struct {
    context: *fixture.Context,
    sql: []const u8,
    failure: ?anyerror = null,

    fn run(self: *@This()) std.Io.Cancelable!void {
        command(self.context, self.sql, null) catch |err| {
            self.failure = err;
        };
    }
};

const Campaign = struct {
    pub const native_sessions = true;
    entries: []const Entry,
    scoped_bootstrap: bool = false,
    old: [32]u8 = undefined,
    fresh: [32]u8 = undefined,
    identities: [2]?local.system_catalog_settings.Identity = .{ null, null },

    fn expectCatalog(self: *@This(), ctx: *fixture.Context, expected: []const [2][]const u8) !void {
        const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const bytes = try source.systemCatalog(ctx.alloc, .{}, .snapshot);
        defer ctx.alloc.free(bytes);
        var state = try std.json.parseFromSlice(local.system_catalog_domain.State, ctx.alloc, bytes, .{});
        defer state.deinit();
        try std.testing.expect(state.value.settings.len >= 1 and state.value.settings.len <= 2);
        var count: usize = 0;
        for (state.value.settings) |record| {
            const index: usize = if (std.mem.eql(u8, record.name, "app.tenant_id")) 0 else if (std.mem.eql(u8, record.name, "statement_timeout")) 1 else return error.UnexpectedSetting;
            if (self.identities[index]) |identity| try std.testing.expectEqualDeep(identity, record.identity) else self.identities[index] = record.identity;
            try std.testing.expect(record.session_writable and !record.policy_sensitive);
            for (record.role_defaults) |entry| {
                try std.testing.expectEqualStrings("app_writer", entry.principal);
                try std.testing.expect(entry.principal_instance != null);
                try std.testing.expectEqual(entry.database.len != 0, entry.database_id != null);
                const config = switch (entry.value) {
                    .string => |value| try std.fmt.allocPrint(ctx.alloc, "{s}={s}", .{ record.name, value }),
                    .integer => |value| try std.fmt.allocPrint(ctx.alloc, "{s}={d}ms", .{ record.name, value }),
                    else => return error.UnexpectedSettingType,
                };
                defer ctx.alloc.free(config);
                const found = for (expected) |wanted| {
                    if (std.mem.eql(u8, wanted[0], entry.database) and std.mem.eql(u8, wanted[1], config)) break true;
                } else false;
                try std.testing.expect(found);
                count += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, if (self.identities[1] != null) 2 else 1), state.value.settings.len);
        try std.testing.expectEqual(expected.len, count);
    }

    pub fn configureAuth(manager: *users.UserManager) !void {
        for ([_]users.ResourceType{ .user, .database, .namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
        var read = try users.Permission.initOwned(manager.alloc, .table, "*", .read);
        defer read.deinit(manager.alloc);
        var user = try manager.createUser("app_writer", "sql-role-default-witness", &.{read});
        user.deinit(manager.alloc);
    }

    fn authorization(ctx: *fixture.Context) ![]u8 {
        const raw = "app_writer:sql-role-default-witness";
        const value = try ctx.alloc.alloc(u8, 6 + std.base64.standard.Encoder.calcSize(raw.len));
        @memcpy(value[0..6], "Basic ");
        _ = std.base64.standard.Encoder.encode(value[6..], raw);
        return value;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE DATABASE appdb", null);
        const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const auth = try authorization(ctx);
        defer ctx.alloc.free(auth);
        const headers = [_]transport.RequestHeader{.{ .name = "authorization", .value = auth }};
        const empty_login = try open(ctx, &headers);
        errdefer close(ctx, &headers, &empty_login) catch {};
        for ([_]local.system_catalog_settings.Input{
            .{ .name = "app.tenant_id", .kind = .string, .session_writable = true, .default = .{ .string = "" } },
        }) |input| {
            const bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .setting_mutate = .{ .put = input } });
            ctx.alloc.free(bytes);
        }
        if (self.scoped_bootstrap) {
            try command(ctx, "ALTER ROLE app_writer IN DATABASE appdb SET statement_timeout='1ms'", null);
            try self.expectCatalog(ctx, &.{.{ "appdb", "statement_timeout=1ms" }});
            try expectQueryValue(ctx, &headers, &empty_login, "statement_timeout", "0");
            const scoped = try open(ctx, &headers);
            errdefer close(ctx, &headers, &scoped) catch {};
            try expectValue(ctx, &headers, &scoped, "statement_timeout", "1ms");
            try close(ctx, &headers, &scoped);
            try command(ctx, "ALTER ROLE app_writer IN DATABASE appdb RESET statement_timeout", null);
        }
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (self.entries) |entry| {
            for ([_][]const u8{ "app.tenant_id", "statement_timeout" }) |name| {
                for ([_][]const u8{ "", " IN DATABASE appdb" }) |scope| {
                    const reset = try std.fmt.allocPrint(ctx.alloc, "ALTER ROLE app_writer{s} RESET {s}", .{ scope, name });
                    defer ctx.alloc.free(reset);
                    try command(ctx, reset, null);
                }
            }
            for (entry.before) |value| {
                const equals = std.mem.indexOfScalar(u8, value[1], '=') orelse return error.InvalidReference;
                const set = try std.fmt.allocPrint(ctx.alloc, "ALTER ROLE app_writer SET {s} = '{s}'", .{ value[1][0..equals], value[1][equals + 1 ..] });
                defer ctx.alloc.free(set);
                try std.testing.expectEqualStrings("", value[0]);
                try command(ctx, set, null);
            }
            const name: []const u8 = if (std.mem.order(u8, entry.id, "sql-0346") != .lt and !std.mem.eql(u8, entry.id, "sql-0349")) "statement_timeout" else "app.tenant_id";
            try self.expectCatalog(ctx, entry.before);
            const old = try open(ctx, &headers);
            errdefer close(ctx, &headers, &old) catch {};
            try expectValue(ctx, &headers, &old, name, entry.old_login_value);
            try command(ctx, (try corpus.get(entry.id)).sql, entry.sqlstate);
            try self.expectCatalog(ctx, entry.after);
            if (std.mem.eql(u8, entry.id, "sql-0343")) {
                // This connection captured an empty catalog, not an unpinned
                // request. The later registration/role SET cannot affect it.
                try expectQueryValue(ctx, &headers, &empty_login, "app.tenant_id", "");
                try close(ctx, &headers, &empty_login);
            }
            if (std.mem.eql(u8, entry.id, "sql-0346"))
                try expectQueryValue(ctx, &headers, &old, "statement_timeout", "0");
            try expectValue(ctx, &headers, &old, name, entry.old_login_value);
            const fresh = try open(ctx, &headers);
            errdefer close(ctx, &headers, &fresh) catch {};
            try expectValue(ctx, &headers, &fresh, name, entry.new_login_value);
            try close(ctx, &headers, &fresh);
            try close(ctx, &headers, &old);
        }
        // Proven pre-admission failures cannot leave a setting fragment.
        try std.testing.expectError(error.DeadlineExceeded, source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .deadline_ns = 0 }, .{ .setting_mutate = .{ .role_default = .{
            .name = "app.tenant_id",
            .principal = "app_writer",
            .principal_instance = try ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?.currentUserInstance("app_writer"),
            .database = "",
            .value = .{ .string = "expired" },
        } } }));
        try command(ctx, "ALTER ROLE app_writer SET statement_timeout = '-1ms'", "22023");
        try command(ctx, "ALTER ROLE app_writer IN DATABASE missing_role_database SET app.tenant_id = 'rejected'", "3D000");
        try self.expectCatalog(ctx, &.{});

        // Two independent scopes patch the same setting concurrently. The
        // metadata cut and consensus publication must not lose either edit.
        var changes = [_]ConcurrentChange{
            .{ .context = ctx, .sql = "ALTER ROLE app_writer SET app.tenant_id = 'global'" },
            .{ .context = ctx, .sql = "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id = 'scoped'" },
        };
        var tasks: std.Io.Group = .init;
        defer tasks.cancel(ctx.io);
        for (&changes) |*change| try tasks.concurrent(ctx.io, ConcurrentChange.run, .{change});
        try tasks.await(ctx.io);
        for (changes) |change| if (change.failure) |err| return err;
        try self.expectCatalog(ctx, &.{ .{ "", "app.tenant_id=global" }, .{ "appdb", "app.tenant_id=scoped" } });
        const scoped = try open(ctx, &headers);
        errdefer close(ctx, &headers, &scoped) catch {};
        try expectValue(ctx, &headers, &scoped, "app.tenant_id", "scoped");
        try close(ctx, &headers, &scoped);
        try command(ctx, "ALTER ROLE app_writer IN DATABASE appdb RESET app.tenant_id", null);

        var dropped: LostReply = .{ .inner = ctx.transport };
        var client = ctx.*;
        client.transport = .{ .ptr = &dropped, .vtable = &.{ .execute = LostReply.execute } };
        try std.testing.expectError(error.ConnectionResetByPeer, send(&client, client.admin_headers, "ALTER ROLE app_writer SET app.tenant_id = 'reply-lost'", null));
        try std.testing.expect(dropped.reached);
        try self.expectCatalog(ctx, &.{.{ "", "app.tenant_id=reply-lost" }});
        const admitted = try open(ctx, &headers);
        errdefer close(ctx, &headers, &admitted) catch {};
        try expectValue(ctx, &headers, &admitted, "app.tenant_id", "reply-lost");
        try close(ctx, &headers, &admitted);
        try command(ctx, "ALTER ROLE app_writer RESET app.tenant_id", null);
        self.old = try open(ctx, &headers);
        try command(ctx, (try corpus.get("sql-0343")).sql, null);
        self.fresh = try open(ctx, &headers);
        try expectValue(ctx, &headers, &self.old, "app.tenant_id", "");
        try expectValue(ctx, &headers, &self.fresh, "app.tenant_id", "acme");
        try self.expectCatalog(ctx, &.{.{ "", "app.tenant_id=acme" }});
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        const auth = try authorization(ctx);
        defer ctx.alloc.free(auth);
        const headers = [_]transport.RequestHeader{.{ .name = "authorization", .value = auth }};
        try expectValue(ctx, &headers, &self.old, "app.tenant_id", "");
        try expectValue(ctx, &headers, &self.fresh, "app.tenant_id", "acme");
        try self.expectCatalog(ctx, &.{.{ "", "app.tenant_id=acme" }});
        const future = try open(ctx, &headers);
        errdefer close(ctx, &headers, &future) catch {};
        try expectValue(ctx, &headers, &future, "app.tenant_id", "acme");
        try close(ctx, &headers, &self.old);
        try close(ctx, &headers, &self.fresh);
        try close(ctx, &headers, &future);
        try command(ctx, "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id='scoped'", null);
        try command(ctx, "ALTER DATABASE appdb RENAME TO renameddb", null);
        try self.expectCatalog(ctx, &.{ .{ "", "app.tenant_id=acme" }, .{ "renameddb", "app.tenant_id=scoped" } });
        const renamed_login = try openDatabase(ctx, &headers, "renameddb");
        errdefer close(ctx, &headers, &renamed_login) catch {};
        try expectValue(ctx, &headers, &renamed_login, "app.tenant_id", "scoped");
        try close(ctx, &headers, &renamed_login);
        try command(ctx, "DROP DATABASE renameddb", null);
        try self.expectCatalog(ctx, &.{.{ "", "app.tenant_id=acme" }});
        try command(ctx, "CREATE DATABASE renameddb", null);
        const reused_database = try openDatabase(ctx, &headers, "renameddb");
        errdefer close(ctx, &headers, &reused_database) catch {};
        try expectValue(ctx, &headers, &reused_database, "app.tenant_id", "acme");
        try close(ctx, &headers, &reused_database);
        try command(ctx, "DROP DATABASE renameddb", null);
        try command(ctx, "CREATE DATABASE appdb", null);
        const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
        const predecessor = try manager.currentUserInstance("app_writer");
        try manager.deleteUser("app_writer");
        var read = try users.Permission.initOwned(manager.alloc, .table, "*", .read);
        defer read.deinit(manager.alloc);
        var recreated = try manager.createUser("app_writer", "sql-role-default-witness", &.{read});
        defer recreated.deinit(manager.alloc);
        try std.testing.expect(!std.mem.eql(u8, &predecessor, &recreated.instance_id));
        const replacement = try open(ctx, &headers);
        errdefer close(ctx, &headers, &replacement) catch {};
        try expectValue(ctx, &headers, &replacement, "app.tenant_id", "");
        // Repeating the same value must rebind the new incarnation rather
        // than being incorrectly classified as an old-owner no-op.
        try command(ctx, "ALTER ROLE app_writer SET app.tenant_id='acme'", null);
        const rebound = try open(ctx, &headers);
        errdefer close(ctx, &headers, &rebound) catch {};
        try expectValue(ctx, &headers, &replacement, "app.tenant_id", "");
        try expectValue(ctx, &headers, &rebound, "app.tenant_id", "acme");
        try close(ctx, &headers, &replacement);
        try close(ctx, &headers, &rebound);
    }
};

test "mounted SQL original role defaults retain old logins admit future defaults and recover cold connections" {
    // sql-0343 sql-0344 sql-0345 sql-0346 sql-0347 sql-0348 sql-0349
    var parsed = try std.json.parseFromSlice(struct { entries: []const Entry }, std.testing.allocator, local.sql_parity_fixtures.role_defaults_reference, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 7), parsed.value.entries.len);
    for ([_]bool{ false, true }) |scoped_bootstrap|
        try fixture.runWithState(Campaign, .{ .entries = parsed.value.entries, .scoped_bootstrap = scoped_bootstrap });
}
