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

//! Native sync visibility shares real SQL connection and transaction lifetimes.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const distributed = @import("distributed_txn.zig");
const Json = std.json.Value;
const Reference = struct {
    cases: []const struct { id: []const u8, sql: []const u8 },
    actions: []const struct { id: ?[]const u8, sql: []const u8, command_tag: []const u8, rows: []const []const []const u8, notices: []const []const u8 },
};

fn send(ctx: *fixture.Context, connection: []const u8, statement: []const u8, retry: client.Retry) !http.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    return client.executeConnected(&authenticated, null, connection, statement, retry);
}
fn command(ctx: *fixture.Context, connection: []const u8, statement: []const u8, expected: []const u8) !?[16]u8 {
    var response = try send(ctx, connection, statement, if (std.mem.startsWith(u8, statement, "CREATE TABLE")) .write_admission else .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("sync SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    var parsed = try std.json.parseFromSlice(struct { command_tag: []const u8, session_id: ?[]const u8 = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(expected, parsed.value.command_tag);
    return if (parsed.value.session_id) |id| try distributed.parseTxnIdHex(id) else null;
}
fn show(ctx: *fixture.Context, connection: []const u8, statement: []const u8, expected: []const u8) !void {
    var response = try send(ctx, connection, statement, .session_observation);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("sync observation={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { rows: []const []const Json }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows[0].len);
    try std.testing.expectEqualStrings(expected, parsed.value.rows[0][0].string);
}
fn open(ctx: *fixture.Context) ![32]u8 {
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}");
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("sync connection status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 32), parsed.value.connection_id.len);
    return parsed.value.connection_id[0..32].*;
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    reference: Reference,
    connection: [32]u8 = undefined,
    cold_txn: [16]u8 = undefined,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }

    fn nativeWire(self: *@This(), ctx: *fixture.Context) !void {
        const frames = @import("../pgwire/cursor_reference_test_support.zig");
        const protocol = @import("../pgwire/protocol.zig");
        const a = ctx.alloc;
        var input: std.Io.Writer.Allocating = .init(a);
        defer input.deinit();
        try frames.startup(a, &input.writer, "pk-rewriter", "pk-rewrite-durable-password", "default");
        var expected: std.ArrayList([]const u8) = .empty;
        defer expected.deinit(a);
        for (self.reference.actions) |action| {
            const text = try std.fmt.allocPrint(a, "{s}\x00", .{action.sql});
            defer a.free(text);
            try frames.frame(&input.writer, 'Q', text);
            for (action.rows) |row| try expected.append(a, row[0]);
        }
        const extra = [_][]const u8{
            "SET antfly.sync_level='full_index'",       "BEGIN",
            "INSERT INTO sync_a VALUES('wire',77)",     "INSERT INTO sync_b VALUES('wire',77)",
            "SET LOCAL antfly.sync_level='propose'",    "SHOW antfly.sync_level",
            "COMMIT",                                   "SHOW antfly.sync_level",
            "SET antfly.sync_level='full_text'",        "BEGIN",
            "SET LOCAL antfly.sync_level='full_index'", "INSERT INTO sync_a VALUES('wire2',88)",
            "INSERT INTO sync_b VALUES('wire2',88)",    "COMMIT",
            "SHOW antfly.sync_level",                   "RESET ALL",
            "SHOW antfly.sync_level",
        };
        for (extra) |statement| {
            const text = try std.fmt.allocPrint(a, "{s}\x00", .{statement});
            defer a.free(text);
            try frames.frame(&input.writer, 'Q', text);
        }
        try expected.appendSlice(a, &.{ "propose", "full_index", "full_text", "write" });
        try frames.frame(&input.writer, 'X', "");
        var reader = std.Io.Reader.fixed(input.written());
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        const api = if (ctx.data.http_server) |*api| api else return error.MissingDataApi;
        var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
        var session: protocol.Session = .{ .alloc = a, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer, .limits = .{ .statement_timeout_ms = 15_000 } };
        defer session.deinit();
        try session.run();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var rows: usize = 0;
        var commands: usize = 0;
        while (cursor.offset < cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const length = try cursor.int(u32);
            var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
            if (tag == 'E') {
                std.debug.print("native sync pgwire failure command={d} payload={s}\n", .{ commands, payload.bytes });
                return error.NativeSyncWireFailure;
            }
            if (tag == 'C') commands += 1;
            if (tag == 'D') {
                try std.testing.expect(rows < expected.items.len);
                try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
                try std.testing.expectEqualStrings(expected.items[rows], try payload.take(try payload.int(u32)));
                rows += 1;
            }
        }
        try std.testing.expectEqual(self.reference.actions.len + extra.len, commands);
        try std.testing.expectEqual(expected.items.len, rows);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        self.connection = try open(ctx);
        for (self.reference.actions) |action| {
            var response = try send(ctx, &self.connection, action.sql, if (action.rows.len == 0) .never else .session_observation);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("sync lifetime SQL={s} status={d} body={s}\n", .{ action.sql, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(Json, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            const object = parsed.value.object;
            try std.testing.expectEqualStrings(action.command_tag, object.get("command_tag").?.string);
            const rows = object.get("rows").?.array.items;
            try std.testing.expectEqual(action.rows.len, rows.len);
            for (rows, action.rows) |row, expected| {
                try std.testing.expectEqual(expected.len, row.array.items.len);
                for (row.array.items, expected) |value, text| try std.testing.expectEqualStrings(text, value.string);
            }
            if (action.notices.len != 0) {
                const notices = object.get("notices") orelse return error.MissingSqlNotice;
                try std.testing.expectEqual(action.notices.len, notices.array.items.len);
                for (notices.array.items, action.notices) |notice, code| try std.testing.expectEqualStrings(code, notice.object.get("code").?.string);
            }
        }
        _ = try command(ctx, &self.connection, "CREATE TABLE sync_a(id TEXT PRIMARY KEY, value BIGINT)", "CREATE TABLE");
        _ = try command(ctx, &self.connection, "CREATE TABLE sync_b(id TEXT PRIMARY KEY, value BIGINT)", "CREATE TABLE");
        for ([_][]const u8{ "sync_a", "sync_b" }) |name| {
            const serving = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
        }
        for (std.enums.values(local.storage_db_types.SyncLevel), 0..) |level, index| {
            const setting = try std.fmt.allocPrint(ctx.alloc, "SET antfly.sync_level='{s}'", .{local.storage_db_types.publicSyncLevelText(level)});
            defer ctx.alloc.free(setting);
            _ = try command(ctx, &self.connection, setting, "SET");
            const id = (try command(ctx, &self.connection, "BEGIN", "BEGIN")).?;
            for ([_][]const u8{ "sync_a", "sync_b" }) |table| {
                const insert = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO {s}(id,value) VALUES('row{d}',{d})", .{ table, index, index });
                defer ctx.alloc.free(insert);
                _ = try command(ctx, &self.connection, insert, "INSERT");
            }
            _ = try command(ctx, &self.connection, "COMMIT", "COMMIT");
            const api = if (ctx.data.http_server) |*api| api else return error.MissingDataApi;
            const frozen = api.txn_sessions.getInfo(id) orelse return error.MissingCommitProof;
            try std.testing.expectEqual(if (level == .propose) .write else level, frozen.sync_level);
        }
        try self.nativeWire(ctx);
        var invalid = try send(ctx, &self.connection, "SET antfly.sync_level='aknn'", .never);
        defer invalid.deinit(ctx.alloc);
        try std.testing.expect(invalid.status >= 400);
        try show(ctx, &self.connection, "SELECT current_setting('antfly.sync_level')", "full_index");
        _ = try command(ctx, &self.connection, "SET antfly.sync_level='full_text'", "SET");
        self.cold_txn = (try command(ctx, &self.connection, "BEGIN", "BEGIN")).?;
        _ = try command(ctx, &self.connection, "SET LOCAL antfly.sync_level='full_index'", "SET");
        _ = try command(ctx, &self.connection, "INSERT INTO sync_a VALUES('cold',99)", "INSERT");
        _ = try command(ctx, &self.connection, "INSERT INTO sync_b VALUES('cold',99)", "INSERT");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        // A different key for the same username cannot inherit a saved
        // connection. Preserve the originating credential across recovery.
        var unrelated: client.ApiKeyHeaders = .{};
        try unrelated.activateWithCatalog(ctx, true);
        var denied = try send(ctx, &self.connection, "SHOW antfly.sync_level", .never);
        defer denied.deinit(ctx.alloc);
        try std.testing.expect(denied.status >= 400);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, denied.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("08003", diagnostic.value.code);
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try show(ctx, &self.connection, "SHOW antfly.sync_level", "full_index");
        _ = try command(ctx, &self.connection, "COMMIT", "COMMIT");
        const api = if (ctx.data.http_server) |*api| api else return error.MissingDataApi;
        try std.testing.expectEqual(.full_index, api.txn_sessions.getInfo(self.cold_txn).?.sync_level);
        try show(ctx, &self.connection, "SHOW antfly.sync_level", "full_text");
        try show(ctx, &self.connection, "SELECT COUNT(*) FROM sync_a", "8");
        try show(ctx, &self.connection, "SELECT COUNT(*) FROM sync_b", "8");
        _ = try command(ctx, &self.connection, "DISCARD ALL", "DISCARD ALL");
        try show(ctx, &self.connection, "SHOW antfly.sync_level", "write");
    }
};

test "mounted SQL native sync controls preserve original SET lifetimes and durable multi-owner visibility choices" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.sync_control_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0774", "sql-0775" }, reference.value.cases) |id, entry| {
        try std.testing.expectEqualStrings(id, entry.id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, entry.sql);
    }
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}

test "mounted SQL native sync controls remain available without authentication and recover local overlays" {
    const Anonymous = struct {
        pub const native_sessions = true;
        pub const public_anonymous = true;
        pub const cold_catalog = true;
        connection: [32]u8 = undefined,
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            ctx.headers = &.{};
            ctx.admin_headers = &.{};
            self.connection = try open(ctx);
            try show(ctx, &self.connection, "SHOW antfly.sync_level", "write");
            _ = try command(ctx, &self.connection, "SET antfly.sync_level='full_index'", "SET");
            try show(ctx, &self.connection, "SELECT current_setting('antfly.sync_level')", "full_index");
            _ = try command(ctx, &self.connection, "BEGIN", "BEGIN");
            _ = try command(ctx, &self.connection, "SET LOCAL antfly.sync_level='propose'", "SET");
            try show(ctx, &self.connection, "SHOW antfly.sync_level", "propose");
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            ctx.headers = &.{};
            ctx.admin_headers = &.{};
            try show(ctx, &self.connection, "SHOW antfly.sync_level", "propose");
            _ = try command(ctx, &self.connection, "ROLLBACK", "ROLLBACK");
            try show(ctx, &self.connection, "SHOW antfly.sync_level", "full_index");
            _ = try command(ctx, &self.connection, "RESET antfly.sync_level", "RESET");
            try show(ctx, &self.connection, "SHOW antfly.sync_level", "write");
            var response = try send(ctx, &self.connection, "SET app.unregistered='x'", .never);
            defer response.deinit(ctx.alloc);
            try std.testing.expect(response.status >= 400);
        }
    };
    try fixture.runWithState(Anonymous, .{});
}
