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

//! Public SQL lock lifetimes use independent authenticated connections and
//! native metadata/data owners. Never retry statements containing lock effects.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const Json = std.json.Value;

fn send(ctx: *fixture.Context, connection: []const u8, sql: []const u8) !http.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    return client.executeConnected(&authenticated, null, connection, sql, .never);
}

fn command(ctx: *fixture.Context, connection: []const u8, sql: []const u8, tag: []const u8) !void {
    var response = try send(ctx, connection, sql);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("locking SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    var parsed = try std.json.parseFromSlice(struct { command_tag: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(tag, parsed.value.command_tag);
}

fn scalar(ctx: *fixture.Context, connection: []const u8, sql: []const u8, expected: Json) !void {
    var response = try send(ctx, connection, sql);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("locking scalar={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { rows: []const []const Json, notices: []const struct { code: []const u8, message: []const u8 } = &.{} }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows[0].len);
    const value = parsed.value.rows[0][0];
    try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(value));
    switch (expected) {
        .bool => |v| {
            try std.testing.expectEqual(v, value.bool);
            if (!v and (std.mem.startsWith(u8, sql, "SELECT pg_advisory_unlock(") or std.mem.startsWith(u8, sql, "SELECT pg_advisory_unlock_shared("))) {
                try std.testing.expectEqual(@as(usize, 1), parsed.value.notices.len);
                try std.testing.expectEqualStrings("01000", parsed.value.notices[0].code);
                try std.testing.expectEqualStrings(if (std.mem.startsWith(u8, sql, "SELECT pg_advisory_unlock_shared(")) "you don't own a lock of type ShareLock" else "you don't own a lock of type ExclusiveLock", parsed.value.notices[0].message);
            }
        },
        .string => |v| try std.testing.expectEqualStrings(v, value.string),
        .null => {},
        else => return error.InvalidLockExpectation,
    }
}

fn open(ctx: *fixture.Context) ![32]u8 {
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}");
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 32), parsed.value.connection_id.len);
    return parsed.value.connection_id[0..32].*;
}

const AdvisoryCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    holder: [32]u8 = undefined,
    contender: [32]u8 = undefined,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        self.holder = try open(ctx);
        self.contender = try open(ctx);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        // Execute the original statements, preserving their signed overloads
        // and actual void/bool values rather than replacing them with probes.
        try scalar(ctx, &self.holder, (try corpus.get("sql-0089")).sql, .{ .string = "" });
        try scalar(ctx, &self.holder, (try corpus.get("sql-0090")).sql, .{ .string = "" });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(42)", .{ .bool = false });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(-42,7)", .{ .bool = false });
        try scalar(ctx, &self.holder, (try corpus.get("sql-0091")).sql, .{ .bool = true });
        try scalar(ctx, &self.holder, "SELECT pg_advisory_unlock(-42,7)", .{ .bool = true });
        try self.matrix(ctx);
        try self.transactionLifetimes(ctx);
        try self.wire(ctx);
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(99003)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        // A durable session grant must remain observable through cold control
        // owners and may only be released through the original logical owner.
        try scalar(ctx, &self.holder, "SELECT pg_advisory_lock(99001)", .{ .string = "" });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(99001)", .{ .bool = false });
    }

    fn matrix(self: *@This(), ctx: *fixture.Context) !void {
        for ([_][]const u8{ "51", "-51,7" }) |key| {
            for ([_]bool{ false, true }) |shared| {
                const suffix = if (shared) "_shared" else "";
                const acquire = try std.fmt.allocPrint(ctx.alloc, "SELECT pg_advisory_lock{s}({s})", .{ suffix, key });
                defer ctx.alloc.free(acquire);
                const unlock = try std.fmt.allocPrint(ctx.alloc, "SELECT pg_advisory_unlock{s}({s})", .{ suffix, key });
                defer ctx.alloc.free(unlock);
                try scalar(ctx, &self.holder, acquire, .{ .string = "" });
                try scalar(ctx, &self.holder, acquire, .{ .string = "" });
                for ([_]bool{ false, true }) |requested_shared| {
                    const attempt = try std.fmt.allocPrint(ctx.alloc, "SELECT pg_try_advisory_lock{s}({s})", .{ if (requested_shared) "_shared" else "", key });
                    defer ctx.alloc.free(attempt);
                    try scalar(ctx, &self.contender, attempt, .{ .bool = shared and requested_shared });
                    try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
                }
                try scalar(ctx, &self.holder, unlock, .{ .bool = true });
                const attempt = try std.fmt.allocPrint(ctx.alloc, "SELECT pg_try_advisory_lock({s})", .{key});
                defer ctx.alloc.free(attempt);
                try scalar(ctx, &self.contender, attempt, .{ .bool = false });
                try scalar(ctx, &self.holder, unlock, .{ .bool = true });
                try scalar(ctx, &self.holder, unlock, .{ .bool = false });
                try scalar(ctx, &self.contender, attempt, .{ .bool = true });
                try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
            }
        }
        try scalar(ctx, &self.holder, "SELECT pg_advisory_lock(1,2)", .{ .string = "" });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(4294967298)", .{ .bool = true });
        try scalar(ctx, &self.holder, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        try scalar(ctx, &self.holder, "SELECT CASE WHEN false THEN pg_advisory_lock(52) ELSE NULL::void END", .null);
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(52)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        try scalar(ctx, &self.holder, "SELECT pg_advisory_lock(NULL::bigint)", .null);
    }

    fn transactionLifetimes(self: *@This(), ctx: *fixture.Context) !void {
        for ([_][]const u8{ "COMMIT", "ROLLBACK" }) |finish| {
            try command(ctx, &self.holder, "BEGIN", "BEGIN");
            try scalar(ctx, &self.holder, "SELECT pg_advisory_xact_lock(53)", .{ .string = "" });
            try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(53)", .{ .bool = false });
            try command(ctx, &self.holder, "SAVEPOINT inner_lock", "SAVEPOINT");
            try scalar(ctx, &self.holder, "SELECT pg_advisory_xact_lock(54)", .{ .string = "" });
            try command(ctx, &self.holder, "ROLLBACK TO SAVEPOINT inner_lock", "ROLLBACK");
            try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(54)", .{ .bool = true });
            try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
            try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(53)", .{ .bool = false });
            // Session locks survive transaction rollback; xact grants do not.
            try scalar(ctx, &self.holder, "SELECT pg_advisory_lock(55)", .{ .string = "" });
            try command(ctx, &self.holder, finish, finish);
            try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(53)", .{ .bool = true });
            try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(55)", .{ .bool = false });
            try scalar(ctx, &self.holder, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
            try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        }
        try scalar(ctx, &self.holder, "SELECT pg_advisory_xact_lock(56)", .{ .string = "" });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(56)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        try scalar(ctx, &self.holder, "SELECT pg_advisory_lock(57)", .{ .string = "" });
        try command(ctx, &self.holder, "DISCARD ALL", "DISCARD ALL");
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(57)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
    }

    fn wire(_: *@This(), ctx: *fixture.Context) !void {
        const frames = @import("../pgwire/cursor_reference_test_support.zig");
        const protocol = @import("../pgwire/protocol.zig");
        var input: std.Io.Writer.Allocating = .init(ctx.alloc);
        defer input.deinit();
        try frames.startup(ctx.alloc, &input.writer, "pk-rewriter", "pk-rewrite-durable-password", "default");
        for ([_][]const u8{ "SELECT pg_advisory_lock(99002)", "SELECT pg_advisory_unlock(99002)", "SELECT pg_advisory_lock(99003)" }) |sql| {
            const text = try std.fmt.allocPrint(ctx.alloc, "{s}\x00", .{sql});
            defer ctx.alloc.free(text);
            try frames.frame(&input.writer, 'Q', text);
        }
        try frames.frame(&input.writer, 'X', "");
        var output: std.Io.Writer.Allocating = .init(ctx.alloc);
        defer output.deinit();
        var reader: std.Io.Reader = .fixed(input.written());
        const api = if (ctx.data.http_server) |*value| value else return error.MissingDataApi;
        var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
        var session: protocol.Session = .{ .alloc = ctx.alloc, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer, .limits = .{ .statement_timeout_ms = 15_000 } };
        defer session.deinit();
        try session.run();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var rows: usize = 0;
        var descriptions: usize = 0;
        while (cursor.offset < cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const length = try cursor.int(u32);
            var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
            if (tag == 'E') return error.LockWireFailure;
            if (tag == 'T') {
                try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
                _ = try payload.string();
                _ = try payload.int(u32);
                _ = try payload.int(u16);
                try std.testing.expectEqual(@as(u32, if (descriptions == 1) 16 else 2278), try payload.int(u32));
                descriptions += 1;
            }
            if (tag == 'D') {
                try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
                try std.testing.expectEqualStrings(if (rows == 1) "t" else "", try payload.take(try payload.int(u32)));
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 3), descriptions);
        try std.testing.expectEqual(@as(usize, 3), rows);
        // Disconnect makes bounded durable progress, and can hand remaining
        // control to upkeep after a lost reply or concurrent metadata write.
        // Observe that completion before asserting reuse from another owner;
        // never retry the advisory SQL to wait for cleanup.
        const identity = session.identity orelse return error.MissingWireIdentity;
        const backend = adapter.backend();
        const complete = backend.vtable.completion_safe orelse return error.MissingWireCompletion;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
        while (true) {
            backend.vtable.disconnect(backend.context, identity, session.session_id);
            if (complete(backend.context, identity)) break;
            try ctx.checkDrivers();
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.WireDisconnectNotRetired;
            try ctx.io.sleep(.fromMilliseconds(5), .awake);
        }
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(99001)", .{ .bool = false });
        try scalar(ctx, &self.holder, "SELECT pg_advisory_unlock(99001)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(99001)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
    }
};

test "mounted SQL original advisory locks enforce public contention overloads transaction savepoint discard and cold lifetimes" {
    // sql-0089, sql-0090 and sql-0091 execute unchanged in beforeRestart.
    try fixture.run(AdvisoryCampaign);
}
