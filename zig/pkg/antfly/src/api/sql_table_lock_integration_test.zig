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

//! Exact original table/row statements through authenticated SQL connections
//! and cold native owners. No lock-bearing SQL statement is replayed.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const Json = std.json.Value;
fn send(ctx: *fixture.Context, connection: []const u8, sql: []const u8) !http.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    return client.executeConnected(&authenticated, null, connection, sql, if (std.mem.startsWith(u8, sql, "CREATE TABLE")) .write_admission else .never);
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

fn rows(ctx: *fixture.Context, connection: []const u8, sql: []const u8, expected: []const []const u8) !void {
    var response = try send(ctx, connection, sql);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("table/row lock SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { rows: []const []const Json }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    for (expected) |value| {
        var count: usize = 0;
        for (parsed.value.rows) |row| {
            try std.testing.expectEqual(@as(usize, 1), row.len);
            try std.testing.expect(row[0] == .string);
            if (std.mem.eql(u8, value, row[0].string)) count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), count);
    }
}
fn unavailable(ctx: *fixture.Context, connection: []const u8, sql: []const u8) !void {
    var response = try send(ctx, connection, sql);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("expected 55P03 SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("55P03", parsed.value.code);
}
const Campaign = struct {
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
        try command(ctx, &self.holder, "CREATE TABLE usage_records(id TEXT PRIMARY KEY, status TEXT)", "CREATE TABLE");
        try command(ctx, &self.holder, "CREATE TABLE archived_records(id TEXT PRIMARY KEY, status TEXT)", "CREATE TABLE");
        try command(ctx, &self.holder, "INSERT INTO usage_records VALUES('a','queued'),('b','queued'),('c','queued')", "INSERT");
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for ([_][]const u8{ "sql-0080", "sql-0081" }) |id| {
            try command(ctx, &self.holder, "BEGIN", "BEGIN");
            try command(ctx, &self.holder, (try corpus.get(id)).sql, "LOCK TABLE");
            try command(ctx, &self.contender, "BEGIN", "BEGIN");
            try unavailable(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT");
            try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
            try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
            try command(ctx, &self.contender, "BEGIN", "BEGIN");
            try command(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
            try command(ctx, &self.contender, "COMMIT", "COMMIT");
        }
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try rows(ctx, &self.holder, (try corpus.get("sql-0173")).sql, &.{"a"});
        try rows(ctx, &self.contender, (try corpus.get("sql-0173")).sql, &.{"b"});
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        for ([_][]const u8{ "sql-0433", "sql-0434", "sql-0435", "sql-0436" }) |id| {
            try command(ctx, &self.holder, "BEGIN", "BEGIN");
            try rows(ctx, &self.holder, (try corpus.get(id)).sql, &.{ "a", "b", "c" });
            try command(ctx, &self.contender, "BEGIN", "BEGIN");
            try unavailable(ctx, &self.contender, "SELECT id FROM usage_records WHERE status='queued' FOR UPDATE NOWAIT");
            try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
            try command(ctx, &self.holder, "COMMIT", "COMMIT");
        }
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE", &.{"a"});
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
        try rows(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT", &.{"a"});
    }
};
test "mounted SQL original table and row locks execute all seven cases with contention skip locked transaction release and cold recovery" {
    // sql-0080, sql-0081, sql-0173, sql-0433, sql-0434, sql-0435 and
    // sql-0436 execute unchanged in beforeRestart, then retain a row across
    // complete metadata/session/data restart in afterRestart.
    try fixture.run(Campaign);
}

const LifecycleCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    holder: [32]u8 = undefined,
    contender: [32]u8 = undefined,
    pub const configureAuth = Campaign.configureAuth;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        self.holder = try open(ctx);
        self.contender = try open(ctx);
        try command(ctx, &self.holder, "CREATE TABLE usage_records(id TEXT PRIMARY KEY, status TEXT)", "CREATE TABLE");
        // Observe the installed native reader before the first mutation. This
        // waits for real routing/schema readiness without replaying writes.
        try rows(ctx, &self.holder, "SELECT id FROM usage_records", &.{});
        try command(ctx, &self.holder, "INSERT INTO usage_records VALUES('a','queued'),('b','queued')", "INSERT");
        // A claimed row must remain writable by its own SQL transaction, with
        // native self-exclusion derived from the actual durable decision.
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE", &.{"a"});
        try command(ctx, &self.holder, "UPDATE usage_records SET status='processing' WHERE id='a'", "UPDATE");
        try command(ctx, &self.holder, "COMMIT", "COMMIT");
        try rows(ctx, &self.contender, "SELECT status FROM usage_records WHERE id='a'", &.{"processing"});
        // Rolling back a child releases only its grants; a parent grant must
        // remain contended, while a released child grant promotes to parent.
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE", &.{"a"});
        try command(ctx, &self.holder, "SAVEPOINT child_lock", "SAVEPOINT");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='b' FOR UPDATE", &.{"b"});
        try command(ctx, &self.holder, "ROLLBACK TO SAVEPOINT child_lock", "ROLLBACK");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try rows(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='b' FOR UPDATE NOWAIT", &.{"b"});
        try unavailable(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='b' FOR UPDATE", &.{"b"});
        try command(ctx, &self.holder, "RELEASE SAVEPOINT child_lock", "RELEASE");
        try command(ctx, &self.holder, "SAVEPOINT later_lock", "SAVEPOINT");
        try command(ctx, &self.holder, "ROLLBACK TO SAVEPOINT later_lock", "ROLLBACK");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='b' FOR UPDATE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try rows(ctx, &self.contender, "SELECT id FROM usage_records ORDER BY id FOR UPDATE NOWAIT", &.{ "a", "b" });
    }
};

test "mounted SQL original table and row locks maintain native SQL write ownership and savepoint lifetimes" {
    try fixture.run(LifecycleCampaign);
}

const PreparedCustodyCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    holder: [32]u8 = undefined,
    contender: [32]u8 = undefined,
    pub const configureAuth = Campaign.configureAuth;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        self.holder = try open(ctx);
        self.contender = try open(ctx);
        try command(ctx, &self.holder, "CREATE TABLE usage_records(id TEXT PRIMARY KEY, status TEXT)", "CREATE TABLE");
        try command(ctx, &self.holder, "CREATE TABLE archived_records(id TEXT PRIMARY KEY, status TEXT)", "CREATE TABLE");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records", &.{});
        try command(ctx, &self.holder, "INSERT INTO usage_records VALUES('a','queued')", "INSERT");
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records FOR UPDATE", &.{"a"});
        try command(ctx, &self.holder, "UPDATE usage_records SET status='prepared' WHERE id='a'", "UPDATE");
        // This second owner contributes only a table lock, not a row write.
        try command(ctx, &self.holder, "LOCK TABLE archived_records IN SHARE MODE", "LOCK TABLE");
        try command(ctx, &self.holder, "PREPARE TRANSACTION 'native_lock_custody'", "PREPARE TRANSACTION");
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{self.holder});
        defer ctx.alloc.free(path);
        var closed = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .DELETE, "");
        defer closed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), closed.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "SELECT id FROM usage_records FOR UPDATE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "LOCK TABLE archived_records IN ACCESS EXCLUSIVE MODE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.contender, "COMMIT PREPARED 'native_lock_custody'", "COMMIT PREPARED");
        try rows(ctx, &self.contender, "SELECT status FROM usage_records FOR UPDATE NOWAIT", &.{"prepared"});
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try command(ctx, &self.contender, "LOCK TABLE archived_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
        try command(ctx, &self.contender, "COMMIT", "COMMIT");
    }
};
test "mounted SQL original table and row locks transfer native prepared custody across source close and cold recovery" {
    try fixture.run(PreparedCustodyCampaign);
}

const ReadBarrierCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    holder: [32]u8 = undefined,
    contender: [32]u8 = undefined,
    pub const configureAuth = Campaign.configureAuth;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        self.holder = try open(ctx);
        self.contender = try open(ctx);
        try command(ctx, &self.holder, "CREATE TABLE usage_records(id TEXT PRIMARY KEY)", "CREATE TABLE");
        try command(ctx, &self.holder, "BEGIN READ ONLY", "BEGIN");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records", &.{});
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "COMMIT", "COMMIT");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try command(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
        const headers = try ctx.alloc.alloc(http.RequestHeader, ctx.admin_headers.len + 1);
        defer ctx.alloc.free(headers);
        @memcpy(headers[0..ctx.admin_headers.len], ctx.admin_headers);
        // Include Debug catalog/read-index admission in this budget. A 1s
        // whole-request timeout can expire before the native waiter is queued,
        // exercising availability (53300) instead of lock cancellation (57014).
        headers[ctx.admin_headers.len] = .{ .name = @import("antfly_public_limits").Execution.timeout_header, .value = "5000" };
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .connection_id = self.holder, .statement = "SELECT id FROM usage_records" }, .{});
        defer ctx.alloc.free(body);
        var blocked = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, headers, ctx.base, "/db/v1/sql", .POST, body, 10000);
        defer blocked.deinit(ctx.alloc);
        var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, blocked.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("57014", parsed.value.code);
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records", &.{});
        // Explicit read ownership survives the same complete process restart
        // as row and table locks, including READ ONLY transaction semantics.
        try command(ctx, &self.holder, "BEGIN READ ONLY", "BEGIN");
        try rows(ctx, &self.holder, "SELECT id FROM usage_records", &.{});
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try command(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
        try command(ctx, &self.contender, "COMMIT", "COMMIT");
    }
};
test "mounted SQL original table and row locks retain ordinary read barriers through delivery transaction and restart" {
    try fixture.run(ReadBarrierCampaign);
}

fn divisionFailure(ctx: *fixture.Context, connection: []const u8) !void {
    return statementFailure(ctx, connection, "SELECT 1/0", "22012");
}
fn statementFailure(ctx: *fixture.Context, connection: []const u8, sql: []const u8, code: []const u8) !void {
    var response = try send(ctx, connection, sql);
    defer response.deinit(ctx.alloc);
    var parsed = try std.json.parseFromSlice(struct { code: []const u8, transaction_status: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
    try std.testing.expectEqualStrings("failed", parsed.value.transaction_status);
}
const ErrorReleaseCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    holder: [32]u8 = undefined,
    contender: [32]u8 = undefined,
    pub const configureAuth = Campaign.configureAuth;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        self.holder = try open(ctx);
        self.contender = try open(ctx);
        try command(ctx, &self.holder, "CREATE TABLE usage_records(id TEXT PRIMARY KEY)", "CREATE TABLE");
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try command(ctx, &self.holder, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE", "LOCK TABLE");
        try scalar(ctx, &self.holder, "SELECT pg_advisory_xact_lock(610)", .{ .string = "" });
        try scalar(ctx, &self.holder, "SELECT pg_advisory_lock(611)", .{ .string = "" });
        try divisionFailure(ctx, &self.holder);
        // Failure releases transaction grants before an explicit ROLLBACK,
        // while session advisory ownership survives exactly as PostgreSQL's.
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try command(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_xact_lock(610)", .{ .bool = true });
        try scalar(ctx, &self.contender, "SELECT pg_try_advisory_lock(611)", .{ .bool = false });
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
        try scalar(ctx, &self.holder, "SELECT pg_advisory_unlock_all()", .{ .string = "" });
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try command(ctx, &self.holder, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE", "LOCK TABLE");
        try statementFailure(ctx, &self.holder, "SELECT id FROM usage_records FOR UPDATE OF missing", "42P01");
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try command(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        try command(ctx, &self.holder, "LOCK TABLE usage_records IN SHARE MODE", "LOCK TABLE");
        try command(ctx, &self.holder, "SAVEPOINT child", "SAVEPOINT");
        try command(ctx, &self.holder, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE", "LOCK TABLE");
        try divisionFailure(ctx, &self.holder);
        try command(ctx, &self.holder, "ROLLBACK TO child", "ROLLBACK");
        try command(ctx, &self.holder, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE", "LOCK TABLE");
        try statementFailure(ctx, &self.holder, "SELECT FROM", "42601");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        // Child ACCESS EXCLUSIVE is gone, parent SHARE remains after restart.
        try command(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS SHARE MODE NOWAIT", "LOCK TABLE");
        try unavailable(ctx, &self.contender, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK TO child", "ROLLBACK");
        try command(ctx, &self.holder, "LOCK TABLE usage_records IN ACCESS EXCLUSIVE MODE NOWAIT", "LOCK TABLE");
        try command(ctx, &self.holder, "RELEASE child", "RELEASE");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
    }
};
test "mounted SQL original table and row locks release failed root and child ownership before rollback across cold recovery" {
    try fixture.run(ErrorReleaseCampaign);
}
