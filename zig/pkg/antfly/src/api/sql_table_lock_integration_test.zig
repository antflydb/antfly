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
    const started = @import("antfly_platform").time.monotonicNs();
    var response = try send(ctx, connection, sql);
    errdefer std.debug.print("table/row lock failure elapsed_ms={d}\n", .{(@import("antfly_platform").time.monotonicNs() -| started) / std.time.ns_per_ms});
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
    authorization_server: *@import("../data/runtime.zig").DataServer = undefined,
    authorization_io: std.Io = undefined,
    authorization_checks: std.atomic.Value(usize) = .init(0),
    admission_failures: std.atomic.Value(usize) = .init(0),
    lose_reply: std.atomic.Value(bool) = .init(false),
    fn observeAuthorization(raw: *anyopaque, event: @import("../data/runtime.zig").DataRequestLifecycleEvent) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // Recovery may submit controls concurrently. Only the selected SQL
        // connection's row acquisition may consume either injected fault.
        const target_row = if (event.sql_lock_command) |lock_command|
            lock_command.operation == .row and std.mem.eql(u8, &lock_command.admission.source.connection, &self.holder)
        else
            false;
        if (event.phase == .apply_confirmed and target_row and
            self.lose_reply.swap(false, .acq_rel)) return error.InjectedSqlLockReplyLost;
        if (event.phase != .sql_lock_authorization) return;
        const failures = self.admission_failures.load(.acquire);
        if (target_row and failures != 0) {
            self.admission_failures.store(failures - 1, .release);
            return error.StorageBusy;
        }
        const until = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
        while (!self.authorization_server.data_raft_mutex.tryLock()) {
            try std.testing.expect(@import("antfly_platform").time.monotonicNs() < until);
            try self.authorization_io.sleep(.fromMilliseconds(1), .awake);
        }
        self.authorization_server.data_raft_mutex.unlock();
        _ = self.authorization_checks.fetchAdd(1, .monotonic);
    }
    pub fn configureData(self: *@This(), data: *@import("../data/runtime.zig").DataServer, io: std.Io) !void {
        self.authorization_server = data;
        self.authorization_io = io;
        data.data_request_lifecycle_hook = .{ .ptr = self, .reach_fn = observeAuthorization };
    }
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
        try std.testing.expect(self.authorization_checks.load(.monotonic) > 0);
        ctx.admin_headers = &self.credentials.admin;
        ctx.reader_headers = &self.credentials.reader;
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try unavailable(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT");
        try command(ctx, &self.contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &self.holder, "ROLLBACK", "ROLLBACK");
        try rows(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT", &.{"a"});
        // Native admission retries retain one SQL evaluation and one effect
        // identity. A lost post-apply reply must instead expose uncertainty.
        try command(ctx, &self.holder, "BEGIN", "BEGIN");
        self.admission_failures.store(3, .release);
        try rows(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT", &.{"a"});
        try std.testing.expectEqual(@as(usize, 0), self.admission_failures.load(.acquire));
        self.lose_reply.store(true, .release);
        var lost = try send(ctx, &self.holder, "SELECT id FROM usage_records WHERE id='b' FOR UPDATE NOWAIT");
        defer lost.deinit(ctx.alloc);
        errdefer std.debug.print("lost lock reply status={d} body={s}\n", .{ lost.status, lost.body });
        try std.testing.expectEqual(@as(u16, 409), lost.status);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, transaction_id: ?[]const u8 = null, retryable: ?bool = null, transaction_status: ?[]const u8 = null }, ctx.alloc, lost.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("40003", diagnostic.value.code);
        try std.testing.expectEqual(@as(?bool, false), diagnostic.value.retryable);
        if (diagnostic.value.transaction_status) |status|
            try std.testing.expectEqualStrings("failed", status);
        try std.testing.expect(diagnostic.value.transaction_id != null);
        try std.testing.expectEqual(@as(usize, 32), diagnostic.value.transaction_id.?.len);
        try std.testing.expect(!self.lose_reply.load(.acquire));
        // Failed-root cleanup durably retires both the earlier grant and the
        // acquisition whose reply was lost. The error still carries the exact
        // owner receipt; the statement itself is never replayed.
        try command(ctx, &self.contender, "BEGIN", "BEGIN");
        try rows(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='a' FOR UPDATE NOWAIT", &.{"a"});
        try rows(ctx, &self.contender, "SELECT id FROM usage_records WHERE id='b' FOR UPDATE NOWAIT", &.{"b"});
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
        // Catalog/read-index admission can reject before the lock waiter is
        // queued. Only the shared exact nonadmission witness permits another
        // preparation attempt under the same outer budget. Once admitted, this
        // read must time out waiting on the real native lock and return 57014.
        headers[ctx.admin_headers.len] = .{ .name = @import("antfly_public_limits").Execution.timeout_header, .value = "5000" };
        var waiting = ctx.*;
        waiting.headers = headers;
        var blocked = try client.executeConnected(&waiting, null, &self.holder, "SELECT id FROM usage_records", .read);
        defer blocked.deinit(ctx.alloc);
        errdefer std.debug.print("blocked read cancellation status={d} body={s}\n", .{ blocked.status, blocked.body });
        var parsed = try std.json.parseFromSlice(struct { code: []const u8, transaction_id: ?[]const u8 = null }, ctx.alloc, blocked.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("57014", parsed.value.code);
        try std.testing.expect(parsed.value.transaction_id == null);
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

// Status sampling may resolve catalog routes or acquire storage owners. The
// committed apply callback must schedule it without entering that IO path.
test "mounted SQL original table and row locks defer observability outside Raft apply" {
    const writes = @import("table_writes.zig");
    const statuses = local.api_local_table_writes.runtime_status;
    const Probe = struct {
        raft: std.Io.Mutex = .init,
        samples: usize = 0,
        sampled_under_raft: bool = false,
        scheduled: usize = 0,
        applies: usize = 0,
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: local.storage_db_types.BatchRequest) !?void {
            return error.TestUnexpectedOrdinaryMutation;
        }
        fn apply(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: local.storage_db_types.BatchRequest, prepared: bool, entry: ?local.api_local_table_writes.db_mod.OrderedApplyReceipt) !?void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(prepared and entry != null);
            self.applies += 1;
            return {};
        }
        fn sample(raw: *anyopaque, _: std.mem.Allocator, _: []const u8) !?statuses.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.samples += 1;
            if (self.raft.tryLock()) self.raft.unlock(std.testing.io) else self.sampled_under_raft = true;
            return null;
        }
        fn changed(raw: *anyopaque, _: []const u8, kind: writes.ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (kind == .data) self.scheduled += 1;
        }
    };
    var probe: Probe = .{};
    var cache = statuses.TableRuntimeSnapshotCache.init(std.testing.allocator);
    defer cache.deinit();
    var write_cache = writes.ProvisionedTableWriteCache.init(std.testing.allocator);
    defer write_cache.deinit();
    var source = writes.ProvisionedTableWriteSource.init("/tmp/unused-sql-raft-status-guard", @import("table_catalog.zig").emptyCatalogSource());
    defer source.deinit();
    source.write_cache = &write_cache;
    const port: writes.TableWriteSource = .{ .ptr = &probe, .vtable = &.{ .batch = Probe.batch, .replicated_batch_group_local = Probe.apply, .local_runtime_statuses = Probe.sample } };
    source.local_write_source = port;
    source.runtime_status_cache = &cache;
    source.setLocalChangeHook(.{ .ptr = &probe, .on_change = Probe.changed });
    probe.raft.lockUncancelable(std.testing.io);
    source.publishStorageOwnerDataChange("docs", .{});
    probe.raft.unlock(std.testing.io);
    try std.testing.expectEqual(@as(usize, 0), probe.samples);
    try std.testing.expectEqual(@as(usize, 1), probe.scheduled);
    // Descriptor-free replay uses the same deferred status rule.
    probe.raft.lockUncancelable(std.testing.io);
    const applied = source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(std.testing.allocator, 7, "docs", .{}, .{ .term = 1, .index = 1 }) catch |err| {
        probe.raft.unlock(std.testing.io);
        return err;
    };
    probe.raft.unlock(std.testing.io);
    try std.testing.expect(applied != null);
    try std.testing.expectEqual(@as(usize, 1), probe.applies);
    try std.testing.expectEqual(@as(usize, 0), probe.samples);
    try std.testing.expectEqual(@as(usize, 2), probe.scheduled);
    // A row reader may retain cache ownership while waiting for Raft apply.
    // Pure lock controls must finish under that exact ownership inversion,
    // without invalidating unchanged rows or scheduling another status scan.
    const lock_only: local.storage_db_types.BatchRequest = .{ .sql_lock = .{
        .admission = std.mem.zeroes(@FieldType(local.storage_pessimistic_lock_control.Command, "admission")),
        .operation = .begin,
    } };
    {
        try std.testing.expect(source.local_db_mutex.shared == null);
        try std.testing.expect(source.local_db_mutex.owned.tryLock());
        defer source.local_db_mutex.owned.unlock();
        probe.raft.lockUncancelable(std.testing.io);
        defer probe.raft.unlock(std.testing.io);
        source.publishStorageOwnerDataChange("docs", lock_only);
        try std.testing.expect((try source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(std.testing.allocator, 7, "docs", lock_only, .{ .term = 1, .index = 2 })) != null);
    }
    try std.testing.expectEqual(@as(usize, 2), probe.applies);
    try std.testing.expectEqual(@as(usize, 0), probe.samples);
    try std.testing.expectEqual(@as(usize, 2), probe.scheduled);
    // The exact same owner/status capability is available to maintenance
    // after consensus ownership is released; the refresh was not discarded.
    try std.testing.expect((try port.localRuntimeStatuses(std.testing.allocator, "docs")) == null);
    try std.testing.expectEqual(@as(usize, 1), probe.samples);
    try std.testing.expect(!probe.sampled_under_raft);
}

test "SQL implicit lock cleanup preserves native admission across failure envelopes" {
    const identity = @import("kernel_error_identity");
    const failure = @import("runtime_failure_abi");
    const callbacks = @import("antfly_runtime_abi").error_abi;
    const status = identity.statusFromError(error.RaftApplyWriterUnavailable);
    try std.testing.expectEqual(failure.Status.raft_apply_writer_unavailable, status);
    try std.testing.expectEqual(@as(u32, 848), @backingInt(status));
    try std.testing.expectError(error.RaftApplyWriterUnavailable, identity.statusToError(status));
    const callback = callbacks.statusFromError(error.RaftApplyWriterUnavailable);
    try std.testing.expectEqual(@backingInt(callbacks.Code.retryable), callback.code);
    try std.testing.expectEqual(error.RaftApplyWriterUnavailable, callbacks.errorFromStatus(callback));
}

test "SQL implicit lock cleanup preserves the caller integrity preparation budget" {
    try local.api_relational_integrity_commit.Testing.scopedPreparationBudget();
}
