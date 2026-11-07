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

//! Exact notification originals through authenticated HTTP and native recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const http = @import("../raft/transport/http_common.zig");

fn open(ctx: *fixture.Context) ![32]u8 {
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}");
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return parsed.value.connection_id[0..32].*;
}
fn execute(ctx: *fixture.Context, connection: [32]u8, sql: []const u8) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    var response = try client.executeConnected(&authenticated, null, &connection, sql, .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("notification SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
}
fn page(ctx: *fixture.Context, connection: [32]u8, ack: ?[]const u8, limit: u16) !std.json.Parsed(wire.SQLNotificationPage) {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}/notifications", .{connection});
    defer ctx.alloc.free(path);
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .acknowledge = ack, .limit = limit }, .{ .emit_null_optional_fields = false });
    defer ctx.alloc.free(body);
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .POST, body);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("notification poll status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    return std.json.parseFromSlice(wire.SQLNotificationPage, ctx.alloc, response.body, .{ .allocate = .alloc_always });
}
fn empty(ctx: *fixture.Context, connection: [32]u8) !void {
    var result = try page(ctx, connection, null, 128);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.value.notifications.len);
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    listener: [32]u8 = undefined,
    sender: [32]u8 = undefined,
    cold_cursor: [20]u8 = undefined,
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
        self.listener = try open(ctx);
        self.sender = try open(ctx);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        try execute(ctx, self.listener, (try corpus.get("sql-0126")).sql);
        try execute(ctx, self.sender, (try corpus.get("sql-0127")).sql);
        var first = try page(ctx, self.listener, null, 128);
        defer first.deinit();
        try std.testing.expectEqual(@as(usize, 1), first.value.notifications.len);
        try std.testing.expectEqualStrings("usage_events", first.value.notifications[0].channel);
        try std.testing.expectEqualStrings("updated", first.value.notifications[0].payload);
        try std.testing.expect(first.value.notifications[0].sender_pid > 0);
        var replay = try page(ctx, self.listener, null, 128);
        defer replay.deinit();
        try std.testing.expectEqualStrings(first.value.cursor.?, replay.value.cursor.?);
        try std.testing.expectEqualStrings(first.value.notifications[0].payload, replay.value.notifications[0].payload);
        var consumed = try page(ctx, self.listener, first.value.cursor, 128);
        defer consumed.deinit();
        try std.testing.expectEqual(@as(usize, 0), consumed.value.notifications.len);

        try execute(ctx, self.listener, "BEGIN");
        try execute(ctx, self.sender, "BEGIN");
        try execute(ctx, self.sender, "NOTIFY usage_events, 'first'");
        try execute(ctx, self.sender, "SAVEPOINT child");
        try execute(ctx, self.sender, "NOTIFY usage_events, 'removed'");
        try execute(ctx, self.sender, "ROLLBACK TO child");
        try execute(ctx, self.sender, "NOTIFY usage_events, 'first'");
        try execute(ctx, self.sender, "NOTIFY usage_events, 'second'");
        try execute(ctx, self.sender, "RELEASE child");
        try execute(ctx, self.sender, "COMMIT");
        {
            var deferred = try page(ctx, self.listener, null, 128);
            defer deferred.deinit();
            try std.testing.expectEqual(@as(usize, 0), deferred.value.notifications.len);
            try std.testing.expect(deferred.value.cursor == null);
        }
        try execute(ctx, self.listener, "ROLLBACK");
        var ordered = try page(ctx, self.listener, null, 1);
        defer ordered.deinit();
        try std.testing.expectEqual(@as(usize, 1), ordered.value.notifications.len);
        try std.testing.expectEqualStrings("first", ordered.value.notifications[0].payload);
        var next = try page(ctx, self.listener, ordered.value.cursor, 1);
        defer next.deinit();
        try std.testing.expectEqual(@as(usize, 1), next.value.notifications.len);
        try std.testing.expectEqualStrings("second", next.value.notifications[0].payload);
        var done = try page(ctx, self.listener, next.value.cursor, 128);
        defer done.deinit();
        try std.testing.expectEqual(@as(usize, 0), done.value.notifications.len);

        try execute(ctx, self.sender, "BEGIN");
        try execute(ctx, self.sender, "NOTIFY usage_events, 'rolled-back'");
        try execute(ctx, self.sender, "ROLLBACK");
        try empty(ctx, self.listener);
        try execute(ctx, self.listener, "BEGIN");
        try execute(ctx, self.listener, (try corpus.get("sql-0128")).sql);
        try execute(ctx, self.listener, "ROLLBACK");
        try self.wire(ctx);
        try execute(ctx, self.sender, "NOTIFY usage_events, 'cold'");
        var cold = try page(ctx, self.listener, null, 128);
        defer cold.deinit();
        try std.testing.expectEqual(@as(usize, 1), cold.value.notifications.len);
        try std.testing.expectEqualStrings("cold", cold.value.notifications[0].payload);
        @memcpy(&self.cold_cursor, cold.value.cursor.?);
    }
    fn wire(_: *@This(), ctx: *fixture.Context) !void {
        const frames = @import("../pgwire/cursor_reference_test_support.zig");
        const protocol = @import("../pgwire/protocol.zig");
        var input: std.Io.Writer.Allocating = .init(ctx.alloc);
        defer input.deinit();
        try frames.startup(ctx.alloc, &input.writer, "pk-rewriter", "pk-rewrite-durable-password", "default");
        for ([_][]const u8{ "LISTEN wire_events", "NOTIFY wire_events, 'wire'", "DISCARD ALL", "NOTIFY wire_events, 'ignored'", "LISTEN wire_events", "NOTIFY wire_events, 'after-discard'", "UNLISTEN wire_events" }) |sql| {
            const text = try std.fmt.allocPrint(ctx.alloc, "{s}\x00", .{sql});
            defer ctx.alloc.free(text);
            try frames.frame(&input.writer, 'Q', text);
        }
        try frames.frame(&input.writer, 'X', "");
        var output: std.Io.Writer.Allocating = .init(ctx.alloc);
        defer output.deinit();
        var reader = std.Io.Reader.fixed(input.written());
        const api = if (ctx.data.http_server) |*server| server else return error.MissingDataApi;
        var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
        var session: protocol.Session = .{ .alloc = ctx.alloc, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer, .backend_pid = 44321, .limits = .{ .statement_timeout_ms = 15000 } };
        defer session.deinit();
        try session.run();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var events: usize = 0;
        var commands: usize = 0;
        while (cursor.offset < cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const count = try cursor.int(u32);
            var body: protocol.Cursor = .{ .bytes = try cursor.take(count - 4) };
            if (tag == 'E') {
                std.debug.print("native wire notification error after {d} commands: {s}\n", .{ commands, body.bytes });
                return error.NotificationWireFailure;
            }
            if (tag == 'C') commands += 1;
            if (tag == 'A') {
                try std.testing.expectEqual(@as(i32, 44321), try body.int(i32));
                try std.testing.expectEqualStrings("wire_events", try body.string());
                try std.testing.expectEqualStrings(if (events == 0) "wire" else "after-discard", try body.string());
                events += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 7), commands);
        try std.testing.expectEqual(@as(usize, 2), events);
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
        var cold = try page(ctx, self.listener, null, 128);
        defer cold.deinit();
        try std.testing.expectEqual(@as(usize, 1), cold.value.notifications.len);
        try std.testing.expectEqualStrings("cold", cold.value.notifications[0].payload);
        try std.testing.expectEqualStrings(&self.cold_cursor, cold.value.cursor.?);
        var consumed = try page(ctx, self.listener, &self.cold_cursor, 128);
        defer consumed.deinit();
        try std.testing.expectEqual(@as(usize, 0), consumed.value.notifications.len);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        try execute(ctx, self.listener, (try corpus.get("sql-0128")).sql);
        try execute(ctx, self.sender, "NOTIFY usage_events, 'unsubscribed'");
        try empty(ctx, self.listener);
        try execute(ctx, self.listener, "LISTEN usage_events");
        try execute(ctx, self.listener, "DISCARD ALL");
        try execute(ctx, self.sender, "NOTIFY usage_events, 'discarded'");
        try empty(ctx, self.listener);
        try execute(ctx, self.listener, "BEGIN READ ONLY");
        try execute(ctx, self.listener, "LISTEN usage_events");
        try execute(ctx, self.listener, "NOTIFY usage_events, 'self-after-discard'");
        try execute(ctx, self.listener, "COMMIT");
        var self_event = try page(ctx, self.listener, null, 128);
        defer self_event.deinit();
        try std.testing.expectEqual(@as(usize, 1), self_event.value.notifications.len);
        try std.testing.expectEqualStrings("self-after-discard", self_event.value.notifications[0].payload);
        try execute(ctx, self.listener, "BEGIN");
        var acknowledged = try page(ctx, self.listener, self_event.value.cursor, 128);
        defer acknowledged.deinit();
        try std.testing.expectEqual(@as(usize, 0), acknowledged.value.notifications.len);
        try execute(ctx, self.listener, "ROLLBACK");
        try empty(ctx, self.listener);

        // A pending registration cannot escape the notification commit boundary
        // by transferring into named prepared custody (PostgreSQL 0A000).
        try execute(ctx, self.sender, "BEGIN");
        try execute(ctx, self.sender, "LISTEN cannot_prepare");
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        var rejected = try client.executeConnected(&authenticated, null, &self.sender, "PREPARE TRANSACTION 'notification_pending'", .never);
        defer rejected.deinit(ctx.alloc);
        try std.testing.expect(rejected.status != 200);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, rejected.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("0A000", diagnostic.value.code);
        try execute(ctx, self.sender, "ROLLBACK");
        try execute(ctx, self.listener, "NOTIFY cannot_prepare, 'aborted-registration'");
        try empty(ctx, self.sender);
    }
};
test "mounted SQL original notifications preserve transactional registration ordered delivery savepoints replay and cold recovery" {
    // sql-0126, sql-0127 and sql-0128 execute unchanged through native owners.
    try fixture.run(Campaign);
}
