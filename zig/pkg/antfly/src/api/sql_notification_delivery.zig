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

//! Connection-bound notification control. Polling and acknowledgements never
//! enter the SQL executor or borrow a command occurrence for replay.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const producer = @import("sql_command_producer.zig");
const authority = @import("../system_catalog/lock_authority.zig");
const journal = @import("../system_catalog/sql_notifications.zig");
const types = local.sql_ast.notifications;
const hub = @import("sql_notification_hub.zig");
const A = std.mem.Allocator;

pub fn encodeCursor(cursor: types.Cursor) [20]u8 {
    var bytes: [10]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], cursor.sequence, .big);
    std.mem.writeInt(u16, bytes[8..10], cursor.ordinal, .big);
    return std.fmt.bytesToHex(bytes, .lower);
}
pub fn decodeCursor(text: []const u8) !types.Cursor {
    if (text.len != 20) return error.SqlInvalidParameterValue;
    var bytes: [10]u8 = undefined;
    _ = std.fmt.hexToBytes(&bytes, text) catch return error.SqlInvalidParameterValue;
    const result: types.Cursor = .{ .sequence = std.mem.readInt(u64, bytes[0..8], .big), .ordinal = std.mem.readInt(u16, bytes[8..10], .big) };
    if (result.sequence == 0) return error.SqlInvalidParameterValue;
    return result;
}
pub const Owner = struct {
    server: *http.ApiHttpServer,
    user: ?http.AuthenticatedIdentity,
    context: local.api_operation.RequestContext,
    connection: [32]u8,
    observation: ?*hub.Observation = null,

    pub fn poll(self: Owner, a: A, acknowledgement: ?types.Cursor, maximum: u16) !journal.Page {
        return self.perform(a, acknowledgement, maximum, true);
    }
    pub fn acknowledge(self: Owner, a: A, cursor: types.Cursor) !void {
        _ = try self.perform(a, cursor, 128, false);
    }
    fn perform(self: Owner, a: A, acknowledgement: ?types.Cursor, maximum: u16, read_next: bool) !journal.Page {
        if (maximum == 0 or maximum > 128) return error.SqlInvalidParameterValue;
        try self.context.ensureActive();
        const store = self.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
        const principal = http.transactionPrincipal(self.user) orelse "";
        const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        var connection = try @import("sql_connections.zig").load(store, a, self.connection, principal, self.server.localSessionNodeId(), now);
        defer connection.deinit();
        try http.requireSqlPrincipalInstance(self.server, self.user, connection.value.principal_instance);
        if (connection.value.state != .idle and (acknowledgement == null or connection.value.state != .active)) {
            if (acknowledgement != null) return error.SqlConnectionBusy;
            return .{ .deferred = true, .waiting = true };
        }
        if (!connection.value.notification_listen_seen) {
            if (acknowledgement != null) return error.SqlInvalidParameterValue;
            return .{};
        }
        var source = (try producer.settleCompletion(self.server, a, self.context, self.connection, principal)) orelse return .{};
        defer source.deinit();
        if (!std.mem.eql(u8, source.value.principal, principal)) return error.Forbidden;
        if ((source.value.phase != .ready and source.value.phase != .idle) or source.value.recovery or source.value.pending != null or source.value.ticket != null) {
            if (acknowledgement != null) return error.SqlConnectionBusy;
            return .{ .deferred = true, .waiting = true };
        }
        const actor = try @import("sql_authorization_recovery.zig").actor(self.server, self.user);
        var native: producer.NativePort = .{ .server = self.server, .context = self.context };
        const port = native.port();
        const hub_key: hub.Key = .{ .group = source.value.registration.group, .database = source.value.registration.lease.database, .incarnation = source.value.registration.lease.backend.incarnation };
        const io = self.server.embedding_provider_runtime.io;
        var refresh: ?hub.Token = null;
        errdefer if (refresh) |token| self.server.sql_notification_hub.cancel(io, token);
        if (acknowledgement == null) if (self.observation) |observed| {
            if (observed.generation != connection.value.generation or !std.mem.eql(u8, &observed.backend_id, &source.value.registration.lease.backend.id)) observed.* = .{ .generation = connection.value.generation, .backend_id = source.value.registration.lease.backend.id };
            switch (try self.server.sql_notification_hub.observe(io, hub_key, observed.*, @import("antfly_platform").time.monotonicNs())) {
                .skip => return .{ .subscribed = true },
                .poll => {},
                .refresh => |token| refresh = token,
            }
        };
        var cut = try snapshot(a, port, source.value, actor, connection.value.generation, if (acknowledgement == null) maximum else null);
        if (acknowledgement) |cursor| {
            try self.control(a, port, source.value, actor, cut.head, .{ .notification_ack = .{ .connection = self.connection, .generation = connection.value.generation, .cursor = cursor } });
            if (!read_next or connection.value.state != .idle) return .{};
            cut = try snapshot(a, port, source.value, actor, connection.value.generation, maximum);
        }
        const page = cut.notifications orelse return error.InvalidSqlBackendResponse;
        if (page.subscribed and types.Cursor.order(page.cursor, page.offered) == .gt) {
            try self.control(a, port, source.value, actor, cut.head, .{ .notification_offer = .{ .connection = self.connection, .generation = connection.value.generation, .cursor = page.cursor, .maximum = maximum } });
        }
        try self.context.ensureActive();
        if (self.observation) |observed| {
            self.server.sql_notification_hub.complete(io, hub_key, refresh, page.revision, @import("antfly_platform").time.monotonicNs());
            observed.revision = if (page.subscribed) page.revision else null;
            observed.backlog = page.pending and !page.waiting;
        }
        return page;
    }
    fn control(self: Owner, a: A, port: producer.Port, source: producer.Record, actor: authority.prepared.Actor, head: u64, action: @FieldType(authority.Request, "operation")) !void {
        var nonce: [16]u8 = undefined;
        try self.server.embedding_provider_runtime.io.randomSecure(&nonce);
        _ = try port.submit(a, .{ .admission = .{ .head = head, .nonce = nonce }, .incarnation = source.registration.lease.backend.incarnation, .standalone = source.registration.standalone, .database = source.registration.lease.database, .actor = actor, .backend = source.registration.lease.backend, .operation = action });
    }
    fn snapshot(a: A, port: producer.Port, source: producer.Record, actor: authority.prepared.Actor, generation: u64, maximum: ?u16) !authority.Snapshot {
        const bytes = try port.call(port.ptr, a, .{ .sql_lock_read = .{ .incarnation = source.registration.lease.backend.incarnation, .standalone = source.registration.standalone, .actor = actor, .database = source.registration.lease.database, .backend = source.registration.lease.backend, .notifications = if (maximum) |limit| .{ .generation = generation, .maximum = limit } else null } });
        defer a.free(bytes);
        if (bytes.len > 1024 * 1024) return error.InvalidSqlBackendResponse;
        const result = try std.json.parseFromSliceLeaky(authority.Snapshot, a, bytes, .{ .allocate = .alloc_always });
        if (result.group != source.registration.group or result.standalone != source.registration.standalone or !std.mem.eql(u8, &result.incarnation, &source.registration.lease.backend.incarnation) or !result.notification_capable) return error.CatalogGenerationChanged;
        return result;
    }
};

test "SQL notification cursors preserve full width and reject malformed tokens" {
    const cursor: types.Cursor = .{ .sequence = std.math.maxInt(u64), .ordinal = 127 };
    const text = encodeCursor(cursor);
    try std.testing.expectEqualDeep(cursor, try decodeCursor(&text));
    try std.testing.expectError(error.SqlInvalidParameterValue, decodeCursor("00000000000000000000"));
    try std.testing.expectError(error.SqlInvalidParameterValue, decodeCursor("x0000000000000000000"));
    try std.testing.expectError(error.SqlInvalidParameterValue, decodeCursor("01"));
}
