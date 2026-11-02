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

//! Prepared GIDs through the mounted SQL API, data Raft, native intents and a
//! complete metadata/session/data-owner reopen. No SQL mutation is replayed.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const transport = @import("../raft/transport/http_common.zig");
const http = @import("http_server.zig");
const registry = @import("../system_catalog/prepared_transactions.zig");
const wire = @import("antfly_metadata_openapi").types;

fn execute(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8) !transport.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    return client.executeConnected(&authenticated, null, connection, statement, .never);
}
fn command(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8, expected: []const u8) !void {
    var response = try execute(ctx, connection, statement);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("prepared SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    var parsed = try std.json.parseFromSlice(struct { command_tag: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(expected, parsed.value.command_tag);
}
fn rows(ctx: *fixture.Context, statement: []const u8, expected: std.json.Value) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    // A pure observer may repeat only a proven pre-admission read failure.
    // Never retry PREPARE or either decision command after an uncertain reply.
    var response = try client.executeConnected(&authenticated, null, null, statement, .read);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("prepared observation SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const compare = @import("sql_parity_reference.zig");
    try std.testing.expectEqual(@as(usize, 1), parsed.value.columns.len);
    try std.testing.expectEqualStrings("amount", parsed.value.columns[0].name);
    try std.testing.expect(compare.postgresColumnMatches(parsed.value.columns[0], 20));
    try std.testing.expect(expected == .array);
    try std.testing.expectEqual(expected.array.items.len, parsed.value.rows.len);
    const nulls = parsed.value.sql_nulls orelse return error.MissingSqlNullBitmap;
    for (parsed.value.rows, nulls, expected.array.items) |actual, flags, want| {
        try std.testing.expect(want == .array);
        try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, parsed.value.columns, actual, flags, want.array.items, &.{false}));
    }
}
fn diagnostic(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8, code: []const u8, reader: bool) !void {
    var authenticated = ctx.*;
    authenticated.headers = if (reader) ctx.reader_headers else ctx.admin_headers;
    var response = try client.executeConnected(&authenticated, null, connection, statement, .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("prepared diagnostic SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status >= 400);
    var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
}
fn openConnection(ctx: *fixture.Context) ![32]u8 {
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.connection_id.len != 32) return error.InvalidSqlConnection;
    return parsed.value.connection_id[0..32].*;
}
fn prepared(ctx: *fixture.Context, gid: []const u8, expected: bool) !void {
    var region = std.heap.ArenaAllocator.init(ctx.alloc);
    defer region.deinit();
    const a = region.allocator();
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_prepared_read = .{ .gid = gid } });
    const cut = try std.json.parseFromSliceLeaky(registry.Snapshot, a, bytes, .{});
    if (!expected) {
        try std.testing.expectEqual(@as(usize, 0), cut.records.len);
        return;
    }
    try std.testing.expectEqual(@as(usize, 1), cut.records.len);
    const record = cut.records[0];
    try std.testing.expect(record.phase == .prepared and record.was_prepared);
    try std.testing.expect(record.participants.len >= 2);
    try record.validate(a);
    const work = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_prepared_read = .{ .work = null } });
    const page = try std.json.parseFromSliceLeaky(registry.Snapshot, a, work, .{});
    for (page.records) |job| try std.testing.expect(!std.mem.eql(u8, &job.identity.operation_id, &record.identity.operation_id));
}

const Reference = struct {
    format: u32,
    postgres_major: u32,
    cases: []const struct { id: []const u8, sql: []const u8, command_tag: []const u8 },
    preimage: std.json.Value,
    committed: std.json.Value,
    rolled_back: std.json.Value,
};

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    reference: Reference,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }

    fn original(self: *@This(), ctx: *fixture.Context, connection_id: ?[]const u8, id: []const u8) !void {
        for (self.reference.cases) |entry| if (std.mem.eql(u8, entry.id, id)) return command(ctx, connection_id, entry.sql, entry.command_tag);
        return error.TestUnexpectedResult;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        inline for (.{ "prepared_a", "prepared_b" }) |name| {
            const create = "CREATE TABLE " ++ name ++ " (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)";
            var created = try client.execute(&authenticated, create, .write_admission);
            defer created.deinit(ctx.alloc);
            errdefer std.debug.print("prepared setup SQL={s} status={d} body={s}\n", .{ create, created.status, created.body });
            try std.testing.expect(created.status == 200 or created.status == 202);
            const serving = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
            try command(ctx, null, "INSERT INTO " ++ name ++ " VALUES(1,10)", "INSERT");
        }
        const first = try openConnection(ctx);
        try command(ctx, &first, "BEGIN", "BEGIN");
        try command(ctx, &first, "UPDATE prepared_a SET amount=11 WHERE id=1", "UPDATE");
        try command(ctx, &first, "UPDATE prepared_b SET amount=11 WHERE id=1", "UPDATE");
        // Execute the unchanged original inventory statement.
        try self.original(ctx, &first, "sql-0771");
        try prepared(ctx, "usage_batch", true);
        // GID ownership is independent of the originating connection. Neither
        // another principal nor an explicit transaction may decide it.
        try diagnostic(ctx, null, "ROLLBACK PREPARED 'usage_batch'", "42501", true);
        const contender = try openConnection(ctx);
        try command(ctx, &contender, "BEGIN", "BEGIN");
        try diagnostic(ctx, &contender, "COMMIT PREPARED 'usage_batch'", "25001", false);
        try command(ctx, &contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &contender, "BEGIN", "BEGIN");
        try diagnostic(ctx, &contender, "PREPARE TRANSACTION 'usage_batch'", "42710", false);
        try prepared(ctx, "usage_batch", true);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.preimage);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.preimage);
        // The detached original connection can start another transaction;
        // the durable GID independently owns the first transaction's intents.
        try command(ctx, &first, "BEGIN", "BEGIN");
        try command(ctx, &first, "ROLLBACK", "ROLLBACK");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try prepared(ctx, "usage_batch", true);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.preimage);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.preimage);
        try self.original(ctx, null, "sql-0772");
        try prepared(ctx, "usage_batch", false);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.committed);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.committed);
        const second = try openConnection(ctx);
        try command(ctx, &second, "BEGIN", "BEGIN");
        try command(ctx, &second, "UPDATE prepared_a SET amount=12 WHERE id=1", "UPDATE");
        try command(ctx, &second, "UPDATE prepared_b SET amount=12 WHERE id=1", "UPDATE");
        try self.original(ctx, &second, "sql-0771");
        try self.original(ctx, null, "sql-0773");
        try prepared(ctx, "usage_batch", false);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.rolled_back);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.rolled_back);
        try diagnostic(ctx, null, "COMMIT PREPARED 'usage_batch'", "42704", false);
        // Empty string is a valid PostgreSQL GID. An empty transaction still
        // has a real durable metadata decision and releases its reservation.
        const empty = try openConnection(ctx);
        try command(ctx, &empty, "BEGIN", "BEGIN");
        try command(ctx, &empty, "PREPARE TRANSACTION ''", "PREPARE TRANSACTION");
        try command(ctx, null, "COMMIT PREPARED ''", "COMMIT PREPARED");
        try prepared(ctx, "", false);
        try command(ctx, null, "PREPARE TRANSACTION 'outside'", "ROLLBACK");
    }
};

test "mounted SQL original prepared transactions retain multi-owner intents across complete cold restart" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.prepared_transaction_reference, .{});
    defer reference.deinit();
    try std.testing.expectEqual(@as(u32, 1), reference.value.format);
    try std.testing.expect(reference.value.postgres_major >= 18);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqual(@as(usize, 3), reference.value.cases.len);
    for ([_][]const u8{ "sql-0771", "sql-0772", "sql-0773" }, reference.value.cases) |id, entry| {
        try std.testing.expectEqualStrings(id, entry.id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, entry.sql);
    }
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
