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

//! Original SQL-language routines through mounted native catalog and data owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Reference = struct {
    entries: []const struct { id: []const u8, sql: []const u8, command_tag: ?[]const u8 = null, sqlstate: ?[]const u8 = null, query: ?[]const u8 = null, output: ?Output = null },
    probes: []const struct { sql: []const u8, sqlstate: []const u8 },
    overload: struct { sql: []const u8, output: Output },
    replaced: Output,
};
fn request(ctx: *fixture.Context, statement: []const u8, reader: bool) !@import("../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = if (reader) ctx.reader_headers else ctx.admin_headers;
    return client.execute(&authenticated, statement, .write_admission);
}
fn command(ctx: *fixture.Context, statement: []const u8) !void {
    var response = try request(ctx, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("routine command status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
        return error.RoutineCommandFailed;
    }
}
fn rejected(ctx: *fixture.Context, statement: []const u8, code: []const u8) !void {
    var response = try request(ctx, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status != 400) std.debug.print("routine rejection status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 400), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
}
fn observe(ctx: *fixture.Context, statement: []const u8, expected: Output) !void {
    return observeAs(ctx, statement, expected, false);
}
fn observeAs(ctx: *fixture.Context, statement: []const u8, expected: Output, reader: bool) !void {
    var response = try request(ctx, statement, reader);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("routine query sql={s} body={s}\n", .{ statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqual(expected.columns.len, result.value.columns.len);
    for (expected.columns, expected.column_oids, result.value.columns) |name, oid, column| {
        try std.testing.expectEqualStrings(name, column.name);
        if (!compare.postgresColumnMatches(column, oid)) std.debug.print("routine column={s} expected_oid={d} actual={f}\n", .{ name, oid, std.json.fmt(column, .{}) });
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, result.value.rows.len);
    const nulls = result.value.sql_nulls orelse return error.MissingSqlNullBitmap;
    for (expected.rows, expected.sql_nulls, result.value.rows, nulls) |want, flags, row, actual_flags| {
        if (!try compare.rowMatchesWithNulls(ctx.alloc, result.value.columns, row, actual_flags, want, flags)) {
            std.debug.print("routine postimage sql={s} expected={f} body={s}\n", .{ statement, std.json.fmt(want, .{}), response.body });
            return error.RoutinePostimageMismatch;
        }
    }
}

// Lose the ACK only after the real Raft mutation returns, then make the
// producer's first read-only observation unavailable. Never retry the mutation.
fn lostAck(ctx: *fixture.Context) !void {
    const http = @import("http_server.zig");
    const Call = @import("../system_catalog/server_call.zig").Call;
    const Context = local.api_operation.RequestContext;
    const Probe = struct {
        source: http.StatusSource,
        mutations: usize = 0,
        hidden: bool = true,
        fn status(raw: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return (@as(*@This(), @ptrCast(@alignCast(raw)))).source.status();
        }
        fn catalog(raw: *anyopaque, a: std.mem.Allocator, context: Context, call: Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .sql_routine_outcome and self.hidden) return error.MetadataMutationOutcomeUnknown;
            const bytes = try self.source.systemCatalog(a, context, call);
            if (call == .sql_routine_mutate) {
                self.mutations += 1;
                a.free(bytes);
                return error.MetadataMutationOutcomeUnknown;
            }
            return bytes;
        }
    };
    const original = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var probe: Probe = .{ .source = original.source };
    var gateway = http.ApiHttpServer.init(ctx.alloc, .{}, .{ .ptr = &probe, .vtable = &.{ .supports_sql_routines = true, .status = Probe.status, .system_catalog = Probe.catalog } }, null, null);
    defer gateway.deinit();
    gateway.embedding_provider_runtime.io = ctx.io;
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: @import("sql_execution.zig").Adapter = .{ .server = &gateway, .identity = &identity, .context = .{} };
    defer adapter.releaseExecutionOwners();
    const padding = try ctx.alloc.alloc(u8, 12000);
    defer ctx.alloc.free(padding);
    @memset(padding, 'x');
    const text = try std.fmt.allocPrint(ctx.alloc, "CREATE FUNCTION lost_ack(text) RETURNS text LANGUAGE sql AS $$SELECT lower($1) /*{s}*/$$", .{padding});
    defer ctx.alloc.free(text);
    var parsed = try local.sql_compiler.compile(ctx.alloc, text, .{});
    defer parsed.deinit();
    const outcome = try @import("sql_routine_ddl.zig").execute(&adapter, .{ .result = ctx.alloc, .scratch = ctx.alloc }, parsed.statement.catalog_ddl);
    const receipt = outcome.catalog_receipt orelse return error.MissingRoutineReceipt;
    defer receipt.deinit(ctx.alloc);
    try std.testing.expectEqual(.unknown, receipt.state);
    try std.testing.expect(receipt.recovery_token.len < 2048);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    // Publication of unrelated functions cannot replace this exact receipt.
    try command(ctx, "CREATE FUNCTION unrelated_ack() RETURNS text LANGUAGE sql AS $$SELECT 'ok'$$");
    probe.hidden = false;
    const recovered = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, receipt.recovery_token);
    defer recovered.deinit(ctx.alloc);
    try std.testing.expectEqual(.committed, recovered.state);
    try std.testing.expect(recovered.object_id != null);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
}

const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    reference: Reference,
    credentials: client.ApiKeyHeaders = .{},
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
        try lostAck(ctx);
        for (self.reference.entries) |entry| {
            if (entry.sqlstate) |code| {
                try rejected(ctx, entry.sql, code);
            } else {
                var response = try request(ctx, entry.sql, false);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("routine declaration status={d} sql={s} body={s}\n", .{ response.status, entry.sql, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                try std.testing.expectEqualStrings(entry.command_tag.?, parsed.value.command_tag);
                try observe(ctx, entry.query.?, entry.output.?);
            }
        }
        for (self.reference.probes) |probe| try rejected(ctx, probe.sql, probe.sqlstate);
        // Function and relation names occupy different catalog namespaces.
        try command(ctx, "CREATE TABLE normalize_status(id INTEGER PRIMARY KEY,status TEXT)");
        const serving = try fixture.waitServingSchema(ctx, "normalize_status", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "normalize_status");
        try command(ctx, "INSERT INTO normalize_status(id,status) VALUES(1,'ReAdY'),(2,NULL)");
        try command(ctx, "UPDATE normalize_status SET status=normalize_status(status)");
        try observe(ctx, "SELECT normalize_status(status) AS value,normalize_status(NULL) AS absent FROM normalize_status WHERE id=1", self.reference.entries[0].output.?);
        const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const manager = api.cfg.user_manager orelse return error.MissingUserManager;
        try manager.setRowFilter("pk-reader", "normalize_status", "{\"term\":{\"status\":\"ready\"}}");
        defer manager.removeRowFilter("pk-reader", "normalize_status") catch {};
        try observeAs(ctx, "SELECT normalize_status(status) AS value, normalize_status(NULL) AS absent FROM normalize_status", self.reference.entries[0].output.?, true);
        var denied = try request(ctx, "DROP FUNCTION normalize_status(text)", true);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
        try command(ctx, "CREATE FUNCTION normalize_status(numeric) RETURNS numeric LANGUAGE sql AS $$SELECT $1+1$$");
        try observe(ctx, self.reference.overload.sql, self.reference.overload.output);
        try command(ctx, "CREATE OR REPLACE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT upper($1)$$");
        try observe(ctx, self.reference.entries[0].query.?, self.reference.replaced);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "normalize_status", 0);
        try observe(ctx, self.reference.entries[0].query.?, self.reference.replaced);
        for (self.reference.entries[1..]) |entry| if (entry.query) |query| try observe(ctx, query, entry.output.?);
        try command(ctx, "DROP FUNCTION normalize_status(numeric)");
        try rejected(ctx, "SELECT normalize_status(2.25)", "42883");
        try observe(ctx, "SELECT normalize_status(status) AS value,normalize_status(NULL) AS absent FROM normalize_status WHERE id=1", self.reference.replaced);
        try command(ctx, "DROP FUNCTION normalize_status(text)");
        try rejected(ctx, self.reference.entries[0].query.?, "42883");
        try command(ctx, "DROP FUNCTION IF EXISTS normalize_status(text)");
        try rejected(ctx, "SELECT shift_amount(1)", "42883");
    }
};

test "mounted SQL original routine declarations preserve typed calls overloads mutations receipts and cold owners" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.routine_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0397", "sql-0398", "sql-0399", "sql-0400", "sql-0401", "sql-0402", "sql-0403", "sql-0404" };
    try std.testing.expectEqual(originals.len, reference.value.entries.len);
    for (originals, reference.value.entries) |id, entry| {
        try std.testing.expectEqualStrings(id, entry.id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, entry.sql);
    }
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
