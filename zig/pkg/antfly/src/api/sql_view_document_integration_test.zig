// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Exact original document queries over a public SQL view and real raw JSON
//! rows. Both immutable scalar paths and explicit typed arrays survive restart.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;
const golden_bytes = local.sql_parity_fixtures.view_read_reference;
const Golden = struct {
    format: u8,
    reference: []const u8,
    profile: struct {
        description: []const u8,
        schema: Json,
        rows: []const struct { key: []const u8, value: Json },
        view_sql: []const u8,
    },
    entries: []const struct {
        id: []const u8,
        sql: []const u8,
        columns: []const []const u8,
        column_oids: []const u32,
        rows: []const []const Json,
        sql_nulls: []const []const bool,
        affected: u64,
    },
};

fn query(ctx: *fixture.Context, statement: []const u8, admin: bool) !@import("../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = if (admin) ctx.admin_headers else ctx.reader_headers;
    authenticated.base = ctx.metadata_uri;
    return client.execute(&authenticated, statement, .read);
}

fn qualify(ctx: *fixture.Context, golden: Golden, phase: []const u8) !void {
    const started = @import("antfly_platform").time.monotonicNs();
    var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
    defer corpus.deinit();
    for (golden.entries) |expected| {
        errdefer std.debug.print("document view original {s} phase={s}\n", .{ expected.id, phase });
        const original = try corpus.get(expected.id);
        try std.testing.expectEqualStrings(original.sql, expected.sql);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        var response = try query(ctx, original.sql, false);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("document view id={s} status={d} body={s}\n", .{ expected.id, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const actual = parsed.value;
        try std.testing.expectEqualStrings("SELECT", actual.command_tag);
        try std.testing.expectEqual(@as(i64, 0), actual.rows_affected);
        try std.testing.expectEqual(expected.rows.len, actual.rows.len);
        try std.testing.expectEqual(expected.columns.len, actual.columns.len);
        for (actual.columns, expected.columns, expected.column_oids) |column, name, oid| {
            try std.testing.expectEqualStrings(name, column.name);
            try std.testing.expect(compare.postgresColumnMatches(column, oid));
        }
        var compiled = try local.sql_compiler.compile(ctx.alloc, original.sql, .{});
        defer compiled.deinit();
        const ordered = compiled.statement.select.order_by.len != 0;
        const used = try ctx.alloc.alloc(bool, expected.rows.len);
        defer ctx.alloc.free(used);
        @memset(used, false);
        const nulls = actual.sql_nulls orelse return error.MissingSqlNullBitmap;
        try std.testing.expectEqual(actual.rows.len, nulls.len);
        for (actual.rows, nulls, 0..) |row, flags, index| {
            const matched = for (expected.rows, expected.sql_nulls, 0..) |want, want_flags, position| {
                if (used[position] or (ordered and index != position)) continue;
                if (!try compare.rowMatchesWithNulls(ctx.alloc, actual.columns, row, flags, want, want_flags)) continue;
                used[position] = true;
                break true;
            } else false;
            if (!matched) std.debug.print("document view mismatch id={s} body={s}\n", .{ expected.id, response.body });
            try std.testing.expect(matched);
        }
    }
    const elapsed = @import("antfly_platform").time.monotonicNs() -| started;
    std.debug.print("document view cohort phase={s} cases={d} elapsed_ms={d}\n", .{ phase, golden.entries.len, elapsed / std.time.ns_per_ms });
}

const Campaign = struct {
    credentials: client.ApiKeyHeaders = .{},

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        var parsed = try std.json.parseFromSlice(Golden, ctx.alloc, golden_bytes, .{});
        defer parsed.deinit();
        const golden = parsed.value;
        try std.testing.expectEqual(@as(u8, 2), golden.format);
        try std.testing.expectEqualStrings("PostgreSQL exact SQL", golden.reference);
        try std.testing.expectEqual(@as(usize, 38), golden.entries.len);
        // Storage epochs are server-owned at public table creation.
        var declaration = golden.profile.schema;
        _ = declaration.object.orderedRemove("version");
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema = declaration }, .{});
        defer ctx.alloc.free(body);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/docs", .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 201 and created.status != 200) std.debug.print("document source creation status={d} body={s}\n", .{ created.status, created.body });
        try std.testing.expect(created.status == 201 or created.status == 200);
        var observed = try fixture.table(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs");
        defer observed.deinit(ctx.alloc);
        var table = try std.json.parseFromSlice(Json, ctx.alloc, observed.body, .{});
        defer table.deinit();
        const id = try std.fmt.parseUnsigned(u64, (try fixture.field(table.value, "table_id")).string, 10);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        var inserts: std.json.ObjectMap = .empty;
        defer inserts.deinit(ctx.alloc);
        for (golden.profile.rows) |seed| try inserts.put(ctx.alloc, seed.key, seed.value);
        const batch = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .inserts = Json{ .object = inserts }, .sync_level = "full_text" }, .{});
        defer ctx.alloc.free(batch);
        var inserted = try fixture.batch(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs", batch);
        defer inserted.deinit(ctx.alloc);
        if (inserted.status != 201) std.debug.print("document source seed status={d} body={s}\n", .{ inserted.status, inserted.body });
        try std.testing.expectEqual(@as(u16, 201), inserted.status);
        var view = try query(ctx, golden.profile.view_sql, true);
        defer view.deinit(ctx.alloc);
        if (view.status != 200) std.debug.print("document view creation status={d} body={s}\n", .{ view.status, view.body });
        try std.testing.expectEqual(@as(u16, 200), view.status);
        try qualify(ctx, golden, "initial-owner");
    }

    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        var parsed = try std.json.parseFromSlice(Golden, ctx.alloc, golden_bytes, .{});
        defer parsed.deinit();
        try qualify(ctx, parsed.value, "reopened-owner");
    }
};

test "mounted SQL original document views preserve real paths typed arrays and lateral reads across owner restart" {
    const original_ids = [_][]const u8{
        "sql-0797", "sql-0798", "sql-0799", "sql-0800", "sql-0802", "sql-0803",
        "sql-0804", "sql-0805", "sql-0806", "sql-0807", "sql-0808", "sql-0809",
        "sql-0810", "sql-0811", "sql-0812", "sql-0813", "sql-0815", "sql-0816",
        "sql-0817", "sql-0818", "sql-0819", "sql-0820", "sql-0821", "sql-0822",
        "sql-0823", "sql-0836", "sql-0837", "sql-0857", "sql-0866", "sql-0872",
        "sql-0873", "sql-0874", "sql-0875", "sql-0876", "sql-0877", "sql-0878",
        "sql-0879", "sql-0880",
    };
    var parsed = try std.json.parseFromSlice(Golden, std.testing.allocator, golden_bytes, .{});
    defer parsed.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    try std.testing.expectEqual(original_ids.len, parsed.value.entries.len);
    for (original_ids, parsed.value.entries) |id, entry| {
        try std.testing.expectEqualStrings(id, entry.id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, entry.sql);
    }
    try fixture.run(Campaign);
}
