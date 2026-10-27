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

//! Native document producers over the shared immutable expression pipeline.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Entry = struct { id: []const u8, sql: []const u8, schema: Json, base: []const u8, generated: []const u8, seeds: []const struct { key: []const u8, value: Json }, affected: u64, sqlstate: ?[]const u8, before: Output, rows: Output, cold_rows: Output };
fn observe(ctx: *fixture.Context, statement: []const u8, expected: Output) !void {
    var response = try client.execute(ctx, statement, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("document alias query sql={s} body={s}\n", .{ statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqual(expected.columns.len, result.value.columns.len);
    for (expected.columns, expected.column_oids, result.value.columns) |name, oid, column| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, result.value.rows.len);
    const nulls = result.value.sql_nulls orelse return error.MissingSqlNullBitmap;
    for (expected.rows, expected.sql_nulls, result.value.rows, nulls) |want, flags, row, actual_flags| {
        if (!try compare.rowMatchesWithNulls(ctx.alloc, result.value.columns, row, actual_flags, want, flags)) {
            std.debug.print("document alias mismatch sql={s} body={s}\n", .{ statement, response.body });
            return error.DocumentAliasPostimageMismatch;
        }
    }
}

fn execute(ctx: *fixture.Context, sql: []const u8, expected: ?[]const u8, affected: u64) !void {
    var response = try client.execute(ctx, sql, .never);
    defer response.deinit(ctx.alloc);
    if (expected) |code| {
        if (response.status != 400) std.debug.print("document generated rejection sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 400), response.status);
        var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(code, diagnostic.value.code);
    } else {
        if (response.status != 200) std.debug.print("document generated mutation sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(affected)), result.value.rows_affected);
    }
}
const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    entry: Entry,
    credentials: client.ApiKeyHeaders = .{},
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema = self.entry.schema }, .{});
        defer ctx.alloc.free(body);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/docs", .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 200 and created.status != 201) std.debug.print("document generated creation body={s}\n", .{created.body});
        try std.testing.expect(created.status == 200 or created.status == 201);
        const serving = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        var inserts: std.json.ObjectMap = .empty;
        defer inserts.deinit(ctx.alloc);
        for (self.entry.seeds) |seed| try inserts.put(ctx.alloc, seed.key, seed.value);
        const batch = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .inserts = Json{ .object = inserts }, .sync_level = "full_text" }, .{});
        defer ctx.alloc.free(batch);
        var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs", batch);
        defer seeded.deinit(ctx.alloc);
        if (seeded.status != 201) std.debug.print("document generated seed body={s}\n", .{seeded.body});
        try std.testing.expectEqual(@as(u16, 201), seeded.status);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.before);
        const invalid_batch = try std.fmt.allocPrint(ctx.alloc, "{{\"inserts\":{{\"doc:new\":{{\"{s}\":\"VALID\"}},\"doc:a\":{{\"{s}\":42}}}},\"sync_level\":\"full_text\"}}", .{ self.entry.base, self.entry.base });
        defer ctx.alloc.free(invalid_batch);
        var rejected = try fixture.batch(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs", invalid_batch);
        defer rejected.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), rejected.status);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.before);
        var denied = ctx.*;
        denied.headers = ctx.reader_headers;
        const update = try std.fmt.allocPrint(ctx.alloc, "UPDATE docs SET {s}='COLD' WHERE _id='doc:a'", .{self.entry.base});
        defer ctx.alloc.free(update);
        var forbidden = try client.execute(&denied, update, .never);
        defer forbidden.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), forbidden.status);
        try execute(ctx, self.entry.sql, self.entry.sqlstate, self.entry.affected);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.rows);
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        const filter = try std.fmt.allocPrint(ctx.alloc, "{{\"term\":{{\"{s}\":\"launch\"}}}}", .{self.entry.generated});
        defer ctx.alloc.free(filter);
        try manager.setRowFilter("pk-reader", "docs", filter);
        defer manager.removeRowFilter("pk-reader", "docs") catch {};
        var rows: std.ArrayList([]const Json) = .empty;
        defer rows.deinit(ctx.alloc);
        var flags: std.ArrayList([]const bool) = .empty;
        defer flags.deinit(ctx.alloc);
        for (self.entry.rows.rows, self.entry.rows.sql_nulls) |row, nulls| {
            const value = row[1].object.get(self.entry.generated) orelse continue;
            if (value == .string and std.mem.eql(u8, value.string, "launch")) {
                try rows.append(ctx.alloc, row);
                try flags.append(ctx.alloc, nulls);
            }
        }
        var expected = self.entry.rows;
        expected.rows = rows.items;
        expected.sql_nulls = flags.items;
        try observe(&denied, "SELECT _id,_doc FROM docs ORDER BY _id", expected);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.rows);
        const update = try std.fmt.allocPrint(ctx.alloc, "UPDATE docs SET {s}='COLD' WHERE _id='doc:a' RETURNING {s}", .{ self.entry.base, self.entry.generated });
        defer ctx.alloc.free(update);
        var response = try client.execute(ctx, update, .never);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("document generated cold returning body={s}\n", .{response.body});
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
        try std.testing.expectEqualStrings("cold", result.value.rows[0][0].string);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.cold_rows);
    }
};
test "mounted SQL original document producers preserve generated domains row filters and cold postimages" {
    // sql-0910, sql-0920, sql-1072, sql-1074, sql-1079. Source expressions
    // migrate explicitly to public generated_columns; legacy metadata has no authority.
    var reference = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, entries: []const Entry }, std.testing.allocator, local.sql_parity_fixtures.document_generated_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 5), reference.value.entries.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    for (reference.value.entries) |entry| {
        const original = try corpus.get(entry.id);
        try std.testing.expectEqualStrings(original.sql, entry.sql);
        fixture.runWithState(Campaign, .{ .entry = entry }) catch |err| {
            std.debug.print("document generated original id={s}\n", .{entry.id});
            return err;
        };
    }
}
