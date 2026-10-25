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

//! Exact source-owned aliases through mounted native document owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Entry = struct { id: []const u8, sql: []const u8, schema: Json, seeds: []const struct { key: []const u8, value: Json }, affected: u64, rows: Output, aliases: Output, null_rows: Output, cold_rows: Output };
const bytes = local.sql_parity_fixtures.document_alias_reference;

fn mutate(ctx: *fixture.Context, statement: []const u8, expected: i64) !void {
    var response = try client.execute(ctx, statement, .never);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("document alias mutation sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqual(expected, result.value.rows_affected);
}
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
const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    entry: Entry,
    credentials: client.ApiKeyHeaders = .{},
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        var schema = self.entry.schema;
        _ = schema.object.orderedRemove("version");
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema = schema }, .{});
        defer ctx.alloc.free(body);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/docs", .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 200 and created.status != 201) std.debug.print("document alias creation body={s}\n", .{created.body});
        try std.testing.expect(created.status == 200 or created.status == 201);
        const serving = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        if (self.entry.seeds.len != 0) {
            var inserts: std.json.ObjectMap = .empty;
            defer inserts.deinit(ctx.alloc);
            for (self.entry.seeds) |seed| try inserts.put(ctx.alloc, seed.key, seed.value);
            const batch = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .inserts = Json{ .object = inserts }, .sync_level = "full_text" }, .{});
            defer ctx.alloc.free(batch);
            var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs", batch);
            defer seeded.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 201), seeded.status);
        }
        var denied = ctx.*;
        denied.headers = ctx.reader_headers;
        var response = try client.execute(&denied, self.entry.sql, .never);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), response.status);
        try mutate(ctx, self.entry.sql, @intCast(self.entry.affected));
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.rows);
        try observe(ctx, "SELECT _id,metadata_plan FROM docs ORDER BY _id", self.entry.aliases);
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        try manager.setRowFilter("pk-reader", "docs", "{\"term\":{\"metadata.plan\":\"pro\"}}");
        defer manager.removeRowFilter("pk-reader", "docs") catch {};
        var visible_rows: std.ArrayList([]const Json) = .empty;
        defer visible_rows.deinit(ctx.alloc);
        var visible_nulls: std.ArrayList([]const bool) = .empty;
        defer visible_nulls.deinit(ctx.alloc);
        for (self.entry.aliases.rows, self.entry.aliases.sql_nulls) |row, flags| {
            if (row[1] == .string and std.mem.eql(u8, row[1].string, "pro")) {
                try visible_rows.append(ctx.alloc, row);
                try visible_nulls.append(ctx.alloc, flags);
            }
        }
        var visible = self.entry.aliases;
        visible.rows = visible_rows.items;
        visible.sql_nulls = visible_nulls.items;
        try observe(&denied, "SELECT _id,metadata_plan FROM docs WHERE metadata_plan='pro' ORDER BY _id", visible);
        try manager.removeRowFilter("pk-reader", "docs");
        try mutate(ctx, "UPDATE docs SET metadata_plan=NULL WHERE _id='doc:a'", 1);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.null_rows);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.null_rows);
        try mutate(ctx, "INSERT INTO docs(_id,metadata_plan) VALUES('doc:cold','cold')", 1);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.entry.cold_rows);
    }
};
test "mounted SQL original document aliases preserve physical JSON authorization and cold mutations" {
    // sql-0917, sql-1082, sql-1088: exact source schemas, original SQL and full
    // postimages; old index-readiness annotations are never authority.
    var reference = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, entries: []const Entry }, std.testing.allocator, bytes, .{});
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 3), reference.value.entries.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    for (reference.value.entries) |entry| {
        try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
        errdefer std.debug.print("document alias original id={s}\n", .{entry.id});
        try fixture.runWithState(Campaign, .{ .entry = entry });
    }
}
