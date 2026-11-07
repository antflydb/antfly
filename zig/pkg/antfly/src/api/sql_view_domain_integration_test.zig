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

//! Exact view domains and engine-owned document identity through native owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Reference = struct {
    view: struct { select: []const u8, read: []const u8, before: Output, incompatible: []const u8, replacement: []const u8, after: Output, cold: Output },
    document: struct { id: []const u8, sql: []const u8, schema: Json, seeds: []const struct { key: []const u8, value: Json }, before: Output, mutation: []const u8, after: Output },
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
        std.debug.print("view domain command status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
        return error.ViewDomainCommandFailed;
    }
}
fn rejected(ctx: *fixture.Context, statement: []const u8, code: []const u8) !void {
    var response = try request(ctx, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status != 400) std.debug.print("view domain rejection status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 400), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
}
fn observe(ctx: *fixture.Context, statement: []const u8, expected: Output) !void {
    var response = try request(ctx, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("view domain query sql={s} body={s}\n", .{ statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqual(expected.columns.len, result.value.columns.len);
    for (expected.columns, expected.column_oids, result.value.columns) |name, oid, column| {
        try std.testing.expectEqualStrings(name, column.name);
        if (!compare.postgresColumnMatches(column, oid)) std.debug.print("view domain column={s} expected_oid={d} actual={f}\n", .{ name, oid, std.json.fmt(column, .{}) });
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, result.value.rows.len);
    const nulls = result.value.sql_nulls orelse return error.MissingSqlNullBitmap;
    for (expected.rows, expected.sql_nulls, result.value.rows, nulls) |want, flags, row, actual_flags| {
        if (!try compare.rowMatchesWithNulls(ctx.alloc, result.value.columns, row, actual_flags, want, flags)) {
            std.debug.print("view domain postimage sql={s} expected={f} body={s}\n", .{ statement, std.json.fmt(want, .{}), response.body });
            return error.ViewDomainPostimageMismatch;
        }
    }
}
const ViewCampaign = struct {
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
        try command(ctx, "CREATE TABLE usage_records(id TEXT PRIMARY KEY)");
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try command(ctx, "INSERT INTO usage_records(id) VALUES('a'),('b')");
        const create = try std.fmt.allocPrint(ctx.alloc, "CREATE VIEW inner_view AS {s}", .{self.reference.view.select});
        defer ctx.alloc.free(create);
        try command(ctx, create);
        try command(ctx, "CREATE VIEW outer_view AS SELECT * FROM inner_view");
        try observe(ctx, self.reference.view.read, self.reference.view.before);
        try rejected(ctx, self.reference.view.incompatible, "42P16");
        try observe(ctx, self.reference.view.read, self.reference.view.before);
        try command(ctx, self.reference.view.replacement);
        try command(ctx, "ALTER VIEW inner_view RENAME TO renamed_view");
        try command(ctx, "ALTER TABLE usage_records ADD COLUMN note TEXT");
        try observe(ctx, self.reference.view.read, self.reference.view.after);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try observe(ctx, self.reference.view.read, self.reference.view.after);
        try command(ctx, "UPDATE usage_records SET id='cold' WHERE id='a'");
        try observe(ctx, self.reference.view.read, self.reference.view.cold);
        try rejected(ctx, "DROP VIEW renamed_view RESTRICT", "2BP01");
        try command(ctx, "DROP VIEW renamed_view CASCADE");
        var response = try request(ctx, self.reference.view.read, false);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), response.status);
        var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("42P01", diagnostic.value.code);
    }
};
const IdentityCampaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    reference: Reference,
    retained: *std.heap.ArenaAllocator,
    baseline: []const u8 = "",
    credentials: client.ApiKeyHeaders = .{},
    fn snapshot(self: *@This(), ctx: *fixture.Context, first: bool) !void {
        var response = try request(ctx, "SELECT _id,_doc,_version FROM docs ORDER BY _id", false);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        // Retain the exact public result, including the full document envelope
        // and native versions, so a failed write cannot quietly create versions.
        if (first) self.baseline = try self.retained.allocator().dupe(u8, response.body) else try std.testing.expectEqualStrings(self.baseline, response.body);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        var schema = self.reference.document.schema;
        _ = schema.object.orderedRemove("version");
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema = schema }, .{});
        defer ctx.alloc.free(body);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/docs", .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 200 and created.status != 201) std.debug.print("identity schema status={d} body={s}\n", .{ created.status, created.body });
        try std.testing.expect(created.status == 200 or created.status == 201);
        const serving = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        var inserts: std.json.ObjectMap = .empty;
        defer inserts.deinit(ctx.alloc);
        for (self.reference.document.seeds) |seed| try inserts.put(ctx.alloc, seed.key, seed.value);
        const batch = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .inserts = Json{ .object = inserts }, .sync_level = "full_text" }, .{});
        defer ctx.alloc.free(batch);
        var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs", batch);
        defer seeded.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), seeded.status);
        try self.snapshot(ctx, true);
        var denied = try request(ctx, self.reference.document.sql, true);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
        try rejected(ctx, self.reference.document.sql, "428C9");
        try self.snapshot(ctx, false);
        for ([_][]const u8{
            "UPDATE docs SET _id='new' WHERE _id='doc:a'",
            "UPDATE docs SET _version=1 WHERE _id='doc:a'",
            "INSERT INTO docs(_id,title) VALUES('doc:a','changed') ON CONFLICT(_id) DO UPDATE SET _id=excluded.title",
            "MERGE INTO docs AS target USING docs AS source ON target._id=source._id WHEN MATCHED THEN UPDATE SET _id=source.title",
        }) |sql| {
            try rejected(ctx, sql, "428C9");
            try self.snapshot(ctx, false);
        }
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.reference.document.before);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try self.snapshot(ctx, false);
        try rejected(ctx, self.reference.document.sql, "428C9");
        try self.snapshot(ctx, false);
        try command(ctx, self.reference.document.mutation);
        try observe(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", self.reference.document.after);
    }
};
test "mounted SQL temporal view domains survive nested replacement and cold owners" {
    var reference = try std.json.parseFromSlice(Reference, std.testing.allocator, local.sql_parity_fixtures.view_domain_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try fixture.runWithState(ViewCampaign, .{ .reference = reference.value });
}
test "mounted SQL original document identity assignment rejects before commit and preserves cold versions" {
    // sql-1073: exact original source statement and schema, no unsupported waiver.
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.view_domain_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqualStrings((try corpus.get("sql-1073")).sql, reference.value.document.sql);
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    try fixture.runWithState(IdentityCampaign, .{ .reference = reference.value, .retained = &retained });
}
