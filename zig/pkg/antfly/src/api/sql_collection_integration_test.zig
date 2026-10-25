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

//! PostgreSQL ordinary aggregate contracts through mounted native owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Entry = struct { sql: []const u8, parameters: []const Json, output: ?Output, sqlstate: ?[]const u8, original: bool };
const Probe = struct { sql: []const u8, output: Output };
const Reference = struct {
    create: []const u8,
    seed: []const u8,
    read: []const u8,
    before: Output,
    after: Output,
    entries: []const Entry,
    view_create: []const u8,
    view_read: []const u8,
    view_before: Output,
    view_after: Output,
    filtered: []const Probe,
    mutation: []const u8,
    cold: []const Probe,
};
fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var admin = ctx.*;
    admin.headers = ctx.admin_headers;
    var response = try client.execute(&admin, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("collection command status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
        return error.CollectionCommandFailed;
    }
}
fn checkOutput(ctx: *fixture.Context, actual: wire.SQLResponse, expected: Output) !void {
    if (actual.columns.len != expected.columns.len or actual.rows.len != expected.rows.len) return error.CollectionMismatch;
    for (expected.columns, expected.column_oids, actual.columns) |name, oid, column| {
        if (!std.mem.eql(u8, name, column.name) or !compare.postgresColumnMatches(column, oid)) return error.CollectionMismatch;
    }
    const nulls = actual.sql_nulls orelse return error.MissingSqlNullBitmap;
    if (nulls.len != actual.rows.len) return error.CollectionMismatch;
    for (expected.rows, expected.sql_nulls, actual.rows, nulls) |want, flags, row, actual_flags| {
        if (!try compare.rowMatchesWithNulls(ctx.alloc, actual.columns, row, actual_flags, want, flags)) return error.CollectionMismatch;
    }
}
fn observe(ctx: *fixture.Context, sql: []const u8, expected: Output) !void {
    var response = try client.execute(ctx, sql, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) {
        std.debug.print("collection query status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
        return error.CollectionObservationFailed;
    }
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    checkOutput(ctx, parsed.value, expected) catch |err| {
        std.debug.print("collection observation sql={s} body={s} expected={f}\n", .{ sql, response.body, std.json.fmt(expected, .{}) });
        return err;
    };
}
fn matrix(ctx: *fixture.Context, entries: []const Entry) !void {
    var failures: usize = 0;
    for (entries, 0..) |entry, index| {
        var response = try client.executeParameters(ctx, entry.sql, entry.parameters, .read);
        defer response.deinit(ctx.alloc);
        if (response.status == 503) return error.CollectionAdmissionUnavailable;
        const matched = if (entry.sqlstate) |code| rejection: {
            if (response.status != (if (std.mem.eql(u8, code, "0A000")) @as(u16, 501) else 400)) break :rejection false;
            var parsed = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            break :rejection std.mem.eql(u8, code, parsed.value.code);
        } else success: {
            if (response.status != 200) break :success false;
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            checkOutput(ctx, parsed.value, entry.output.?) catch |err| {
                if (err != error.CollectionMismatch) return err;
                break :success false;
            };
            break :success true;
        };
        if (!matched) {
            failures += 1;
            if (failures <= 24) std.debug.print("collection mismatch index={d} sql={s} status={d} body={s} expected={f}\n", .{ index, entry.sql, response.status, response.body, std.json.fmt(entry, .{}) });
            try ctx.checkDrivers();
        }
    }
    std.debug.print("collection PostgreSQL matrix cases={d} failures={d}\n", .{ entries.len, failures });
    if (failures != 0) return error.CollectionMismatch;
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
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
        ctx.headers = ctx.admin_headers;
        try command(ctx, self.reference.create);
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try command(ctx, self.reference.seed);
        try observe(ctx, self.reference.read, self.reference.before);
        try matrix(ctx, self.reference.entries);
        try command(ctx, self.reference.view_create);
        try observe(ctx, self.reference.view_read, self.reference.view_before);
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        try manager.setRowFilter("pk-reader", "usage_records", "{\"term\":{\"status\":\"a\"}}");
        defer manager.removeRowFilter("pk-reader", "usage_records") catch {};
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        for (self.reference.filtered) |probe| try observe(&reader, probe.sql, probe.output);
        try manager.removeRowFilter("pk-reader", "usage_records");
        try observe(ctx, self.reference.read, self.reference.before);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try observe(ctx, self.reference.read, self.reference.before);
        try matrix(ctx, self.reference.entries);
        try observe(ctx, self.reference.view_read, self.reference.view_before);
        try command(ctx, self.reference.mutation);
        for (self.reference.cold) |probe| try observe(ctx, probe.sql, probe.output);
        try observe(ctx, self.reference.view_read, self.reference.view_after);
        try observe(ctx, self.reference.read, self.reference.after);
    }
};
test "mounted SQL ordinary aggregates preserve PostgreSQL ordering distinct filters and cold rows" {
    // sql-1230: unchanged original SQL and parameters; PostgreSQL requires
    // DISTINCT ORDER BY keys to be aggregate arguments. Positive contracts
    // independently exercise the complete ordinary aggregation machinery.
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.collection_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expect(reference.value.entries.len >= 190);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    var originals: usize = 0;
    for (reference.value.entries) |entry| if (entry.original) {
        originals += 1;
        try std.testing.expectEqualStrings((try corpus.get("sql-1230")).sql, entry.sql);
    };
    try std.testing.expectEqual(@as(usize, 1), originals);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
