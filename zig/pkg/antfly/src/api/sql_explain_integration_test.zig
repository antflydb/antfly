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

//! Mounted execution evidence for EXPLAIN through native owners and RLS.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;
const Options = struct { analyze: bool, timing: bool, summary: bool, buffers: bool, settings: bool, wal: bool, costs: bool };
const Entry = struct { sql: []const u8, parameters: []const Json, sqlstate: ?[]const u8, rows: ?usize, options: ?Options };
const Original = struct { id: []const u8, sql: []const u8, rows: usize, options: ?Options };
const Truncation = struct { sql: []const u8, value: []const u8 };
const Override = struct { id: []const u8, sql: []const u8, rows: []const []const Json };
const Reference = struct { override: Override, truncations: []const Truncation, entries: []const Entry, originals: []const Original, filtered: []const Entry, create: []const u8, seed: []const u8, read: []const u8, rows: []const []const Json };
fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var response = try client.execute(ctx, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("EXPLAIN setup status={d} body={s}\n", .{ response.status, response.body });
        return error.ExplainSetupFailed;
    }
}
fn matchPlan(a: std.mem.Allocator, actual: wire.SQLResponse, entry: Entry) !bool {
    if (actual.rows.len != 1 or actual.columns.len != 1 or !std.mem.eql(u8, actual.command_tag, "EXPLAIN")) return false;
    var parsed = try std.json.parseFromSlice(Json, a, actual.rows[0][0].string, .{});
    defer parsed.deinit();
    const plan = parsed.value.object;
    const options = plan.get("options").?.object;
    inline for (@typeInfo(Options).@"struct".field_names) |field| if (options.get(field).?.bool != @field(entry.options.?, field)) return false;
    if (entry.rows) |rows| {
        const profile = plan.get("runtime").?.object;
        if (profile.get("result_rows").?.integer != rows) return false;
        if (!entry.options.?.summary and profile.get("elapsed_ns").? != .null) return false;
        if (entry.options.?.buffers and profile.get("buffers").? == .null) return false;
        if (entry.options.?.wal) {
            const journal = profile.get("wal").?.object;
            // Never relabel unavailable provider I/O as a measured zero.
            if (journal.get("records").? != .null or journal.get("bytes").? != .null) return false;
        }
        const nodes = profile.get("nodes").?.array.items;
        if (nodes.len == 0 or nodes[0].object.get("rows").?.integer != rows) return false;
        if (!entry.options.?.timing) for (nodes) |node| if (node.object.get("elapsed_ns").? != .null) return false;
    } else if (plan.get("runtime").? != .null) return false;
    if (entry.options.?.settings and plan.get("settings").? == .null) return false;
    return true;
}
fn matrix(ctx: *fixture.Context, entries: []const Entry) !void {
    var failures: usize = 0;
    for (entries, 0..) |entry, index| {
        var response = try client.executeParameters(ctx, entry.sql, entry.parameters, .read);
        defer response.deinit(ctx.alloc);
        if (response.status == 503) return error.ExplainReadUnavailable;
        const matched = if (entry.sqlstate) |expected| rejection: {
            if (response.status != 400) break :rejection false;
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer diagnostic.deinit();
            break :rejection std.mem.eql(u8, expected, diagnostic.value.code);
        } else success: {
            if (response.status != 200) break :success false;
            var actual = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer actual.deinit();
            break :success try matchPlan(ctx.alloc, actual.value, entry);
        };
        if (!matched) {
            failures += 1;
            if (failures <= 16) std.debug.print("EXPLAIN native mismatch index={d} sql={s} status={d} body={s}\n", .{ index, entry.sql, response.status, response.body });
            try ctx.checkDrivers();
        }
    }
    std.debug.print("EXPLAIN mounted PostgreSQL matrix cases={d} failures={d}\n", .{ entries.len, failures });
    if (failures != 0) return error.ExplainNativeMismatch;
}
fn overrideOriginal(ctx: *fixture.Context, entry: Override) !void {
    var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
    defer corpus.deinit();
    try std.testing.expectEqualStrings("sql-1409", entry.id);
    try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
    var response = try client.execute(ctx, entry.sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) {
        std.debug.print("OVERRIDING original status={d} body={s}\n", .{ response.status, response.body });
        return error.OverrideOriginalFailed;
    }
    var actual = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer actual.deinit();
    const got = try std.json.Stringify.valueAlloc(ctx.alloc, actual.value.rows, .{});
    defer ctx.alloc.free(got);
    const want = try std.json.Stringify.valueAlloc(ctx.alloc, entry.rows, .{});
    defer ctx.alloc.free(want);
    try std.testing.expectEqualStrings(want, got);
}

fn temporalValues(ctx: *fixture.Context, entries: []const Truncation) !void {
    for (entries) |entry| {
        var response = try client.execute(ctx, entry.sql, .read);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) {
            std.debug.print("timestamp truncation status={d} sql={s} body={s}\n", .{ response.status, entry.sql, response.body });
            return error.ExplainTemporalMismatch;
        }
        var actual = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer actual.deinit();
        try std.testing.expectEqual(@as(usize, 1), actual.value.rows.len);
        try std.testing.expectEqualStrings(entry.value, actual.value.rows[0][0].string);
    }
}

fn originals(ctx: *fixture.Context, entries: []const Original) !void {
    var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
    defer corpus.deinit();
    for (entries) |entry| {
        try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
        if (entry.options) |options| {
            try matrix(ctx, &.{.{ .sql = entry.sql, .parameters = &.{}, .sqlstate = null, .rows = entry.rows, .options = options }});
            continue;
        }
        var response = try client.execute(ctx, entry.sql, .read);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var actual = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer actual.deinit();
        try std.testing.expectEqual(@as(usize, 1), actual.value.rows.len);
        const observer = try std.fmt.allocPrint(ctx.alloc, "Execution: {d} result rows", .{entry.rows});
        defer ctx.alloc.free(observer);
        try std.testing.expect(std.mem.indexOf(u8, actual.value.rows[0][0].string, observer) != null);
    }
}
fn unchanged(ctx: *fixture.Context, reference: Reference) !void {
    var response = try client.execute(ctx, reference.read, .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var actual = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer actual.deinit();
    const got = try std.json.Stringify.valueAlloc(ctx.alloc, actual.value.rows, .{});
    defer ctx.alloc.free(got);
    const want = try std.json.Stringify.valueAlloc(ctx.alloc, reference.rows, .{});
    defer ctx.alloc.free(want);
    try std.testing.expectEqualStrings(want, got);
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
        const table = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, table.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try command(ctx, self.reference.seed);
        try overrideOriginal(ctx, self.reference.override);
        try unchanged(ctx, self.reference);
        try matrix(ctx, self.reference.entries);
        try temporalValues(ctx, self.reference.truncations);
        try originals(ctx, self.reference.originals);
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        try manager.setRowFilter("pk-reader", "usage_records", "{\"term\":{\"status\":\"open\"}}");
        defer manager.removeRowFilter("pk-reader", "usage_records") catch {};
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        try matrix(&reader, self.reference.filtered);
        try manager.removeRowFilter("pk-reader", "usage_records");
        try unchanged(ctx, self.reference);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try unchanged(ctx, self.reference);
        try matrix(ctx, self.reference.entries);
        try temporalValues(ctx, self.reference.truncations);
        try originals(ctx, self.reference.originals);
        try unchanged(ctx, self.reference);
    }
};
test "mounted SQL EXPLAIN original reads and options preserve full execution RLS and cold owners" {
    // sql-0070 and sql-0071 execute unchanged. sql-1409 qualifies the
    // PostgreSQL OVERRIDING write and complete durable postimage during setup. Native physical plans and
    // provider accounting retain explicit engine units and unknown coverage.
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.explain_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expect(reference.value.entries.len >= 500);
    try std.testing.expectEqual(@as(usize, 2), reference.value.originals.len);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
