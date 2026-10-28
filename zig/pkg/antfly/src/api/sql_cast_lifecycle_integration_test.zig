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

const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const support = @import("sql_callable_lifecycle_reference.zig");
const Prepared = struct { setup: []const []const u8, statement: []const u8, before: support.Output, create: []const u8, after_create: support.Output, replace: []const u8, after_replace: support.Output };
const Reference = struct { before: []const support.Step, cold: []const support.Step, after: []const support.Step, prepared: Prepared };
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
        var usage = try users.Permission.initOwned(manager.alloc, .namespace, "*", .read);
        defer usage.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-reader", usage);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try preparedDependencies(ctx, self.reference.prepared);
        for (self.reference.before) |step| {
            try support.observe(ctx, step, false);
            if (std.mem.startsWith(u8, step.sql, "CREATE TABLE cast_rows")) try ready(ctx, "cast_rows");
        }
        const unsupported_definitions = [_][]const u8{
            "CREATE VIEW deferred_cast_view AS SELECT value::text AS converted FROM cast_rows",
            "CREATE TABLE deferred_cast_default(id integer PRIMARY KEY,label text DEFAULT ('{}'::jsonb)::text)",
            "ALTER TABLE cast_rows ALTER COLUMN label SET DEFAULT ('{}'::jsonb)::text",
            "ALTER TABLE cast_rows ADD CONSTRAINT deferred_cast_check CHECK(value::text<>'')",
            "CREATE POLICY deferred_cast_policy ON cast_rows USING(value::text<>'')",
        };
        for (unsupported_definitions) |sql| try support.observe(ctx, .{ .sql = sql, .sqlstate = "0A000", .notices = &.{} }, false);
        for (self.reference.cold) |step| try support.observe(ctx, step, true);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "DROP CAST (jsonb AS text)", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.reference.cold) |step| try support.observe(ctx, step, false);
        for (self.reference.after) |step| try support.observe(ctx, step, false);
        try lostAck(ctx);
    }
};
fn ready(ctx: *fixture.Context, name: []const u8) !void {
    const serving = try fixture.waitServingSchema(ctx, name, 0);
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
    try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
}
fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    var response = try client.execute(&authenticated, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("cast setup status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
}
fn preparedDependencies(ctx: *fixture.Context, reference: Prepared) !void {
    for (reference.setup) |sql| {
        try command(ctx, sql);
        if (std.mem.startsWith(u8, sql, "CREATE TABLE ")) try ready(ctx, "prepared_cast_rows");
    }
    var absent = try support.PreparedResource.create(ctx, reference.statement);
    defer absent.deinit();
    try absent.expectResult(.{ .sql = reference.statement, .output = reference.before, .notices = &.{} });
    try command(ctx, reference.create);
    try absent.expectStale();
    var created = try support.PreparedResource.create(ctx, reference.statement);
    defer created.deinit();
    try created.expectResult(.{ .sql = reference.statement, .output = reference.after_create, .notices = &.{} });
    try command(ctx, reference.replace);
    try created.expectStale();
    var replaced = try support.PreparedResource.create(ctx, reference.statement);
    defer replaced.deinit();
    try replaced.expectResult(.{ .sql = reference.statement, .output = reference.after_replace, .notices = &.{} });
    try absent.close();
    try created.close();
    try replaced.close();
    try command(ctx, "DROP CAST (jsonb AS text)");
}
test "mounted SQL original database casts preserve conversion contexts prepared witnesses and cold dependency retirement" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.cast_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0139", "sql-0140", "sql-0141" };
    var checked: usize = 0;
    for ([_][]const support.Step{ reference.value.before, reference.value.after }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(originals[checked], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        checked += 1;
    };
    try std.testing.expectEqual(originals.len, checked);
    // Absence is a prepared dependency too: publication cannot silently change
    // an already prepared builtin conversion into a user-defined call.
    const Binding = @import("sql_prepared.zig").Binding;
    const absent: Binding = .{ .kind = .cast, .database = "default", .namespace = "", .name = "jsonb;text", .physical_name = "", .id = 0, .schema_version = 0, .revision = 1, .routine_incarnation = @splat(1), .cast_database_id = 1, .cast_pair = .{ .source = .{ .scalar = .jsonb }, .target = .{ .scalar = .text } } };
    try Binding.verifyCast(&.{absent}, absent);
    var current = absent;
    current.id = 10;
    current.schema_version = 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verifyCast(&.{absent}, current));
    current = absent;
    current.cast_database_id = 3;
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verifyCast(&.{absent}, current));
    current = absent;
    current.routine_incarnation = @splat(0);
    try std.testing.expectError(error.InvalidSqlBackendResponse, Binding.verifyCast(&.{absent}, current));
    current = absent;
    current.schema_version = 1;
    try std.testing.expectError(error.InvalidSqlBackendResponse, Binding.verifyCast(&.{absent}, current));
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}

fn lostAck(ctx: *fixture.Context) !void {
    const http = @import("http_server.zig");
    const calls = @import("../system_catalog/server_call.zig");
    const casts = @import("../system_catalog/casts.zig");
    const Probe = struct {
        source: http.StatusSource,
        hidden: bool = true,
        mutations: usize = 0,
        fn status(raw: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return (@as(*@This(), @ptrCast(@alignCast(raw)))).source.status();
        }
        fn catalog(raw: *anyopaque, a: std.mem.Allocator, context: local.api_operation.RequestContext, call: calls.Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .sql_cast_outcome and self.hidden) return error.MetadataMutationOutcomeUnknown;
            const bytes = try self.source.systemCatalog(a, context, call);
            if (call == .sql_cast_mutate) {
                self.mutations += 1;
                a.free(bytes);
                return error.MetadataMutationOutcomeUnknown;
            }
            return bytes;
        }
    };
    const original = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var probe: Probe = .{ .source = original.source };
    var gateway = http.ApiHttpServer.init(ctx.alloc, .{}, .{ .ptr = &probe, .vtable = &.{ .supports_sql_routines = true, .supports_sql_casts = true, .status = Probe.status, .system_catalog = Probe.catalog } }, null, null);
    defer gateway.deinit();
    gateway.embedding_provider_runtime.io = ctx.io;
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: @import("sql_execution.zig").Adapter = .{ .server = &gateway, .identity = &identity, .context = .{} };
    defer adapter.releaseExecutionOwners();
    var parsed = try local.sql_compiler.compile(ctx.alloc, "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT", .{});
    defer parsed.deinit();
    const outcome = try @import("sql_cast_ddl.zig").execute(&adapter, .{ .result = ctx.alloc, .scratch = ctx.alloc }, parsed.statement.catalog_ddl);
    const receipt = outcome.catalog_receipt orelse return error.MissingCastReceipt;
    defer receipt.deinit(ctx.alloc);
    try std.testing.expectEqual(.cast, receipt.kind);
    try std.testing.expectEqual(.unknown, receipt.state);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    try std.testing.expect(receipt.recovery_token.len < 2048);
    try command(ctx, "CREATE FUNCTION cast_receipt_noise() RETURNS text LANGUAGE sql AS $$SELECT 'noise'$$");
    probe.hidden = false;
    const recovered = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, receipt.recovery_token);
    defer recovered.deinit(ctx.alloc);
    try std.testing.expectEqual(.committed, recovered.state);
    const bytes = try original.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_cast_snapshot = .{ .database = "default", .pair = .{ .source = .{ .scalar = .jsonb }, .target = .{ .scalar = .text } } } });
    defer ctx.alloc.free(bytes);
    var cut = try std.json.parseFromSlice(casts.Snapshot, ctx.alloc, bytes, .{});
    defer cut.deinit();
    try std.testing.expectEqual(cut.value.record.?.id, recovered.object_id.?);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    try command(ctx, "DROP CAST (jsonb AS text)");
}
