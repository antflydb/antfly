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

//! Public enum catalog qualification only. These checks do not certify native
//! enum columns, indexed values or dependent-schema activation.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const support = @import("sql_callable_lifecycle_reference.zig");
const types = @import("../system_catalog/named_types.zig");
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    reference: support.Reference,
    identity: ?types.records.Identity = null,
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
        try connectedBatch(ctx, self.reference.before);
        self.identity = try inspect(ctx);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "ALTER TYPE usage_status ADD VALUE 'denied'", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expectEqualDeep(self.identity.?, try inspect(ctx));
        try connectedBatch(ctx, self.reference.after);
        try lostAck(ctx);
    }
};
fn connectedBatch(ctx: *fixture.Context, steps: []const support.Step) !void {
    var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
    defer opened.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), opened.status);
    var connection = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLConnectionResponse, ctx.alloc, opened.body, .{});
    defer connection.deinit();
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{connection.value.connection_id});
    defer ctx.alloc.free(path);
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    for (steps) |step| {
        var response = try client.executeConnected(&authenticated, null, connection.value.connection_id, step.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        try support.compareResponse(ctx, step, response);
    }
    var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .DELETE, null, 15_000);
    defer closed.deinit(ctx.alloc);
    try std.testing.expect(closed.status == 200 or closed.status == 204);
}
fn inspect(ctx: *fixture.Context) !types.records.Identity {
    const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var region = std.heap.ArenaAllocator.init(ctx.alloc);
    defer region.deinit();
    const a = region.allocator();
    const targets = [_]local.system_catalog_domain.Target{ .{ .table = "usage_status" }, .{ .namespace = "enum_tools", .table = "usage_status" } };
    const request: types.SnapshotRequest = .{ .targets = &targets };
    const bytes = try server.source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_type_snapshot = request });
    const snapshot = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{});
    try snapshot.validate(a, request);
    const record = snapshot.found.?.record;
    var view = try types.records.View.init(a, record.definition);
    defer view.deinit(a);
    const expected = [_][]const u8{ "queued", "started", "processing", "done", "archived" };
    try std.testing.expectEqual(expected.len, view.ordered.len);
    for (view.ordered, expected) |ordinal, label| try std.testing.expectEqualStrings(label, record.definition.labels[ordinal].text);
    try std.testing.expect(record.identity.oid != 25 and record.identity.array_oid != record.identity.oid);
    try std.testing.expect(!std.mem.allEqual(u8, &record.identity.origin_id, 0));
    return record.identity;
}
test "mounted SQL named enum catalog publication preserves ordered symbols scoped names and cold outcomes" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(support.Reference, a, local.sql_parity_fixtures.enum_catalog_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0096", "sql-0097", "sql-0098", "sql-0099", "sql-0100" };
    var checked: usize = 0;
    for ([_][]const support.Step{ reference.value.before, reference.value.after }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(originals[checked], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        checked += 1;
    };
    try std.testing.expectEqual(originals.len, checked);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
fn lostAck(ctx: *fixture.Context) !void {
    const http = @import("http_server.zig");
    const calls = @import("../system_catalog/server_call.zig");
    const Probe = struct {
        source: http.StatusSource,
        hidden: bool = true,
        mutations: usize = 0,
        fn status(raw: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return (@as(*@This(), @ptrCast(@alignCast(raw)))).source.status();
        }
        fn catalog(raw: *anyopaque, a: std.mem.Allocator, context: local.api_operation.RequestContext, call: calls.Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .sql_type_outcome and self.hidden) return error.MetadataMutationOutcomeUnknown;
            const bytes = try self.source.systemCatalog(a, context, call);
            if (call == .sql_type_mutate) {
                self.mutations += 1;
                a.free(bytes);
                return error.MetadataMutationOutcomeUnknown;
            }
            return bytes;
        }
    };
    const original = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var probe: Probe = .{ .source = original.source };
    var gateway = http.ApiHttpServer.init(ctx.alloc, .{}, .{ .ptr = &probe, .vtable = &.{ .supports_sql_types = true, .status = Probe.status, .system_catalog = Probe.catalog } }, null, null);
    defer gateway.deinit();
    gateway.embedding_provider_runtime.io = ctx.io;
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: @import("sql_execution.zig").Adapter = .{ .server = &gateway, .identity = &identity, .context = .{} };
    defer adapter.releaseExecutionOwners();
    var parsed = try local.sql_compiler.compile(ctx.alloc, "CREATE TYPE recover_enum AS ENUM ('one','two')", .{});
    defer parsed.deinit();
    const outcome = try @import("sql_type_ddl.zig").execute(&adapter, .{ .result = ctx.alloc, .scratch = ctx.alloc }, parsed.statement.catalog_ddl);
    const receipt = outcome.catalog_receipt orelse return error.MissingTypeReceipt;
    defer receipt.deinit(ctx.alloc);
    try std.testing.expectEqual(.type, receipt.kind);
    try std.testing.expectEqual(.unknown, receipt.state);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    try std.testing.expect(receipt.recovery_token.len < 2048);
    probe.hidden = false;
    const recovered = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, receipt.recovery_token);
    defer recovered.deinit(ctx.alloc);
    try std.testing.expectEqual(.committed, recovered.state);
    try std.testing.expect(recovered.object_id != null);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
}
