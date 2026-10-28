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

const IngressCampaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    type_id: u64 = 0,
    schema_version: u32 = 0,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        try Campaign.configureAuth(manager);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        var created_type = try client.execute(ctx, "CREATE TYPE ingress_state AS ENUM ('z','a','')", .write_admission);
        defer created_type.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), created_type.status);
        const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        var region = std.heap.ArenaAllocator.init(ctx.alloc);
        defer region.deinit();
        const a = region.allocator();
        const request: types.SnapshotRequest = .{ .targets = &.{.{ .table = "ingress_state" }} };
        const bytes = try server.source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_type_snapshot = request });
        const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{});
        try cut.validate(a, request);
        self.type_id = cut.found.?.record.identity.id;
        const body = try std.json.Stringify.valueAlloc(a, .{
            .indexes = std.json.Value{ .object = .empty },
            .schema = .{
                .storage_mode = "relational",
                .default_type = "row",
                // A readonly assertion has no authority, even if malformed.
                .named_types = .{ .forged = "not a catalog definition" },
                .document_schemas = .{ .row = .{ .schema = .{
                    .type = "object",
                    .additionalProperties = false,
                    .properties = .{ .state = .{ .type = "sql_enum", .@"x-antfly-sql-type-id" = self.type_id, .nullable = true } },
                } } },
            },
        }, .{});
        var created = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/enum_rows", .POST, body, 15_000);
        defer created.deinit(ctx.alloc);
        errdefer std.debug.print("enum table create status={d} body={s}\n", .{ created.status, created.body });
        try std.testing.expect(created.status == 200 or created.status == 201);
        const serving = try fixture.waitServingSchema(ctx, "enum_rows", 0);
        self.schema_version = serving.version;
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "enum_rows");
        const mutations = try std.json.Stringify.valueAlloc(a, .{ .schema_version = self.schema_version, .mutations = .{
            .{ .key = "first", .expected_version = "0", .row = .{ .state = "z" } },
            .{ .key = "empty", .expected_version = "0", .row = .{ .state = "" } },
            .{ .key = "null", .expected_version = "0", .row = .{ .state = @as(?[]const u8, null) } },
        } }, .{});
        var written = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/enum_rows/rows/mutate", .POST, mutations, 15_000);
        defer written.deinit(ctx.alloc);
        errdefer std.debug.print("enum typed writes status={d} body={s}\n", .{ written.status, written.body });
        try std.testing.expectEqual(@as(u16, 201), written.status);
        const invalid = try std.json.Stringify.valueAlloc(a, .{ .schema_version = self.schema_version, .mutations = .{.{ .key = "invalid", .expected_version = "0", .row = .{ .state = "Z" } }} }, .{});
        var rejected = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/enum_rows/rows/mutate", .POST, invalid, 15_000);
        defer rejected.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), rejected.status);
        // PATCH binds the full postimage, replaces readonly assertions, and
        // preserves historical rows under a newly published immutable epoch.
        var patched = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/enum_rows/schema", .PATCH, "{\"named_types\":{\"forged\":true}}", 15_000);
        defer patched.deinit(ctx.alloc);
        errdefer std.debug.print("enum schema patch status={d} body={s}\n", .{ patched.status, patched.body });
        try std.testing.expectEqual(@as(u16, 200), patched.status);
        var updated = try std.json.parseFromSlice(struct { schema: struct { version: u32 } }, ctx.alloc, patched.body, .{ .ignore_unknown_fields = true });
        defer updated.deinit();
        self.schema_version = updated.value.schema.version;
        try self.observe(ctx);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try self.observe(ctx);
    }
    fn observe(self: *@This(), ctx: *fixture.Context) !void {
        const serving = try fixture.waitServingSchema(ctx, "enum_rows", self.schema_version);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        var table = try fixture.table(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "enum_rows");
        defer table.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), table.status);
        // Public reads retain the server-resolved executable snapshot.
        var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, table.body, .{});
        defer parsed.deinit();
        const registry = parsed.value.object.get("schema").?.object.get("named_types").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), registry.len);
        try std.testing.expectEqual(self.type_id, @as(u64, @intCast(registry[0].object.get("identity").?.object.get("id").?.integer)));
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema_version = self.schema_version, .fields = &[_][]const u8{"state"}, .limit = 4 }, .{});
        defer ctx.alloc.free(body);
        var rows = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/enum_rows/rows/query", .POST, body, 15_000);
        defer rows.deinit(ctx.alloc);
        errdefer std.debug.print("enum projected rows status={d} body={s}\n", .{ rows.status, rows.body });
        try std.testing.expectEqual(@as(u16, 200), rows.status);
        const Row = struct { _id: []const u8, row: struct { state: ?[]const u8 } };
        var lines = std.mem.tokenizeScalar(u8, rows.body, '\n');
        const expected_keys = [_][]const u8{ "empty", "first", "null" };
        const expected_labels = [_]?[]const u8{ "", "z", null };
        for (expected_keys, expected_labels) |key, label| {
            const line = lines.next() orelse return error.MissingEnumRow;
            var row = try std.json.parseFromSlice(Row, ctx.alloc, line, .{ .ignore_unknown_fields = true });
            defer row.deinit();
            try std.testing.expectEqualStrings(key, row.value._id);
            if (label) |text| try std.testing.expectEqualStrings(text, row.value.row.state.?) else try std.testing.expect(row.value.row.state == null);
        }
        try std.testing.expect(lines.next() == null);
        const filtered_body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema_version = self.schema_version, .fields = &[_][]const u8{"state"}, .conditions = .{.{ .column = "state", .op = "lt", .value = "a" }} }, .{});
        defer ctx.alloc.free(filtered_body);
        var filtered = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/enum_rows/rows/query", .POST, filtered_body, 15_000);
        defer filtered.deinit(ctx.alloc);
        errdefer std.debug.print("enum filtered rows status={d} body={s}\n", .{ filtered.status, filtered.body });
        try std.testing.expectEqual(@as(u16, 200), filtered.status);
        var matches = std.mem.tokenizeScalar(u8, filtered.body, '\n');
        var match = try std.json.parseFromSlice(Row, ctx.alloc, matches.next() orelse return error.MissingEnumRow, .{ .ignore_unknown_fields = true });
        defer match.deinit();
        try std.testing.expectEqualStrings("first", match.value._id);
        try std.testing.expectEqualStrings("z", match.value.row.state.?);
        try std.testing.expect(matches.next() == null);
    }
};

test "mounted SQL named enum schema binding prepares public typed writes and retains cold projections" {
    try fixture.runWithState(IngressCampaign, .{});
}
