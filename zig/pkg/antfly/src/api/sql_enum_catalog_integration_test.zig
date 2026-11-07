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

//! Public catalog and native enum ingress qualification across cold owners.
//! Dependent-schema activation is qualified separately.
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
    type_comment_sql: []const u8,
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
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        var created = try client.execute(&authenticated, "CREATE TYPE usage_status_enum AS ENUM ('queued','done')", .write_admission);
        defer created.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), created.status);
        var annotated = try client.execute(&authenticated, self.type_comment_sql, .write_admission);
        defer annotated.deinit(ctx.alloc);
        try support.compareResponse(ctx, .{ .sql = self.type_comment_sql, .command_tag = "COMMENT", .notices = &.{} }, annotated);
        try inspectComment(ctx, "usage status enum");
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "ALTER TYPE usage_status ADD VALUE 'denied'", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
        var denied_comment = try client.execute(&reader, "COMMENT ON TYPE usage_status_enum IS 'forged'", .write_admission);
        defer denied_comment.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied_comment.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expectEqualDeep(self.identity.?, try inspect(ctx));
        try inspectComment(ctx, "usage status enum");
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        var removed = try client.execute(&authenticated, "COMMENT ON TYPE usage_status_enum IS NULL", .write_admission);
        defer removed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), removed.status);
        try inspectComment(ctx, null);
        var dropped = try client.execute(&authenticated, "DROP TYPE usage_status_enum", .write_admission);
        defer dropped.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), dropped.status);
        try connectedBatch(ctx, self.reference.after);
        try lostAck(ctx);
    }
};
fn inspectComment(ctx: *fixture.Context, expected: ?[]const u8) !void {
    const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var region = std.heap.ArenaAllocator.init(ctx.alloc);
    defer region.deinit();
    const a = region.allocator();
    const request: types.SnapshotRequest = .{ .targets = &.{.{ .table = "usage_status_enum" }} };
    const bytes = try server.source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_type_snapshot = request });
    const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{});
    try cut.validate(a, request);
    const found = cut.found orelse return error.MissingTypeAfterPublication;
    try std.testing.expectEqual(@as(u32, 1), found.record.identity.generation);
    if (expected) |text| try std.testing.expectEqualStrings(text, found.description.?) else try std.testing.expect(found.description == null);
}
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
    try fixture.runWithState(Campaign, .{ .reference = reference.value, .type_comment_sql = (try corpus.get("sql-0149")).sql });
}
fn lostAck(ctx: *fixture.Context) !void {
    return lostAckWithSql(ctx, "CREATE TYPE recover_enum AS ENUM ('one','two')", "COMMENT ON TYPE recover_enum IS 'retained outcome'");
}
fn lostAckWithSql(ctx: *fixture.Context, create_sql: []const u8, comment_sql: []const u8) !void {
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
    var parsed = try local.sql_compiler.compile(ctx.alloc, create_sql, .{});
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
    probe.hidden = true;
    probe.mutations = 0;
    var annotation = try local.sql_compiler.compile(ctx.alloc, comment_sql, .{});
    defer annotation.deinit();
    // Catalog observation supplies a fresh cut after the preceding creation.
    adapter.revision = null;
    const annotated = try @import("sql_type_ddl.zig").execute(&adapter, .{ .result = ctx.alloc, .scratch = ctx.alloc }, annotation.statement.catalog_ddl);
    const comment_receipt = annotated.catalog_receipt orelse return error.MissingTypeReceipt;
    defer comment_receipt.deinit(ctx.alloc);
    try std.testing.expectEqual(.unknown, comment_receipt.state);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    probe.hidden = false;
    const comment_recovered = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, comment_receipt.recovery_token);
    defer comment_recovered.deinit(ctx.alloc);
    try std.testing.expectEqual(.committed, comment_recovered.state);
    try std.testing.expectEqual(recovered.object_id, comment_recovered.object_id);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    probe.hidden = true;
    probe.mutations = 0;
    const drop_sql = try std.fmt.allocPrint(ctx.alloc, "DROP {s} {s}", .{ if (parsed.statement.catalog_ddl.named_type_kind == .domain) "DOMAIN" else "TYPE", parsed.statement.catalog_ddl.name.table });
    defer ctx.alloc.free(drop_sql);
    var drop_statement = try local.sql_compiler.compile(ctx.alloc, drop_sql, .{});
    defer drop_statement.deinit();
    adapter.revision = null;
    const dropped = try @import("sql_type_ddl.zig").execute(&adapter, .{ .result = ctx.alloc, .scratch = ctx.alloc }, drop_statement.statement.catalog_ddl);
    const drop_receipt = dropped.catalog_receipt orelse return error.MissingTypeReceipt;
    defer drop_receipt.deinit(ctx.alloc);
    try std.testing.expectEqual(.unknown, drop_receipt.state);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    probe.hidden = false;
    const drop_recovered = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, drop_receipt.recovery_token);
    defer drop_recovered.deinit(ctx.alloc);
    try std.testing.expectEqual(.committed, drop_recovered.state);
    try std.testing.expect(drop_recovered.object_id == null and drop_recovered.generation == null);
    const drop_again = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, drop_receipt.recovery_token);
    defer drop_again.deinit(ctx.alloc);
    try std.testing.expectEqual(drop_recovered.catalog_revision, drop_again.catalog_revision);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
}

const IngressCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    type_id: u64 = 0,
    schema_version: u32 = 0,
    prepared_id: [32]u8 = undefined,
    cast_prepared_id: [32]u8 = undefined,
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
        for ([_][]const u8{ "CREATE TABLE declared_enum_rows (id BIGINT PRIMARY KEY, state ingress_state DEFAULT 'a', mirror public.ingress_state GENERATED ALWAYS AS (state) STORED, CHECK (state < ''))", "INSERT INTO declared_enum_rows(id,state) VALUES(1,'a')", "INSERT INTO declared_enum_rows(id) VALUES(2)" }) |statement| {
            var declaration = try client.execute(ctx, statement, .write_admission);
            defer declaration.deinit(ctx.alloc);
            if (declaration.status != 200) std.debug.print("enum declaration SQL={s} status={d} body={s}\n", .{ statement, declaration.status, declaration.body });
            try std.testing.expectEqual(@as(u16, 200), declaration.status);
        }
        try observeCasts(ctx);
        var cast_type = try client.execute(ctx, "CREATE TYPE cast_lifecycle_state AS ENUM ('a','z')", .write_admission);
        defer cast_type.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), cast_type.status);
        var cast_prepared = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/prepared", .POST, "{\"statement\":\"SELECT 'a'::cast_lifecycle_state AS value\"}", 15_000);
        defer cast_prepared.deinit(ctx.alloc);
        if (cast_prepared.status != 200) std.debug.print("enum cast prepare status={d} body={s}\n", .{ cast_prepared.status, cast_prepared.body });
        try std.testing.expectEqual(@as(u16, 200), cast_prepared.status);
        var cast_resource = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLPreparedResponse, ctx.alloc, cast_prepared.body, .{});
        defer cast_resource.deinit();
        try std.testing.expectEqual(@as(usize, 32), cast_resource.value.prepared_id.len);
        @memcpy(&self.cast_prepared_id, cast_resource.value.prepared_id);
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
        for ([_][]const u8{ "a", "z" }) |label| {
            var mutation = try client.executeParameters(ctx, "UPDATE enum_rows SET state=$1 WHERE _id='first' RETURNING state", &.{.{ .string = label }}, .write_admission);
            defer mutation.deinit(ctx.alloc);
            errdefer std.debug.print("enum SQL mutation status={d} body={s}\n", .{ mutation.status, mutation.body });
            try std.testing.expectEqual(@as(u16, 200), mutation.status);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, mutation.body, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
            try std.testing.expectEqualStrings(label, result.value.rows[0][0].string);
            try std.testing.expectEqual(self.type_id, result.value.columns[0].named_type.?.id);
        }
        var prepared = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/prepared", .POST, "{\"statement\":\"SELECT state FROM enum_rows WHERE state = $1\"}", 15_000);
        defer prepared.deinit(ctx.alloc);
        errdefer std.debug.print("enum prepare status={d} body={s}\n", .{ prepared.status, prepared.body });
        try std.testing.expectEqual(@as(u16, 200), prepared.status);
        var prepared_body = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLPreparedResponse, ctx.alloc, prepared.body, .{});
        defer prepared_body.deinit();
        try std.testing.expectEqual(@as(usize, 32), prepared_body.value.prepared_id.len);
        @memcpy(&self.prepared_id, prepared_body.value.prepared_id);
        try std.testing.expectEqual(self.type_id, prepared_body.value.parameter_descriptors[0].named_type.?.id);
        try self.observe(ctx);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try observeCasts(ctx);
        try self.observe(ctx);
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}/execute", .{self.cast_prepared_id});
        defer ctx.alloc.free(path);
        var before = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, path, .POST, "{}", 15_000);
        defer before.deinit(ctx.alloc);
        if (before.status != 200) std.debug.print("cold enum cast execute status={d} body={s}\n", .{ before.status, before.body });
        try std.testing.expectEqual(@as(u16, 200), before.status);
        for ([_][]const u8{ "DROP TYPE cast_lifecycle_state", "CREATE TYPE cast_lifecycle_state AS ENUM ('a','z')" }) |statement| {
            var response = try client.execute(ctx, statement, .write_admission);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
        }
        var after = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, path, .POST, "{}", 15_000);
        defer after.deinit(ctx.alloc);
        if (after.status == 200) std.debug.print("retargeted enum cast body={s}\n", .{after.body});
        try std.testing.expect(after.status != 200);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, after.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("40001", diagnostic.value.code);
    }
    fn observeCasts(ctx: *fixture.Context) !void {
        for ([_][]const u8{
            "SELECT 'a'::ingress_state AS value",
            "SELECT CAST('a' AS public.ingress_state) AS value",
            "SELECT ('a'::text)::ingress_state AS value",
            "SELECT CASE WHEN false THEN ('invalid'::text)::ingress_state ELSE 'a'::ingress_state END AS value",
            "SELECT 'a'::pg_catalog.text AS value",
            "SELECT 'a'::\"text\" AS value",
        }) |statement| {
            var response = try client.execute(ctx, statement, .read);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("enum cast SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var body = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer body.deinit();
            try std.testing.expectEqual(@as(usize, 1), body.value.rows.len);
            try std.testing.expectEqualStrings("a", body.value.rows[0][0].string);
        }
    }
    fn observe(self: *@This(), ctx: *fixture.Context) !void {
        var declared = try client.execute(ctx, "SELECT state,mirror FROM declared_enum_rows WHERE state='a'", .read);
        defer declared.deinit(ctx.alloc);
        if (declared.status != 200) std.debug.print("enum declared read status={d} body={s}\n", .{ declared.status, declared.body });
        try std.testing.expectEqual(@as(u16, 200), declared.status);
        var declared_result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, declared.body, .{});
        defer declared_result.deinit();
        try std.testing.expectEqual(@as(usize, 2), declared_result.value.rows.len);
        for (declared_result.value.rows) |row| {
            try std.testing.expect(row[0] == .string and row[1] == .string);
            try std.testing.expectEqualStrings("a", row[0].string);
            try std.testing.expectEqualStrings("a", row[1].string);
        }
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
        const Query = struct { sql: []const u8, expected: []const u8, enumeration: bool = true };
        const queries = [_]Query{
            .{ .sql = "SELECT state FROM enum_rows ORDER BY state", .expected = "[[\"z\"],[\"\"],[null]]" },
            .{ .sql = "SELECT state FROM enum_rows WHERE state < 'a'", .expected = "[[\"z\"]]" },
            .{ .sql = "SELECT state::text FROM enum_rows ORDER BY state", .expected = "[[\"\"],[\"z\"],[null]]", .enumeration = false },
            .{ .sql = "SELECT state::text FROM enum_rows ORDER BY enum_rows.state", .expected = "[[\"z\"],[\"\"],[null]]", .enumeration = false },
            .{ .sql = "SELECT min(state), max(state) FROM enum_rows", .expected = "[[\"z\",\"\"]]" },
            .{ .sql = "SELECT DISTINCT state FROM enum_rows ORDER BY state", .expected = "[[\"z\"],[\"\"],[null]]" },
        };
        for (queries) |query| {
            var response = try client.execute(ctx, query.sql, .read);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("enum SQL query={s} status={d} body={s}\n", .{ query.sql, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            const encoded = try std.json.Stringify.valueAlloc(ctx.alloc, result.value.rows, .{});
            defer ctx.alloc.free(encoded);
            try std.testing.expectEqualStrings(query.expected, encoded);
            for (result.value.columns) |column| {
                if (query.enumeration) {
                    try std.testing.expectEqual(.enumeration, column.type);
                    const named = column.named_type orelse return error.MissingEnumIdentity;
                    try std.testing.expectEqual(self.type_id, named.id);
                    try std.testing.expectEqual(@as(u32, @intCast(16384 + self.type_id * 2)), named.oid);
                } else try std.testing.expectEqual(.string, column.type);
            }
        }
        var parameter = try client.executeParameters(ctx, "SELECT state FROM enum_rows WHERE state = $1", &.{.{ .string = "z" }}, .read);
        defer parameter.deinit(ctx.alloc);
        errdefer std.debug.print("enum parameter status={d} body={s}\n", .{ parameter.status, parameter.body });
        try std.testing.expectEqual(@as(u16, 200), parameter.status);
        var parameter_result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, parameter.body, .{});
        defer parameter_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), parameter_result.value.rows.len);
        try std.testing.expectEqualStrings("z", parameter_result.value.rows[0][0].string);
        const prepared_path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}/execute", .{self.prepared_id});
        defer ctx.alloc.free(prepared_path);
        var prepared = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, prepared_path, .POST, "{\"parameters\":[\"z\"]}", 15_000);
        defer prepared.deinit(ctx.alloc);
        errdefer std.debug.print("enum durable execute status={d} body={s}\n", .{ prepared.status, prepared.body });
        try std.testing.expectEqual(@as(u16, 200), prepared.status);
        var prepared_result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, prepared.body, .{});
        defer prepared_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), prepared_result.value.rows.len);
        try std.testing.expectEqualStrings("z", prepared_result.value.rows[0][0].string);
        var invalid = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, prepared_path, .POST, "{\"parameters\":[\"Z\"]}", 15_000);
        defer invalid.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), invalid.status);
    }
};

test "mounted SQL named enum schema binding prepares public typed writes and retains cold projections" {
    try fixture.runWithState(IngressCampaign, .{});
}

const EpochCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    create_sql: []const u8,
    add_sql: []const u8,
    conditional: support.Step,
    drop_sql: []const u8,
    owners: [2]fixture.ServingSchema = undefined,
    token: [8192]u8 = undefined,
    token_len: usize = 0,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        try Campaign.configureAuth(manager);
    }
    fn command(ctx: *fixture.Context, sql: []const u8) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status == 202 and std.mem.startsWith(u8, sql, "CREATE TABLE ")) {
            var value = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer value.deinit();
            const receipt = value.value.ddl_receipt orelse return error.MissingSchemaValidationToken;
            try std.testing.expect(receipt.fk_generation_publication_id != null);
            try std.testing.expectEqual(.pending, receipt.state);
            const serving = fixture.waitServingSchema(ctx, receipt.table, 0) catch |err| {
                const server = ctx.metadata.server.owned_public_http_server orelse return err;
                const private: local.api_operation.RequestContext = .{ .setting_admin = true, .fk_generation_publication_authority = true };
                const raw = server.source.systemCatalog(ctx.alloc, private, .{ .fk_initial_create_status = try std.fmt.parseInt(u64, receipt.table_id, 10) }) catch null;
                defer if (raw) |bytes| ctx.alloc.free(bytes);
                if (raw) |bytes| {
                    var status = try std.json.parseFromSlice(@import("../metadata/fk_generation_publication.zig").InitialPublication, ctx.alloc, bytes, .{});
                    defer status.deinit();
                    const publication_state = status.value;
                    std.debug.print("enum initial FK stalled phase={s} revision={} provisioned={} staged={} activated={} acknowledged={} released={} authority={}\n", .{ @tagName(publication_state.phase), publication_state.revision, publication_state.child_provisioned.len, publication_state.parent_staged.len, publication_state.parent_activated.len, publication_state.parent_acknowledged.len, publication_state.child_released.len, server.cfg.catalog_publication_authority_available });
                }
                return err;
            };
            try std.testing.expectEqual(serving.table_id, try std.fmt.parseInt(u64, receipt.table_id, 10));
            return;
        }
        if (response.status != 200) std.debug.print("enum epoch SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    fn rows(ctx: *fixture.Context, archived: bool) !void {
        const Ordering = struct { projection: []const u8, order_by: []const u8, rows: []const []const u8 };
        var reference = try std.json.parseFromSlice(struct { ordering: []const Ordering }, ctx.alloc, local.sql_parity_fixtures.enum_execution_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        // PostgreSQL's bare output label sorts the cast as TEXT; a qualified
        // input retains enum rank. Check both domains after cold publication.
        for ([_][]const u8{ "enum_epoch_parent", "enum_epoch_child" }) |table| {
            for (reference.value.ordering) |ordering| {
                const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT {s} FROM {s} AS e ORDER BY {s}", .{ ordering.projection, table, ordering.order_by });
                defer ctx.alloc.free(sql);
                var response = try client.execute(ctx, sql, .read);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("enum epoch rows SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var value = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
                defer value.deinit();
                const expected: []const []const u8 = if (archived) ordering.rows else &.{"queued"};
                try std.testing.expectEqual(expected.len, value.value.rows.len);
                for (expected, value.value.rows) |label, row| try std.testing.expectEqualStrings(label, row[0].string);
            }
        }
    }
    fn observation(self: *@This(), ctx: *fixture.Context, reader: bool) !@import("antfly_metadata_openapi").types.SQLCatalogOperationState {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = self.token[0..self.token_len] }, .{});
        defer ctx.alloc.free(body);
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, if (reader) ctx.reader_headers else ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
        defer response.deinit(ctx.alloc);
        if (reader) {
            try std.testing.expectEqual(@as(u16, 403), response.status);
            return .pending;
        }
        if (response.status != 200) std.debug.print("enum epoch recovery status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var value = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
        defer value.deinit();
        try std.testing.expectEqualStrings(self.token[0..self.token_len], value.value.recovery_token);
        if (value.value.state == .committed) {
            try std.testing.expectEqualStrings("2", value.value.generation.?);
            try std.testing.expect(value.value.catalog_revision != null);
        }
        return value.value.state;
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try command(ctx, self.create_sql);
        try command(ctx, "CREATE TABLE enum_epoch_parent (id UUID PRIMARY KEY, state usage_status UNIQUE)");
        try command(ctx, "CREATE TABLE enum_epoch_child (id UUID PRIMARY KEY, state usage_status REFERENCES enum_epoch_parent(state))");
        for ([_][]const u8{ "enum_epoch_parent", "enum_epoch_child" }, &self.owners) |name, *owner| {
            owner.* = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
        }
        try command(ctx, "INSERT INTO enum_epoch_parent VALUES('00000000-0000-0000-0000-000000000001','queued')");
        try command(ctx, "INSERT INTO enum_epoch_child VALUES('00000000-0000-0000-0000-000000000001','queued')");
        try rows(ctx, false);
        ctx.data.pauseSchemaValidationAckForTest(self.owners[0].table_id, .ready);
        var response = try client.execute(ctx, self.add_sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 202) std.debug.print("enum epoch begin status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 202), response.status);
        var value = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
        defer value.deinit();
        try std.testing.expectEqualStrings("DDL PENDING", value.value.command_tag);
        const receipt = value.value.catalog_receipt orelse return error.MissingTypeReceipt;
        try std.testing.expectEqual(.pending, receipt.state);
        try std.testing.expect(receipt.generation == null and receipt.catalog_revision == null);
        self.token_len = receipt.recovery_token.len;
        try std.testing.expect(self.token_len <= self.token.len);
        @memcpy(self.token[0..self.token_len], receipt.recovery_token);
        try std.testing.expectEqual(.pending, try self.observation(ctx, false));
        _ = try self.observation(ctx, true);
        // Deterministically retain one accepted OLD-reporter readiness proof
        // across restart while the other owner's real ACK is paused. Without
        // current-runtime re-attestation the coordinator cannot install.
        const ready_deadline = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (true) {
            const page = (try self.recoveryPage(ctx)) orelse return error.MissingTypeOutcome;
            const head = page.head orelse return error.MissingTypeOutcome;
            try std.testing.expectEqual(@as(u16, 2), head.owner_count);
            try std.testing.expectEqual(@import("../metadata/schema_validation_publication.zig").Phase.validating, head.phase);
            try std.testing.expect(head.ready <= 1);
            if (head.ready == 1 and ctx.data.schemaValidationScanPausedForTest()) break;
            if (@import("antfly_platform").time.monotonicNs() >= ready_deadline) {
                try self.dumpRecovery(ctx);
                return error.EnumEpochReadyBarrierTimeout;
            }
            try ctx.checkDrivers();
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        try rows(ctx, false);
    }
    fn rejected(ctx: *fixture.Context, sql: []const u8, code: []const u8) !void {
        var response = try client.execute(ctx, sql, .never);
        defer response.deinit(ctx.alloc);
        if (response.status != 400) std.debug.print("enum epoch rejection SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 400), response.status);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(code, diagnostic.value.code);
    }
    fn positioned(ctx: *fixture.Context) !void {
        const wire = @import("antfly_metadata_openapi").types;
        var response = try client.execute(ctx, "ALTER TYPE usage_status ADD VALUE 'started' BEFORE 'processing'", .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("enum positioned epoch status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        const receipt = result.value.catalog_receipt orelse return error.MissingTypeReceipt;
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = receipt.recovery_token }, .{});
        defer ctx.alloc.free(body);
        const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (true) {
            var recovered = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
            defer recovered.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), recovered.status);
            var outcome = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, recovered.body, .{});
            defer outcome.deinit();
            if (outcome.value.state == .committed) {
                try std.testing.expectEqualStrings("3", outcome.value.generation.?);
                break;
            }
            try std.testing.expectEqual(.pending, outcome.value.state);
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.EnumEpochRecoveryTimeout;
            try ctx.checkDrivers();
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        for ([_][]const u8{ "enum_epoch_parent", "enum_epoch_child" }) |table| {
            const sql = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO {s} VALUES('00000000-0000-0000-0000-000000000003','started')", .{table});
            defer ctx.alloc.free(sql);
            try command(ctx, sql);
        }
        var golden = try std.json.parseFromSlice(struct { positioned_ordering: []const []const u8 }, ctx.alloc, local.sql_parity_fixtures.enum_execution_reference, .{ .ignore_unknown_fields = true });
        defer golden.deinit();
        for ([_][]const u8{ "enum_epoch_parent", "enum_epoch_child" }) |table| {
            const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT e.state FROM {s} AS e ORDER BY e.state", .{table});
            defer ctx.alloc.free(sql);
            var rows_response = try client.execute(ctx, sql, .read);
            defer rows_response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), rows_response.status);
            var rows_value = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, rows_response.body, .{});
            defer rows_value.deinit();
            try std.testing.expectEqual(golden.value.positioned_ordering.len, rows_value.value.rows.len);
            for (golden.value.positioned_ordering, rows_value.value.rows) |label, row| try std.testing.expectEqualStrings(label, row[0].string);
        }
    }
    fn recoveryPage(self: *@This(), ctx: *fixture.Context) !?@import("../metadata/schema_validation_work.zig").Page {
        const svc = ctx.metadata.server.svc;
        const store = svc.projectedStore() orelse return error.MissingMetadataStore;
        const incarnation = (try svc.metadataIncarnation()) orelse return error.MetadataIncarnationUnavailable;
        var region = std.heap.ArenaAllocator.init(ctx.alloc);
        defer region.deinit();
        const a = region.allocator();
        const reads = @import("../metadata/schema_validation_read.zig");
        const lookup = (try std.json.parseFromSliceLeaky(?reads.Lookup, a, try store.schemaValidationDiscover(a, svc.metadata_group_id, .{ .group = svc.metadata_group_id, .metadata_incarnation = incarnation, .table = self.owners[0].table_id }), .{})) orelse {
            return null;
        };
        const work = @import("../metadata/schema_validation_work.zig");
        const page = try std.json.parseFromSliceLeaky(work.Page, a, try store.schemaValidationWork(a, lookup.group, .{ .group = lookup.group, .metadata_incarnation = lookup.metadata_incarnation, .cursor = .{ .owners = .{ .lookup = lookup, .ordinal = 0 } } }), .{});
        return page;
    }
    fn dumpRecovery(self: *@This(), ctx: *fixture.Context) !void {
        const page = (try self.recoveryPage(ctx)) orelse {
            std.debug.print("enum epoch diagnostic has no active head\n", .{});
            return;
        };
        const head = page.head orelse return error.MissingTypeOutcome;
        std.debug.print("enum epoch diagnostic phase={s} revision={d} ready={d}/{d} installed={d} released={d} attempts={d} deferred={d} failed={d}\n", .{ @tagName(head.phase), head.revision, head.ready, head.owner_count, head.installed, head.released, ctx.data.schema_recovery_attempts.load(.acquire), ctx.data.schema_recovery_deferred.load(.acquire), ctx.data.schema_recovery_failed.load(.acquire) });
        for (page.tasks[0..page.len]) |slot| {
            const task = slot.?;
            std.debug.print("enum epoch owner ordinal={d} table={d} group={d}\n", .{ task.ordinal, task.physicalTable(), task.group_id });
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        // Await the restarted peer registration separately from the schema
        // protocol's deadline; an old endpoint is not a schema ACK failure.
        for (self.owners) |owner| try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
        const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (try self.observation(ctx, false) != .committed) {
            try ctx.checkDrivers();
            if (@import("antfly_platform").time.monotonicNs() >= deadline) {
                std.debug.print("enum epoch owner recovery error={s}\n", .{ctx.data.schemaValidationLastErrorForTest()});
                self.dumpRecovery(ctx) catch |err| std.debug.print("enum epoch diagnostic unavailable err={s}\n", .{@errorName(err)});
                return error.EnumEpochRecoveryTimeout;
            }
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        for ([_][]const u8{ "enum_epoch_parent", "enum_epoch_child" }, self.owners) |name, before| {
            const owner = try fixture.waitServingSchema(ctx, name, before.version + 1);
            try std.testing.expectEqual(before.table_id, owner.table_id);
            try std.testing.expectEqual(before.version + 1, owner.version);
        }
        try rows(ctx, false);
        try command(ctx, "INSERT INTO enum_epoch_parent VALUES('00000000-0000-0000-0000-000000000002','archived')");
        try command(ctx, "INSERT INTO enum_epoch_child VALUES('00000000-0000-0000-0000-000000000002','archived')");
        try rows(ctx, true);
        try std.testing.expectEqual(.committed, try self.observation(ctx, false));
        // The unchanged conditional original emits PostgreSQL's notice without
        // another schema install. RESTRICT and failed writes preserve all rows.
        var conditional = try client.execute(ctx, self.conditional.sql, .never);
        defer conditional.deinit(ctx.alloc);
        try support.compareResponse(ctx, self.conditional, conditional);
        for ([_][]const u8{ "enum_epoch_parent", "enum_epoch_child" }, self.owners) |name, before| {
            try std.testing.expectEqual(before.version + 1, (try fixture.waitServingSchema(ctx, name, before.version + 1)).version);
        }
        try rejected(ctx, self.drop_sql, "2BP01");
        try rejected(ctx, "INSERT INTO enum_epoch_parent VALUES('00000000-0000-0000-0000-000000000004','queued')", "23505");
        try rejected(ctx, "INSERT INTO enum_epoch_child VALUES('00000000-0000-0000-0000-000000000004','done')", "23503");
        try rows(ctx, true);
        try positioned(ctx);
        // A later generation cannot change an earlier nonce's exact outcome.
        try std.testing.expectEqual(.committed, try self.observation(ctx, false));
    }
};

test "mounted SQL named enum schema binding prepares public typed writes and retains cold projections through all owner additive epochs" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    var reference = try std.json.parseFromSlice(support.Reference, std.testing.allocator, local.sql_parity_fixtures.enum_catalog_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    const conditional = reference.value.before[3];
    try std.testing.expectEqualStrings("sql-0098", conditional.id.?);
    try std.testing.expectEqualStrings((try corpus.get("sql-0098")).sql, conditional.sql);
    try fixture.runWithState(EpochCampaign, .{ .create_sql = (try corpus.get("sql-0096")).sql, .add_sql = (try corpus.get("sql-0097")).sql, .conditional = conditional, .drop_sql = (try corpus.get("sql-0099")).sql });
}

const RetirementCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    const Golden = struct { retirement: struct {
        notices: []const struct { code: []const u8, message: []const u8 },
        sequence_values: []const i64,
        rows: std.json.Value,
        columns: std.json.Value,
    } };
    credentials: client.ApiKeyHeaders = .{},
    token: [8192]u8 = undefined,
    token_len: usize = 0,
    drop_sql: []const u8,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        try Campaign.configureAuth(manager);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try EpochCampaign.command(ctx, "CREATE TYPE usage_status AS ENUM ('queued','done')");
        try EpochCampaign.command(ctx, "CREATE TABLE retirement_parent (id UUID PRIMARY KEY, state usage_status UNIQUE, payload TEXT, seq BIGINT GENERATED ALWAYS AS IDENTITY)");
        try EpochCampaign.command(ctx, "CREATE TABLE retirement_child (id UUID PRIMARY KEY, state usage_status REFERENCES retirement_parent(state), payload TEXT)");
        for ([_][]const u8{ "retirement_parent", "retirement_child" }) |name| {
            const owner = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
            const sql = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO {s}(id,state,payload) VALUES('00000000-0000-0000-0000-000000000001','queued','retained')", .{name});
            defer ctx.alloc.free(sql);
            try EpochCampaign.command(ctx, sql);
        }
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, self.drop_sql, .never);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
        // Admit the real immutable job, then reopen before any worker can
        // pin/copy sources. Cold scheduling must adopt this exact work.
        const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        server.restore_dispatch_paused.store(true, .release);
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        var response = try client.execute(&coordinator, self.drop_sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("type retirement status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 202), response.status);
        var value = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
        defer value.deinit();
        const notices = value.value.notices orelse return error.MissingTypeCascadeNotice;
        var golden = try std.json.parseFromSlice(Golden, ctx.alloc, local.sql_parity_fixtures.enum_execution_reference, .{ .ignore_unknown_fields = true });
        defer golden.deinit();
        try std.testing.expectEqual(golden.value.retirement.notices.len, notices.len);
        for (golden.value.retirement.notices, notices) |expected, actual| {
            try std.testing.expectEqualStrings(expected.code, actual.code);
            try std.testing.expectEqualStrings(expected.message, actual.message);
        }
        const receipt = value.value.catalog_receipt orelse return error.MissingTypeReceipt;
        try std.testing.expectEqual(.pending, receipt.state);
        self.token_len = receipt.recovery_token.len;
        try std.testing.expect(self.token_len <= self.token.len);
        @memcpy(self.token[0..self.token_len], receipt.recovery_token);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        var golden = try std.json.parseFromSlice(Golden, ctx.alloc, local.sql_parity_fixtures.enum_execution_reference, .{ .ignore_unknown_fields = true });
        defer golden.deinit();
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = self.token[0..self.token_len] }, .{});
        defer ctx.alloc.free(body);
        var committed_revision: u64 = 0;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
        while (true) {
            var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var receipt = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
            defer receipt.deinit();
            if (receipt.value.state == .committed) {
                try std.testing.expect(receipt.value.catalog_revision != null and receipt.value.generation == null);
                committed_revision = try std.fmt.parseInt(u64, receipt.value.catalog_revision.?, 10);
                break;
            }
            if (receipt.value.state != .pending) std.debug.print("type retirement recovery={s}\n", .{response.body});
            try std.testing.expectEqual(.pending, receipt.value.state);
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TypeRetirementTimeout;
            try ctx.checkDrivers();
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        for ([_][]const u8{ "retirement_parent", "retirement_child" }) |name| {
            const owner = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
            const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT * FROM {s}", .{name});
            defer ctx.alloc.free(sql);
            var response = try client.execute(ctx, sql, .read);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("type retirement rows={s}\n", .{response.body});
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            const expected_columns = golden.value.retirement.columns.object.get(name).?.array.items;
            try std.testing.expectEqual(expected_columns.len, result.value.columns.len);
            for (expected_columns, result.value.columns) |expected, actual| try std.testing.expectEqualStrings(expected.string, actual.name);
            const payload_column = for (result.value.columns, 0..) |column, ordinal| {
                if (std.mem.eql(u8, column.name, "payload")) break ordinal;
            } else return error.MissingRetainedPayload;
            const expected_rows = golden.value.retirement.rows.object.get(name).?.array.items;
            try std.testing.expectEqual(expected_rows.len, result.value.rows.len);
            for (expected_rows, result.value.rows) |expected, actual| try std.testing.expectEqualStrings(expected.string, actual[payload_column].string);
            const insert = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO {s}(id,payload) VALUES('00000000-0000-0000-0000-000000000002','new')", .{name});
            defer ctx.alloc.free(insert);
            try EpochCampaign.command(ctx, insert);
        }
        // The unchanged original observes the missing type after its dependent
        // publication, including PostgreSQL's IF EXISTS notice.
        try support.observe(ctx, .{ .sql = self.drop_sql, .command_tag = "DROP TYPE", .notices = &.{.{ .code = "00000", .message = "type \"usage_status\" does not exist, skipping" }} }, false);
        try EpochCampaign.command(ctx, "CREATE TYPE usage_status AS ENUM ('replacement')");
        var sequence_rows = try client.execute(ctx, "SELECT seq FROM retirement_parent ORDER BY seq", .read);
        defer sequence_rows.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), sequence_rows.status);
        var sequence_values = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, sequence_rows.body, .{});
        defer sequence_values.deinit();
        try std.testing.expectEqual(golden.value.retirement.sequence_values.len, sequence_values.value.rows.len);
        for (sequence_values.value.rows, golden.value.retirement.sequence_values) |row, expected| try std.testing.expectEqual(expected, try fixture.sqlInteger(row[0]));
        // Name reuse and later catalog changes cannot rewrite this receipt's
        // exact committed revision or make it observe the replacement type.
        var recovered = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
        defer recovered.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), recovered.status);
        var final = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLCatalogOperationReceipt, ctx.alloc, recovered.body, .{});
        defer final.deinit();
        try std.testing.expectEqual(.committed, final.value.state);
        try std.testing.expectEqual(committed_revision, try std.fmt.parseInt(u64, final.value.catalog_revision.?, 10));
        const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        var region = std.heap.ArenaAllocator.init(ctx.alloc);
        defer region.deinit();
        const cut = try std.json.parseFromSliceLeaky(types.Snapshot, region.allocator(), try server.source.systemCatalog(region.allocator(), .{ .setting_admin = true }, .{ .sql_type_snapshot = .{ .targets = &.{.{ .table = "usage_status" }}, .include_dependents = true } }), .{});
        try std.testing.expectEqual(@as(usize, 0), cut.dependents.len);
    }
};
test "mounted SQL named enum schema binding prepares public typed writes and retains cold projections through dependent retirement" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    try fixture.runWithState(RetirementCampaign, .{ .drop_sql = (try corpus.get("sql-0100")).sql });
}

const DomainCatalogCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub const configureAuth = Campaign.configureAuth;
    reference: support.Reference,
    credentials: client.ApiKeyHeaders = .{},
    identity: ?types.records.Identity = null,

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try connectedBatch(ctx, self.reference.before);
        self.identity = (try inspectDomain(ctx, "status domain")).identity;
        try checkValues(ctx, self.identity.?);
        var admin = ctx.*;
        admin.headers = ctx.admin_headers;
        for ([_][]const u8{
            "CREATE DOMAIN premature_nested_domain AS usage_status",
        }) |sql| {
            var guarded = try client.execute(&admin, sql, .write_admission);
            defer guarded.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 501), guarded.status);
        }
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "COMMENT ON DOMAIN usage_status IS 'forged'", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expectEqualDeep(self.identity.?, (try inspectDomain(ctx, "status domain")).identity);
        try checkValues(ctx, self.identity.?);
        const metadata = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const retired_request: types.SnapshotRequest = .{ .targets = &.{.{ .table = "positive_amount" }} };
        const retired_bytes = try metadata.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_type_snapshot = retired_request });
        defer ctx.alloc.free(retired_bytes);
        var retired = try std.json.parseFromSlice(types.Snapshot, ctx.alloc, retired_bytes, .{});
        defer retired.deinit();
        try retired.value.validate(ctx.alloc, retired_request);
        try std.testing.expect(retired.value.found == null);
        try connectedBatch(ctx, self.reference.cold[0..1]);
        try std.testing.expectEqualDeep(self.identity.?, (try inspectDomain(ctx, null)).identity);
        try connectedBatch(ctx, self.reference.cold[1..]);
        try std.testing.expectEqualDeep(self.identity.?, (try inspectDomain(ctx, "status domain")).identity);
        try lostAckWithSql(ctx, "CREATE DOMAIN recover_domain AS text DEFAULT 'queued' NOT NULL", "COMMENT ON DOMAIN recover_domain IS 'retained outcome'");
        try connectedBatch(ctx, self.reference.after);
        const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const request: types.SnapshotRequest = .{ .targets = &.{.{ .table = "usage_status" }} };
        const bytes = try server.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_type_snapshot = request });
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(types.Snapshot, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        try parsed.value.validate(ctx.alloc, request);
        try std.testing.expect(parsed.value.found == null);
    }
    fn inspectDomain(ctx: *fixture.Context, expected_comment: ?[]const u8) !struct { identity: types.records.Identity } {
        const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const request: types.SnapshotRequest = .{ .targets = &.{.{ .table = "usage_status" }} };
        const bytes = try server.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_type_snapshot = request });
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(types.Snapshot, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        try parsed.value.validate(ctx.alloc, request);
        const found = parsed.value.found orelse return error.MissingDomainAfterPublication;
        if (expected_comment) |text| try std.testing.expectEqualStrings(text, found.description.?) else try std.testing.expect(found.description == null);
        try std.testing.expectEqual(@as(u32, 1), found.record.identity.generation);
        try std.testing.expect(found.record.definition == .domain);
        const definition = found.record.definition.domain;
        try std.testing.expectEqual(.text, definition.base.builtin);
        try std.testing.expectEqual(@as(usize, 1), definition.checks.len);
        try std.testing.expect(!definition.not_null);
        var view = try local.sql_catalog.domain_values.View.init(ctx.alloc, definition);
        defer view.deinit();
        return .{ .identity = found.record.identity };
    }
    fn checkValues(ctx: *fixture.Context, identity: types.records.Identity) !void {
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        for ([_][]const u8{
            "SELECT 'queued'::usage_status AS status",
            "SELECT d.status FROM (SELECT 'queued'::usage_status AS status) d",
            "SELECT CASE WHEN false THEN 'forged'::usage_status ELSE 'queued'::usage_status END AS status",
            "SELECT 'queued'::usage_status AS status UNION ALL SELECT 'done'::usage_status",
        }) |sql| {
            var response = try client.execute(&authenticated, sql, .write_admission);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("domain value status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings("queued", result.value.rows[0][0].string);
            try std.testing.expectEqual(identity.id, result.value.columns[0].named_type.?.id);
            try std.testing.expectEqual(identity.generation, result.value.columns[0].named_type.?.generation);
            try std.testing.expectEqual(identity.oid, result.value.columns[0].named_type.?.oid);
        }
        try support.observe(ctx, .{ .sql = "SELECT 'forged'::usage_status", .sqlstate = "23514", .notices = &.{} }, false);
        var prepared = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, authenticated.headers, ctx.base, "/db/v1/sql/prepared", .POST, "{\"statement\":\"SELECT $1::usage_status AS status\"}", 15_000);
        defer prepared.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), prepared.status);
        var resource = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLPreparedResponse, ctx.alloc, prepared.body, .{});
        defer resource.deinit();
        try std.testing.expectEqual(identity.id, resource.value.parameter_descriptors[0].named_type.?.id);
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}/execute", .{resource.value.prepared_id});
        defer ctx.alloc.free(path);
        for ([_]struct { body: []const u8, status: u16 }{
            .{ .body = "{\"parameters\":[\"queued\"]}", .status = 200 },
            .{ .body = "{\"parameters\":[\"forged\"]}", .status = 400 },
        }) |case| {
            var executed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, authenticated.headers, ctx.base, path, .POST, case.body, 15_000);
            defer executed.deinit(ctx.alloc);
            try std.testing.expectEqual(case.status, executed.status);
        }
    }
};

test "mounted SQL domain catalog annotations and independent retirement preserve PostgreSQL diagnostics cold identity and outcomes" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(support.Reference, a, local.sql_parity_fixtures.domain_annotation_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const ids = [_][]const u8{ "sql-0150", "sql-0309" };
    var originals: usize = 0;
    for (reference.value.before) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(ids[originals], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        originals += 1;
    };
    try std.testing.expectEqual(ids.len, originals);
    try fixture.runWithState(DomainCatalogCampaign, .{ .reference = reference.value });
}

const DomainRowCampaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    pub const configureAuth = Campaign.configureAuth;
    reference: support.Reference,

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        var admin = ctx.*;
        admin.headers = ctx.admin_headers;
        try phase(&admin, self.reference.before);
        try check(&admin, 2);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        var admin = ctx.*;
        admin.headers = ctx.admin_headers;
        try check(&admin, 2);
        try phase(&admin, self.reference.cold);
        try check(&admin, 3);
        try phase(&admin, self.reference.after);
    }
    fn phase(ctx: *fixture.Context, steps: []const support.Step) !void {
        for (steps) |step| {
            if (std.mem.startsWith(u8, step.sql, "CREATE TABLE ")) {
                try EpochCampaign.command(ctx, step.sql);
            } else if (std.mem.startsWith(u8, step.sql, "DROP DOMAIN ") or std.mem.startsWith(u8, step.sql, "ALTER DOMAIN ")) {
                var coordinator = ctx.*;
                coordinator.base = ctx.metadata_uri;
                try support.observe(&coordinator, step, false);
            } else try support.observe(ctx, step, false);
        }
    }
    fn check(ctx: *fixture.Context, count: usize) !void {
        var response = try client.execute(ctx, "SELECT id,amount,status FROM domain_rows ORDER BY id", .read);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("domain rows: status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(count, parsed.value.rows.len);
        try std.testing.expectEqualStrings("queued", parsed.value.rows[0][2].string);
        try std.testing.expect(parsed.value.rows[1][2] == .null);
        try std.testing.expect(parsed.value.columns[1].named_type != null);
        try std.testing.expect(parsed.value.columns[2].named_type != null);
    }
};

test "mounted SQL domain rows preserve original numeric checks defaults typed NULLs and cold mutations" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(support.Reference, a, local.sql_parity_fixtures.domain_row_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const ids = [_][]const u8{ "sql-0306", "sql-0307", "sql-0308", "sql-0310" };
    var originals: usize = 0;
    for ([_][]const support.Step{ reference.value.before, reference.value.after }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(ids[originals], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        originals += 1;
    };
    try std.testing.expectEqual(ids.len, originals);
    try fixture.runWithState(DomainRowCampaign, .{ .reference = reference.value });
}
