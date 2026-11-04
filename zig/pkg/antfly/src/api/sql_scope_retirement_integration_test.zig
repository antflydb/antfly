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

//! Scope capture through the production metadata service and storage-owner ABI.
//! Publication remains guarded until the shared retirement coordinator owns it.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const contract = local.system_catalog_domain.scope_retirement;
const http = @import("http_server.zig");
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    identity: ?contract.Digest = null,
    table_id: u64 = 0,
    retained_alloc: std.mem.Allocator,
    reservation: ?[]u8 = null,
    admitted: ?@import("../metadata/scope_retirement_wire.zig").Outcome = null,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn execute(ctx: *fixture.Context, sql: []const u8) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("scope capture setup sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        if (response.status == 202) {
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            const receipt = parsed.value.ddl_receipt orelse return error.MissingScopeSetupReceipt;
            const serving = try fixture.waitServingSchemaInScope(ctx, receipt.database, receipt.namespace, receipt.table, 0);
            try std.testing.expectEqual(try std.fmt.parseInt(u64, receipt.table_id, 10), serving.table_id);
        }
    }
    fn observe(self: *@This(), ctx: *fixture.Context) !void {
        var arena: std.heap.ArenaAllocator = .init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const call: @import("../system_catalog/server_call.zig").Call = .{ .scope_retirement_prepare = .{ .kind = .namespace, .name = "tenant_ops", .creator = .bootstrap, .cascade = true } };
        try std.testing.expectError(error.Forbidden, source.systemCatalog(a, .{}, call));
        const context: local.api_operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 10 * std.time.ns_per_s };
        const bytes = if (self.admitted) |admitted| stored: {
            try std.testing.expectError(error.TableTransitionActive, source.systemCatalog(a, context, call));
            break :stored try source.systemCatalog(a, context, .{ .scope_retirement_manifest = admitted.query.lookup });
        } else try source.systemCatalog(a, context, call);
        const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, a, bytes, .{});
        try manifest.validate(a);
        try std.testing.expectEqualStrings("tenant_ops", manifest.scope.root().name);
        try std.testing.expectEqual(@as(usize, 2), manifest.tables.len);
        try std.testing.expectEqual(@as(usize, 5), manifest.resources.len);
        var routines: usize = 0;
        for (manifest.resources) |resource| if (resource.kind == .routine) {
            routines += 1;
        };
        try std.testing.expectEqual(@as(usize, 2), routines);
        try std.testing.expectEqual(@as(usize, 4), manifest.dependencies.len);
        try std.testing.expectEqual(@as(usize, 3), manifest.stamps.len);
        try std.testing.expect(manifest.view_edges.len >= 6);
        var rewrites: usize = 0;
        var held: usize = 0;
        var retired_views: usize = 0;
        for (manifest.dependencies) |dependency| switch (dependency.action) {
            .rewrite => {
                rewrites += 1;
                try std.testing.expectEqualStrings("scope_borrower", dependency.resource.?.name);
                const header = try std.json.parseFromSliceLeaky(struct { schema_json: []const u8 }, a, dependency.table.?.preimage, .{ .ignore_unknown_fields = true });
                const before = try std.json.parseFromSliceLeaky(std.json.Value, a, header.schema_json, .{});
                const after = try std.json.parseFromSliceLeaky(std.json.Value, a, dependency.schema_after.?, .{});
                try std.testing.expectEqual(before.object.get("version").?.integer + 1, after.object.get("version").?.integer);
                try std.testing.expectEqual(@as(usize, 3), before.object.get("foreign_keys").?.array.items.len);
                try std.testing.expectEqual(@as(usize, 1), after.object.get("foreign_keys").?.array.items.len);
                try std.testing.expectEqual(@as(usize, 2), before.object.get("column_defaults").?.array.items.len);
                try std.testing.expectEqual(@as(usize, 1), after.object.get("column_defaults").?.array.items.len);
            },
            .hold => {
                held += 1;
                try std.testing.expectEqualStrings("scope_unchanged_parent", dependency.resource.?.name);
                try std.testing.expect(dependency.schema_after == null);
            },
            .retire => {
                retired_views += 1;
                try std.testing.expectEqual(@as(@TypeOf(dependency.kind), .view), dependency.kind);
            },
        };
        try std.testing.expectEqual(@as(usize, 1), rewrites);
        try std.testing.expectEqual(@as(usize, 1), held);
        try std.testing.expectEqual(@as(usize, 2), retired_views);
        const identity = try manifest.digest(a);
        if (self.identity) |expected| {
            try std.testing.expectEqual(self.table_id, manifest.tables[0].id);
            try std.testing.expectEqualDeep(expected, identity);
        } else {
            self.identity = identity;
            self.table_id = manifest.tables[0].id;
        }
    }
    fn admission(self: *@This(), ctx: *fixture.Context, cancel: bool) !void {
        const wire = @import("../metadata/scope_retirement_wire.zig");
        const Transport = struct {
            inner: @import("../raft/transport/http_common.zig").RequestExecutor,
            fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, request: @import("../raft/transport/http_common.zig").HttpRequest) !@import("../raft/transport/http_common.zig").HttpResponse {
                const self_transport: *@This() = @ptrCast(@alignCast(ptr));
                const response = try self_transport.inner.execute(alloc, request);
                if (response.status >= 400) std.debug.print("scope admission HTTP status={d} body={s}\n", .{ response.status, response.body[0..@min(response.body.len, 256)] });
                return response;
            }
        };
        var transport: Transport = .{ .inner = ctx.transport };
        var remote = ctx.catalogClient();
        remote.executor = .{ .ptr = &transport, .vtable = &.{ .execute = Transport.execute } };
        var region: std.heap.ArenaAllocator = .init(ctx.alloc);
        defer region.deinit();
        const a = region.allocator();
        const context: local.api_operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 20 * std.time.ns_per_s };
        if (!cancel) {
            const bytes = try http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc).systemCatalog(a, context, .{ .scope_retirement_prepare = .{ .kind = .namespace, .name = "tenant_ops", .creator = .bootstrap, .cascade = true } });
            const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, a, bytes, .{});
            const request: wire.Request = .{ .admission = .{ .nonce = @splat(71), .head = manifest.catalog_revision }, .operation = .{ .reserve = manifest } };
            self.reservation = try request.encodeAlloc(self.retained_alloc);
            const response = try remote.forwardSystemCatalog(ctx.metadata_uri, .{ .scope_retirement_submit = request }, .{ .remaining_ms = 20000, .forwards_remaining = 0, .campaign_allowed = false }, true);
            defer ctx.alloc.free(response);
            const result = try std.json.parseFromSliceLeaky(wire.Observation, a, response, .{});
            self.admitted = result.outcome.?;
            try std.testing.expectEqual(contract.Phase.draining_connections, self.admitted.?.result.accepted.progress.phase);
            var dependent = try client.execute(ctx, "CREATE VIEW scope_new_dependent AS SELECT id FROM tenant_ops.events", .never);
            defer dependent.deinit(ctx.alloc);
            errdefer std.debug.print("reserved scope dependent status={d} body={s}\n", .{ dependent.status, dependent.body });
            try std.testing.expect(dependent.status >= 400);
            for ([_][]const u8{
                "ALTER TABLE public.scope_borrower ADD COLUMN scope_after text",
                "ALTER TABLE public.scope_unchanged_parent ADD COLUMN scope_after text",
            }) |statement| {
                var blocked = try client.execute(ctx, statement, .never);
                defer blocked.deinit(ctx.alloc);
                errdefer std.debug.print("scope participant mutation SQL={s} status={d} body={s}\n", .{ statement, blocked.status, blocked.body });
                try std.testing.expect(blocked.status >= 400);
            }
        } else {
            const original = self.admitted.?;
            const prior = try std.json.parseFromSliceLeaky(wire.Observation, a, try http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc).systemCatalog(a, context, .{ .scope_retirement_observation = original.query }), .{});
            try std.testing.expectEqualDeep(original, prior.outcome.?);
            const request: wire.Request = .{ .admission = .{ .nonce = @splat(72), .head = 0 }, .operation = .{ .cancel = .{ .lookup = original.query.lookup, .expected_revision = 1 } } };
            const canceled_bytes = try remote.forwardSystemCatalog(ctx.metadata_uri, .{ .scope_retirement_submit = request }, .{ .remaining_ms = 20000, .forwards_remaining = 0, .campaign_allowed = false }, true);
            defer ctx.alloc.free(canceled_bytes);
            const canceled = try std.json.parseFromSliceLeaky(wire.Observation, a, canceled_bytes, .{});
            try std.testing.expectEqual(contract.Phase.canceled, canceled.current.?.progress.phase);
            const retry = try std.json.parseFromSliceLeaky(wire.Request, a, self.reservation.?, .{});
            const retried_bytes = try remote.forwardSystemCatalog(ctx.metadata_uri, .{ .scope_retirement_submit = retry }, .{ .remaining_ms = 20000, .forwards_remaining = 0, .campaign_allowed = false }, true);
            defer ctx.alloc.free(retried_bytes);
            const observed = try std.json.parseFromSliceLeaky(wire.Observation, a, retried_bytes, .{});
            try std.testing.expectEqualDeep(original, observed.outcome.?);
            try std.testing.expectEqual(contract.Phase.canceled, observed.current.?.progress.phase);
            // Identical dependency admission succeeds after native cancellation;
            // a parser/permission failure cannot satisfy the reservation test.
            try execute(ctx, "CREATE VIEW scope_new_dependent AS SELECT id FROM tenant_ops.events");
            try execute(ctx, "ALTER TABLE public.scope_borrower ADD COLUMN scope_after text");
            try execute(ctx, "ALTER TABLE public.scope_unchanged_parent ADD COLUMN scope_after text");
            try execute(ctx, "CREATE TABLE tenant_ops.resumed(id bigint PRIMARY KEY)");
        }
    }
    fn rows(ctx: *fixture.Context) !void {
        var response = try client.execute(ctx, "SELECT id,payload FROM tenant_ops.events ORDER BY id", .read);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const result = parsed.value;
        const compare = @import("sql_parity_reference.zig");
        try std.testing.expectEqual(@as(usize, 2), result.columns.len);
        try std.testing.expectEqualStrings("id", result.columns[0].name);
        try std.testing.expectEqualStrings("payload", result.columns[1].name);
        try std.testing.expect(compare.postgresColumnMatches(result.columns[0], 20));
        try std.testing.expect(compare.postgresColumnMatches(result.columns[1], 25));
        const expected = [_][]const std.json.Value{ &.{ .{ .integer = 1 }, .{ .string = "kept" } }, &.{ .{ .integer = 2 }, .{ .string = "durable" } } };
        try std.testing.expectEqual(expected.len, result.rows.len);
        const nulls = result.sql_nulls orelse return error.MissingSqlNullBitmap;
        for (expected, result.rows, nulls) |want, row, flags| try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, result.columns, row, flags, want, &.{ false, false }));
        var borrower = try client.execute(ctx, "SELECT id,event_id,other_id,parent_id,token,status FROM public.scope_borrower", .read);
        defer borrower.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), borrower.status);
        var parsed_borrower = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, borrower.body, .{});
        defer parsed_borrower.deinit();
        const borrowed = parsed_borrower.value;
        try std.testing.expectEqual(@as(usize, 1), borrowed.rows.len);
        const values = [_]std.json.Value{ .{ .integer = 7 }, .{ .integer = 1 }, .{ .integer = 1 }, .{ .integer = 9 }, .{ .integer = 100 }, .{ .string = "kept" } };
        try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, borrowed.columns, borrowed.rows[0], borrowed.sql_nulls.?[0], &values, &.{ false, false, false, false, false, false }));
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try execute(ctx, "CREATE TABLE public.scope_unchanged_parent(id bigint PRIMARY KEY)");
        try execute(ctx, "INSERT INTO public.scope_unchanged_parent VALUES (9)");
        try execute(ctx, "CREATE SCHEMA tenant_ops");
        try execute(ctx, "CREATE TABLE tenant_ops.events(id bigint PRIMARY KEY, payload text, outside_id bigint REFERENCES public.scope_unchanged_parent(id), token BIGSERIAL)");
        try execute(ctx, "INSERT INTO tenant_ops.events(id,payload,outside_id,token) VALUES (1,'kept',9,1000),(2,'durable',9,1001)");
        try execute(ctx, "CREATE TABLE tenant_ops.parent2(id bigint PRIMARY KEY)");
        try execute(ctx, "INSERT INTO tenant_ops.parent2 VALUES (1)");
        try execute(ctx, "ALTER SEQUENCE tenant_ops.events_token_seq RESTART WITH 100");
        try execute(ctx, "CREATE TABLE public.scope_borrower(LIKE tenant_ops.events INCLUDING DEFAULTS, PRIMARY KEY(id), event_id bigint REFERENCES tenant_ops.events(id), other_id bigint REFERENCES tenant_ops.parent2(id), parent_id bigint REFERENCES public.scope_unchanged_parent(id), status text DEFAULT 'kept')");
        try execute(ctx, "INSERT INTO public.scope_borrower(id,event_id,other_id,parent_id) VALUES (7,1,1,9)");
        try execute(ctx, "CREATE VIEW public.scope_v1 AS SELECT id FROM tenant_ops.events");
        try execute(ctx, "CREATE VIEW public.scope_v2 AS SELECT v.id FROM public.scope_v1 v JOIN tenant_ops.parent2 p ON v.id=p.id");
        try execute(ctx, "CREATE FUNCTION tenant_ops.audit(text) RETURNS text LANGUAGE SQL AS 'SELECT $1'");
        try execute(ctx, "CREATE FUNCTION tenant_ops.audit(bigint) RETURNS bigint LANGUAGE SQL AS 'SELECT $1'");
        try self.observe(ctx);
        try rows(ctx);
        try self.admission(ctx, false);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try self.observe(ctx);
        try rows(ctx);
        try self.admission(ctx, true);
    }
};

test "mounted SQL scope retirement capture crosses native storage owners and cold catalog cuts" {
    // Preparation alone earns no positive credit for sql-0105/0108/0109.
    var retained: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer retained.deinit();
    try fixture.runWithState(Campaign, .{ .retained_alloc = retained.allocator() });
}
