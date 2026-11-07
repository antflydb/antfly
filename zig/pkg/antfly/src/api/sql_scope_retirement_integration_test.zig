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
        return executeInScope(ctx, null, sql);
    }
    fn executeInScope(ctx: *fixture.Context, namespace: ?[]const u8, sql: []const u8) !void {
        var response = try client.executeIn(ctx, namespace, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("scope capture setup sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        if (response.status == 202) {
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            if (parsed.value.ddl_receipt) |receipt| {
                if (receipt.restore_job_id) |job| {
                    const status_path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/restore/jobs/{s}", .{job});
                    defer ctx.alloc.free(status_path);
                    const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
                    while (@import("antfly_platform").time.monotonicNs() < deadline) {
                        try ctx.checkDrivers();
                        var reply = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, status_path, .GET, null, 5000);
                        defer reply.deinit(ctx.alloc);
                        if (reply.status != 200) std.debug.print("scope population status={d} body={s}\n", .{ reply.status, reply.body });
                        try std.testing.expectEqual(@as(u16, 200), reply.status);
                        var state = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.RestoreJob, ctx.alloc, reply.body, .{});
                        defer state.deinit();
                        try std.testing.expectEqualStrings(job, state.value.job_id);
                        try std.testing.expectEqualStrings(receipt.idempotency_key.?, state.value.idempotency_key.?);
                        if (std.mem.eql(u8, state.value.phase, "succeeded")) return;
                        if (std.mem.eql(u8, state.value.phase, "failed") or std.mem.eql(u8, state.value.phase, "cancelled")) {
                            std.debug.print("scope population terminal state={s} detail={s}\n", .{ state.value.phase, state.value.@"error" orelse "" });
                            return error.ScopePopulationFailed;
                        }
                        try ctx.io.sleep(.fromMilliseconds(20), .awake);
                    }
                    return error.ScopeSetupReceiptTimeout;
                }
                const serving = try fixture.waitServingSchemaInScope(ctx, receipt.database, receipt.namespace, receipt.table, 0);
                try std.testing.expectEqual(try std.fmt.parseInt(u64, receipt.table_id, 10), serving.table_id);
            } else if (parsed.value.catalog_receipt) |receipt| {
                const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = receipt.recovery_token }, .{});
                defer ctx.alloc.free(body);
                const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
                while (@import("antfly_platform").time.monotonicNs() < deadline) {
                    try ctx.checkDrivers();
                    var reply = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/sql/operations/status", .POST, body, 5000);
                    defer reply.deinit(ctx.alloc);
                    if (reply.status != 200) std.debug.print("scope setup receipt status={d} body={s}\n", .{ reply.status, reply.body });
                    try std.testing.expectEqual(@as(u16, 200), reply.status);
                    var observation = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLCatalogOperationReceipt, ctx.alloc, reply.body, .{});
                    defer observation.deinit();
                    try std.testing.expectEqualStrings(receipt.recovery_token, observation.value.recovery_token);
                    if (observation.value.state == .committed) return;
                    if (observation.value.state == .rejected) return error.ScopeSetupRejected;
                    try ctx.io.sleep(.fromMilliseconds(20), .awake);
                }
                return error.ScopeSetupReceiptTimeout;
            } else return error.MissingScopeSetupReceipt;
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
        try std.testing.expectEqual(@as(usize, 10), manifest.resources.len);
        var routines: usize = 0;
        for (manifest.resources) |resource| if (resource.kind == .routine) {
            routines += 1;
        };
        try std.testing.expectEqual(@as(usize, 5), routines);
        try std.testing.expectEqual(@as(usize, 14), manifest.dependencies.len);
        try std.testing.expectEqual(@as(usize, 5), manifest.stamps.len);
        try std.testing.expect(manifest.view_edges.len >= 6);
        var rewrites: usize = 0;
        var held: usize = 0;
        var retired_views: usize = 0;
        var retired_callables: usize = 0;
        for (manifest.dependencies) |dependency| switch (dependency.action) {
            .rewrite => {
                rewrites += 1;
                const name = dependency.resource.?.name;
                if (std.mem.eql(u8, name, "typed_borrower") or std.mem.eql(u8, name, "typed_borrower_child")) {
                    const header = try std.json.parseFromSliceLeaky(struct { schema_json: []const u8 }, a, dependency.table.?.preimage, .{ .ignore_unknown_fields = true });
                    const before = try std.json.parseFromSliceLeaky(std.json.Value, a, header.schema_json, .{});
                    const after = try std.json.parseFromSliceLeaky(std.json.Value, a, dependency.schema_after.?, .{});
                    try std.testing.expectEqual(before.object.get("version").?.integer + 1, after.object.get("version").?.integer);
                    const columns = try local.sql_catalog.schema_columns.properties(after);
                    try std.testing.expect(columns.object.contains("id"));
                    if (std.mem.eql(u8, name, "typed_borrower")) {
                        for ([_][]const u8{ "phase", "detail", "rendered" }) |removed| try std.testing.expect(!columns.object.contains(removed));
                        try std.testing.expect(columns.object.contains("unchanged"));
                    } else {
                        try std.testing.expect(columns.object.contains("link"));
                        if (after.object.get("foreign_keys")) |foreign| try std.testing.expectEqual(@as(usize, 0), foreign.array.items.len);
                    }
                    continue;
                }
                try std.testing.expectEqualStrings("scope_borrower", name);
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
                try std.testing.expectEqualStrings(if (dependency.kind == .table) "scope_unchanged_parent" else "scope_finish", dependency.resource.?.name);
                try std.testing.expect(dependency.schema_after == null);
            },
            .retire => {
                if (dependency.kind == .view) retired_views += 1 else if (dependency.kind == .table) {
                    try std.testing.expect(dependency.resource == null and dependency.view_owner != null and dependency.table != null);
                } else {
                    try std.testing.expect(dependency.kind == .routine or dependency.kind == .cast);
                    retired_callables += 1;
                }
            },
        };
        try std.testing.expectEqual(@as(usize, 3), rewrites);
        try std.testing.expectEqual(@as(usize, 2), held);
        try std.testing.expectEqual(@as(usize, 5), retired_views);
        try std.testing.expectEqual(@as(usize, 3), retired_callables);
        if (self.admitted) |admitted| {
            var remote = ctx.catalogClient();
            const work_bytes = try remote.forwardSystemCatalog(ctx.metadata_uri, .{ .scope_retirement_work = 0 }, .{ .remaining_ms = 10000, .forwards_remaining = 0, .campaign_allowed = false }, true);
            defer ctx.alloc.free(work_bytes);
            const work = (try std.json.parseFromSliceLeaky(?@import("../metadata/scope_retirement_ledger.zig").Work, a, work_bytes, .{})) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualDeep(admitted.result.accepted, work.head);
            const child = work.child orelse return error.TestUnexpectedResult;
            try child.validate();
            try std.testing.expectEqualDeep(admitted.query.lookup, child.lookup);
            try std.testing.expectEqual(@as(u16, 0), child.ordinal);
            // Child ordinals cover the sorted union of contained tables and
            // external materialized-view backing tables. A backing ID may sort
            // before the first directly contained table, including after reopen.
            var retiring = manifest.retiringTables();
            const first = retiring.next() orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(first.id, child.table);
            try std.testing.expectEqualSlices(u8, &first.definition, &child.definition);
            try std.testing.expectEqualSlices(u8, &first.ranges, &child.ranges);
            try std.testing.expectEqualSlices(u8, &try admitted.query.lookup.reference.childId(child.table), &child.id);
        }
        try manifest.requireSurvivorSchemas(ctx.alloc);
        // The same native recovery verifier rejects a plausible postimage
        // that also removes an unrelated surviving FK declaration.
        var weakened = manifest;
        const altered = try a.dupe(contract.Dependency, manifest.dependencies);
        weakened.dependencies = altered;
        for (altered) |*dependency| if (dependency.resource != null and std.mem.eql(u8, dependency.resource.?.name, "scope_borrower")) {
            var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, dependency.schema_after.?, .{ .parse_numbers = false });
            try std.testing.expect(schema.object.swapRemove("foreign_keys"));
            dependency.schema_after = try std.json.Stringify.valueAlloc(a, schema, .{});
        };
        try std.testing.expectError(error.InvalidScopeRetirement, weakened.requireSurvivorSchemas(ctx.alloc));
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
            for ([_][]const u8{
                "CREATE AGGREGATE scope_blocked(text)(SFUNC=tenant_ops.step,STYPE=text)",
                "CREATE OR REPLACE FUNCTION scope_finish(text) RETURNS text LANGUAGE SQL AS 'SELECT lower($1)'",
            }) |statement| {
                var blocked = try client.execute(ctx, statement, .never);
                defer blocked.deinit(ctx.alloc);
                errdefer std.debug.print("scope callable mutation SQL={s} status={d} body={s}\n", .{ statement, blocked.status, blocked.body });
                try std.testing.expect(blocked.status >= 400);
            }
            // New uses of selected type identities cannot escape the cut.
            for ([_][]const u8{
                "CREATE TABLE escaped_type(id text PRIMARY KEY,phase tenant_ops.phase)",
                "CREATE VIEW escaped_column AS SELECT phase FROM typed_borrower",
            }) |statement| {
                var rejected = try client.execute(ctx, statement, .never);
                defer rejected.deinit(ctx.alloc);
                try std.testing.expect(rejected.status >= 400);
            }
            // Holding a support function must not block new unrelated users.
            try execute(ctx, "CREATE AGGREGATE scope_unrelated(text)(SFUNC=scope_outside_step,STYPE=text,FINALFUNC=scope_finish)");
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
            try execute(ctx, "CREATE AGGREGATE scope_blocked(text)(SFUNC=tenant_ops.step,STYPE=text)");
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
        const statements = [_][]const u8{
            "SELECT id,phase::text,detail::text,unchanged,rendered FROM typed_borrower",
            "SELECT id,link FROM typed_borrower_child",
            "SELECT unchanged FROM typed_kept",
        };
        const expected_rows = [_][]const []const u8{ &.{ "typed", "Queued", "one", "preserved", "queued" }, &.{ "child", "queued" }, &.{"preserved"} };
        for (statements, expected_rows) |statement, expected_row| {
            var typed_response = try client.execute(ctx, statement, .read);
            defer typed_response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), typed_response.status);
            var result_row = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, typed_response.body, .{});
            defer result_row.deinit();
            try std.testing.expectEqual(@as(usize, 1), result_row.value.rows.len);
            try std.testing.expectEqual(expected_row.len, result_row.value.rows[0].len);
            for (result_row.value.rows[0], expected_row, result_row.value.sql_nulls.?[0]) |value, expected_value, is_null| {
                try std.testing.expect(!is_null);
                try std.testing.expectEqualStrings(expected_value, value.string);
            }
        }
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
        try execute(ctx, "CREATE FUNCTION tenant_ops.step(text,text) RETURNS text LANGUAGE SQL AS 'SELECT coalesce($1,$2)'");
        try execute(ctx, "CREATE FUNCTION tenant_ops.same(text,text) RETURNS boolean LANGUAGE SQL AS 'SELECT $1=$2'");
        try execute(ctx, "CREATE FUNCTION tenant_ops.convert(jsonb) RETURNS text LANGUAGE SQL AS $$SELECT 'native'$$");
        try execute(ctx, "CREATE FUNCTION scope_finish(text) RETURNS text LANGUAGE SQL AS 'SELECT upper($1)'");
        try execute(ctx, "CREATE FUNCTION scope_outside_step(text,text) RETURNS text LANGUAGE SQL AS 'SELECT coalesce($1,$2)'");
        try execute(ctx, "CREATE AGGREGATE scope_collect(text)(SFUNC=tenant_ops.step,STYPE=text,FINALFUNC=scope_finish)");
        try execute(ctx, "CREATE OPERATOR === (FUNCTION=tenant_ops.same,LEFTARG=text,RIGHTARG=text)");
        try execute(ctx, "CREATE CAST (jsonb AS text) WITH FUNCTION tenant_ops.convert(jsonb) AS ASSIGNMENT");
        try execute(ctx, "CREATE DOMAIN tenant_ops.phase AS TEXT CHECK (VALUE <> '')");
        try execute(ctx, "CREATE TYPE tenant_ops.detail AS ENUM ('one','two')");
        try execute(ctx, "CREATE TABLE typed_borrower(id text PRIMARY KEY,phase tenant_ops.phase,detail tenant_ops.detail,unchanged text,rendered text GENERATED ALWAYS AS (lower(phase)) STORED UNIQUE)");
        try execute(ctx, "CREATE TABLE typed_borrower_child(id text PRIMARY KEY,link text REFERENCES typed_borrower(rendered))");
        try execute(ctx, "INSERT INTO typed_borrower(id,phase,detail,unchanged) VALUES('typed','Queued','one','preserved')");
        try execute(ctx, "INSERT INTO typed_borrower_child VALUES('child','queued')");
        try execute(ctx, "CREATE VIEW typed_removed AS SELECT phase FROM typed_borrower");
        try execute(ctx, "CREATE VIEW typed_removed_child AS SELECT phase FROM typed_removed");
        try execute(ctx, "CREATE VIEW typed_kept AS SELECT unchanged FROM typed_borrower");
        try execute(ctx, "CREATE MATERIALIZED VIEW typed_materialized AS SELECT phase FROM typed_borrower WITH NO DATA");
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

const PopulationCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub const producer_plan_reads_forbidden = true;
    pub const configureAuth = Campaign.configureAuth;
    credentials: client.ApiKeyHeaders = .{},
    cancelled_jobs: [3]u64 = @splat(0),

    fn checkRows(ctx: *fixture.Context, table: []const u8, expected_status: []const u8) !void {
        return checkNamedRows(ctx, table, "id", "status", expected_status);
    }
    fn checkNamedRows(ctx: *fixture.Context, table: []const u8, id_column: []const u8, status_column: []const u8, expected_status: []const u8) !void {
        const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT {s}, {s} FROM {s} ORDER BY {s}", .{ id_column, status_column, table, id_column });
        defer ctx.alloc.free(sql);
        var response = try client.execute(ctx, sql, .read);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("population rows table={s} status={d} body={s}\n", .{ table, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 2), parsed.value.columns.len);
        try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(parsed.value.columns[0], 2950));
        try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(parsed.value.columns[1], 25));
        try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
        for (parsed.value.rows, [_][]const u8{ "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002" }) |row, id| {
            try std.testing.expectEqual(@as(usize, 2), row.len);
            try std.testing.expectEqualStrings(id, row[0].string);
            try std.testing.expectEqualStrings(expected_status, row[1].string);
        }
    }
    fn waitCancelled(self: *@This(), ctx: *fixture.Context) !void {
        for (self.cancelled_jobs) |id| if (id != 0) try waitCancelledJob(ctx, id);
    }
    fn waitCancelledJob(ctx: *fixture.Context, id: u64) !void {
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/restore/jobs/{d}", .{id});
        defer ctx.alloc.free(path);
        const deadline = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (@import("antfly_platform").time.monotonicNs() < deadline) {
            var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, path, .GET, null);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(struct { phase: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (std.mem.eql(u8, parsed.value.phase, "cancelled")) return;
            try std.testing.expect(!std.mem.eql(u8, parsed.value.phase, "succeeded"));
            try ctx.checkDrivers();
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.PopulationCancellationNotRecovered;
    }
    fn lostAdmission(self: *@This(), ctx: *fixture.Context) !void {
        const wire = @import("../metadata/query_population_job.zig");
        const jobs = @import("restore_jobs.zig");
        const stages = @import("../metadata/restore_staging.zig");
        const Fault = struct {
            alloc: std.mem.Allocator,
            next: wire.Port,
            admitted: usize = 0,
            mode: enum { lost, substituted, malformed } = .lost,
            reference: ?wire.Reference = null,
            fn execute(ptr: *anyopaque, a: std.mem.Allocator, context: local.api_operation.RequestContext, request: wire.Request) ![]u8 {
                const value: *@This() = @ptrCast(@alignCast(ptr));
                const bytes = try value.next.execute(value.next.ptr, a, context, request);
                if (request != .admit) return bytes;
                defer a.free(bytes);
                value.admitted += 1;
                const req = request.admit;
                var plan = try std.json.parseFromSlice(stages.Plan, a, req.generation_plan_json.?, .{});
                defer plan.deinit();
                value.reference = .{
                    .job_id = try jobs.jobIdForIdempotency(a, req.idempotency_namespace, req.idempotency_key.?),
                    .namespace = try value.alloc.dupe(u8, req.idempotency_namespace),
                    .key = try value.alloc.dupe(u8, req.idempotency_key.?),
                    .fingerprint = try jobs.requestFingerprintAlloc(value.alloc, req),
                    .plan_digest = try plan.value.digest(a),
                };
                // The actual replicated create has committed; lose only its
                // reply. The source must not be evaluated by recovery or retry.
                switch (value.mode) {
                    .lost => return error.MetadataMutationOutcomeUnknown,
                    .malformed => return a.dupe(u8, "{}"),
                    .substituted => {
                        var parsed = try std.json.parseFromSlice(wire.Admission, a, bytes, .{});
                        defer parsed.deinit();
                        parsed.value.plan_digest.?[0] ^= 1;
                        return std.json.Stringify.valueAlloc(a, parsed.value, .{});
                    },
                }
            }
        };
        try Campaign.execute(ctx, "CREATE SEQUENCE population_evaluations");
        var retained: std.heap.ArenaAllocator = .init(ctx.alloc);
        defer retained.deinit();
        const svc = ctx.metadata.server.svc;
        const original = svc.query_population_jobs orelse return error.AsyncRestoreUnavailable;
        var fault: Fault = .{ .alloc = retained.allocator(), .next = original };
        svc.query_population_jobs = .{ .ptr = &fault, .execute = Fault.execute };
        defer svc.query_population_jobs = original;
        for ([_][]const u8{ "population_unknown", "population_substituted", "population_malformed" }, 0..) |name, index| {
            fault.mode = @fromBackingInt(@intCast(index));
            const sql = try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE {s} AS SELECT id, nextval('population_evaluations') AS n FROM population_source", .{name});
            defer ctx.alloc.free(sql);
            var response = try client.execute(ctx, sql, .never);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("population lost admission status={d} body={s}\n", .{ response.status, response.body });
            try std.testing.expectEqual(@as(u16, 409), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(index + 1, fault.admitted);
            try std.testing.expectEqual(@as(@TypeOf(parsed.value.ddl_receipt.?.state), .admission_unknown), parsed.value.ddl_receipt.?.state);
            const reference = fault.reference orelse return error.MissingPopulationAdmission;
            try std.testing.expectEqual(reference.job_id, try std.fmt.parseInt(u64, parsed.value.ddl_receipt.?.restore_job_id.?, 10));
            self.cancelled_jobs[index] = reference.job_id;
            var probe = try client.execute(ctx, "SELECT nextval('population_evaluations')", .never);
            defer probe.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), probe.status);
            var scalar = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, probe.body, .{ .ignore_unknown_fields = true });
            defer scalar.deinit();
            try std.testing.expectEqual(@as(i64, @intCast(index + 1)), try fixture.sqlInteger(scalar.value.rows[0][0]));
            // Send cancellation through the Data API's real signed metadata port,
            // never through its unrelated local restore-job store.
            const context: local.api_operation.RequestContext = .{ .setting_admin = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s };
            var substituted = reference;
            substituted.plan_digest[0] ^= 1;
            try std.testing.expectError(error.IdempotencyConflict, ctx.data.http_server.?.source.systemCatalog(ctx.alloc, context, .{ .query_population_job = .{ .cancel = substituted } }));
            const cancelled = try ctx.data.http_server.?.source.systemCatalog(ctx.alloc, context, .{ .query_population_job = .{ .cancel = reference } });
            defer ctx.alloc.free(cancelled);
            try self.waitCancelled(ctx);
            try std.testing.expectEqual(index + 1, fault.admitted);
        }
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try Campaign.execute(ctx, "CREATE TABLE population_source(id UUID PRIMARY KEY, status TEXT)");
        try Campaign.execute(ctx, "INSERT INTO population_source VALUES('00000000-0000-0000-0000-000000000001','closed'),('00000000-0000-0000-0000-000000000002','closed')");
        try Campaign.execute(ctx, "CREATE VIEW usage_records AS SELECT id, status FROM population_source");
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for ([_][]const u8{ "sql-0364", "sql-0365", "sql-0366" }) |id| try Campaign.execute(ctx, (try corpus.get(id)).sql);
        try Campaign.execute(ctx, "CREATE SCHEMA population_into_scope");
        try Campaign.execute(ctx, "CREATE VIEW population_into_scope.usage_records AS SELECT id, status FROM public.population_source");
        try Campaign.executeInScope(ctx, "population_into_scope", (try corpus.get("sql-0567")).sql);
        try Campaign.execute(ctx, (try corpus.get("sql-0351")).sql);
        for ([_][]const u8{ "population_alias_scope", "population_projection_scope" }, [_][]const u8{ "sql-0352", "sql-0353" }) |namespace, id| {
            const create_schema = try std.fmt.allocPrint(ctx.alloc, "CREATE SCHEMA {s}", .{namespace});
            defer ctx.alloc.free(create_schema);
            try Campaign.execute(ctx, create_schema);
            const source_view = try std.fmt.allocPrint(ctx.alloc, "CREATE VIEW {s}.usage_records AS SELECT id, status FROM public.population_source", .{namespace});
            defer ctx.alloc.free(source_view);
            try Campaign.execute(ctx, source_view);
            try Campaign.executeInScope(ctx, namespace, (try corpus.get(id)).sql);
            const view_name = try std.fmt.allocPrint(ctx.alloc, "{s}.usage_records_mv", .{namespace});
            defer ctx.alloc.free(view_name);
            try checkNamedRows(ctx, view_name, "record_id", "record_status", "closed");
        }
        try checkRows(ctx, "usage_archive", "closed");
        try checkRows(ctx, "population_into_scope.usage_archive", "closed");
        try checkRows(ctx, "usage_records_mv", "closed");
        try Campaign.execute(ctx, "UPDATE population_source SET status='refreshed'");
        try Campaign.execute(ctx, (try corpus.get("sql-0356")).sql);
        var refused = try client.execute(ctx, (try corpus.get("sql-0357")).sql, .never);
        defer refused.deinit(ctx.alloc);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, refused.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("42601", diagnostic.value.code);
        try checkRows(ctx, "usage_records_mv", "refreshed");
        // Exact IF NOT EXISTS must preserve the admitted old rows, even
        // when the original source predicate now produces an empty result.
        try Campaign.execute(ctx, (try corpus.get("sql-0366")).sql);
        try checkRows(ctx, "usage_archive", "closed");
        try emptyArchive(ctx);
        try self.lostAdmission(ctx);
        try Campaign.execute(ctx, (try corpus.get("sql-0358")).sql);
        try absentMaterialized(ctx);
        try Campaign.execute(ctx, "UPDATE population_source SET status='recreated'");
        try Campaign.execute(ctx, (try corpus.get("sql-0351")).sql);
        try checkRows(ctx, "usage_records_mv", "recreated");
    }
    fn absentMaterialized(ctx: *fixture.Context) !void {
        var response = try client.execute(ctx, "SELECT id, status FROM usage_records_mv", .never);
        defer response.deinit(ctx.alloc);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("42P01", diagnostic.value.code);
    }
    fn emptyArchive(ctx: *fixture.Context) !void {
        var response = try client.execute(ctx, "SELECT count(*) FROM usage_archive_empty", .read);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 0), try fixture.sqlInteger(parsed.value.rows[0][0]));
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try checkRows(ctx, "usage_archive", "closed");
        try checkRows(ctx, "population_into_scope.usage_archive", "closed");
        try checkRows(ctx, "usage_records_mv", "recreated");
        try emptyArchive(ctx);
        try checkNamedRows(ctx, "population_alias_scope.usage_records_mv", "record_id", "record_status", "closed");
        try checkNamedRows(ctx, "population_projection_scope.usage_records_mv", "record_id", "record_status", "closed");
        try self.waitCancelled(ctx);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (0..2) |_| try Campaign.execute(ctx, (try corpus.get("sql-0359")).sql);
        try absentMaterialized(ctx);
    }
};

test "mounted SQL scope retirement population jobs preserve source evaluation admission cancellation refresh and cold publication" {
    try fixture.run(PopulationCampaign);
}

test "mounted SQL scope retirement capture drives registered physical children with lost replies and cold owners" {
    const scopes = @import("../metadata/scope_retirement_ledger.zig");
    const root = @import("../metadata/table_retirement_ledger.zig");
    const receipts = @import("../metadata/table_retirement_receipt.zig");
    const scope_wire = @import("../metadata/scope_retirement_wire.zig");
    const coordinator = @import("table_retirement_coordinator.zig");
    const PhysicalCampaign = struct {
        pub const native_auth = true;
        pub const native_sessions = true;
        pub const cold_catalog = true;
        pub const configureAuth = Campaign.configureAuth;
        credentials: client.ApiKeyHeaders = .{},
        lookup: scopes.Lookup = undefined,
        first: root.Lookup = undefined,
        fn context() local.api_operation.RequestContext {
            return .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 20 * std.time.ns_per_s };
        }
        fn source(ctx: *fixture.Context) http.StatusSource {
            return http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        }
        fn deferred(err: anyerror) bool {
            return @import("../metadata/schema_validation_coordinator.zig").isDeferred(err) or switch (err) {
                error.GenerationAdmissionPending, error.ReadIndexTimeout, error.CatalogRoutingSnapshotTimeout, error.StorageReadTemporarilyUnavailable, error.IndexBuildPending => true,
                else => false,
            };
        }
        fn pause(ctx: *fixture.Context) !void {
            const driver = http.ApiHttpServer.FkGenerationPublicationTestDriver;
            try driver.pauseBackground(ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi, ctx.io);
            if (ctx.data.http_server) |*api| try driver.pauseBackground(api, ctx.io);
        }
        fn ownerWork(ctx: *fixture.Context, a: std.mem.Allocator, lookup: root.Lookup) !root.Work {
            return std.json.parseFromSliceLeaky(root.Work, a, try source(ctx).systemCatalog(a, context(), .{ .table_retirement_owner_work = lookup }), .{});
        }
        fn start(ctx: *fixture.Context, child: scopes.Child) !void {
            const a = ctx.alloc;
            const bytes = try (root.Command{ .scope_begin = child }).encodeAlloc(a);
            defer a.free(bytes);
            const reply = try source(ctx).systemCatalog(a, context(), .{ .table_retirement_submit = bytes });
            defer a.free(reply);
            var result = try std.json.parseFromSlice(receipts.Observation, a, reply, .{});
            defer result.deinit();
            try std.testing.expect(result.value.accepted);
            try std.testing.expectEqualDeep(child, result.value.head.?.scope.parent.?);
        }
        fn awaitForeignPublication(ctx: *fixture.Context, table: u64) !u32 {
            const publication = @import("../metadata/fk_generation_publication.zig");
            const driver = http.ApiHttpServer.FkGenerationPublicationTestDriver;
            const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
            try pause(ctx);
            defer driver.resumeBackground(api);
            defer if (ctx.data.http_server) |*data_api| driver.resumeBackground(data_api);
            const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
            var last: ?publication.Phase = null;
            var retry: ?anyerror = null;
            while (@import("antfly_platform").time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                var arena: std.heap.ArenaAllocator = .init(ctx.alloc);
                defer arena.deinit();
                const a = arena.allocator();
                const bytes = try source(ctx).systemCatalog(a, context(), .{ .fk_generation_publication_status = table });
                const value = try std.json.parseFromSliceLeaky(publication.Publication, a, bytes, .{});
                if (last == null or last.? != value.phase) {
                    std.debug.print("scope cycle preparation phase={s} revision={d} fenced={d} staged={d} activated={d} acknowledged={d} installed={d}\n", .{ @tagName(value.phase), value.revision, value.child_fenced.len, value.parent_staged.len, value.parent_activated.len, value.parent_acknowledged.len, value.child_installed.len });
                    last = value.phase;
                }
                if (value.phase == .published) return @import("tables.zig").schemaVersion(value.plan.child_after.schema_json);
                if (value.phase == .canceled or value.phase == .canceling) return error.CycleConstraintCanceled;
                driver.step(api, .none) catch |err| {
                    if (!deferred(err)) return err;
                    retry = err;
                };
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
            std.debug.print("scope cycle preparation stalled phase={?} retry={?}\n", .{ last, retry });
            return error.CycleConstraintPublicationTimeout;
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activateWithCatalog(ctx, true);
            ctx.headers = ctx.admin_headers;
            try Campaign.execute(ctx, "CREATE TABLE scope_parent(id BIGINT PRIMARY KEY)");
            try Campaign.execute(ctx, "INSERT INTO scope_parent VALUES(9)");
            try Campaign.execute(ctx, "CREATE SCHEMA scope_children");
            try Campaign.execute(ctx, "CREATE TABLE scope_children.first(id BIGINT PRIMARY KEY, parent BIGINT REFERENCES public.scope_parent(id), self_id BIGINT REFERENCES scope_children.first(id), second_id BIGINT)");
            const first = try fixture.waitServingSchemaInScope(ctx, "default", "scope_children", "first", 0);
            try Campaign.execute(ctx, "INSERT INTO scope_children.first(id,parent,self_id) VALUES(1,9,1)");
            try Campaign.execute(ctx, "CREATE TABLE scope_children.second(id BIGINT PRIMARY KEY, first_id BIGINT REFERENCES scope_children.first(id), parent BIGINT REFERENCES public.scope_parent(id))");
            _ = try fixture.waitServingSchemaInScope(ctx, "default", "scope_children", "second", 0);
            try Campaign.execute(ctx, "INSERT INTO scope_children.second VALUES(2,1,9)");
            try Campaign.execute(ctx, "ALTER TABLE scope_children.first ADD CONSTRAINT first_second_fk FOREIGN KEY(second_id) REFERENCES scope_children.second(id)");
            const successor = try awaitForeignPublication(ctx, first.table_id);
            try std.testing.expectEqual(first.version + 1, successor);
            _ = try fixture.waitServingSchemaInScope(ctx, "default", "scope_children", "first", successor);
            try Campaign.execute(ctx, "UPDATE scope_children.first SET second_id=2 WHERE id=1");
            try Campaign.execute(ctx, "CREATE MATERIALIZED VIEW scope_children.display AS SELECT id FROM scope_children.first WITH NO DATA");
            try pause(ctx);
            var arena: std.heap.ArenaAllocator = .init(ctx.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const bytes = try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_prepare = .{ .kind = .namespace, .name = "scope_children", .creator = .bootstrap, .cascade = true } });
            const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, a, bytes, .{});
            const admitted = try std.json.parseFromSliceLeaky(scope_wire.Observation, a, try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_submit = .{ .admission = .{ .nonce = @splat(181), .head = manifest.catalog_revision }, .operation = .{ .reserve = manifest } } }), .{});
            self.lookup = admitted.current.?.lookup;
            try std.testing.expectEqual(@as(u16, 3), self.lookup.reference.tables);
            const drained = try std.json.parseFromSliceLeaky(scope_wire.Observation, a, try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_submit = .{ .admission = .{ .nonce = @splat(182), .head = 0 }, .operation = .{ .drain = .{ .lookup = self.lookup, .expected_revision = 1 } } } }), .{});
            try std.testing.expectEqual(.retiring_tables, drained.current.?.progress.phase);
            const work = try std.json.parseFromSliceLeaky(scopes.Work, a, try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_work = 0 }), .{});
            const child = work.child.?;
            self.first = .{ .group = child.lookup.group, .table = child.table, .id = child.id };
            try start(ctx, child);
            const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
            const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
            while (@import("antfly_platform").time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                var scratch: std.heap.ArenaAllocator = .init(ctx.alloc);
                defer scratch.deinit();
                const before = try ownerWork(ctx, scratch.allocator(), self.first);
                if (before.head.phase == .retiring and before.head.physical.?.phase == .fencing_sources) {
                    if (coordinator.TestDriver.loseOwnerReply(api, 0, context())) |_| return error.ScopeFaultNotObserved else |err| {
                        if (err == error.InjectedRetirementOwnerReplyLoss) {
                            const after = try ownerWork(ctx, scratch.allocator(), self.first);
                            try std.testing.expectEqualDeep(before.head, after.head);
                            return;
                        }
                        if (!deferred(err)) return err;
                    }
                } else _ = coordinator.step(api, 0, context()) catch |err| blk: {
                    if (!deferred(err)) return err;
                    break :blk @as(u64, 0);
                };
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
            return error.ScopePhysicalFenceTimeout;
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activateWithCatalog(ctx, true);
            ctx.headers = ctx.admin_headers;
            try pause(ctx);
            const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
            const deadline = @import("antfly_platform").time.monotonicNs() +| 90 * std.time.ns_per_s;
            var cursor: u64 = 0;
            var last: ?struct { child: u16, phase: root.Phase, barrier: ?root.physical.Phase } = null;
            var retry: ?anyerror = null;
            var checked_internal_parent = false;
            while (@import("antfly_platform").time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                var arena: std.heap.ArenaAllocator = .init(ctx.alloc);
                defer arena.deinit();
                const a = arena.allocator();
                const scope = try std.json.parseFromSliceLeaky(scopes.Work, a, try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_work = 0 }), .{});
                try std.testing.expectEqualDeep(self.lookup, scope.head.lookup);
                if (scope.head.progress.phase == .publishing_catalog) {
                    try std.testing.expectEqual(@as(u16, 3), scope.head.progress.completed_tables);
                    try std.testing.expect(checked_internal_parent);
                    var surviving = try client.execute(ctx, "SELECT id FROM public.scope_parent", .read);
                    defer surviving.deinit(ctx.alloc);
                    try std.testing.expectEqual(@as(u16, 200), surviving.status);
                    const rows = try std.json.parseFromSliceLeaky(@import("antfly_metadata_openapi").types.SQLResponse, a, surviving.body, .{});
                    try std.testing.expectEqual(@as(usize, 1), rows.rows.len);
                    try std.testing.expect(try @import("sql_parity_reference.zig").rowMatchesWithNulls(ctx.alloc, rows.columns, rows.rows[0], rows.sql_nulls.?[0], &.{.{ .integer = 9 }}, &.{false}));
                    // No child has independent catalog-publication authority.
                    // The shared survivor and ancestor cut is still retained.
                    try Campaign.execute(ctx, "CREATE TABLE public.scope_disjoint(id BIGINT PRIMARY KEY)");
                    const request: scope_wire.Request = .{ .admission = .{ .nonce = @splat(199), .head = 0 }, .operation = .{ .publish = .{ .lookup = self.lookup, .expected_revision = scope.head.progress.revision } } };
                    const published = try std.json.parseFromSliceLeaky(scope_wire.Observation, a, try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_submit = request }), .{});
                    try std.testing.expect(published.outcome.?.result == .accepted);
                    try std.testing.expectEqual(.published, published.current.?.progress.phase);
                    const replay = try std.json.parseFromSliceLeaky(scope_wire.Observation, a, try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_submit = request }), .{});
                    try std.testing.expectEqualDeep(published.outcome, replay.outcome);
                    const retired = try ownerWork(ctx, a, self.first);
                    try std.testing.expectEqual(.published, retired.head.phase);
                    var removed = try client.execute(ctx, "SELECT id FROM scope_children.first", .read);
                    defer removed.deinit(ctx.alloc);
                    try std.testing.expect(removed.status != 200);
                    // Held parents are free to evolve after the atomic cut;
                    // archived root proofs must not pin their old definitions.
                    try Campaign.execute(ctx, "ALTER TABLE public.scope_parent ADD COLUMN label TEXT");
                    const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
                    const snapshot = try store.snapshotBuilder().buildSnapshot(a, self.lookup.group);
                    var tmp = std.testing.tmpDir(.{});
                    defer tmp.cleanup();
                    const clone_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/scope-completed", .{tmp.sub_path});
                    var clone = try @import("../storage/metadata_raft_apply_client.zig").RaftApplyStore.init(ctx.alloc, .{ .root_dir = clone_path });
                    defer clone.deinit();
                    try std.testing.expect(try clone.snapshotBuilder().installSnapshot(a, self.lookup.group, 1, snapshot));
                    const recovered = try std.json.parseFromSliceLeaky(?scopes.Work, a, try clone.scopeRetirementWorkJson(a, self.lookup.group, 0), .{});
                    try std.testing.expect(recovered == null);
                    const cold = try std.json.parseFromSliceLeaky(scope_wire.Observation, a, try clone.scopeRetirementObservationJson(a, self.lookup.group, published.outcome.?.query), .{});
                    try std.testing.expectEqualDeep(published.outcome, cold.outcome);
                    try std.testing.expectEqualDeep(published.current, cold.current);
                    _ = try clone.exportHotStandbyCheckpoint(ctx.io, try std.fmt.allocPrint(a, "{s}/ready.bin", .{clone_path}));
                    return;
                }
                const child = scope.child.?;
                const lookup: root.Lookup = .{ .group = child.lookup.group, .table = child.table, .id = child.id };
                try start(ctx, child);
                const work = try ownerWork(ctx, a, lookup);
                const state: @typeInfo(@TypeOf(last)).optional.child = .{ .child = child.ordinal, .phase = work.head.phase, .barrier = if (work.head.physical) |physical| physical.phase else null };
                if (!std.meta.eql(last, @as(@TypeOf(last), state))) {
                    std.debug.print("scope physical child={d} phase={s} barrier={s}\n", .{ child.ordinal, @tagName(state.phase), if (state.barrier) |barrier| @tagName(barrier) else "unsealed" });
                    last = state;
                }
                if (!checked_internal_parent and work.head.phase == .retiring and work.head.physical.?.phase == .staging_parents) {
                    const bytes = try source(ctx).systemCatalog(a, context(), .{ .table_retirement_effects = lookup });
                    var effects = try @import("../metadata/table_retirement_manifest.zig").Owned.parse(ctx.alloc, bytes);
                    defer effects.deinit();
                    for (effects.value.retiring_parents) |retiring| {
                        const ranges = if (retiring.table == lookup.table) effects.value.root_ranges else blk: {
                            for (effects.value.external_parents) |parent| if (parent.table.table_id == retiring.table) break :blk parent.ranges;
                            return error.MissingRetiringParentCut;
                        };
                        try std.testing.expect(ranges.len != 0);
                        try std.testing.expectError(error.GenerationPublicationChanged, source(ctx).systemCatalog(a, context(), .{ .fk_generation_publication_decision = .{ .plan_id = child.id, .parent_table_id = retiring.table, .parent_group_id = ranges[0].group_id, .child_table_id = lookup.table, .child_table_name = effects.value.root.name, .action = .stage } }));
                        checked_internal_parent = true;
                    }
                }
                if (work.head.phase == .physical_ready) {
                    const reply = try source(ctx).systemCatalog(a, context(), .{ .scope_retirement_submit = .{ .admission = .{ .nonce = @splat(@as(u8, 183) + @as(u8, @intCast(child.ordinal))), .head = 0 }, .operation = .{ .child_ready = .{ .lookup = self.lookup, .expected_revision = scope.head.progress.revision, .ordinal = child.ordinal } } } });
                    const observed = try std.json.parseFromSliceLeaky(scope_wire.Observation, a, reply, .{});
                    try std.testing.expect(observed.outcome.?.result == .accepted);
                } else cursor = coordinator.step(api, cursor, context()) catch |err| blk: {
                    if (!deferred(err)) {
                        std.debug.print("scope child ordinal={d} table={d} phase={s} barrier={s} failed={s}\n", .{ child.ordinal, child.table, @tagName(work.head.phase), if (work.head.physical) |physical| @tagName(physical.phase) else "unsealed", @errorName(err) });
                        return err;
                    }
                    retry = err;
                    break :blk @as(u64, 0);
                };
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
            std.debug.print("scope physical timed out state={any} retry={?}\n", .{ last, retry });
            return error.ScopePhysicalCompletionTimeout;
        }
    };
    try fixture.run(PhysicalCampaign);
}
