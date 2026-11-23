// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;

const Campaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    sql: []const u8,
    connected_sql: []const u8,
    token: [2048]u8 = undefined,
    token_len: usize = 0,
    object_id: [20]u8 = undefined,
    object_id_len: usize = 0,

    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
        var permission = try users.Permission.initOwned(manager.alloc, .database, "default", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }

    fn expectDiagnostic(ctx: *fixture.Context, sql: []const u8, code: []const u8) !void {
        var response = try client.execute(ctx, sql, .never);
        defer response.deinit(ctx.alloc);
        const parsed = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        errdefer std.debug.print("subscription SQL diagnostic HTTP {d}: {s}\n", .{ response.status, response.body });
        try std.testing.expectEqualStrings(code, parsed.value.code);
    }

    fn verifyDisconnected(self: *const @This(), ctx: *fixture.Context) !void {
        const server = if (ctx.data.http_server) |*api| api else return error.MissingMetadataStore;
        const work: @import("../../../../system_catalog/server_call.zig").Call = .{ .sql_publication_capture_work = .{} };
        try std.testing.expect(work.requiresAdministrativeGrant() and work.requiresSettingAuthorityReadGrant() and !work.isMutation());
        if (server.source.systemCatalog(ctx.alloc, .{}, work)) |unauthorized| {
            ctx.alloc.free(unauthorized);
            return error.UnexpectedPublicationWorkerAuthority;
        } else |err| try std.testing.expectEqual(error.Forbidden, err);
        const pending = try server.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, work);
        defer ctx.alloc.free(pending);
        try std.testing.expectEqualStrings("null", pending);
        const advance: @import("../../../../system_catalog/server_call.zig").Call = .{
            .sql_publication_advance = .{
                .address = .{ .owner = .{ .publication = 555 }, .operation = @splat(1) },
                .incarnation = @splat(2),
                .expected_revision = 1,
                .action = .publish,
            },
        };
        try std.testing.expect(advance.requiresAdministrativeGrant() and advance.isMutation());
        if (server.source.systemCatalog(ctx.alloc, .{}, advance)) |unauthorized| {
            ctx.alloc.free(unauthorized);
            return error.UnexpectedPublicationWorkerAuthority;
        } else |err| try std.testing.expectEqual(error.Forbidden, err);
        const stale = try server.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, advance);
        defer ctx.alloc.free(stale);
        try std.testing.expectEqualStrings("null", stale);
        const capture_route = @import("../../../logical_replication_capture_worker.zig");
        const proof: capture_route.Capture.ObservationRequest = .{
            .scope = .{ .realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 1 }, .subject = .{ .publication = .{ .id = 2, .generation = 1 } }, .operation_id = @splat(3), .change_digest = @splat(4) },
            .logical_relation_id = 7,
            .physical_table_id = 8,
            .owner_group_id = 9,
            .namespace = .{ .table_id = 8, .shard_id = 9, .range_id = 10 },
            .schema_version = 1,
            .schema_digest = @splat(5),
            .catalog_revision = 1,
            .previous = .{},
            .next = .{ .table = .{ .count = 1, .digest = @splat(6) } },
        };
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, capture_route.Capture.Request{ .observe = proof }, .{});
        defer ctx.alloc.free(body);
        const path = "/internal/v1/groups/11/tables/docs" ++ capture_route.suffix;
        var denied = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, &.{.{ .name = capture_route.budget_header, .value = "1000" }}, ctx.base, path, .POST, body, 1000);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 401), denied.status);
        const auth = @import("../../../internal_service_auth.zig");
        const now = @import("antfly_platform").time.realtimeNs();
        const token = try auth.tokenAlloc(ctx.alloc, .{ .secret = server.cfg.internal_service_secret.?, .issuer = server.cfg.internal_service_issuer.?, .subject = "node:test" }, @intCast(@divFloor(now, std.time.ns_per_s)));
        defer ctx.alloc.free(token);
        var wrong_owner = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, &.{
            .{ .name = capture_route.budget_header, .value = "1000" }, .{ .name = auth.header_name, .value = token },
        }, ctx.base, path, .POST, body, 1000);
        defer wrong_owner.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 409), wrong_owner.status);
        const svc = ctx.metadata.server.svc;
        const store = svc.projectedStore() orelse return error.MissingMetadataStore;
        var snapshot = try store.systemCatalogSnapshot(ctx.alloc, svc.metadata_group_id);
        defer snapshot.deinit();
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var compiled = try local.sql_compiler.compile(a, self.sql, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        const creation = ddl.replication.?.create_subscription;
        const resource = snapshot.value.find(.subscription, local.system_catalog_domain.default_database_id, ddl.name.table) orelse return error.MissingSubscription;
        const record = try local.system_catalog_logical_replication_catalog.decodeSubscription(a, resource);
        try std.testing.expectEqualStrings(creation.connection, record.connection);
        try std.testing.expectEqualDeep(creation.publications, record.publications);
        try std.testing.expectEqualDeep(try local.system_catalog_logical_replication.subscriptionOptions(ddl.name.table, creation.options), record.parameters);
        try std.testing.expect(record.phase == .disconnected and record.source == null and record.operation == null and !record.desired_enabled);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.reader_headers;
        try expectDiagnostic(ctx, self.sql, "42501");
        ctx.headers = ctx.admin_headers;
        const protocol = @import("../../../../metadata/topology_protocol.zig");
        if (protocol.current_version < protocol.sql_subscription_apply_version) {
            try expectDiagnostic(ctx, self.connected_sql, "53300");
            const svc = ctx.metadata.server.svc;
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            var snapshot = try store.systemCatalogSnapshot(ctx.alloc, svc.metadata_group_id);
            defer snapshot.deinit();
            try std.testing.expect(snapshot.value.find(.subscription, local.system_catalog_domain.default_database_id, "usage_sub") == null);
        }
        var response = try client.execute(ctx, self.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("subscription SQL HTTP {d}: {s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        const result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("CREATE SUBSCRIPTION", result.value.command_tag);
        const receipt = result.value.catalog_receipt orelse return error.MissingCatalogReceipt;
        try std.testing.expectEqual(.subscription, receipt.kind);
        try std.testing.expectEqual(.committed, receipt.state);
        try std.testing.expectEqualStrings("usage_sub_options", receipt.name);
        const id = receipt.object_id orelse return error.MissingCatalogReceipt;
        try std.testing.expect(receipt.recovery_token.len <= self.token.len and id.len <= self.object_id.len);
        @memcpy(self.token[0..receipt.recovery_token.len], receipt.recovery_token);
        self.token_len = receipt.recovery_token.len;
        @memcpy(self.object_id[0..id.len], id);
        self.object_id_len = id.len;
        try self.verifyDisconnected(ctx);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = self.token[0..self.token_len] }, .{});
        defer ctx.alloc.free(body);
        var recovery = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
        defer recovery.deinit(ctx.alloc);
        errdefer std.debug.print("subscription recovery HTTP {d}: {s}\n", .{ recovery.status, recovery.body });
        try std.testing.expectEqual(@as(u16, 200), recovery.status);
        const recovered = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, recovery.body, .{});
        defer recovered.deinit();
        try std.testing.expectEqual(.subscription, recovered.value.kind);
        try std.testing.expectEqual(.committed, recovered.value.state);
        try std.testing.expectEqualStrings(self.object_id[0..self.object_id_len], recovered.value.object_id.?);
        try self.verifyDisconnected(ctx);
        ctx.headers = ctx.admin_headers;
        try expectDiagnostic(ctx, self.sql, "42710");
    }
};

test "mounted SQL disconnected subscription original preserves catalog receipt across cold restart" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    try fixture.runWithState(Campaign, .{ .sql = (try corpus.get("sql-0120")).sql, .connected_sql = (try corpus.get("sql-0119")).sql });
}

const PublicationCampaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    pub const shared_artifacts = true;
    const payload = @import("../../../../system_catalog/replication/payload.zig");
    const artifact_scope: local.storage_artifact_identity.UploadScope = .{ .domain = @splat(8), .attempt = @splat(9) };
    const artifact_body = "\"subscription snapshot evidence\"";
    retained: ?payload.Retained = null,
    statements: [5][]const u8,
    recovery_token: [8192]u8 = undefined,
    recovery_len: usize = 0,

    fn verifyRemoteSubscriptionSigner(self: *const @This(), ctx: *fixture.Context) !void {
        const coordinator = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataStore;
        try std.testing.expect(coordinator.cfg.replication_snapshot_signer == null);
        const retained = self.retained orelse return error.ArtifactUnavailable;
        var store = coordinator.cfg.document_artifact_store orelse return error.ArtifactUnavailable;
        const decoded = try payload.readJson([]const u8, ctx.alloc, &store, retained, artifact_scope, .none, 1024);
        defer decoded.deinit();
        try std.testing.expectEqualStrings("subscription snapshot evidence", decoded.value);
        const svc = ctx.metadata.server.svc;
        const wire_attestation = @import("../../../../metadata/replication_initialization_attestation.zig");
        const expected: wire_attestation.Expected = .{
            .metadata_group_id = svc.metadata_group_id,
            .metadata_incarnation = (try svc.metadataIncarnation()) orelse return error.MetadataIncarnationUnavailable,
            .source_digest = @splat(2),
            .expected_generation = 1,
            .txn_id = @splat(3),
            .request_digest = @splat(4),
            .retained_digest = retained.content_digest,
        };
        const time = @import("antfly_platform").time;
        const deadline = time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            if (coordinator.subscriptionSnapshotSigner()) |signer| {
                const proof = signer.sign(signer.ptr, ctx.alloc, expected, .{
                    .setting_admin = true,
                    .deadline_ns = @min(deadline, time.monotonicNs() +| std.time.ns_per_s),
                }) catch |err| switch (err) {
                    error.GroupLeaderUnavailable, error.DeadlineExceeded => {
                        try ctx.io.sleep(.fromMilliseconds(25), .awake);
                        continue;
                    },
                    else => return err,
                };
                try proof.verify(expected);
                try std.testing.expectEqual(@as(u64, 9), proof.identity.store_id);
                try std.testing.expectEqual(@as(u64, 9), proof.identity.node_id);
                return;
            }
            try ctx.io.sleep(.fromMilliseconds(25), .awake);
        }
        return error.SubscriptionSignerUnavailable;
    }

    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        try Campaign.configureAuth(manager);
        const users = @import("../../../../usermgr/mod.zig");
        var permission = try users.Permission.initOwned(manager.alloc, .user, "*", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }

    fn awaitPublication(ctx: *fixture.Context, token: []const u8) !void {
        const a = ctx.alloc;
        const body = try std.json.Stringify.valueAlloc(a, .{ .recovery_token = token }, .{});
        defer a.free(body);
        const coordinator = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataStore;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 25 * std.time.ns_per_s;
        while (@import("antfly_platform").time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            try coordinator.scheduleSessionMaintenance();
            var response = try fixture.requestWithTimeout(a, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
            defer response.deinit(a);
            const receipt = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, a, response.body, .{});
            defer receipt.deinit();
            try std.testing.expectEqual(.publication, receipt.value.kind);
            switch (receipt.value.state) {
                .committed => return,
                .pending => try ctx.io.sleep(.fromMilliseconds(25), .awake),
                else => {
                    std.debug.print("Publication recovery HTTP {d}: {s}\n", .{ response.status, response.body });
                    return error.UnexpectedPublicationOutcome;
                },
            }
        }
        const server = if (ctx.data.http_server) |*api| api else return error.MissingMetadataStore;
        const pending = try server.source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_publication_capture_work = .{} });
        defer a.free(pending);
        const install = @import("../../../../system_catalog/replication/installation.zig");
        const work = try std.json.parseFromSlice(?install.Work, a, pending, .{});
        defer work.deinit();
        if (work.value) |entry| {
            std.debug.print("Publication still pending owner={d} revision={d} stage={s}\n", .{ entry.address.owner.id(), entry.revision, @tagName(entry.action) });
            if (entry.action == .apply) {
                const task = entry.action.apply;
                if (server.cfg.publication_capture_driver) |driver| {
                    _ = driver.execute(driver.ptr, a, task.table, .{ .apply = task.request }, .{
                        .setting_admin = true,
                        .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s,
                    }) catch |err| {
                        std.debug.print("Publication data owner {d} refused {s}: {s}\n", .{ task.request.binding.owner_group_id, @tagName(task.request.action), @errorName(err) });
                        return err;
                    };
                }
            }
        }
        return error.PublicationActivationTimedOut;
    }

    fn ddl(ctx: *fixture.Context, sql: []const u8, tag: []const u8, retain: ?*@This()) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("Publication DDL {s}\nHTTP {d}: {s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        const parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(if (response.status == 202) "DDL PENDING" else tag, parsed.value.command_tag);
        const receipt = parsed.value.catalog_receipt orelse return error.MissingCatalogReceipt;
        try std.testing.expectEqual(.publication, receipt.kind);
        if (retain) |self| {
            try std.testing.expect(receipt.recovery_token.len <= self.recovery_token.len);
            self.recovery_len = receipt.recovery_token.len;
            @memcpy(self.recovery_token[0..self.recovery_len], receipt.recovery_token);
        }
        try awaitPublication(ctx, receipt.recovery_token);
    }

    fn expectDefinition(ctx: *fixture.Context, name: []const u8, all: bool, members: ?usize) !void {
        const installation = @import("../../../../system_catalog/replication/installation.zig");
        const server = if (ctx.data.http_server) |*api| api else return error.MissingMetadataStore;
        const bytes = try server.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_publication_capture_work = .{ .ddl = .{ .database = "default", .name = name } } });
        defer ctx.alloc.free(bytes);
        const view = try std.json.parseFromSlice(installation.DdlView, ctx.alloc, bytes, .{});
        defer view.deinit();
        if (members) |count| {
            const definition = view.value.definition orelse return error.MissingPublication;
            try std.testing.expect(definition.active);
            try std.testing.expectEqual(all, definition.all_tables);
            try std.testing.expectEqual(count, definition.tables.len);
            if (count > 0) {
                const svc = ctx.metadata.server.svc;
                const store = svc.projectedStore() orelse return error.MissingMetadataStore;
                var catalog = try store.systemCatalogSnapshot(ctx.alloc, svc.metadata_group_id);
                defer catalog.deinit();
                const namespace = catalog.value.find(.namespace, view.value.database_id, "public") orelse return error.MissingPublicationMember;
                for (([_][]const u8{ "usage_records", "usage_events" })[0..count]) |member| {
                    const table = catalog.value.find(.table, namespace.id, member) orelse return error.MissingPublicationMember;
                    for (definition.tables) |binding| {
                        if (binding.id == table.sql_relation_id and binding.namespace_id == namespace.id) break;
                    } else return error.MissingPublicationMember;
                }
            }
        } else try std.testing.expect(view.value.definition == null);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.reader_headers;
        try Campaign.expectDiagnostic(ctx, self.statements[0], "42501");
        ctx.headers = ctx.admin_headers;
        for ([_][]const u8{ "usage_records", "usage_events" }) |table| {
            const sql = try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE {s} (id bigint PRIMARY KEY, description text)", .{table});
            defer ctx.alloc.free(sql);
            var response = try client.execute(ctx, sql, .write_admission);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("Publication fixture table HTTP {d}: {s}\n", .{ response.status, response.body });
            try std.testing.expect(response.status == 200 or response.status == 202);
            _ = try fixture.waitServingSchema(ctx, table, 0);
        }
        try ddl(ctx, self.statements[0], "CREATE PUBLICATION", null);
        try expectDefinition(ctx, "usage_pub", false, 1);
        try ddl(ctx, self.statements[2], "ALTER PUBLICATION", self);
        try expectDefinition(ctx, "usage_pub", false, 2);
        try ddl(ctx, self.statements[1], "CREATE PUBLICATION", null);
        try expectDefinition(ctx, "usage_pub_all", true, 0);
        const coordinator = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataStore;
        var store = coordinator.cfg.document_artifact_store orelse return error.ArtifactUnavailable;
        self.retained = try payload.retain(ctx.alloc, &store, artifact_scope, artifact_body, .none);
        try self.verifyRemoteSubscriptionSigner(ctx);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try awaitPublication(ctx, self.recovery_token[0..self.recovery_len]);
        try self.verifyRemoteSubscriptionSigner(ctx);
        try expectDefinition(ctx, "usage_pub", false, 2);
        try expectDefinition(ctx, "usage_pub_all", true, 0);
        try ddl(ctx, self.statements[3], "DROP PUBLICATION", null);
        try expectDefinition(ctx, "usage_pub", false, null);
        try ddl(ctx, self.statements[4], "DROP PUBLICATION", null);
        try expectDefinition(ctx, "usage_pub_all", true, 0);
    }
};

test "mounted SQL publication originals activate exact memberships across cold recovery" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    var statements: [5][]const u8 = undefined;
    for ([_][]const u8{ "sql-0114", "sql-0115", "sql-0116", "sql-0117", "sql-0118" }, &statements) |id, *out| out.* = (try corpus.get(id)).sql;
    try fixture.runWithState(PublicationCampaign, .{ .statements = statements });
}

const ConnectedCampaign = struct {
    pub const native_sessions = true;
    pub const public_anonymous = true;
    pub const metadata_auth_disabled = true;
    pub const cold_catalog = true;
    pub const shared_artifacts = true;
    psql: []const u8,
    publisher_port: u16,
    originals: [6][]const u8,

    fn upstream(self: *const @This(), ctx: *fixture.Context, sql: []const u8) ![]u8 {
        const connection = try std.fmt.allocPrint(ctx.alloc, "--dbname=host=localhost dbname=usage port={d} connect_timeout=3", .{self.publisher_port});
        defer ctx.alloc.free(connection);
        const result = try std.process.run(ctx.alloc, ctx.io, .{
            .argv = &.{ self.psql, "--no-psqlrc", "--no-password", connection, "-vON_ERROR_STOP=1", "-Atc", sql },
        });
        defer ctx.alloc.free(result.stderr);
        errdefer ctx.alloc.free(result.stdout);
        if (result.term != .exited or result.term.exited != 0) {
            std.debug.print("publisher command failed: {s}\n", .{result.stderr});
            return error.PublisherCommandFailed;
        }
        return result.stdout;
    }

    fn mutatePublisher(self: *const @This(), ctx: *fixture.Context, sql: []const u8) !void {
        const stdout = try self.upstream(ctx, sql);
        ctx.alloc.free(stdout);
    }

    fn expectSlot(self: *const @This(), ctx: *fixture.Context, count: []const u8) !void {
        const stdout = try self.upstream(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name IN ('usage_sub','usage_sub_multi')");
        defer ctx.alloc.free(stdout);
        try std.testing.expectEqualStrings(count, std.mem.trim(u8, stdout, "\r\n"));
    }

    fn executeStatement(ctx: *fixture.Context, statement: []const u8, tag: []const u8) !void {
        var response = try client.execute(ctx, statement, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("subscription SQL failed HTTP {d}: {s}\n", .{ response.status, statement });
        const parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.catalog_receipt) |receipt| {
            if (receipt.state == .unknown or receipt.state == .pending) {
                try std.testing.expectEqualStrings("DDL PENDING", parsed.value.command_tag);
                return awaitReceipt(ctx, receipt);
            }
            try std.testing.expectEqual(.committed, receipt.state);
            if (receipt.kind == .subscription) try awaitReceipt(ctx, receipt);
        }
        try std.testing.expect(response.status == 200 or response.status == 202);
        try std.testing.expectEqualStrings(if (response.status == 202 or parsed.value.mutation_outcome == .committed_pending) "DDL PENDING" else tag, parsed.value.command_tag);
    }

    fn awaitReceipt(ctx: *fixture.Context, receipt: wire.SQLCatalogOperationReceipt) !void {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .recovery_token = receipt.recovery_token }, .{});
        defer ctx.alloc.free(body);
        const time = @import("antfly_platform").time;
        const deadline = time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (time.monotonicNs() < deadline) {
            try upkeep(ctx);
            var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
            defer response.deinit(ctx.alloc);
            if (response.status != 503) {
                const parsed = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                try std.testing.expectEqual(receipt.kind, parsed.value.kind);
                try std.testing.expectEqual(receipt.operation, parsed.value.operation);
                try std.testing.expectEqualStrings(receipt.name, parsed.value.name);
                switch (parsed.value.state) {
                    .committed => return,
                    .unknown, .pending => {},
                    else => {
                        std.debug.print("subscription receipt failed name={s} state={s} diagnostic={?s}\n", .{ receipt.name, @tagName(parsed.value.state), if (parsed.value.diagnostic) |diagnostic| diagnostic.code else null });
                        return error.SubscriptionCatalogOperationFailed;
                    },
                }
            }
            try ctx.io.sleep(.fromMilliseconds(40), .awake);
        }
        return error.SubscriptionReceiptTimedOut;
    }

    fn upkeep(ctx: *fixture.Context) !void {
        try ctx.checkDrivers();
        const coordinator = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataStore;
        try coordinator.scheduleSessionMaintenance();
    }

    fn awaitPhase(ctx: *fixture.Context, name: []const u8, expected: ?local.system_catalog_logical_replication_catalog.Phase) !void {
        const svc = ctx.metadata.server.svc;
        const store = svc.projectedStore() orelse return error.MissingMetadataStore;
        const time = @import("antfly_platform").time;
        const deadline = time.monotonicNs() +| 30 * std.time.ns_per_s;
        var observed: ?local.system_catalog_logical_replication_catalog.Phase = null;
        while (time.monotonicNs() < deadline) {
            try upkeep(ctx);
            var snapshot = try store.systemCatalogSnapshot(ctx.alloc, svc.metadata_group_id);
            defer snapshot.deinit();
            observed = if (snapshot.value.find(.subscription, local.system_catalog_domain.default_database_id, name)) |resource|
                (try local.system_catalog_logical_replication_catalog.decodeSubscription(snapshot.arena.allocator(), resource)).phase
            else
                null;
            if (observed == expected) return;
            try ctx.io.sleep(.fromMilliseconds(40), .awake);
        }
        std.debug.print("subscription {s} phase expected={?} observed={?}\n", .{ name, expected, observed });
        return error.SubscriptionLifecycleTimedOut;
    }

    fn rowsMatch(ctx: *fixture.Context, table: []const u8, expected: []const u8) !bool {
        const statement = try std.fmt.allocPrint(ctx.alloc, "SELECT id,payload FROM {s} ORDER BY id", .{table});
        defer ctx.alloc.free(statement);
        var response = try client.execute(ctx, statement, .never);
        defer response.deinit(ctx.alloc);
        if (response.status == 503) return false;
        errdefer std.debug.print("subscription rows HTTP {d}: {s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        const parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const actual = try std.json.Stringify.valueAlloc(ctx.alloc, parsed.value.rows, .{});
        defer ctx.alloc.free(actual);
        return std.mem.eql(u8, actual, expected);
    }

    fn awaitRows(ctx: *fixture.Context, table: []const u8, expected: []const u8) !void {
        const time = @import("antfly_platform").time;
        const deadline = time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (time.monotonicNs() < deadline) {
            try upkeep(ctx);
            if (try rowsMatch(ctx, table, expected)) return;
            try ctx.io.sleep(.fromMilliseconds(40), .awake);
        }
        std.debug.print("subscription rows timeout table={s} expected={s}\n", .{ table, expected });
        return error.SubscriptionRowsTimedOut;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = &.{};
        try self.expectSlot(ctx, "0");
        for ([_][]const u8{ "usage_records", "usage_events" }) |table| {
            const statement = try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE {s} (id integer PRIMARY KEY,payload text)", .{table});
            defer ctx.alloc.free(statement);
            try executeStatement(ctx, statement, "CREATE TABLE");
            _ = try fixture.waitServingSchema(ctx, table, 0);
        }
        try executeStatement(ctx, self.originals[0], "CREATE SUBSCRIPTION");
        try awaitPhase(ctx, "usage_sub", .enabled);
        try awaitRows(ctx, "usage_records", "[[\"1\",\"initial\"]]");
        try self.mutatePublisher(ctx, "UPDATE usage_records SET payload='streamed' WHERE id=1; INSERT INTO usage_records VALUES (2,'second')");
        try awaitRows(ctx, "usage_records", "[[\"1\",\"streamed\"],[\"2\",\"second\"]]");
        try executeStatement(ctx, self.originals[2], "ALTER SUBSCRIPTION");
        try awaitPhase(ctx, "usage_sub", .disabled);
        try self.mutatePublisher(ctx, "INSERT INTO usage_records VALUES(3,'paused')");
        try std.testing.expect(try rowsMatch(ctx, "usage_records", "[[\"1\",\"streamed\"],[\"2\",\"second\"]]"));
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = &.{};
        try awaitPhase(ctx, "usage_sub", .disabled);
        try std.testing.expect(try rowsMatch(ctx, "usage_records", "[[\"1\",\"streamed\"],[\"2\",\"second\"]]"));
        try executeStatement(ctx, self.originals[3], "ALTER SUBSCRIPTION");
        try awaitPhase(ctx, "usage_sub", .enabled);
        try awaitRows(ctx, "usage_records", "[[\"1\",\"streamed\"],[\"2\",\"second\"],[\"3\",\"paused\"]]");
        try self.mutatePublisher(ctx, "DELETE FROM usage_records WHERE id=2");
        try awaitRows(ctx, "usage_records", "[[\"1\",\"streamed\"],[\"3\",\"paused\"]]");
        try executeStatement(ctx, self.originals[4], "DROP SUBSCRIPTION");
        try awaitPhase(ctx, "usage_sub", null);
        try self.expectSlot(ctx, "0");
        try executeStatement(ctx, self.originals[5], "DROP SUBSCRIPTION");
        {
            var response = try client.execute(ctx, "DELETE FROM usage_records", .write_admission);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            const parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("DELETE", parsed.value.command_tag);
            try std.testing.expectEqual(@as(i64, 2), parsed.value.rows_affected);
        }
        try executeStatement(ctx, self.originals[1], "CREATE SUBSCRIPTION");
        try awaitPhase(ctx, "usage_sub_multi", .enabled);
        try awaitRows(ctx, "usage_records", "[[\"1\",\"streamed\"],[\"3\",\"paused\"]]");
        try awaitRows(ctx, "usage_events", "[[\"9\",\"audit\"]]");
        try executeStatement(ctx, "DROP SUBSCRIPTION usage_sub_multi", "DROP SUBSCRIPTION");
        try awaitPhase(ctx, "usage_sub_multi", null);
        try self.expectSlot(ctx, "0");
    }
};

test "mounted SQL connected subscription originals copy stream and resume after cold restart" {
    const previous_log_level = std.testing.log_level;
    std.testing.log_level = .debug;
    defer std.testing.log_level = previous_log_level;
    const protocol = @import("../../../../metadata/topology_protocol.zig");
    if (protocol.current_version < protocol.sql_subscription_apply_version) return error.SkipZigTest;
    const psql = if (std.c.getenv("ANTFLY_TEST_SUBSCRIPTION_PSQL")) |value| std.mem.span(value) else return error.SkipZigTest;
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const pg_port = if (std.c.getenv("PGPORT")) |value| std.mem.span(value) else return error.MissingPublisherPort;
    const publisher_port = try std.fmt.parseUnsigned(u16, pg_port, 10);
    var originals: [6][]const u8 = undefined;
    for ([_][]const u8{ "sql-0119", "sql-0121", "sql-0122", "sql-0123", "sql-0124", "sql-0125" }, &originals) |id, *out| out.* = (try corpus.get(id)).sql;
    try fixture.runWithState(ConnectedCampaign, .{ .psql = psql, .publisher_port = publisher_port, .originals = originals });
}
