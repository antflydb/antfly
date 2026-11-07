// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Exact lifetime originals use the public connection API and real native data
//! owners. This campaign is activated only with the complete lifetime producer.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const Wire = @import("antfly_metadata_openapi").types;
const first_id = "11111111-1111-4111-8111-111111111111";
const second_id = "22222222-2222-4222-8222-222222222222";
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub const require_clean_native_shutdown = true;
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    credentials: client.ApiKeyHeaders = .{},
    connection: [32]u8 = undefined,
    fn open(ctx: *fixture.Context) ![32]u8 {
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15000);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.connection_id.len != 32) return error.InvalidSqlConnection;
        return parsed.value.connection_id[0..32].*;
    }
    fn command(self: *@This(), ctx: *fixture.Context, sql: []const u8) !void {
        var response = try client.executeConnected(ctx, null, &self.connection, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("native lifetime SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        var parsed = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.ddl_receipt) |receipt| if (receipt.restore_job_id) |id| {
            const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
            while (@import("antfly_platform").time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                var observed = try fixture.job(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, id);
                defer observed.deinit(ctx.alloc);
                try std.testing.expectEqual(@as(u16, 200), observed.status);
                var job = try std.json.parseFromSlice(Wire.RestoreJob, ctx.alloc, observed.body, .{ .ignore_unknown_fields = true });
                defer job.deinit();
                try std.testing.expectEqualStrings(id, job.value.job_id);
                try std.testing.expectEqualStrings(receipt.idempotency_key orelse return error.MissingSqlLifetimeReceiptKey, job.value.idempotency_key orelse return error.MissingSqlLifetimeJobKey);
                if (std.mem.eql(u8, job.value.phase, "succeeded")) return;
                if (std.mem.eql(u8, job.value.phase, "failed") or std.mem.eql(u8, job.value.phase, "cancelled")) {
                    std.debug.print("native lifetime job SQL={s} phase={s} error={s}\n", .{ sql, job.value.phase, job.value.@"error" orelse "" });
                    return error.NativeLifetimePublicationFailed;
                }
                try ctx.io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.NativeLifetimePublicationTimeout;
        };
    }
    fn rows(self: *@This(), ctx: *fixture.Context, sql: []const u8, expected: []const []const []const u8, oids: []const u32) !void {
        const deadline = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (true) {
            var response = try client.executeConnected(ctx, null, &self.connection, sql, .session_observation);
            defer response.deinit(ctx.alloc);
            if (response.status == 404 and @import("antfly_platform").time.monotonicNs() < deadline) {
                const failure = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
                defer failure.deinit();
                // Only observe publication of an already admitted CREATE.
                if (std.mem.eql(u8, failure.value.code, "42P01")) {
                    try ctx.io.sleep(.fromMilliseconds(5), .awake);
                    continue;
                }
            }
            if (response.status != 200) std.debug.print("native lifetime read={s} status={d} body={s}\n", .{ sql, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(oids.len, parsed.value.columns.len);
            for (parsed.value.columns, oids) |column, oid| try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(column, oid));
            try std.testing.expectEqual(expected.len, parsed.value.rows.len);
            const nulls = parsed.value.sql_nulls orelse return error.MissingSqlNullBitmap;
            try std.testing.expectEqual(expected.len, nulls.len);
            for (parsed.value.rows, nulls, expected) |row, flags, values| {
                try std.testing.expectEqual(values.len, row.len);
                try std.testing.expectEqual(values.len, flags.len);
                for (row, flags, values) |value, is_null, wanted| {
                    try std.testing.expect(!is_null);
                    try std.testing.expect(value == .string or value == .integer);
                    if (value == .string) try std.testing.expectEqualStrings(wanted, value.string) else try std.testing.expectEqual(try std.fmt.parseInt(i64, wanted, 10), value.integer);
                }
            }
            return;
        }
    }
    fn populated(self: *@This(), ctx: *fixture.Context, table: []const u8) !void {
        const query = try std.fmt.allocPrint(ctx.alloc, "SELECT id,status FROM {s} ORDER BY id", .{table});
        defer ctx.alloc.free(query);
        try self.rows(ctx, query, &.{ &.{ first_id, "closed" }, &.{ second_id, "closed" } }, &.{ 2950, 25 });
    }
    fn original(self: *@This(), ctx: *fixture.Context, corpus: *local.sql_parity_fixtures.Corpus, id: []const u8) !void {
        try self.command(ctx, (try corpus.get(id)).sql);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        self.connection = try open(ctx);
        try self.command(ctx, "CREATE TABLE usage_records(id UUID PRIMARY KEY,status TEXT,payload JSONB)");
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try self.command(ctx, "INSERT INTO usage_records(id,status) VALUES ('" ++ first_id ++ "','closed'),('" ++ second_id ++ "','closed')");
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original_owner = self.connection;
        self.connection = try open(ctx);
        // This owner has never declared a TEMP relation. Its first original
        // CTAS must publish its namespace before binding the source snapshot.
        try self.original(ctx, &corpus, "sql-0367");
        try self.populated(ctx, "usage_session_archive");
        try self.original(ctx, &corpus, "sql-0367");
        try self.populated(ctx, "usage_session_archive");
        // PostgreSQL still analyzes the query for an existing target, but
        // does not evaluate it. Neither error may alter the admitted rows.
        for ([_]struct { sql: []const u8, code: []const u8 }{
            .{ .sql = "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT id FROM missing_ctas_source", .code = "42P01" },
            .{ .sql = "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT missing_ctas_column FROM usage_records", .code = "42703" },
        }) |negative| {
            var response = try client.executeConnected(ctx, null, &self.connection, negative.sql, .write_admission);
            defer response.deinit(ctx.alloc);
            try std.testing.expect(response.status >= 400);
            var failure = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer failure.deinit();
            try std.testing.expectEqualStrings(negative.code, failure.value.code);
        }
        try self.command(ctx, "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT 1/0 AS n");
        try self.populated(ctx, "usage_session_archive");
        const fresh_close_target = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{self.connection});
        defer ctx.alloc.free(fresh_close_target);
        var fresh_closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, fresh_close_target, .DELETE, null, 30000);
        defer fresh_closed.deinit(ctx.alloc);
        try std.testing.expect(fresh_closed.status == 200 or fresh_closed.status == 204);
        self.connection = original_owner;
        // An unrelated named PREPARED keeps its own row/range custody. It
        // must not become part of this implicit TEMP statement's read set.
        try self.command(ctx, "CREATE UNLOGGED TABLE lifetime_unrelated_prepared(id INT)");
        try self.command(ctx, "CREATE TABLE lifetime_unrelated_permanent(id INT)");
        // Declarations may acknowledge before current-image initialization.
        // Observe both empty images while idle, before the explicit BEGIN;
        // only the existing bounded pure-read admission policy may retry.
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_prepared", &.{}, &.{23});
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_permanent", &.{}, &.{23});
        const temp_owner = self.connection;
        self.connection = try open(ctx);
        try self.command(ctx, "BEGIN");
        try self.command(ctx, "INSERT INTO lifetime_unrelated_prepared VALUES (19)");
        try self.command(ctx, "INSERT INTO lifetime_unrelated_permanent VALUES (19)");
        try self.command(ctx, "PREPARE TRANSACTION 'lifetime_unrelated'");
        self.connection = temp_owner;
        try self.original(ctx, &corpus, "sql-0360");
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{}, &.{ 2950, 25 });
        try self.command(ctx, "INSERT INTO usage_session_records(id,status) VALUES ('" ++ first_id ++ "','session')");
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_prepared", &.{}, &.{23});
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_permanent", &.{}, &.{23});
        // Explicit rollback keeps its own boundary, including after an empty
        // read and a write. It cannot undo the prior implicit statement.
        try self.command(ctx, "BEGIN");
        try self.command(ctx, "INSERT INTO usage_session_records(id,status) VALUES ('" ++ second_id ++ "','rollback')");
        try self.command(ctx, "ROLLBACK");
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{&.{ first_id, "session" }}, &.{ 2950, 25 });
        try self.command(ctx, "ROLLBACK PREPARED 'lifetime_unrelated'");
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_prepared", &.{}, &.{23});
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_permanent", &.{}, &.{23});
        try self.original(ctx, &corpus, "sql-0361");
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{&.{ first_id, "session" }}, &.{ 2950, 25 });
        const first_connection = self.connection;
        self.connection = try open(ctx);
        try self.original(ctx, &corpus, "sql-0360");
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{}, &.{ 2950, 25 });
        try self.command(ctx, "INSERT INTO usage_session_records(id,status) VALUES ('" ++ first_id ++ "','other-owner')");
        {
            var duplicate = try client.executeConnected(ctx, null, &self.connection, "INSERT INTO usage_session_records(id,status) VALUES ('" ++ first_id ++ "','duplicate')", .write_admission);
            defer duplicate.deinit(ctx.alloc);
            try std.testing.expect(duplicate.status >= 400);
            var failure = try std.json.parseFromSlice(struct { code: []const u8, transaction_status: []const u8 }, ctx.alloc, duplicate.body, .{ .ignore_unknown_fields = true });
            defer failure.deinit();
            try std.testing.expectEqualStrings("23505", failure.value.code);
            try std.testing.expectEqualStrings("idle", failure.value.transaction_status);
        }
        // The definite statement failure detached its own implicit session.
        // A new write and public close must not inherit that terminal ID.
        try self.command(ctx, "INSERT INTO usage_session_records(id,status) VALUES ('" ++ second_id ++ "','next-write')");
        try self.command(ctx, "DELETE FROM usage_session_records WHERE id='" ++ second_id ++ "'");
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{&.{ first_id, "other-owner" }}, &.{ 2950, 25 });
        const close_target = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{self.connection});
        defer ctx.alloc.free(close_target);
        var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, close_target, .DELETE, null, 30000);
        defer closed.deinit(ctx.alloc);
        if (closed.status != 200 and closed.status != 204)
            std.debug.print("native lifetime connection close status={d} body={s} truncated={}\n", .{ closed.status, closed.body[0..@min(closed.body.len, 1024)], closed.body.len > 1024 });
        try std.testing.expect(closed.status == 200 or closed.status == 204);
        self.connection = first_connection;
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{&.{ first_id, "session" }}, &.{ 2950, 25 });
        try self.original(ctx, &corpus, "sql-0362");
        try self.command(ctx, "INSERT INTO usage_ingest_records(id,payload) VALUES ('" ++ first_id ++ "','{\"native\":\"preserved\"}'::jsonb)");
        try self.original(ctx, &corpus, "sql-0363");
        try self.original(ctx, &corpus, "sql-0367");
        try self.populated(ctx, "usage_session_archive");
        // IF NOT EXISTS must preserve the populated private generation.
        try self.original(ctx, &corpus, "sql-0367");
        try self.populated(ctx, "usage_session_archive");
        // Both successful implicit reads must deliver their final receipt and
        // release metadata table reservations before this same-connection DDL.
        // No background cleanup wait/retry is part of the DROP contract.
        try self.command(ctx, "DROP TABLE usage_session_archive");
        try self.original(ctx, &corpus, "sql-0568");
        try self.populated(ctx, "usage_session_archive");
        try self.original(ctx, &corpus, "sql-0368");
        try self.populated(ctx, "usage_ingest_archive");
        try self.command(ctx, "DROP TABLE usage_ingest_archive");
        try self.original(ctx, &corpus, "sql-0569");
        try self.populated(ctx, "usage_ingest_archive");
        try self.command(ctx, "BEGIN");
        try self.command(ctx, "UPDATE usage_ingest_records SET payload='{\"native\":\"rolled-back\"}'::jsonb WHERE id='" ++ first_id ++ "'");
        try self.command(ctx, "SAVEPOINT native_point");
        try self.command(ctx, "UPDATE usage_session_records SET status='rolled-back' WHERE id='" ++ first_id ++ "'");
        try self.command(ctx, "ROLLBACK TO native_point");
        try self.command(ctx, "ROLLBACK");
        try self.rows(ctx, "SELECT id,status FROM usage_session_records", &.{&.{ first_id, "session" }}, &.{ 2950, 25 });
        try self.rows(ctx, "SELECT payload->>'native' AS marker FROM usage_ingest_records", &.{&.{"preserved"}}, &.{25});
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        self.connection = try open(ctx);
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_prepared", &.{}, &.{23});
        try self.rows(ctx, "SELECT id FROM lifetime_unrelated_permanent", &.{}, &.{23});
        try self.populated(ctx, "usage_ingest_archive");
        try self.rows(ctx, "SELECT payload->>'native' AS marker FROM usage_ingest_records", &.{&.{"preserved"}}, &.{25});
        var response = try client.executeConnected(ctx, null, &self.connection, "SELECT id FROM usage_session_records", .read);
        defer response.deinit(ctx.alloc);
        try std.testing.expect(response.status >= 400);
        var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("42P01", parsed.value.code);
    }
};

test "mounted SQL original native TEMP and UNLOGGED lifetimes preserve owner isolation population rollback and clean restart" {
    // sql-0360/0361/0362/0363/0367/0368/0568/0569 execute unchanged.
    try fixture.run(Campaign);
}

// This uses the same mounted native owner, metadata lock authority and real
// CREATE/constraint activation as the original lifetime campaign. In contrast
// to stateless checked atomic writes, the explicit command must pass the API
// UniqueAdmission planner before its row reaches the transaction journal.
const ExplicitNativePrimaryKey = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub const configureAuth = Campaign.configureAuth;
    sql: Campaign = .{},

    fn statementOnce(self: *@This(), ctx: *fixture.Context, statement: []const u8) !void {
        var response = try client.executeConnected(ctx, null, &self.sql.connection, statement, .never);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("native explicit PK SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }

    fn awaitRegisteredNativeOwner(ctx: *fixture.Context) !void {
        const boots = @import("../metadata/sql_relation_boots.zig");
        const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const registration = ctx.data.store_registration orelse return error.StoreRegistrationRequired;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
        var last_phase: ?boots.Phase = null;
        var registered = false;
        while (@import("antfly_platform").time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            const bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .deadline_ns = deadline }, .{ .sql_relation_boot_read = .{ .node = registration.node_id } });
            defer ctx.alloc.free(bytes);
            var parsed = try std.json.parseFromSlice(boots.Observation, ctx.alloc, bytes, .{});
            defer parsed.deinit();
            const observed = parsed.value;
            registered = observed.registration_current;
            if (observed.record) |boot| {
                try boot.validate();
                last_phase = boot.phase;
                if (boot.node_id != registration.node_id or boot.identity.node_id != registration.node_id or boot.identity.store_id != registration.store_id) return error.SqlRelationOwnerChanged;
                // The ordinary maintenance worker claims/reconciles this Boot
                // even with no tables. Observe its actual registered ready cut;
                // never initialize an owner or replay CREATE from the fixture.
                if (registered and boot.phase == .ready) if (ctx.data.kernel_owner_source) |owner| {
                    const instance = try owner.nativeExecutionInstance();
                    if (std.mem.eql(u8, &boot.instance, &instance)) return;
                };
            }
            try ctx.io.sleep(.fromMilliseconds(10), .awake);
        }
        std.debug.print("native explicit PK bootstrap pending node={} store={} phase={?} registration_current={}\n", .{ registration.node_id, registration.store_id, last_phase, registered });
        return error.NativePrimaryKeyBootstrapTimeout;
    }

    fn awaitCreation(ctx: *fixture.Context, receipt: Wire.SQLDDLReceipt) !void {
        try std.testing.expectEqualStrings("default", receipt.database);
        try std.testing.expectEqualStrings("public", receipt.namespace);
        try std.testing.expectEqualStrings("native_explicit_pk", receipt.table);
        if (receipt.state == .invalid or receipt.state == .admission_unknown) return error.NativePrimaryKeyCreationNotConfirmed;
        const expected_table = try std.fmt.parseInt(u64, receipt.table_id, 10);
        const expected_version = std.math.cast(u32, receipt.schema_version) orelse return error.InvalidSqlBackendResponse;
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/{s}/namespaces/{s}/tables/{s}/constraints/status", .{ receipt.database, receipt.namespace, receipt.table });
        defer ctx.alloc.free(path);
        const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
        var last_status: u16 = 0;
        var last_version: ?u32 = null;
        while (@import("antfly_platform").time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            const remaining_ms = (deadline -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms;
            var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .GET, null, @intCast(@max(1, @min(remaining_ms, 5_000)))) catch |err| switch (err) {
                error.Timeout, error.DeadlineExceeded => continue,
                else => return err,
            };
            defer response.deinit(ctx.alloc);
            last_status = response.status;
            if (response.status == 200) {
                var parsed = try std.json.parseFromSlice(Wire.RelationalConstraintStatus, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                const observed = parsed.value;
                last_version = observed.schema_version;
                if (observed.schema_version > expected_version) return error.SchemaVersionChanged;
                if (observed.state == .invalid) return error.NativePrimaryKeyActivationInvalid;
                if (observed.retirement) |retirement| {
                    if (retirement.failure != null) return error.NativePrimaryKeyActivationFailed;
                } else if (observed.schema_version == expected_version and observed.state == .enforced) {
                    // The status route is logical. Rejoin the immutable table
                    // ID from the admitted receipt before accepting readiness.
                    try std.testing.expectEqual(expected_table, try requireNativeDefinition(ctx));
                    return;
                }
            } else if (response.status != 503 and response.status != 504) {
                std.debug.print("native explicit PK activation status={d} body={s}\n", .{ response.status, response.body });
                return error.NativePrimaryKeyActivationFailed;
            }
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        std.debug.print("native explicit PK activation pending table={} version={} last_status={} last_version={?}\n", .{ expected_table, expected_version, last_status, last_version });
        return error.NativePrimaryKeyActivationTimeout;
    }

    fn requireNativeDefinition(ctx: *fixture.Context) !u64 {
        const domain = local.system_catalog_domain;
        const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const request: domain.ResolveMany = .{ .relations = &.{.{ .database = "default", .namespace = "public", .name = "native_explicit_pk" }}, .include_query_definitions = true };
        const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = request });
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        try parsed.value.validateRelations(request);
        const relation = parsed.value.relations[0] orelse return error.TableNotFound;
        const native = relation.table.sql_persistence orelse return error.MissingNativeRelationDescriptor;
        try std.testing.expectEqual(.unlogged, native.kind);
        const definition = relation.table.query_definition orelse return error.InvalidSqlBackendResponse;
        try std.testing.expect(std.meta.eql(definition.sql_persistence, relation.table.sql_persistence));
        try std.testing.expectEqual(relation.owner.table_id, definition.table_id);
        var schema = try local.schema_mod.parseValidatedTableSchema(ctx.alloc, definition.schema_json);
        defer schema.deinit(ctx.alloc);
        try std.testing.expectEqual(relation.owner.schema_version, schema.version);
        try std.testing.expect((schema.unique_constraints orelse return error.MissingNativePrimaryKey).value.len != 0);
        // A metadata initialized receipt alone is insufficient for control
        // routing. Require the genuinely published native runtime component.
        var snapshot = (try source.adminSnapshot()) orelse return error.IntegrityCatalogUnavailable;
        defer source.freeAdminSnapshot(&snapshot);
        const owner_group = one: {
            var found: ?u64 = null;
            for (snapshot.ranges) |range| if (range.table_id == relation.owner.table_id) {
                if (found != null) return error.TopologyChanged;
                found = range.group_id;
            };
            break :one found orelse return error.UnknownGroup;
        };
        const report_bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_relation_boot_read = .{ .node = native.owner_node_id, .report_group = owner_group } });
        defer ctx.alloc.free(report_bytes);
        var reported = try std.json.parseFromSlice(@import("../metadata/sql_relation_boots.zig").Observation, ctx.alloc, report_bytes, .{});
        defer reported.deinit();
        _ = try reported.value.requireReady();
        try std.testing.expect(reported.value.report_digest != null);
        return relation.owner.table_id;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.sql.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try awaitRegisteredNativeOwner(ctx);
        self.sql.connection = try Campaign.open(ctx);
        // Keep the original source schema and native CREATE/index activation.
        // Only pure readiness observations may wait; the row mutation below
        // executes once and must never be replayed on a definition conflict.
        {
            var response = try client.executeConnected(ctx, null, &self.sql.connection, "CREATE UNLOGGED TABLE native_explicit_pk(id BIGINT PRIMARY KEY)", .write_admission);
            defer response.deinit(ctx.alloc);
            if (response.status != 200 and response.status != 202) std.debug.print("native explicit PK CREATE status={d} body={s}\n", .{ response.status, response.body });
            try std.testing.expect(response.status == 200 or response.status == 202);
            var parsed = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            const receipt = parsed.value.ddl_receipt orelse return error.MissingNativePrimaryKeyActivationReceipt;
            // CREATE is already admitted. Observe its existing native status;
            // never replay DDL to complete a pending activation receipt.
            try awaitCreation(ctx, receipt);
        }
        _ = try requireNativeDefinition(ctx);
        try self.sql.rows(ctx, "SELECT id FROM native_explicit_pk", &.{}, &.{20});
        try self.statementOnce(ctx, "BEGIN");
        try self.statementOnce(ctx, "INSERT INTO native_explicit_pk VALUES (18)");
        try self.sql.rows(ctx, "SELECT id FROM native_explicit_pk", &.{&.{"18"}}, &.{20});
        try self.statementOnce(ctx, "ROLLBACK");
        try self.sql.rows(ctx, "SELECT id FROM native_explicit_pk", &.{}, &.{20});
        try self.statementOnce(ctx, "BEGIN");
        try self.statementOnce(ctx, "INSERT INTO native_explicit_pk VALUES (18)");
        try self.statementOnce(ctx, "COMMIT");
        try self.sql.rows(ctx, "SELECT id FROM native_explicit_pk", &.{&.{"18"}}, &.{20});
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.sql.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try awaitRegisteredNativeOwner(ctx);
        self.sql.connection = try Campaign.open(ctx);
        _ = try requireNativeDefinition(ctx);
        try self.sql.rows(ctx, "SELECT id FROM native_explicit_pk", &.{&.{"18"}}, &.{20});
    }
};

test "mounted native explicit primary key command stages unique admission after real activation and preserves rollback and cold commit" {
    try fixture.run(ExplicitNativePrimaryKey);
}
