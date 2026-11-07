// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Original trigger statements execute with PostgreSQL's default STATEMENT
//! level. An explicit ordinary function supplies their missing corpus setup.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const Reference = struct {
    cases: struct {
        @"sql-0751": Case,
        @"sql-0752": Case,
        @"sql-0753": Case,
    },
    const Case = struct { sql: []const u8, statement_level: bool, trigger_present: bool, trigger_arguments: ?u32 = null, replacement_kept_identity: bool, notices: []const []const u8, rows: []const []const i64 };
};
fn query(ctx: *fixture.Context, statement: []const u8) !std.json.Parsed(wire.SQLResponse) {
    var authorized = ctx.*;
    authorized.headers = ctx.admin_headers;
    var response = try client.execute(&authorized, statement, if (std.mem.startsWith(u8, statement, "SELECT")) .read else .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("trigger campaign statement={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        return error.TriggerPublicQueryFailed;
    }
    return std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{ .allocate = .alloc_always });
}
fn command(ctx: *fixture.Context, statement: []const u8) !void {
    return commandWithNotices(ctx, statement, null);
}
fn commandWithNotices(ctx: *fixture.Context, statement: []const u8, expected_notices: ?usize) !void {
    var result = try query(ctx, statement);
    defer result.deinit();
    if (expected_notices) |count| {
        const notices = result.value.notices orelse &.{};
        try std.testing.expectEqual(count, notices.len);
        for (notices) |notice| try std.testing.expectEqualStrings("audit fired", notice.message);
    }
    return awaitCommandResult(ctx, result.value);
}
fn awaitCommandResult(ctx: *fixture.Context, result: wire.SQLResponse) !void {
    if (result.ddl_receipt) |ddl| if (ddl.restore_job_id) |id| {
        const key = ddl.idempotency_key orelse return error.MissingTriggerJobKey;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
        var complete = false;
        while (@import("antfly_platform").time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            var response = try fixture.job(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, id);
            defer response.deinit(ctx.alloc);
            if (response.status == 404 or response.status == 503 or response.status == 504) {
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            }
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var observed = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer observed.deinit();
            try std.testing.expectEqualStrings(id, observed.value.job_id);
            try std.testing.expectEqualStrings(key, observed.value.idempotency_key orelse return error.MissingTriggerJobKey);
            if (std.mem.eql(u8, observed.value.phase, "succeeded")) {
                const summary = observed.value.result orelse return error.MissingTriggerJobResult;
                if (summary != .object) return error.InvalidTriggerJobResult;
                const status = summary.object.get("status") orelse return error.InvalidTriggerJobResult;
                if (status != .string) return error.InvalidTriggerJobResult;
                try std.testing.expectEqualStrings("completed", status.string);
                try std.testing.expectEqual(@as(i64, 1), try fixture.sqlInteger(summary.object.get("committed_table_count") orelse return error.InvalidTriggerJobResult));
                inline for (.{ "triggered_table_count", "durability_pending_table_count", "skipped_table_count", "failed_table_count" }) |field| {
                    try std.testing.expectEqual(@as(i64, 0), try fixture.sqlInteger(summary.object.get(field) orelse return error.InvalidTriggerJobResult));
                }
                complete = true;
                break;
            }
            if (std.mem.eql(u8, observed.value.phase, "failed") or std.mem.eql(u8, observed.value.phase, "cancelled")) {
                std.debug.print("trigger generation job phase={s} error={s}\n", .{ observed.value.phase, observed.value.@"error" orelse "" });
                return error.TriggerDdlRejected;
            }
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        if (!complete) return error.TriggerDdlDeadlineExceeded;
    };
    const receipt = result.catalog_receipt orelse return awaitDdlActivation(ctx, result.ddl_receipt);
    if (receipt.state == .committed) return awaitDdlActivation(ctx, result.ddl_receipt);
    if (receipt.state == .rejected) return error.TriggerDdlRejected;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, wire.SQLCatalogRecoveryRequest{ .recovery_token = receipt.recovery_token }, .{});
    defer ctx.alloc.free(body);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var status = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
        defer status.deinit(ctx.alloc);
        if (status.status == 503 or status.status == 504) continue;
        try std.testing.expectEqual(@as(u16, 200), status.status);
        var observed = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, status.body, .{});
        defer observed.deinit();
        const actual = observed.value;
        try std.testing.expectEqualStrings(receipt.recovery_token, actual.recovery_token);
        try std.testing.expectEqual(receipt.operation, actual.operation);
        try std.testing.expectEqual(receipt.kind, actual.kind);
        inline for (.{ "database", "namespace", "name" }) |field| try std.testing.expectEqualStrings(@field(receipt, field), @field(actual, field));
        if (actual.state == .committed) return awaitDdlActivation(ctx, result.ddl_receipt);
        if (actual.state == .rejected) return error.TriggerDdlRejected;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.TriggerDdlDeadlineExceeded;
}
/// Observe only the admitted immutable DDL receipt. The statement is never
/// issued again while owners publish its exact schema/policy pair.
fn awaitDdlActivation(ctx: *fixture.Context, value: ?wire.SQLDDLReceipt) !void {
    const ddl = value orelse return;
    if (ddl.restore_job_id != null or ddl.state == .ready) return;
    if (ddl.state == .invalid or ddl.state == .admission_unknown) return error.TriggerDdlRejected;
    const expected_id = try std.fmt.parseInt(u64, ddl.table_id, 10);
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/{s}/namespaces/{s}/tables/{s}/constraints/status", .{ ddl.database, ddl.namespace, ddl.table });
    defer ctx.alloc.free(path);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .GET, null, 5000);
        defer response.deinit(ctx.alloc);
        if (response.status == 503 or response.status == 504) {
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
            continue;
        }
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var observed = try std.json.parseFromSlice(wire.RelationalConstraintStatus, ctx.alloc, response.body, .{});
        defer observed.deinit();
        if (observed.value.schema_version > ddl.schema_version or observed.value.state == .invalid) return error.TriggerDdlRejected;
        if (observed.value.schema_version == ddl.schema_version and observed.value.state == .enforced) {
            // The HTTP status route is logical. Rejoin the immutable physical
            // table from the original admission before accepting completion.
            var completion_client = ctx.catalogClient();
            const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .database = ddl.database, .namespace = ddl.namespace, .name = ddl.table }}, .include_query_definitions = true };
            var cut = try completion_client.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = lookup }, 5_000, null);
            defer cut.deinit(ctx.alloc);
            var resolved = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, cut.body, .{});
            defer resolved.deinit();
            try resolved.value.validateRelations(lookup);
            const table = resolved.value.relations[0] orelse return error.TableNotFound;
            try std.testing.expectEqual(expected_id, table.table.table_id);
            return;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.TriggerDdlDeadlineExceeded;
}
fn attachments(ctx: *fixture.Context) !std.json.Parsed(local.sql_catalog.triggers.State) {
    var transport = ctx.catalogClient();
    var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = "usage_records" } }}, .include_query_definitions = true } }, 15_000, null);
    defer response.deinit(ctx.alloc);
    var resolved = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, response.body, .{});
    defer resolved.deinit();
    const table = resolved.value.sql_sources[0].?.value.table.binding;
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, table.query_definition.?.schema_json, .{});
    defer parsed.deinit();
    const value = parsed.value.object.get("sql_triggers") orelse return error.MissingTriggerState;
    const bytes = try std.json.Stringify.valueAlloc(ctx.alloc, value, .{});
    defer ctx.alloc.free(bytes);
    return std.json.parseFromSlice(local.sql_catalog.triggers.State, ctx.alloc, bytes, .{ .allocate = .alloc_always });
}
fn expectRows(ctx: *fixture.Context, expected: []const []const i64) !void {
    var rows = try query(ctx, "SELECT updated_at_ns FROM usage_records ORDER BY updated_at_ns");
    defer rows.deinit();
    try std.testing.expectEqual(expected.len, rows.value.rows.len);
    for (rows.value.rows, expected) |row, want| {
        try std.testing.expectEqual(@as(usize, 1), row.len);
        try std.testing.expectEqual(want[0], try fixture.sqlInteger(row[0]));
    }
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    pub fn beforeRestart(_: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE TABLE usage_records (id UUID PRIMARY KEY, updated_at_ns BIGINT)");
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try command(ctx, "CREATE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at_ns := 42; PERFORM TG_ARGV[0]; RAISE NOTICE 'trigger fired'; RETURN NEW; END $$");
        try command(ctx, "INSERT INTO usage_records (id, updated_at_ns) VALUES ('00000000-0000-0000-0000-000000000001', 1), ('00000000-0000-0000-0000-000000000002', 2)");
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, @embedFile("fixtures/sql_trigger_attachment_reference.json"), .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        var previous_id: ?u64 = null;
        inline for (.{ "sql-0751", "sql-0752", "sql-0753" }, 0..) |id, ordinal| {
            const original = @field(reference.value.cases, id);
            try command(ctx, original.sql);
            _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
            var state = try attachments(ctx);
            defer state.deinit();
            try std.testing.expectEqual(@as(usize, @intFromBool(original.trigger_present)), state.value.entries.len);
            if (original.trigger_present) {
                const trigger = state.value.entries[0];
                try std.testing.expectEqual(.statement, trigger.level);
                try std.testing.expectEqualStrings("update_timestamp", trigger.name);
                try std.testing.expectEqualStrings("updated_at_ns", trigger.arguments[0]);
                if (original.replacement_kept_identity) try std.testing.expectEqual(previous_id.?, trigger.id);
                previous_id = trigger.id;
            }
            // Stateless protected admission must preserve the authored error
            // after a known abort, not replace it with an unknown decision.
            var authorized = ctx.*;
            authorized.headers = ctx.admin_headers;
            var failed = try client.execute(&authorized, "UPDATE usage_records SET updated_at_ns = updated_at_ns / 0", .write_admission);
            defer failed.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 400), failed.status);
            var diagnostic = try std.json.parseFromSlice(std.json.Value, ctx.alloc, failed.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("22012", diagnostic.value.object.get("code").?.string);
            try expectRows(ctx, &.{ &.{1 + ordinal * 10}, &.{2 + ordinal * 10} });
            var updated = try query(ctx, "UPDATE usage_records SET updated_at_ns = updated_at_ns + 10");
            defer updated.deinit();
            const notices = updated.value.notices orelse &.{};
            try std.testing.expectEqual(original.notices.len, notices.len);
            for (notices, original.notices) |notice, expected| try std.testing.expectEqualStrings(expected, notice.message);
            try expectRows(ctx, original.rows);
        }
    }
    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        var state = try attachments(ctx);
        defer state.deinit();
        try std.testing.expectEqual(@as(usize, 0), state.value.entries.len);
        try expectRows(ctx, &.{ &.{31}, &.{32} });
        // Cold attachment creation runs the same real declared function. This
        // explicit ROW counterpart proves the originals were not inferred ROW.
        try command(ctx, "CREATE TRIGGER touch_row BEFORE UPDATE ON usage_records FOR EACH ROW EXECUTE FUNCTION touch_updated_at('updated_at_ns')");
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try command(ctx, "UPDATE usage_records SET updated_at_ns = updated_at_ns + 10");
        try expectRows(ctx, &.{ &.{42}, &.{42} });
    }
};
test "mounted PostgreSQL original trigger attachment replacement and retirement preserve default statement semantics across native cold restart" {
    // sql-0751 sql-0752 sql-0753: exact immutable original SQL, ordinary
    // source-faithful function, declared argument, native publication + writes,
    // independent PG18 notices/rows, preserved replacement ID and cold DROP.
    try fixture.run(Campaign);
}

const LogicalRelationCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub const configureAuth = Campaign.configureAuth;
    logical: u64 = 0,
    physical: u64 = 0,
    fn identity(ctx: *fixture.Context) !local.system_catalog_domain.ResolvedTable {
        var transport = ctx.catalogClient();
        var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = "logical_items" } }}, .include_query_definitions = true } }, 15000, null);
        defer response.deinit(ctx.alloc);
        var resolved = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, response.body, .{});
        defer resolved.deinit();
        return resolved.value.sql_sources[0].?.value.table.binding.clone(ctx.alloc);
    }
    fn expectId(ctx: *fixture.Context, id: u64) !void {
        var rows = try query(ctx, "SELECT observed_relation FROM logical_items ORDER BY id");
        defer rows.deinit();
        try std.testing.expectEqual(@as(usize, 1), rows.value.rows.len);
        try std.testing.expectEqual(@as(i64, @intCast(id)), try fixture.sqlInteger(rows.value.rows[0][0]));
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE TABLE logical_items(id BIGINT PRIMARY KEY,observed_relation BIGINT)");
        _ = try fixture.waitServingSchema(ctx, "logical_items", 0);
        try command(ctx, "CREATE FUNCTION stamp_relation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.observed_relation := TG_RELID; RETURN NEW; END $$");
        try command(ctx, "CREATE TRIGGER stamp_relation BEFORE INSERT ON logical_items FOR EACH ROW EXECUTE FUNCTION stamp_relation()");
        _ = try fixture.waitServingSchema(ctx, "logical_items", 0);
        var before = try identity(ctx);
        defer before.deinit(ctx.alloc);
        self.logical = before.effectiveLogicalRelationId();
        self.physical = before.table_id;
        try command(ctx, "INSERT INTO logical_items(id,observed_relation) VALUES(1,0)");
        try expectId(ctx, self.logical);
        // This frontend is a data API, not the metadata's durable job owner.
        // The public DDL must route the genuine empty-generation job remotely.
        try std.testing.expect(ctx.data.http_server.?.restore_job_store.replicated == null);
        try command(ctx, "TRUNCATE TABLE logical_items");
        _ = try fixture.waitServingSchema(ctx, "logical_items", 0);
        var after = try identity(ctx);
        defer after.deinit(ctx.alloc);
        try std.testing.expect(after.table_id != self.physical);
        try std.testing.expectEqual(self.logical, after.effectiveLogicalRelationId());
        self.physical = after.table_id;
        try command(ctx, "INSERT INTO logical_items(id,observed_relation) VALUES(2,0)");
        try expectId(ctx, self.logical);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        var current = try identity(ctx);
        defer current.deinit(ctx.alloc);
        try std.testing.expectEqual(self.physical, current.table_id);
        try std.testing.expectEqual(self.logical, current.effectiveLogicalRelationId());
        try expectId(ctx, self.logical);
        try command(ctx, "DROP TABLE logical_items");
        try command(ctx, "CREATE TABLE logical_items(id BIGINT PRIMARY KEY,observed_relation BIGINT)");
        _ = try fixture.waitServingSchema(ctx, "logical_items", 0);
        try command(ctx, "CREATE TRIGGER stamp_relation BEFORE INSERT ON logical_items FOR EACH ROW EXECUTE FUNCTION stamp_relation()");
        _ = try fixture.waitServingSchema(ctx, "logical_items", 0);
        var fresh = try identity(ctx);
        defer fresh.deinit(ctx.alloc);
        try std.testing.expect(fresh.effectiveLogicalRelationId() != self.logical);
        try command(ctx, "INSERT INTO logical_items(id,observed_relation) VALUES(3,0)");
        try expectId(ctx, fresh.effectiveLogicalRelationId());
    }
};
test "mounted trigger logical relation identity survives TRUNCATE cold restart and fresh same-name CREATE gets a new identity" {
    try fixture.run(LogicalRelationCampaign);
}

const OrdinaryTriggerCampaign = struct {
    truncate_job: ?[]const u8 = null,
    truncate_result: ?[]const u8 = null,
    fn tableIdentity(ctx: *fixture.Context) !u64 {
        var transport = ctx.catalogClient();
        var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = "truncate_notices" } }} } }, 15000, null);
        defer response.deinit(ctx.alloc);
        var resolved = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, response.body, .{});
        defer resolved.deinit();
        return resolved.value.sql_sources[0].?.value.table.binding.table_id;
    }
    fn completedNotices(ctx: *fixture.Context, id: []const u8) ![]u8 {
        var response = try fixture.job(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, id);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var job = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer job.deinit();
        try std.testing.expectEqualStrings("succeeded", job.value.phase);
        const summary = job.value.result orelse return error.MissingTriggerJobResult;
        const prepared = summary.object.get("truncate_preparation") orelse return error.MissingTriggerJobResult;
        const notices = prepared.object.get("notices") orelse return error.MissingTriggerJobResult;
        try std.testing.expectEqual(@as(usize, 2), notices.array.items.len);
        for (notices.array.items) |notice| try std.testing.expectEqualStrings("audit fired", notice.object.get("message").?.string);
        return std.json.Stringify.valueAlloc(ctx.alloc, prepared, .{});
    }
    fn truncatePhases(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE TABLE truncate_notices(id BIGINT PRIMARY KEY)");
        _ = try fixture.waitServingSchema(ctx, "truncate_notices", 0);
        try command(ctx, "INSERT INTO truncate_notices VALUES(1)");
        try command(ctx, "CREATE FUNCTION truncate_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'audit fired'; RETURN NEW; END $$");
        try command(ctx, "CREATE FUNCTION truncate_after_error() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'must stay private'; PERFORM 1/TG_NARGS; RETURN NEW; END $$");
        try command(ctx, "CREATE TRIGGER before_truncate BEFORE TRUNCATE ON truncate_notices EXECUTE FUNCTION truncate_before()");
        try command(ctx, "CREATE TRIGGER after_truncate AFTER TRUNCATE ON truncate_notices EXECUTE FUNCTION truncate_after_error()");
        _ = try fixture.waitServingSchema(ctx, "truncate_notices", 0);
        const before = try tableIdentity(ctx);
        var authorized = ctx.*;
        authorized.headers = ctx.admin_headers;
        var failed = try client.execute(&authorized, "TRUNCATE truncate_notices", .write_admission);
        defer failed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), failed.status);
        var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, failed.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("22012", diagnostic.value.code);
        if (diagnostic.value.notices) |notices| try std.testing.expectEqual(@as(usize, 0), notices.len);
        try std.testing.expectEqual(before, try tableIdentity(ctx));
        var rows = try query(ctx, "SELECT count(*) FROM truncate_notices");
        defer rows.deinit();
        try std.testing.expectEqual(@as(i64, 1), try fixture.sqlInteger(rows.value.rows[0][0]));
        try command(ctx, "CREATE OR REPLACE FUNCTION truncate_after_error() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'audit fired'; RETURN NEW; END $$");
        _ = try fixture.waitServingSchema(ctx, "truncate_notices", 0);
        var accepted = try query(ctx, "TRUNCATE truncate_notices");
        defer accepted.deinit();
        const accepted_notices = accepted.value.notices orelse return error.MissingTriggerNotices;
        try std.testing.expectEqual(@as(usize, 1), accepted_notices.len);
        try std.testing.expectEqualStrings("audit fired", accepted_notices[0].message);
        self.truncate_job = try ctx.alloc.dupe(u8, accepted.value.ddl_receipt.?.restore_job_id.?);
        try awaitCommandResult(ctx, accepted.value);
        self.truncate_result = try completedNotices(ctx, self.truncate_job.?);
        const repeated = try completedNotices(ctx, self.truncate_job.?);
        defer ctx.alloc.free(repeated);
        try std.testing.expectEqualStrings(self.truncate_result.?, repeated);
        try std.testing.expect(before != try tableIdentity(ctx));
    }

    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        try Campaign.configureAuth(manager);
    }
    fn count(ctx: *fixture.Context, expected: i64) !void {
        var rows = try query(ctx, "SELECT count(*) FROM usage_records");
        defer rows.deinit();
        try std.testing.expectEqual(@as(usize, 1), rows.value.rows.len);
        try std.testing.expectEqual(expected, try fixture.sqlInteger(rows.value.rows[0][0]));
    }
    fn status(ctx: *fixture.Context, id: []const u8, expected: []const u8) !void {
        const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT status FROM usage_records WHERE id='{s}'", .{id});
        defer ctx.alloc.free(sql);
        var rows = try query(ctx, sql);
        defer rows.deinit();
        try std.testing.expectEqual(@as(usize, 1), rows.value.rows.len);
        try std.testing.expectEqualStrings(expected, rows.value.rows[0][0].string);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.truncatePhases(ctx);
        try command(ctx, "CREATE TABLE usage_records(id UUID PRIMARY KEY,status TEXT)");
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try command(ctx, "CREATE FUNCTION audit_usage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'audit fired'; RETURN NEW; END $$");
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for ([_][]const u8{ "sql-1150", "sql-1153", "sql-1154", "sql-1155", "sql-1157" }, 0..) |id, index| {
            try command(ctx, "DELETE FROM usage_records");
            try command(ctx, "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000001','old')");
            // The trigger VM supports straight-line procedural steps. Use a
            // proper common callable for the no-argument declarations, then
            // replace its body for the actual argument-bearing declaration;
            // procedural IF is outside the currently supported routine subset.
            if (index == 4) try command(ctx, "CREATE OR REPLACE FUNCTION audit_usage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.status := TG_ARGV[0]; RAISE NOTICE 'audit fired'; RETURN NEW; END $$");
            // Exact immutable original declaration, with the genuine table and
            // callable it needs. An absent dependency is not an implementation.
            try command(ctx, (try corpus.get(id)).sql);
            var state = try attachments(ctx);
            defer state.deinit();
            try std.testing.expectEqual(@as(usize, 1), state.value.entries.len);
            const entry = state.value.entries[0];
            try std.testing.expectEqual(index == 0, entry.timing == .after);
            try std.testing.expectEqual(index != 1, entry.level == .row);
            try std.testing.expectEqual(@as(usize, @intFromBool(index == 4)), entry.arguments.len);
            if (index == 4) try std.testing.expectEqualStrings("audit-context", entry.arguments[0]);
            switch (index) {
                0, 4 => try commandWithNotices(ctx, "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000002','inserted')", 1),
                1 => try commandWithNotices(ctx, "TRUNCATE usage_records", 1),
                2 => {
                    try commandWithNotices(ctx, "UPDATE usage_records SET id=id", 0);
                    try commandWithNotices(ctx, "UPDATE usage_records SET status='new'", 1);
                },
                3 => {
                    try commandWithNotices(ctx, "UPDATE usage_records SET status=status", 0);
                    try commandWithNotices(ctx, "UPDATE usage_records SET status='new'", 1);
                },
                else => unreachable,
            }
            try count(ctx, if (index == 1) 0 else if (index == 0 or index == 4) 2 else 1);
            if (index == 4) try status(ctx, "00000000-0000-0000-0000-000000000002", "audit-context");
            if (index != 4) {
                const drop = try std.fmt.allocPrint(ctx.alloc, "DROP TRIGGER {s} ON usage_records", .{entry.name});
                defer ctx.alloc.free(drop);
                try command(ctx, drop);
                var absent = try attachments(ctx);
                defer absent.deinit();
                try std.testing.expectEqual(@as(usize, 0), absent.value.entries.len);
            }
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        defer ctx.alloc.free(self.truncate_job.?);
        defer ctx.alloc.free(self.truncate_result.?);
        const cold = try completedNotices(ctx, self.truncate_job.?);
        defer ctx.alloc.free(cold);
        try std.testing.expectEqualStrings(self.truncate_result.?, cold);
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        var state = try attachments(ctx);
        defer state.deinit();
        try std.testing.expectEqual(@as(usize, 1), state.value.entries.len);
        try std.testing.expectEqualStrings("usage_args", state.value.entries[0].name);
        try std.testing.expectEqualStrings("audit-context", state.value.entries[0].arguments[0]);
        try commandWithNotices(ctx, "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000003','cold')", 1);
        try status(ctx, "00000000-0000-0000-0000-000000000003", "audit-context");
        try command(ctx, "DROP TRIGGER usage_args ON usage_records");
        try commandWithNotices(ctx, "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000004','dropped')", 0);
        try status(ctx, "00000000-0000-0000-0000-000000000004", "dropped");
        try count(ctx, 4);
    }
};

test "mounted PostgreSQL original ordinary trigger supersessions preserve events predicates arguments and cold activation" {
    // sql-1150 sql-1153 sql-1154 sql-1155 sql-1157: old corpus rejections are
    // superseded by genuine supported trigger execution, not missing setup.
    try fixture.run(OrdinaryTriggerCampaign);
}
