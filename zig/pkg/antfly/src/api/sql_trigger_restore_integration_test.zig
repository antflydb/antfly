// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Real archives cross two independently created metadata realms. Function
//! bodies remain destination catalog objects, never executable backup input.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const wire = @import("antfly_metadata_openapi").types;
const Config = local.common_config.Config;
const A = std.mem.Allocator;
const SourceIdentity = struct { realm: [16]u8 = @splat(0), routine: u64 = 0, table: u64 = 0 };
fn send(ctx: *fixture.Context, path: []const u8, method: http.Method, body: ?[]const u8) !http.HttpResponse {
    return fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, method, body, 30000);
}
fn sql(ctx: *fixture.Context, statement: []const u8) !std.json.Parsed(wire.SQLResponse) {
    var authorized = ctx.*;
    authorized.headers = ctx.admin_headers;
    var response = try client.execute(&authorized, statement, if (std.mem.startsWith(u8, statement, "SELECT")) .read else .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("restore SQL statement={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    return std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{ .allocate = .alloc_always });
}
fn command(ctx: *fixture.Context, statement: []const u8) !void {
    var result = try sql(ctx, statement);
    defer result.deinit();
    const receipt = result.value.catalog_receipt orelse return;
    if (receipt.state == .committed) return;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, wire.SQLCatalogRecoveryRequest{ .recovery_token = receipt.recovery_token }, .{});
    defer ctx.alloc.free(body);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try send(ctx, "/db/v1/sql/operations/status", .POST, body);
        defer response.deinit(ctx.alloc);
        if (response.status == 503 or response.status == 504) continue;
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var status = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
        defer status.deinit();
        try std.testing.expectEqualStrings(receipt.recovery_token, status.value.recovery_token);
        if (status.value.state == .committed) return;
        if (status.value.state == .rejected) return error.RestoreCampaignDdlRejected;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.RestoreCampaignDdlDeadline;
}
fn scalar(ctx: *fixture.Context, statement: []const u8, expected: i64) !void {
    var result = try sql(ctx, statement);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), result.value.rows[0].len);
    try std.testing.expectEqual(expected, try fixture.sqlInteger(result.value.rows[0][0]));
}
fn function(ctx: *fixture.Context, name: []const u8) !struct { realm: [16]u8, id: u64, generation: u32 } {
    var transport = ctx.catalogClient();
    var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .sql_routine_snapshot = .{ .targets = &.{.{ .database = "default", .namespace = "public", .table = name }}, .kind = .function } }, 5000, null);
    defer response.deinit(ctx.alloc);
    var parsed = try std.json.parseFromSlice(@import("../system_catalog/routines.zig").Snapshot, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const found = parsed.value.found orelse return error.RestoreCampaignFunctionAbsent;
    return .{ .realm = parsed.value.incarnation, .id = found.record.id, .generation = found.record.generation };
}
fn tableState(ctx: *fixture.Context, name: []const u8) !std.json.Parsed(local.sql_catalog.triggers.State) {
    var transport = ctx.catalogClient();
    var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = name } }}, .include_query_definitions = true } }, 5000, null);
    defer response.deinit(ctx.alloc);
    var source = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, response.body, .{});
    defer source.deinit();
    const table = source.value.sql_sources[0].?.value.table.binding;
    var schema = try std.json.parseFromSlice(std.json.Value, ctx.alloc, table.query_definition.?.schema_json, .{});
    defer schema.deinit();
    return local.schema_mod.table_triggers.parseValue(ctx.alloc, schema.value.object.get("sql_triggers") orelse return error.RestoreCampaignAttachmentAbsent);
}
fn authorize(manager: *@import("../usermgr/mod.zig").UserManager) !void {
    const users = @import("../usermgr/mod.zig");
    try manager.removePermissionFromUser("pk-rewriter", "*", .table);
    inline for (.{ "trigger_source", "restored_table", "touch_source", "touch_destination" }) |name| {
        var permission = try users.Permission.initOwned(manager.alloc, .table, name, .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }
    inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
        var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }
}
fn backup(ctx: *fixture.Context, cluster: bool, location: []const u8) !void {
    const path = if (cluster) "/db/v1/backup" else "/db/v1/tables/trigger_source/backup";
    const body = if (cluster)
        try std.json.Stringify.valueAlloc(ctx.alloc, .{ .backup_id = "cohort", .location = location, .connection = "archive", .format = "native", .table_names = &.{"trigger_source"} }, .{})
    else
        try std.json.Stringify.valueAlloc(ctx.alloc, .{ .backup_id = "independent", .location = location, .connection = "archive", .format = "portable" }, .{});
    defer ctx.alloc.free(body);
    var response = try send(ctx, path, .POST, body);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("restore campaign backup status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    if (cluster) {
        var completed = try std.json.parseFromSlice(struct { status: []const u8, tables: []const struct { name: []const u8, status: []const u8 } }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer completed.deinit();
        try std.testing.expectEqualStrings("completed", completed.value.status);
        try std.testing.expectEqual(@as(usize, 1), completed.value.tables.len);
        try std.testing.expectEqualStrings("trigger_source", completed.value.tables[0].name);
        try std.testing.expectEqualStrings("completed", completed.value.tables[0].status);
    } else {
        var completed = try std.json.parseFromSlice(struct { backup: []const u8 }, ctx.alloc, response.body, .{});
        defer completed.deinit();
        try std.testing.expectEqualStrings("successful", completed.value.backup);
    }
}
fn waitJob(ctx: *fixture.Context, id: []const u8, expected: []const u8) !void {
    const deadline = @import("antfly_platform").time.monotonicNs() +| 120 * std.time.ns_per_s;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.job(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, id);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var body = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
        defer body.deinit();
        const phase = (try fixture.field(body.value, "phase")).string;
        if (std.mem.eql(u8, phase, expected)) return;
        if (std.mem.eql(u8, phase, "failed") or std.mem.eql(u8, phase, "cancelled") or std.mem.eql(u8, phase, "succeeded")) {
            std.debug.print("restore campaign unexpected terminal expected={s} body={s}\n", .{ expected, response.body });
            return error.RestoreCampaignUnexpectedTerminal;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.RestoreCampaignJobDeadline;
}
fn restoreBody(a: A, source: SourceIdentity, destination: u64, cluster: bool, location: []const u8) ![]u8 {
    const mapping = .{ .source_metadata_incarnation = &std.fmt.bytesToHex(source.realm, .lower), .source_routine_id = try std.fmt.allocPrint(a, "{d}", .{source.routine}), .destination_routine_id = try std.fmt.allocPrint(a, "{d}", .{destination}) };
    defer a.free(mapping.source_routine_id);
    defer a.free(mapping.destination_routine_id);
    if (cluster) return std.json.Stringify.valueAlloc(a, .{ .backup_id = "cohort", .location = location, .connection = "archive", .table_names = &.{"trigger_source"}, .restore_mode = "overwrite", .routine_mappings = &.{mapping} }, .{});
    return std.json.Stringify.valueAlloc(a, .{ .backup_id = "independent", .location = location, .connection = "archive", .routine_mappings = &.{mapping} }, .{});
}
fn startRestore(ctx: *fixture.Context, body: []const u8, cluster: bool, key: []const u8) ![]u8 {
    const headers = try ctx.alloc.alloc(http.RequestHeader, ctx.admin_headers.len + 1);
    defer ctx.alloc.free(headers);
    @memcpy(headers[0..ctx.admin_headers.len], ctx.admin_headers);
    headers[ctx.admin_headers.len] = .{ .name = "Idempotency-Key", .value = key };
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, headers, ctx.base, if (cluster) "/db/v1/restore" else "/db/v1/tables/restored_table/restore", .POST, body, 30000);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("restore campaign admission status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 202), response.status);
    var parsed = try std.json.parseFromSlice(struct { job_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return ctx.alloc.dupe(u8, parsed.value.job_id);
}
/// A client-side proxy forwards the complete public request, then loses the
/// successful response. The server's admission and durable job are unchanged.
const AdmissionReplyLoss = struct {
    inner: http.RequestExecutor,
    lost: bool = false,
    fn execute(ptr: *anyopaque, a: A, request: http.HttpRequest) !http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var response = try self.inner.execute(a, request);
        if (!self.lost and request.method == .POST and response.status == 202 and std.mem.endsWith(u8, request.uri, "/db/v1/tables/restored_table/restore")) {
            self.lost = true;
            response.deinit(a);
            return error.ConnectionResetByPeer;
        }
        return response;
    }
    fn executor(self: *@This()) http.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute }, .clock_io = self.inner.clock_io };
    }
};
const Producer = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    config: *const Config,
    location: []const u8,
    identity: *SourceIdentity,
    pub fn nodeConfig(self: *@This()) *const Config {
        return self.config;
    }
    pub const configureAuth = authorize;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE TABLE trigger_source (id UUID PRIMARY KEY, n BIGINT)");
        try command(ctx, "CREATE FUNCTION touch_source() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := NEW.n + 100; RAISE NOTICE 'source'; RETURN NEW; END $$");
        try command(ctx, "CREATE TRIGGER touch BEFORE UPDATE ON trigger_source FOR EACH ROW EXECUTE FUNCTION touch_source()");
        _ = try fixture.waitServingSchema(ctx, "trigger_source", 0);
        try command(ctx, "INSERT INTO trigger_source (id,n) VALUES ('00000000-0000-0000-0000-000000000001',10)");
        const routine = try function(ctx, "touch_source");
        var state = try tableState(ctx, "trigger_source");
        defer state.deinit();
        self.identity.* = .{ .realm = routine.realm, .routine = routine.id, .table = state.value.table_id };
        try backup(ctx, false, self.location);
        try backup(ctx, true, self.location);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        const current = try function(ctx, "touch_source");
        try std.testing.expectEqual(self.identity.realm, current.realm);
        try scalar(ctx, "SELECT n FROM trigger_source", 10);
    }
};
fn connected(ctx: *fixture.Context, connection: []const u8, statement: []const u8) !void {
    var authorized = ctx.*;
    authorized.headers = ctx.admin_headers;
    var response = try client.executeConnectedParameters(&authorized, connection, statement, &.{}, .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("restore lock SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
}
fn lockDestination(ctx: *fixture.Context) ![32]u8 {
    var response = try send(ctx, "/db/v1/sql/connections", .POST, "{}");
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.connection_id.len != 32) return error.RestoreCampaignConnectionIdentity;
    const id = parsed.value.connection_id[0..32].*;
    try connected(ctx, &id, "BEGIN");
    try connected(ctx, &id, "LOCK TABLE trigger_source IN ACCESS EXCLUSIVE MODE");
    return id;
}
fn nativeState(ctx: *fixture.Context, id: []const u8) !?std.json.Parsed(@import("../metadata/restore_staging.zig").Job) {
    const native_id = try @import("../metadata/restore_staging.zig").idForAttempt(try std.fmt.parseInt(u64, id, 10), 1);
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = (try source.getRestoreStaging(ctx.alloc, native_id, .{ .setting_admin = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s })) orelse return null;
    defer ctx.alloc.free(bytes);
    return try std.json.parseFromSlice(@import("../metadata/restore_staging.zig").Job, ctx.alloc, bytes, .{ .allocate = .alloc_always });
}
fn waitNative(ctx: *fixture.Context, id: []const u8, wanted: @import("../metadata/restore_staging.zig").State) !void {
    const deadline = @import("antfly_platform").time.monotonicNs() +| 120 * std.time.ns_per_s;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var observed = try nativeState(ctx, id);
        defer if (observed) |*owned| owned.deinit();
        if (observed) |job| {
            if (job.value.state == wanted) {
                if (wanted == .cutover) {
                    // SQL participants block the final metadata publication,
                    // after cancellable source fencing, not child validation.
                    var owners: usize = 0;
                    for (job.value.plan.targets) |target| if (target.replace) |prior| {
                        owners += prior.ranges.len;
                    };
                    for (job.value.plan.external_fk_parents) |parent| owners += parent.ranges.len;
                    if (job.value.completed_owners != owners) {
                        try ctx.io.sleep(.fromMilliseconds(20), .awake);
                        continue;
                    }
                }
                return;
            }
            if (job.value.state == .activating or job.value.state == .published or (job.value.state == .canceled and wanted != .canceled)) return error.RestoreIgnoredLiveSqlParticipant;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.RestoreCampaignNativeDeadline;
}
fn faultQualification(ctx: *fixture.Context, body: []const u8, destination: u64) !void {
    // A real SQL transaction prevents final publication, while permitting
    // import, validation and cancellable source fencing. No test flag is used.
    const lock = try lockDestination(ctx);
    var lock_live = true;
    defer if (lock_live) connected(ctx, &lock, "ROLLBACK") catch {};
    const id = try startRestore(ctx, body, true, "cohort-revoked-before-cutover");
    defer ctx.alloc.free(id);
    try waitNative(ctx, id, .cutover);
    const before = try function(ctx, "touch_destination");
    var authorized = ctx.*;
    authorized.headers = ctx.admin_headers;
    var replacement = try client.execute(&authorized, "CREATE OR REPLACE FUNCTION touch_destination() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := NEW.n + 999; RETURN NEW; END $$", .never);
    defer replacement.deinit(ctx.alloc);
    var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, replacement.body, .{ .ignore_unknown_fields = true });
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings("2BP01", diagnostic.value.code);
    const after = try function(ctx, "touch_destination");
    try std.testing.expectEqual(before.generation, after.generation);
    try std.testing.expectEqual(destination, after.id);
    const manager = ctx.data.api_server_cfg.user_manager orelse return error.RestoreCampaignManagerAbsent;
    try manager.removePermissionFromUser("pk-rewriter", "touch_destination", .table);
    try manager.removePermissionFromUser("pk-rewriter", "*", .namespace);
    try waitJob(ctx, id, "failed");
    try waitNative(ctx, id, .canceled);
    try connected(ctx, &lock, "ROLLBACK");
    lock_live = false;
    try scalar(ctx, "SELECT n FROM trigger_source", 99);
    const users = @import("../usermgr/mod.zig");
    inline for (.{ users.ResourceType.table, users.ResourceType.namespace }) |kind| {
        var permission = try users.Permission.initOwned(manager.alloc, kind, if (kind == .table) "touch_destination" else "*", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }
    const second_lock = try lockDestination(ctx);
    var second_live = true;
    defer if (second_live) connected(ctx, &second_lock, "ROLLBACK") catch {};
    const canceled = try startRestore(ctx, body, true, "cohort-canceled-before-cutover");
    defer ctx.alloc.free(canceled);
    try waitNative(ctx, canceled, .cutover);
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/restore/jobs/{s}", .{canceled});
    defer ctx.alloc.free(path);
    var cancellation = try send(ctx, path, .DELETE, null);
    defer cancellation.deinit(ctx.alloc);
    try std.testing.expect(cancellation.status == 200 or cancellation.status == 202);
    try waitJob(ctx, canceled, "cancelled");
    try waitNative(ctx, canceled, .canceled);
    try connected(ctx, &second_lock, "ROLLBACK");
    second_live = false;
    try scalar(ctx, "SELECT n FROM trigger_source", 99);
    // Confirmed cleanup releases the actual callable lease, not just a
    // public job flag. Replacement can now preserve its immutable routine ID.
    try command(ctx, "CREATE OR REPLACE FUNCTION touch_destination() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := NEW.n + 1000; RAISE NOTICE 'destination'; RETURN NEW; END $$");
    try std.testing.expectEqual(destination, (try function(ctx, "touch_destination")).id);
}
const Consumer = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    config: *const Config,
    location: []const u8,
    identity: *const SourceIdentity,
    destination: u64 = 0,
    pub fn nodeConfig(self: *@This()) *const Config {
        return self.config;
    }
    pub const configureAuth = authorize;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE FUNCTION touch_destination() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := NEW.n + 1000; RAISE NOTICE 'destination'; RETURN NEW; END $$");
        const destination = try function(ctx, "touch_destination");
        self.destination = destination.id;
        try std.testing.expect(!std.mem.eql(u8, &destination.realm, &self.identity.realm));
        const body = try restoreBody(ctx.alloc, self.identity.*, destination.id, false, self.location);
        defer ctx.alloc.free(body);
        var loss: AdmissionReplyLoss = .{ .inner = ctx.transport };
        var uncertain = ctx.*;
        uncertain.transport = loss.executor();
        try std.testing.expectError(error.ConnectionResetByPeer, startRestore(&uncertain, body, false, "independent-cross-realm"));
        try std.testing.expect(loss.lost);
        // The caller did not receive the first job ID. An identical public
        // retry must recover the existing durable admission, not new targets.
        const id = try startRestore(ctx, body, false, "independent-cross-realm");
        defer ctx.alloc.free(id);
        const retried = try startRestore(ctx, body, false, "independent-cross-realm");
        defer ctx.alloc.free(retried);
        try std.testing.expectEqualStrings(id, retried);
        try waitJob(ctx, id, "succeeded");
        try scalar(ctx, "SELECT n FROM restored_table", 10);
        var restored = try tableState(ctx, "restored_table");
        defer restored.deinit();
        try std.testing.expectEqual(destination.realm, restored.value.metadata_incarnation);
        // Object identity includes its catalog realm; two independent catalogs
        // can legitimately allocate the same numeric object identifier.
        try std.testing.expect(restored.value.table_id != 0);
        try std.testing.expect(!std.mem.eql(u8, &restored.value.metadata_incarnation, &self.identity.realm));
        try std.testing.expectEqual(destination.id, restored.value.entries[0].function.id);
        try command(ctx, "UPDATE restored_table SET n=n+1");
        try scalar(ctx, "SELECT n FROM restored_table", 1011);
        const cluster = try restoreBody(ctx.alloc, self.identity.*, destination.id, true, self.location);
        defer ctx.alloc.free(cluster);
        try command(ctx, "CREATE TABLE trigger_source (id UUID PRIMARY KEY, n BIGINT)");
        _ = try fixture.waitServingSchema(ctx, "trigger_source", 0);
        try command(ctx, "INSERT INTO trigger_source (id,n) VALUES ('00000000-0000-0000-0000-000000000002',99)");
        try faultQualification(ctx, cluster, destination.id);
        const cluster_id = try startRestore(ctx, cluster, true, "cohort-cross-realm");
        defer ctx.alloc.free(cluster_id);
        try waitJob(ctx, cluster_id, "succeeded");
        try scalar(ctx, "SELECT n FROM trigger_source", 10);
        try command(ctx, "UPDATE trigger_source SET n=n+1");
        try scalar(ctx, "SELECT n FROM trigger_source", 1011);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try scalar(ctx, "SELECT n FROM restored_table", 1011);
        try scalar(ctx, "SELECT n FROM trigger_source", 1011);
        var first = try tableState(ctx, "restored_table");
        defer first.deinit();
        var second = try tableState(ctx, "trigger_source");
        defer second.deinit();
        try std.testing.expectEqual(self.destination, first.value.entries[0].function.id);
        try std.testing.expectEqual(self.destination, second.value.entries[0].function.id);
    }
};
test "mounted public independent and cohort trigger restores cross genuine metadata realms through explicit immutable function mappings and cold restart" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const config_json = try std.json.Stringify.valueAlloc(a, .{ .connections = .{ .archive = .{ .kind = "external_io", .capabilities = &.{ "backup.write", "restore.read" }, .external_io = .{ .protocol = "filesystem", .root = path } } } }, .{});
    defer a.free(config_json);
    var config = try Config.parseFromSlice(a, config_json);
    defer config.deinit();
    const location = try std.fmt.allocPrint(a, "file://{s}", .{path});
    defer a.free(location);
    var identity: SourceIdentity = .{};
    try fixture.runWithState(Producer, .{ .config = &config, .location = location, .identity = &identity });
    try std.testing.expect(identity.routine != 0 and identity.table != 0);
    try fixture.runWithState(Consumer, .{ .config = &config, .location = location, .identity = &identity });
}
