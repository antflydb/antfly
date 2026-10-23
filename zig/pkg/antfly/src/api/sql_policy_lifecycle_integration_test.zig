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

//! Exact policy DDL through metadata Raft, owner installation and cold reads.
const std = @import("std");
const local = @import("antfly_local_sources");
const platform = @import("antfly_platform");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const server = @import("http_server.zig");
const users = @import("../usermgr/mod.zig");
const http = @import("../raft/transport/http_common.zig");
const wire = @import("antfly_metadata_openapi").types;
const policies = local.system_catalog_policies;
const Cell = ?[]const u8;
const Case = struct {
    id: []const u8,
    command_tag: []const u8,
    setup: []const []const u8,
    rows: []const []const Cell,
    all_rows: []const []const Cell,
    filtered_rows: []const []const Cell,
    probes: []const struct { statement: []const u8, sqlstate: ?[]const u8, rows: ?[]const []const Cell },
};
const Golden = struct { format: u8, reference: []const u8, create: []const u8, seed: []const u8, read: []const u8, cases: []const Case };

fn execute(ctx: *fixture.Context, headers: []const http.RequestHeader, statement: []const u8, read: bool) !http.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = headers;
    authenticated.base = ctx.metadata_uri;
    return client.execute(&authenticated, statement, if (read) .read else .write_admission);
}

fn command(ctx: *fixture.Context, headers: []const http.RequestHeader, statement: []const u8, expected_error: ?[]const u8) !void {
    return commandRows(ctx, headers, statement, expected_error, null);
}

fn commandRows(ctx: *fixture.Context, headers: []const http.RequestHeader, statement: []const u8, expected_error: ?[]const u8, expected_rows: ?[]const []const Cell) !void {
    var response = try execute(ctx, headers, statement, false);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("policy SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    if (expected_error) |code| {
        try std.testing.expect(response.status == 400 or response.status == 403);
        var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings(code, parsed.value.code);
    } else {
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const tag = commandTag(statement) orelse return error.UnexpectedPolicyCommand;
        if (std.mem.eql(u8, parsed.value.command_tag, "DDL PENDING")) {
            // Public async DDL acknowledges a durable intent, not activation.
            // The campaign waits for exact owner ACKs before any row probe.
            try std.testing.expect(parsed.value.mutation_outcome == .committed_pending);
            try std.testing.expect(std.mem.eql(u8, tag, "ALTER TABLE") or std.mem.eql(u8, tag, "CREATE POLICY") or std.mem.eql(u8, tag, "ALTER POLICY") or std.mem.eql(u8, tag, "DROP POLICY"));
        } else try std.testing.expectEqualStrings(tag, parsed.value.command_tag);
        if (expected_rows) |rows| try expectResult(parsed.value, rows) else {
            try std.testing.expectEqual(@as(usize, 0), parsed.value.columns.len);
            try std.testing.expectEqual(@as(usize, 0), parsed.value.rows.len);
        }
    }
}

fn commandTag(statement: []const u8) ?[]const u8 {
    for ([_][]const u8{ "CREATE TABLE", "CREATE POLICY", "ALTER TABLE", "ALTER POLICY", "DROP POLICY", "INSERT", "UPDATE", "DELETE" }) |tag| {
        if (std.mem.startsWith(u8, statement, tag)) return tag;
    }
    return null;
}

fn expectRows(ctx: *fixture.Context, headers: []const http.RequestHeader, statement: []const u8, expected: []const []const Cell) !void {
    var response = try execute(ctx, headers, statement, true);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("policy read status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try expectResult(parsed.value, expected);
}

fn expectResult(result: wire.SQLResponse, expected: []const []const Cell) !void {
    try std.testing.expectEqual(@as(usize, 3), result.columns.len);
    for (result.columns, [_][]const u8{ "id", "tenant_id", "status" }) |column, name| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(column, 25));
    }
    try std.testing.expectEqual(expected.len, result.rows.len);
    const bitmap = result.sql_nulls orelse return error.MissingSqlNullBitmap;
    try std.testing.expectEqual(expected.len, bitmap.len);
    for (expected, result.rows, bitmap) |wanted, actual, nulls| {
        try std.testing.expectEqual(wanted.len, actual.len);
        try std.testing.expectEqual(wanted.len, nulls.len);
        for (wanted, actual, nulls) |cell, value, is_null| {
            try std.testing.expectEqual(cell == null, is_null);
            if (cell) |text| {
                try std.testing.expect(value == .string);
                try std.testing.expectEqualStrings(text, value.string);
            } else try std.testing.expect(value == .null);
        }
    }
}

fn waitPublication(ctx: *fixture.Context, table_id: u64, phase: policies.Publication.Phase, after_generation: u64) !u64 {
    const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .row_policy_install_authority = true, .deadline_ns = deadline }, .{ .policy_publication_status = table_id });
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(?policies.PublicationStamp, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        if (parsed.value) |publication| if (publication.phase == phase and publication.generation > after_generation) {
            // Disabled is committed metadata-first. The final owner ACK is
            // still required before an unprotected request can be served.
            if (phase == .disabled) {
                const snapshot_bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .row_policy_install_authority = true, .deadline_ns = deadline }, .snapshot);
                defer ctx.alloc.free(snapshot_bytes);
                var state = try std.json.parseFromSlice(local.system_catalog_domain.State, ctx.alloc, snapshot_bytes, .{});
                defer state.deinit();
                const ready = for (state.value.policy_publications) |value| {
                    if (value.table_id == table_id) break value.disabled_acknowledged_owners.len == value.required_owners.len;
                } else false;
                if (!ready) {
                    try ctx.io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                }
            }
            return publication.generation;
        };
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.PolicyPublicationTimeout;
}

const Campaign = struct {
    golden: *const Golden,
    case: Case,
    credentials: client.ApiKeyHeaders = .{},
    reader_authorization: [192]u8 = undefined,
    reader: [1]http.RequestHeader = undefined,
    owner: fixture.ServingSchema = undefined,
    generation: u64 = 0,
    final_phase: policies.Publication.Phase = .active,

    pub fn configureAuth(manager: *users.UserManager) !void {
        var permission = try users.Permission.initOwned(manager.alloc, .table, "*", .write);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-reader", permission);
        try manager.addRoleToUser("pk-reader", "app_reader");
    }

    fn authenticate(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
        var read = try users.Permission.initOwned(ctx.alloc, .table, "*", .read);
        defer read.deinit(ctx.alloc);
        var write = try users.Permission.initOwned(ctx.alloc, .table, "*", .write);
        defer write.deinit(ctx.alloc);
        var key = try manager.createApiKey("pk-reader", "sql-policy-campaign", &.{ read, write }, &.{}, null);
        defer key.deinit(manager.alloc);
        self.reader[0] = .{ .name = "authorization", .value = try std.fmt.bufPrint(&self.reader_authorization, "ApiKey {s}", .{key.encoded}) };
        ctx.reader_headers = &self.reader;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.authenticate(ctx);
        const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const configured = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .setting_mutate = .{ .put = .{ .name = "app.tenant_id", .kind = .string, .policy_sensitive = true, .default = .{ .string = "tenant-a" } } } });
        ctx.alloc.free(configured);
        try command(ctx, ctx.admin_headers, self.golden.create, null);
        self.owner = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.owner.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try @import("sql_policy_owner_test_support.zig").expectNoSearchArtifacts(ctx, self.owner.table_id);
        try command(ctx, ctx.admin_headers, self.golden.seed, null);
        for (self.case.setup) |statement| {
            try command(ctx, ctx.admin_headers, statement, null);
            if (std.mem.indexOf(u8, statement, "ENABLE ROW LEVEL SECURITY") != null)
                self.generation = try waitPublication(ctx, self.owner.table_id, .active, self.generation);
        }
        if (self.generation != 0 and !std.mem.eql(u8, self.case.id, "sql-0322") and !std.mem.eql(u8, self.case.id, "sql-0329") and !std.mem.eql(u8, self.case.id, "sql-0331"))
            try expectRows(ctx, ctx.reader_headers, self.golden.read, &.{});
        // A separate administrator policy exposes every physical row to the
        // independent auditor without granting the tested principal anything.
        // This proves rejected mutations preserve hidden as well as visible rows.
        try command(ctx, ctx.admin_headers, "CREATE POLICY fixture_audit_read ON usage_records TO \"pk-rewriter\" USING (true)", null);
        if (self.generation != 0) self.generation = try waitPublication(ctx, self.owner.table_id, .active, self.generation);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.case.id);
        try std.testing.expectEqualStrings(self.case.command_tag, commandTag(original.sql).?);
        try command(ctx, ctx.admin_headers, original.sql, null);
        self.final_phase = if (std.mem.eql(u8, self.case.id, "sql-0322")) .disabled else .active;
        self.generation = try waitPublication(ctx, self.owner.table_id, self.final_phase, self.generation);
        try self.check(ctx);
    }

    fn check(self: *@This(), ctx: *fixture.Context) !void {
        try expectRows(ctx, ctx.reader_headers, self.golden.read, self.case.rows);
        try expectRows(ctx, ctx.admin_headers, self.golden.read, self.case.all_rows);
        for (self.case.probes) |probe| {
            try commandRows(ctx, ctx.reader_headers, probe.statement, probe.sqlstate, probe.rows);
            if (probe.sqlstate == null) {
                // Successful probes are real committed writes, not a mocked
                // validator. Return to the independently checked preimage.
                if (std.mem.startsWith(u8, probe.statement, "INSERT"))
                    try command(ctx, ctx.admin_headers, "DELETE FROM usage_records WHERE id IN ('p','q','r')", null)
                else
                    try command(ctx, ctx.admin_headers, "UPDATE usage_records SET tenant_id = 'tenant-a',status = 'active' WHERE id = 'a'", null);
            }
            try expectRows(ctx, ctx.reader_headers, self.golden.read, self.case.rows);
            try expectRows(ctx, ctx.admin_headers, self.golden.read, self.case.all_rows);
        }
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.reader_headers = &self.reader;
        try std.testing.expectEqualDeep(self.owner, try fixture.waitServingSchema(ctx, "usage_records", self.owner.version));
        try self.check(ctx);
        // Disabled policies need not eagerly open a resident owner during
        // control recovery. Observe real inventory after the cold SQL reads
        // have admitted that owner; those reads must not create search indexes.
        try @import("sql_policy_owner_test_support.zig").expectNoSearchArtifacts(ctx, self.owner.table_id);
        // SQL predicates and legacy mandatory Antfly filters compose; neither
        // replaces the other. Authenticate anew after the filter mutation.
        const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
        try manager.setRowFilter("pk-reader", "*", "{\"term\":{\"status\":\"active\"}}");
        try expectRows(ctx, ctx.reader_headers, self.golden.read, self.case.filtered_rows);
    }
};

test "mounted SQL original row policies publish default denial compose filters and recover owner enforcement" {
    const ids = [_][]const u8{ "sql-0321", "sql-0322", "sql-0323", "sql-0324", "sql-0325", "sql-0326", "sql-0327", "sql-0328", "sql-0329", "sql-0330", "sql-0331" };
    var parsed = try std.json.parseFromSlice(Golden, std.testing.allocator, local.sql_parity_fixtures.policy_lifecycle_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(ids.len, parsed.value.cases.len);
    for (parsed.value.cases, ids) |case, id| {
        try std.testing.expectEqualStrings(id, case.id);
        errdefer std.debug.print("policy lifecycle original={s}\n", .{case.id});
        try fixture.runWithState(Campaign, .{ .golden = &parsed.value, .case = case });
    }
}
