// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Exact original schema changes qualified against PostgreSQL with complete
//! postimages, future-write probes and independently reopened native owners.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;

const SessionGolden = struct {
    format: u8,
    reference: []const u8,
    create_sql: []const u8,
    read_sql: []const u8,
    cases: []const struct {
        id: []const u8,
        steps: []const struct {
            statement: []const u8,
            command_tag: []const u8,
            inside: []const []const []const u8,
            outside: []const []const []const u8,
        },
    },
};

fn connected(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8) !@import("../raft/transport/http_common.zig").HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .connection_id = connection }, .{ .emit_null_optional_fields = false });
    defer ctx.alloc.free(body);
    return fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql", .POST, body, 15_000);
}

fn expectSessionRows(ctx: *fixture.Context, id: ?[]const u8, statement: []const u8, expected: []const []const []const u8) !void {
    var response = try connected(ctx, id, statement);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("session observer status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const value = parsed.value;
    try std.testing.expectEqual(@as(usize, 2), value.columns.len);
    try std.testing.expectEqualStrings("id", value.columns[0].name);
    try std.testing.expectEqualStrings("status", value.columns[1].name);
    for (value.columns) |column| try std.testing.expect(compare.postgresColumnMatches(column, 25));
    try std.testing.expectEqual(expected.len, value.rows.len);
    const flags = value.sql_nulls orelse return error.MissingSqlNullBitmap;
    try std.testing.expectEqual(expected.len, flags.len);
    for (expected, value.rows, flags) |wanted, actual, nulls| {
        try std.testing.expectEqual(wanted.len, actual.len);
        try std.testing.expectEqual(actual.len, nulls.len);
        for (wanted, actual, nulls) |text, cell, is_null| {
            try std.testing.expect(!is_null);
            try std.testing.expect(cell == .string);
            try std.testing.expectEqualStrings(text, cell.string);
        }
    }
}

const SessionCampaign = struct {
    pub const native_sessions = true;
    golden: SessionGolden,
    credentials: client.ApiKeyHeaders = .{},
    final_owner: fixture.ServingSchema = undefined,

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        try command(ctx, self.golden.create_sql, null);
        self.final_owner = try readyTable(ctx, "usage_records", 0);
        var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
        defer opened.deinit(ctx.alloc);
        if (opened.status != 200) std.debug.print("session open status={d} body={s}\n", .{ opened.status, opened.body });
        try std.testing.expectEqual(@as(u16, 200), opened.status);
        var connection = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
        defer connection.deinit();
        const id = connection.value.connection_id;
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (self.golden.cases) |case| {
            const original = try corpus.get(case.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            var original_seen: usize = 0;
            for (case.steps) |step| {
                if (std.mem.eql(u8, original.sql, step.statement)) original_seen += 1;
                errdefer std.debug.print("session original={s} step={s}\n", .{ case.id, step.statement });
                var response = try connected(ctx, id, step.statement);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("session command status={d} body={s}\n", .{ response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                // Public SQL reports INSERT separately from affected rows;
                // PostgreSQL's wire command tag includes the legacy OID/count.
                // Transaction-control tags must match exactly, not by prefix.
                const expected_tag = if (std.mem.startsWith(u8, step.command_tag, "INSERT ")) "INSERT" else step.command_tag;
                try std.testing.expectEqualStrings(expected_tag, parsed.value.command_tag);
                try expectSessionRows(ctx, id, self.golden.read_sql, step.inside);
                try expectSessionRows(ctx, null, self.golden.read_sql, step.outside);
            }
            try std.testing.expectEqual(@as(usize, 1), original_seen);
        }
        const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
        defer ctx.alloc.free(suffix);
        var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .DELETE, null, 15_000);
        defer closed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), closed.status);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expectEqualDeep(self.final_owner, try readyTable(ctx, "usage_records", self.final_owner.version));
        const cases = self.golden.cases;
        const steps = cases[cases.len - 1].steps;
        try expectSessionRows(ctx, null, self.golden.read_sql, steps[steps.len - 1].outside);
    }
};

test "mounted SQL original transaction controls preserve independent visibility savepoints and cold postimages" {
    const ids = [_][]const u8{
        "sql-0085", "sql-0086", "sql-0088", "sql-0764", "sql-0765", "sql-0766", "sql-0767",
        "sql-0768", "sql-0769", "sql-0770", "sql-0092", "sql-0093", "sql-0094",
    };
    var parsed = try std.json.parseFromSlice(SessionGolden, std.testing.allocator, local.sql_parity_fixtures.session_control_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(ids.len, parsed.value.cases.len);
    for (ids, parsed.value.cases) |id, case| try std.testing.expectEqualStrings(id, case.id);
    try fixture.runWithState(SessionCampaign, .{ .golden = parsed.value });
}

const Rows = struct {
    id: []const u8,
    columns: []const []const u8,
    column_oids: []const u32,
    rows: []const []const Json,
    sql_nulls: []const []const bool,
    affected: u64,
};
const Probe = struct { statement: []const u8, sqlstate: ?[]const u8, after: Rows, repeat_on_reopen: bool = false, absent_table: ?[]const u8 = null };
const Outcome = struct { command_tag: []const u8, result: Rows };

test "mounted SQL original indexed mutations publish UUID owners and preserve predicate membership across restart" {
    const ids = [_][]const u8{ "sql-1509", "sql-1510", "sql-1511", "sql-1512", "sql-1514", "sql-1515", "sql-1516", "sql-1517" };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.indexed_mutation_lifecycle_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(ids.len, parsed.value.cases.len);
    for (ids, parsed.value.cases) |id, case| {
        try std.testing.expectEqualStrings(id, case.id);
        try fixture.runWithState(Campaign, .{ .case = case });
    }
}

const Case = struct {
    id: []const u8,
    table: []const u8,
    setup: []const struct { statement: []const u8, table: ?[]const u8 = null },
    read_sql: []const u8,
    ddl_sqlstate: ?[]const u8,
    after_ddl: Rows,
    probes: []const Probe,
    outcome: ?Outcome = null,
};

fn execute(ctx: *fixture.Context, statement: []const u8, read: bool) !@import("../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    authenticated.base = ctx.metadata_uri;
    return client.execute(&authenticated, statement, if (read) .read else .write_admission);
}

fn expectDiagnostic(ctx: *fixture.Context, response: anytype, expected: []const u8) !void {
    if (response.status != 400) std.debug.print("schema rejection status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 400), response.status);
    var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(expected, parsed.value.code);
}

fn readyTable(ctx: *fixture.Context, table: []const u8, version: u32) !fixture.ServingSchema {
    const owner = try fixture.waitServingSchema(ctx, table, version);
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
    try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, table);
    return owner;
}

fn expectAbsent(ctx: *fixture.Context, table: []const u8) !void {
    const domain = local.system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = table }}, .include_query_definitions = true };
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validateRelations(lookup);
    try std.testing.expect(parsed.value.relations[0] == null);
}

fn command(ctx: *fixture.Context, statement: []const u8, expected_error: ?[]const u8) !void {
    return commandParameters(ctx, statement, expected_error, &.{}, null);
}

fn awaitConstraintReceipt(ctx: *fixture.Context, receipt: wire.SQLDDLReceipt, version: u32) !void {
    const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/{s}/namespaces/{s}/tables/{s}/constraints/status", .{ receipt.database, receipt.namespace, receipt.table });
    defer ctx.alloc.free(suffix);
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const remaining_ms = (deadline -| platform.time.monotonicNs()) / std.time.ns_per_ms;
        var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .GET, null, @intCast(@max(1, @min(remaining_ms, 5_000)))) catch |err| switch (err) {
            // A status GET is idempotent. Keep one outer deadline; this never
            // authorizes replaying the admitted DDL or a future-write probe.
            error.Timeout, error.DeadlineExceeded => continue,
            else => return err,
        };
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(wire.RelationalConstraintStatus, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            const status = parsed.value;
            if (status.schema_version > version) return error.SchemaVersionChanged;
            if (status.schema_version == version) {
                if (status.state == .invalid) return error.SchemaLifecycleInvalidConstraint;
                if (status.retirement) |retirement| {
                    if (retirement.failure != null) return error.SchemaLifecyclePublicationFailed;
                } else if (status.state == .enforced) return;
            }
        } else if (response.status != 503 and response.status != 409 and response.status != 504) {
            std.debug.print("schema activation status={d} body={s}\n", .{ response.status, response.body });
            return error.SchemaLifecyclePublicationFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SchemaLifecyclePublicationTimeout;
}

fn commandParameters(ctx: *fixture.Context, statement: []const u8, expected_error: ?[]const u8, parameters: []const Json, outcome: ?Outcome) !void {
    var response = if (parameters.len == 0) try execute(ctx, statement, false) else response: {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .parameters = parameters }, .{});
        defer ctx.alloc.free(body);
        break :response try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/sql", .POST, body, 15_000);
    };
    defer response.deinit(ctx.alloc);
    if (expected_error) |code| return expectDiagnostic(ctx, response, code);
    if (response.status != 200 and response.status != 202 and response.status != 409) {
        std.debug.print("schema command sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        return error.SchemaLifecycleCommandFailed;
    }
    var parsed = std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{}) catch |err| {
        std.debug.print("schema command invalid response sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        return err;
    };
    defer parsed.deinit();
    if (outcome) |expected| try expectResult(ctx, parsed.value, expected.command_tag, expected.result);
    const receipt = parsed.value.ddl_receipt orelse {
        try std.testing.expectEqual(@as(u16, 200), response.status);
        return;
    };
    if (receipt.restore_job_id) |id| {
        const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        while (platform.time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            var observed = try fixture.job(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, id);
            defer observed.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), observed.status);
            var job = try std.json.parseFromSlice(Json, ctx.alloc, observed.body, .{});
            defer job.deinit();
            const phase = (try fixture.field(job.value, "phase")).string;
            if (std.mem.eql(u8, phase, "succeeded")) return;
            if (std.mem.eql(u8, phase, "failed")) {
                std.debug.print("schema job failed sql={s} body={s}\n", .{ statement, observed.body });
                return error.SchemaLifecyclePublicationFailed;
            }
            try ctx.io.sleep(.fromMilliseconds(10), .awake);
        }
        return error.SchemaLifecyclePublicationTimeout;
    }
    // Pending FK/index/constraint receipts are observed through serving
    // publication. Never repeat the admitted ALTER or allocate another job.
    if (receipt.state == .invalid) return error.SchemaLifecycleInvalidConstraint;
    const version = std.math.cast(u32, receipt.schema_version) orelse return error.InvalidSchemaReceipt;
    const owner = try readyTable(ctx, receipt.table, version);
    try std.testing.expectEqual(try std.fmt.parseInt(u64, receipt.table_id, 10), owner.table_id);
    if (receipt.state != .ready) try awaitConstraintReceipt(ctx, receipt, version);
}

fn expectRows(ctx: *fixture.Context, statement: []const u8, expected: Rows) !void {
    var response = try execute(ctx, statement, true);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("schema observer status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try expectResult(ctx, parsed.value, "SELECT", expected);
}

fn expectResult(ctx: *fixture.Context, actual: wire.SQLResponse, command_tag: []const u8, expected: Rows) !void {
    try std.testing.expectEqualStrings(command_tag, actual.command_tag);
    try std.testing.expectEqual(expected.affected, std.math.cast(u64, actual.rows_affected) orelse return error.InvalidAffectedRows);
    try std.testing.expectEqual(expected.rows.len, actual.rows.len);
    try std.testing.expectEqual(expected.columns.len, actual.columns.len);
    for (actual.columns, expected.columns, expected.column_oids) |column, name, oid| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    const nulls = actual.sql_nulls orelse return error.MissingSqlNullBitmap;
    try std.testing.expectEqual(actual.rows.len, nulls.len);
    const used = try ctx.alloc.alloc(bool, expected.rows.len);
    defer ctx.alloc.free(used);
    @memset(used, false);
    for (actual.rows, nulls) |row, flags| {
        const matched = for (expected.rows, expected.sql_nulls, 0..) |want, want_flags, index| {
            if (used[index]) continue;
            if (!try compare.rowMatchesWithNulls(ctx.alloc, actual.columns, row, flags, want, want_flags)) continue;
            used[index] = true;
            break true;
        } else false;
        if (!matched) std.debug.print("schema observer mismatch original={s}\n", .{expected.id});
        try std.testing.expect(matched);
    }
}

const Campaign = struct {
    case: Case,
    credentials: client.ApiKeyHeaders = .{},
    final_owner: fixture.ServingSchema = undefined,

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        errdefer std.debug.print("schema lifecycle id={s} before restart\n", .{self.case.id});
        for (self.case.setup) |step| {
            try command(ctx, step.statement, null);
            if (step.table) |name| _ = try readyTable(ctx, name, 0);
        }
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.case.id);
        var parameters_arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer parameters_arena.deinit();
        const parameters = try local.sql_parity_fixtures.Corpus.logicalParameters(parameters_arena.allocator(), original);
        try commandParameters(ctx, original.sql, self.case.ddl_sqlstate, parameters, self.case.outcome);
        _ = try readyTable(ctx, self.case.table, 0);
        try expectRows(ctx, self.case.read_sql, self.case.after_ddl);
        for (self.case.probes) |probe| {
            try command(ctx, probe.statement, probe.sqlstate);
            if (probe.absent_table) |name| try expectAbsent(ctx, name);
            try expectRows(ctx, self.case.read_sql, probe.after);
        }
        self.final_owner = try readyTable(ctx, self.case.table, 0);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        errdefer std.debug.print("schema lifecycle id={s} after restart\n", .{self.case.id});
        try std.testing.expectEqualDeep(self.final_owner, try readyTable(ctx, self.case.table, self.final_owner.version));
        const final = self.case.probes[self.case.probes.len - 1].after;
        try expectRows(ctx, self.case.read_sql, final);
        if (self.case.ddl_sqlstate) |code| {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            const original = try corpus.get(self.case.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            try command(ctx, original.sql, code);
            try expectRows(ctx, self.case.read_sql, final);
        }
        for (self.case.probes) |probe| if (probe.sqlstate != null or probe.repeat_on_reopen) {
            try command(ctx, probe.statement, probe.sqlstate);
            if (probe.absent_table) |name| try expectAbsent(ctx, name);
            try expectRows(ctx, self.case.read_sql, final);
        };
    }
};

test "mounted SQL original schema lifecycles preserve postimages enforcement and cold owners" {
    const ids = [_][]const u8{
        "sql-0677", "sql-0678", "sql-0680", "sql-0681", "sql-0727", "sql-0728", "sql-0729", "sql-0730", "sql-0731",
        "sql-0733", "sql-0738", "sql-0740", "sql-0779", "sql-1408", "sql-1412",
    };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(ids.len, parsed.value.cases.len);
    for (ids, parsed.value.cases) |id, case| {
        try std.testing.expectEqualStrings(id, case.id);
        try fixture.runWithState(Campaign, .{ .case = case });
    }
}
