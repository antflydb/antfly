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
    table: []const u8 = "usage_records",
    setup: []const struct { statement: []const u8, table: ?[]const u8 = null } = &.{},
    read_sql: []const u8,
    cases: []const struct {
        id: []const u8,
        steps: []const struct {
            statement: []const u8,
            command_tag: []const u8,
            sqlstate: ?[]const u8 = null,
            observe_inside: bool = true,
            observer_statement: ?[]const u8 = null,
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
    var response = try client.executeConnected(ctx, null, id, statement, if (id != null) .session_observation else .read);
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
        for (self.golden.setup) |step| {
            try command(ctx, step.statement, null);
            if (step.table) |name| _ = try readyTable(ctx, name, 0);
        }
        self.final_owner = try readyTable(ctx, self.golden.table, 0);
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
                if (step.observer_statement) |statement| try command(ctx, statement, null);
                if (std.mem.startsWith(u8, step.statement, "ALTER TABLE ")) {
                    try command(ctx, step.statement, null);
                    _ = try readyTable(ctx, self.golden.table, 0);
                    if (step.observe_inside) try expectSessionRows(ctx, id, self.golden.read_sql, step.inside);
                    try expectSessionRows(ctx, null, self.golden.read_sql, step.outside);
                    continue;
                }
                var response = try connected(ctx, id, step.statement);
                defer response.deinit(ctx.alloc);
                if (step.sqlstate) |code| {
                    try expectDiagnostic(ctx, response, code);
                    try std.testing.expect(!step.observe_inside);
                    try expectSessionRows(ctx, null, self.golden.read_sql, step.outside);
                    continue;
                }
                if (response.status != 200) std.debug.print("session command status={d} body={s}\n", .{ response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                // Public SQL reports INSERT separately from affected rows;
                // PostgreSQL's wire command tag includes the legacy OID/count.
                // Transaction-control tags must match exactly, not by prefix.
                const expected_tag = if (std.mem.startsWith(u8, step.command_tag, "INSERT ")) "INSERT" else step.command_tag;
                const normalized_tag = if (std.mem.startsWith(u8, expected_tag, "UPDATE ")) "UPDATE" else expected_tag;
                const final_tag = if (std.mem.startsWith(u8, normalized_tag, "SELECT ")) "SELECT" else normalized_tag;
                try std.testing.expectEqualStrings(final_tag, parsed.value.command_tag);
                if (step.observe_inside) try expectSessionRows(ctx, id, self.golden.read_sql, step.inside);
                try expectSessionRows(ctx, null, self.golden.read_sql, step.outside);
            }
            try std.testing.expectEqual(@as(usize, 1), original_seen);
        }
        self.final_owner = try readyTable(ctx, self.golden.table, 0);
        const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
        defer ctx.alloc.free(suffix);
        var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .DELETE, null, 15_000);
        defer closed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), closed.status);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        const owner = try readyTable(ctx, self.golden.table, self.final_owner.version);
        try std.testing.expectEqualDeep(self.final_owner, owner);
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

test "mounted SQL deferred uniqueness permits repaired transient conflicts with isolated observers and restart" {
    try constraintTimingCampaign(null, &.{ "sql-0016", "sql-0082", "sql-0018", "sql-0083" });
}

test "mounted SQL named FK and CHECK timing preserves schema epochs" {
    try constraintTimingCampaign("sql-0083", &.{"sql-0083"});
}

test "mounted SQL original document CREATE shares native schemas writes and cold recovery" {
    const DocumentCampaign = struct {
        pub const native_sessions = true;
        credentials: client.ApiKeyHeaders = .{},
        owner: fixture.ServingSchema = undefined,

        fn check(ctx: *fixture.Context) !void {
            try expectSessionRows(ctx, null, "SELECT _id AS id,title AS status FROM docs ORDER BY _id", &.{&.{ "doc:one", "stored" }});
            try expectSessionRows(ctx, null, "SELECT _id AS id,_doc->'extra'->>'counter' AS status FROM docs ORDER BY _id", &.{&.{ "doc:one", "9007199254740993" }});
        }

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activate(ctx);
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            const original = try corpus.get("sql-0670");
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            try command(ctx, original.sql, null);
            self.owner = try readyTable(ctx, "docs", 0);
            try command(ctx, "INSERT INTO docs(_id,_doc) VALUES('doc:one','{\"title\":\"stored\",\"extra\":{\"counter\":9007199254740993}}')", null);
            const fingerprint = try schemaFingerprint(ctx, "docs");
            // This conditional declaration differs from the accepted schema.
            // Its success must not replace a live table or erase its data.
            try command(ctx, "CREATE TABLE IF NOT EXISTS docs () WITH (antfly.storage_mode='document')", null);
            try std.testing.expectEqualDeep(fingerprint, try schemaFingerprint(ctx, "docs"));
            try std.testing.expectEqualDeep(self.owner, try readyTable(ctx, "docs", self.owner.version));
            try check(ctx);
            try command(ctx, original.sql, "42P07");
            for ([_][]const u8{ "sql-0780", "sql-0783", "sql-0784" }) |id| {
                const invalid = try corpus.get(id);
                try std.testing.expectEqualStrings("rejection", invalid.source_expectation);
                try command(ctx, invalid.sql, "22023");
                try std.testing.expectEqualDeep(fingerprint, try schemaFingerprint(ctx, "docs"));
                try check(ctx);
            }
            try command(ctx, "CREATE TABLE bad_document () WITH (antfly.storage_mode='document',antfly.default_type='doc') DOCUMENT SCHEMA doc AS JSON '{'", "22023");
            try expectAbsent(ctx, "bad_document");
        }

        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try std.testing.expectEqualDeep(self.owner, try readyTable(ctx, "docs", self.owner.version));
            try check(ctx);
            try command(ctx, "INSERT INTO docs(_id,_doc) VALUES('doc:two','{\"title\":\"after restart\"}')", null);
            try expectSessionRows(ctx, null, "SELECT _id AS id,title AS status FROM docs ORDER BY _id", &.{ &.{ "doc:one", "stored" }, &.{ "doc:two", "after restart" } });
        }
    };
    try fixture.runWithState(DocumentCampaign, .{});
}

test "mounted SQL original transaction characteristics retain snapshots and savepoint modes" {
    const Golden = struct { format: u8, campaigns: []const SessionGolden };
    var parsed = try std.json.parseFromSlice(Golden, std.testing.allocator, local.sql_parity_fixtures.transaction_characteristics_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    const ids = [_][]const u8{ "sql-0084", "sql-0087" };
    try std.testing.expectEqual(ids.len, parsed.value.campaigns.len);
    for (ids, parsed.value.campaigns) |id, golden| {
        try std.testing.expectEqual(@as(usize, 1), golden.cases.len);
        try std.testing.expectEqualStrings(id, golden.cases[0].id);
        try fixture.runWithState(SessionCampaign, .{ .golden = golden });
    }
}

fn constraintTimingCampaign(only: ?[]const u8, expected_ids: []const []const u8) !void {
    const Golden = struct { format: u8, campaigns: []const SessionGolden };
    const bytes = local.sql_parity_fixtures.constraint_timing_reference;
    var parsed = try std.json.parseFromSlice(Golden, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqual(@as(usize, 4), parsed.value.campaigns.len);
    var selected: usize = 0;
    for (parsed.value.campaigns) |golden| {
        if (only) |id| if (!std.mem.eql(u8, id, golden.cases[0].id)) continue;
        try std.testing.expect(selected < expected_ids.len);
        try std.testing.expectEqualStrings(expected_ids[selected], golden.cases[0].id);
        errdefer std.debug.print("constraint timing original={s}\n", .{golden.cases[0].id});
        try fixture.runWithState(SessionCampaign, .{ .golden = golden });
        selected += 1;
    }
    try std.testing.expectEqual(expected_ids.len, selected);
}

const Rows = struct {
    id: []const u8,
    columns: []const []const u8,
    column_oids: []const u32,
    rows: []const []const Json,
    sql_nulls: []const []const bool,
    affected: u64,
};

const TablespaceCampaign = struct {
    const domain = local.system_catalog_domain;
    const TablespaceCase = struct {
        id: []const u8,
        setup: []const []const u8,
        name: []const u8,
        present: bool,
        command_tag: []const u8,
        prior_name: ?[]const u8 = null,
        unchanged: bool = false,
        repeat_noop: bool = false,
        witness: ?struct {
            in_use_sqlstate: []const u8,
            preserved_binding: bool,
            rows: []const []const []const u8,
        } = null,
    };
    case: TablespaceCase,
    owner: fixture.ServingSchema = undefined,
    tablespace_id: u64 = 0,
    location_hash: [32]u8 = undefined,
    revision: u64 = 0,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        var permission = try @import("../usermgr/mod.zig").Permission.initOwned(manager.alloc, .tablespace, "*", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }

    fn snapshot(ctx: *fixture.Context) !std.json.Parsed(domain.State) {
        const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const bytes = try source.systemCatalog(ctx.alloc, .{}, .snapshot);
        defer ctx.alloc.free(bytes);
        return std.json.parseFromSlice(domain.State, ctx.alloc, bytes, .{ .allocate = .alloc_always });
    }

    fn verifyBinding(self: *@This(), ctx: *fixture.Context) !void {
        var catalog = try snapshot(ctx);
        defer catalog.deinit();
        const space = catalog.value.find(.tablespace, 0, self.case.name) orelse return error.MissingSqlTablespace;
        if (self.case.prior_name) |old| try std.testing.expect(catalog.value.find(.tablespace, 0, old) == null);
        try std.testing.expectEqual(self.tablespace_id, space.id);
        var location_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(space.location_json, &location_hash, .{});
        try std.testing.expectEqualSlices(u8, &self.location_hash, &location_hash);
        const table = catalog.value.byId(.table, self.owner.table_id) orelse return error.MissingSqlTable;
        try std.testing.expectEqual(self.tablespace_id, table.tablespace_id);
        try expectSessionRows(ctx, null, "SELECT id::text AS id,status FROM usage_records ORDER BY id", self.case.witness.?.rows);
    }

    fn originalCommand(self: *@This(), ctx: *fixture.Context, statement: []const u8) !void {
        var response = try execute(ctx, statement, false);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("tablespace original={s} status={d} body={s}\n", .{ self.case.id, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(self.case.command_tag, result.value.command_tag);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.case.id);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        for (self.case.setup) |statement| try command(ctx, statement, null);
        if (self.case.witness != null) {
            const name = self.case.prior_name orelse self.case.name;
            {
                var catalog = try snapshot(ctx);
                defer catalog.deinit();
                const space = catalog.value.find(.tablespace, 0, name) orelse return error.MissingSqlTablespace;
                self.tablespace_id = space.id;
                std.crypto.hash.sha2.Sha256.hash(space.location_json, &self.location_hash, .{});
            }
            const create = try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE usage_records(id UUID,status TEXT) TABLESPACE {s}", .{name});
            defer ctx.alloc.free(create);
            try command(ctx, create, null);
            self.owner = try readyTable(ctx, "usage_records", 0);
            try command(ctx, "INSERT INTO usage_records(id,status) VALUES ('00000000-0000-0000-0000-000000000001','retained')", null);
            if (self.case.present) {
                try self.originalCommand(ctx, original.sql);
                try verifyBinding(self, ctx);
            }
            // A definite in-use rejection must preserve both the binding and
            // complete native rows. The original successful DROP runs only
            // after the dependent table has been removed.
            try command(ctx, "DROP TABLESPACE fastspace_archive;", self.case.witness.?.in_use_sqlstate);
            try verifyBinding(self, ctx);
            if (!self.case.present) {
                try command(ctx, "DROP TABLE usage_records", null);
                try self.originalCommand(ctx, original.sql);
            }
        } else {
            var before = try snapshot(ctx);
            defer before.deinit();
            self.revision = before.value.revision;
            try self.originalCommand(ctx, original.sql);
            if (self.case.repeat_noop) try self.originalCommand(ctx, original.sql);
        }
        var final = try snapshot(ctx);
        defer final.deinit();
        try std.testing.expectEqual(self.case.present, final.value.find(.tablespace, 0, self.case.name) != null);
        if (self.case.unchanged) try std.testing.expectEqual(self.revision, final.value.revision);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        var catalog = try snapshot(ctx);
        defer catalog.deinit();
        const space = catalog.value.find(.tablespace, 0, self.case.name);
        try std.testing.expectEqual(self.case.present, space != null);
        if (self.case.unchanged) try std.testing.expectEqual(self.revision, catalog.value.revision);
        if (!self.case.present) return;
        try std.testing.expectEqual(self.tablespace_id, space.?.id);
        try verifyBinding(self, ctx);
        try std.testing.expectEqualDeep(self.owner, try readyTable(ctx, "usage_records", self.owner.version));
    }
};

test "mounted SQL original tablespace names preserve bound owners deletion guards and restart" {
    const Golden = struct { format: u8, entries: []const TablespaceCampaign.TablespaceCase };
    var parsed = try std.json.parseFromSlice(Golden, std.testing.allocator, local.sql_parity_fixtures.tablespace_lifecycle_reference, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    const ids = [_][]const u8{ "sql-0111", "sql-0112", "sql-0113" };
    try std.testing.expectEqual(ids.len, parsed.value.entries.len);
    for (ids, parsed.value.entries) |id, case| {
        try std.testing.expectEqualStrings(id, case.id);
        if (case.witness) |witness| try std.testing.expect(witness.preserved_binding);
        try fixture.runWithState(TablespaceCampaign, .{ .case = case });
    }
}
const Probe = struct { statement: []const u8, sqlstate: ?[]const u8, after: Rows, repeat_on_reopen: bool = false, absent_table: ?[]const u8 = null, declared_not_valid: ?bool = null };
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

test "mounted SQL original named arbiters distinguish constraints from indexes and retain cold enforcement" {
    const ids = [_][]const u8{ "sql-1456", "sql-1459", "sql-1464" };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.named_conflict_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(ids.len, parsed.value.cases.len);
    for (ids, parsed.value.cases) |id, input| {
        try std.testing.expectEqualStrings(id, input.id);
        var case = input;
        case.verify_primary_identity = true;
        case.unchanged_owner = true;
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
    scenario: ?[]const u8 = null,
    native_job_error: ?[]const u8 = null,
    unchanged_owner: bool = false,
    verify_primary_identity: bool = false,
    check_name: ?[]const u8 = null,
    declared_not_valid: ?bool = null,
    retained_not_valid_check: ?[]const u8 = null,
    cover: ?struct { index: []const u8, statement: []const u8, result: Rows } = null,
};

fn planDeclaresIndex(plan: Json, name: []const u8, depth: usize) !bool {
    if (depth > 64 or plan != .object) return error.InvalidSqlBackendResponse;
    if (plan.object.get("index")) |index| {
        if (index == .string and std.mem.eql(u8, index.string, name)) return true;
    }
    if (plan.object.get("declared_indexes")) |indexes| if (indexes == .array) {
        for (indexes.array.items) |index| {
            if (index == .string and std.mem.eql(u8, index.string, name)) return true;
        }
    };
    if (plan.object.get("plans")) |children| {
        if (children != .array) return error.InvalidSqlBackendResponse;
        for (children.array.items) |child| if (try planDeclaresIndex(child, name, depth + 1)) return true;
    }
    return false;
}

fn expectCoverConsumer(ctx: *fixture.Context, case: Case) !void {
    const cover = case.cover orelse return;
    try expectRows(ctx, cover.statement, cover.result);
    const statement = try std.fmt.allocPrint(ctx.alloc, "EXPLAIN (FORMAT JSON) {s}", .{cover.statement});
    defer ctx.alloc.free(statement);
    var response = try execute(ctx, statement, true);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("EXPLAIN", parsed.value.command_tag);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows[0].len);
    const cell = parsed.value.rows[0][0];
    try std.testing.expect(cell == .string);
    var plan = try std.json.parseFromSlice(Json, ctx.alloc, cell.string, .{});
    defer plan.deinit();
    // EXPLAIN describes available declarations; physical selection occurs at
    // the retained owner snapshot. Never misreport this as an executed index
    // choice. The storage work-count gate independently proves index-only I/O.
    try std.testing.expect(try planDeclaresIndex(try fixture.field(plan.value, "Plan"), cover.index, 0));
}

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

/// Prove an aborted bind did not rewrite identical logical values and cold
/// recovery retained the exact primary stamps, not merely an equal SQL bag.
fn primaryFingerprint(ctx: *fixture.Context, table: []const u8) ![32]u8 {
    const domain = local.system_catalog_domain;
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .targets = &.{.{ .table = table }} };
    const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var bound = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer bound.deinit();
    try bound.value.validateRelations(lookup);
    if (bound.value.tables.len != 1) return error.InvalidSqlBackendResponse;
    const owner = bound.value.tables[0] orelse return error.TableNotFound;
    const statement = try std.fmt.allocPrint(ctx.alloc, "SELECT _id FROM {s} ORDER BY _id", .{table});
    defer ctx.alloc.free(statement);
    var response = try execute(ctx, statement, true);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("sql:native-primary-postimage:v1\x00");
    for (parsed.value.rows) |row| {
        try std.testing.expectEqual(@as(usize, 1), row.len);
        const key = row[0].string;
        var primary = (try reader.lookup(ctx.alloc, owner.name, key, .{ .include_primary_digest = true }, .read_index)) orelse return error.MissingMutationPrimary;
        defer primary.deinit(ctx.alloc);
        var frame: [16]u8 = undefined;
        std.mem.writeInt(u64, frame[0..8], @intCast(key.len), .big);
        std.mem.writeInt(u64, frame[8..16], primary.version, .big);
        hash.update(&frame);
        hash.update(key);
        hash.update(&(primary.expected_content_digest orelse return error.MissingMutationDigest));
    }
    return hash.finalResult();
}

fn schemaFingerprint(ctx: *fixture.Context, table: []const u8) ![32]u8 {
    const domain = local.system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = table }}, .include_query_definitions = true };
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validateRelations(lookup);
    const relation = parsed.value.relations[0] orelse return error.TableNotFound;
    const definition = relation.table.query_definition orelse return error.MissingSchemaDefinition;
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(definition.schema_json, &result, .{});
    return result;
}

fn expectDeclaredValidity(ctx: *fixture.Context, table: []const u8, check_name: []const u8, not_valid: bool) !void {
    const domain = local.system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = table }}, .include_query_definitions = true };
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validateRelations(lookup);
    const relation = parsed.value.relations[0] orelse return error.TableNotFound;
    const definition = relation.table.query_definition orelse return error.MissingSchemaDefinition;
    var schema = try std.json.parseFromSlice(Json, ctx.alloc, definition.schema_json, .{});
    defer schema.deinit();
    const checks = try fixture.field(schema.value, "checks");
    for (checks.array.items) |check| {
        const name = try fixture.field(check, "name");
        if (!std.mem.eql(u8, check_name, name.string)) continue;
        const declared = check.object.get("not_valid") orelse Json{ .bool = false };
        try std.testing.expect(declared == .bool);
        try std.testing.expectEqual(not_valid, declared.bool);
        return;
    }
    return error.ConstraintNotFound;
}

fn expectPublicValidationRequiresRewrite(ctx: *fixture.Context, table: []const u8, check_name: []const u8) !void {
    const domain = local.system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = table }}, .include_query_definitions = true };
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var catalog = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer catalog.deinit();
    try catalog.value.validateRelations(lookup);
    const relation = catalog.value.relations[0] orelse return error.TableNotFound;
    var schema = try std.json.parseFromSlice(Json, ctx.alloc, relation.table.query_definition.?.schema_json, .{});
    defer schema.deinit();
    const a = schema.arena.allocator();
    _ = schema.value.object.orderedRemove("version");
    const checks = schema.value.object.getPtr("checks").?.array.items;
    for (checks) |*check| {
        if (std.mem.eql(u8, check.object.get("name").?.string, check_name)) {
            try check.object.put(a, "not_valid", .{ .bool = false });
            break;
        }
    } else return error.ConstraintNotFound;
    const body = try std.json.Stringify.valueAlloc(a, schema.value, .{});
    const path = try std.fmt.allocPrint(a, "/db/v1/tables/{s}/schema", .{table});
    const before = try schemaFingerprint(ctx, table);
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, path, .PUT, body);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("NOT VALID public admission status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 409), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "rewrite=true") != null);
    try std.testing.expectEqualDeep(before, try schemaFingerprint(ctx, table));
    try expectDeclaredValidity(ctx, table, check_name, true);
}

fn command(ctx: *fixture.Context, statement: []const u8, expected_error: ?[]const u8) !void {
    return commandParameters(ctx, statement, expected_error, &.{}, null, null);
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

fn commandParameters(ctx: *fixture.Context, statement: []const u8, expected_error: ?[]const u8, parameters: []const Json, outcome: ?Outcome, expected_job_error: ?[]const u8) !void {
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
            var job = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, observed.body, .{ .ignore_unknown_fields = true });
            defer job.deinit();
            if (std.mem.eql(u8, job.value.phase, "succeeded")) {
                try std.testing.expect(expected_job_error == null);
                return;
            }
            if (std.mem.eql(u8, job.value.phase, "failed")) {
                if (expected_job_error) |expected| {
                    // Public clients see the generated job error contract,
                    // not private durable worker checkpoint field names.
                    const failure = job.value.@"error" orelse return error.MissingRestoreJobError;
                    try std.testing.expectEqualStrings(expected, failure);
                    return;
                }
                std.debug.print("schema job failed sql={s} body={s}\n", .{ statement, observed.body });
                return error.SchemaLifecyclePublicationFailed;
            }
            try ctx.io.sleep(.fromMilliseconds(10), .awake);
        }
        return error.SchemaLifecyclePublicationTimeout;
    }
    try std.testing.expect(expected_job_error == null);
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
    failed_predecessor_digest: ?[32]u8 = null,
    primary_digest: ?[32]u8 = null,

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
        const predecessor: ?fixture.ServingSchema = if (self.case.native_job_error != null or self.case.unchanged_owner) try readyTable(ctx, self.case.table, 0) else null;
        if (predecessor != null) self.failed_predecessor_digest = try schemaFingerprint(ctx, self.case.table);
        const before_primary = if (self.case.verify_primary_identity and self.case.ddl_sqlstate != null) try primaryFingerprint(ctx, self.case.table) else null;
        try commandParameters(ctx, original.sql, if (self.case.native_job_error != null) null else self.case.ddl_sqlstate, parameters, self.case.outcome, self.case.native_job_error);
        if (before_primary) |digest| try std.testing.expectEqual(digest, try primaryFingerprint(ctx, self.case.table));
        if (predecessor) |owner|
            try std.testing.expectEqualDeep(owner, try readyTable(ctx, self.case.table, owner.version));
        if (self.failed_predecessor_digest) |digest|
            try std.testing.expectEqualDeep(digest, try schemaFingerprint(ctx, self.case.table));
        _ = try readyTable(ctx, self.case.table, 0);
        try expectRows(ctx, self.case.read_sql, self.case.after_ddl);
        if (self.case.declared_not_valid) |state|
            try expectDeclaredValidity(ctx, self.case.table, self.case.check_name.?, state);
        if (self.case.declared_not_valid orelse false)
            try expectPublicValidationRequiresRewrite(ctx, self.case.table, self.case.check_name.?);
        if (self.case.retained_not_valid_check) |name|
            try expectDeclaredValidity(ctx, self.case.table, name, true);
        for (self.case.probes) |probe| {
            try command(ctx, probe.statement, probe.sqlstate);
            if (probe.absent_table) |name| try expectAbsent(ctx, name);
            try expectRows(ctx, self.case.read_sql, probe.after);
            if (probe.declared_not_valid) |state|
                try expectDeclaredValidity(ctx, self.case.table, self.case.check_name.?, state);
        }
        self.final_owner = try readyTable(ctx, self.case.table, 0);
        try expectCoverConsumer(ctx, self.case);
        if (self.case.verify_primary_identity) self.primary_digest = try primaryFingerprint(ctx, self.case.table);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        errdefer std.debug.print("schema lifecycle id={s} after restart\n", .{self.case.id});
        try std.testing.expectEqualDeep(self.final_owner, try readyTable(ctx, self.case.table, self.final_owner.version));
        if (self.primary_digest) |digest| try std.testing.expectEqual(digest, try primaryFingerprint(ctx, self.case.table));
        if (self.failed_predecessor_digest) |digest|
            try std.testing.expectEqualDeep(digest, try schemaFingerprint(ctx, self.case.table));
        const final = self.case.probes[self.case.probes.len - 1].after;
        try expectRows(ctx, self.case.read_sql, final);
        try expectCoverConsumer(ctx, self.case);
        if (self.case.retained_not_valid_check) |name|
            try expectDeclaredValidity(ctx, self.case.table, name, true);
        if (self.case.probes[self.case.probes.len - 1].declared_not_valid) |state|
            try expectDeclaredValidity(ctx, self.case.table, self.case.check_name.?, state);
        if (self.case.ddl_sqlstate) |code| if (self.case.native_job_error == null) {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            const original = try corpus.get(self.case.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            try command(ctx, original.sql, code);
            try expectRows(ctx, self.case.read_sql, final);
        };
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
        "sql-0733", "sql-0737", "sql-0738", "sql-0739", "sql-0740", "sql-0744", "sql-0745", "sql-0779", "sql-1408",
        "sql-1412", "sql-0738", "sql-0730", "sql-0739", "sql-0739",
    };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    var completed: usize = 0;
    for (parsed.value.cases) |case| {
        if (std.mem.eql(u8, case.id, "sql-0747") or std.mem.eql(u8, case.id, "sql-0679") or std.mem.eql(u8, case.id, "sql-0732")) continue;
        if (std.mem.eql(u8, case.id, "sql-0674") or std.mem.eql(u8, case.id, "sql-0735") or std.mem.eql(u8, case.id, "sql-0726")) continue;
        if (std.mem.eql(u8, case.id, "sql-0748") or std.mem.eql(u8, case.id, "sql-0749") or std.mem.eql(u8, case.id, "sql-0750")) continue;
        if (std.mem.eql(u8, case.id, "sql-0746")) continue;
        try std.testing.expect(completed < ids.len);
        try std.testing.expectEqualStrings(ids[completed], case.id);
        try fixture.runWithState(Campaign, .{ .case = case });
        completed += 1;
    }
    try std.testing.expectEqual(ids.len, completed);
}

test "mounted SQL original nullability relaxation preserves UUID rows through lost replies and cold recovery" {
    const http = @import("../raft/transport/http_common.zig");
    const Relaxation = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        credentials: client.ApiKeyHeaders = .{},
        final_owner: fixture.ServingSchema = undefined,
        final_primary: [32]u8 = undefined,
        final_schema: [32]u8 = undefined,

        const first = "00000000-0000-0000-0000-000000000001";
        const second = "00000000-0000-0000-0000-000000000002";
        const third = "00000000-0000-0000-0000-000000000003";
        const fourth = "00000000-0000-0000-0000-000000000004";
        const probe_id = "00000000-0000-0000-0000-000000000005";
        const read_sql = "SELECT id::text AS id,status,status_upper,amount::text AS amount FROM usage_records ORDER BY id";
        const rows = [_][]const Json{
            &.{ .{ .string = first }, .{ .string = "ready" }, .{ .string = "READY" }, .{ .string = "1" } },
            &.{ .{ .string = second }, .{ .string = "open" }, .{ .string = "OPEN" }, .{ .string = "2" } },
            &.{ .{ .string = third }, .null, .null, .{ .string = "3" } },
            &.{ .{ .string = fourth }, .{ .string = "open" }, .{ .string = "OPEN" }, .{ .string = "4" } },
        };
        const nulls = [_][]const bool{ &.{ false, false, false, false }, &.{ false, false, false, false }, &.{ false, true, true, false }, &.{ false, false, false, false } };

        fn expectImage(ctx: *fixture.Context, count: usize) !void {
            try expectRows(ctx, read_sql, .{ .id = "sql-0745", .columns = &.{ "id", "status", "status_upper", "amount" }, .column_oids = &.{ 25, 25, 25, 25 }, .rows = rows[0..count], .sql_nulls = nulls[0..count], .affected = 0 });
        }
        fn reject(ctx: *fixture.Context, sql: []const u8, code: []const u8) !void {
            const before = try primaryFingerprint(ctx, "usage_records");
            const schema = try schemaFingerprint(ctx, "usage_records");
            try command(ctx, sql, code);
            try std.testing.expectEqual(before, try primaryFingerprint(ctx, "usage_records"));
            try std.testing.expectEqual(schema, try schemaFingerprint(ctx, "usage_records"));
        }
        const Disconnect = struct {
            inner: http.RequestExecutor,
            before_send: bool,
            calls: usize = 0,
            fn run(raw: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                if (self.before_send) {
                    if (request.delivery_tracker) |tracker| tracker.markNotSent();
                    return error.ConnectionRefused;
                }
                var result = try self.inner.execute(a, request);
                defer result.deinit(a);
                if (result.status != 200) std.debug.print("relaxation response before injected loss status={d} body={s}\n", .{ result.status, result.body });
                try std.testing.expectEqual(@as(u16, 200), result.status);
                var parsed = try std.json.parseFromSlice(wire.SQLResponse, a, result.body, .{});
                defer parsed.deinit();
                try std.testing.expectEqualStrings("ALTER TABLE", parsed.value.command_tag);
                if (request.delivery_tracker) |tracker| tracker.markUnknown();
                return error.ConnectionResetByPeer;
            }
        };

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activate(ctx);
            ctx.headers = ctx.admin_headers;
            // Declare the prerequisite directly. DROP must not depend on the
            // separate SET NOT NULL validation/restore job under qualification.
            try command(ctx, "CREATE TABLE usage_records(id UUID PRIMARY KEY,status TEXT NOT NULL DEFAULT 'open',status_upper TEXT GENERATED ALWAYS AS (upper(status)) STORED,amount NUMERIC CHECK(amount >= 0))", null);
            const before = try readyTable(ctx, "usage_records", 0);
            try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ first ++ "','ready',1),('" ++ second ++ "','open',2)", null);
            const primaries = try primaryFingerprint(ctx, "usage_records");
            const schema = try schemaFingerprint(ctx, "usage_records");
            try expectImage(ctx, 2);
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            const original = try corpus.get("sql-0745");
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            var readonly = ctx.*;
            readonly.headers = ctx.reader_headers;
            var denied = try client.execute(&readonly, original.sql, .never);
            defer denied.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), denied.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, denied.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("42501", diagnostic.value.code);
            try reject(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ third ++ "',NULL,3)", "23502");
            var disconnected: Disconnect = .{ .inner = ctx.transport, .before_send = true };
            var uncertain = ctx.*;
            uncertain.transport = .{ .ptr = &disconnected, .vtable = &.{ .execute = Disconnect.run } };
            try std.testing.expectError(error.ConnectionRefused, client.execute(&uncertain, original.sql, .never));
            try std.testing.expectEqual(@as(usize, 1), disconnected.calls);
            try std.testing.expectEqualDeep(before, try readyTable(ctx, "usage_records", before.version));
            try std.testing.expectEqual(schema, try schemaFingerprint(ctx, "usage_records"));
            try std.testing.expectEqual(primaries, try primaryFingerprint(ctx, "usage_records"));
            disconnected.before_send = false;
            try std.testing.expectError(error.ConnectionResetByPeer, client.execute(&uncertain, original.sql, .never));
            try std.testing.expectEqual(@as(usize, 2), disconnected.calls); // No hidden replay.
            self.final_owner = try readyTable(ctx, "usage_records", before.version + 1);
            try std.testing.expectEqual(before.table_id, self.final_owner.table_id);
            try std.testing.expectEqual(before.version + 1, self.final_owner.version);
            try std.testing.expectEqual(primaries, try primaryFingerprint(ctx, "usage_records"));
            try expectImage(ctx, 2);
            const relaxed_schema = try schemaFingerprint(ctx, "usage_records");
            try command(ctx, original.sql, null); // Now a deliberate observed no-op.
            try std.testing.expectEqualDeep(self.final_owner, try readyTable(ctx, "usage_records", self.final_owner.version));
            try std.testing.expectEqual(relaxed_schema, try schemaFingerprint(ctx, "usage_records"));
            try std.testing.expectEqual(primaries, try primaryFingerprint(ctx, "usage_records"));
            try reject(ctx, "ALTER TABLE usage_records ALTER COLUMN id DROP NOT NULL", "42P16");
            try reject(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ first ++ "',NULL,3)", "23505");
            try reject(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ third ++ "',NULL,-1)", "23514");
            try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ third ++ "',NULL,3)", null);
            try command(ctx, "INSERT INTO usage_records(id,amount) VALUES ('" ++ fourth ++ "',4)", null);
            try expectImage(ctx, 4);
            self.final_primary = try primaryFingerprint(ctx, "usage_records");
            self.final_schema = try schemaFingerprint(ctx, "usage_records");
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            ctx.headers = ctx.admin_headers;
            try std.testing.expectEqualDeep(self.final_owner, try readyTable(ctx, "usage_records", self.final_owner.version));
            try std.testing.expectEqual(self.final_primary, try primaryFingerprint(ctx, "usage_records"));
            try std.testing.expectEqual(self.final_schema, try schemaFingerprint(ctx, "usage_records"));
            try expectImage(ctx, 4);
            try reject(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ first ++ "',NULL,0)", "23505");
            try reject(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ probe_id ++ "',NULL,-1)", "23514");
            try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ probe_id ++ "',NULL,5)", null);
            try command(ctx, "DELETE FROM usage_records WHERE id='" ++ probe_id ++ "'", null);
            try std.testing.expectEqual(self.final_primary, try primaryFingerprint(ctx, "usage_records"));
            try expectImage(ctx, 4);
        }
    };
    try fixture.run(Relaxation);
}

test "mounted SQL original column rename preserves dependencies literals and cold postimages" {
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    var completed: usize = 0;
    for (parsed.value.cases) |case| {
        if (!std.mem.eql(u8, case.id, "sql-0746")) continue;
        try fixture.runWithState(Campaign, .{ .case = case });
        completed += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), completed);
}

test "mounted SQL original USING conversions transform historical rows preserve future writes and recover cold owners" {
    const ids = [_][]const u8{ "sql-0748", "sql-0749", "sql-0750" };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    var completed: usize = 0;
    for (parsed.value.cases) |case| {
        if (!std.mem.eql(u8, case.id, ids[0]) and !std.mem.eql(u8, case.id, ids[1]) and !std.mem.eql(u8, case.id, ids[2])) continue;
        try std.testing.expect(completed < ids.len);
        try std.testing.expectEqualStrings(ids[completed], case.id);
        try fixture.runWithState(Campaign, .{ .case = case });
        completed += 1;
    }
    try std.testing.expectEqual(ids.len, completed);
}

test "mounted SQL original constraint rename preserves validation and cold enforcement" {
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    for (parsed.value.cases) |case| {
        if (!std.mem.eql(u8, case.id, "sql-0747")) continue;
        try fixture.runWithState(Campaign, .{ .case = case });
        return;
    }
    return error.MissingConstraintRenameReference;
}

test "mounted SQL original covering constraints publish atomic native payloads and cold enforcement" {
    const ids = [_][]const u8{ "sql-0679", "sql-0732" };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    var completed: usize = 0;
    for (parsed.value.cases) |case| {
        if (!std.mem.eql(u8, case.id, ids[0]) and !std.mem.eql(u8, case.id, ids[1])) continue;
        try std.testing.expect(completed < ids.len);
        try std.testing.expectEqualStrings(ids[completed], case.id);
        try fixture.runWithState(Campaign, .{ .case = case });
        completed += 1;
    }
    try std.testing.expectEqual(ids.len, completed);
}

test "mounted SQL original FK schema lifecycles preserve parent references and cold owners" {
    const ids = [_][]const u8{ "sql-0674", "sql-0726", "sql-0735" };
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.schema_lifecycle_reference, .{});
    defer parsed.deinit();
    var completed: usize = 0;
    for (parsed.value.cases) |case| {
        if (!std.mem.eql(u8, case.id, "sql-0674") and !std.mem.eql(u8, case.id, "sql-0735") and !std.mem.eql(u8, case.id, "sql-0726")) continue;
        try std.testing.expect(completed < ids.len);
        try std.testing.expectEqualStrings(ids[completed], case.id);
        try fixture.runWithState(Campaign, .{ .case = case });
        completed += 1;
    }
    try std.testing.expectEqual(ids.len, completed);
}
