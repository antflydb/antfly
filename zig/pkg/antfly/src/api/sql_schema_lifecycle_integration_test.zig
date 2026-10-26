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

test "mounted SQL transaction cohort pins unseen tables document roots savepoints and rejects cold recapture" {
    const SnapshotCampaign = struct {
        pub const native_sessions = true;
        pub const cold_catalog = true;
        connection: [32]u8 = undefined,

        fn sql(ctx: *fixture.Context, id: []const u8, statement: []const u8) !void {
            var response = try connected(ctx, id, statement);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("snapshot command={s} status={d} body={s}\n", .{ statement, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            for ([_][]const u8{ "cut_left", "cut_right" }) |name| {
                const ddl = try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE {s}(id UUID PRIMARY KEY,status TEXT)", .{name});
                defer ctx.alloc.free(ddl);
                try command(ctx, ddl, null);
                _ = try readyTable(ctx, name, 0);
            }
            try command(ctx, "CREATE TABLE cut_docs () WITH (antfly.storage_mode='document')", null);
            _ = try readyTable(ctx, "cut_docs", 0);
            try command(ctx, "INSERT INTO cut_docs(_id,_doc) VALUES('doc:before','{\"status\":\"before\"}')", null);
            try command(ctx, "INSERT INTO cut_left(id,status) VALUES('00000000-0000-0000-0000-000000000001','before')", null);
            var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
            defer opened.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), opened.status);
            var parsed = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
            defer parsed.deinit();
            const id = parsed.value.connection_id;
            try std.testing.expectEqual(self.connection.len, id.len);
            @memcpy(&self.connection, id);
            try sql(ctx, id, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            // Scalar-only first query fixes even the still-unseen relations.
            try sql(ctx, id, "SELECT 1");
            try command(ctx, "INSERT INTO cut_right(id,status) VALUES('00000000-0000-0000-0000-000000000001','outside')", null);
            try command(ctx, "INSERT INTO cut_docs(_id,_doc) VALUES('doc:one','{\"status\":\"outside\"}')", null);
            try expectSessionRows(ctx, id, "SELECT id::text AS id,status FROM cut_right ORDER BY id", &.{});
            try expectSessionRows(ctx, id, "SELECT _id AS id,_doc->>'status' AS status FROM cut_docs ORDER BY _id", &.{&.{ "doc:before", "before" }});
            try expectSessionRows(ctx, id, "SELECT l.id::text AS id,r.status AS status FROM cut_left l JOIN cut_right r ON l.id=r.id", &.{});
            try sql(ctx, id, "SAVEPOINT same_cut");
            try sql(ctx, id, "UPDATE cut_left SET status='own' WHERE id='00000000-0000-0000-0000-000000000001'");
            try expectSessionRows(ctx, id, "SELECT id::text AS id,status FROM cut_left", &.{&.{ "00000000-0000-0000-0000-000000000001", "own" }});
            try expectSessionRows(ctx, null, "SELECT id::text AS id,status FROM cut_left", &.{&.{ "00000000-0000-0000-0000-000000000001", "before" }});
            try sql(ctx, id, "ROLLBACK TO same_cut");
            try expectSessionRows(ctx, id, "SELECT id::text AS id,status FROM cut_left", &.{&.{ "00000000-0000-0000-0000-000000000001", "before" }});
            try expectSessionRows(ctx, id, "SELECT id::text AS id,status FROM cut_right", &.{});
            try sql(ctx, id, "ROLLBACK");
            // A new transaction sees the outside commit and retains that cut.
            try sql(ctx, id, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            try expectSessionRows(ctx, id, "SELECT id::text AS id,status FROM cut_right", &.{&.{ "00000000-0000-0000-0000-000000000001", "outside" }});
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try readyTable(ctx, "cut_right", 0);
            var response = try connected(ctx, &self.connection, "SELECT id::text AS id,status FROM cut_right");
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 409), response.status);
            var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, transaction_status: []const u8, retryable: bool }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("40001", diagnostic.value.code);
            try std.testing.expectEqualStrings("failed", diagnostic.value.transaction_status);
            try std.testing.expect(diagnostic.value.retryable);
            try sql(ctx, &self.connection, "ROLLBACK");
            try expectSessionRows(ctx, null, "SELECT id::text AS id,status FROM cut_right", &.{&.{ "00000000-0000-0000-0000-000000000001", "outside" }});
            try expectSessionRows(ctx, null, "SELECT _id AS id,_doc->>'status' AS status FROM cut_docs ORDER BY _id", &.{ &.{ "doc:before", "before" }, &.{ "doc:one", "outside" } });
        }
    };
    try fixture.runWithState(SnapshotCampaign, .{});
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
const RelatedImage = struct { statement: []const u8, output: Rows };
const Probe = struct { preserve_owner: bool = false, related_after: []const RelatedImage = &.{}, statement: []const u8, sqlstate: ?[]const u8, after: Rows, repeat_on_reopen: bool = false, reopen: ?struct { sqlstate: ?[]const u8 = null } = null, absent_table: ?[]const u8 = null, declared_not_valid: ?bool = null };
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
    require_absent_before_ddl: bool = false,
    constraint_collection: []const u8 = "checks",
    related_after_ddl: []const RelatedImage = &.{},
    preserve_ddl_primary: bool = false,

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
    const status: u16 = if (std.mem.eql(u8, expected, "0A000")) 501 else 400;
    if (response.status != status) std.debug.print("schema rejection status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(status, response.status);
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

fn expectDeclaredValidity(ctx: *fixture.Context, table: []const u8, check_name: []const u8, not_valid: bool, collection: []const u8) !void {
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
    const checks = try fixture.field(schema.value, collection);
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
    try expectDeclaredValidity(ctx, table, check_name, true, "checks");
}

fn command(ctx: *fixture.Context, statement: []const u8, expected_error: ?[]const u8) !void {
    return commandParameters(ctx, statement, expected_error, &.{}, null, null);
}

fn awaitConstraintReceipt(ctx: *fixture.Context, receipt: wire.SQLDDLReceipt, version: u32) !void {
    // FK DDL is explicitly asynchronous. Native child coverage alone does not
    // attest the final parent coverage ACK: follow the admitted publication ID,
    // never replay the ALTER or infer readiness from a fixed delay.
    if (receipt.fk_generation_publication_id) |id| {
        const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const table_id = try std.fmt.parseInt(u64, receipt.table_id, 10);
        const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (true) {
            if (platform.time.monotonicNs() >= deadline) return error.SchemaLifecyclePublicationTimeout;
            try ctx.checkDrivers();
            const context: local.api_operation.RequestContext = .{ .deadline_ns = deadline, .fk_generation_publication_authority = true };
            const bytes = source.systemCatalog(ctx.alloc, context, .{ .fk_generation_publication_status = table_id }) catch |err| switch (err) {
                error.GenerationPublicationNotFound, error.CatalogNotFound => try source.systemCatalog(ctx.alloc, context, .{ .fk_initial_create_status = table_id }),
                else => return err,
            };
            defer ctx.alloc.free(bytes);
            var progress = try std.json.parseFromSlice(struct { phase: []const u8, plan: struct { id: [16]u8 } }, ctx.alloc, bytes, .{ .ignore_unknown_fields = true });
            defer progress.deinit();
            const observed_id = std.fmt.bytesToHex(progress.value.plan.id, .lower);
            if (!std.mem.eql(u8, id, &observed_id)) return error.GenerationPublicationChanged;
            if (std.mem.eql(u8, progress.value.phase, "published")) break;
            if (std.mem.eql(u8, progress.value.phase, "canceled")) return error.SchemaLifecyclePublicationFailed;
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
    }
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
        if (!matched) {
            const observed = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .row = row, .sql_nulls = flags }, .{});
            defer ctx.alloc.free(observed);
            const wanted = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .rows = expected.rows, .sql_nulls = expected.sql_nulls }, .{});
            defer ctx.alloc.free(wanted);
            std.debug.print("schema observer mismatch original={s} observed={s} expected={s}\n", .{ expected.id, observed, wanted });
        }
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
        if (self.case.require_absent_before_ddl) try expectAbsent(ctx, self.case.table);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.case.id);
        var parameters_arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer parameters_arena.deinit();
        const parameters = try local.sql_parity_fixtures.Corpus.logicalParameters(parameters_arena.allocator(), original);
        const predecessor: ?fixture.ServingSchema = if (self.case.native_job_error != null or self.case.unchanged_owner) try readyTable(ctx, self.case.table, 0) else null;
        if (predecessor != null) self.failed_predecessor_digest = try schemaFingerprint(ctx, self.case.table);
        const before_primary = if (self.case.verify_primary_identity and (self.case.ddl_sqlstate != null or self.case.preserve_ddl_primary)) try primaryFingerprint(ctx, self.case.table) else null;
        try commandParameters(ctx, original.sql, if (self.case.native_job_error != null) null else self.case.ddl_sqlstate, parameters, self.case.outcome, self.case.native_job_error);
        if (before_primary) |digest| try std.testing.expectEqual(digest, try primaryFingerprint(ctx, self.case.table));
        if (predecessor) |owner|
            try std.testing.expectEqualDeep(owner, try readyTable(ctx, self.case.table, owner.version));
        if (self.failed_predecessor_digest) |digest|
            try std.testing.expectEqualDeep(digest, try schemaFingerprint(ctx, self.case.table));
        _ = try readyTable(ctx, self.case.table, 0);
        try expectRows(ctx, self.case.read_sql, self.case.after_ddl);
        for (self.case.related_after_ddl) |related| try expectRows(ctx, related.statement, related.output);
        if (self.case.declared_not_valid) |state|
            try expectDeclaredValidity(ctx, self.case.table, self.case.check_name.?, state, self.case.constraint_collection);
        if ((self.case.declared_not_valid orelse false) and std.mem.eql(u8, self.case.constraint_collection, "checks"))
            try expectPublicValidationRequiresRewrite(ctx, self.case.table, self.case.check_name.?);
        if (self.case.retained_not_valid_check) |name|
            try expectDeclaredValidity(ctx, self.case.table, name, true, self.case.constraint_collection);
        for (self.case.probes) |probe| {
            errdefer std.debug.print("schema lifecycle failed probe={s}\n", .{probe.statement});
            const probe_owner = if (probe.preserve_owner) try readyTable(ctx, self.case.table, 0) else null;
            try command(ctx, probe.statement, probe.sqlstate);
            if (probe_owner) |owner| try std.testing.expectEqualDeep(owner, try readyTable(ctx, self.case.table, owner.version));
            if (probe.absent_table) |name| try expectAbsent(ctx, name);
            try expectRows(ctx, self.case.read_sql, probe.after);
            for (probe.related_after) |related| try expectRows(ctx, related.statement, related.output);
            if (probe.declared_not_valid) |state|
                try expectDeclaredValidity(ctx, self.case.table, self.case.check_name.?, state, self.case.constraint_collection);
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
        const final_related = self.case.probes[self.case.probes.len - 1].related_after;
        for (final_related) |related| try expectRows(ctx, related.statement, related.output);
        const final = self.case.probes[self.case.probes.len - 1].after;
        try expectRows(ctx, self.case.read_sql, final);
        try expectCoverConsumer(ctx, self.case);
        if (self.case.retained_not_valid_check) |name|
            try expectDeclaredValidity(ctx, self.case.table, name, true, self.case.constraint_collection);
        if (self.case.probes[self.case.probes.len - 1].declared_not_valid) |state|
            try expectDeclaredValidity(ctx, self.case.table, self.case.check_name.?, state, self.case.constraint_collection);
        if (self.case.ddl_sqlstate) |code| if (self.case.native_job_error == null) {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            const original = try corpus.get(self.case.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            try command(ctx, original.sql, code);
            try expectRows(ctx, self.case.read_sql, final);
        };
        for (self.case.probes) |probe| if (probe.sqlstate != null or probe.repeat_on_reopen or probe.reopen != null) {
            try command(ctx, probe.statement, if (probe.reopen) |expected| expected.sqlstate else probe.sqlstate);
            if (probe.preserve_owner) try std.testing.expectEqualDeep(self.final_owner, try readyTable(ctx, self.case.table, self.final_owner.version));
            if (probe.absent_table) |name| try expectAbsent(ctx, name);
            try expectRows(ctx, self.case.read_sql, final);
            for (final_related) |related| try expectRows(ctx, related.statement, related.output);
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

test "mounted SQL original NOT VALID FK preserves orphan coverage future enforcement and cold owners" {
    var parsed = try std.json.parseFromSlice(struct { format: u8, reference: []const u8, cases: []const Case }, std.testing.allocator, local.sql_parity_fixtures.fk_not_valid_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.cases.len);
    try std.testing.expectEqualStrings("sql-0734", parsed.value.cases[0].id);
    const NativeCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: Campaign,
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.beforeRestart(ctx);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.afterRestart(ctx);
        }
    };
    try fixture.runWithState(NativeCampaign, .{ .inner = .{ .case = parsed.value.cases[0] } });
}

test "mounted SQL original combined table contract preserves defaults constraints arrays UUID evolution and cold ownership" {
    var parsed = try std.json.parseFromSlice(struct {
        format: u8,
        reference: []const u8,
        typed_oids: []const u32,
        clock_checks: []const RelatedImage,
        cases: []const Case,
        restart_probes: []const Probe,
    }, std.testing.allocator, local.sql_parity_fixtures.table_contract_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.cases.len);
    try std.testing.expectEqualStrings("sql-0669", parsed.value.cases[0].id);
    const CombinedCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: Campaign,
        typed_oids: []const u32,
        clock_checks: []const RelatedImage,
        restart_probes: []const Probe,

        fn descriptors(self: *@This(), ctx: *fixture.Context) !void {
            var response = try execute(ctx, "SELECT id,tenant_id,quantity,metadata,tags,created_at,tenant_key FROM usage_records LIMIT 0", true);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqual(self.typed_oids.len, result.value.columns.len);
            for (result.value.columns, self.typed_oids) |column, oid| try std.testing.expect(compare.postgresColumnMatches(column, oid));
        }
        fn transactionDefaults(self: *@This(), ctx: *fixture.Context) !void {
            var authorized = ctx.*;
            authorized.headers = ctx.admin_headers;
            // Use the same native connection owner as the session campaigns.
            authorized.base = ctx.base;
            var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, authorized.headers, authorized.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
            defer opened.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), opened.status);
            var connection = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
            defer connection.deinit();
            const id = connection.value.connection_id;
            const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
            defer ctx.alloc.free(suffix);
            defer {
                var closed = fixture.requestWithTimeout(ctx.alloc, ctx.transport, authorized.headers, authorized.base, suffix, .DELETE, null, 15_000) catch null;
                if (closed) |*response| response.deinit(ctx.alloc);
            }
            var begun = try connected(&authorized, id, "BEGIN");
            defer begun.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), begun.status);
            for (self.clock_checks) |check| {
                errdefer std.debug.print("transaction clock failed check={s}\n", .{check.statement});
                var response = try client.executeConnected(&authorized, null, id, check.statement, .session_observation);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("clock default sql={s} status={d} body={s}\n", .{ check.statement, response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer result.deinit();
                try expectResult(ctx, result.value, if (std.mem.startsWith(u8, check.statement, "INSERT")) "INSERT" else "SELECT", check.output);
            }
            var rolled_back = try connected(&authorized, id, "ROLLBACK");
            defer rolled_back.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), rolled_back.status);
            try std.testing.expectEqual(self.inner.primary_digest.?, try primaryFingerprint(ctx, self.inner.case.table));
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            // Parent setup establishes the catalog's authoritative naming cut
            // before proving this conditional CREATE's target is absent.
            self.inner.case.require_absent_before_ddl = true;
            try self.inner.beforeRestart(ctx);
            try self.descriptors(ctx);
            try self.transactionDefaults(ctx);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.afterRestart(ctx);
            try self.descriptors(ctx);
            try self.transactionDefaults(ctx);
            for (self.restart_probes) |probe| {
                try command(ctx, probe.statement, probe.sqlstate);
                try expectRows(ctx, self.inner.case.read_sql, probe.after);
                for (probe.related_after) |related| try expectRows(ctx, related.statement, related.output);
            }
        }
    };
    try fixture.runWithState(CombinedCampaign, .{ .inner = .{ .case = parsed.value.cases[0] }, .typed_oids = parsed.value.typed_oids, .clock_checks = parsed.value.clock_checks, .restart_probes = parsed.value.restart_probes });
}

test "mounted SQL table placement and name evolution follows captured search path through cold recovery" {
    const InsertStep = struct { statement: []const u8, params: []const Json, cold: bool, command_tag: []const u8 = "", columns: []const []const u8 = &.{}, oids: []const u32 = &.{}, rows: []const []const Json = &.{}, sql_nulls: []const []const bool = &.{}, sqlstate: ?[]const u8 = null };
    const PathStep = struct { statement: []const u8, command_tag: []const u8, path: []const u8, cold: bool };
    const Golden = struct { paths: []const PathStep, implicit: []const InsertStep, format: u8, id: []const u8, sql: []const u8, create: []const u8, seed: []const u8, command_tag: []const u8, rename: []const u8, rename_tag: []const u8, read: []const u8, rows: []const []const []const u8, bound: bool, drop: []const u8, drop_tag: []const u8, absent_tag: []const u8 };
    const PlacementCampaign = struct {
        pub const native_sessions = true;
        pub const cold_catalog = true;
        pub const native_auth = true;
        golden: Golden,
        owner: fixture.ServingSchema = undefined,
        tablespace_id: u64 = 0,
        connection_id: [32]u8 = undefined,
        pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
            const users = @import("../usermgr/mod.zig");
            inline for (.{ users.ResourceType.database, users.ResourceType.namespace, users.ResourceType.tablespace }) |kind| {
                var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
                defer permission.deinit(manager.alloc);
                try manager.addPermissionToUser("pk-rewriter", permission);
            }
        }
        fn implicit(self: *@This(), ctx: *fixture.Context, cold: bool) !void {
            var authenticated = ctx.*;
            authenticated.headers = ctx.admin_headers;
            for (self.golden.implicit) |step| {
                if (step.cold != cold) continue;
                if (std.mem.startsWith(u8, step.statement, "ALTER TABLE implicit_rows ")) {
                    try command(ctx, step.statement, step.sqlstate);
                    _ = try readyTable(ctx, "implicit_rows", if (std.mem.indexOf(u8, step.statement, " ADD ") != null) 1 else 2);
                    continue;
                }
                var response = try client.executeParameters(&authenticated, step.statement, step.params, .write_admission);
                defer response.deinit(ctx.alloc);
                if (step.sqlstate) |code| {
                    try expectDiagnostic(ctx, response, code);
                    continue;
                }
                if (response.status != 200 and response.status != 202) std.debug.print("implicit INSERT sql={s} status={d} body={s}\n", .{ step.statement, response.status, response.body });
                try std.testing.expect(response.status == 200 or response.status == 202);
                var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer result.deinit();
                // HTTP keeps the command and affected count separate, while
                // PostgreSQL's wire tag appends SELECT/INSERT counts.
                const inserting = std.mem.startsWith(u8, step.command_tag, "INSERT ");
                const tag = if (inserting) "INSERT" else if (std.mem.startsWith(u8, step.command_tag, "SELECT ")) "SELECT" else step.command_tag;
                try std.testing.expectEqualStrings(tag, result.value.command_tag);
                if (inserting) {
                    const count_start = (std.mem.lastIndexOfScalar(u8, step.command_tag, ' ') orelse return error.InvalidReferenceCommand) + 1;
                    const affected = try std.fmt.parseInt(u64, step.command_tag[count_start..], 10);
                    try std.testing.expectEqual(affected, std.math.cast(u64, result.value.rows_affected) orelse return error.InvalidAffectedRows);
                }
                try std.testing.expectEqual(step.columns.len, result.value.columns.len);
                for (step.columns, step.oids, result.value.columns) |name, oid, column| {
                    try std.testing.expectEqualStrings(name, column.name);
                    try std.testing.expect(compare.postgresColumnMatches(column, oid));
                }
                try std.testing.expectEqual(step.rows.len, result.value.rows.len);
                if (step.rows.len != 0) {
                    const nulls = result.value.sql_nulls orelse return error.MissingSqlNullBitmap;
                    for (step.rows, step.sql_nulls, result.value.rows, nulls) |expected, flags, actual, actual_flags| {
                        if (!try compare.rowMatchesWithNulls(ctx.alloc, result.value.columns, actual, actual_flags, expected, flags)) {
                            std.debug.print("implicit INSERT sql={s} expected={f} actual={s}\n", .{ step.statement, std.json.fmt(expected, .{}), response.body });
                            return error.ImplicitInsertPostimageMismatch;
                        }
                    }
                }
                if (std.mem.startsWith(u8, step.statement, "CREATE TABLE public.implicit_rows")) _ = try readyTable(ctx, "implicit_rows", 0);
                if (std.mem.startsWith(u8, step.statement, "CREATE TABLE public.implicit_generated")) _ = try readyTable(ctx, "implicit_generated", 0);
                if (std.mem.startsWith(u8, step.statement, "ALTER TABLE implicit_rows ADD")) _ = try readyTable(ctx, "implicit_rows", 1);
                if (std.mem.startsWith(u8, step.statement, "ALTER TABLE implicit_rows RENAME")) _ = try readyTable(ctx, "implicit_rows", 2);
            }
        }
        fn paths(self: *@This(), ctx: *fixture.Context, cold: bool) !void {
            for (self.golden.paths) |step| {
                if (step.cold != cold) continue;
                try sql(ctx, &self.connection_id, step.statement, step.command_tag);
                var authenticated = ctx.*;
                authenticated.headers = ctx.admin_headers;
                var response = try client.executeConnected(&authenticated, null, &self.connection_id, "SHOW search_path", .session_observation);
                defer response.deinit(ctx.alloc);
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer result.deinit();
                try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
                try std.testing.expectEqualStrings(step.path, result.value.rows[0][0].string);
            }
        }
        fn open(ctx: *fixture.Context) !std.json.Parsed(wire.SQLConnectionResponse) {
            var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("DDL connection status={d} body={s}\n", .{ response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            return std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, response.body, .{ .allocate = .alloc_always });
        }
        fn sql(ctx: *fixture.Context, id: []const u8, statement: []const u8, tag: []const u8) !void {
            var authenticated = ctx.*;
            authenticated.headers = ctx.admin_headers;
            var response = try client.executeConnected(&authenticated, null, id, statement, .write_admission);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("DDL path sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(tag, parsed.value.command_tag);
        }
        fn verify(self: *@This(), ctx: *fixture.Context) !void {
            var state = try TablespaceCampaign.snapshot(ctx);
            defer state.deinit();
            const table = state.value.byId(.table, self.owner.table_id) orelse return error.MissingSqlTable;
            try std.testing.expectEqualStrings("renamed", table.name);
            try std.testing.expectEqual(self.tablespace_id, table.tablespace_id);
            try std.testing.expectEqualDeep(self.owner, try readyTable(ctx, "renamed", self.owner.version));
            try expectSessionRows(ctx, null, self.golden.read, self.golden.rows);
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            const original = try corpus.get(self.golden.id);
            try std.testing.expectEqualStrings(original.sql, self.golden.sql);
            try std.testing.expect(self.golden.bound);
            try self.implicit(ctx, false);
            // RESTRICT must reject dependencies before fencing the table.
            // Its constrained rows/schema must remain writable after recovery.
            try command(ctx, "CREATE VIEW implicit_dependency AS SELECT z,a FROM implicit_rows", null);
            {
                var authenticated = ctx.*;
                authenticated.headers = ctx.admin_headers;
                var rejected = try client.executeConnected(&authenticated, null, null, "DROP TABLE implicit_rows", .write_admission);
                defer rejected.deinit(ctx.alloc);
                try expectDiagnostic(ctx, rejected, "2BP01");
            }
            try command(ctx, "DROP VIEW implicit_dependency", null);
            try command(ctx, "CREATE SCHEMA empty_ns", null);
            try command(ctx, "CREATE TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace'", null);
            try command(ctx, self.golden.create, null);
            self.owner = try readyTable(ctx, "usage_records", 0);
            try command(ctx, self.golden.seed, null);
            {
                var state = try TablespaceCampaign.snapshot(ctx);
                defer state.deinit();
                self.tablespace_id = (state.value.find(.tablespace, 0, "fastspace") orelse return error.MissingSqlTablespace).id;
            }
            // A restored metadata incarnation can reuse a revision number.
            // Exercise the exact native ownership guard rather than relying
            // only on a global revision or a forwarding mock.
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .name = "usage_records" }} };
            const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
            defer ctx.alloc.free(bytes);
            var bound = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, bytes, .{});
            defer bound.deinit();
            try bound.value.validateRelations(lookup);
            const owner = bound.value.relations[0].?;
            const guard: local.system_catalog_domain.RelationMutationGuard = .{ .target = lookup.relations[0], .logical_table = owner.logical_table, .owner = owner.owner, .incarnation = bound.value.relation_epoch.?.incarnation };
            var stale = guard;
            stale.incarnation[0] ^= 1;
            {
                var before = try TablespaceCampaign.snapshot(ctx);
                defer before.deinit();
                try std.testing.expectError(error.CatalogGenerationChanged, source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .mutate = .{
                    .mutation = .{ .kind = .table, .action = .drop, .name = "usage_records" },
                    .binding_guard = stale,
                } }));
                var after = try TablespaceCampaign.snapshot(ctx);
                defer after.deinit();
                try std.testing.expectEqual(before.value.revision, after.value.revision);
                try std.testing.expect(after.value.byId(.table, self.owner.table_id) != null);
            }
            try command(ctx, "CREATE SCHEMA intervening", null);
            var connection = try open(ctx);
            defer connection.deinit();
            const id = connection.value.connection_id;
            @memcpy(&self.connection_id, id);
            try sql(ctx, id, "SET search_path=empty_ns,public", "SET");
            try sql(ctx, id, original.sql, self.golden.command_tag);
            _ = try readyTable(ctx, "usage_records", self.owner.version);
            const observed = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .mutate = .{
                .mutation = .{ .kind = .table, .action = .set_tablespace, .name = "usage_records", .tablespace = "fastspace" },
                .binding_guard = guard,
            } });
            ctx.alloc.free(observed);
            try sql(ctx, id, self.golden.rename, self.golden.rename_tag);
            try self.verify(ctx);
            // A missing first path entry must never become an admission into
            // that namespace or change the SQL table's physical identity.
            try sql(ctx, id, "ALTER TABLE renamed SET TABLESPACE fastspace", self.golden.command_tag);
            try self.verify(ctx);
            var authenticated = ctx.*;
            authenticated.headers = ctx.admin_headers;
            try expectSessionRows(&authenticated, id, self.golden.read, self.golden.rows);
            try sql(ctx, id, "RESET search_path", "RESET");
            try self.paths(ctx, false);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.verify(ctx);
            try self.paths(ctx, true);
            const id = &self.connection_id;
            var authenticated = ctx.*;
            authenticated.headers = ctx.admin_headers;
            try expectSessionRows(&authenticated, id, self.golden.read, self.golden.rows);
            try sql(ctx, id, self.golden.drop, self.golden.drop_tag);
            try sql(ctx, id, "DROP TABLE IF EXISTS renamed", self.golden.absent_tag);
            try expectAbsent(ctx, "renamed");
            try command(ctx, "DROP TABLESPACE fastspace", null);
            // Finish the placement lifecycle before starting independent
            // schema-rewrite jobs. Their publication must not race this
            // campaign's single-command catalog outcome observation.
            try self.implicit(ctx, true);
        }
    };
    var parsed = try std.json.parseFromSlice(Golden, std.testing.allocator, local.sql_parity_fixtures.table_placement_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("sql-1202", parsed.value.id);
    try fixture.runWithState(PlacementCampaign, .{ .golden = parsed.value });
}

test "mounted SQL constrained table drops drain outgoing and self references preserve RESTRICT and recover names" {
    const DropCampaign = struct {
        pub const cold_catalog = true;
        pub const native_auth = true;
        pub const native_sessions = true;
        old_child: u64 = 0,
        old_parent: u64 = 0,
        pending_child: u64 = 0,
        pending_plan: [16]u8 = @splat(0),
        lost_final_reply: bool = false,
        credentials: client.ApiKeyHeaders = .{},
        pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
            const users = @import("../usermgr/mod.zig");
            for ([_]users.ResourceType{ .database, .namespace }) |kind| {
                var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
                defer permission.deinit(manager.alloc);
                try manager.addPermissionToUser("pk-rewriter", permission);
            }
        }
        fn scoped(ctx: *fixture.Context, statement: []const u8) !@import("../raft/transport/http_common.zig").HttpResponse {
            const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .database = "retirement_db", .namespace = "retirement_ns" }, .{});
            defer ctx.alloc.free(body);
            return fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/sql", .POST, body, 15_000);
        }
        fn scopedReady(ctx: *fixture.Context, name: []const u8, version: u32) !fixture.ServingSchema {
            const owner = try fixture.waitServingSchemaInScope(ctx, "retirement_db", "retirement_ns", name, version);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
            return owner;
        }
        fn scopedCommand(ctx: *fixture.Context, statement: []const u8, expected: ?[]const u8) !void {
            var response = try scoped(ctx, statement);
            defer response.deinit(ctx.alloc);
            if (expected) |code| return expectDiagnostic(ctx, response, code);
            errdefer std.debug.print("scoped DROP SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
            try std.testing.expect(response.status == 200 or response.status == 202);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            if (result.value.ddl_receipt) |receipt| {
                try std.testing.expect(receipt.state == .ready or receipt.state == .pending);
                if (!std.mem.startsWith(u8, statement, "DROP TABLE")) {
                    const version = std.math.cast(u32, receipt.schema_version) orelse return error.InvalidSchemaReceipt;
                    const owner = try scopedReady(ctx, receipt.table, version);
                    try std.testing.expectEqual(try std.fmt.parseInt(u64, receipt.table_id, 10), owner.table_id);
                    if (receipt.state != .ready) try awaitConstraintReceipt(ctx, receipt, version);
                }
            }
        }
        fn scopedGone(ctx: *fixture.Context, name: []const u8) !void {
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve = .{ .database = "retirement_db", .namespace = "retirement_ns", .table = name } });
                defer ctx.alloc.free(bytes);
                var resolved = try std.json.parseFromSlice(?local.system_catalog_domain.ResolvedTable, ctx.alloc, bytes, .{});
                defer resolved.deinit();
                if (resolved.value == null) return;
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
            return error.SqlDropRetirementTimeout;
        }
        fn reject(ctx: *fixture.Context, statement: []const u8) !void {
            var response = try execute(ctx, statement, false);
            defer response.deinit(ctx.alloc);
            try expectDiagnostic(ctx, response, "2BP01");
        }
        fn drop(ctx: *fixture.Context, name: []const u8) !void {
            const statement = try std.fmt.allocPrint(ctx.alloc, "DROP TABLE {s}", .{name});
            defer ctx.alloc.free(statement);
            var response = try execute(ctx, statement, false);
            defer response.deinit(ctx.alloc);
            if (response.status != 200 and response.status != 202) std.debug.print("constrained DROP sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
            try std.testing.expect(response.status == 200 or response.status == 202);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(if (response.status == 200) "DROP TABLE" else "DDL PENDING", parsed.value.command_tag);
            if (parsed.value.ddl_receipt) |receipt| {
                try std.testing.expect(receipt.state == .ready or receipt.state == .pending);
                try std.testing.expectEqualStrings(name, receipt.table);
            }
            try awaitGone(ctx, name);
        }
        fn awaitGone(ctx: *fixture.Context, name: []const u8) !void {
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve = .{ .table = name } });
                defer ctx.alloc.free(bytes);
                var resolved = try std.json.parseFromSlice(?local.system_catalog_domain.ResolvedTable, ctx.alloc, bytes, .{});
                defer resolved.deinit();
                if (resolved.value == null) return;
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
            return error.SqlDropRetirementTimeout;
        }
        fn count(ctx: *fixture.Context, table: []const u8, expected: []const u8) !void {
            const statement = try std.fmt.allocPrint(ctx.alloc, "SELECT count(*)::text AS n FROM {s}", .{table});
            defer ctx.alloc.free(statement);
            var response = try execute(ctx, statement, true);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
            try std.testing.expectEqualStrings(expected, parsed.value.rows[0][0].string);
        }
        fn publication(ctx: *fixture.Context, id: u64) !std.json.Parsed(@import("../metadata/fk_generation_publication.zig").Publication) {
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s, .fk_generation_publication_authority = true }, .{ .fk_generation_publication_status = id });
            defer ctx.alloc.free(bytes);
            return std.json.parseFromSlice(@import("../metadata/fk_generation_publication.zig").Publication, ctx.alloc, bytes, .{ .allocate = .alloc_always });
        }
        fn enablePolicy(ctx: *fixture.Context, name: []const u8, table_id: u64, expression: []const u8) !void {
            const create = try std.fmt.allocPrint(ctx.alloc, "CREATE POLICY retirement_policy ON {s} USING ({s})", .{ name, expression });
            defer ctx.alloc.free(create);
            const enable = try std.fmt.allocPrint(ctx.alloc, "ALTER TABLE {s} ENABLE ROW LEVEL SECURITY", .{name});
            defer ctx.alloc.free(enable);
            for ([_][]const u8{ create, enable }) |statement| {
                var response = try execute(ctx, statement, false);
                defer response.deinit(ctx.alloc);
                try std.testing.expect(response.status == 200 or response.status == 202);
            }
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
            while (true) {
                try ctx.checkDrivers();
                const bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .row_policy_install_authority = true, .deadline_ns = deadline }, .{ .policy_publication_status = table_id });
                defer ctx.alloc.free(bytes);
                var state = try std.json.parseFromSlice(?local.system_catalog_policies.PublicationStamp, ctx.alloc, bytes, .{});
                defer state.deinit();
                if (state.value) |value| if (value.phase == .active) break;
                if (platform.time.monotonicNs() >= deadline) return error.PolicyPublicationTimeout;
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
        }
        fn policyTable(ctx: *fixture.Context) !void {
            try command(ctx, "CREATE TABLE drop_policy(id BIGINT PRIMARY KEY)", null);
            const table = try readyTable(ctx, "drop_policy", 0);
            try command(ctx, "INSERT INTO drop_policy VALUES(1)", null);
            try enablePolicy(ctx, "drop_policy", table.table_id, "false");
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            try drop(ctx, "drop_policy");
            const bytes = try source.systemCatalog(ctx.alloc, .{ .setting_admin = true, .row_policy_install_authority = true }, .{ .policy_publication_status = table.table_id });
            defer ctx.alloc.free(bytes);
            var state = try std.json.parseFromSlice(?local.system_catalog_policies.PublicationStamp, ctx.alloc, bytes, .{});
            defer state.deinit();
            try std.testing.expect(state.value == null);
        }
        fn pauseAfterParentAck(self: *@This(), ctx: *fixture.Context) !void {
            const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
            const driver = @import("http_server.zig").ApiHttpServer.FkGenerationPublicationTestDriver;
            try driver.pauseBackground(api, ctx.io);
            errdefer driver.resumeBackground(api);
            var response = try scoped(ctx, "DROP TABLE recovery_child");
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 202), response.status);
            var reply = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer reply.deinit();
            const token = reply.value.ddl_receipt.?.fk_generation_publication_id orelse return error.MissingSqlDropToken;
            // An admitted deletion owns both the logical name and dependency
            // admission. Neither rename nor a new view may strand its final CAS.
            try scopedCommand(ctx, "ALTER TABLE recovery_child RENAME TO stolen_child", "55006");
            try scopedCommand(ctx, "CREATE VIEW late_dependency AS SELECT id FROM recovery_child", "55006");
            try scopedCommand(ctx, "ALTER SCHEMA retirement_ns RENAME TO stolen_namespace", "55006");
            try command(ctx, "ALTER DATABASE retirement_db RENAME TO stolen_database", "55006");
            const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < deadline) {
                try ctx.checkDrivers();
                var state = try publication(ctx, self.pending_child);
                defer state.deinit();
                self.pending_plan = state.value.plan.id;
                try std.testing.expectEqualStrings(token, &std.fmt.bytesToHex(self.pending_plan, .lower));
                const interrupt_phase: @import("../metadata/fk_generation_publication.zig").Phase = if (self.lost_final_reply) .publishing_child else .acknowledging_parents;
                if (state.value.phase == interrupt_phase) {
                    driver.forgetVolatileCursor(api);
                    try std.testing.expectError(error.InjectedPublicationReplyLoss, driver.step(api, .after_metadata_mutate));
                    var lost = try publication(ctx, self.pending_child);
                    defer lost.deinit();
                    try std.testing.expectEqual(if (self.lost_final_reply) @import("../metadata/fk_generation_publication.zig").Phase.published else .acknowledging_reference_coverage, lost.value.phase);
                    if (self.lost_final_reply) {
                        try scopedGone(ctx, "recovery_child");
                    } else {
                        // The early parent ACK is durable. Cold recovery must
                        // clear its coverage barrier before catalog deletion.
                        try std.testing.expectEqual(self.pending_child, (try scopedReady(ctx, "recovery_child", 0)).table_id);
                    }
                    return;
                }
                driver.forgetVolatileCursor(api);
                driver.step(api, .none) catch |err| switch (err) {
                    error.GenerationAdmissionPending, error.ReadIndexTimeout, error.CatalogRoutingSnapshotTimeout, error.StorageReadTemporarilyUnavailable => {},
                    else => return err,
                };
                try ctx.io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.SqlDropFaultBoundaryTimeout;
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activateWithCatalog(ctx, true);
            try command(ctx, "CREATE TABLE drop_parent(id BIGINT PRIMARY KEY)", null);
            self.old_parent = (try readyTable(ctx, "drop_parent", 0)).table_id;
            try command(ctx, "CREATE TABLE drop_child(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_parent(id),email TEXT UNIQUE)", null);
            self.old_child = (try readyTable(ctx, "drop_child", 0)).table_id;
            try command(ctx, "INSERT INTO drop_parent VALUES(1),(2)", null);
            try command(ctx, "INSERT INTO drop_child VALUES(1,1,'a'),(2,2,'b')", null);
            {
                var reader = ctx.*;
                reader.base = ctx.metadata_uri;
                reader.headers = ctx.reader_headers;
                var denied = try client.execute(&reader, "DROP TABLE drop_child", .write_admission);
                defer denied.deinit(ctx.alloc);
                try std.testing.expectEqual(@as(u16, 403), denied.status);
                try std.testing.expectEqual(self.old_child, (try readyTable(ctx, "drop_child", 0)).table_id);
            }
            try reject(ctx, "DROP TABLE drop_parent");
            try command(ctx, "CREATE VIEW drop_child_view AS SELECT id,email FROM drop_child", null);
            try reject(ctx, "DROP TABLE drop_child");
            // Both dependency rejections precede fencing: normal writes still work.
            try command(ctx, "INSERT INTO drop_child VALUES(3,1,'c')", null);
            try command(ctx, "DROP VIEW drop_child_view", null);
            {
                const users = @import("../usermgr/mod.zig");
                const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
                const scope = try (local.system_catalog_domain.Target{ .table = "drop_child" }).resourceNameAlloc(ctx.alloc);
                defer ctx.alloc.free(scope);
                var permission = try users.Permission.initOwned(ctx.alloc, .table, scope, .admin);
                defer permission.deinit(ctx.alloc);
                var key = try manager.createApiKey("pk-rewriter", "sql-drop-child-only", &.{permission}, &.{}, null);
                defer key.deinit(manager.alloc);
                const authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{key.encoded});
                defer ctx.alloc.free(authorization);
                const headers = [_]@import("../raft/transport/http_common.zig").RequestHeader{.{ .name = "authorization", .value = authorization }};
                var child_owner = ctx.*;
                child_owner.admin_headers = &headers;
                var denied_parent = try execute(&child_owner, "DROP TABLE drop_parent", false);
                defer denied_parent.deinit(ctx.alloc);
                try std.testing.expectEqual(@as(u16, 403), denied_parent.status);
                try drop(&child_owner, "drop_child");
            }
            try count(ctx, "drop_parent", "2");
            // Deleting a child must drain its inverse parent witnesses, so the
            // surviving parent is immediately usable and subsequently droppable.
            try command(ctx, "DELETE FROM drop_parent WHERE id=1", null);
            try drop(ctx, "drop_parent");
            try command(ctx, "CREATE TABLE drop_self(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_self(id))", null);
            _ = try readyTable(ctx, "drop_self", 0);
            try command(ctx, "INSERT INTO drop_self VALUES(1,NULL),(2,1)", null);
            try drop(ctx, "drop_self");
            try command(ctx, "CREATE TABLE mixed_parent(id BIGINT PRIMARY KEY)", null);
            const mixed_parent = try readyTable(ctx, "mixed_parent", 0);
            try command(ctx, "CREATE TABLE drop_mixed(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_mixed(id),external_parent BIGINT REFERENCES mixed_parent(id))", null);
            _ = try readyTable(ctx, "drop_mixed", 0);
            try command(ctx, "INSERT INTO mixed_parent VALUES(1)", null);
            try command(ctx, "INSERT INTO drop_mixed VALUES(1,NULL,1),(2,1,1)", null);
            try enablePolicy(ctx, "mixed_parent", mixed_parent.table_id, "true");
            try drop(ctx, "drop_mixed");
            try command(ctx, "DELETE FROM mixed_parent WHERE id=1", null);
            try drop(ctx, "mixed_parent");
            try policyTable(ctx);
            try command(ctx, "CREATE DATABASE retirement_db", null);
            try scopedCommand(ctx, "CREATE SCHEMA retirement_ns", null);
            try scopedCommand(ctx, "CREATE TABLE recovery_parent(id BIGINT PRIMARY KEY)", null);
            _ = try scopedReady(ctx, "recovery_parent", 0);
            try scopedCommand(ctx, "CREATE TABLE recovery_child(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES recovery_parent(id))", null);
            self.pending_child = (try scopedReady(ctx, "recovery_child", 0)).table_id;
            try scopedCommand(ctx, "INSERT INTO recovery_parent VALUES(1)", null);
            var seed: std.Io.Writer.Allocating = .init(ctx.alloc);
            defer seed.deinit();
            try seed.writer.writeAll("INSERT INTO recovery_child VALUES");
            for (0..@as(usize, if (self.lost_final_reply) 128 else 1)) |i|
                try seed.writer.print("{s}({d},1)", .{ if (i == 0) "" else ",", i + 1 });
            try scopedCommand(ctx, seed.written(), null);
            try self.pauseAfterParentAck(ctx);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activateWithCatalog(ctx, true);
            try scopedGone(ctx, "recovery_child");
            var recovered = try publication(ctx, self.pending_child);
            defer recovered.deinit();
            try std.testing.expectEqualSlices(u8, &self.pending_plan, &recovered.value.plan.id);
            try std.testing.expectEqual(@import("../metadata/fk_generation_publication.zig").Phase.published, recovered.value.phase);
            // One source fence, four parent receipts and the final publication:
            // the same seven durable revisions for one or 128 real child rows.
            try std.testing.expectEqual(@as(u64, 7), recovered.value.revision);
            try scopedCommand(ctx, "DELETE FROM recovery_parent WHERE id=1", null);
            try scopedCommand(ctx, "DROP TABLE recovery_parent", null);
            try scopedGone(ctx, "recovery_parent");
            // The same legitimate ancestor renames succeed once the exact
            // deletion intent has completed and released its lifecycle locks.
            try scopedCommand(ctx, "ALTER SCHEMA retirement_ns RENAME TO retired_namespace", null);
            try command(ctx, "ALTER DATABASE retirement_db RENAME TO retired_database", null);
            inline for (.{ "drop_child", "drop_parent", "drop_self" }) |name| try expectAbsent(ctx, name);
            try command(ctx, "CREATE TABLE drop_parent(id BIGINT PRIMARY KEY)", null);
            const parent = try readyTable(ctx, "drop_parent", 0);
            try std.testing.expect(parent.table_id != self.old_parent);
            try command(ctx, "CREATE TABLE drop_child(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES drop_parent(id),email TEXT UNIQUE)", null);
            const child = try readyTable(ctx, "drop_child", 0);
            try std.testing.expect(child.table_id != self.old_child);
            // Old generation claims cannot collide with fresh logical name reuse.
            try command(ctx, "INSERT INTO drop_parent VALUES(1)", null);
            try command(ctx, "INSERT INTO drop_child VALUES(1,1,'a')", null);
            try count(ctx, "drop_child", "1");
        }
    };
    for ([_]bool{ false, true }) |lost_final_reply|
        try fixture.runWithState(DropCampaign, .{ .lost_final_reply = lost_final_reply });
}
