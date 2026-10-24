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

//! Document MERGE uses real native sessions, owner range proofs and Raft,
//! rather than downgrading serializable admission for a bound-write fixture.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;
const Seed = struct { key: []const u8, value: Json };
const Entry = struct {
    id: []const u8,
    requires_native_session: bool = false,
    schema: Json,
    command_tag: []const u8,
    affected: i64,
    sqlstate: ?[]const u8,
    untouched: []const []const u8 = &.{},
    final: []const Seed,
    seeds: ?[]const Seed = null,
    occupied: ?bool = null,
    columns: []const []const u8 = &.{},
    column_oids: []const u32 = &.{},
    rows: []const []const Json = &.{},
};
const Golden = struct { seed_version: u64, seeds: []const Seed = &.{}, entries: []const Entry };

const SourceResult = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json };
pub const SourceTable = struct { name: []const u8, create: []const u8, seed: []const u8, read: []const u8, final: SourceResult };
const SourceEntry = struct { id: []const u8, profile: []const u8, prepared_name: ?[]const u8 = null, sqlstate: ?[]const u8, command_tag: []const u8, affected: i64, output: SourceResult, tables: []const SourceTable };
const Fingerprint = struct { table: []const u8, key: []const u8, version: u64, digest: [32]u8 };

fn expectSourceResult(a: std.mem.Allocator, actual: wire.SQLResponse, expected: SourceResult, ordered: bool) !void {
    try std.testing.expectEqual(expected.columns.len, actual.columns.len);
    for (actual.columns, expected.columns, expected.column_oids) |column, name, oid| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, actual.rows.len);
    const nulls = actual.sql_nulls orelse if (actual.rows.len == 0) &.{} else return error.MissingSqlNullBitmap;
    try std.testing.expectEqual(actual.rows.len, nulls.len);
    const used = try a.alloc(bool, expected.rows.len);
    defer a.free(used);
    @memset(used, false);
    for (actual.rows, nulls, 0..) |row, flags, ordinal| {
        var found = false;
        for (expected.rows, 0..) |wanted, index| {
            if (used[index] or (ordered and index != ordinal)) continue;
            const expected_nulls = try a.alloc(bool, wanted.len);
            defer a.free(expected_nulls);
            for (wanted, expected_nulls) |cell, *flag| flag.* = cell == .null;
            if (try compare.rowMatchesWithNulls(a, actual.columns, row, flags, wanted, expected_nulls)) {
                used[index] = true;
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

pub const SourceCampaign = struct {
    pub const native_sessions = true;
    entry: SourceEntry,
    retained: *std.heap.ArenaAllocator,
    cold_fingerprints: std.ArrayList(Fingerprint) = .empty,
    initial_fingerprints: std.ArrayList(Fingerprint) = .empty,
    credentials: client.ApiKeyHeaders = .{},

    fn resolve(ctx: *fixture.Context, name: []const u8) ![]u8 {
        const domain = local.system_catalog_domain;
        const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = name }}, .include_query_definitions = true };
        const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
        defer ctx.alloc.free(bytes);
        var result = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer result.deinit();
        try result.value.validateRelations(lookup);
        return ctx.alloc.dupe(u8, (result.value.relations[0] orelse return error.MissingSourceRelation).table.name);
    }

    pub fn fingerprints(self: *@This(), ctx: *fixture.Context, stage: enum { initial, prepared, mutated, cold }) !void {
        const cold = stage == .cold;
        const initial = stage == .initial;
        const compare_initial = stage == .prepared or (stage == .mutated and self.entry.sqlstate != null);
        const a = self.retained.allocator();
        var observed: usize = 0;
        for (self.entry.tables) |table| {
            const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT _id FROM {s} ORDER BY _id", .{table.name});
            defer ctx.alloc.free(sql);
            var response = try client.execute(ctx, sql, .read);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            const physical = try resolve(ctx, table.name);
            defer ctx.alloc.free(physical);
            for (parsed.value.rows) |row| {
                const key = row[0].string;
                var primary = (try ctx.data.http_server.?.table_reads.?.lookup(ctx.alloc, physical, key, .{ .include_primary_digest = true }, .read_index)) orelse return error.MissingMutationPrimary;
                defer primary.deinit(ctx.alloc);
                const digest = primary.expected_content_digest orelse return error.MissingMutationDigest;
                const current: Fingerprint = .{ .table = table.name, .key = key, .version = primary.version, .digest = digest };
                const reference = if (cold) self.cold_fingerprints.items else if (compare_initial) self.initial_fingerprints.items else &.{};
                if (reference.len != 0) {
                    const before = for (reference) |item| {
                        if (std.mem.eql(u8, item.table, table.name) and std.mem.eql(u8, item.key, key)) break item;
                    } else return error.UnexpectedMutationPrimary;
                    try std.testing.expectEqual(before.version, current.version);
                    try std.testing.expectEqual(before.digest, current.digest);
                }
                if (stage == .initial or stage == .mutated) {
                    var owned = current;
                    owned.key = try a.dupe(u8, key);
                    if (initial) try self.initial_fingerprints.append(a, owned) else try self.cold_fingerprints.append(a, owned);
                }
                observed += 1;
            }
        }
        if (cold) try std.testing.expectEqual(self.cold_fingerprints.items.len, observed);
        if (compare_initial) try std.testing.expectEqual(self.initial_fingerprints.items.len, observed);
    }

    pub fn verifyRows(self: *@This(), ctx: *fixture.Context) !void {
        for (self.entry.tables) |table| {
            var response = try client.execute(ctx, table.read, .read);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("source mutation read original={s} table={s} status={d} body={s}\n", .{ self.entry.id, table.name, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try expectSourceResult(ctx.alloc, result.value, table.final, true);
        }
    }

    pub fn verify(self: *@This(), ctx: *fixture.Context, cold: bool) !void {
        try self.verifyRows(ctx);
        try self.fingerprints(ctx, if (cold) .cold else .mutated);
    }

    fn probe(ctx: *fixture.Context, sql: []const u8, code: ?[]const u8) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (code) |expected| {
            try std.testing.expectEqual(@as(u16, 400), response.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(expected, diagnostic.value.code);
        } else {
            if (response.status != 200) std.debug.print("source mutation ownership probe sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 1), result.value.rows_affected);
        }
    }

    /// Final row bytes alone cannot prove that a key retirement or an aborted
    /// recursive batch released every unique claim. Exercise future writes,
    /// then restore the exact original postimage before measuring cold state.
    fn verifyClaims(self: *@This(), ctx: *fixture.Context) !void {
        try self.verifyRows(ctx);
        if (std.mem.eql(u8, self.entry.id, "sql-0595")) {
            try probe(ctx, "INSERT INTO usage_records(id,status) VALUES ('u2','collision')", "23505");
            if (self.entry.sqlstate == null) {
                try probe(ctx, "INSERT INTO usage_records(id,status) VALUES ('u1','released')", null);
                try probe(ctx, "DELETE FROM usage_records WHERE id='u1'", null);
            } else try probe(ctx, "INSERT INTO usage_records(id,status) VALUES ('u1','collision')", "23505");
        } else if (std.mem.eql(u8, self.entry.id, "sql-0011") or std.mem.eql(u8, self.entry.id, "sql-0005")) {
            if (self.entry.sqlstate == null) {
                try probe(ctx, "INSERT INTO archived_records(id) VALUES ('root')", "23505");
            } else {
                try probe(ctx, "INSERT INTO archived_records(id) VALUES ('root')", null);
                try probe(ctx, "DELETE FROM archived_records WHERE id='root'", null);
            }
        } else if (std.mem.eql(u8, self.entry.id, "sql-0580") or std.mem.eql(u8, self.entry.id, "sql-0586")) {
            try probe(ctx, "INSERT INTO usage_records(id,status) VALUES ('u3','collision')", "23505");
        } else if (std.mem.eql(u8, self.entry.id, "sql-0591")) {
            try probe(ctx, "INSERT INTO prices(sku,status) VALUES ('a','collision')", "23505");
        } else if (std.mem.eql(u8, self.entry.id, "sql-0008")) {
            try probe(ctx, "INSERT INTO usage_records(id,status) VALUES ('u1','collision')", "23505");
        } else if (std.mem.eql(u8, self.entry.id, "sql-0009") or std.mem.eql(u8, self.entry.id, "sql-0010")) {
            try probe(ctx, "INSERT INTO usage_records(id,status) VALUES ('root','collision')", "23505");
        }
    }

    pub fn setup(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        for (self.entry.tables) |table| {
            var created = try client.execute(ctx, table.create, .write_admission);
            defer created.deinit(ctx.alloc);
            if (created.status != 200 and created.status != 202) std.debug.print("source mutation create status={d} body={s}\n", .{ created.status, created.body });
            try std.testing.expect(created.status == 200 or created.status == 202);
            const serving = try fixture.waitServingSchema(ctx, table.name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, table.name);
            var seeded = try client.execute(ctx, table.seed, .write_admission);
            defer seeded.deinit(ctx.alloc);
            if (seeded.status != 200) std.debug.print("source mutation seed status={d} body={s}\n", .{ seeded.status, seeded.body });
            try std.testing.expectEqual(@as(u16, 200), seeded.status);
        }
        try self.fingerprints(ctx, .initial);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.setup(ctx);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.entry.id);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        if (self.entry.prepared_name) |name| {
            const prepared = @import("sql_prepared_source_test_client.zig");
            try prepared.run(ctx, original.sql, name, self.entry, true);
            try self.fingerprints(ctx, .prepared);
            try prepared.run(ctx, original.sql, name, self.entry, false);
            try self.verifyClaims(ctx);
            try self.verify(ctx, false);
            return;
        }
        var response = try client.execute(ctx, original.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (self.entry.sqlstate) |code| {
            if (response.status != 400) std.debug.print("source mutation reject original={s} status={d} body={s}\n", .{ self.entry.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 400), response.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(code, diagnostic.value.code);
        } else {
            if (response.status != 200) std.debug.print("source mutation original={s} status={d} body={s}\n", .{ self.entry.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings(self.entry.command_tag, result.value.command_tag);
            try std.testing.expectEqual(self.entry.affected, result.value.rows_affected);
            try expectSourceResult(ctx.alloc, result.value, self.entry.output, false);
        }
        try self.verifyClaims(ctx);
        try self.verify(ctx, false);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.entry.tables) |table| _ = try fixture.waitServingSchema(ctx, table.name, 0);
        try self.verify(ctx, true);
        try self.verifyClaims(ctx);
        try self.verify(ctx, true);
    }
};

test "mounted SQL original source mutations preserve constrained postimages errors defaults and cold identities" {
    const ids = [_][]const u8{ "sql-0011", "sql-0014", "sql-0570", "sql-0580", "sql-0586", "sql-0591", "sql-0595" };
    const GoldenSources = struct { format: u8, reference: []const u8, entries: []const SourceEntry };
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(GoldenSources, a, local.sql_parity_fixtures.source_mutation_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    var completed: usize = 0;
    for (ids) |id| {
        var witnesses: usize = 0;
        for (parsed.value.entries) |entry| {
            if (!std.mem.eql(u8, id, entry.id)) continue;
            errdefer std.debug.print("source mutation original={s} profile={s}\n", .{ id, entry.profile });
            var retained = std.heap.ArenaAllocator.init(a);
            defer retained.deinit();
            try fixture.runWithState(SourceCampaign, .{ .entry = entry, .retained = &retained });
            witnesses += 1;
            completed += 1;
        }
        try std.testing.expectEqual(@as(usize, if (std.mem.eql(u8, id, "sql-0011") or std.mem.eql(u8, id, "sql-0014") or std.mem.eql(u8, id, "sql-0595")) 2 else 1), witnesses);
    }
    try std.testing.expectEqual(@as(usize, 10), completed);
}

test "mounted SQL original prepared CTE sources preserve deferral exact protocol results and cold identities" {
    const PreparedSourceCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: SourceCampaign,
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.beforeRestart(ctx);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.afterRestart(ctx);
        }
    };
    const GoldenSources = struct { format: u8, reference: []const u8, entries: []const SourceEntry };
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(GoldenSources, a, local.sql_parity_fixtures.prepared_source_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    var completed: usize = 0;
    for ([_][]const u8{ "sql-0005", "sql-0008", "sql-0009", "sql-0010" }) |id| {
        var witnesses: usize = 0;
        for (parsed.value.entries) |entry| {
            if (!std.mem.eql(u8, id, entry.id)) continue;
            try std.testing.expect(entry.prepared_name != null);
            errdefer std.debug.print("prepared source original={s} profile={s}\n", .{ id, entry.profile });
            var retained = std.heap.ArenaAllocator.init(a);
            defer retained.deinit();
            try fixture.runWithState(PreparedSourceCampaign, .{ .inner = .{ .entry = entry, .retained = &retained } });
            witnesses += 1;
            completed += 1;
        }
        try std.testing.expectEqual(@as(usize, if (std.mem.eql(u8, id, "sql-0008")) 1 else 2), witnesses);
    }
    try std.testing.expectEqual(@as(usize, 7), completed);
}

test "mounted SQL original table comment preserves schema rows privileges and all cold owners" {
    // sql-0142: existing native presentation metadata, not a new annotation DB.
    const Reference = struct {
        format: u8,
        id: []const u8,
        steps: []const struct { sql: []const u8, description: ?[]const u8 },
        denied_sqlstate: []const u8,
    };
    const TableRecord = @import("../metadata/table_manager.zig").TableRecord;
    const CommentCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: SourceCampaign,
        reference: Reference,
        initial: ?TableRecord = null,

        fn record(ctx: *fixture.Context, a: std.mem.Allocator) !TableRecord {
            const domain = local.system_catalog_domain;
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve = .{ .table = "usage_records" } });
            defer ctx.alloc.free(bytes);
            var resolved = try std.json.parseFromSlice(?domain.ResolvedTable, ctx.alloc, bytes, .{});
            defer resolved.deinit();
            const binding = resolved.value orelse return error.MissingSourceRelation;
            const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
            return (try store.getTable(a, 2188, binding.table_id)) orelse error.MissingSourceRelation;
        }

        fn verifyMetadata(self: *@This(), ctx: *fixture.Context, description: ?[]const u8) !void {
            const manager = @import("../metadata/table_manager.zig");
            const current = try record(ctx, ctx.alloc);
            defer manager.freeTable(ctx.alloc, current);
            try std.testing.expectEqualStrings(description orelse "", current.description);
            var without_presentation = current;
            without_presentation.description = self.initial.?.description;
            try std.testing.expect(manager.tableDefinitionsEqual(self.initial.?, without_presentation));
            try self.inner.fingerprints(ctx, .prepared);
            try self.inner.verifyRows(ctx);
        }

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.setup(ctx);
            // Actual public credentials, not an auth-disabled DDL fixture.
            ctx.headers = ctx.admin_headers;
            self.initial = try record(ctx, self.inner.retained.allocator());
            for (self.reference.steps) |step| {
                var response = try client.execute(ctx, step.sql, .write_admission);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("table comment status={d} body={s}\n", .{ response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var output = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer output.deinit();
                try std.testing.expectEqualStrings("COMMENT", output.value.command_tag);
                try std.testing.expectEqual(@as(i64, 0), output.value.rows_affected);
                try std.testing.expectEqual(@as(usize, 0), output.value.rows.len);
                try self.verifyMetadata(ctx, step.description);
            }
            var reader = ctx.*;
            reader.headers = ctx.reader_headers;
            var rejected = try client.execute(&reader, self.reference.steps[0].sql, .write_admission);
            defer rejected.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), rejected.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, rejected.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(self.reference.denied_sqlstate, diagnostic.value.code);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            try self.inner.verify(ctx, false);
        }

        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            try self.inner.verify(ctx, true);
        }
    };
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.table_comment_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("sql-0142", parsed.value.id);
    try std.testing.expectEqual(@as(usize, 5), parsed.value.steps.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqualStrings((try corpus.get("sql-0142")).sql, parsed.value.steps[0].sql);
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    const rows = [_][]const Json{ &.{ .{ .string = "u1" }, .{ .string = "before" } }, &.{ .{ .string = "u9" }, .{ .string = "untouched" } } };
    try fixture.runWithState(CommentCampaign, .{
        .reference = parsed.value,
        .inner = .{
            .retained = &retained,
            .entry = .{
                .id = "sql-0142",
                .profile = "description",
                .sqlstate = null,
                .command_tag = "COMMENT",
                .affected = 0,
                .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} },
                .tables = &.{.{
                    .name = "usage_records",
                    .create = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT)",
                    .seed = "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')",
                    .read = "SELECT id,status FROM usage_records ORDER BY id",
                    .final = .{ .columns = &.{ "id", "status" }, .column_oids = &.{ 25, 25 }, .rows = &rows },
                }},
            },
        },
    });
}

test "mounted SQL original column comment preserves stable identities rows privileges and cold owners" {
    // sql-0143: the existing stable column map owns presentation metadata.
    const Reference = struct {
        format: u8,
        id: []const u8,
        steps: []const struct { sql: []const u8, description: ?[]const u8 },
        denied_sqlstate: []const u8,
    };
    const TableRecord = @import("../metadata/table_manager.zig").TableRecord;
    const CommentCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: SourceCampaign,
        reference: Reference,
        initial: ?TableRecord = null,
        column_id: u64 = 0,

        fn record(ctx: *fixture.Context, a: std.mem.Allocator) !TableRecord {
            const domain = local.system_catalog_domain;
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve = .{ .table = "usage_records" } });
            defer ctx.alloc.free(bytes);
            var resolved = try std.json.parseFromSlice(?domain.ResolvedTable, ctx.alloc, bytes, .{});
            defer resolved.deinit();
            const binding = resolved.value orelse return error.MissingSourceRelation;
            const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
            return (try store.getTable(a, 2188, binding.table_id)) orelse error.MissingSourceRelation;
        }

        fn verifyMetadata(self: *@This(), ctx: *fixture.Context, description: ?[]const u8) !void {
            const manager = @import("../metadata/table_manager.zig");
            const current = try record(ctx, ctx.alloc);
            defer manager.freeTable(ctx.alloc, current);
            try std.testing.expect(manager.tableDefinitionsEqual(self.initial.?, current));
            var arena = std.heap.ArenaAllocator.init(ctx.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const api = @import("sql_catalog.zig");
            const captured = (try api.captureTableAdmission(ctx.metadata.server.owned_public_http_server.?, null, .{}, a, .{ .table = "usage_records" }, false, null)) orelse return error.MissingSourceRelation;
            const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
            var attributes = try store.getSqlColumnIdentities(a, 2188, captured.guard);
            defer attributes.deinit();
            const column = for (attributes.value.columns) |column| {
                if (std.mem.eql(u8, column.name, "status")) break column;
            } else return error.MissingSourceColumn;
            if (self.column_id == 0) self.column_id = column.id else try std.testing.expectEqual(self.column_id, column.id);
            if (description) |text| try std.testing.expectEqualStrings(text, column.description.?) else try std.testing.expect(column.description == null);
            for (attributes.value.columns) |other| if (other.id != column.id) try std.testing.expect(other.description == null);
            try self.inner.fingerprints(ctx, .prepared);
            try self.inner.verifyRows(ctx);
        }

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.setup(ctx);
            // Actual public credentials, not an auth-disabled DDL fixture.
            ctx.headers = ctx.admin_headers;
            self.initial = try record(ctx, self.inner.retained.allocator());
            for (self.reference.steps) |step| {
                var response = try client.execute(ctx, step.sql, .write_admission);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("table comment status={d} body={s}\n", .{ response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var output = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer output.deinit();
                try std.testing.expectEqualStrings("COMMENT", output.value.command_tag);
                try std.testing.expectEqual(@as(i64, 0), output.value.rows_affected);
                try std.testing.expectEqual(@as(usize, 0), output.value.rows.len);
                try self.verifyMetadata(ctx, step.description);
            }
            var reader = ctx.*;
            reader.headers = ctx.reader_headers;
            var rejected = try client.execute(&reader, self.reference.steps[0].sql, .write_admission);
            defer rejected.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), rejected.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, rejected.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(self.reference.denied_sqlstate, diagnostic.value.code);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            var missing = try client.execute(ctx, "COMMENT ON COLUMN usage_records.missing_column IS 'must not create'", .write_admission);
            defer missing.deinit(ctx.alloc);
            if (missing.status != 400) std.debug.print("missing column status={d} body={s}\n", .{ missing.status, missing.body });
            try std.testing.expectEqual(@as(u16, 400), missing.status);
            var missing_diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, missing.body, .{});
            defer missing_diagnostic.deinit();
            try std.testing.expectEqualStrings("42703", missing_diagnostic.value.code);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            try self.inner.verify(ctx, false);
        }

        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            try self.inner.verify(ctx, true);
        }
    };
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.column_comment_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqualStrings("sql-0143", parsed.value.id);
    try std.testing.expectEqual(@as(usize, 5), parsed.value.steps.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqualStrings((try corpus.get("sql-0143")).sql, parsed.value.steps[0].sql);
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    const rows = [_][]const Json{ &.{ .{ .string = "u1" }, .{ .string = "before" } }, &.{ .{ .string = "u9" }, .{ .string = "untouched" } } };
    try fixture.runWithState(CommentCampaign, .{
        .reference = parsed.value,
        .inner = .{
            .retained = &retained,
            .entry = .{
                .id = "sql-0143",
                .profile = "description",
                .sqlstate = null,
                .command_tag = "COMMENT",
                .affected = 0,
                .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} },
                .tables = &.{.{
                    .name = "usage_records",
                    .create = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT)",
                    .seed = "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')",
                    .read = "SELECT id,status FROM usage_records ORDER BY id",
                    .final = .{ .columns = &.{ "id", "status" }, .column_oids = &.{ 25, 25 }, .rows = &rows },
                }},
            },
        },
    });
}

test "transaction commit version retains nanoseconds and rejects overflow" {
    const version = local.api_local_transaction_contract.commitVersion;
    try std.testing.expectEqual(@as(u64, 9007199254740994), try version(9007199254740993));
    try std.testing.expectEqual(@as(u64, 1), try version(0));
    try std.testing.expectError(error.InvalidTxnRequest, version(std.math.maxInt(u64)));
}

fn physicalName(ctx: *fixture.Context) ![]u8 {
    const domain = local.system_catalog_domain;
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = "docs" }}, .include_query_definitions = true };
    const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var result = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{ .allocate = .alloc_always });
    defer result.deinit();
    try result.value.validateRelations(lookup);
    const relation = result.value.relations[0] orelse return error.MissingDocumentOwner;
    return ctx.alloc.dupe(u8, relation.table.name);
}

const State = struct {
    golden: Golden,
    entry: Entry,
    versions: std.StringHashMapUnmanaged(u64) = .empty,
    digests: std.StringHashMapUnmanaged([32]u8) = .empty,
};

const Campaign = struct {
    pub const native_sessions = true;
    state: *State,
    credentials: client.ApiKeyHeaders = .{},

    fn version(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8) !u64 {
        var response = try client.executeConnected(ctx, null, connection, statement, if (std.mem.startsWith(u8, statement, "SELECT")) .session_observation else .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("document session version sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
        try std.testing.expectEqual(@as(usize, 1), parsed.value.rows[0].len);
        try std.testing.expect(!parsed.value.sql_nulls.?[0][0]);
        return switch (parsed.value.rows[0][0]) {
            .integer => |v| std.math.cast(u64, v) orelse error.InvalidDocumentVersion,
            .string => |v| std.fmt.parseInt(u64, v, 10),
            else => error.InvalidDocumentVersion,
        };
    }

    fn verifyStagedVersions(ctx: *fixture.Context) !void {
        var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
        defer opened.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), opened.status);
        var connection = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
        defer connection.deinit();
        const id = connection.value.connection_id;
        const old = try version(ctx, null, "SELECT _version FROM docs WHERE _id='doc:a'");
        var began = try client.executeConnected(ctx, null, id, "BEGIN ISOLATION LEVEL READ COMMITTED", .never);
        defer began.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), began.status);
        const stamp = try version(ctx, id, "UPDATE docs SET title='staged' WHERE _id='doc:a' RETURNING _version");
        try std.testing.expect(stamp != old and stamp != 0);
        try std.testing.expectEqual(stamp, try version(ctx, id, "SELECT _version FROM docs WHERE _id='doc:a'"));
        // A second mutation must retain the original primary fence, rather
        // than mistakenly testing the projected stamp against committed data.
        try std.testing.expectEqual(stamp, try version(ctx, id, "UPDATE docs SET title='staged_again' WHERE _id='doc:a' RETURNING _version"));
        try std.testing.expectEqual(old, try version(ctx, null, "SELECT _version FROM docs WHERE _id='doc:a'"));
        var rolled_back = try client.executeConnected(ctx, null, id, "ROLLBACK", .never);
        defer rolled_back.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), rolled_back.status);
        try std.testing.expectEqual(old, try version(ctx, null, "SELECT _version FROM docs WHERE _id='doc:a'"));
        const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
        defer ctx.alloc.free(suffix);
        var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .DELETE, null, 15_000);
        defer closed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), closed.status);
    }

    fn verify(self: *@This(), ctx: *fixture.Context, cold: bool) !void {
        var result = try client.execute(ctx, "SELECT _id,_doc FROM docs ORDER BY _id", .read);
        defer result.deinit(ctx.alloc);
        if (result.status != 200) std.debug.print("document MERGE read id={s} status={d} body={s}\n", .{ self.state.entry.id, result.status, result.body });
        try std.testing.expectEqual(@as(u16, 200), result.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, result.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(self.state.entry.final.len, parsed.value.rows.len);
        const nulls = parsed.value.sql_nulls orelse return error.MissingSqlNullBitmap;
        const physical = try physicalName(ctx);
        defer ctx.alloc.free(physical);
        const reads = ctx.data.http_server.?.table_reads orelse return error.MissingDocumentReader;
        for (parsed.value.rows, nulls, self.state.entry.final) |row, flags, expected| {
            try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, parsed.value.columns, row, flags, &.{ .{ .string = expected.key }, expected.value }, &.{ false, false }));
            var primary = (try reads.lookup(ctx.alloc, physical, expected.key, .{ .include_primary_digest = true }, .read_index)) orelse return error.MissingDocumentPostimage;
            defer primary.deinit(ctx.alloc);
            const digest = primary.expected_content_digest orelse return error.MissingDocumentDigest;
            if (cold) {
                try std.testing.expectEqual(self.state.versions.get(expected.key).?, primary.version);
                try std.testing.expectEqual(self.state.digests.get(expected.key).?, digest);
            } else {
                try self.state.versions.put(ctx.alloc, expected.key, primary.version);
                try self.state.digests.put(ctx.alloc, expected.key, digest);
                for (self.state.entry.untouched) |key| if (std.mem.eql(u8, key, expected.key))
                    try std.testing.expectEqual(self.state.golden.seed_version, primary.version);
                if (self.state.entry.seeds) |seeds| for (seeds) |seed| if (std.mem.eql(u8, seed.key, expected.key)) {
                    try std.testing.expectEqual(self.state.golden.seed_version, primary.version);
                };
            }
        }
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        var declaration = self.state.entry.schema;
        _ = declaration.object.orderedRemove("version");
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema = declaration }, .{});
        defer ctx.alloc.free(body);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/tables/docs", .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 200) std.debug.print("document MERGE create status={d} body={s}\n", .{ created.status, created.body });
        try std.testing.expectEqual(@as(u16, 200), created.status);
        const serving = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        const physical = try physicalName(ctx);
        defer ctx.alloc.free(physical);
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const seeds = self.state.entry.seeds orelse self.state.golden.seeds;
        const writes = try a.alloc(local.storage_db_types.BatchWrite, seeds.len);
        for (writes, seeds) |*write, seed| write.* = .{ .key = seed.key, .value = try std.json.Stringify.valueAlloc(a, seed.value, .{}) };
        const writer = ctx.data.http_server.?.table_writes orelse return error.MissingDocumentWriter;
        // The original predicates compare the actual physical version 42.
        // This trusted test seed goes through the real Raft owner, not a
        // direct store write or a synthetic SQL projection of the version.
        _ = (try writer.batch(ctx.alloc, physical, .{ .writes = writes, .timestamp_ns = self.state.golden.seed_version })) orelse return error.MissingDocumentOwner;
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.state.entry.id);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        var response = try client.execute(ctx, original.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (self.state.entry.sqlstate) |code| {
            if (response.status != 400) std.debug.print("document MERGE rejection id={s} status={d} body={s}\n", .{ self.state.entry.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 400), response.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(code, diagnostic.value.code);
        } else {
            if (response.status != 200) std.debug.print("document MERGE id={s} status={d} body={s}\n", .{ self.state.entry.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var output = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer output.deinit();
            try std.testing.expectEqualStrings(self.state.entry.command_tag, output.value.command_tag);
            try std.testing.expectEqual(self.state.entry.affected, output.value.rows_affected);
            if (self.state.entry.occupied != null) {
                try std.testing.expectEqual(self.state.entry.columns.len, output.value.columns.len);
                try std.testing.expectEqual(self.state.entry.rows.len, output.value.rows.len);
                for (self.state.entry.columns, self.state.entry.column_oids, output.value.columns) |name, oid, column| {
                    try std.testing.expectEqualStrings(name, column.name);
                    try std.testing.expect(compare.postgresColumnMatches(column, oid));
                }
                const nulls = output.value.sql_nulls orelse if (output.value.rows.len == 0) &.{} else return error.MissingSqlNullBitmap;
                try std.testing.expectEqual(output.value.rows.len, nulls.len);
                for (output.value.rows, self.state.entry.rows, nulls) |actual, expected, flags| {
                    try std.testing.expectEqual(expected.len, actual.len);
                    try std.testing.expectEqual(actual.len, flags.len);
                    for (actual, expected, flags, output.value.columns) |cell, wanted, is_null, column| {
                        try std.testing.expect(!is_null);
                        if (std.mem.eql(u8, column.name, "_version")) {
                            const returned_version: u64 = switch (cell) {
                                .string => |v| try std.fmt.parseInt(u64, v, 10),
                                .integer => |v| std.math.cast(u64, v) orelse return error.InvalidDocumentVersion,
                                else => return error.InvalidDocumentVersion,
                            };
                            var primary = (try ctx.data.http_server.?.table_reads.?.lookup(ctx.alloc, physical, "doc:a", .{}, .read_index)) orelse return error.MissingDocumentPostimage;
                            defer primary.deinit(ctx.alloc);
                            try std.testing.expectEqual(primary.version, returned_version);
                            try std.testing.expect(returned_version != 0 and returned_version != self.state.golden.seed_version);
                        } else try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, &.{column}, &.{cell}, &.{false}, &.{wanted}, &.{false}));
                    }
                }
            } else try std.testing.expectEqual(@as(usize, 0), output.value.rows.len);
        }
        if (std.mem.eql(u8, self.state.entry.id, "sql-0927") and self.state.entry.occupied == false) try verifyStagedVersions(ctx);
        try self.verify(ctx, false);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try self.verify(ctx, true);
    }
};

test "mounted SQL original document identities enforce create only conflicts returning versions and cold postimages" {
    const ids = [_][]const u8{ "sql-0883", "sql-0927", "sql-0935" };
    const a = std.testing.allocator;
    var golden = try std.json.parseFromSlice(Golden, a, local.sql_parity_fixtures.document_identity_reference, .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    try std.testing.expectEqual(ids.len * 2, golden.value.entries.len);
    for (golden.value.entries, 0..) |entry, index| {
        try std.testing.expectEqualStrings(ids[index / 2], entry.id);
        try std.testing.expectEqual(index % 2 == 1, entry.occupied.?);
        var state: State = .{ .golden = golden.value, .entry = entry };
        defer state.versions.deinit(a);
        defer state.digests.deinit(a);
        errdefer std.debug.print("document identity original={s} occupied={any}\n", .{ entry.id, entry.occupied });
        try fixture.runWithState(Campaign, .{ .state = &state });
    }
}

test "mounted SQL original document MERGE retains native session fences postimages and cold versions" {
    const ids = [_][]const u8{ "sql-1105", "sql-1106", "sql-1107", "sql-1108" };
    const a = std.testing.allocator;
    var golden = try std.json.parseFromSlice(Golden, a, local.sql_parity_fixtures.document_envelope_reference, .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    var index: usize = 0;
    for (golden.value.entries) |entry| {
        if (!entry.requires_native_session) continue;
        if (index >= ids.len) return error.UnexpectedDocumentMerge;
        try std.testing.expectEqualStrings(ids[index], entry.id);
        index += 1;
        errdefer std.debug.print("document MERGE original {s}\n", .{entry.id});
        var state: State = .{ .golden = golden.value, .entry = entry };
        defer state.versions.deinit(a);
        defer state.digests.deinit(a);
        try fixture.runWithState(Campaign, .{ .state = &state });
    }
    try std.testing.expectEqual(ids.len, index);
}

test "mounted SQL original resource comments preserve identities authorization and cold owners" {
    // sql-0146 and sql-0147 execute unchanged from independent PostgreSQL witnesses.
    const Case = struct {
        id: []const u8,
        kind: []const u8,
        name: []const u8,
        steps: []const struct { sql: []const u8, description: ?[]const u8 },
        denied_sqlstate: []const u8,
        missing_sqlstate: ?[]const u8,
        rename_preserves_identity: bool,
        recreate_clears_description: bool,
    };
    const Reference = struct { format: u8, cases: []const Case };
    const TableRecord = @import("../metadata/table_manager.zig").TableRecord;
    const CommentCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: SourceCampaign,
        reference: Case,
        resource_id: u64 = 0,
        initial: ?TableRecord = null,

        fn record(ctx: *fixture.Context, a: std.mem.Allocator) !TableRecord {
            const domain = local.system_catalog_domain;
            const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve = .{ .table = "usage_records" } });
            defer ctx.alloc.free(bytes);
            var resolved = try std.json.parseFromSlice(?domain.ResolvedTable, ctx.alloc, bytes, .{});
            defer resolved.deinit();
            const binding = resolved.value orelse return error.MissingSourceRelation;
            const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
            return (try store.getTable(a, 2188, binding.table_id)) orelse error.MissingSourceRelation;
        }

        pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
            const users = @import("../usermgr/mod.zig");
            inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
                var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
                defer permission.deinit(manager.alloc);
                try manager.addPermissionToUser("pk-rewriter", permission);
            }
        }

        fn observe(self: *@This(), ctx: *fixture.Context, name: []const u8, description: ?[]const u8) !u64 {
            const database = std.mem.eql(u8, self.reference.kind, "database");
            const suffix = if (database)
                try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/{s}", .{name})
            else
                try ctx.alloc.dupe(u8, "/db/v1/databases/default/namespaces");
            defer ctx.alloc.free(suffix);
            var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, suffix, .GET, null);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("comment catalog status={d} body={s}\n", .{ response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var json = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
            defer json.deinit();
            const object = if (database) json.value.object else blk: {
                for (json.value.array.items) |item| if (std.mem.eql(u8, item.object.get("name").?.string, name)) break :blk item.object;
                return error.MissingResourceIdentity;
            };
            const actual = object.get("description") orelse .null;
            if (description) |text| {
                try std.testing.expect(actual == .string);
                try std.testing.expectEqualStrings(text, actual.string);
            } else try std.testing.expect(actual == .null);
            const id = object.get(if (database) "database_id" else "namespace_id") orelse return error.MissingResourceIdentity;
            return @intCast(id.integer);
        }

        fn verifyMetadata(self: *@This(), ctx: *fixture.Context, description: ?[]const u8) !void {
            const id = try self.observe(ctx, self.reference.name, description);
            if (self.resource_id == 0) self.resource_id = id else try std.testing.expectEqual(self.resource_id, id);
            const manager = @import("../metadata/table_manager.zig");
            const current = try record(ctx, ctx.alloc);
            defer manager.freeTable(ctx.alloc, current);
            try std.testing.expect(manager.tableDefinitionsEqual(self.initial.?, current));
            try self.inner.fingerprints(ctx, .prepared);
            try self.inner.verifyRows(ctx);
        }

        fn execute(ctx: *fixture.Context, sql: []const u8) !void {
            var response = try client.execute(ctx, sql, .write_admission);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("resource lifecycle SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
        }

        fn lifecycle(self: *@This(), ctx: *fixture.Context) !void {
            const kind = self.reference.kind;
            const a = self.inner.retained.allocator();
            try execute(ctx, try std.fmt.allocPrint(a, "CREATE {s} comment_scope", .{kind}));
            try execute(ctx, try std.fmt.allocPrint(a, "COMMENT ON {s} comment_scope IS 'retained'", .{kind}));
            const id = try self.observe(ctx, "comment_scope", "retained");
            try execute(ctx, try std.fmt.allocPrint(a, "ALTER {s} comment_scope RENAME TO comment_scope_moved", .{kind}));
            try std.testing.expectEqual(id, try self.observe(ctx, "comment_scope_moved", "retained"));
            try execute(ctx, try std.fmt.allocPrint(a, "DROP {s} comment_scope_moved", .{kind}));
            try execute(ctx, try std.fmt.allocPrint(a, "CREATE {s} comment_scope_moved", .{kind}));
            try std.testing.expect(id != try self.observe(ctx, "comment_scope_moved", null));
            try execute(ctx, try std.fmt.allocPrint(a, "DROP {s} comment_scope_moved", .{kind}));
        }

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.setup(ctx);
            try self.inner.credentials.activateWithCatalog(ctx, true);
            // Actual public scoped credentials; table read does not grant catalog admin.
            ctx.headers = ctx.admin_headers;
            if (std.mem.eql(u8, self.reference.kind, "database")) try execute(ctx, "CREATE DATABASE appdb");
            self.initial = try record(ctx, self.inner.retained.allocator());
            for (self.reference.steps) |step| {
                var response = try client.execute(ctx, step.sql, .write_admission);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("resource comment status={d} body={s}\n", .{ response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var output = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer output.deinit();
                try std.testing.expectEqualStrings("COMMENT", output.value.command_tag);
                try std.testing.expectEqual(@as(i64, 0), output.value.rows_affected);
                try std.testing.expectEqual(@as(usize, 0), output.value.rows.len);
                try self.verifyMetadata(ctx, step.description);
            }
            var reader = ctx.*;
            reader.headers = ctx.reader_headers;
            var rejected = try client.execute(&reader, self.reference.steps[0].sql, .write_admission);
            defer rejected.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), rejected.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, rejected.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(self.reference.denied_sqlstate, diagnostic.value.code);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            const missing = try std.fmt.allocPrint(self.inner.retained.allocator(), "COMMENT ON {s} missing_comment_object IS 'x'", .{self.reference.kind});
            if (self.reference.missing_sqlstate) |code| try SourceCampaign.probe(ctx, missing, code) else try execute(ctx, missing);
            try self.lifecycle(ctx);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            try self.inner.verify(ctx, false);
        }

        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
            try self.verifyMetadata(ctx, self.reference.steps[self.reference.steps.len - 1].description);
            try self.inner.verify(ctx, true);
        }
    };
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.resource_comment_reference, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.format);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.cases.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const original_ids = [_][]const u8{ "sql-0146", "sql-0147" };
    for (parsed.value.cases, original_ids) |reference, original_id| {
        try std.testing.expectEqualStrings(original_id, reference.id);
        try std.testing.expectEqualStrings((try corpus.get(reference.id)).sql, reference.steps[0].sql);
        try std.testing.expect(reference.rename_preserves_identity and reference.recreate_clears_description);
        var retained = std.heap.ArenaAllocator.init(a);
        defer retained.deinit();
        const rows = [_][]const Json{ &.{ .{ .string = "u1" }, .{ .string = "before" } }, &.{ .{ .string = "u9" }, .{ .string = "untouched" } } };
        try fixture.runWithState(CommentCampaign, .{
            .reference = reference,
            .inner = .{
                .retained = &retained,
                .entry = .{
                    .id = reference.id,
                    .profile = "description",
                    .sqlstate = null,
                    .command_tag = "COMMENT",
                    .affected = 0,
                    .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} },
                    .tables = &.{.{
                        .name = "usage_records",
                        .create = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT)",
                        .seed = "INSERT INTO usage_records(id,status) VALUES ('u1','before'),('u9','untouched')",
                        .read = "SELECT id,status FROM usage_records ORDER BY id",
                        .final = .{ .columns = &.{ "id", "status" }, .column_oids = &.{ 25, 25 }, .rows = &rows },
                    }},
                },
            },
        });
    }
}

test "mounted SQL original calendar intervals preserve typed results and cold rows" {
    const Case = struct { id: []const u8, sql: []const u8, columns: []const []const u8, column_oids: []const u32, rows: []const []const Json };
    const CalendarCampaign = struct {
        pub const native_sessions = true;
        pub const native_auth = true;
        pub const cold_catalog = true;
        inner: SourceCampaign,
        cases: []const Case,
        fn verify(self: *@This(), ctx: *fixture.Context) !void {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            for (self.cases) |case| {
                const original = try corpus.get(case.id);
                try std.testing.expectEqualStrings(original.sql, case.sql);
                const parameters = try local.sql_parity_fixtures.Corpus.logicalParameters(ctx.alloc, original);
                defer ctx.alloc.free(parameters);
                var response = try client.executeParameters(ctx, original.sql, parameters, .read);
                defer response.deinit(ctx.alloc);
                if (response.status != 200) std.debug.print("calendar interval original={s} status={d} body={s}\n", .{ case.id, response.status, response.body });
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer result.deinit();
                try std.testing.expectEqualStrings("SELECT", result.value.command_tag);
                try std.testing.expectEqual(@as(i64, 0), result.value.rows_affected);
                expectSourceResult(ctx.alloc, result.value, .{ .columns = case.columns, .column_oids = case.column_oids, .rows = case.rows }, true) catch |err| {
                    std.debug.print("calendar interval result original={s} body={s}\n", .{ case.id, response.body });
                    return err;
                };
            }
        }
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.inner.setup(ctx);
            try self.verify(ctx);
            try self.inner.verify(ctx, false);
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
            try self.verify(ctx);
            try self.inner.verify(ctx, true);
        }
    };
    const a = std.testing.allocator;
    const reference = try std.json.parseFromSlice(struct { cases: []const Case }, a, local.sql_parity_fixtures.temporal_arithmetic_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    const ids = [_][]const u8{ "sql-0593", "sql-0594" };
    try std.testing.expectEqual(ids.len, reference.value.cases.len);
    for (ids, reference.value.cases) |id, case| try std.testing.expectEqualStrings(id, case.id);
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    const rows = [_][]const Json{ &.{ .{ .string = "u1" }, .{ .string = "2024-01-31T23:59:59.123456Z" } }, &.{ .{ .string = "u2" }, .{ .string = "2023-01-31T00:00:00Z" } } };
    try fixture.runWithState(CalendarCampaign, .{
        .cases = reference.value.cases,
        .inner = .{ .retained = &retained, .entry = .{
            .id = "sql-0593",
            .profile = "UTC calendar arithmetic",
            .sqlstate = null,
            .command_tag = "SELECT",
            .affected = 0,
            .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} },
            .tables = &.{.{
                .name = "usage_records",
                .create = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,created_at TIMESTAMPTZ)",
                .seed = "INSERT INTO usage_records(id,created_at) VALUES ('u1',TIMESTAMPTZ '2024-01-31T23:59:59.123456Z'),('u2',TIMESTAMPTZ '2023-01-31T00:00:00Z')",
                .read = "SELECT id,created_at FROM usage_records ORDER BY id",
                .final = .{ .columns = &.{ "id", "created_at" }, .column_oids = &.{ 25, 1184 }, .rows = &rows },
            }},
        } },
    });
}
