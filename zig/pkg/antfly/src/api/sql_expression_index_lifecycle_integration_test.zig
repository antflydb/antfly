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

//! Exact expression-index originals through real owners and cold recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const platform = @import("antfly_platform");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const lifecycle = @import("sql_index_lifecycle_integration_test.zig");
const server = @import("http_server.zig");
const wire = @import("antfly_metadata_openapi").types;

const Profile = struct { id: []const u8, namespace: []const u8, unique: bool = false, partial: bool = false, covering: bool = false, mutable: bool = false };
const profiles = [_]Profile{
    .{ .id = "sql-0017", .namespace = "expr_replace_unique", .unique = true },
    .{ .id = "sql-0712", .namespace = "expr_concat", .mutable = true },
    .{ .id = "sql-0713", .namespace = "expr_concat_ws", .mutable = true },
    .{ .id = "sql-0714", .namespace = "expr_lower_wrapped" },
    .{ .id = "sql-0715", .namespace = "expr_lower_cover", .covering = true },
    .{ .id = "sql-0716", .namespace = "expr_upper" },
    .{ .id = "sql-0717", .namespace = "expr_md5" },
    .{ .id = "sql-0718", .namespace = "expr_replace" },
    .{ .id = "sql-0722", .namespace = "expr_partial_unique", .unique = true, .partial = true },
    .{ .id = "sql-0723", .namespace = "expr_partial_wrapped", .unique = true, .partial = true },
};
const Row = struct { id: u8, email: ?[]const u8, tenant: []const u8 = "tenant", status: ?[]const u8 };
const Cut = struct { generation: u64, version: u32, comparison: [32]u8 };

fn command(ctx: *fixture.Context, profile: Profile, sql: []const u8, ddl: bool) !void {
    var response = try client.executeIn(ctx, profile.namespace, sql, if (ddl) .never else .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and !(ddl and response.status == 202)) {
        std.debug.print("expression index command case={s} status={d} sql={s} body={s}\n", .{ profile.id, response.status, sql, response.body });
        return error.ExpressionIndexCommandFailed;
    }
}
fn rejection(ctx: *fixture.Context, profile: Profile, sql: []const u8) !void {
    return failure(ctx, profile, sql, "23505", .write_admission, 400);
}
fn failure(ctx: *fixture.Context, profile: Profile, sql: []const u8, code: []const u8, retry: client.Retry, expected_status: u16) !void {
    var response = try client.executeIn(ctx, profile.namespace, sql, retry);
    defer response.deinit(ctx.alloc);
    if (response.status != expected_status) std.debug.print("expression index rejection case={s} status={d} body={s}\n", .{ profile.id, response.status, response.body });
    try std.testing.expectEqual(expected_status, response.status);
    var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, retryable: bool = false }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings(code, diagnostic.value.code);
    try std.testing.expect(!diagnostic.value.retryable);
}
fn rows(ctx: *fixture.Context, profile: Profile, expected: []const Row) !void {
    var response = try client.executeIn(ctx, profile.namespace, "SELECT id,email,tenant_id,status FROM usage_records ORDER BY id", .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    try std.testing.expectEqual(@as(usize, 4), parsed.value.columns.len);
    for (parsed.value.columns, [_][]const u8{ "id", "email", "tenant_id", "status" }) |column, label| try std.testing.expectEqualStrings(label, column.name);
    for (parsed.value.rows, parsed.value.sql_nulls.?, expected) |cells, nulls, want| {
        try std.testing.expectEqual(@as(usize, 4), cells.len);
        const id = try std.fmt.allocPrint(ctx.alloc, "00000000-0000-0000-0000-{d:0>12}", .{want.id});
        defer ctx.alloc.free(id);
        for (cells, nulls, [_]?[]const u8{ id, want.email, want.tenant, want.status }) |cell, is_null, text| {
            try std.testing.expectEqual(text == null, is_null);
            if (text) |value| try std.testing.expectEqualStrings(value, cell.string) else try std.testing.expect(cell == .null);
        }
    }
}
fn indexedRows(ctx: *fixture.Context, profile: Profile, name: []const u8, expected: []const Row) !void {
    const predicate = if (profile.partial)
        "tenant_id='tenant' AND lower(email)='owner@example.com' AND status='active'"
    else if (std.mem.eql(u8, profile.id, "sql-0017") or std.mem.eql(u8, profile.id, "sql-0718"))
        "replace(status,'old','new')='new-start'"
    else if (std.mem.eql(u8, profile.id, "sql-0716"))
        "upper(email)='OWNER@EXAMPLE.COM'"
    else if (std.mem.eql(u8, profile.id, "sql-0717"))
        "md5(email)=md5('owner@example.com')"
    else
        "lower(email)='owner@example.com'";
    const query = try std.fmt.allocPrint(ctx.alloc, "SELECT id FROM usage_records WHERE {s} ORDER BY id", .{predicate});
    defer ctx.alloc.free(query);
    const explain = try std.fmt.allocPrint(ctx.alloc, "EXPLAIN {s}", .{query});
    defer ctx.alloc.free(explain);
    var plan_response = try client.executeIn(ctx, profile.namespace, explain, .read);
    defer plan_response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), plan_response.status);
    var plan = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, plan_response.body, .{});
    defer plan.deinit();
    var declaration_seen = false;
    for (plan.value.rows) |row| if (row[0] == .string and std.mem.indexOf(u8, row[0].string, "index selection: snapshot-local") != null and std.mem.indexOf(u8, row[0].string, name) != null) {
        declaration_seen = true;
    };
    if (!declaration_seen) std.debug.print("expression index plan case={s} body={s}\n", .{ profile.id, plan_response.body });
    try std.testing.expect(declaration_seen);
    // A two-row table may legitimately cost less than an automatic index
    // probe. EXPLAIN must not invent a choice before native snapshot planning.
    // Independently force the exact named tuple span to prove physical index
    // contents and mutation maintenance, rather than weakening this to DDL-only
    // metadata or SQL result counts.
    const metadata = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const domain = local.system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .namespace = profile.namespace, .name = "usage_records" }} };
    const bytes = try metadata.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var owner = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer owner.deinit();
    try owner.value.validateRelations(lookup);
    const table = owner.value.relations[0] orelse return error.MissingCreatedRelation;
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    // Named tuple reads carry membership proofs. Admit their durable tracking
    // capability through the normal writer before pinning the read cut.
    const writer = ctx.data.http_server.?.table_writes orelse return error.MissingOwnerWriteSource;
    try writer.activateRangeTracking(ctx.alloc, table.table.name, .{});
    var md5: [16]u8 = undefined;
    std.crypto.hash.Md5.hash("owner@example.com", &md5, .{});
    const md5_text = std.fmt.bytesToHex(md5, .lower);
    const key = if (std.mem.eql(u8, profile.id, "sql-0017") or std.mem.eql(u8, profile.id, "sql-0718")) "new-start" else if (std.mem.eql(u8, profile.id, "sql-0716")) "OWNER@EXAMPLE.COM" else if (std.mem.eql(u8, profile.id, "sql-0717")) &md5_text else "owner@example.com";
    const tuple: []const std.json.Value = if (profile.partial) &.{ .{ .string = "tenant" }, .{ .string = key } } else &.{.{ .string = key }};
    var physical_page = page: {
        const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
        const physical = admitted: {
            while (true) {
                try ctx.checkDrivers();
                if (platform.time.monotonicNs() >= deadline) return error.IndexProbeAdmissionTimeout;
                const view = reader.openRelationalRead(ctx.alloc, table.table.name, "", "", .{
                    .fields = &.{"id"},
                    .include_documents = true,
                    .include_range_proofs = true,
                    .execution_deadline_ns = deadline,
                    .limit = 128,
                    .relational_query = .{ .index = name, .schema_version = table.owner.schema_version, .fields = &.{"id"}, .lower = .{ .values = tuple }, .upper = .{ .values = tuple } },
                }, .read_index) catch |err| switch (err) {
                    // No read view or row has escaped admission. Reopen the whole
                    // cut, just as the mounted SQL read client does; never retry
                    // a write, a page, or an integrity failure here.
                    error.SqlStatementReadUnavailable => {
                        try ctx.io.sleep(.fromMilliseconds(20), .awake);
                        continue;
                    },
                    else => return err,
                };
                break :admitted view orelse return error.MissingOwnerReadSource;
            }
        };
        defer physical.deinit();
        break :page try physical.next(ctx.alloc, 128);
    };
    defer physical_page.deinit();
    try std.testing.expect(physical_page.after == null);
    var response = try client.executeIn(ctx, profile.namespace, query, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("expression index query case={s} body={s}\n", .{ profile.id, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    var count: usize = 0;
    for (expected) |row| {
        const matches = if (profile.partial)
            std.mem.eql(u8, row.tenant, "tenant") and row.status != null and std.mem.eql(u8, row.status.?, "active") and row.email != null and std.ascii.eqlIgnoreCase(row.email.?, "owner@example.com")
        else if (std.mem.eql(u8, profile.id, "sql-0017") or std.mem.eql(u8, profile.id, "sql-0718"))
            row.status != null and (std.mem.eql(u8, row.status.?, "old-start") or std.mem.eql(u8, row.status.?, "new-start"))
        else if (std.mem.eql(u8, profile.id, "sql-0717"))
            row.email != null and std.mem.eql(u8, row.email.?, "owner@example.com")
        else
            row.email != null and std.ascii.eqlIgnoreCase(row.email.?, "owner@example.com");
        if (!matches) continue;
        try std.testing.expect(count < parsed.value.rows.len);
        const id = try std.fmt.allocPrint(ctx.alloc, "00000000-0000-0000-0000-{d:0>12}", .{row.id});
        defer ctx.alloc.free(id);
        try std.testing.expectEqualStrings(id, parsed.value.rows[count][0].string);
        // Equal expression keys are ordered by physical row identity, not
        // this table's logical UUID key. Verify exact membership and unique
        // occurrence independently of the SQL ORDER BY above.
        var occurrences: usize = 0;
        for (physical_page.rows) |physical_row| {
            const physical_id = try physical_row.cell("id");
            try std.testing.expect(!physical_id.sql_null);
            if (std.mem.eql(u8, id, physical_id.value.string)) occurrences += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), occurrences);
        count += 1;
    }
    try std.testing.expectEqual(count, parsed.value.rows.len);
    try std.testing.expectEqual(count, physical_page.rows.len);
}
fn ready(ctx: *fixture.Context, profile: Profile, name: []const u8, expected_id: u64) !Cut {
    const domain = local.system_catalog_domain;
    const metadata = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .namespace = profile.namespace, .name = "usage_records" }}, .include_query_definitions = true };
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const bytes = metadata.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve_many = lookup }) catch |err| switch (err) {
            error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => {
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        defer ctx.alloc.free(bytes);
        var resolved = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer resolved.deinit();
        try resolved.value.validateRelations(lookup);
        const relation = resolved.value.relations[0] orelse return error.MissingCreatedRelation;
        try std.testing.expectEqual(expected_id, relation.owner.table_id);
        var schema = try std.json.parseFromSlice(std.json.Value, ctx.alloc, relation.table.query_definition.?.schema_json, .{});
        defer schema.deinit();
        const definitions = schema.value.object.get("relational_indexes").?.array.items;
        var found = false;
        for (definitions) |definition| if (std.mem.eql(u8, name, definition.object.get("name").?.string)) {
            const keys = definition.object.get("keys").?.array.items;
            try std.testing.expectEqual(@as(usize, if (profile.partial) 2 else 1), keys.len);
            try std.testing.expect(keys[keys.len - 1].object.get("expression") != null);
            try std.testing.expectEqual(@as(usize, @intFromBool(profile.covering)), definition.object.get("include_columns").?.array.items.len);
            try std.testing.expectEqual(profile.partial, definition.object.get("where").?.array.items.len != 0);
            found = true;
        };
        try std.testing.expect(found);
        const native = local.storage_db_relational_index_status_contract;
        const request = try std.json.Stringify.valueAlloc(ctx.alloc, native.Request{ .name = name, .schema_version = relation.owner.schema_version }, .{});
        defer ctx.alloc.free(request);
        const observed = reader.lookup(ctx.alloc, relation.table.name, "", .{ .relational_index_status_json = request, .execution_deadline_ns = deadline }, .read_index) catch |err| switch (err) {
            error.PreparedGenerationChanged, error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.UnknownGroup, error.ReadIndexTimeout, error.ReadUnavailable, error.StorageReadTemporarilyUnavailable, error.IndexNotFound => null,
            else => return err,
        };
        if (observed) |value| {
            var response = value;
            defer response.deinit(ctx.alloc);
            var status = try std.json.parseFromSlice(native.Status, ctx.alloc, response.json, .{});
            defer status.deinit();
            try std.testing.expectEqual(expected_id, status.value.table_id);
            try std.testing.expectEqual(relation.owner.schema_version, status.value.schema_version);
            if (status.value.state == .failed) return error.ExpressionIndexBuildFailed;
            if (status.value.state == .ready) {
                try std.testing.expectEqual(@TypeOf(status.value.failure).none, status.value.failure);
                try std.testing.expect(status.value.generation != 0);
                try std.testing.expect(!std.mem.allEqual(u8, &status.value.comparison, 0));
                return .{ .generation = status.value.generation, .version = status.value.schema_version, .comparison = status.value.comparison };
            }
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ExpressionIndexBuildTimeout;
}

fn Campaign(comptime nonunique_only: bool) type {
    return struct {
        ids: [profiles.len]u64 = undefined,
        cuts: [profiles.len]?Cut = @splat(null),
        expected: [profiles.len][6]Row = undefined,
        counts: [profiles.len]usize = @splat(0),
        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            for (profiles, 0..) |profile, position| {
                if (nonunique_only and profile.unique) continue;
                const ns = try std.fmt.allocPrint(ctx.alloc, "CREATE SCHEMA {s}", .{profile.namespace});
                defer ctx.alloc.free(ns);
                try command(ctx, .{ .id = profile.id, .namespace = "public" }, ns, true);
                try std.testing.expect((try lifecycle.relationOwner(ctx, profile.namespace)) == null);
                try command(ctx, profile, "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT)", true);
                self.ids[position] = try lifecycle.owner(ctx, profile.namespace);
                try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.ids[position]);
                try lifecycle.waitEnforced(ctx, profile.namespace);
                if (nonunique_only and std.mem.eql(u8, profile.id, "sql-0712")) {
                    for ([_][]const u8{ "COPY usage_records TO STDOUT", "COPY (SELECT 1 AS n) TO STDOUT" }) |statement|
                        try failure(ctx, profile, statement, "0A000", .never, 501);
                }
                const first_status = if (profile.partial) "active" else "old-start";
                const second_status = if (profile.partial) "active" else "old-finish";
                const seed = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000001','Owner@Example.com','tenant','{s}'),('00000000-0000-0000-0000-000000000002','other@example.com','tenant','{s}')", .{ first_status, second_status });
                defer ctx.alloc.free(seed);
                try command(ctx, profile, seed, false);
                self.expected[position][0] = .{ .id = 1, .email = "Owner@Example.com", .status = first_status };
                self.expected[position][1] = .{ .id = 2, .email = "other@example.com", .status = second_status };
                self.counts[position] = 2;
                try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                const original = try corpus.get(profile.id);
                try std.testing.expectEqual(@as(usize, 0), original.params.len);
                var compiled = try local.sql_compiler.compile(ctx.alloc, original.sql, .{});
                defer compiled.deinit();
                const index_name = compiled.statement.catalog_ddl.schema_change.?.create_index.name;
                if (profile.mutable) {
                    const before = try lifecycle.snapshotDefinition(ctx, profile.namespace);
                    defer ctx.alloc.free(before);
                    try failure(ctx, profile, original.sql, "42P17", .never, 400);
                    const after = try lifecycle.snapshotDefinition(ctx, profile.namespace);
                    defer ctx.alloc.free(after);
                    try std.testing.expectEqualStrings(before, after);
                    try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                    continue;
                }
                try command(ctx, profile, original.sql, true);
                self.cuts[position] = try ready(ctx, profile, index_name, self.ids[position]);
                try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                try indexedRows(ctx, profile, index_name, self.expected[position][0..self.counts[position]]);
                const collision_status = if (profile.partial) "active" else "new-start";
                const collision = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000003','owner@example.com','tenant','{s}')", .{collision_status});
                defer ctx.alloc.free(collision);
                const update = try std.fmt.allocPrint(ctx.alloc, "UPDATE usage_records SET email='OWNER@EXAMPLE.COM',status='{s}' WHERE id='00000000-0000-0000-0000-000000000002'", .{collision_status});
                defer ctx.alloc.free(update);
                if (profile.unique) {
                    try rejection(ctx, profile, collision);
                    try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                    try rejection(ctx, profile, update);
                } else {
                    try command(ctx, profile, collision, false);
                    self.expected[position][2] = .{ .id = 3, .email = "owner@example.com", .status = collision_status };
                    self.counts[position] = 3;
                    try command(ctx, profile, update, false);
                    self.expected[position][1] = .{ .id = 2, .email = "OWNER@EXAMPLE.COM", .status = collision_status };
                }
                try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                const nullable = if (profile.partial)
                    "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000004','owner@example.com','tenant','inactive')"
                else
                    "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000004',NULL,'tenant',NULL)";
                try command(ctx, profile, nullable, false);
                self.expected[position][self.counts[position]] = .{ .id = 4, .email = if (profile.partial) "owner@example.com" else null, .status = if (profile.partial) "inactive" else null };
                self.counts[position] += 1;
                if (profile.partial) try rejection(ctx, profile, "UPDATE usage_records SET status='active' WHERE id='00000000-0000-0000-0000-000000000004'");
                try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                try indexedRows(ctx, profile, index_name, self.expected[position][0..self.counts[position]]);
            }
        }
        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            for (profiles, 0..) |profile, position| {
                if (nonunique_only and profile.unique) continue;
                try std.testing.expectEqual(self.ids[position], try lifecycle.owner(ctx, profile.namespace));
                try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.ids[position]);
                const original = try corpus.get(profile.id);
                var compiled = try local.sql_compiler.compile(ctx.alloc, original.sql, .{});
                defer compiled.deinit();
                if (self.cuts[position]) |cut| try std.testing.expectEqualDeep(cut, try ready(ctx, profile, compiled.statement.catalog_ddl.schema_change.?.create_index.name, self.ids[position]));
                try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                if (!profile.mutable) try indexedRows(ctx, profile, compiled.statement.catalog_ddl.schema_change.?.create_index.name, self.expected[position][0..self.counts[position]]);
                try command(ctx, profile, "DELETE FROM usage_records WHERE id='00000000-0000-0000-0000-000000000001'", false);
                const status = if (profile.partial) "active" else "new-start";
                const insert = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000006','owner@example.com','tenant','{s}')", .{status});
                defer ctx.alloc.free(insert);
                try command(ctx, profile, insert, false);
                for (1..self.counts[position]) |i| self.expected[position][i - 1] = self.expected[position][i];
                self.expected[position][self.counts[position] - 1] = .{ .id = 6, .email = "owner@example.com", .status = status };
                try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                if (profile.unique) {
                    const collision = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000007','OWNER@EXAMPLE.COM','tenant','{s}')", .{status});
                    defer ctx.alloc.free(collision);
                    try rejection(ctx, profile, collision);
                    try rows(ctx, profile, self.expected[position][0..self.counts[position]]);
                }
                if (!profile.mutable) try indexedRows(ctx, profile, compiled.statement.catalog_ddl.schema_change.?.create_index.name, self.expected[position][0..self.counts[position]]);
            }
        }
    };
}

test "mounted SQL original nonunique expression indexes preserve keys mutations and cold owners" {
    // Original sql-0712, sql-0713, sql-0714, sql-0715, sql-0716,
    // sql-0717 and sql-0718. Mutable declarations retain their PostgreSQL
    // rejection; supported declarations require physical indexed reads.
    try fixture.run(Campaign(true));
}

test "mounted SQL original expression indexes build maintain partial keys and recover owners" {
    // Exact originals sql-0017, sql-0712, sql-0713, sql-0714, sql-0715,
    // sql-0716, sql-0717, sql-0718, sql-0722, sql-0723, unchanged.
    try fixture.run(Campaign(false));
}

const UnsupportedOwnerCampaign = struct {
    id: u64 = 0,
    definition_digest: [32]u8 = undefined,
    fn unchanged(self: *@This(), ctx: *fixture.Context) !void {
        const definition = try lifecycle.snapshotDefinition(ctx, profiles[0].namespace);
        defer ctx.alloc.free(definition);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(definition, &digest, .{});
        try std.testing.expectEqualSlices(u8, &self.definition_digest, &digest);
        try std.testing.expectEqual(self.id, try lifecycle.owner(ctx, profiles[0].namespace));
        const work = @import("../metadata/relational_index_work.zig");
        const svc = ctx.metadata.server.svc;
        const store = svc.projectedStore() orelse return error.MissingMetadataStore;
        const bytes = try store.relationalIndexWorkJson(ctx.alloc, svc.metadata_group_id, .{ .discover = 0 });
        defer ctx.alloc.free(bytes);
        var observed = try std.json.parseFromSlice(work.Response, ctx.alloc, bytes, .{});
        defer observed.deinit();
        try std.testing.expect(observed.value == .discovered);
        try std.testing.expect(observed.value.discovered.head == null);
        try std.testing.expect(observed.value.discovered.exhausted);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        const profile = profiles[0];
        try command(ctx, .{ .id = profile.id, .namespace = "public" }, "CREATE SCHEMA expr_replace_unique", true);
        try std.testing.expect((try lifecycle.relationOwner(ctx, profile.namespace)) == null);
        try command(ctx, profile, "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT)", true);
        self.id = try lifecycle.owner(ctx, profile.namespace);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.id);
        try lifecycle.waitEnforced(ctx, profile.namespace);
        try command(ctx, profile, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000001','Owner@Example.com','tenant','old-start')", false);
        const definition = try lifecycle.snapshotDefinition(ctx, profile.namespace);
        defer ctx.alloc.free(definition);
        std.crypto.hash.Blake3.hash(definition, &self.definition_digest, .{});
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(profile.id);
        // Unsupported SQL features use HTTP 501; constraint/input errors use
        // 400. Preserve the existing public contract rather than masking it.
        try failure(ctx, profile, original.sql, "0A000", .never, 501);
        try self.unchanged(ctx);
        try rows(ctx, profile, &.{.{ .id = 1, .email = "Owner@Example.com", .status = "old-start" }});
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.id);
        try self.unchanged(ctx);
        try rows(ctx, profiles[0], &.{.{ .id = 1, .email = "Owner@Example.com", .status = "old-start" }});
        try command(ctx, profiles[0], "UPDATE usage_records SET status='new-start' WHERE id='00000000-0000-0000-0000-000000000001'", false);
        try rows(ctx, profiles[0], &.{.{ .id = 1, .email = "Owner@Example.com", .status = "new-start" }});
    }
};
test "mounted SQL unsupported atomic index owner preserves definite nonadmission across restart" {
    // This is an authority/diagnostic regression, not sql-0017 qualification.
    // The positive original cohort remains a separate, deliberately strict gate.
    try fixture.run(UnsupportedOwnerCampaign);
}
