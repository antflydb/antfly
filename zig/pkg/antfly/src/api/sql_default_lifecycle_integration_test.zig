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

//! Original default DDL against real metadata/data owners, not schema fixtures.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const http = @import("../raft/transport/http_common.zig");
const wire = @import("antfly_metadata_openapi").types;
const server = @import("http_server.zig");
const executeMode = @import("sql_owner_test_client.zig").execute;

fn execute(ctx: *fixture.Context, statement: []const u8) !http.HttpResponse {
    return executeMode(ctx, statement, .never);
}

fn command(ctx: *fixture.Context, statement: []const u8, creation: bool) !void {
    var response = if (creation) try execute(ctx, statement) else try executeMode(ctx, statement, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and !(creation and response.status == 202)) {
        std.debug.print("default lifecycle command status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
        return error.DefaultLifecycleCommandFailed;
    }
}

fn alter(ctx: *fixture.Context, statement: []const u8) !void {
    var response = try execute(ctx, statement);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("default lifecycle ALTER status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("ALTER TABLE", parsed.value.command_tag);
    try std.testing.expectEqual(wire.SQLMutationOutcome.committed, parsed.value.mutation_outcome.?);
    try std.testing.expectEqual(@as(i64, 0), parsed.value.rows_affected);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.columns.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.rows.len);
}

/// Observe both publication and the actual serving schema. This is a bounded
/// pure read; no DDL or mutation is replayed while propagation catches up.
const Owner = struct { table_id: u64, version: u32 };

fn waitSchema(ctx: *fixture.Context, minimum: u32) !Owner {
    const domain = local.system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = "usage_records" }}, .include_query_definitions = true };
    const metadata = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
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
        if (resolved.value.relations[0]) |relation| {
            if (relation.owner.schema_version >= minimum) {
                const observed = reader.lookup(ctx.alloc, relation.table.name, "", .{
                    .relational_topology_json = "{\"mode\":\"public_schema\"}",
                    .execution_deadline_ns = deadline,
                }, .read_index) catch |err| switch (err) {
                    error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.UnknownGroup, error.ReadIndexTimeout, error.ReadUnavailable, error.StorageReadTemporarilyUnavailable, error.TableNotFound, error.IntegrityCatalogChanged => null,
                    else => return err,
                };
                if (observed) |value| {
                    var response = value;
                    defer response.deinit(ctx.alloc);
                    var public = try std.json.parseFromSlice([]const u8, ctx.alloc, response.json, .{});
                    defer public.deinit();
                    var schema = try std.json.parseFromSlice(struct { version: u32 }, ctx.alloc, public.value, .{ .ignore_unknown_fields = true });
                    defer schema.deinit();
                    if (schema.value.version == relation.owner.schema_version) return .{ .table_id = relation.owner.table_id, .version = schema.value.version };
                }
            }
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.DefaultLifecycleSchemaTimeout;
}

const Expected = struct { id: []const u8, status: ?[]const u8, amount: ?[]const u8 };

const Primary = struct { version: u64, digest: [32]u8 };

fn oldPrimary(ctx: *fixture.Context) !Primary {
    var result = try executeMode(ctx, "SELECT _id FROM usage_records WHERE id='before'", .read);
    defer result.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), result.status);
    var row = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, result.body, .{});
    defer row.deinit();
    try std.testing.expectEqual(@as(usize, 1), row.value.rows.len);
    const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .name = "usage_records" }} };
    const metadata = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try metadata.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var resolved = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer resolved.deinit();
    try resolved.value.validateRelations(lookup);
    const relation = resolved.value.relations[0] orelse return error.MissingSourceRelation;
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    var observed = (try reader.lookup(ctx.alloc, relation.table.name, row.value.rows[0][0].string, .{ .include_primary_digest = true, .execution_deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s }, .read_index)) orelse return error.MissingSqlPrimary;
    defer observed.deinit(ctx.alloc);
    return .{ .version = observed.version, .digest = observed.expected_content_digest orelse return error.MissingPrimaryDigest };
}

fn rows(ctx: *fixture.Context, expected: []const Expected) !void {
    var response = try executeMode(ctx, "SELECT id,status,CAST(amount AS text) AS amount FROM usage_records ORDER BY id", .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("default lifecycle rows status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.columns.len);
    for (parsed.value.columns, [_][]const u8{ "id", "status", "amount" }) |column, name| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expectEqual(wire.SQLColumnType.string, column.type);
    }
    for (parsed.value.rows, parsed.value.sql_nulls.?, expected) |row, nulls, want| {
        try std.testing.expectEqual(@as(usize, 3), row.len);
        try std.testing.expectEqualStrings(want.id, row[0].string);
        try std.testing.expect(!nulls[0]);
        for (row[1..], nulls[1..], [_]?[]const u8{ want.status, want.amount }) |cell, is_null, text| {
            try std.testing.expectEqual(text == null, is_null);
            if (text) |value| try std.testing.expectEqualStrings(value, cell.string) else try std.testing.expect(cell == .null);
        }
    }
}

const final_rows = [_]Expected{
    .{ .id = "after_amount", .status = "pending", .amount = "7" },
    .{ .id = "after_drop", .status = null, .amount = "7" },
    .{ .id = "after_status", .status = "pending", .amount = "2.50" },
    .{ .id = "before", .status = "initial", .amount = "2.50" },
    .{ .id = "default", .status = "pending", .amount = "2.50" },
    .{ .id = "explicit", .status = null, .amount = null },
};

const Campaign = struct {
    table_id: u64 = 0,
    version: u32 = 0,
    primary: Primary = undefined,
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        try command(ctx, "CREATE TABLE usage_records (id TEXT, status TEXT DEFAULT 'initial', amount NUMERIC DEFAULT 2.50)", true);
        const initial = try waitSchema(ctx, 0);
        self.table_id = initial.table_id;
        self.version = initial.version;
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.table_id);
        try command(ctx, "INSERT INTO usage_records(id) VALUES('before')", false);
        self.primary = try oldPrimary(ctx);
        for ([_][]const u8{ "sql-0741", "sql-0742", "sql-0743" }, 0..) |id, step| {
            const original = try corpus.get(id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            try alter(ctx, original.sql);
            self.version += 1;
            const observed = try waitSchema(ctx, self.version);
            try std.testing.expectEqual(self.table_id, observed.table_id);
            try std.testing.expectEqual(self.version, observed.version);
            try std.testing.expectEqualDeep(self.primary, try oldPrimary(ctx));
            switch (step) {
                0 => {
                    try rows(ctx, &.{.{ .id = "before", .status = "initial", .amount = "2.50" }});
                    try command(ctx, "INSERT INTO usage_records(id) VALUES('after_status')", false);
                    try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES('explicit',NULL,NULL),('default',DEFAULT,DEFAULT)", false);
                },
                1 => {
                    try rows(ctx, &.{ final_rows[2], final_rows[3], final_rows[4], final_rows[5] });
                    try command(ctx, "INSERT INTO usage_records(id) VALUES('after_amount')", false);
                },
                2 => try command(ctx, "INSERT INTO usage_records(id) VALUES('after_drop')", false),
                else => unreachable,
            }
        }
        try rows(ctx, &final_rows);
        var rejected = try execute(ctx, "ALTER TABLE usage_records ALTER COLUMN absent SET DEFAULT 'bad'");
        defer rejected.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), rejected.status);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, rejected.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("42703", diagnostic.value.code);
        const unchanged = try waitSchema(ctx, self.version);
        try std.testing.expectEqual(self.version, unchanged.version);
        try rows(ctx, &final_rows);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.table_id);
        const recovered = try waitSchema(ctx, self.version);
        try std.testing.expectEqual(self.table_id, recovered.table_id);
        try std.testing.expectEqual(self.version, recovered.version);
        try rows(ctx, &final_rows);
        try std.testing.expectEqualDeep(self.primary, try oldPrimary(ctx));
        try command(ctx, "INSERT INTO usage_records(id) VALUES('after_restart')", false);
        try command(ctx, "UPDATE usage_records SET status=DEFAULT,amount=DEFAULT WHERE id='before'", false);
        try rows(ctx, &.{
            final_rows[0],                                      final_rows[1], .{ .id = "after_restart", .status = null, .amount = "7" }, final_rows[2],
            .{ .id = "before", .status = null, .amount = "7" }, final_rows[4], final_rows[5],
        });
    }
};

test "mounted SQL original column defaults preserve old rows and recover future mutation semantics" {
    // Exact original DDL: sql-0741, sql-0742, sql-0743. Corpus statements and
    // parameters are unchanged; the PostgreSQL oracle owns the same lifecycle.
    try fixture.run(Campaign);
}
