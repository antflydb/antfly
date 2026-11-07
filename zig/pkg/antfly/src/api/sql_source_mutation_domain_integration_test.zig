// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Original UUID mutation domains, PostgreSQL diagnostics and native no-effect
//! proofs. Later expressions earn no credit from an earlier UUID rejection.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const http = @import("http_server.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Entry = struct {
    id: []const u8,
    namespace: []const u8,
    setup: []const struct { sql: []const u8, sqlstate: ?[]const u8 },
    sql: []const u8,
    sqlstate: []const u8,
    seed: []const u8,
    read: []const u8,
    before: Output,
    control: []const u8,
    returned: Output,
    after: Output,
    cold_control: []const u8,
    cold_returned: Output,
    cold_after: Output,
};
const Snapshot = struct {
    table_id: u64,
    schema_version: u32,
    schema: [32]u8,
    read_schema: [32]u8,
    indexes: [32]u8,
    physical_name: [32]u8,
    primary_key: [32]u8,
    primary_version: u64,
    primary_digest: [32]u8,
    rows: [32]u8,
};
fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn command(ctx: *fixture.Context, namespace: ?[]const u8, sql: []const u8) !void {
    var response = try client.executeIn(ctx, namespace, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) std.debug.print("source domain command sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    if (std.mem.startsWith(u8, sql, "CREATE UNIQUE INDEX"))
        try @import("sql_index_lifecycle_integration_test.zig").awaitIndexPublication(ctx, namespace orelse "public", sql, response);
}
fn rejected(ctx: *fixture.Context, entry: Entry, sql: []const u8, code: []const u8) !void {
    var response = try client.executeIn(ctx, entry.namespace, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 400) std.debug.print("source domain rejection id={s} sql={s} status={d} body={s}\n", .{ entry.id, sql, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 400), response.status);
    var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
    defer diagnostic.deinit();
    if (!std.mem.eql(u8, code, diagnostic.value.code)) std.debug.print("source domain expected={s} body={s}\n", .{ code, response.body });
    try std.testing.expectEqualStrings(code, diagnostic.value.code);
    var envelope = try std.json.parseFromSlice(Json, ctx.alloc, response.body, .{});
    defer envelope.deinit();
    if (envelope.value.object.get("ddl_receipt")) |receipt| try std.testing.expect(receipt == .null);
}
fn expectOutput(ctx: *fixture.Context, actual: wire.SQLResponse, expected: Output) !void {
    try std.testing.expectEqual(expected.columns.len, actual.columns.len);
    for (actual.columns, expected.columns, expected.column_oids) |column, name, oid| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, actual.rows.len);
    const nulls = actual.sql_nulls orelse return error.MissingSqlNullBitmap;
    try std.testing.expectEqual(actual.rows.len, nulls.len);
    for (actual.rows, nulls, expected.rows, expected.sql_nulls) |row, flags, want, want_flags| {
        if (!try compare.rowMatchesWithNulls(ctx.alloc, actual.columns, row, flags, want, want_flags)) {
            std.debug.print("source domain row expected={f} actual={f}\n", .{ std.json.fmt(want, .{}), std.json.fmt(row, .{}) });
            return error.SourceDomainPostimageMismatch;
        }
    }
}
fn observe(ctx: *fixture.Context, entry: Entry, statement: []const u8, expected: Output) !void {
    var response = try client.executeIn(ctx, entry.namespace, statement, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("source domain observation sql={s} body={s}\n", .{ statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try expectOutput(ctx, parsed.value, expected);
}
fn control(ctx: *fixture.Context, entry: Entry, statement: []const u8, expected: Output) !void {
    var response = try client.executeIn(ctx, entry.namespace, statement, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("source domain valid neighbor sql={s} body={s}\n", .{ statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.rows_affected);
    try std.testing.expectEqual(wire.SQLMutationOutcome.committed, parsed.value.mutation_outcome.?);
    try expectOutput(ctx, parsed.value, expected);
}
fn snapshot(ctx: *fixture.Context, entry: Entry, expected: Output) !Snapshot {
    const domain = local.system_catalog_domain;
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .database = "default", .namespace = entry.namespace, .name = "usage_records" }}, .include_query_definitions = true };
    const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var resolved = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer resolved.deinit();
    try resolved.value.validateRelations(lookup);
    const relation = resolved.value.relations[0] orelse return error.MissingSourceRelation;
    const definition = relation.table.query_definition orelse return error.MissingQueryDefinition;
    var response = try client.executeIn(ctx, entry.namespace, entry.read, .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var rows = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer rows.deinit();
    try expectOutput(ctx, rows.value, expected);
    const row_bytes = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .columns = rows.value.columns, .rows = rows.value.rows, .sql_nulls = rows.value.sql_nulls }, .{});
    defer ctx.alloc.free(row_bytes);
    var keys = try client.executeIn(ctx, entry.namespace, "SELECT _id FROM usage_records ORDER BY id", .read);
    defer keys.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), keys.status);
    var primary = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, keys.body, .{});
    defer primary.deinit();
    try std.testing.expectEqual(@as(usize, 1), primary.value.rows.len);
    const key = primary.value.rows[0][0].string;
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    var observed = (try reader.lookup(ctx.alloc, relation.table.name, key, .{ .include_primary_digest = true }, .read_index)) orelse return error.MissingSqlPrimary;
    defer observed.deinit(ctx.alloc);
    return .{
        .table_id = relation.table.table_id,
        .schema_version = relation.owner.schema_version,
        .schema = digest(definition.schema_json),
        .read_schema = digest(definition.read_schema_json),
        .indexes = digest(definition.indexes_json),
        .physical_name = digest(relation.table.name),
        .primary_key = digest(key),
        .primary_version = observed.version,
        .primary_digest = observed.expected_content_digest orelse return error.MissingPrimaryDigest,
        .rows = digest(row_bytes),
    };
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    entries: []const Entry,
    credentials: client.ApiKeyHeaders = .{},
    retained: [4]Snapshot = undefined,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn unchanged(_: *@This(), ctx: *fixture.Context, entry: Entry, expected: Output, before: Snapshot) !void {
        try rejected(ctx, entry, entry.sql, entry.sqlstate);
        try std.testing.expectEqualDeep(before, try snapshot(ctx, entry, expected));
        for (entry.setup) |step| if (step.sqlstate) |code| {
            try rejected(ctx, entry, step.sql, code);
            try std.testing.expectEqualDeep(before, try snapshot(ctx, entry, expected));
        };
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        for (self.entries, 0..) |entry, i| {
            const create = try std.fmt.allocPrint(ctx.alloc, "CREATE SCHEMA {s}", .{entry.namespace});
            defer ctx.alloc.free(create);
            try command(ctx, null, create);
            for (entry.setup) |step| {
                if (step.sqlstate) |code| try rejected(ctx, entry, step.sql, code) else {
                    try command(ctx, entry.namespace, step.sql);
                    const serving = try fixture.waitServingSchemaInScope(ctx, "default", entry.namespace, "usage_records", 0);
                    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
                }
            }
            try command(ctx, entry.namespace, entry.seed);
            const before = try snapshot(ctx, entry, entry.before);
            try self.unchanged(ctx, entry, entry.before, before);
            try control(ctx, entry, entry.control, entry.returned);
            try observe(ctx, entry, entry.read, entry.after);
            self.retained[i] = try snapshot(ctx, entry, entry.after);
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.entries, 0..) |entry, i| {
            _ = try fixture.waitServingSchemaInScope(ctx, "default", entry.namespace, "usage_records", self.retained[i].schema_version);
            try std.testing.expectEqualDeep(self.retained[i], try snapshot(ctx, entry, entry.after));
            try self.unchanged(ctx, entry, entry.after, self.retained[i]);
            try control(ctx, entry, entry.cold_control, entry.cold_returned);
            try observe(ctx, entry, entry.read, entry.cold_after);
        }
    }
};

test "mounted SQL original UUID mutation domains preserve diagnostics complete postimages and cold native state" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(struct { entries: []const Entry }, a, local.sql_parity_fixtures.source_mutation_domain_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var originals = try local.sql_parity_fixtures.Corpus.init(a);
    defer originals.deinit();
    const expected = [_][]const u8{ "sql-1403", "sql-1468", "sql-1469", "sql-1476" };
    try std.testing.expectEqual(expected.len, reference.value.entries.len);
    for (reference.value.entries, expected) |entry, id| {
        try std.testing.expectEqualStrings(id, entry.id);
        const source = try originals.get(id);
        try std.testing.expectEqualStrings(source.sql, entry.sql);
        try std.testing.expectEqual(@as(usize, 0), source.params.len);
    }
    // One cluster, four isolated native namespaces and one actual restart.
    try fixture.runWithState(Campaign, .{ .entries = reference.value.entries });
}
