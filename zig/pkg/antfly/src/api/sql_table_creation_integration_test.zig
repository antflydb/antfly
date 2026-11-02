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

//! Original CREATE owns admission, typed rows and enforcement through restart.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const server = @import("http_server.zig");
const wire = @import("antfly_metadata_openapi").types;
const A = std.mem.Allocator;

const Profile = struct { id: []const u8, table: []const u8 };
const profiles = [_]Profile{
    .{ .id = "sql-0671", .table = "generated_expr_records" },
    .{ .id = "sql-0675", .table = "casted_defaults" },
};
const Owner = struct { id: u64, version: u32 };

/// Await the production worker's published occupancy root. Do not adopt a
/// decoder, manufacture coverage, or repeat CREATE to bootstrap readiness.
fn waitNamingPublication(ctx: *fixture.Context, name: []const u8) !void {
    const svc = ctx.metadata.server.svc;
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        try svc.ensureLinearizableReadWithContext(.{ .deadline_ns = deadline });
        const work = try store.relationReconciliationWork(svc.metadata_group_id);
        if (work.current) |current| if (work.epoch != null and current.epoch.eql(work.epoch.?) and
            current.phase == .ready and current.failure == .none and current.coverage == .constraint_names and
            work.root != null and work.root.?.eql(local.system_catalog_relation_reconciliation.Generation.of(&current))) return;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    const work = try store.relationReconciliationWork(svc.metadata_group_id);
    std.debug.print("CREATE naming timeout table={s} epoch={} current={} root={}\n", .{ name, work.epoch != null, work.current != null, work.root != null });
    if (work.current) |current| std.debug.print("CREATE naming publication phase={s} coverage={s} failure={s} epoch_matches={} root_matches={}\n", .{ @tagName(current.phase), @tagName(current.coverage), @tagName(current.failure), work.epoch != null and current.epoch.eql(work.epoch.?), work.root != null and work.root.?.eql(local.system_catalog_relation_reconciliation.Generation.of(&current)) });
    return error.SqlCreateNamingPublicationTimeout;
}

fn owner(ctx: *fixture.Context, name: []const u8) !Owner {
    const domain = local.system_catalog_domain;
    const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = name }}, .include_query_definitions = true };
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const bytes = source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve_many = lookup }) catch |err| switch (err) {
            error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => {
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        try parsed.value.validateRelations(lookup);
        const found = parsed.value.relations[0] orelse return error.MissingCreatedSqlTable;
        const definition = found.table.query_definition orelse return error.MissingSqlDefinition;
        var declaration = try std.json.parseFromSlice(std.json.Value, ctx.alloc, definition.schema_json, .{});
        defer declaration.deinit();
        try std.testing.expectEqualStrings("relational", declaration.value.object.get("storage_mode").?.string);
        const keys = declaration.value.object.get("unique_constraints").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), keys.len);
        const key_name = try std.fmt.allocPrint(ctx.alloc, "{s}_pkey", .{name});
        defer ctx.alloc.free(key_name);
        try std.testing.expectEqualStrings(key_name, keys[0].object.get("name").?.string);
        try std.testing.expect(keys[0].object.get("primary").?.bool);
        const key_columns = keys[0].object.get("columns").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), key_columns.len);
        try std.testing.expectEqualStrings("id", key_columns[0].string);
        return .{ .id = found.owner.table_id, .version = found.owner.schema_version };
    }
    return error.SqlCreateOwnerTimeout;
}

fn ready(ctx: *fixture.Context, name: []const u8, expected: Owner) !void {
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, expected.id);
    const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/default/namespaces/public/tables/{s}/constraints/status", .{name});
    defer ctx.alloc.free(suffix);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .GET, null, 1000);
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            var status = try std.json.parseFromSlice(struct { state: enum { enforced, validating, invalid }, schema_version: u32 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer status.deinit();
            if (status.value.state == .invalid) return error.CreatedConstraintInvalid;
            if (status.value.state == .enforced and status.value.schema_version == expected.version) return;
        } else if (response.status != 503) {
            std.debug.print("CREATE constraint readiness table={s} status={d} body={s}\n", .{ name, response.status, response.body });
            return error.SqlCreateReadinessFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlCreateReadinessTimeout;
}

fn command(ctx: *fixture.Context, statement: []const u8) !void {
    var response = try client.execute(ctx, statement, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("CREATE campaign mutation status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(wire.SQLMutationOutcome.committed, parsed.value.mutation_outcome.?);
}

fn rejected(ctx: *fixture.Context, statement: []const u8, code: []const u8) !void {
    var response = try client.execute(ctx, statement, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 400) std.debug.print("CREATE campaign rejection status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
    try std.testing.expectEqual(@as(u16, 400), response.status);
    var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
}

fn quote(a: A, text: []const u8) ![]const u8 {
    const escaped = try std.mem.replaceOwned(u8, a, text, "'", "''");
    defer a.free(escaped);
    return std.fmt.allocPrint(a, "'{s}'", .{escaped});
}

const generated_read = "SELECT id::text AS id,status,amount::text AS amount,fee::text AS fee,metadata::text AS metadata,status_slug,amount_cents::text AS amount_cents,source_key FROM generated_expr_records ORDER BY id";
const casted_read = "SELECT id::text AS id,amount::text AS amount,enabled::text AS enabled FROM casted_defaults ORDER BY id";
const generated_labels = [_][]const u8{ "id", "status", "amount", "fee", "metadata", "status_slug", "amount_cents", "source_key" };
const casted_labels = [_][]const u8{ "id", "amount", "enabled" };
const generated_initial = [_][]const ?[]const u8{
    &.{ "00000000-0000-0000-0000-000000000001", " Ready Now ", "1.235", "0.005", "{\"source\": 123.00}", "-ready-now-", "124", "123.00" },
    &.{ "00000000-0000-0000-0000-000000000002", null, "-0.005", "0", "{\"source\": null}", null, "-1", null },
    &.{ "00000000-0000-0000-0000-000000000003", "CLOSED", "9007199254740993.5", "0.005", "{\"source\": \"exact\"}", "closed", "900719925474099351", "exact" },
};
const generated_updated = [_][]const ?[]const u8{
    &.{ "00000000-0000-0000-0000-000000000001", "Ready Again", "1.235", "0.015", "{\"source\": \"updated\"}", "ready-again", "125", "updated" },
    generated_initial[1],
    generated_initial[2],
};
const casted_initial = [_][]const ?[]const u8{
    &.{ "00000000-0000-0000-0000-000000000001", "0", "true" },
    &.{ "00000000-0000-0000-0000-000000000002", null, null },
    &.{ "00000000-0000-0000-0000-000000000003", "0", "true" },
    &.{ "00000000-0000-0000-0000-000000000004", "9007199254740993.5000", "false" },
};

fn expectRows(ctx: *fixture.Context, statement: []const u8, labels: []const []const u8, expected: []const []const ?[]const u8) !void {
    return expectRowsMode(ctx, statement, labels, expected, .read);
}

fn expectRowsMode(ctx: *fixture.Context, statement: []const u8, labels: []const []const u8, expected: []const []const ?[]const u8, retry: client.Retry) !void {
    var response = try client.execute(ctx, statement, retry);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("CREATE campaign read status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    if (retry == .write_admission) {
        try std.testing.expectEqual(wire.SQLMutationOutcome.committed, parsed.value.mutation_outcome.?);
        try std.testing.expectEqual(@as(i64, @intCast(expected.len)), parsed.value.rows_affected);
        try std.testing.expectEqualStrings("INSERT", parsed.value.command_tag);
    }
    try std.testing.expectEqual(labels.len, parsed.value.columns.len);
    for (labels, parsed.value.columns) |name, column| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expectEqual(wire.SQLColumnType.string, column.type);
    }
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    for (expected, parsed.value.rows, parsed.value.sql_nulls.?) |want, row, nulls| {
        try std.testing.expectEqual(want.len, row.len);
        try std.testing.expectEqual(want.len, nulls.len);
        for (want, row, nulls) |text, value, is_null| {
            try std.testing.expectEqual(text == null, is_null);
            if (text) |target| {
                if (!std.mem.eql(u8, target, value.string)) std.debug.print("CREATE postimage mismatch (bounded response): {s}\n", .{response.body[0..@min(response.body.len, 2048)]});
                try std.testing.expectEqualStrings(target, value.string);
            } else try std.testing.expect(value == .null);
        }
    }
}

fn nativeDescriptors(ctx: *fixture.Context) !void {
    for ([_]struct { sql: []const u8, types: []const wire.SQLColumnType, builtins: []const @FieldType(wire.SQLColumn, "element_type") }{
        .{ .sql = "SELECT id,status,amount,fee,metadata,status_slug,amount_cents,source_key FROM generated_expr_records LIMIT 0", .types = &.{ .uuid, .string, .number, .number, .json, .string, .number, .string }, .builtins = &.{ null, null, .numeric, .numeric, null, null, .numeric, null } },
        .{ .sql = "SELECT id,amount,enabled FROM casted_defaults LIMIT 0", .types = &.{ .uuid, .number, .boolean }, .builtins = &.{ null, .numeric, null } },
    }) |query| {
        var response = try client.execute(ctx, query.sql, .read);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 0), parsed.value.rows.len);
        try std.testing.expectEqual(query.types.len, parsed.value.columns.len);
        for (parsed.value.columns, query.types, query.builtins) |column, kind, builtin| {
            try std.testing.expectEqual(kind, column.type);
            if (builtin) |expected| try std.testing.expectEqual(expected, column.element_type.?);
        }
    }
}

/// The committed primary must retain the same display-sensitive source and
/// generated value as RETURNING; a correct speculative image is insufficient.
fn expectCommittedJsonbDisplay(ctx: *fixture.Context) !void {
    var response = try client.execute(ctx, "SELECT _id FROM generated_expr_records WHERE id='00000000-0000-0000-0000-000000000001'", .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var row = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer row.deinit();
    try std.testing.expectEqual(@as(usize, 1), row.value.rows.len);
    const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .name = "generated_expr_records" }} };
    const metadata = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try metadata.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var resolved = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer resolved.deinit();
    try resolved.value.validateRelations(lookup);
    const relation = resolved.value.relations[0] orelse return error.MissingSourceRelation;
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    var observed = (try reader.lookup(ctx.alloc, relation.table.name, row.value.rows[0][0].string, .{ .execution_deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s }, .read_index)) orelse return error.MissingSqlPrimary;
    defer observed.deinit(ctx.alloc);
    var primary = try std.json.parseFromSlice(std.json.Value, ctx.alloc, observed.json, .{ .parse_numbers = false });
    defer primary.deinit();
    const source = primary.value.object.get("metadata").?.object.get("source").?;
    const generated = primary.value.object.get("source_key").?;
    if (source != .number_string or !std.mem.eql(u8, source.number_string, "123.00") or generated != .string or !std.mem.eql(u8, generated.string, "123.00"))
        std.debug.print("committed CREATE primary mismatch (bounded): {s}\n", .{observed.json[0..@min(observed.json.len, 2048)]});
    try std.testing.expectEqualStrings("123.00", source.number_string);
    try std.testing.expectEqualStrings("123.00", generated.string);
}

const Campaign = struct {
    owners: [profiles.len]Owner = undefined,
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (profiles, &self.owners) |profile, *saved| {
            try waitNamingPublication(ctx, profile.table);
            const original = try corpus.get(profile.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            var response = try client.execute(ctx, original.sql, .never);
            defer response.deinit(ctx.alloc);
            if (response.status != 200 and response.status != 202) std.debug.print("original CREATE id={s} status={d} body={s}\n", .{ profile.id, response.status, response.body });
            try std.testing.expect(response.status == 200 or response.status == 202);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 0), result.value.rows_affected);
            try std.testing.expectEqual(@as(usize, 0), result.value.rows.len);
            try std.testing.expectEqual(@as(usize, 0), result.value.columns.len);
            if (response.status == 202) {
                try std.testing.expectEqualStrings("DDL PENDING", result.value.command_tag);
                try std.testing.expectEqual(wire.SQLMutationOutcome.committed_pending, result.value.mutation_outcome.?);
                try std.testing.expect(result.value.ddl_receipt != null);
            } else {
                try std.testing.expectEqualStrings("CREATE TABLE", result.value.command_tag);
                try std.testing.expectEqual(wire.SQLMutationOutcome.committed, result.value.mutation_outcome.?);
            }
            saved.* = try owner(ctx, profile.table);
            if (result.value.ddl_receipt) |receipt| try std.testing.expectEqual(saved.id, try std.fmt.parseUnsigned(u64, receipt.table_id, 10));
            try ready(ctx, profile.table, saved.*);
        }
        try nativeDescriptors(ctx);
        const Input = struct { id: []const u8, status: ?[]const u8, amount: []const u8, fee: []const u8, metadata_json: []const u8 };
        var inputs = try std.json.parseFromSlice(struct { id: []const u8, inputs: []const Input }, ctx.alloc, local.sql_parity_fixtures.generated_original_reference, .{ .ignore_unknown_fields = true });
        defer inputs.deinit();
        try std.testing.expectEqualStrings("sql-0671", inputs.value.id);
        for (inputs.value.inputs, 0..) |input, index| {
            var arena = std.heap.ArenaAllocator.init(ctx.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const statement = try std.fmt.allocPrint(a, "INSERT INTO generated_expr_records(id,status,amount,fee,metadata) VALUES({s},{s},{s}::numeric,{s}::numeric,{s}::jsonb)", .{ try quote(a, input.id), if (input.status) |status| try quote(a, status) else "NULL", try quote(a, input.amount), try quote(a, input.fee), try quote(a, input.metadata_json) });
            const returning = try std.fmt.allocPrint(a, "{s} RETURNING id::text AS id,status,amount::text AS amount,fee::text AS fee,metadata::text AS metadata,status_slug,amount_cents::text AS amount_cents,source_key", .{statement});
            if (index == 0) {
                // Exercise the ordinary commit path as well as speculative
                // RETURNING preparation: neither may erase JSONB scale.
                try command(ctx, statement);
                const extra_id = "00000000-0000-0000-0000-000000000004";
                const extra = try std.mem.replaceOwned(u8, a, returning, input.id, extra_id);
                var expected: [8]?[]const u8 = undefined;
                @memcpy(&expected, generated_initial[0]);
                expected[0] = extra_id;
                try expectRowsMode(ctx, extra, &generated_labels, &.{&expected}, .write_admission);
                try command(ctx, "DELETE FROM generated_expr_records WHERE id='00000000-0000-0000-0000-000000000004'");
            } else try expectRowsMode(ctx, returning, &generated_labels, generated_initial[index .. index + 1], .write_admission);
        }
        try command(ctx, "INSERT INTO casted_defaults(id) VALUES('00000000-0000-0000-0000-000000000001')");
        try command(ctx, "INSERT INTO casted_defaults(id,amount,enabled) VALUES('00000000-0000-0000-0000-000000000002',NULL,NULL),('00000000-0000-0000-0000-000000000003',DEFAULT,DEFAULT),('00000000-0000-0000-0000-000000000004','9007199254740993.5000'::numeric,false)");
        try expectCommittedJsonbDisplay(ctx);
        try expectRows(ctx, generated_read, &generated_labels, &generated_initial);
        try expectRows(ctx, casted_read, &casted_labels, &casted_initial);
        try rejected(ctx, "INSERT INTO generated_expr_records(id,status) VALUES('00000000-0000-0000-0000-000000000001','duplicate')", "23505");
        try rejected(ctx, "INSERT INTO generated_expr_records(id) VALUES(NULL)", "23502");
        try rejected(ctx, "INSERT INTO generated_expr_records(id,status_slug) VALUES('00000000-0000-0000-0000-000000000004','bad')", "428C9");
        try rejected(ctx, "UPDATE generated_expr_records SET status_slug='bad' WHERE false", "428C9");
        try rejected(ctx, "INSERT INTO casted_defaults(id) VALUES('00000000-0000-0000-0000-000000000001')", "23505");
        try rejected(ctx, "INSERT INTO casted_defaults(id) VALUES(NULL)", "23502");
        try expectRows(ctx, generated_read, &generated_labels, &generated_initial);
        try expectRows(ctx, casted_read, &casted_labels, &casted_initial);
        try command(ctx, "UPDATE generated_expr_records SET status='Ready Again',fee=0.015,metadata='{\"source\":\"updated\"}'::jsonb,status_slug=DEFAULT,amount_cents=DEFAULT,source_key=DEFAULT WHERE id='00000000-0000-0000-0000-000000000001'");
        try expectRows(ctx, generated_read, &generated_labels, &generated_updated);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (profiles, self.owners) |profile, saved| {
            try ready(ctx, profile.table, saved);
            try std.testing.expectEqualDeep(saved, try owner(ctx, profile.table));
        }
        try nativeDescriptors(ctx);
        try expectRows(ctx, generated_read, &generated_labels, &generated_updated);
        try expectRows(ctx, casted_read, &casted_labels, &casted_initial);
        try rejected(ctx, "INSERT INTO generated_expr_records(id) VALUES('00000000-0000-0000-0000-000000000001')", "23505");
        try rejected(ctx, "INSERT INTO casted_defaults(id) VALUES('00000000-0000-0000-0000-000000000001')", "23505");
        try command(ctx, "UPDATE generated_expr_records SET status=DEFAULT,amount=DEFAULT,fee=DEFAULT,metadata=DEFAULT,status_slug=DEFAULT,amount_cents=DEFAULT,source_key=DEFAULT WHERE id='00000000-0000-0000-0000-000000000002'");
        try expectRows(ctx, generated_read, &generated_labels, &.{
            generated_updated[0],
            &.{ "00000000-0000-0000-0000-000000000002", null, null, null, null, null, null, null },
            generated_updated[2],
        });
        try command(ctx, "UPDATE casted_defaults SET amount=DEFAULT,enabled=DEFAULT WHERE id='00000000-0000-0000-0000-000000000002'");
        try expectRows(ctx, casted_read, &casted_labels, &.{
            casted_initial[0],
            &.{ "00000000-0000-0000-0000-000000000002", "0", "true" },
            casted_initial[2],
            casted_initial[3],
        });
    }
};

test "mounted SQL original generated and casted tables own creation enforcement and restart" {
    // Exact original CREATE statements: sql-0671 and sql-0675. Never preinstall
    // schemas through the DB API or weaken their declared primary keys.
    try fixture.run(Campaign);
}

const DefinitionCopyCampaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    copied: Owner = undefined,
    source: Owner = undefined,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.user, users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
        try manager.removePermissionFromUser("pk-reader", "*", .table);
        var namespace = try users.Permission.initOwned(manager.alloc, .namespace, "default.public", .read);
        defer namespace.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-reader", namespace);
        for ([_][]const u8{ "reader_copy", "denied_copy", "reader_identity_copy", "reader_borrowed_copy" }) |name| {
            var permission = try users.Permission.initOwned(manager.alloc, .table, name, .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-reader", permission);
        }
    }

    fn create(ctx: *fixture.Context, sql: []const u8) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("LIKE creation status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    fn truncateShared(ctx: *fixture.Context, name: []const u8) !void {
        const statement = try std.fmt.allocPrint(ctx.alloc, "TRUNCATE {s} RESTART IDENTITY", .{name});
        defer ctx.alloc.free(statement);
        try replaceShared(ctx, name, statement);
        try expectColumnComment(ctx, name, "n", if (std.mem.eql(u8, name, "shared_source")) "source current" else "shared original");
        try primaryAnnotations(ctx, name, false);
    }

    fn replaceShared(ctx: *fixture.Context, name: []const u8, statement: []const u8) !void {
        const before = try fixture.waitServingSchema(ctx, name, 0);
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        var response = try client.execute(&coordinator, statement, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("shared default TRUNCATE table={s} status={d} body={s}\n", .{ name, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202 or response.status == 409);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const receipt = parsed.value.ddl_receipt orelse return error.SharedTruncateReceiptMissing;
        if (receipt.restore_job_id) |job_id| {
            const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
            while (true) {
                try ctx.checkDrivers();
                var observed = try fixture.job(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, job_id);
                defer observed.deinit(ctx.alloc);
                if (observed.status != 200) return error.SharedTruncateStatusUnavailable;
                var job = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, observed.body, .{ .ignore_unknown_fields = true });
                defer job.deinit();
                if (std.mem.eql(u8, job.value.phase, "succeeded")) break;
                if (std.mem.eql(u8, job.value.phase, "failed") or std.mem.eql(u8, job.value.phase, "canceled")) {
                    std.debug.print("shared default TRUNCATE failed table={s} body={s}\n", .{ name, observed.body });
                    return error.SharedTruncateFailed;
                }
                if (platform.time.monotonicNs() >= deadline) return error.SharedTruncateTimeout;
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
            }
        }
        const after = try fixture.waitServingSchema(ctx, name, 0);
        try std.testing.expect(before.table_id != after.table_id);
        try ready(ctx, name, .{ .id = after.table_id, .version = after.version });
    }

    fn primaryAnnotations(ctx: *fixture.Context, name: []const u8, write: bool) !void {
        return primaryAnnotationsFrom(ctx, name, name, write);
    }

    fn primaryAnnotationsFrom(ctx: *fixture.Context, name: []const u8, source: []const u8, write: bool) !void {
        const domain = local.system_catalog_domain;
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const captured = (try @import("sql_catalog.zig").captureTableAdmission(ctx.metadata.server.owned_public_http_server.?, null, .{}, a, .{ .table = name }, false, null)) orelse return error.MissingCopiedRelation;
        const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
        const table = (try store.getTable(a, ctx.metadata.server.svc.metadata_group_id, captured.guard.owner.table_id)) orelse return error.MissingCopiedRelation;
        const schema = try std.json.parseFromSliceLeaky(std.json.Value, a, table.schema_json, .{});
        const declarations = schema.object.get("unique_constraints") orelse return error.MissingPrimaryDeclaration;
        const primary = for (declarations.array.items) |declaration| {
            const flag = declaration.object.get("primary") orelse continue;
            if (flag == .bool and flag.bool) break declaration.object.get("name").?.string;
        } else return error.MissingPrimaryDeclaration;
        for ([_]domain.object_annotations.Kind{ .index, .constraint }) |kind| {
            const expected = try std.fmt.allocPrint(a, "{s} primary {s} Ω", .{ source, @tagName(kind) });
            if (write) {
                const quoted = try std.mem.replaceOwned(u8, a, primary, "\"", "\"\"");
                const statement = if (kind == .index)
                    try std.fmt.allocPrint(a, "COMMENT ON INDEX \"{s}\" IS '{s}'", .{ quoted, expected })
                else
                    try std.fmt.allocPrint(a, "COMMENT ON CONSTRAINT \"{s}\" ON \"{s}\" IS '{s}'", .{ quoted, name, expected });
                try command(ctx, statement);
            }
            const bytes = try store.getSqlObjectAnnotation(a, ctx.metadata.server.svc.metadata_group_id, .{ .guard = captured.guard, .key = .{ .kind = kind, .name = primary } });
            const note = try std.json.parseFromSliceLeaky(domain.object_annotation_store.Snapshot, a, bytes, .{});
            if (!write and !std.mem.eql(u8, source, name) and kind == .constraint) {
                try std.testing.expect(note.annotation == null);
            } else try std.testing.expectEqualStrings(expected, note.annotation.?.description.?);
        }
    }

    fn expectComment(ctx: *fixture.Context, name: []const u8, expected: ?[]const u8) !void {
        return expectColumnComment(ctx, name, "status", expected);
    }

    fn expectColumnComment(ctx: *fixture.Context, name: []const u8, column_name: []const u8, expected: ?[]const u8) !void {
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const captured = (try @import("sql_catalog.zig").captureTableAdmission(ctx.metadata.server.owned_public_http_server.?, null, .{}, a, .{ .table = name }, false, null)) orelse return error.MissingCopiedRelation;
        const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
        var attributes = try store.getSqlColumnIdentities(a, ctx.metadata.server.svc.metadata_group_id, captured.guard);
        defer attributes.deinit();
        const column = for (attributes.value.columns) |column| {
            if (std.mem.eql(u8, column.name, column_name)) break column;
        } else return error.MissingCopiedColumn;
        if (expected) |value| try std.testing.expectEqualStrings(value, column.description.?) else try std.testing.expect(column.description == null);
    }

    fn rows(ctx: *fixture.Context) !void {
        try expectComment(ctx, "usage_records_copy", "source status");
        try expectComment(ctx, "mixed_copy", "source status");
        try expectComment(ctx, "no_comments_copy", null);
        try expectRows(ctx, "SELECT id::text AS id,status FROM usage_records_copy ORDER BY id", &.{ "id", "status" }, &.{
            &.{ "00000000-0000-0000-0000-000000000009", "retained" },
            &.{ "00000000-0000-0000-0000-000000000010", "queued" },
        });
        try expectRows(ctx, "SELECT prefix,id::text AS id,status,suffix FROM mixed_copy", &.{ "prefix", "id", "status", "suffix" }, &.{&.{ null, "00000000-0000-0000-0000-000000000011", "queued", "done" }});
        try expectRows(ctx, "SELECT id::text AS id,status FROM policy_copy", &.{ "id", "status" }, &.{&.{ "00000000-0000-0000-0000-000000000012", "queued" }});
        try expectRows(ctx, "SELECT prefix,id::text AS id,status,middle::text AS middle,payload,tail::text AS tail FROM ordered_copy", &.{ "prefix", "id", "status", "middle", "payload", "tail" }, &.{&.{ null, "00000000-0000-0000-0000-000000000015", "queued", null, "payload", null }});
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try waitNamingPublication(ctx, "usage_records");
        try create(ctx, "CREATE TABLE usage_records (id uuid PRIMARY KEY, status text NOT NULL DEFAULT 'queued', CONSTRAINT status_nonempty CHECK (length(status)>0))");
        self.source = try owner(ctx, "usage_records");
        try ready(ctx, "usage_records", self.source);
        try command(ctx, "COMMENT ON COLUMN usage_records.status IS 'source status'");
        try command(ctx, "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000001','closed')");
        // Copied definitions/notes join the existing hidden FK publication.
        try primaryAnnotations(ctx, "usage_records", true);
        try create(ctx, "CREATE TABLE fk_copy_parent(id UUID PRIMARY KEY)");
        const parent = try owner(ctx, "fk_copy_parent");
        try ready(ctx, "fk_copy_parent", parent);
        try command(ctx, "INSERT INTO fk_copy_parent VALUES ('00000000-0000-0000-0000-000000000001'),('00000000-0000-0000-0000-000000000002')");
        try CopyLifetimeCampaign.createSource(ctx, "CREATE TABLE fk_usage_copy (LIKE usage_records INCLUDING ALL, FOREIGN KEY(id) REFERENCES fk_copy_parent(id))");
        const fk_copy = try owner(ctx, "fk_usage_copy");
        try ready(ctx, "fk_usage_copy", fk_copy);
        try primaryAnnotationsFrom(ctx, "fk_usage_copy", "usage_records", false);
        try expectColumnComment(ctx, "fk_usage_copy", "status", "source status");
        try command(ctx, "INSERT INTO fk_usage_copy(id) VALUES ('00000000-0000-0000-0000-000000000001')");
        try rejected(ctx, "INSERT INTO fk_usage_copy(id) VALUES ('00000000-0000-0000-0000-000000000099')", "23503");
        try rejected(ctx, "INSERT INTO fk_usage_copy(id) VALUES ('00000000-0000-0000-0000-000000000001')", "23505");
        try rejected(ctx, "INSERT INTO fk_usage_copy(id,status) VALUES ('00000000-0000-0000-0000-000000000002','')", "23514");

        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get("sql-0369");
        try std.testing.expectEqualStrings("CREATE TABLE usage_records_copy (LIKE usage_records INCLUDING ALL)", original.sql);
        try create(ctx, original.sql);
        self.copied = try owner(ctx, "usage_records_copy");
        try std.testing.expect(self.source.id != self.copied.id);
        try ready(ctx, "usage_records_copy", self.copied);

        try expectRows(ctx, "SELECT id::text AS id,status FROM usage_records_copy", &.{ "id", "status" }, &.{});
        try command(ctx, "INSERT INTO usage_records_copy VALUES ('00000000-0000-0000-0000-000000000009','retained')");
        try command(ctx, "INSERT INTO usage_records_copy(id) VALUES ('00000000-0000-0000-0000-000000000010')");
        try rejected(ctx, "INSERT INTO usage_records_copy VALUES ('00000000-0000-0000-0000-000000000009','duplicate')", "23505");
        try rejected(ctx, "INSERT INTO usage_records_copy VALUES ('00000000-0000-0000-0000-000000000013','')", "23514");
        try rejected(ctx, "INSERT INTO usage_records_copy VALUES ('00000000-0000-0000-0000-000000000013',NULL)", "23502");
        try create(ctx, "CREATE TABLE IF NOT EXISTS usage_records_copy (LIKE missing_source INCLUDING ALL)");
        try std.testing.expectEqualDeep(self.copied, try owner(ctx, "usage_records_copy"));
        try create(ctx, "CREATE TABLE mixed_copy (prefix text, LIKE usage_records INCLUDING ALL EXCLUDING INDEXES, suffix text DEFAULT 'done')");
        const mixed = try fixture.waitServingSchema(ctx, "mixed_copy", 0);
        try ready(ctx, "mixed_copy", .{ .id = mixed.table_id, .version = mixed.version });
        try command(ctx, "INSERT INTO mixed_copy(id) VALUES ('00000000-0000-0000-0000-000000000011')");
        try create(ctx, "CREATE TABLE no_comments_copy (LIKE usage_records EXCLUDING ALL)");
        const without_comments = try fixture.waitServingSchema(ctx, "no_comments_copy", 0);
        try ready(ctx, "no_comments_copy", .{ .id = without_comments.table_id, .version = without_comments.version });
        try create(ctx, "CREATE POLICY hidden_rows ON usage_records USING (false)");
        try create(ctx, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY");
        _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, self.source.id, .active, 0);
        try expectRows(ctx, "SELECT id::text AS id,status FROM usage_records", &.{ "id", "status" }, &.{});
        try create(ctx, "CREATE TABLE policy_copy (LIKE usage_records INCLUDING ALL)");
        const policy = try owner(ctx, "policy_copy");
        try ready(ctx, "policy_copy", policy);
        try command(ctx, "INSERT INTO policy_copy(id) VALUES ('00000000-0000-0000-0000-000000000012')");
        try create(ctx, "CREATE TABLE right_definition(payload text DEFAULT 'payload')");
        _ = try fixture.waitServingSchema(ctx, "right_definition", 0);
        try command(ctx, "INSERT INTO right_definition VALUES ('not copied')");
        try command(ctx, "COMMENT ON COLUMN right_definition.payload IS 'payload source'");
        try create(ctx, "CREATE TABLE ordered_copy(prefix text, LIKE usage_records INCLUDING ALL EXCLUDING INDEXES, middle int, LIKE right_definition INCLUDING DEFAULTS, tail jsonb)");
        const ordered = try fixture.waitServingSchema(ctx, "ordered_copy", 0);
        try ready(ctx, "ordered_copy", .{ .id = ordered.table_id, .version = ordered.version });
        var order_response = try client.execute(ctx, "SELECT * FROM ordered_copy LIMIT 0", .read);
        defer order_response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), order_response.status);
        var order = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, order_response.body, .{});
        defer order.deinit();
        const labels = [_][]const u8{ "prefix", "id", "status", "middle", "payload", "tail" };
        try std.testing.expectEqual(labels.len, order.value.columns.len);
        for (labels, order.value.columns) |label, column| try std.testing.expectEqualStrings(label, column.name);
        try command(ctx, "INSERT INTO ordered_copy(id) VALUES ('00000000-0000-0000-0000-000000000015')");
        try create(ctx, "CREATE ROLE definition_reader");
        try create(ctx, "GRANT SELECT ON usage_records TO definition_reader");
        try create(ctx, "GRANT definition_reader TO \"pk-reader\"");
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        try expectRows(&reader, "SELECT id::text AS id,status FROM usage_records", &.{ "id", "status" }, &.{});
        try create(&reader, "CREATE TABLE reader_copy (LIKE usage_records INCLUDING ALL)");
        const read_copy = try owner(ctx, "reader_copy");
        try ready(ctx, "reader_copy", read_copy);
        try command(ctx, "INSERT INTO reader_copy(id) VALUES ('00000000-0000-0000-0000-000000000014')");
        try create(ctx, "CREATE TABLE identity_source(id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 42 INCREMENT BY 3), quantity INT DEFAULT 4, doubled INT GENERATED ALWAYS AS (quantity * 2) STORED)");
        _ = try fixture.waitServingSchema(ctx, "identity_source", 0);
        try command(ctx, "INSERT INTO identity_source(quantity) VALUES (2)");
        try command(ctx, "ALTER SEQUENCE identity_source_id_seq AS SMALLINT");
        try create(ctx, "CREATE TABLE identity_copy (LIKE identity_source INCLUDING ALL)");
        _ = try fixture.waitServingSchema(ctx, "identity_copy", 0);
        try command(ctx, "INSERT INTO identity_copy(quantity) VALUES (9)");
        try command(ctx, "INSERT INTO identity_source(quantity) VALUES (1)");
        try expectRows(ctx, "SELECT id::text AS id,quantity::text AS quantity,doubled::text AS doubled FROM identity_source ORDER BY id", &.{ "id", "quantity", "doubled" }, &.{ &.{ "42", "2", "4" }, &.{ "45", "1", "2" } });
        try expectRows(ctx, "SELECT id::text AS id,quantity::text AS quantity,doubled::text AS doubled FROM identity_copy ORDER BY id", &.{ "id", "quantity", "doubled" }, &.{&.{ "42", "9", "18" }});
        try create(ctx, "GRANT SELECT ON identity_source TO definition_reader");
        try create(&reader, "CREATE TABLE reader_identity_copy (LIKE identity_source INCLUDING ALL)");
        _ = try fixture.waitServingSchema(ctx, "reader_identity_copy", 0);
        try command(&reader, "INSERT INTO reader_identity_copy(quantity) VALUES (5)");
        try expectRows(ctx, "SELECT id::text AS id,doubled::text AS doubled FROM reader_identity_copy", &.{ "id", "doubled" }, &.{&.{ "42", "10" }});
        try create(ctx, "CREATE TABLE shared_source(id TEXT PRIMARY KEY,n BIGSERIAL)");
        try ready(ctx, "shared_source", try owner(ctx, "shared_source"));
        try command(ctx, "COMMENT ON COLUMN shared_source.n IS 'shared original'");
        try command(ctx, "ALTER SEQUENCE shared_source_n_seq RENAME TO shared_counter");
        try primaryAnnotations(ctx, "shared_source", true);
        try create(ctx, "CREATE TABLE shared_copy(prefix TEXT,LIKE shared_source INCLUDING ALL)");
        try ready(ctx, "shared_copy", try owner(ctx, "shared_copy"));
        try expectColumnComment(ctx, "shared_copy", "n", "shared original");
        try primaryAnnotationsFrom(ctx, "shared_copy", "shared_source", false);
        try command(ctx, "COMMENT ON COLUMN shared_source.n IS 'source current'");
        try primaryAnnotations(ctx, "shared_source", true);
        try primaryAnnotations(ctx, "shared_copy", true);
        try command(ctx, "INSERT INTO shared_source(id) VALUES ('source')");
        try command(ctx, "INSERT INTO shared_copy(id) VALUES ('copy')");
        try expectRows(ctx, "SELECT n::text AS n FROM shared_source", &.{"n"}, &.{&.{"1"}});
        try expectRows(ctx, "SELECT n::text AS n FROM shared_copy", &.{"n"}, &.{&.{"2"}});
        try truncateShared(ctx, "shared_copy");
        try command(ctx, "INSERT INTO shared_copy(id) VALUES ('borrower_restart')");
        try expectRows(ctx, "SELECT n::text AS n FROM shared_copy", &.{"n"}, &.{&.{"3"}});
        try truncateShared(ctx, "shared_source");
        try command(ctx, "INSERT INTO shared_copy(id) VALUES ('owner_restart')");
        try expectRows(ctx, "SELECT n::text AS n FROM shared_copy WHERE id='owner_restart'", &.{"n"}, &.{&.{"1"}});
        try command(ctx, "ALTER SEQUENCE shared_counter OWNED BY NONE");
        try truncateShared(ctx, "shared_source");
        try command(ctx, "INSERT INTO shared_source(id) VALUES ('unowned_restart')");
        try expectRows(ctx, "SELECT n::text AS n FROM shared_source", &.{"n"}, &.{&.{"2"}});
        try create(ctx, "GRANT SELECT ON shared_source TO definition_reader");
        try create(&reader, "CREATE TABLE reader_borrowed_copy (LIKE shared_source INCLUDING ALL)");
        try ready(ctx, "reader_borrowed_copy", try owner(ctx, "reader_borrowed_copy"));
        try primaryAnnotationsFrom(ctx, "reader_borrowed_copy", "shared_source", false);
        try ready(ctx, "reader_borrowed_copy", try owner(ctx, "reader_borrowed_copy"));
        {
            // CREATE LIKE needs source-table SELECT, not sequence usage.
            // Ordinary copied defaults must still enforce sequence permission
            // at allocation; table ownership grants no implicit identity right.
            var denied_default = try client.execute(&reader, "INSERT INTO reader_borrowed_copy(id) VALUES ('denied')", .write_admission);
            defer denied_default.deinit(ctx.alloc);
            if (denied_default.status != 403) std.debug.print("borrowed counter permission status={d} body={s}\n", .{ denied_default.status, denied_default.body });
            try std.testing.expectEqual(@as(u16, 403), denied_default.status);
            try command(&reader, "INSERT INTO reader_borrowed_copy(id,n) VALUES ('explicit',77)");
        }
        try create(ctx, "CREATE TABLE rename_source(id TEXT PRIMARY KEY,n BIGSERIAL)");
        try ready(ctx, "rename_source", try owner(ctx, "rename_source"));
        try command(ctx, "COMMENT ON COLUMN rename_source.n IS 'rename number'");
        try create(ctx, "CREATE TABLE rename_borrower(LIKE rename_source INCLUDING ALL)");
        try ready(ctx, "rename_borrower", try owner(ctx, "rename_borrower"));
        try command(ctx, "INSERT INTO rename_source(id) VALUES ('before')");
        try command(ctx, "INSERT INTO rename_borrower(id) VALUES ('before')");
        try replaceShared(ctx, "rename_borrower", "ALTER TABLE rename_borrower RENAME COLUMN n TO value");
        try replaceShared(ctx, "rename_source", "ALTER TABLE rename_source RENAME COLUMN n TO value");
        try command(ctx, "INSERT INTO rename_source(id) VALUES ('after')");
        try command(ctx, "INSERT INTO rename_borrower(id) VALUES ('after')");
        try expectRows(ctx, "SELECT value::text AS n FROM rename_source ORDER BY id", &.{"n"}, &.{ &.{"3"}, &.{"1"} });
        try expectRows(ctx, "SELECT value::text AS n FROM rename_borrower ORDER BY id", &.{"n"}, &.{ &.{"4"}, &.{"2"} });
        try expectColumnComment(ctx, "rename_source", "value", "rename number");
        try expectColumnComment(ctx, "rename_borrower", "value", "rename number");
        try create(ctx, "CREATE TABLE rename_identity(id TEXT PRIMARY KEY,n BIGINT GENERATED ALWAYS AS IDENTITY)");
        try ready(ctx, "rename_identity", try owner(ctx, "rename_identity"));
        try command(ctx, "INSERT INTO rename_identity(id) VALUES ('before')");
        try replaceShared(ctx, "rename_identity", "ALTER TABLE rename_identity RENAME COLUMN n TO value");
        try command(ctx, "INSERT INTO rename_identity(id) VALUES ('after')");
        try expectRows(ctx, "SELECT value::text AS n FROM rename_identity ORDER BY id", &.{"n"}, &.{ &.{"2"}, &.{"1"} });
        try create(ctx, "REVOKE SELECT ON usage_records FROM definition_reader");
        var rejected_copy = try client.execute(&reader, "CREATE TABLE denied_copy (LIKE usage_records INCLUDING ALL)", .write_admission);
        defer rejected_copy.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), rejected_copy.status);
        try create(&reader, "CREATE TABLE IF NOT EXISTS reader_copy (LIKE missing_source INCLUDING ALL)");
        try replaceShared(ctx, "right_definition", "ALTER TABLE right_definition RENAME COLUMN payload TO body");
        try expectColumnComment(ctx, "right_definition", "body", "payload source");
        try expectRows(ctx, "SELECT body FROM right_definition", &.{"body"}, &.{&.{"not copied"}});
        try command(ctx, "COMMENT ON COLUMN usage_records.status IS NULL");
        try rows(ctx);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try ready(ctx, "usage_records_copy", self.copied);
        const fk_copy = try owner(ctx, "fk_usage_copy");
        try ready(ctx, "fk_usage_copy", fk_copy);
        try primaryAnnotationsFrom(ctx, "fk_usage_copy", "usage_records", false);
        try expectColumnComment(ctx, "fk_usage_copy", "status", "source status");
        try expectRows(ctx, "SELECT id::text AS id,status FROM fk_usage_copy", &.{ "id", "status" }, &.{&.{ "00000000-0000-0000-0000-000000000001", "queued" }});
        try rejected(ctx, "INSERT INTO fk_usage_copy(id) VALUES ('00000000-0000-0000-0000-000000000099')", "23503");

        try std.testing.expectEqualDeep(self.copied, try owner(ctx, "usage_records_copy"));
        try expectColumnComment(ctx, "shared_source", "n", "source current");
        try expectColumnComment(ctx, "shared_copy", "n", "shared original");
        try primaryAnnotations(ctx, "shared_source", false);
        try primaryAnnotations(ctx, "shared_copy", false);
        try expectColumnComment(ctx, "right_definition", "body", "payload source");
        try command(ctx, "INSERT INTO rename_source(id) VALUES ('cold')");
        try command(ctx, "INSERT INTO rename_borrower(id) VALUES ('cold')");
        try command(ctx, "INSERT INTO rename_identity(id) VALUES ('cold')");
        try expectRows(ctx, "SELECT value::text AS n FROM rename_source WHERE id='cold'", &.{"n"}, &.{&.{"5"}});
        try expectRows(ctx, "SELECT value::text AS n FROM rename_borrower WHERE id='cold'", &.{"n"}, &.{&.{"6"}});
        try expectRows(ctx, "SELECT value::text AS n FROM rename_identity WHERE id='cold'", &.{"n"}, &.{&.{"3"}});
        try expectColumnComment(ctx, "rename_source", "value", "rename number");
        try expectColumnComment(ctx, "rename_borrower", "value", "rename number");
        try command(ctx, "INSERT INTO shared_copy(id) VALUES ('cold_copy')");
        try command(ctx, "INSERT INTO shared_source(id) VALUES ('cold_source')");
        try expectRows(ctx, "SELECT n::text AS n FROM shared_copy WHERE id='cold_copy'", &.{"n"}, &.{&.{"3"}});
        try expectRows(ctx, "SELECT n::text AS n FROM shared_source WHERE id='cold_source'", &.{"n"}, &.{&.{"4"}});
        try command(ctx, "INSERT INTO identity_copy(quantity) VALUES (7)");
        try command(ctx, "INSERT INTO identity_source(quantity) VALUES (3)");
        try expectRows(ctx, "SELECT id::text AS id,quantity::text AS quantity,doubled::text AS doubled FROM identity_copy ORDER BY id", &.{ "id", "quantity", "doubled" }, &.{ &.{ "42", "9", "18" }, &.{ "45", "7", "14" } });
        try expectRows(ctx, "SELECT id::text AS id,quantity::text AS quantity,doubled::text AS doubled FROM identity_source ORDER BY id", &.{ "id", "quantity", "doubled" }, &.{ &.{ "42", "2", "4" }, &.{ "45", "1", "2" }, &.{ "48", "3", "6" } });
        try rows(ctx);
        try rejected(ctx, "INSERT INTO usage_records_copy VALUES ('00000000-0000-0000-0000-000000000009','duplicate')", "23505");
        try rejected(ctx, "INSERT INTO usage_records_copy VALUES ('00000000-0000-0000-0000-000000000013','')", "23514");
    }
};

test "mounted SQL LIKE declaration copies retain grants constraints isolation and cold owners" {
    // sql-0369 is loaded unchanged from Corpus in beforeRestart; the campaign
    // checks its exact SQL before submitting it through the public API.
    try fixture.run(DefinitionCopyCampaign);
}

/// The coupled ownership lifetime from the independent PostgreSQL LIKE oracle.
/// Dropping a borrower removes only its default edge and fresh identity; the
/// original SERIAL owner may retire only after the last borrower disappears.
const CopyLifetimeCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    keys: client.ApiKeyHeaders = .{},
    source: u64 = 0,
    copy: u64 = 0,
    fk_copy: u64 = 0,

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }

    fn awaitReady(ctx: *fixture.Context, name: []const u8) !u64 {
        const current = try fixture.waitServingSchema(ctx, name, 0);
        try ready(ctx, name, .{ .id = current.table_id, .version = current.version });
        return current.table_id;
    }

    fn sharedCounter(ctx: *fixture.Context) !@import("../system_catalog/sequence_catalog.zig").sequence.State {
        const sequences = @import("../system_catalog/sequence_catalog.zig");
        var region = std.heap.ArenaAllocator.init(ctx.alloc);
        defer region.deinit();
        const a = region.allocator();
        const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const request: sequences.SnapshotRequest = .{ .targets = &.{.{ .table = "lifetime_source_serial_id_seq" }}, .include_counters = true };
        const bytes = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_sequence_snapshot = request });
        const snapshot = try std.json.parseFromSliceLeaky(sequences.Snapshot, a, bytes, .{});
        try snapshot.validate(request);
        if (snapshot.entries[0].value != .sequence) return error.MissingSharedCounter;
        return snapshot.entries[0].counter orelse error.MissingSharedCounterState;
    }

    fn drop(ctx: *fixture.Context, name: []const u8) !void {
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        const sql = try std.fmt.allocPrint(ctx.alloc, "DROP TABLE {s}", .{name});
        defer ctx.alloc.free(sql);
        var response = try client.execute(&coordinator, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("LIKE retirement sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(if (response.status == 200) "DROP TABLE" else "DDL PENDING", parsed.value.command_tag);
        const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
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
        return error.CopyRetirementTimeout;
    }

    fn createSource(ctx: *fixture.Context, statement: []const u8) !void {
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        var response = try client.execute(&coordinator, statement, .write_admission);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("combined FK/counter CREATE status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202 or response.status == 409);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const receipt = parsed.value.ddl_receipt orelse return error.MissingInitialCounterReceipt;
        if (response.status == 409) try std.testing.expect(receipt.state == .admission_unknown);
        const token = receipt.fk_generation_publication_id orelse return error.MissingInitialCounterToken;
        const table_id = try std.fmt.parseInt(u64, receipt.table_id, 10);
        const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        while (platform.time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            const bytes = source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline, .fk_generation_publication_authority = true }, .{ .fk_initial_create_status = table_id }) catch |err| switch (err) {
                error.GenerationPublicationNotFound => {
                    try ctx.io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                },
                else => return err,
            };
            defer ctx.alloc.free(bytes);
            var publication = try std.json.parseFromSlice(@import("../metadata/fk_generation_publication.zig").InitialPublication, ctx.alloc, bytes, .{});
            defer publication.deinit();
            try publication.value.validateState(ctx.alloc);
            try std.testing.expectEqualStrings(token, &std.fmt.bytesToHex(publication.value.plan.id, .lower));
            if (publication.value.phase == .published) return;
            if (publication.value.phase == .canceled) return error.InitialCounterCreateCanceled;
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.InitialCounterCreateTimeout;
    }

    fn read(ctx: *fixture.Context, name: []const u8, expected: []const []const ?[]const u8) !void {
        const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT id::text AS id,serial_id::text AS serial_id,doubled::text AS doubled FROM {s} ORDER BY id", .{name});
        defer ctx.alloc.free(sql);
        try expectRows(ctx, sql, &.{ "id", "serial_id", "doubled" }, expected);
    }

    fn indexComment(ctx: *fixture.Context) !void {
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const captured = (try @import("sql_catalog.zig").captureTableAdmission(ctx.metadata.server.owned_public_http_server.?, null, .{}, a, .{ .table = "lifetime_copy" }, false, null)) orelse return error.MissingCopy;
        const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
        const table = (try store.getTable(a, ctx.metadata.server.svc.metadata_group_id, captured.guard.owner.table_id)) orelse return error.MissingCopy;
        try std.testing.expectEqualStrings("", table.description);
        const bytes = try store.getSqlObjectAnnotation(a, ctx.metadata.server.svc.metadata_group_id, .{ .guard = captured.guard, .key = .{ .kind = .index, .name = "lifetime_copy_quantity_key" } });
        const note = try std.json.parseFromSliceLeaky(local.system_catalog_domain.object_annotation_store.Snapshot, a, bytes, .{});
        try std.testing.expectEqualStrings("copied index comment", note.annotation.?.description.?);
        try DefinitionCopyCampaign.expectColumnComment(ctx, "lifetime_copy", "quantity", "copied quantity");
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.keys.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try waitNamingPublication(ctx, "lifetime_parent");
        try DefinitionCopyCampaign.create(ctx, "CREATE TABLE lifetime_parent(id INT PRIMARY KEY)");
        _ = try awaitReady(ctx, "lifetime_parent");
        try command(ctx, "INSERT INTO lifetime_parent VALUES (1),(2)");
        try createSource(ctx, "CREATE TABLE lifetime_source(id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 42 INCREMENT BY 3),serial_id BIGSERIAL,quantity INT NOT NULL DEFAULT 2,doubled INT GENERATED ALWAYS AS (quantity * 2) STORED,PRIMARY KEY(id),UNIQUE(quantity) DEFERRABLE INITIALLY DEFERRED,CHECK(quantity > 0),FOREIGN KEY(quantity) REFERENCES lifetime_parent(id))");
        self.source = try awaitReady(ctx, "lifetime_source");
        try command(ctx, "COMMENT ON TABLE lifetime_source IS 'source table only'");
        try command(ctx, "COMMENT ON COLUMN lifetime_source.quantity IS 'copied quantity'");
        try command(ctx, "COMMENT ON INDEX lifetime_source_quantity_key IS 'copied index comment'");
        try command(ctx, "INSERT INTO lifetime_source DEFAULT VALUES");
        try DefinitionCopyCampaign.create(ctx, "CREATE TABLE lifetime_copy(LIKE lifetime_source INCLUDING ALL)");
        self.copy = try awaitReady(ctx, "lifetime_copy");
        try std.testing.expect(self.source != self.copy);
        try read(ctx, "lifetime_copy", &.{});
        try indexComment(ctx);
        // This value is absent from the source FK parent: LIKE excludes FKs.
        try command(ctx, "INSERT INTO lifetime_copy(quantity) VALUES (9)");
        try command(ctx, "INSERT INTO lifetime_source(quantity) VALUES (1)");
        try read(ctx, "lifetime_copy", &.{&.{ "42", "2", "18" }});
        try read(ctx, "lifetime_source", &.{ &.{ "42", "1", "4" }, &.{ "45", "3", "2" } });
        const before_prepare = try sharedCounter(ctx);
        const prepare_body = "{\"statement\":\"INSERT INTO lifetime_copy(quantity) VALUES (NULL)\"}";
        var prepared = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/prepared", .POST, prepare_body, 15_000);
        defer prepared.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), prepared.status);
        var prepared_id = try std.json.parseFromSlice(struct { prepared_id: []const u8 }, ctx.alloc, prepared.body, .{ .ignore_unknown_fields = true });
        defer prepared_id.deinit();
        const prepared_path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}", .{prepared_id.value.prepared_id});
        defer ctx.alloc.free(prepared_path);
        const execute_path = try std.fmt.allocPrint(ctx.alloc, "{s}/execute", .{prepared_path});
        defer ctx.alloc.free(execute_path);
        try std.testing.expectEqualDeep(before_prepare, try sharedCounter(ctx));
        for (0..2) |attempt| {
            var execution = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, execute_path, .POST, "{}", 15_000);
            defer execution.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 400), execution.status);
            var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, execution.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            if (!std.mem.eql(u8, "23502", diagnostic.value.code)) std.debug.print("prepared NULL insert status={d} body={s}\n", .{ execution.status, execution.body });
            try std.testing.expectEqualStrings("23502", diagnostic.value.code);
            const allocated = try sharedCounter(ctx);
            try std.testing.expectEqual(before_prepare.last_value + @as(i64, @intCast(attempt + 1)), allocated.last_value);
            try std.testing.expectEqual(before_prepare.revision + attempt + 1, allocated.revision);
        }
        var closed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, prepared_path, .DELETE, null, 15_000);
        defer closed.deinit(ctx.alloc);
        try std.testing.expect(closed.status == 200 or closed.status == 204);
        try rejected(ctx, "INSERT INTO lifetime_copy(quantity) VALUES (NULL)", "23502");
        try rejected(ctx, "INSERT INTO lifetime_copy(quantity) VALUES (-1)", "23514");
        try rejected(ctx, "INSERT INTO lifetime_copy(quantity) VALUES (9)", "23505");
        try std.testing.expectEqual(@as(i64, 8), (try sharedCounter(ctx)).last_value);
        // Reusing the cached validator must still prepare defaults per row.
        try command(ctx, "INSERT INTO lifetime_copy(quantity) VALUES (2),(3)");
        try std.testing.expectEqual(@as(i64, 10), (try sharedCounter(ctx)).last_value);
        try rejected(ctx, "INSERT INTO lifetime_copy(quantity) VALUES (4),(NULL),(5)", "23502");
        // Two defaults survive the rejected statement; its third row is never
        // evaluated and its valid first row must not become visible.
        try std.testing.expectEqual(@as(i64, 12), (try sharedCounter(ctx)).last_value);
        try read(ctx, "lifetime_copy", &.{ &.{ "42", "2", "18" }, &.{ "60", "9", "4" }, &.{ "63", "10", "6" } });
        try rejected(ctx, "DROP TABLE lifetime_source", "2BP01");
        // The rejected retirement cannot detach either default or owner edge.
        try read(ctx, "lifetime_source", &.{ &.{ "42", "1", "4" }, &.{ "45", "3", "2" } });
        // One hidden publication combines a copied fresh identity, a shared
        // SERIAL default, generated columns, deferred UNIQUE, CHECK and FK.
        try createSource(ctx, "CREATE TABLE lifetime_fk_copy(LIKE lifetime_source INCLUDING ALL, FOREIGN KEY(quantity) REFERENCES lifetime_parent(id))");
        self.fk_copy = try awaitReady(ctx, "lifetime_fk_copy");
        try std.testing.expect(self.fk_copy != self.source and self.fk_copy != self.copy);
        try read(ctx, "lifetime_fk_copy", &.{});
        try DefinitionCopyCampaign.expectColumnComment(ctx, "lifetime_fk_copy", "quantity", "copied quantity");
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try std.testing.expectEqual(self.source, try awaitReady(ctx, "lifetime_source"));
        try std.testing.expectEqual(self.copy, try awaitReady(ctx, "lifetime_copy"));
        try indexComment(ctx);
        try read(ctx, "lifetime_copy", &.{ &.{ "42", "2", "18" }, &.{ "60", "9", "4" }, &.{ "63", "10", "6" } });
        try std.testing.expectEqual(self.fk_copy, try awaitReady(ctx, "lifetime_fk_copy"));
        try read(ctx, "lifetime_fk_copy", &.{});
        try command(ctx, "INSERT INTO lifetime_fk_copy DEFAULT VALUES");
        try read(ctx, "lifetime_fk_copy", &.{&.{ "42", "13", "4" }});
        try DefinitionCopyCampaign.expectColumnComment(ctx, "lifetime_fk_copy", "quantity", "copied quantity");
        try rejected(ctx, "DROP TABLE lifetime_source", "2BP01");
        const with_fk = try sharedCounter(ctx);
        try drop(ctx, "lifetime_fk_copy");
        try std.testing.expectEqualDeep(with_fk, try sharedCounter(ctx));
        try rejected(ctx, "SELECT nextval('lifetime_fk_copy_id_seq')", "42P01");
        try rejected(ctx, "DROP TABLE lifetime_source", "2BP01");
        const shared_before = try sharedCounter(ctx);
        try drop(ctx, "lifetime_copy");
        try std.testing.expectEqualDeep(shared_before, try sharedCounter(ctx));
        try rejected(ctx, "SELECT nextval('lifetime_copy_id_seq')", "42P01");
        // A copied identity is independently owned; its retirement must not
        // remove or reset the source identity or the shared SERIAL counter.
        try expectRows(ctx, "SELECT nextval('lifetime_source_id_seq')::text AS id", &.{"id"}, &.{&.{"48"}});
        try read(ctx, "lifetime_source", &.{ &.{ "42", "1", "4" }, &.{ "45", "3", "2" } });
        try drop(ctx, "lifetime_source");
        // Both source-owned counters are retired, while the external FK
        // parent has completed reference coverage and remains writable.
        try rejected(ctx, "SELECT nextval('lifetime_source_serial_id_seq')", "42P01");
        try rejected(ctx, "SELECT nextval('lifetime_source_id_seq')", "42P01");
        try command(ctx, "DELETE FROM lifetime_parent");
        try command(ctx, "INSERT INTO lifetime_parent VALUES (9)");
        // Reusing the logical name must allocate a fresh catalog identity.
        try DefinitionCopyCampaign.create(ctx, "CREATE TABLE lifetime_source(id TEXT PRIMARY KEY)");
        const recreated = try awaitReady(ctx, "lifetime_source");
        try std.testing.expect(recreated != self.source);
        try command(ctx, "INSERT INTO lifetime_source VALUES ('new owner')");
        const svc = ctx.metadata.server.svc;
        const store = svc.projectedStore() orelse return error.MissingMetadataStore;
        const checkpoint = try store.snapshotBuilder().buildSnapshot(ctx.alloc, svc.metadata_group_id);
        defer ctx.alloc.free(checkpoint);
        try std.testing.expect(checkpoint.len != 0);
    }
};

test "mounted SQL LIKE full ownership lifetime retires borrowed defaults and fresh identities through cold owners" {
    try fixture.run(CopyLifetimeCampaign);
}
