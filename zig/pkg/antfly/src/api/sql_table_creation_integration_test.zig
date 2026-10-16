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
