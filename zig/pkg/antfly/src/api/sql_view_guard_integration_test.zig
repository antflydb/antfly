// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Public document views replace private index mappings with explicit SQL
//! casts/paths and durable source/column identities. Never qualify these guards
//! by merely rejecting an unresolved support_view name.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const views = @import("../system_catalog/views.zig");
const domain = local.system_catalog_domain;
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;
const bytes = @embedFile("fixtures/sql_view_guard_reference.json");
const Result = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Reference = struct {
    number_view: []const u8,
    plan_view: []const u8,
    entries: []const struct { id: []const u8, sql: []const u8, sqlstate: ?[]const u8 = null, result: ?Result = null },
    valid_numeric_result: Result,
};
fn query(ctx: *fixture.Context, sql: []const u8) !@import("../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    authenticated.base = ctx.metadata_uri;
    return client.execute(&authenticated, sql, .read);
}
fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var response = try query(ctx, sql);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("view guard command sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        return error.ViewGuardCommandFailed;
    }
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    if (parsed.value.catalog_receipt) |receipt| try awaitCatalogReceipt(ctx, receipt);
}
fn awaitCatalogReceipt(ctx: *fixture.Context, expected: wire.SQLCatalogOperationReceipt) !void {
    if (expected.state == .committed) return;
    if (expected.state == .rejected) return error.ViewGuardCatalogRejected;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, wire.SQLCatalogRecoveryRequest{ .recovery_token = expected.recovery_token }, .{});
    defer ctx.alloc.free(body);
    const time = @import("antfly_platform").time;
    const deadline = time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const remaining_ms = @max(1, (deadline -| time.monotonicNs()) / std.time.ns_per_ms);
        var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/sql/operations/status", .POST, body, @intCast(@min(remaining_ms, 5000))) catch |err| switch (err) {
            error.Timeout, error.DeadlineExceeded => continue,
            else => return err,
        };
        defer response.deinit(ctx.alloc);
        if (response.status == 503 or response.status == 504) {
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
            continue;
        }
        if (response.status != 200) std.debug.print("view guard catalog observation status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var observed = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
        defer observed.deinit();
        const actual = observed.value;
        try std.testing.expectEqualStrings(expected.recovery_token, actual.recovery_token);
        try std.testing.expectEqual(expected.kind, actual.kind);
        try std.testing.expectEqual(expected.operation, actual.operation);
        inline for (.{ "database", "namespace", "name" }) |field| try std.testing.expectEqualStrings(@field(expected, field), @field(actual, field));
        switch (actual.state) {
            .committed => {
                _ = try std.fmt.parseUnsigned(u64, actual.catalog_revision orelse return error.MissingCatalogRevision, 10);
                return;
            },
            .rejected => {
                std.debug.print("view guard catalog operation rejected body={s}\n", .{response.body});
                return error.ViewGuardCatalogRejected;
            },
            .pending, .unknown => {},
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ViewGuardCatalogCompletionTimeout;
}
fn expectQuery(ctx: *fixture.Context, sql: []const u8, expected: ?Result, state: ?[]const u8) !void {
    var response = try query(ctx, sql);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("view guard query sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    if (state) |code| {
        try std.testing.expect(response.status >= 400);
        var parsed = try std.json.parseFromSlice(Json, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(code, (try fixture.field(parsed.value, "code")).string);
        return;
    }
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const want = expected.?;
    const actual = parsed.value;
    try std.testing.expectEqual(want.rows.len, actual.rows.len);
    try std.testing.expectEqual(want.columns.len, actual.columns.len);
    for (actual.columns, want.columns, want.column_oids) |column, name, oid| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(column, oid));
    }
    const used = try ctx.alloc.alloc(bool, want.rows.len);
    defer ctx.alloc.free(used);
    @memset(used, false);
    for (actual.rows, actual.sql_nulls.?) |row, nulls| {
        const found = for (want.rows, want.sql_nulls, 0..) |want_row, want_nulls, i| {
            if (used[i]) continue;
            if (!try @import("sql_parity_reference.zig").rowMatchesWithNulls(ctx.alloc, actual.columns, row, nulls, want_row, want_nulls)) continue;
            used[i] = true;
            break true;
        } else false;
        try std.testing.expect(found);
    }
}
fn resolve(ctx: *fixture.Context, name: []const u8) !std.json.Parsed(domain.ResolvedMany) {
    var transport = ctx.catalogClient();
    const response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = name } }}, .include_query_definitions = true } }, 15_000, null);
    defer response.deinit(ctx.alloc);
    return std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, response.body, .{});
}
fn publish(ctx: *fixture.Context, definition: local.sql_view_definition.Definition, name: []const u8) !void {
    const native = @import("../system_catalog/catalog_mutations.zig");
    const Command = @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand;
    var current = try resolve(ctx, "support_view");
    defer current.deinit();
    const intent: views.Request = .{ .expected_revision = current.value.revision, .mutation = .{ .target = .{ .table = name }, .operation = .{ .publish = .{ .definition = definition } } } };
    var nonce: [16]u8 = undefined;
    try ctx.io.randomSecure(&nonce);
    var transport = ctx.catalogClient();
    // Use the same prepared native journal as public CREATE VIEW. Legacy
    // sql_view_mutate refusal cannot serve as evidence of a source guard.
    const prepared = try transport.readSystemCatalog(ctx.metadata_uri, .{ .sql_catalog_prepare = .{ .request = .{ .mutation = views.command(intent).mutation }, .nonce = nonce, .view = intent } }, 15_000, null);
    defer prepared.deinit(ctx.alloc);
    var prepared_command = try std.json.parseFromSlice(Command, ctx.alloc, prepared.body, .{});
    defer prepared_command.deinit();
    try std.testing.expect(try native.matchesViewIntent(ctx.alloc, prepared_command.value.sql_view, intent.mutation));
    try std.testing.expectEqual(current.value.revision, prepared_command.value.expected_revision);
    const observation = try native.observation(ctx.alloc, prepared_command.value);
    const forwarding = @import("raft_mutation_forwarding.zig");
    const output = try transport.forwardSystemCatalog(ctx.metadata_uri, .{ .sql_catalog_mutate = prepared_command.value }, .{ .remaining_ms = forwarding.max_remaining_ms, .forwards_remaining = forwarding.max_forwards, .campaign_allowed = true }, true);
    defer ctx.alloc.free(output);
    var outcome = try std.json.parseFromSlice(native.Outcome, ctx.alloc, output, .{});
    defer outcome.deinit();
    // Independently read the body-bound durable outcome. HTTP acceptance alone
    // proves neither semantic rejection nor successful native publication.
    const retained = try transport.readSystemCatalog(ctx.metadata_uri, .{ .sql_catalog_outcome = observation }, 15_000, null);
    defer retained.deinit(ctx.alloc);
    var recovered = try std.json.parseFromSlice(native.Outcome, ctx.alloc, retained.body, .{});
    defer recovered.deinit();
    try std.testing.expectEqualDeep(outcome.value, recovered.value);
    _ = try recovered.value.result();
}
fn evolveSource(ctx: *fixture.Context, old_version: u32) !void {
    var current = try resolve(ctx, "docs");
    defer current.deinit();
    const definition = current.value.sql_sources[0].?.value.table.binding.query_definition.?;
    var schema = try std.json.parseFromSlice(Json, ctx.alloc, definition.schema_json, .{});
    defer schema.deinit();
    const a = schema.arena.allocator();
    _ = schema.value.object.orderedRemove("version");
    const properties = schema.value.object.getPtr("document_schemas").?.object.getPtr("doc").?.object.getPtr("schema").?.object.getPtr("properties").?;
    var extra: std.json.ObjectMap = .empty;
    try extra.put(a, "type", .{ .string = "keyword" });
    try properties.object.put(a, "extra", .{ .object = extra });
    var request = try std.json.parseFromValue(@import("antfly_schema_openapi").types.TableSchema, ctx.alloc, schema.value, .{});
    defer request.deinit();
    try std.testing.expect(request.value.storage_mode.? == .document);
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, request.value, .{ .emit_null_optional_fields = false });
    defer ctx.alloc.free(body);
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/tables/docs/schema", .PUT, body);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("document view source evolution status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    _ = try fixture.waitServingSchema(ctx, "docs", old_version + 1);
    try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
    // Ordinary document schema PUT returns TableStatus, not a schema-validation
    // receipt. A serving write version can advance while the old read layout
    // still owns a migration. Observe that exact public lifecycle to completion
    // before admitting another DDL; never replay DROP on arbitrary 55006.
    const time = @import("antfly_platform").time;
    const deadline = time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var observed = try fixture.table(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "docs");
        defer observed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), observed.status);
        var status = try std.json.parseFromSlice(wire.TableStatus, ctx.alloc, observed.body, .{});
        defer status.deinit();
        try std.testing.expectEqual(current.value.sql_sources[0].?.value.table.binding.table_id, try std.fmt.parseUnsigned(u64, status.value.table_id.?, 10));
        try std.testing.expectEqual(old_version + 1, status.value.schema.?.version.?);
        try std.testing.expect(status.value.schema.?.storage_mode.? == .document);
        if (status.value.migration == null) return;
        if (std.mem.eql(u8, status.value.migration.?.state, "failed")) {
            std.debug.print("document view source migration failed body={s}\n", .{observed.body});
            return error.ViewSourceMigrationFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ViewSourceMigrationTimeout;
}
fn rejectedPublication(ctx: *fixture.Context, definition: local.sql_view_definition.Definition, expected: anyerror) !void {
    var before = try resolve(ctx, "guard_probe");
    defer before.deinit();
    try std.testing.expect(before.value.sql_sources[0] == null);
    try std.testing.expectError(expected, publish(ctx, definition, "guard_probe"));
    var after = try resolve(ctx, "guard_probe");
    defer after.deinit();
    try std.testing.expectEqual(before.value.revision, after.value.revision);
    try std.testing.expect(after.value.sql_sources[0] == null);
}
const State = struct { stale_definition: ?[]u8 = null };
const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    state: *State,
    credentials: client.ApiKeyHeaders = .{},
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn createSource(ctx: *fixture.Context) !u64 {
        // The private corpus used `type:json`, now a relational-only physical
        // declaration. An unconstrained JSON Schema property preserves that
        // arbitrary-JSON domain at public document ingress; object would narrow it.
        const declaration =
            \\{"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"metadata":{}},"additionalProperties":true}}}}
        ;
        var schema = try std.json.parseFromSlice(@import("antfly_schema_openapi").types.TableSchema, ctx.alloc, declaration, .{});
        defer schema.deinit();
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, wire.CreateTableRequest{ .schema = schema.value }, .{ .emit_null_optional_fields = false });
        defer ctx.alloc.free(body);
        // Exercise the accepting production parser before any network effects.
        var admitted = try @import("table_contract.zig").parseCreateTableRequest(ctx.alloc, body);
        defer admitted.deinit(ctx.alloc);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "/db/v1/tables/docs", .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 200 and created.status != 201) std.debug.print("view source status={d} body={s}\n", .{ created.status, created.body });
        try std.testing.expect(created.status == 200 or created.status == 201);
        var observed = try fixture.table(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, "docs");
        defer observed.deinit(ctx.alloc);
        var parsed = try std.json.parseFromSlice(Json, ctx.alloc, observed.body, .{});
        defer parsed.deinit();
        const id = try std.fmt.parseUnsigned(u64, (try fixture.field(parsed.value, "table_id")).string, 10);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "docs", "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"metadata\":{\"plan\":\"pro\"}},\"doc:b\":{\"title\":\"17\",\"metadata\":{\"plan\":\"free\"}}},\"sync_level\":\"full_text\"}");
        defer seeded.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), seeded.status);
        return id;
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        var parsed = try std.json.parseFromSlice(Reference, ctx.alloc, bytes, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const reference = parsed.value;
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (reference.entries) |entry| try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
        const old_id = try createSource(ctx);
        try command(ctx, reference.number_view);
        try expectQuery(ctx, reference.entries[0].sql, null, reference.entries[0].sqlstate);
        try command(ctx, "UPDATE docs SET _doc=jsonb_set(_doc,'{title}','\"23\"') WHERE _id='doc:a'");
        try expectQuery(ctx, reference.entries[0].sql, reference.valid_numeric_result, null);
        try command(ctx, "DROP VIEW support_view");
        try command(ctx, reference.plan_view);
        for (reference.entries[1..]) |entry| try expectQuery(ctx, entry.sql, entry.result, entry.sqlstate);
        var captured = try resolve(ctx, "support_view");
        defer captured.deinit();
        var record = try views.Record.decode(ctx.alloc, captured.value.sql_sources[0].?.value.view.source_json);
        defer record.deinit();
        self.state.stale_definition = try std.json.Stringify.valueAlloc(ctx.alloc, record.value.definition, .{});
        const old_source = record.value.definition.sources[0];
        try std.testing.expectEqual(old_id, old_source.table_id);
        const sources = try ctx.alloc.dupe(local.sql_view_definition.Source, record.value.definition.sources);
        defer ctx.alloc.free(sources);
        var definition = record.value.definition;
        definition.sources = sources;
        // Prove this exact path admits an unchanged definition before testing
        // forged captures, so a generic endpoint refusal cannot qualify them.
        try publish(ctx, definition, "guard_probe");
        try command(ctx, "DROP VIEW guard_probe");
        sources[0].schema_version += 1;
        try rejectedPublication(ctx, definition, error.CatalogGenerationChanged);
        sources[0] = old_source;
        sources[0].schema_digest[0] ^= 1;
        try rejectedPublication(ctx, definition, error.CatalogGenerationChanged);
        sources[0] = old_source;
        {
            const columns = try ctx.alloc.dupe(local.sql_view_definition.ColumnDependency, old_source.columns);
            defer ctx.alloc.free(columns);
            try std.testing.expect(columns.len != 0);
            sources[0].columns = columns;
            columns[0].signature[0] ^= 1;
            try rejectedPublication(ctx, definition, error.SqlViewColumnTypeChanged);
            sources[0] = old_source;
        }
        // A valid epoch must be accepted by the same native publication path.
        try publish(ctx, definition, "guard_probe");
        try command(ctx, "DROP VIEW guard_probe");
        try evolveSource(ctx, old_source.schema_version);
        for (reference.entries[1..]) |entry| try expectQuery(ctx, entry.sql, entry.result, entry.sqlstate);
        // Current catalog revision does not authorize a captured old epoch.
        try rejectedPublication(ctx, definition, error.CatalogGenerationChanged);
        {
            var current_source = try resolve(ctx, "docs");
            defer current_source.deinit();
            const binding = current_source.value.sql_sources[0].?.value.table;
            var identities = try local.system_catalog_column_identities.State.decode(ctx.alloc, binding.column_identities_json.?);
            defer identities.deinit();
            try std.testing.expectEqual(old_id, identities.value.table_id);
            try std.testing.expect(identities.value.epoch.version != old_source.schema_version);
            // Existing execution dependencies retain exact column IDs/signatures
            // even though the unrelated addition advances the source epoch.
            try record.value.requireSource(0, identities.value, false);
            try std.testing.expectError(error.CatalogGenerationChanged, record.value.requireSource(0, identities.value, true));
            sources[0].schema_version = identities.value.epoch.version;
            sources[0].schema_digest = identities.value.epoch.digest;
            try publish(ctx, definition, "guard_probe");
            try command(ctx, "DROP VIEW guard_probe");
            sources[0] = old_source;
        }
        try command(ctx, "DROP TABLE docs CASCADE");
        const new_id = try createSource(ctx);
        try std.testing.expect(old_id != new_id);
        try command(ctx, reference.plan_view);
        {
            var replacement = try resolve(ctx, "docs");
            defer replacement.deinit();
            const binding = replacement.value.sql_sources[0].?.value.table;
            var identities = try local.system_catalog_column_identities.State.decode(ctx.alloc, binding.column_identities_json.?);
            defer identities.deinit();
            try std.testing.expectEqual(new_id, identities.value.table_id);
            // Column numbers are scoped by source ID. A recreated table cannot
            // adopt dependencies even when its names/domains match exactly.
            try std.testing.expectError(error.InvalidSqlViewRecord, record.value.requireSource(0, identities.value, false));
        }
        // Reusing a spelling never resurrects a retired source identity.
        try rejectedPublication(ctx, definition, error.CatalogGenerationChanged);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        var parsed = try std.json.parseFromSlice(Reference, ctx.alloc, bytes, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        for (parsed.value.entries[1..]) |entry| try expectQuery(ctx, entry.sql, entry.result, entry.sqlstate);
        var stale = try std.json.parseFromSlice(local.sql_view_definition.Definition, ctx.alloc, self.state.stale_definition.?, .{});
        defer stale.deinit();
        try rejectedPublication(ctx, stale.value, error.CatalogGenerationChanged);
    }
};
test "mounted SQL document view guards reject forged epochs and source reincarnation with PostgreSQL casts and projections" {
    // sql-0853: Campaign reads original TEXT title rows through an explicit
    // public NUMERIC cast; exact original SQL matches PostgreSQL failure/success.
    // sql-0855 sql-0856: accepting native publication checks captured source IDs,
    // schema epochs/digests and column signatures across public schema PUT, DROP/CREATE and
    // cold restart; valid current publication is the positive control.
    // sql-0859: the exact projection fails on an existing public plan-only view;
    // valid plan reads execute over the original JSON source path/domain.
    var state: State = .{};
    defer if (state.stale_definition) |value| std.testing.allocator.free(value);
    try fixture.runWithState(Campaign, .{ .state = &state });
}
