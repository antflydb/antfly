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

//! Public document views replace private index mappings with explicit SQL
//! casts/paths and durable source/column identities. Never qualify these guards
//! by merely rejecting an unresolved support_view name.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const compare = @import("../parity_reference.zig");
const views = @import("../../../../system_catalog/views.zig");
const domain = local.system_catalog_domain;
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;
const bytes = @embedFile("../fixtures/sql_view_guard_reference.json");
const Result = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Reference = struct {
    number_view: []const u8,
    plan_view: []const u8,
    entries: []const struct { id: []const u8, sql: []const u8, sqlstate: ?[]const u8 = null, result: ?Result = null },
    valid_numeric_result: Result,
};
fn query(ctx: *fixture.Context, sql: []const u8) !@import("../../../../raft/transport/http_common.zig").HttpResponse {
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
        try std.testing.expect(@import("../parity_reference.zig").postgresColumnMatches(column, oid));
    }
    const used = try ctx.alloc.alloc(bool, want.rows.len);
    defer ctx.alloc.free(used);
    @memset(used, false);
    for (actual.rows, actual.sql_nulls.?) |row, nulls| {
        const found = for (want.rows, want.sql_nulls, 0..) |want_row, want_nulls, i| {
            if (used[i]) continue;
            if (!try @import("../parity_reference.zig").rowMatchesWithNulls(ctx.alloc, actual.columns, row, nulls, want_row, want_nulls)) continue;
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
    const native = @import("../../../../system_catalog/catalog_mutations.zig");
    const Command = @import("../../../../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand;
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
    const forwarding = @import("../../../raft_mutation_forwarding.zig");
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
    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
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
        var admitted = try @import("../../../table_contract.zig").parseCreateTableRequest(ctx.alloc, body);
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

fn requiredIndexQuery(ctx: *fixture.Context, statement: []const u8, admin: bool) !@import("../../../../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = if (admin) ctx.admin_headers else ctx.reader_headers;
    authenticated.base = ctx.metadata_uri;
    return client.execute(&authenticated, statement, .read);
}

const RequiredIndexViewCampaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn executeDdl(ctx: *fixture.Context, statement: []const u8) !void {
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        authenticated.base = ctx.metadata_uri;
        var response = client.execute(&authenticated, statement, .write_admission) catch |err| {
            reportIndexOwners(ctx) catch |diagnostic_error| std.debug.print("required-index owner diagnostics unavailable: {s}\n", .{@errorName(diagnostic_error)});
            return err;
        };
        defer response.deinit(ctx.alloc);
        if (response.status != 200) {
            std.debug.print("required-index view command status={d} body={s}\n", .{ response.status, response.body });
            reportIndexOwners(ctx) catch |err| std.debug.print("required-index owner diagnostics unavailable: {s}\n", .{@errorName(err)});
        }
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    fn reportIndexOwners(ctx: *fixture.Context) !void {
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const svc = ctx.metadata.server.svc;
        const store = svc.projectedStore() orelse return error.MissingMetadataStore;
        const tables = try store.listTables(a, svc.metadata_group_id);
        const ranges = try store.listRanges(a, svc.metadata_group_id);
        const placements = try store.listPlacementIntents(a, svc.metadata_group_id);
        for (tables) |table| {
            if (std.mem.indexOf(u8, table.indexes_json, "\"support_fts\"") == null) continue;
            for (ranges) |range| {
                if (range.table_id != table.table_id) continue;
                std.debug.print("required-index table={d} group={d} requirements={any}\n", .{ table.table_id, range.group_id, table.schema_reader_requirements });
                for (placements) |placement| {
                    if (placement.record.group_id != range.group_id) continue;
                    std.debug.print("required-index placement store={d} node={d} state={s}\n", .{ placement.store_id, placement.record.local_node_id, @tagName(placement.serving_state) });
                    const owner = (try store.readStore(a, svc.metadata_group_id, placement.store_id, true)) orelse continue;
                    for (owner.runtime_statuses) |status| {
                        if (status.group_id != range.group_id) continue;
                        std.debug.print("required-index runtime store={d} schema={d} complete={} authority={any}\n", .{ owner.store_id, status.schema_epoch, status.target_observation_complete, status.source_authority });
                        for (status.indexes) |item| {
                            if (!std.mem.eql(u8, item.name, "support_fts")) continue;
                            std.debug.print("required-index incarnation={d} config={d} identity={} snapshot={} backfill={} catch_up={} replay={d}/{d} load_error={any}\n", .{ item.coverage_generation, item.coverage_config_hash, item.coverage_identity_ready, item.serving_snapshot_ready, item.backfill_active, item.replay_catch_up_required, item.replay_applied_sequence, item.replay_target_sequence, item.load_error });
                        }
                    }
                }
            }
        }
    }

    fn reject(ctx: *fixture.Context, statement: []const u8, code: []const u8) !void {
        var response = try requiredIndexQuery(ctx, statement, true);
        defer response.deinit(ctx.alloc);
        try std.testing.expect(response.status != 200);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, transaction_id: ?[]const u8 = null, ddl_receipt: ?Json = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(code, diagnostic.value.code);
        try std.testing.expect(diagnostic.value.transaction_id == null and diagnostic.value.ddl_receipt == null);
    }
    fn original(a: std.mem.Allocator) !local.sql_parity_fixtures.Corpus {
        var corpus = try local.sql_parity_fixtures.Corpus.init(a);
        errdefer corpus.deinit();
        try std.testing.expectEqualStrings("SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10", (try corpus.get("sql-0854")).sql);
        return corpus;
    }
    fn observe(ctx: *fixture.Context, count: usize) !void {
        var corpus = try original(ctx.alloc);
        defer corpus.deinit();
        var response = try requiredIndexQuery(ctx, (try corpus.get("sql-0854")).sql, false);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("required-index original status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings("SELECT", parsed.value.command_tag);
        try std.testing.expectEqual(@as(i64, 0), parsed.value.rows_affected);
        try std.testing.expectEqual(count, parsed.value.rows.len);
        try std.testing.expectEqual(@as(usize, 2), parsed.value.columns.len);
        for (parsed.value.columns, [_][]const u8{ "_id", "plan" }) |column, name| {
            try std.testing.expectEqualStrings(name, column.name);
            try std.testing.expect(compare.postgresColumnMatches(column, 25));
        }
        if (parsed.value.sql_nulls) |nulls| {
            try std.testing.expectEqual(count, nulls.len);
            for (parsed.value.rows, nulls) |row, flags| {
                try std.testing.expectEqual(@as(usize, 2), row.len);
                try std.testing.expectEqual(@as(usize, 2), flags.len);
                try std.testing.expect(!flags[0] and !flags[1]);
            }
        } else try std.testing.expectEqual(@as(usize, 0), count);
        if (count != 0) {
            try std.testing.expectEqualStrings("doc:a", parsed.value.rows[0][0].string);
            try std.testing.expectEqualStrings("pro", parsed.value.rows[0][1].string);
        }
    }
    fn replaceDocument(ctx: *fixture.Context, value: []const u8) !void {
        const batch = try std.fmt.allocPrint(ctx.alloc, "{{\"inserts\":{{\"doc:a\":{s}}},\"sync_level\":\"full_text\"}}", .{value});
        defer ctx.alloc.free(batch);
        var response = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "docs", batch);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), response.status);
    }
    fn index(ctx: *fixture.Context, method: @import("../../../../raft/transport/http_common.zig").Method) !void {
        var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/docs/indexes/support_fts", method, if (method == .POST) "{\"name\":\"support_fts\",\"type\":\"full_text\"}" else null);
        defer response.deinit(ctx.alloc);
        try std.testing.expect(response.status == 200 or response.status == 201 or response.status == 202 or response.status == 204);
        if (method == .POST) try waitIndex(ctx);
    }
    fn waitIndex(ctx: *fixture.Context) !void {
        const platform = @import("antfly_platform");
        const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (platform.time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            const remaining_ms = (deadline -| platform.time.monotonicNs()) / std.time.ns_per_ms;
            if (remaining_ms == 0) break;
            // This is a read-only readiness observation, not a mutation retry.
            // Bound each attempt and wait by the original fifteen-second cut.
            var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/docs/indexes/support_fts", .GET, null, @intCast(@min(remaining_ms, 1_000))) catch |err| switch (err) {
                error.Timeout, error.DeadlineExceeded, error.ConnectionTimedOut, error.ConnectionResetByPeer, error.ConnectionRefused, error.BrokenPipe, error.EndOfStream, error.ReadUnavailable, error.StorageReadTemporarilyUnavailable => {
                    const remaining = deadline -| platform.time.monotonicNs();
                    if (remaining != 0) try ctx.io.sleep(.fromNanoseconds(@min(remaining, 20 * std.time.ns_per_ms)), .awake);
                    continue;
                },
                else => return err,
            };
            defer response.deinit(ctx.alloc);
            if (response.status == 200) {
                var value = try std.json.parseFromSlice(struct { status: ?struct { milestones: ?struct { complete: struct { reached: bool } } = null } = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
                defer value.deinit();
                if (value.value.status) |status| if (status.milestones) |milestones| if (milestones.complete.reached) return;
            } else if (response.status != 503) return error.RequiredViewIndexStatusFailed;
            const remaining = deadline -| platform.time.monotonicNs();
            if (remaining != 0) try ctx.io.sleep(.fromNanoseconds(@min(remaining, 20 * std.time.ns_per_ms)), .awake);
        }
        return error.RequiredViewIndexReadinessTimeout;
    }
    fn lazyDemand(ctx: *fixture.Context) !void {
        for ([_]struct { statement: []const u8, free_rows: usize }{
            .{ .statement = "SELECT CASE WHEN _id='doc:a' THEN 'kept' ELSE plan END FROM support_view", .free_rows = 1 },
            .{ .statement = "SELECT COALESCE('kept',plan) FROM support_view", .free_rows = 0 },
            .{ .statement = "WITH q AS (SELECT _id,plan FROM support_view) SELECT CASE WHEN _id='doc:a' THEN 'kept' ELSE plan END FROM q", .free_rows = 1 },
            .{ .statement = "SELECT CASE WHEN a._id='doc:a' THEN 'kept' ELSE b.plan END FROM support_view a JOIN support_view b ON a._id=b._id", .free_rows = 1 },
        }) |probe| {
            var response = try requiredIndexQuery(ctx, probe.statement, false);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
            var kept: usize = 0;
            var free: usize = 0;
            for (parsed.value.rows) |row| {
                try std.testing.expectEqual(@as(usize, 1), row.len);
                if (std.mem.eql(u8, row[0].string, "kept")) kept += 1 else {
                    try std.testing.expectEqualStrings("free", row[0].string);
                    free += 1;
                }
            }
            try std.testing.expectEqual(probe.free_rows, free);
            try std.testing.expectEqual(2 - probe.free_rows, kept);
        }
        try reject(ctx, "SELECT CASE WHEN _id='never' THEN 'kept' ELSE plan END FROM support_view", "42804");
        try reject(ctx, "SELECT COALESCE(CAST(NULL AS TEXT),plan) FROM support_view", "42804");
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        const declaration =
            \\{"schema":{"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"metadata":{}},"additionalProperties":true}}},"document_sql_columns":[{"name":"plan","path":["metadata","plan"],"type":"keyword"}],"sql_view_requirements":[{"view_name":"support_view","required_indexes":["support_fts"]}]}}
        ;
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/docs", .POST, declaration);
        defer created.deinit(ctx.alloc);
        try std.testing.expect(created.status == 200 or created.status == 201);
        var observed = try fixture.table(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "docs");
        defer observed.deinit(ctx.alloc);
        var record = try std.json.parseFromSlice(Json, ctx.alloc, observed.body, .{});
        defer record.deinit();
        const id = try std.fmt.parseUnsigned(u64, (try fixture.field(record.value, "table_id")).string, 10);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        // Preserve both original source rows: the unchanged predicate must
        // exclude the genuine nonmatching string through every later phase.
        var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "docs",
            \\{"inserts":{"doc:a":{"title":"alpha","metadata":{"plan":"pro"}},"doc:b":{"title":"17","metadata":{"plan":"free"}}},"sync_level":"full_text"}
        );
        defer seeded.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), seeded.status);
        try reject(ctx, "CREATE VIEW support_view WITH(security_invoker=true) AS SELECT _id,plan FROM docs", "42704");
        try index(ctx, .POST);
        try executeDdl(ctx, "CREATE VIEW support_view WITH(security_invoker=true) AS SELECT _id,plan FROM docs");
        try observe(ctx, 1);
        try reject(ctx, "UPDATE docs SET plan='pro'", "428C9");
        try reject(ctx, "INSERT INTO docs(_id,plan) VALUES('alias-write','pro')", "428C9");
        // Source ingress remains arbitrary JSON. Keyword typing belongs to
        // this SQL projection and must not silently stringify wrong leaf types.
        for ([_][]const u8{ "{}", "{\"metadata\":null}", "{\"metadata\":17}", "{\"metadata\":{\"plan\":null}}" }) |body| {
            try replaceDocument(ctx, body);
            try observe(ctx, 0);
        }
        for ([_][]const u8{ "{\"metadata\":{\"plan\":17}}", "{\"metadata\":{\"plan\":{}}}", "{\"metadata\":{\"plan\":[]}}", "{\"metadata\":{\"plan\":true}}" }) |body| {
            try replaceDocument(ctx, body);
            try lazyDemand(ctx);
            try reject(ctx, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10", "42804");
            // Neither an undemanded view column nor LIMIT0 may decode the
            // strict alias. The WHERE comparison above must demand it even
            // when raw JSON/postings would reject this row as a non-match.
            var id_only = try requiredIndexQuery(ctx, "SELECT _id FROM support_view", false);
            defer id_only.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), id_only.status);
            var zero = try requiredIndexQuery(ctx, "SELECT _id,plan FROM support_view WHERE plan='pro' LIMIT 0", false);
            defer zero.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), zero.status);
            var zero_rows = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, zero.body, .{});
            defer zero_rows.deinit();
            try std.testing.expectEqual(@as(usize, 0), zero_rows.value.rows.len);
        }
        try executeDdl(ctx, "CREATE VIEW hidden_plan WITH(security_invoker=true,security_barrier=true) AS SELECT _id,plan FROM docs WHERE _id<>'doc:a'");
        {
            var hidden = try requiredIndexQuery(ctx, "SELECT plan FROM hidden_plan WHERE plan='pro'", false);
            defer hidden.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), hidden.status);
            var rows = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, hidden.body, .{});
            defer rows.deinit();
            try std.testing.expectEqual(@as(usize, 0), rows.value.rows.len);
        }
        try executeDdl(ctx, "DROP VIEW hidden_plan");
        {
            const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
            const manager = api.cfg.user_manager orelse return error.MissingUserManager;
            try manager.setRowFilter("pk-reader", "docs", "{\"term\":{\"visible\":true}}");
            defer manager.removeRowFilter("pk-reader", "docs") catch {};
            try replaceDocument(ctx, "{\"visible\":false,\"metadata\":{\"plan\":17}}");
            // The real mandatory credential filter precedes demanded typing:
            // a hidden malformed leaf must not disclose a conversion error.
            try observe(ctx, 0);
            try reject(ctx, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10", "42804");
        }
        try replaceDocument(ctx, "{\"metadata\":{\"plan\":\"pro\"},\"metadata.plan\":17}");
        try observe(ctx, 1);
        try index(ctx, .DELETE);
        try reject(ctx, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10", "42704");
        try index(ctx, .POST);
        try reject(ctx, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10", "40001");
        try executeDdl(ctx, "CREATE OR REPLACE VIEW support_view WITH(security_invoker=true) AS SELECT _id,plan FROM docs");
        try observe(ctx, 1);
        try replaceDocument(ctx, "{\"metadata\":{\"plan\":17}}");
    }
    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        try lazyDemand(ctx);
        try reject(ctx, "SELECT _id, plan FROM support_view WHERE plan='pro' LIMIT 10", "42804");
        try replaceDocument(ctx, "{\"metadata\":{\"plan\":\"pro\"}}");
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try waitIndex(ctx);
        try observe(ctx, 1);
        const previous = try fixture.waitServingSchema(ctx, "docs", 0);
        var schema = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/docs/schema", .PATCH, "{\"document_sql_columns\":[{\"name\":\"plan\",\"path\":[\"metadata\",\"plan\"],\"type\":\"text\"}]}");
        defer schema.deinit(ctx.alloc);
        try std.testing.expect(schema.status == 200 or schema.status == 202);
        _ = try fixture.waitServingSchema(ctx, "docs", previous.version + 1);
        try reject(ctx, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10", "40001");
        try executeDdl(ctx, "CREATE OR REPLACE VIEW support_view WITH(security_invoker=true) AS SELECT _id,plan FROM docs");
        try observe(ctx, 1);
    }
};

test "mounted SQL original required-index document view preserves typed paths logical rebuild and cold ownership" {
    // sql-0854 retains the unchanged original SELECT and both original source rows.
    try fixture.run(RequiredIndexViewCampaign);
}
