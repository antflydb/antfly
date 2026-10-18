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

//! Exact original index declarations through a real metadata/data owner pair.
//! Native enforcement, complete SQL postimages and cold restart are required;
//! metadata-only declaration tests are not substituted for this campaign.
const std = @import("std");
const platform = @import("antfly_platform");
const sources = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const http = @import("../raft/transport/http_common.zig");
const server = @import("http_server.zig");

const profiles = [_]struct { id: []const u8, namespace: []const u8, nulls_not_distinct: bool }{
    .{ .id = "sql-0692", .namespace = "nulls_not_distinct", .nulls_not_distinct = true },
    .{ .id = "sql-0706", .namespace = "nulls_distinct", .nulls_not_distinct = false },
    .{ .id = "sql-0705", .namespace = "included_columns", .nulls_not_distinct = false },
};

fn execute(ctx: *fixture.Context, namespace: []const u8, statement: []const u8, pure_read: bool) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .namespace = namespace }, .{});
    defer ctx.alloc.free(body);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (true) {
        try ctx.checkDrivers();
        const remaining = deadline -| platform.time.monotonicNs();
        if (remaining == 0) return error.SqlIndexCampaignDeadline;
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql", .POST, body, @intCast(@max(1, remaining / std.time.ns_per_ms)));
        if (pure_read and response.status == 503) {
            var diagnostic = std.json.parseFromSlice(struct { code: []const u8, retryable: bool = false }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true }) catch |err| {
                response.deinit(ctx.alloc);
                return err;
            };
            defer diagnostic.deinit();
            if (diagnostic.value.retryable and std.mem.eql(u8, diagnostic.value.code, "53300")) {
                response.deinit(ctx.alloc);
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            }
        }
        return response;
    }
}

fn command(ctx: *fixture.Context, namespace: []const u8, statement: []const u8) !void {
    var response = try execute(ctx, namespace, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("SQL index campaign command status={d} statement={s} body={s}\n", .{ response.status, statement, response.body });
        return error.SqlIndexCampaignCommandFailed;
    }
    if (std.mem.startsWith(u8, statement, "CREATE UNIQUE INDEX")) try awaitIndexPublication(ctx, namespace, statement, response);
}

pub fn awaitIndexPublication(ctx: *fixture.Context, namespace: []const u8, statement: []const u8, response: http.HttpResponse) !void {
    return awaitIndexTerminal(ctx, namespace, statement, response, .published);
}

fn awaitIndexTerminal(ctx: *fixture.Context, namespace: []const u8, statement: []const u8, response: http.HttpResponse, expected: enum { published, duplicate_key }) !void {
    const Receipt = struct { table_id: []const u8, index_publication_token: ?[]const u8 = null };
    var result = try std.json.parseFromSlice(struct { ddl_receipt: ?Receipt = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer result.deinit();
    const receipt = result.value.ddl_receipt orelse return error.MissingSqlIndexReceipt;
    const token = receipt.index_publication_token orelse return error.MissingSqlIndexPublicationToken;
    var compiled = try sources.sql_compiler.compile(ctx.alloc, statement, .{});
    defer compiled.deinit();
    const name = compiled.statement.catalog_ddl.schema_change.?.create_index.name;
    const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/default/namespaces/{s}/tables/usage_records/indexes/{s}/publication", .{ namespace, name });
    defer ctx.alloc.free(suffix);
    const headers = try ctx.alloc.alloc(http.RequestHeader, ctx.headers.len + 1);
    defer ctx.alloc.free(headers);
    @memcpy(headers[0..ctx.headers.len], ctx.headers);
    headers[ctx.headers.len] = .{ .name = "X-Antfly-Index-Publication-Token", .value = token };
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    var last_phase: ?@import("antfly_indexes_openapi").types.IndexPublicationPhase = null;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var observed = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, headers, ctx.base, suffix, .GET, null, 1_000);
        defer observed.deinit(ctx.alloc);
        if (observed.status == 200) {
            var status = try std.json.parseFromSlice(@import("antfly_indexes_openapi").types.IndexPublicationStatus, ctx.alloc, observed.body, .{});
            defer status.deinit();
            try std.testing.expectEqualStrings(token, status.value.publication_token);
            try std.testing.expectEqualStrings(receipt.table_id, status.value.table_id);
            try std.testing.expectEqualStrings(name, status.value.index_name);
            last_phase = status.value.phase;
            switch (status.value.state) {
                .published => if (!status.value.replication_pending) {
                    try std.testing.expectEqual(.published, expected);
                    return;
                },
                .failed => {
                    try std.testing.expectEqual(.duplicate_key, expected);
                    try std.testing.expectEqual(@import("antfly_indexes_openapi").types.IndexPublicationFailure.duplicate_key, status.value.failure.?);
                    try std.testing.expectEqual(@import("antfly_indexes_openapi").types.IndexPublicationPhase.canceled, status.value.phase.?);
                    try std.testing.expect(!status.value.replication_pending);
                    return;
                },
                .canceled => return error.SqlIndexCampaignBuildFailed,
                .pending, .admission_unknown => {},
            }
        } else if (observed.status != 503) {
            std.debug.print("SQL index publication status={d} body={s}\n", .{ observed.status, observed.body });
            return error.SqlIndexCampaignStatusFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    std.debug.print("SQL index publication timeout namespace={s} phase={s}\n", .{ namespace, if (last_phase) |phase| @tagName(phase) else "unknown" });
    return error.SqlIndexCampaignPublicationTimeout;
}

pub fn relationOwner(ctx: *fixture.Context, namespace: []const u8) !?u64 {
    const domain = sources.system_catalog_domain;
    const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .database = "default", .namespace = namespace, .name = "usage_records" }}, .include_query_definitions = true };
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    var attempts: usize = 0;
    var pending_error: ?anyerror = null;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        attempts += 1;
        const bytes = source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve_many = lookup }) catch |err| switch (err) {
            error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => {
                pending_error = err;
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        defer ctx.alloc.free(bytes);
        var snapshot = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer snapshot.deinit();
        try snapshot.value.validateRelations(lookup);
        return if (snapshot.value.relations[0]) |relation| relation.owner.table_id else null;
    }
    std.debug.print("SQL index catalog publication timeout namespace={s} attempts={d} pending={s}\n", .{ namespace, attempts, if (pending_error) |err| @errorName(err) else "none" });
    const svc = ctx.metadata.server.svc;
    if (svc.projectedStore()) |store| {
        const work = store.relationReconciliationWork(svc.metadata_group_id) catch |err| {
            std.debug.print("SQL index catalog reconciliation diagnostic failed: {s}\n", .{@errorName(err)});
            return error.SqlIndexCampaignPublicationTimeout;
        };
        std.debug.print("SQL index catalog reconciliation epoch={} root={} current={}\n", .{ work.epoch != null, work.root != null, work.current != null });
        if (work.current) |current| std.debug.print("SQL index catalog reconciliation phase={s} coverage={s} failure={s} cursor_bytes={d}\n", .{ @tagName(current.phase), @tagName(current.coverage), @tagName(current.failure), current.cursor_len });
    }
    return error.SqlIndexCampaignPublicationTimeout;
}

pub fn owner(ctx: *fixture.Context, namespace: []const u8) !u64 {
    return (try relationOwner(ctx, namespace)) orelse error.SqlIndexCampaignMissingTable;
}

const ConstraintState = enum { enforced, validating, invalid };

fn waitTerminalActivation(ctx: *fixture.Context, namespace: []const u8) !ConstraintState {
    const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/databases/default/namespaces/{s}/tables/usage_records/constraints/status", .{namespace});
    defer ctx.alloc.free(suffix);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .GET, null, 1_000);
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            var status = try std.json.parseFromSlice(struct { state: ConstraintState }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer status.deinit();
            switch (status.value.state) {
                .enforced, .invalid => return status.value.state,
                .validating => {},
            }
        } else if (response.status != 503) {
            std.debug.print("SQL index activation status={d} body={s}\n", .{ response.status, response.body });
            return error.SqlIndexCampaignStatusFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlIndexCampaignActivationTimeout;
}

pub fn waitEnforced(ctx: *fixture.Context, namespace: []const u8) !void {
    try std.testing.expectEqual(ConstraintState.enforced, try waitTerminalActivation(ctx, namespace));
}

fn snapshotRows(ctx: *fixture.Context, namespace: []const u8) ![]u8 {
    var response = try execute(ctx, namespace, "SELECT id, email, tenant_id, status FROM usage_records ORDER BY id", true);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    return std.json.Stringify.valueAlloc(ctx.alloc, .{
        .columns = try fixture.field(parsed.value, "columns"),
        .rows = try fixture.field(parsed.value, "rows"),
    }, .{});
}

pub fn snapshotDefinition(ctx: *fixture.Context, namespace: []const u8) ![]u8 {
    const domain = sources.system_catalog_domain;
    const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .database = "default", .namespace = namespace, .name = "usage_records" }}, .include_query_definitions = true };
    const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s }, .{ .resolve_many = lookup });
    defer ctx.alloc.free(bytes);
    var snapshot = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
    defer snapshot.deinit();
    try snapshot.value.validateRelations(lookup);
    const relation = snapshot.value.relations[0] orelse return error.SqlIndexCampaignMissingTable;
    return std.json.Stringify.valueAlloc(ctx.alloc, .{
        .owner = relation.owner,
        .definition = relation.table.query_definition orelse return error.InvalidSqlBackendResponse,
    }, .{});
}

fn rejected(ctx: *fixture.Context, namespace: []const u8, statement: []const u8) !void {
    var response = try execute(ctx, namespace, statement, false);
    defer response.deinit(ctx.alloc);
    try expectRejected(ctx, response);
}

fn expectRejected(ctx: *fixture.Context, response: http.HttpResponse) !void {
    if (response.status != 400) std.debug.print("SQL index campaign rejection status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 400), response.status);
    var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, retryable: bool }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings("23505", diagnostic.value.code);
    try std.testing.expect(!diagnostic.value.retryable);
}

fn postimage(ctx: *fixture.Context, namespace: []const u8, extra_nulls: usize) !void {
    var response = try execute(ctx, namespace, "SELECT id, email, tenant_id, status FROM usage_records ORDER BY id", true);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("SQL index campaign postimage status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const rows = (try fixture.field(parsed.value, "rows")).array.items;
    try std.testing.expectEqual(2 + extra_nulls, rows.len);
    for (rows, 0..) |row, i| {
        const cells = row.array.items;
        try std.testing.expectEqual(@as(usize, 4), cells.len);
        const id = try std.fmt.allocPrint(ctx.alloc, "00000000-0000-0000-0000-{d:0>12}", .{i + 1});
        defer ctx.alloc.free(id);
        try std.testing.expectEqualStrings(id, cells[0].string);
        if (i == 1) try std.testing.expectEqualStrings("owner@example.com", cells[1].string) else try std.testing.expect(cells[1] == .null);
        try std.testing.expectEqualStrings(if (i >= 2) "other" else "tenant", cells[2].string);
        try std.testing.expectEqualStrings(if (i >= 2) "changed" else "pending", cells[3].string);
    }
}

const second_null = "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000003',NULL,'other','changed')";
const restart_null = "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000004',NULL,'other','changed')";
const duplicate = "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000004','owner@example.com','other','changed')";

const Campaign = struct {
    table_ids: [profiles.len]u64 = undefined,
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        // Control snapshots drive report projection. They must carry the same
        // negotiated decoder readiness as the public status endpoint, or a
        // healthy owner proof is stripped before it reaches metadata.
        {
            const ready = try ctx.metadata.server.svc.status();
            var control = try ctx.metadata.server.svc.controlSnapshot();
            defer ctx.metadata.server.svc.freeAdminSnapshot(&control);
            const protocol = @import("../metadata/runtime_status_protocol.zig");
            try std.testing.expect(protocol.profileSatisfies(ready.runtime_status_protocol_ready_version, protocol.source_authority_record_version));
            try std.testing.expectEqual(ready.runtime_status_protocol_ready_version, control.status.runtime_status_protocol_ready_version);
        }
        var corpus = try sources.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (profiles, &self.table_ids) |profile, *id| {
            const create_namespace = try std.fmt.allocPrint(ctx.alloc, "CREATE SCHEMA {s}", .{profile.namespace});
            defer ctx.alloc.free(create_namespace);
            try command(ctx, "public", create_namespace);
            // Namespace creation commits before its typed occupancy root is
            // published. Wait on read-only attested absence, not CREATE replay.
            try std.testing.expect((try relationOwner(ctx, profile.namespace)) == null);
            try command(ctx, profile.namespace, "CREATE TABLE usage_records (id UUID PRIMARY KEY, email TEXT, tenant_id TEXT, status TEXT)");
            id.* = try owner(ctx, profile.namespace);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id.*);
            try waitEnforced(ctx, profile.namespace);
            try command(ctx, profile.namespace, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000001',NULL,'tenant','pending'),('00000000-0000-0000-0000-000000000002','owner@example.com','tenant','pending')");
            const original = try corpus.get(profile.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            try command(ctx, profile.namespace, original.sql);
            try waitEnforced(ctx, profile.namespace);
            try rejected(ctx, profile.namespace, duplicate);
            try postimage(ctx, profile.namespace, 0);
            if (profile.nulls_not_distinct) try rejected(ctx, profile.namespace, second_null) else try command(ctx, profile.namespace, second_null);
            try postimage(ctx, profile.namespace, @intFromBool(!profile.nulls_not_distinct));
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (profiles, self.table_ids) |profile, id| {
            try std.testing.expectEqual(id, try owner(ctx, profile.namespace));
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
            try waitEnforced(ctx, profile.namespace);
            try postimage(ctx, profile.namespace, @intFromBool(!profile.nulls_not_distinct));
            try rejected(ctx, profile.namespace, duplicate);
            if (profile.nulls_not_distinct) try rejected(ctx, profile.namespace, second_null) else try command(ctx, profile.namespace, restart_null);
            try postimage(ctx, profile.namespace, if (profile.nulls_not_distinct) 0 else 2);
        }
    }
};

test "mounted SQL original unique indexes enforce exact postimages across owner restart" {
    // Exact corpus originals: sql-0692, sql-0706, sql-0705.
    try fixture.run(Campaign);
}

/// Regular CREATE UNIQUE INDEX must not publish an invalid descriptor. This
/// is deliberately distinct from CREATE INDEX CONCURRENTLY's failed-build
/// semantics and from the public asynchronous schema-maintenance API.
const FailedBuildCampaign = struct {
    table_ids: [profiles.len]u64 = undefined,
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        var corpus = try sources.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for (profiles, &self.table_ids) |profile, *retained_id| {
            const namespace = try std.fmt.allocPrint(ctx.alloc, "failed_{s}", .{profile.namespace});
            defer ctx.alloc.free(namespace);
            const create_namespace = try std.fmt.allocPrint(ctx.alloc, "CREATE SCHEMA {s}", .{namespace});
            defer ctx.alloc.free(create_namespace);
            try command(ctx, "public", create_namespace);
            try std.testing.expect((try relationOwner(ctx, namespace)) == null);
            try command(ctx, namespace, "CREATE TABLE usage_records (id UUID PRIMARY KEY, email TEXT, tenant_id TEXT, status TEXT)");
            const table_id = try owner(ctx, namespace);
            retained_id.* = table_id;
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, table_id);
            try waitEnforced(ctx, namespace);
            const email = if (profile.nulls_not_distinct) "NULL" else "'owner@example.com'";
            const inserts = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000001',{s},'tenant-1','pending'),('00000000-0000-0000-0000-000000000002',{s},'tenant-2','pending')", .{ email, email });
            defer ctx.alloc.free(inserts);
            try command(ctx, namespace, inserts);
            const before_rows = try snapshotRows(ctx, namespace);
            defer ctx.alloc.free(before_rows);
            const before_definition = try snapshotDefinition(ctx, namespace);
            defer ctx.alloc.free(before_definition);
            const original = try corpus.get(profile.id);
            try std.testing.expectEqual(@as(usize, 0), original.params.len);
            var response = try execute(ctx, namespace, original.sql, false);
            defer response.deinit(ctx.alloc);
            // Bounded HTTP DDL may return its preowned recovery handle.
            // Require exact terminal failure AND fence/claim cleanup before
            // observing atomicity; old constraint readiness proves neither.
            if (response.status == 202) {
                try awaitIndexTerminal(ctx, namespace, original.sql, response, .duplicate_key);
            } else try expectRejected(ctx, response);
            const state = try waitTerminalActivation(ctx, namespace);
            const after_rows = try snapshotRows(ctx, namespace);
            defer ctx.alloc.free(after_rows);
            const after_definition = try snapshotDefinition(ctx, namespace);
            defer ctx.alloc.free(after_definition);
            try std.testing.expectEqualStrings(before_rows, after_rows);
            try std.testing.expect(std.mem.eql(u8, before_definition, after_definition));
            try std.testing.expectEqual(ConstraintState.enforced, state);
            // The old owner and enforcement remain usable after a rejected
            // build. Repair is an explicit new user operation, never replay.
            try std.testing.expectEqual(table_id, try owner(ctx, namespace));
            try waitEnforced(ctx, namespace);
            try command(ctx, namespace, "DELETE FROM usage_records WHERE id='00000000-0000-0000-0000-000000000002'");
            try command(ctx, namespace, original.sql);
            try waitEnforced(ctx, namespace);
            try std.testing.expectEqual(table_id, try owner(ctx, namespace));
            try repairedPostimage(ctx, namespace, profile.nulls_not_distinct);
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (profiles, self.table_ids) |profile, table_id| {
            const namespace = try std.fmt.allocPrint(ctx.alloc, "failed_{s}", .{profile.namespace});
            defer ctx.alloc.free(namespace);
            try std.testing.expectEqual(table_id, try owner(ctx, namespace));
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, table_id);
            try waitEnforced(ctx, namespace);
            try repairedPostimage(ctx, namespace, profile.nulls_not_distinct);
            try rejected(ctx, namespace, if (profile.nulls_not_distinct) second_null else duplicate);
            try repairedPostimage(ctx, namespace, profile.nulls_not_distinct);
        }
    }
};

fn repairedPostimage(ctx: *fixture.Context, namespace: []const u8, null_email: bool) !void {
    const bytes = try snapshotRows(ctx, namespace);
    defer ctx.alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, bytes, .{});
    defer parsed.deinit();
    const rows = (try fixture.field(parsed.value, "rows")).array.items;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    const cells = rows[0].array.items;
    try std.testing.expectEqual(@as(usize, 4), cells.len);
    try std.testing.expectEqualStrings("00000000-0000-0000-0000-000000000001", cells[0].string);
    if (null_email) try std.testing.expect(cells[1] == .null) else try std.testing.expectEqualStrings("owner@example.com", cells[1].string);
    try std.testing.expectEqualStrings("tenant-1", cells[2].string);
    try std.testing.expectEqualStrings("pending", cells[3].string);
}

test "mounted SQL regular unique index build failure is atomic" {
    // Exact corpus originals: sql-0692, sql-0706, sql-0705; no rewritten SQL.
    try fixture.run(FailedBuildCampaign);
}

const DropCampaign = struct {
    const namespace = "drop_secondary";
    const index_name = "usage_records_status_idx";
    const create_index = "CREATE UNIQUE INDEX usage_records_status_idx ON usage_records(status)";
    const insert_conflict = "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000003',NULL,'other','pending')";
    table_id: u64 = undefined,

    fn write(ctx: *fixture.Context, statement: []const u8, conflict: bool) !void {
        // Publication may retire a resident read owner. Only the established
        // definite, pre-admission read failure can retry; a pending/ambiguous
        // write is never repeated to make this lifecycle test pass.
        var response = try @import("sql_owner_test_client.zig").executeIn(ctx, namespace, statement, .write_admission);
        defer response.deinit(ctx.alloc);
        if (conflict) return expectRejected(ctx, response);
        if (response.status != 200) std.debug.print("SQL index DROP write status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }

    fn expectDeclared(ctx: *fixture.Context, declared: bool) !void {
        const bytes = try snapshotDefinition(ctx, namespace);
        defer ctx.alloc.free(bytes);
        var definition = try std.json.parseFromSlice(struct { definition: struct { schema_json: []const u8 } }, ctx.alloc, bytes, .{ .ignore_unknown_fields = true });
        defer definition.deinit();
        var schema = try std.json.parseFromSlice(std.json.Value, ctx.alloc, definition.value.definition.schema_json, .{});
        defer schema.deinit();
        for ([_][]const u8{ "relational_indexes", "unique_constraints" }) |field| {
            var found: usize = 0;
            if (schema.value.object.get(field)) |entries| for (entries.array.items) |entry| {
                if (std.mem.eql(u8, index_name, entry.object.get("name").?.string)) found += 1;
            };
            try std.testing.expectEqual(@as(usize, @intFromBool(declared)), found);
        }
    }

    fn dropAndCheck(ctx: *fixture.Context) !void {
        var corpus = try sources.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get("sql-0724");
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        const before = try snapshotRows(ctx, namespace);
        defer ctx.alloc.free(before);
        var response = try execute(ctx, namespace, original.sql, false);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) {
            std.debug.print("SQL index DROP status={d} body={s}\n", .{ response.status, response.body });
            return error.SqlIndexCampaignCommandFailed;
        }
        // A bounded DROP can still serve its predecessor while retirement
        // runs. Old constraint enforcement cannot certify the target epoch.
        var result = try std.json.parseFromSlice(struct { ddl_receipt: struct { table_id: []const u8, schema_version: u32 } }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer result.deinit();
        try awaitRetirement(ctx, result.value.ddl_receipt.schema_version, try std.fmt.parseInt(u64, result.value.ddl_receipt.table_id, 10));
        try expectDeclared(ctx, false);
        const after = try snapshotRows(ctx, namespace);
        defer ctx.alloc.free(after);
        try std.testing.expectEqualStrings(before, after);
        // Removing an index-owned UNIQUE key must retire its enforcement too;
        // otherwise a metadata-only DROP could appear successful indefinitely.
        try write(ctx, insert_conflict, false);
        const rows = try snapshotRows(ctx, namespace);
        defer ctx.alloc.free(rows);
        var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, rows, .{});
        defer parsed.deinit();
        const actual = (try fixture.field(parsed.value, "rows")).array.items;
        try std.testing.expectEqual(@as(usize, 3), actual.len);
        for (actual, 0..) |row, i| {
            const cells = row.array.items;
            try std.testing.expectEqual(@as(usize, 4), cells.len);
            const id = try std.fmt.allocPrint(ctx.alloc, "00000000-0000-0000-0000-{d:0>12}", .{i + 1});
            defer ctx.alloc.free(id);
            try std.testing.expectEqualStrings(id, cells[0].string);
            if (i == 1) try std.testing.expectEqualStrings("owner@example.com", cells[1].string) else try std.testing.expect(cells[1] == .null);
            try std.testing.expectEqualStrings(if (i == 2) "other" else "tenant", cells[2].string);
            try std.testing.expectEqualStrings(if (i == 1) "active" else "pending", cells[3].string);
        }
    }

    fn awaitRetirement(ctx: *fixture.Context, version: u32, table_id: u64) !void {
        const suffix = "/db/v1/databases/default/namespaces/" ++ namespace ++ "/tables/usage_records/constraints/status";
        const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (platform.time.monotonicNs() < deadline) {
            try ctx.checkDrivers();
            var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .GET, null, 1_000);
            defer response.deinit(ctx.alloc);
            if (response.status == 200) {
                var parsed = try std.json.parseFromSlice(struct {
                    schema_version: u32,
                    state: ConstraintState,
                    retirement: ?struct { failure: ?[]const u8 = null } = null,
                }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
                defer parsed.deinit();
                const status = parsed.value;
                if (status.retirement) |retirement| {
                    if (retirement.failure != null) return error.SqlIndexCampaignRetirementFailed;
                } else if (status.schema_version == version and status.state == .enforced) {
                    try std.testing.expectEqual(table_id, try owner(ctx, namespace));
                    return;
                }
                if (status.schema_version > version) return error.SchemaVersionChanged;
            } else if (response.status != 503 and response.status != 409) {
                std.debug.print("SQL index retirement status={d} body={s}\n", .{ response.status, response.body });
                return error.SqlIndexCampaignStatusFailed;
            }
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.SqlIndexCampaignRetirementTimeout;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "public", "CREATE SCHEMA drop_secondary");
        try std.testing.expect((try relationOwner(ctx, namespace)) == null);
        try command(ctx, namespace, "CREATE TABLE usage_records(id UUID PRIMARY KEY,email TEXT,tenant_id TEXT,status TEXT)");
        self.table_id = try owner(ctx, namespace);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.table_id);
        try waitEnforced(ctx, namespace);
        try command(ctx, namespace, "INSERT INTO usage_records(id,email,tenant_id,status) VALUES('00000000-0000-0000-0000-000000000001',NULL,'tenant','pending'),('00000000-0000-0000-0000-000000000002','owner@example.com','tenant','active')");
        try command(ctx, namespace, create_index);
        try expectDeclared(ctx, true);
        try write(ctx, insert_conflict, true);
        try dropAndCheck(ctx);
        try write(ctx, "DELETE FROM usage_records WHERE id='00000000-0000-0000-0000-000000000003'", false);
        try command(ctx, namespace, create_index);
        try expectDeclared(ctx, true);
        try write(ctx, insert_conflict, true);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expectEqual(self.table_id, try owner(ctx, namespace));
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.table_id);
        try waitEnforced(ctx, namespace);
        try expectDeclared(ctx, true);
        try write(ctx, insert_conflict, true);
        try dropAndCheck(ctx);
    }
};

test "mounted SQL original secondary index drop retires enforcement and permits name reuse across restart" {
    // sql-0724; the original statement is executed without rewriting.
    try fixture.run(DropCampaign);
}
