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
    var response = try @import("sql_owner_test_client.zig").executeIn(ctx, namespace, statement, .write_admission);
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
        var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .GET, null, @intCast(@max(1, @min(5_000, (deadline -| platform.time.monotonicNs()) / std.time.ns_per_ms)))) catch |err| {
            if (err == error.Timeout) continue;
            return err;
        };
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
    original_id: []const u8 = "sql-0724",

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

    fn dropAndCheck(ctx: *fixture.Context, original_id: []const u8) !void {
        var corpus = try sources.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(original_id);
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
        var result = try std.json.parseFromSlice(struct { command_tag: []const u8, rows_affected: i64, ddl_receipt: struct { table_id: []const u8, schema_version: u32 } }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer result.deinit();
        try std.testing.expectEqualStrings(if (response.status == 200) "DROP INDEX" else "DDL PENDING", result.value.command_tag);
        try std.testing.expectEqual(@as(i64, 0), result.value.rows_affected);
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
            var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, suffix, .GET, null, @intCast(@max(1, @min(5_000, (deadline -| platform.time.monotonicNs()) / std.time.ns_per_ms)))) catch |err| {
                if (err == error.Timeout) continue;
                return err;
            };
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
        try dropAndCheck(ctx, self.original_id);
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
        try dropAndCheck(ctx, self.original_id);
    }
};

test "mounted SQL original secondary index drop retires enforcement and permits name reuse across restart" {
    // sql-0724; the original statement is executed without rewriting.
    try fixture.run(DropCampaign);
}

test "mounted SQL original concurrent index drop retires enforcement and permits name reuse across restart" {
    // sql-0725; both warm and cold campaigns execute the exact original.
    try fixture.runWithState(DropCampaign, .{ .original_id = "sql-0725" });
}

const NativeFullTextCampaign = struct {
    pub const cold_catalog = true;
    pub fn beforeRestart(_: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expect((try relationOwner(ctx, "public")) == null);
        try command(ctx, "public", "CREATE TABLE usage_records (id UUID PRIMARY KEY, email TEXT, tenant_id TEXT, status TEXT)");
        const id = try owner(ctx, "public");
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
        try waitEnforced(ctx, "public");
        try command(ctx, "public", "INSERT INTO usage_records(id,status) VALUES('00000000-0000-0000-0000-000000000001','pending'),('00000000-0000-0000-0000-000000000002','finished')");
        var baseline = try execute(ctx, "public", "SELECT count(*) FROM usage_records", true);
        defer baseline.deinit(ctx.alloc);
        var rows = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, baseline.body, .{ .ignore_unknown_fields = true });
        defer rows.deinit();
        try std.testing.expectEqual(@as(usize, 1), rows.value.rows.len);
        try std.testing.expectEqual(@as(usize, 1), rows.value.rows[0].len);
        try std.testing.expect(rows.value.rows[0][0] == .string);
        try std.testing.expectEqualStrings("2", rows.value.rows[0][0].string);
        var corpus = try sources.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for ([_][]const u8{ "sql-0696", "sql-0697" }) |case_id| {
            const original = try corpus.get(case_id);
            var result = try execute(ctx, "public", original.sql, false);
            defer result.deinit(ctx.alloc);
            var value = try std.json.parseFromSlice(struct { catalog_receipt: struct { state: enum { committed, unknown, failed } } }, ctx.alloc, result.body, .{ .ignore_unknown_fields = true });
            defer value.deinit();
            try std.testing.expectEqual(@as(u16, 200), result.status);
            try std.testing.expectEqual(.committed, value.value.catalog_receipt.state);
        }
        try checkNativeFullText(ctx, "usage_records_status_fts", "pending");
        try checkNativeFullText(ctx, "usage_records_status_fts_method", "pending");
        try command(ctx, "public", "UPDATE usage_records SET status='Pending Changes' WHERE id='00000000-0000-0000-0000-000000000001'");
        try checkNativeFullText(ctx, "usage_records_status_fts", "changes");
        try checkNativeFullText(ctx, "usage_records_status_fts_method", "changes");
        try nativeIndexLostAck(ctx, false);
        try prepareNativeDocumentAlias(ctx);
        var duplicate_name = try execute(ctx, "public", "CREATE TABLE usage_records_status_fts (id INTEGER)", false);
        defer duplicate_name.deinit(ctx.alloc);
        try std.testing.expect(duplicate_name.status >= 400);
    }
    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        const id = try owner(ctx, "public");
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
        try checkNativeFullText(ctx, "usage_records_status_fts", "changes");
        try checkNativeFullText(ctx, "usage_records_status_fts_method", "changes");
        const document = try fixture.waitServingSchema(ctx, "docs_native", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, document.table_id);
        try checkNativeDocumentAlias(ctx, "ready", "Ready Again", 0, 5);
        try command(ctx, "public", "DROP INDEX docs_status_fts");
        try command(ctx, "public", "DROP INDEX usage_records_status_fts");
        var removed = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/indexes/usage_records_status_fts_method", .DELETE, null);
        defer removed.deinit(ctx.alloc);
        if (removed.status != 201) std.debug.print("native index deletion status={d} body={s}\n", .{ removed.status, removed.body });
        try std.testing.expectEqual(@as(u16, 201), removed.status);
        try waitNativeNamesReleased(ctx);
        // Both SQL and native deletion withdraw their SQL names atomically.
        try command(ctx, "public", "CREATE TABLE usage_records_status_fts (id INTEGER)");
        try command(ctx, "public", "CREATE TABLE usage_records_status_fts_method (id INTEGER)");
    }
};
fn checkNativeFullText(ctx: *fixture.Context, name: []const u8, text: []const u8) !void {
    try waitNativeFullText(ctx, name);
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .full_text_search = .{ .match = text, .field = "status" }, .full_text_index = name, .fields = &[_][]const u8{"status"}, .highlight = .{ .fields = &[_][]const u8{"status"}, .fragment_size = 16, .max_fragments = 1 }, .limit = 5 }, .{});
    defer ctx.alloc.free(body);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    var reported = false;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/query", .POST, body, 1000) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.QueryResponses, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            if (parsed.value.responses) |responses| if (responses.len == 1) if (responses[0].hits) |hits| if (hits.total) |total| if (total.value == 1 and std.mem.eql(u8, total.relation, "exact")) {
                const records = hits.hits orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(usize, 1), records.len);
                const source = records[0]._source orelse return error.TestUnexpectedResult;
                const status = source.map.get("status") orelse return error.TestUnexpectedResult;
                try std.testing.expect(status == .string);
                try std.testing.expectEqualStrings(if (std.mem.eql(u8, text, "pending")) "pending" else "Pending Changes", status.string);
                const highlights = records[0]._highlights orelse {
                    std.debug.print("native index missing highlight name={s} body={s}\n", .{ name, response.body });
                    return error.TestUnexpectedResult;
                };
                const fragments = highlights.map.get("status") orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(usize, 1), fragments.len);
                try std.testing.expectEqualStrings(status.string, fragments[0].text);
                try std.testing.expectEqual(@as(i64, 0), fragments[0].offset);
                try std.testing.expectEqual(@as(usize, 1), fragments[0].spans.len);
                const start: i64 = if (std.mem.eql(u8, text, "pending")) 0 else 8;
                try std.testing.expectEqual(start, fragments[0].spans[0].start);
                try std.testing.expectEqual(start + 7, fragments[0].spans[0].end);
                return;
            };
            if (!reported) {
                std.debug.print("native index unexpected query response name={s} body={s}\n", .{ name, response.body });
                reported = true;
            }
        } else if (response.status != 503 and response.status != 409) {
            std.debug.print("native index query status={d} body={s}\n", .{ response.status, response.body });
            return error.NativeIndexQueryFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    var snapshot = try ctx.metadata.server.svc.adminSnapshot();
    defer ctx.metadata.server.svc.freeAdminSnapshot(&snapshot);
    for (snapshot.tables) |table| std.debug.print("native index backfill timed out name={s} table={s} configs={s}\n", .{ name, table.name, table.indexes_json });
    return error.NativeIndexBackfillTimeout;
}
fn waitNativeFullText(ctx: *fixture.Context, name: []const u8) !void {
    return waitNativeFullTextForTable(ctx, "usage_records", name);
}
fn waitNativeFullTextForTable(ctx: *fixture.Context, table: []const u8, name: []const u8) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/tables/{s}/indexes/{s}", .{ table, name });
    defer ctx.alloc.free(path);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, path, .GET, null, 1000) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            var value = try std.json.parseFromSlice(struct { status: ?struct { milestones: ?struct { complete: struct { reached: bool } } = null } = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer value.deinit();
            if (value.value.status) |status| if (status.milestones) |milestones| if (milestones.complete.reached) return;
        }
        if (platform.time.monotonicNs() +| 250 * std.time.ns_per_ms >= deadline) std.debug.print("native index readiness status={d} body={s}\n", .{ response.status, response.body });
        try ctx.io.sleep(.fromMilliseconds(50), .awake);
    }
    std.debug.print("native index owner refresh started={} completed={} failed={} same_head={} same_fingerprint={} reconciled={} deferred={}\n", .{ ctx.data.provisioned_root_refresh_started.load(.acquire), ctx.data.provisioned_root_refresh_completed.load(.acquire), ctx.data.provisioned_root_refresh_failed.load(.acquire), ctx.data.provisioned_root_probe_same_head.load(.acquire), ctx.data.provisioned_root_probe_same_fingerprint.load(.acquire), ctx.data.provisioned_root_probe_reconciled_groups.load(.acquire), ctx.data.provisioned_root_probe_deferred_catch_up.load(.acquire) });
    return error.NativeIndexReadinessTimeout;
}
test "mounted SQL original native full-text indexes share API lifecycle across cold restart" {
    // sql-0696 and sql-0697 execute unchanged before querying their own backfill.
    try fixture.run(NativeFullTextCampaign);
}

const GinCampaign = struct {
    pub const cold_catalog = true;
    pub fn beforeRestart(_: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expect((try relationOwner(ctx, "public")) == null);
        try command(ctx, "public", "CREATE TABLE usage_records(id UUID PRIMARY KEY,tenant_id TEXT,metadata JSONB,tags TEXT[])");
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try waitEnforced(ctx, "public");
        try command(ctx, "public", "INSERT INTO usage_records VALUES('00000000-0000-0000-0000-000000000001','a','{\"plan\":\"pro\"}'::jsonb,ARRAY['red','blue']),('00000000-0000-0000-0000-000000000002','b','{\"plan\":\"free\"}'::jsonb,ARRAY['green'])");
        var corpus = try sources.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for ([_][]const u8{ "sql-0719", "sql-0721" }) |id| try command(ctx, "public", (try corpus.get(id)).sql);
        try check(ctx, "00000000-0000-0000-0000-000000000001");
        var include_response = try execute(ctx, "public", (try corpus.get("sql-0720")).sql, false);
        defer include_response.deinit(ctx.alloc);
        var error_body = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, include_response.body, .{ .ignore_unknown_fields = true });
        defer error_body.deinit();
        try std.testing.expect(include_response.status >= 400);
        try std.testing.expectEqualStrings("0A000", error_body.value.code);
        // Typed operator-class admission must precede metadata installation.
        var wrong = try execute(ctx, "public", "CREATE INDEX wrong_class ON usage_records USING gin(tenant_id array_ops)", false);
        defer wrong.deinit(ctx.alloc);
        try std.testing.expect(wrong.status >= 400);
        var wrong_body = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, wrong.body, .{ .ignore_unknown_fields = true });
        defer wrong_body.deinit();
        try std.testing.expectEqualStrings("42804", wrong_body.value.code);
        try command(ctx, "public", "CREATE TABLE wrong_class(id INTEGER)");
        try command(ctx, "public", "UPDATE usage_records SET metadata='{\"plan\":\"free\"}'::jsonb,tags=ARRAY['green'] WHERE tenant_id='a'");
        try command(ctx, "public", "UPDATE usage_records SET metadata='{\"plan\":\"pro\"}'::jsonb,tags=ARRAY['red'] WHERE tenant_id='b'");
        try check(ctx, "00000000-0000-0000-0000-000000000002");
        try command(ctx, "public", "ALTER TABLE usage_records ADD COLUMN unrelated BIGINT");
        _ = try fixture.waitServingSchema(ctx, "usage_records", serving.version + 1);
        try check(ctx, "00000000-0000-0000-0000-000000000002");
        try nativeIndexLostAck(ctx, true);
    }
    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try check(ctx, "00000000-0000-0000-0000-000000000002");
        try command(ctx, "public", "DROP INDEX usage_records_metadata_gin");
        try command(ctx, "public", "DROP INDEX usage_records_tags_gin");
        try command(ctx, "public", "CREATE TABLE usage_records_metadata_gin(id INTEGER)");
        try command(ctx, "public", "CREATE TABLE usage_records_tags_gin(id INTEGER)");
    }
    fn check(ctx: *fixture.Context, expected: []const u8) !void {
        for ([_][]const u8{
            "SELECT id FROM usage_records WHERE metadata @> '{\"plan\":\"pro\"}'::jsonb ORDER BY id",
            "SELECT id FROM usage_records WHERE tags @> ARRAY['red'] ORDER BY id",
        }) |sql| {
            var response = try execute(ctx, "public", sql, true);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("GIN read sql={s} body={s}\n", .{ sql, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var value = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer value.deinit();
            try std.testing.expectEqual(@as(usize, 1), value.value.rows.len);
            try std.testing.expectEqual(@as(usize, 1), value.value.rows[0].len);
            try std.testing.expectEqualStrings(expected, value.value.rows[0][0].string);
        }
        // Physical captures cover the whole configured index cohort.
        for ([_][]const u8{ "usage_records_metadata_gin", "usage_records_tags_gin" }) |name| try waitNativeFullText(ctx, name);
        const initial = std.mem.endsWith(u8, expected, "1");
        const Binding = struct { name: []const u8, path: []const u8, before: []const u8, after: []const u8 };
        for ([_]Binding{
            .{ .name = "usage_records_metadata_gin", .path = "/metadata/plan", .before = "pro", .after = "free" },
            // SQL arrays retain their dimensions/values/SQL-null envelope;
            // flattened physical values are distinct from JSON-array cells.
            .{ .name = "usage_records_tags_gin", .path = "/tags/values/0", .before = "red", .after = "green" },
        }) |binding| {
            const name = binding.name;
            try waitNativeFullText(ctx, name);
            const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/tables/usage_records/indexes/{s}", .{name});
            defer ctx.alloc.free(path);
            var response = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, path, .GET, null);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            errdefer std.debug.print("GIN index status name={s} body={s}\n", .{ name, response.body[0..@min(response.body.len, 2048)] });
            var value = try std.json.parseFromSlice(struct { config: struct { type: []const u8 }, status: struct { healthy: bool, parse_error_count: u64, target_revision: u64, published_revision: u64 } }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer value.deinit();
            try std.testing.expectEqualStrings("algebraic", value.value.config.type);
            try std.testing.expect(value.value.status.healthy);
            try std.testing.expectEqual(@as(u64, 0), value.value.status.parse_error_count);
            try std.testing.expect(value.value.status.target_revision > 0);
            try std.testing.expect(value.value.status.published_revision >= value.value.status.target_revision);
            try checkGinFacts(ctx, name, binding.path, if (initial) binding.before else binding.after);
        }
    }
};

// Inspect each exact named sidecar through the production native partials
// reader. SQL postimages alone could pass through a primary-row fallback, and
// algebraic stats do not currently populate the legacy doc_count field.
fn checkGinFacts(ctx: *fixture.Context, name: []const u8, path: []const u8, expected: []const u8) !void {
    const ir = sources.storage_db_algebraic_ir;
    const token = sources.storage_db_algebraic_token;
    const first_id = try token.canonicalTupleAlloc(ctx.alloc, &.{ "string", "00000000-0000-0000-0000-000000000001" });
    defer ctx.alloc.free(first_id);
    const metadata = try sources.storage_db_algebraic_index.pathFactBucketFoldMetadataAlloc(ctx.alloc, .{ .kind = .terms, .op = .count, .bucket_path = path, .bucket_kind = .string, .constraints = &.{.{ .field = "/id", .value = first_id }} });
    defer ctx.alloc.free(metadata);
    const paths = [_]ir.PhysicalAccessPath{ir.pathFactAccessPath(name)};
    const steps = [_]ir.TensorProgramStep{
        .{ .expr = .{ .fragment = .slice, .output_dims = &.{ .doc, .path, .kind, .scalar }, .owner = name, .layout = .pathfact_rows } },
        .{ .expr = .{ .fragment = .reduce, .input_dims = &.{ .doc, .path, .kind, .scalar }, .output_dims = &.{ .bucket, .scalar }, .semantic_id = "gin_source_witness", .law_id = .count, .metadata = metadata }, .inputs = &.{.{ .step = 0 }} },
    };
    const body = try @import("table_reads.zig").encodeAlgebraicPartialsRequestWithProgramAtGeneration(ctx.alloc, name, null, &paths, &.{}, .{ .steps = &steps, .output = .{ .step = 1 } });
    defer ctx.alloc.free(body);
    const axis = try token.canonicalTupleAlloc(ctx.alloc, &.{ "string", expected });
    defer ctx.alloc.free(axis);
    const table_id = try owner(ctx, "public");
    var snapshot = try ctx.metadata.server.svc.adminSnapshot();
    defer ctx.metadata.server.svc.freeAdminSnapshot(&snapshot);
    const group_id = for (snapshot.ranges) |range| {
        if (range.table_id == table_id) break range.group_id;
    } else return error.TableNotFound;
    const table = for (snapshot.tables) |entry| {
        if (entry.table_id == table_id) break entry.name;
    } else return error.TableNotFound;
    const reader = if (ctx.data.http_server) |*api| api.table_reads orelse return error.MissingOwnerReadSource else return error.MissingOwnerReadSource;
    // A bare local read may reuse an older read owner. Capture the current
    // physical generation, as production coordinated index queries do, after
    // the public SQL read barrier and exact-index completion checks.
    var nonce: [32]u8 = undefined;
    try ctx.io.randomSecure(&nonce);
    const id = std.fmt.bytesToHex(nonce, .lower);
    const cut: sources.storage_db_native_query_cut_contract.Request = .{ .id = &id, .table_id = table_id, .expires_ms = platform.time.realtimeNs() / std.time.ns_per_ms + 60_000, .create = true, .timeout_ms = 15_000 };
    const encoded_cut = try std.json.Stringify.valueAlloc(ctx.alloc, cut, .{});
    defer ctx.alloc.free(encoded_cut);
    const captured_body = try std.fmt.allocPrint(ctx.alloc, "{{\"_native_cut\":{s},{s}", .{ encoded_cut, body[1..] });
    defer ctx.alloc.free(captured_body);
    try ctx.checkDrivers();
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    var response = capture: while (true) {
        const result = reader.algebraicPartialsGroupLocal(ctx.alloc, group_id, table, captured_body) catch |err| switch (err) {
            // A capture cannot publish while another configured projection
            // still retires its repair boundary. Observe that definite pending
            // state; never replace a successfully captured immutable cut.
            error.IndexRebuilding => {
                if (platform.time.monotonicNs() >= deadline) return err;
                try ctx.checkDrivers();
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        break :capture result orelse return error.GinNativePartialsUnavailable;
    };
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("GIN persisted fact mismatch index={s} path={s} expected={s} response={s}\n", .{ name, path, expected, response.json[0..@min(response.json.len, 2048)] });
    const Partial = struct { canonical_axis: []const u8, law: []const u8, value: ?[]const u8 = null, value_base64: ?[]const u8 = null };
    var parsed = try std.json.parseFromSlice(struct { partials: []const Partial }, ctx.alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.partials.len);
    const partial = parsed.value.partials[0];
    try std.testing.expectEqualStrings(axis, partial.canonical_axis);
    try std.testing.expectEqualStrings("count", partial.law);
    if (partial.value) |count| try std.testing.expectEqualStrings("1", count) else {
        const encoded = partial.value_base64 orelse return error.InvalidQueryRequest;
        var decoded: [1]u8 = undefined;
        try std.testing.expectEqual(@as(usize, 1), try std.base64.standard.Decoder.calcSizeForSlice(encoded));
        try std.base64.standard.Decoder.decode(&decoded, encoded);
        try std.testing.expectEqualStrings("1", &decoded);
    }
}

test "mounted SQL original GIN indexes publish typed sidecars maintain mutations and recover cold owners" {
    // sql-0719 and sql-0721 execute unchanged; sql-0720 matches PostgreSQL rejection.
    try fixture.run(GinCampaign);
}

fn prepareNativeDocumentAlias(ctx: *fixture.Context) !void {
    const body =
        \\{"num_shards":1,"schema":{"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"metadata":{"type":"object","properties":{"state":{"type":"keyword","x-antfly-column-name":"status"}}},"count":{"type":"integer"},"literal.dot":{"type":"text"}},"additionalProperties":true}}}}}
    ;
    var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/docs_native", .POST, body);
    defer created.deinit(ctx.alloc);
    if (created.status != 200) std.debug.print("native document alias create status={d} body={s}\n", .{ created.status, created.body });
    try std.testing.expectEqual(@as(u16, 200), created.status);
    const serving = try fixture.waitServingSchema(ctx, "docs_native", 0);
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
    try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs_native");
    var seeded = try fixture.batch(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "docs_native", "{\"inserts\":{\"doc-a\":{\"metadata\":{\"state\":\"Pending Changes\"},\"count\":2,\"literal.dot\":\"do not misaddress\"}},\"sync_level\":\"full_text\"}");
    defer seeded.deinit(ctx.alloc);
    if (seeded.status != 201) std.debug.print("native document alias seed body={s}\n", .{seeded.body});
    try std.testing.expectEqual(@as(u16, 201), seeded.status);
    for ([_]struct { sql: []const u8, status: u16, code: []const u8 }{
        .{ .sql = "CREATE INDEX rejected_native ON docs_native USING antfly_full_text(missing)", .status = 400, .code = "42703" },
        .{ .sql = "CREATE INDEX rejected_native ON docs_native USING antfly_full_text(count)", .status = 400, .code = "22023" },
        .{ .sql = "CREATE INDEX rejected_native ON docs_native USING antfly_full_text(\"literal.dot\")", .status = 501, .code = "0A000" },
    }) |probe| {
        var response = try execute(ctx, "public", probe.sql, false);
        defer response.deinit(ctx.alloc);
        if (response.status != probe.status) std.debug.print("native document guard sql={s} status={d} body={s}\n", .{ probe.sql, response.status, response.body });
        try std.testing.expectEqual(probe.status, response.status);
        var diagnostic = std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLDiagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true }) catch |err| {
            std.debug.print("native document diagnostic sql={s} body={s}\n", .{ probe.sql, response.body });
            return err;
        };
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(probe.code, diagnostic.value.code);
        try std.testing.expect(!(diagnostic.value.retryable orelse false));
    }
    try command(ctx, "public", "CREATE TABLE rejected_native (id INTEGER)");
    try command(ctx, "public", "CREATE INDEX docs_status_fts ON docs_native USING antfly_full_text(status)");
    try checkNativeDocumentAlias(ctx, "changes", "Pending Changes", 8, 15);
    try command(ctx, "public", "UPDATE docs_native SET status='Ready Again' WHERE _id='doc-a'");
    try checkNativeDocumentAlias(ctx, "ready", "Ready Again", 0, 5);
}

fn checkNativeDocumentAlias(ctx: *fixture.Context, text: []const u8, expected: []const u8, start: i64, end: i64) !void {
    try waitNativeFullTextForTable(ctx, "docs_native", "docs_status_fts");
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .full_text_search = .{ .match = text, .field = "metadata.state" }, .full_text_index = "docs_status_fts", .highlight = .{ .fields = &[_][]const u8{"metadata.state"}, .fragment_size = 16, .max_fragments = 1 }, .limit = 5 }, .{});
    defer ctx.alloc.free(body);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/docs_native/query", .POST, body, 1000) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            errdefer std.debug.print("native alias postimage text={s} body={s}\n", .{ text, response.body });
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.QueryResponses, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            if (parsed.value.responses) |responses| if (responses.len == 1) if (responses[0].hits) |hits| if (hits.total) |total| if (total.value == 1 and std.mem.eql(u8, total.relation, "exact")) {
                const records = hits.hits orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(usize, 1), records.len);
                const source = records[0]._source orelse return error.TestUnexpectedResult;
                const metadata = source.map.get("metadata") orelse return error.TestUnexpectedResult;
                try std.testing.expect(metadata == .object);
                const state = metadata.object.get("state") orelse return error.TestUnexpectedResult;
                try std.testing.expect(state == .string);
                try std.testing.expectEqualStrings(expected, state.string);
                const highlights = records[0]._highlights orelse return error.TestUnexpectedResult;
                const fragments = highlights.map.get("metadata.state") orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(usize, 1), fragments.len);
                try std.testing.expectEqualStrings(expected, fragments[0].text);
                try std.testing.expectEqual(@as(i64, 0), fragments[0].offset);
                try std.testing.expectEqual(@as(usize, 1), fragments[0].spans.len);
                try std.testing.expectEqual(start, fragments[0].spans[0].start);
                try std.testing.expectEqual(end, fragments[0].spans[0].end);
                return;
            };
        } else if (response.status != 503 and response.status != 409) {
            std.debug.print("native alias query status={d} body={s}\n", .{ response.status, response.body });
            return error.NativeIndexQueryFailed;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.NativeIndexBackfillTimeout;
}

fn waitNativeNamesReleased(ctx: *fixture.Context) !void {
    const domain = sources.system_catalog_domain;
    const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const lookup: domain.ResolveMany = .{ .relations = &.{
        .{ .database = "default", .namespace = "public", .name = "usage_records_status_fts" },
        .{ .database = "default", .namespace = "public", .name = "usage_records_status_fts_method" },
    } };
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
        var snapshot = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer snapshot.deinit();
        try snapshot.value.validateRelations(lookup);
        if (snapshot.value.relations[0] == null and snapshot.value.relations[1] == null) return;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlIndexCampaignPublicationTimeout;
}

fn nativeIndexLostAck(ctx: *fixture.Context, gin: bool) !void {
    const Call = @import("../system_catalog/server_call.zig").Call;
    const Probe = struct {
        source: server.StatusSource,
        hidden: bool = true,
        mutations: usize = 0,
        fn status(raw: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return (@as(*@This(), @ptrCast(@alignCast(raw)))).source.status();
        }
        fn catalog(raw: *anyopaque, a: std.mem.Allocator, context: sources.api_operation.RequestContext, call: Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .sql_native_index_outcome and self.hidden) return error.MetadataMutationOutcomeUnknown;
            const bytes = try self.source.systemCatalog(a, context, call);
            if (call == .sql_native_index_mutate) {
                self.mutations += 1;
                a.free(bytes);
                return error.MetadataMutationOutcomeUnknown;
            }
            return bytes;
        }
    };
    const original = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var probe: Probe = .{ .source = original.source };
    var gateway = server.ApiHttpServer.init(ctx.alloc, .{ .backend_runtime = original.cfg.backend_runtime, .imported_runtime_io = original.cfg.imported_runtime_io }, .{ .ptr = &probe, .vtable = &.{ .supports_query_definitions = true, .status = Probe.status, .system_catalog = Probe.catalog } }, null, null);
    defer gateway.deinit();
    const create_sql = if (gin) "CREATE INDEX lost_ack_gin ON usage_records USING gin(tags array_ops)" else "CREATE INDEX lost_ack_fts ON usage_records USING antfly_full_text(status)";
    const drop_sql = if (gin) "DROP INDEX lost_ack_gin" else "DROP INDEX lost_ack_fts";
    var parsed = try sources.sql_compiler.compile(ctx.alloc, create_sql, .{});
    defer parsed.deinit();
    const outcome = try @import("sql_catalog.zig").execute(&gateway, null, .{}, "default", "public", ctx.alloc, .{ .catalog_ddl = parsed.statement.catalog_ddl });
    const receipt = outcome.catalog_receipt orelse return error.MissingNativeIndexReceipt;
    defer receipt.deinit(ctx.alloc);
    try std.testing.expectEqual(.unknown, receipt.state);
    try std.testing.expectEqual(.index, receipt.kind);
    try std.testing.expect(receipt.recovery_token.len < 2048);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
    try command(ctx, "public", drop_sql);
    try command(ctx, "public", create_sql);
    probe.hidden = false;
    const recovered = try @import("sql_catalog_recovery.zig").recover(&gateway, null, .{}, ctx.alloc, receipt.recovery_token);
    defer recovered.deinit(ctx.alloc);
    try std.testing.expectEqual(.committed, recovered.state);
    try std.testing.expectEqual(.index, recovered.kind);
    try std.testing.expect(recovered.object_id == null);
    try std.testing.expectEqual(@as(usize, 1), probe.mutations);
}
