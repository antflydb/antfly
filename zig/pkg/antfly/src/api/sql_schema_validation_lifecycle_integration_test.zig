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

//! Exact original SET NOT NULL with UUID primary keys, real metadata/data
//! owners, public recovery, failed validation, repair, and cold catalog reopen.
const std = @import("std");
const local = @import("antfly_local_sources");
const platform = @import("antfly_platform");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const wire = @import("antfly_metadata_openapi").types;
const first = "00000000-0000-0000-0000-000000000001";
const second = "00000000-0000-0000-0000-000000000002";

fn command(ctx: *fixture.Context, sql: []const u8, expected: ?[]const u8) !void {
    var response = try client.execute(ctx, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (expected) |code| {
        try std.testing.expect(response.status >= 400);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(code, diagnostic.value.code);
    } else if (response.status != 200 and response.status != 202) {
        std.debug.print("schema validation SQL status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
        return error.UnexpectedSchemaValidationCommand;
    }
}

fn rows(ctx: *fixture.Context, repaired: bool) !void {
    var response = try client.execute(ctx, "SELECT id::text,status,status_upper,amount::text FROM usage_records ORDER BY id", .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
    for (parsed.value.rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(usize, 4), row.len);
        try std.testing.expectEqualStrings(if (i == 0) first else second, row[0].string);
        if (i == 0 and !repaired) {
            try std.testing.expect(row[1] == .null and row[2] == .null);
        } else {
            try std.testing.expectEqualStrings(if (i == 0) "ready" else "open", row[1].string);
            try std.testing.expectEqualStrings(if (i == 0) "READY" else "OPEN", row[2].string);
        }
        try std.testing.expectEqualStrings(if (i == 0) "1" else "2", row[3].string);
    }
}

const Primary = struct { key_digest: [32]u8, version: u64, digest: [32]u8 };
fn resolve(ctx: *fixture.Context) !std.json.Parsed(local.system_catalog_domain.ResolvedMany) {
    const domain = local.system_catalog_domain;
    const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const request: domain.ResolveMany = .{ .relations = &.{.{ .name = "usage_records" }} };
    const bytes = try source.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = request });
    defer ctx.alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validateRelations(request);
    if (parsed.value.relations[0] == null) return error.MissingSourceRelation;
    return parsed;
}

fn primary(ctx: *fixture.Context, relation: local.system_catalog_domain.ResolvedRelation) ![2]Primary {
    var response = try client.execute(ctx, "SELECT _id FROM usage_records ORDER BY id", .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    var result: [2]Primary = undefined;
    try std.testing.expectEqual(result.len, parsed.value.rows.len);
    for (parsed.value.rows, &result) |row, *image| {
        const key = row[0].string;
        var value = (try ctx.data.http_server.?.table_reads.?.lookup(ctx.alloc, relation.table.name, key, .{ .include_primary_digest = true }, .read_index)) orelse return error.MissingMutationPrimary;
        defer value.deinit(ctx.alloc);
        std.crypto.hash.Blake3.hash(key, &image.key_digest, .{});
        image.version = value.version;
        image.digest = value.expected_content_digest orelse return error.MissingMutationDigest;
    }
    return result;
}

const Campaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    original_case: []const u8 = "sql-0744",
    restart_stage: ?@import("../metadata/schema_validation_publication.zig").Stage = null,
    credentials: client.ApiKeyHeaders = .{},
    token: [@import("schema_validation_recovery.zig").Token.encoded_len]u8 = undefined,
    owner: fixture.ServingSchema = undefined,
    image: [2]Primary = undefined,
    last_phase: ?wire.SchemaValidationPhase = null,

    fn status(self: *@This(), ctx: *fixture.Context) !wire.SchemaValidationState {
        const uri = try std.fmt.allocPrint(ctx.alloc, "{s}/db/v1/tables/usage_records/schema/publication", .{ctx.metadata_uri});
        defer ctx.alloc.free(uri);
        const headers = try ctx.alloc.alloc(http.RequestHeader, ctx.admin_headers.len + 1);
        defer ctx.alloc.free(headers);
        @memcpy(headers[0..ctx.admin_headers.len], ctx.admin_headers);
        headers[headers.len - 1] = .{ .name = "X-Antfly-Schema-Validation-Token", .value = &self.token };
        var response = try ctx.transport.execute(ctx.alloc, .{ .method = .GET, .uri = uri, .authorization = headers[0].value, .headers = headers[1..], .timeout_ms = 5000, .max_response_bytes = 16 * 1024 });
        defer response.deinit(ctx.alloc);
        if (response.status == 503 or response.status == 504) return error.SchemaValidationObservationPending;
        if (response.status != 200) std.debug.print("schema validation status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SchemaValidationStatus, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(&self.token, parsed.value.schema_validation_token);
        self.last_phase = parsed.value.phase;
        try std.testing.expectEqual(self.owner.table_id, try std.fmt.parseInt(u64, parsed.value.table_id, 10));
        if (parsed.value.state == .succeeded) {
            try std.testing.expectEqual(parsed.value.owner_count.?, parsed.value.ready.?);
            try std.testing.expectEqual(parsed.value.owner_count.?, parsed.value.installed.?);
            try std.testing.expectEqual(parsed.value.owner_count.?, parsed.value.released.?);
        }
        if (parsed.value.state == .failed) try std.testing.expectEqual(wire.SchemaValidationFailure.not_null_violation, parsed.value.failure.?);
        return parsed.value.state;
    }

    fn awaitState(self: *@This(), ctx: *fixture.Context, succeeds: bool) !void {
        const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (true) {
            try ctx.checkDrivers();
            const state = self.status(ctx) catch |err| switch (err) {
                error.SchemaValidationObservationPending => {
                    if (platform.time.monotonicNs() >= deadline) return error.SchemaValidationPublicationTimeout;
                    try ctx.io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                },
                else => return err,
            };
            if (state == if (succeeds) wire.SchemaValidationState.succeeded else .failed) return;
            try std.testing.expect(state == .pending or state == .admission_unknown);
            if (platform.time.monotonicNs() >= deadline) {
                std.debug.print("schema validation stalled phase={s} attempts={d} deferred={d} last_error={s}\n", .{ if (self.last_phase) |phase| @tagName(phase) else "unknown", ctx.data.schema_recovery_attempts.load(.acquire), ctx.data.schema_recovery_deferred.load(.acquire), ctx.data.schemaValidationLastErrorForTest() });
                const source = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
                const lookup = try @import("schema_validation_recovery.zig").Token.decode(&self.token);
                const raw = try source.systemCatalog(ctx.alloc, .{ .schema_validation_authority = true, .deadline_ns = platform.time.monotonicNs() +| std.time.ns_per_s }, .{ .schema_validation_receipt_read = .{ .lookup = lookup, .expected = .admission } });
                defer ctx.alloc.free(raw);
                var observation = try std.json.parseFromSlice(@import("../metadata/schema_validation_receipt.zig").Observation, ctx.alloc, raw, .{});
                defer observation.deinit();
                std.debug.print("schema exact retained head={s}\n", .{if (observation.value.head) |head| @tagName(head.phase) else "absent"});
                return error.SchemaValidationPublicationTimeout;
            }
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
    }

    fn admit(self: *@This(), ctx: *fixture.Context, sql: []const u8, succeeds: bool) !bool {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (!succeeds and response.status >= 400) {
            var diagnostic = try std.json.parseFromSlice(struct { code: ?[]const u8 = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            if (diagnostic.value.code) |code| {
                try std.testing.expectEqualStrings("23502", code);
                return false;
            }
        }
        if (response.status != 200 and response.status != 202 and response.status != 409) std.debug.print("schema validation admission={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202 or response.status == 409);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const receipt = parsed.value.ddl_receipt orelse return error.MissingSchemaValidationReceipt;
        try std.testing.expect(receipt.restore_job_id == null and receipt.index_publication_token == null);
        const token = receipt.schema_validation_token orelse return error.MissingSchemaValidationToken;
        try std.testing.expectEqual(self.token.len, token.len);
        @memcpy(&self.token, token);
        return true;
    }

    fn submit(self: *@This(), ctx: *fixture.Context, sql: []const u8, succeeds: bool) !void {
        if (try self.admit(ctx, sql, succeeds)) try self.awaitState(ctx, succeeds);
    }

    fn failureRestart(self: *const @This()) bool {
        return self.restart_stage == .failed or self.restart_stage == .canceled;
    }

    fn awaitPaused(_: *@This(), ctx: *fixture.Context) !void {
        const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (!ctx.data.schemaValidationScanPausedForTest()) {
            try ctx.checkDrivers();
            if (platform.time.monotonicNs() >= deadline) return error.SchemaValidationScanPauseTimeout;
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get(self.original_case);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        try command(ctx, "CREATE TABLE usage_records(id UUID PRIMARY KEY,status TEXT DEFAULT 'open',status_upper TEXT GENERATED ALWAYS AS (upper(status)) STORED,amount NUMERIC CHECK(amount >= 0))", null);
        self.owner = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.owner.table_id);
        try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES ('" ++ first ++ "',NULL,1),('" ++ second ++ "','open',2)", null);
        try rows(ctx, false);
        if (self.failureRestart()) {
            var relation = try resolve(ctx);
            defer relation.deinit();
            self.image = try primary(ctx, relation.value.relations[0].?);
            ctx.data.pauseSchemaValidationAckForTest(self.owner.table_id, self.restart_stage.?);
            try std.testing.expect(try self.admit(ctx, original.sql, false));
            try self.awaitPaused(ctx);
            try std.testing.expectEqual(wire.SchemaValidationState.pending, try self.status(ctx));
            try std.testing.expectEqualDeep(self.owner, try fixture.waitServingSchema(ctx, "usage_records", self.owner.version));
            try rows(ctx, false);
            return;
        }
        // Validation failure is not confirmed until the terminal owner cleanup
        // barrier; all old rows and the public schema must remain intact.
        try self.submit(ctx, original.sql, false);
        try std.testing.expectEqualDeep(self.owner, try fixture.waitServingSchema(ctx, "usage_records", self.owner.version));
        try rows(ctx, false);
        try command(ctx, "UPDATE usage_records SET status='ready' WHERE id='" ++ first ++ "'", null);
        try rows(ctx, true);
        var relation = try resolve(ctx);
        defer relation.deinit();
        self.image = try primary(ctx, relation.value.relations[0].?);
        ctx.data.loseNextSchemaValidationBeginReplyForTest();
        try self.submit(ctx, original.sql, true);
        try ctx.data.requireSchemaValidationBeginReplyRecoveryForTest();
        const after = try fixture.waitServingSchema(ctx, "usage_records", self.owner.version + 1);
        try std.testing.expectEqual(self.owner.table_id, after.table_id);
        self.owner = after;
        try std.testing.expectEqualDeep(self.image, try primary(ctx, relation.value.relations[0].?));
        try rows(ctx, true);
        try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES('00000000-0000-0000-0000-000000000003',NULL,3)", "23502");
        try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES('" ++ first ++ "','duplicate',3)", "23505");
        try command(ctx, "INSERT INTO usage_records(id,status,amount) VALUES('00000000-0000-0000-0000-000000000003','invalid',-1)", "23514");
        try rows(ctx, true);
        // A second exact attempt must recover while UNFINISHED, not merely
        // preserve a completed receipt. Pause after real freeze/permission and
        // before the first page, then cold-reopen metadata and data together.
        const loosen = try corpus.get("sql-0745");
        try command(ctx, loosen.sql, null);
        const relaxed = try fixture.waitServingSchema(ctx, "usage_records", self.owner.version + 1);
        try std.testing.expectEqual(self.owner.table_id, relaxed.table_id);
        self.owner = relaxed;
        if (self.restart_stage) |stage|
            ctx.data.pauseSchemaValidationAckForTest(self.owner.table_id, stage)
        else
            ctx.data.pauseSchemaValidationScanForTest(self.owner.table_id);
        const cleanup_before = ctx.data.graph_cleanup_attempts.load(.acquire);
        const source_before = ctx.data.group_source_attempts.load(.acquire);
        const pause_started = platform.time.monotonicNs();
        var pending = try fixture.sql(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, original.sql);
        defer pending.deinit(ctx.alloc);
        try std.testing.expect(pending.status == 202 or pending.status == 409);
        var unfinished = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, pending.body, .{});
        defer unfinished.deinit();
        const retained = unfinished.value.ddl_receipt.?.schema_validation_token orelse return error.MissingSchemaValidationToken;
        try std.testing.expectEqual(self.token.len, retained.len);
        @memcpy(&self.token, retained);
        try self.awaitPaused(ctx);
        const cleanup_quanta = (platform.time.monotonicNs() -| pause_started) / (100 * std.time.ns_per_ms);
        try std.testing.expect(ctx.data.graph_cleanup_attempts.load(.acquire) -| cleanup_before <= cleanup_quanta + 2);
        try std.testing.expect(ctx.data.group_source_attempts.load(.acquire) -| source_before <= cleanup_quanta + 2);
        try std.testing.expectEqual(wire.SchemaValidationState.pending, try self.status(ctx));
        const published = self.restart_stage == .released;
        var logical = try resolve(ctx);
        defer logical.deinit();
        const visible = logical.value.relations[0].?.owner;
        try std.testing.expectEqual(self.owner.table_id, visible.table_id);
        try std.testing.expectEqual(self.owner.version + @intFromBool(published), visible.schema_version);
        // Installation may deliberately defer serving until publication. The
        // final exact image comparison is made after durable recovery below.
        if (self.restart_stage == null or self.restart_stage == .ready)
            try std.testing.expectEqualDeep(self.image, try primary(ctx, relation.value.relations[0].?));
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.owner.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        if (self.failureRestart()) {
            try self.awaitState(ctx, false);
            try std.testing.expect(ctx.data.schema_recovery_attempts.load(.acquire) != 0);
            try std.testing.expectEqualDeep(self.owner, try fixture.waitServingSchema(ctx, "usage_records", self.owner.version));
            var relation = try resolve(ctx);
            defer relation.deinit();
            try std.testing.expectEqualDeep(self.image, try primary(ctx, relation.value.relations[0].?));
            try rows(ctx, false);
            try command(ctx, "UPDATE usage_records SET status='ready' WHERE id='" ++ first ++ "'", null);
            var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
            defer corpus.deinit();
            try self.submit(ctx, (try corpus.get(self.original_case)).sql, true);
            _ = try fixture.waitServingSchema(ctx, "usage_records", self.owner.version + 1);
            try rows(ctx, true);
            try command(ctx, "INSERT INTO usage_records(id,status) VALUES('00000000-0000-0000-0000-000000000003',NULL)", "23502");
            return;
        }
        try self.awaitState(ctx, true);
        try std.testing.expect(ctx.data.schema_recovery_attempts.load(.acquire) != 0);
        const after = try fixture.waitServingSchema(ctx, "usage_records", self.owner.version + 1);
        try std.testing.expectEqual(self.owner.table_id, after.table_id);
        try std.testing.expectEqual(self.owner.version + 1, after.version);
        self.owner = after;
        var relation = try resolve(ctx);
        defer relation.deinit();
        try std.testing.expectEqualDeep(self.image, try primary(ctx, relation.value.relations[0].?));
        try rows(ctx, true);
        try command(ctx, "INSERT INTO usage_records(id,status) VALUES('00000000-0000-0000-0000-000000000003',NULL)", "23502");
    }
};

test "mounted SQL original validation publication retains UUID owners rows constraints and exact status through cold recovery" {
    try fixture.runWithState(Campaign, .{ .original_case = "sql-0744" });
}

test "mounted SQL original validation publication recovers unacknowledged ready owners" {
    try fixture.runWithState(Campaign, .{ .original_case = "sql-0744", .restart_stage = .ready });
}

test "mounted SQL original validation publication recovers unacknowledged installed owners" {
    try fixture.runWithState(Campaign, .{ .original_case = "sql-0744", .restart_stage = .installed });
}

test "mounted SQL original validation publication recovers unacknowledged released owners" {
    try fixture.runWithState(Campaign, .{ .original_case = "sql-0744", .restart_stage = .released });
}

test "mounted SQL original validation publication recovers unacknowledged failed owners" {
    try fixture.runWithState(Campaign, .{ .original_case = "sql-0744", .restart_stage = .failed });
}

test "mounted SQL original validation publication recovers unacknowledged canceled owners" {
    try fixture.runWithState(Campaign, .{ .original_case = "sql-0744", .restart_stage = .canceled });
}
