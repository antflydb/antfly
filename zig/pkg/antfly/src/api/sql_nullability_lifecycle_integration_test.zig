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

//! Original nullability DDL through real schema-rewrite publication and restart.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const execute = @import("sql_owner_test_client.zig").execute;
const wire = @import("antfly_metadata_openapi").types;

const Expected = struct { id: i64, status: ?[]const u8, upper: ?[]const u8 };
const initial = [_]Expected{ .{ .id = 1, .status = null, .upper = null }, .{ .id = 2, .status = "open", .upper = "OPEN" } };
const repaired = [_]Expected{ .{ .id = 1, .status = "ready", .upper = "READY" }, initial[1] };
const relaxed = repaired ++ [_]Expected{ .{ .id = 3, .status = null, .upper = null }, .{ .id = 4, .status = "open", .upper = "OPEN" } };

fn expectRows(ctx: *fixture.Context, expected: []const Expected) !void {
    var response = try execute(ctx, "SELECT id,status,status_upper FROM usage_records ORDER BY id", .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("nullability postimage status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.columns.len);
    try std.testing.expectEqualStrings("id", parsed.value.columns[0].name);
    try std.testing.expectEqualStrings("status", parsed.value.columns[1].name);
    try std.testing.expectEqualStrings("status_upper", parsed.value.columns[2].name);
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    for (expected, parsed.value.rows, 0..) |want, row, i| {
        try std.testing.expectEqual(@as(usize, 3), row.len);
        try std.testing.expectEqual(want.id, try fixture.sqlInteger(row[0]));
        for ([_]?[]const u8{ want.status, want.upper }, row[1..], 1..) |text, cell, column| {
            if (text) |value| {
                try std.testing.expect(cell == .string);
                try std.testing.expectEqualStrings(value, cell.string);
            } else try std.testing.expect(cell == .null);
            if (parsed.value.sql_nulls) |flags| try std.testing.expectEqual(text == null, flags[i][column]);
        }
    }
}

fn command(ctx: *fixture.Context, statement: []const u8, creation: bool) !void {
    var response = try execute(ctx, statement, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and !(creation and response.status == 202)) {
        std.debug.print("nullability command status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
        return error.NullabilityCommandFailed;
    }
}

fn expectOwner(ctx: *fixture.Context, expected: fixture.ServingSchema) !void {
    try std.testing.expectEqualDeep(expected, try fixture.waitServingSchema(ctx, "usage_records", expected.version));
}

const Job = struct {
    bytes: [128]u8 = undefined,
    len: usize = 0,
    fn id(self: *const Job) []const u8 {
        return self.bytes[0..self.len];
    }
};

fn awaitJob(ctx: *fixture.Context, handle: *const Job, succeeds: bool) !void {
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.job(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, handle.id());
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const phase = (try fixture.field(parsed.value, "phase")).string;
        if (std.mem.eql(u8, phase, if (succeeds) "succeeded" else "failed")) {
            if (!succeeds) try std.testing.expectEqualStrings("BackupIntegrityFailure", (try fixture.field(parsed.value, "error")).string);
            return;
        }
        if (std.mem.eql(u8, phase, if (succeeds) "failed" else "succeeded")) {
            std.debug.print("unexpected nullability job: {s}\n", .{response.body});
            return error.UnexpectedNullabilityPublication;
        }
        try ctx.io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.NullabilityPublicationTimeout;
}

fn rewrite(ctx: *fixture.Context, statement: []const u8, handle: *Job, succeeds: bool) !void {
    // Submit once. A pending/unknown acknowledgement is recovered by its job,
    // never by repeating ALTER and possibly creating another physical target.
    var response = try fixture.sql(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, statement);
    defer response.deinit(ctx.alloc);
    if (response.status != 202 and response.status != 409) {
        std.debug.print("nullability admission status={d} body={s}\n", .{ response.status, response.body });
        return error.UnexpectedNullabilityAdmission;
    }
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const receipt = parsed.value.ddl_receipt orelse return error.MissingNullabilityReceipt;
    const expected_state: @TypeOf(receipt.state) = if (response.status == 202) .pending else .admission_unknown;
    try std.testing.expectEqual(expected_state, receipt.state);
    const id = receipt.restore_job_id orelse return error.MissingNullabilityJob;
    if (id.len == 0 or id.len > handle.bytes.len) return error.InvalidNullabilityJob;
    @memcpy(handle.bytes[0..id.len], id);
    handle.len = id.len;
    try awaitJob(ctx, handle, succeeds);
}

const Campaign = struct {
    failed: Job = .{},
    succeeded: Job = .{},
    final_owner: fixture.ServingSchema = undefined,

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const tighten = try corpus.get("sql-0744");
        const loosen = try corpus.get("sql-0745");
        try std.testing.expectEqual(@as(usize, 0), tighten.params.len);
        try std.testing.expectEqual(@as(usize, 0), loosen.params.len);
        try command(ctx, "CREATE TABLE usage_records (id BIGINT, status TEXT DEFAULT 'open', status_upper TEXT GENERATED ALWAYS AS (upper(status)) STORED)", true);
        const before = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, before.table_id);
        try command(ctx, "INSERT INTO usage_records (id,status) VALUES (1,NULL),(2,'open')", false);
        try expectRows(ctx, &initial);
        var denied = try fixture.sql(ctx.alloc, ctx.transport, ctx.reader_headers, ctx.metadata_uri, tighten.sql);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
        try expectOwner(ctx, before);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try rewrite(ctx, tighten.sql, &self.failed, false);
        try expectOwner(ctx, before);
        try expectRows(ctx, &initial);
        try command(ctx, "UPDATE usage_records SET status='ready' WHERE id=1", false);
        try expectRows(ctx, &repaired);
        try rewrite(ctx, tighten.sql, &self.succeeded, true);
        const tight = try fixture.waitServingSchema(ctx, "usage_records", before.version + 1);
        // This is the semantic/publication gate, not evidence that a full
        // physical rewrite is necessary. Validation-only publication must
        // eventually retain the owner; no copy requirement belongs here.
        try std.testing.expectEqual(before.version + 1, tight.version);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, tight.table_id);
        try expectRows(ctx, &repaired);
        try command(ctx, tighten.sql, false);
        try expectOwner(ctx, tight);
        var rejected = try execute(ctx, "INSERT INTO usage_records (id,status) VALUES (3,NULL)", .write_admission);
        defer rejected.deinit(ctx.alloc);
        try std.testing.expect(rejected.status >= 400);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, rejected.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("23502", diagnostic.value.code);
        try expectRows(ctx, &repaired);
        try command(ctx, loosen.sql, false);
        self.final_owner = try fixture.waitServingSchema(ctx, "usage_records", tight.version + 1);
        try std.testing.expectEqual(tight.table_id, self.final_owner.table_id);
        try std.testing.expectEqual(tight.version + 1, self.final_owner.version);
        try command(ctx, loosen.sql, false);
        try expectOwner(ctx, self.final_owner);
        try command(ctx, "INSERT INTO usage_records (id,status) VALUES (3,NULL)", false);
        try command(ctx, "INSERT INTO usage_records (id) VALUES (4)", false);
        try expectRows(ctx, &relaxed);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.final_owner.table_id);
        try expectOwner(ctx, self.final_owner);
        try expectRows(ctx, &relaxed);
        try awaitJob(ctx, &self.failed, false);
        try awaitJob(ctx, &self.succeeded, true);
        try command(ctx, "INSERT INTO usage_records (id) VALUES (5)", false);
        try command(ctx, "INSERT INTO usage_records (id,status) VALUES (6,NULL)", false);
        try expectRows(ctx, &(relaxed ++ [_]Expected{ .{ .id = 5, .status = "open", .upper = "OPEN" }, .{ .id = 6, .status = null, .upper = null } }));
    }
};

test "mounted SQL original nullability validates staged generations and preserves relaxation across restart" {
    // Exact originals sql-0744 and sql-0745, retrieved unchanged by Campaign.
    // Semantic/recovery evidence only; validation-only tightening and the
    // original UUID-primary-key setup remain separate qualification work.
    try fixture.run(Campaign);
}
