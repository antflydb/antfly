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

//! Unchanged maintenance originals through mounted SQL and cold native owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
fn ok(ctx: *fixture.Context, sql: []const u8) !void {
    var response = try client.execute(ctx, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("maintenance SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
}
const Reference = struct {
    cases: []const struct { id: []const u8, sql: []const u8, command_tag: []const u8 },
    rows: []const []const std.json.Value,
};
const FaultState = struct {
    logical_name: []const u8 = "",
    table_id: u64 = 0,
    schema_version: u32 = 0,
    group_id: u64 = 0,
    owner_rows: u64 = 0,
    physical_name: ?[]u8 = null,
    fn deinit(self: *FaultState, a: std.mem.Allocator) void {
        if (self.physical_name) |name| a.free(name);
    }
};
fn quantum(ctx: *fixture.Context, state: FaultState, nonce: u8, reading: bool) !local.api_sql_maintenance_contract.Response {
    const contract = local.api_sql_maintenance_contract;
    const writer = ctx.data.http_server.?.table_writes orelse return error.UnsupportedSqlExecution;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, contract.Request{ .table_id = state.table_id, .schema_version = state.schema_version, .request_id = @splat(nonce), .operation = if (reading) .statistics else .analyze, .max_rows = 1 }, .{});
    defer ctx.alloc.free(body);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
    while (true) {
        const raw = writer.sqlMaintenanceGroup(ctx.alloc, state.group_id, state.physical_name.?, body, .{}) catch |err| switch (err) {
            error.MaintenanceBusy, error.LeaderUnavailable, error.GroupLeaderUnavailable, error.Unavailable, error.StorageReadTemporarilyUnavailable, error.StorageBusy, error.NotLeader => {
                if (@import("antfly_platform").time.monotonicNs() >= deadline) return err;
                try ctx.io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => return err,
        } orelse return error.UnsupportedSqlExecution;
        defer ctx.alloc.free(raw);
        var response = try std.json.parseFromSlice(contract.Response, ctx.alloc, raw, .{});
        defer response.deinit();
        // The metadata probe owns any returned fields beyond this call.
        return .{ .table_id = response.value.table_id, .schema_version = response.value.schema_version, .done = response.value.done, .rows_scanned = if (reading) (if (response.value.statistics) |value| value.rows else 0) else response.value.rows_scanned, .available = response.value.available };
    }
}
fn prepareFault(ctx: *fixture.Context, state: *FaultState, name: []const u8, document: bool) !void {
    const suffix = " WITH (antfly.storage_mode='document',antfly.default_type='doc') DOCUMENT SCHEMA doc AS JSON '{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}'";
    const create = if (document) try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE {s}(){s}", .{ name, suffix }) else try std.fmt.allocPrint(ctx.alloc, "CREATE TABLE {s}(id BIGINT PRIMARY KEY,status TEXT)", .{name});
    defer ctx.alloc.free(create);
    try ok(ctx, create);
    const serving = try fixture.waitServingSchema(ctx, name, 0);
    var insert = std.Io.Writer.Allocating.init(ctx.alloc);
    defer insert.deinit();
    try insert.writer.print("INSERT INTO {s}({s}) VALUES ", .{ name, if (document) @as([]const u8, "_id,id,status") else "id,status" });
    for (0..256) |i| {
        if (i != 0) try insert.writer.writeByte(',');
        if (document) try insert.writer.print("('k{d}',{d},'chunk')", .{ i, i }) else try insert.writer.print("({d},'chunk')", .{i});
    }
    try ok(ctx, insert.written());
    const analyze = try std.fmt.allocPrint(ctx.alloc, "ANALYZE {s}", .{name});
    defer ctx.alloc.free(analyze);
    try ok(ctx, analyze);
    var cut = try ctx.metadata.server.svc.adminSnapshot();
    defer ctx.metadata.server.svc.freeAdminSnapshot(&cut);
    const table = for (cut.tables) |table| {
        if (table.table_id == serving.table_id) break table;
    } else return error.TableNotFound;
    state.* = .{ .logical_name = name, .table_id = serving.table_id, .schema_version = serving.version, .physical_name = try ctx.alloc.dupe(u8, table.name) };
    for (cut.ranges) |range| {
        if (range.table_id != serving.table_id) continue;
        state.group_id = range.group_id;
        const stats = try quantum(ctx, state.*, 16, true);
        if (stats.available and stats.rows_scanned > 2) {
            state.owner_rows = stats.rows_scanned;
            break;
        }
    }
    try std.testing.expect(state.owner_rows > 2);
    const partial = try quantum(ctx, state.*, 17, false);
    try std.testing.expect(!partial.done);
    try std.testing.expectEqual(@as(u64, 1), partial.rows_scanned);
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    fault: *[2]FaultState,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try ok(ctx, "CREATE TABLE usage_records (id BIGINT NOT NULL, status TEXT, amount BIGINT, PRIMARY KEY(id))");
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try ok(ctx, "INSERT INTO usage_records (id,status,amount) VALUES (1,'pending',10),(2,'done',20),(3,NULL,30)");
        try ok(ctx, "UPDATE usage_records SET amount=amount+1");
        try ok(ctx, "UPDATE usage_records SET amount=amount-1");
        try ok(ctx, "CREATE INDEX usage_records_status_idx ON usage_records (status)");
        _ = try fixture.waitServingSchema(ctx, "usage_records", 1);
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, local.sql_parity_fixtures.maintenance_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        try std.testing.expectEqual(@as(usize, 6), reference.value.cases.len);
        for (reference.value.cases) |step| {
            try std.testing.expectEqualStrings((try corpus.get(step.id)).sql, step.sql);
            var response = try client.execute(ctx, step.sql, .write_admission);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("maintenance original={s} status={d} body={s}\n", .{ step.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings(step.command_tag, result.value.command_tag);
        }
        try self.check(ctx);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "VACUUM usage_records", .never);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
        try prepareFault(ctx, &self.fault[0], "statistics_chunks", false);
        try prepareFault(ctx, &self.fault[1], "document_statistics_chunks", true);
    }
    fn check(_: *@This(), ctx: *fixture.Context) !void {
        var response = try client.execute(ctx, "SELECT id,status,amount FROM usage_records ORDER BY id", .read);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("maintenance postimage status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, local.sql_parity_fixtures.maintenance_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        try std.testing.expectEqual(reference.value.rows.len, result.value.rows.len);
        try std.testing.expectEqual(@as(usize, 3), result.value.columns.len);
        for (reference.value.rows, result.value.rows, result.value.sql_nulls.?) |expected, actual, actual_nulls| {
            var expected_nulls: [3]bool = undefined;
            for (expected, &expected_nulls) |cell, *flag| flag.* = cell == .null;
            try std.testing.expect(try @import("sql_parity_reference.zig").rowMatchesWithNulls(ctx.alloc, result.value.columns, actual, actual_nulls, expected, &expected_nulls));
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.fault) |state| {
            _ = try fixture.waitServingSchema(ctx, state.logical_name, state.schema_version);
            // A different producer nonce adopts the unfinished objective.
            const resumed = try quantum(ctx, state, 18, false);
            try std.testing.expect(!resumed.done);
            try std.testing.expectEqual(@as(u64, 2), resumed.rows_scanned);
            const analyze = try std.fmt.allocPrint(ctx.alloc, "ANALYZE {s}", .{state.logical_name});
            defer ctx.alloc.free(analyze);
            try ok(ctx, analyze);
            const completed = try quantum(ctx, state, 19, true);
            try std.testing.expect(completed.available);
            try std.testing.expectEqual(state.owner_rows, completed.rows_scanned);
        }
        try self.check(ctx);
        try ok(ctx, "ANALYZE VERBOSE usage_records (status, amount)");
        try ok(ctx, "REINDEX INDEX CONCURRENTLY usage_records_status_idx");
        try ok(ctx, "UPDATE usage_records SET amount=amount+1 WHERE id=1");
        try ok(ctx, "VACUUM (FULL, VERBOSE, ANALYZE) usage_records");
    }
};
test "mounted SQL original maintenance preserves all owners rows authorization and cold progress" {
    const original_ids = [_][]const u8{ "sql-0072", "sql-0073", "sql-0074", "sql-0075", "sql-0076", "sql-0077" };
    var reference = try std.json.parseFromSlice(Reference, std.testing.allocator, local.sql_parity_fixtures.maintenance_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expectEqual(original_ids.len, reference.value.cases.len);
    for (original_ids, reference.value.cases) |expected, step| try std.testing.expectEqualStrings(expected, step.id);
    var states = [_]FaultState{ .{}, .{} };
    defer for (&states) |*state| state.deinit(std.testing.allocator);
    try fixture.runWithState(Campaign, .{ .fault = &states });
}
