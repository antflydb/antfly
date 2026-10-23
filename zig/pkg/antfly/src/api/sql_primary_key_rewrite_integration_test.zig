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

//! Mounted SQL primary-key rewrite, including metadata-owned donor routing.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const http_server = @import("http_server.zig");
const test_helpers = @import("../public_test_helpers.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

const fixture = @import("sql_owner_test_fixture.zig");
const requestWithTimeout = fixture.requestWithTimeout;
const sql = fixture.sql;
const table = fixture.table;
const batch = fixture.batch;
const document = fixture.document;
const job = fixture.job;
const field = fixture.field;
const sqlInteger = fixture.sqlInteger;
const waitRelationPublication = fixture.waitRelationPublication;
const waitForDonorRoute = fixture.waitForDonorRoute;

fn expectSqlPostimage(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !void {
    const statement = try std.fmt.allocPrint(alloc, "SELECT id, note FROM {s} ORDER BY id", .{name});
    defer alloc.free(statement);
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(body);
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (true) {
        const remaining_ns = deadline -| platform.time.monotonicNs();
        if (remaining_ns == 0) return error.SqlPostimageReadTimeout;
        const timeout_ms: u32 = @intCast(@max(1, remaining_ns / std.time.ns_per_ms));
        var response = try requestWithTimeout(alloc, transport, headers, base, "/db/v1/sql", .POST, body, timeout_ms);
        defer response.deinit(alloc);
        if (response.status != 200) {
            // Repeating this pure read is safe, but only tolerate the
            // server's explicit transient-read diagnostic, for bounded time.
            if (response.status == 503 and platform.time.monotonicNs() < deadline) {
                var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, retryable: bool = false }, alloc, response.body, .{ .ignore_unknown_fields = true });
                defer diagnostic.deinit();
                if (diagnostic.value.retryable and std.mem.eql(u8, diagnostic.value.code, "53300")) {
                    try io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                }
            }
            std.debug.print("PK SQL postimage table={s} status={d} body={s}\n", .{ name, response.status, response.body });
            return error.UnexpectedRewrittenSqlResponse;
        }
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
        defer parsed.deinit();
        const rows = try field(parsed.value, "rows");
        try std.testing.expect(rows == .array);
        try std.testing.expectEqual(@as(usize, 2), rows.array.items.len);
        for (rows.array.items, [_]i64{ 1, 2 }, [_][]const u8{ "first", "second" }) |row, expected, note| {
            try std.testing.expect(row == .array);
            try std.testing.expectEqual(@as(usize, 2), row.array.items.len);
            try std.testing.expectEqual(expected, try sqlInteger(row.array.items[0]));
            try std.testing.expect(row.array.items[1] == .string);
            try std.testing.expectEqualStrings(note, row.array.items[1].string);
        }
        return;
    }
}

fn expectDocumentPostimage(alloc: std.mem.Allocator, bytes: []const u8, id: ?i64, note: []const u8) !void {
    var parsed = try std.json.parseFromSlice(struct { id: ?i64, note: []const u8 }, alloc, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(id, parsed.value.id);
    try std.testing.expectEqualStrings(note, parsed.value.note);
}

/// A local ReadIndex does not prove metadata can route the private donor RPC.
/// Await the exact installed metadata router's projected store-status route.
const Campaign = struct {
    const cases = [_]struct { name: []const u8, rows: []const u8, succeeds: bool, generated: bool = false, has_null: bool = false }{
        .{ .name = "pk_good", .rows = "{\"inserts\":{\"row-a\":{\"id\":1,\"note\":\"first\"},\"row-b\":{\"id\":2,\"note\":\"second\"}},\"sync_level\":\"full_text\"}", .succeeds = true },
        .{ .name = "pk_null", .rows = "{\"inserts\":{\"row-null\":{\"id\":null,\"note\":\"missing\"}},\"sync_level\":\"full_text\"}", .succeeds = false, .has_null = true },
        .{ .name = "pk_duplicate", .rows = "{\"inserts\":{\"row-a\":{\"id\":7,\"note\":\"first\"},\"row-b\":{\"id\":7,\"note\":\"second\"}},\"sync_level\":\"full_text\"}", .succeeds = false },
        .{ .name = "pk_anonymous", .rows = "{\"inserts\":{\"row-a\":{\"id\":1,\"note\":\"first\"},\"row-b\":{\"id\":2,\"note\":\"second\"}},\"sync_level\":\"full_text\"}", .succeeds = true, .generated = true },
        .{ .name = "pk_anonymous_null", .rows = "{\"inserts\":{\"row-null\":{\"id\":null,\"note\":\"missing\"}},\"sync_level\":\"full_text\"}", .succeeds = false, .generated = true, .has_null = true },
        .{ .name = "pk_anonymous_duplicate", .rows = "{\"inserts\":{\"row-a\":{\"id\":7,\"note\":\"first\"},\"row-b\":{\"id\":7,\"note\":\"second\"}},\"sync_level\":\"full_text\"}", .succeeds = false, .generated = true },
    };
    terminal_table_ids: [cases.len]u64 = undefined,

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        const alloc = ctx.alloc;
        const metadata = ctx.metadata;
        const data = ctx.data;
        const transport = ctx.transport;
        const headers = ctx.headers;
        const base = ctx.base;
        const rewrite_headers = ctx.admin_headers;
        const reader_headers = ctx.reader_headers;
        const metadata_uri = ctx.metadata_uri;
        const terminal_table_ids = &self.terminal_table_ids;
        for (cases, 0..) |case, case_index| {
            const create = try std.fmt.allocPrint(alloc, "CREATE TABLE {s} (id BIGINT, note TEXT)", .{case.name});
            defer alloc.free(create);
            var created = try sql(alloc, transport, headers, base, create);
            defer created.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), created.status);
            var before_response = try table(alloc, transport, headers, base, case.name);
            defer before_response.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), before_response.status);
            var before = try std.json.parseFromSlice(std.json.Value, alloc, before_response.body, .{});
            defer before.deinit();
            const before_id = (try field(before.value, "table_id")).string;
            try waitForDonorRoute(alloc, ctx.io, metadata, data, try std.fmt.parseUnsigned(u64, before_id, 10));
            var basic_read = try table(alloc, transport, rewrite_headers, metadata_uri, case.name);
            defer basic_read.deinit(alloc);
            if (basic_read.status != 200) std.debug.print("PK Basic metadata read table={s} status={d} body={s}\n", .{ case.name, basic_read.status, basic_read.body });
            try std.testing.expectEqual(@as(u16, 200), basic_read.status);
            var inserted = blk: {
                for (0..600) |_| {
                    var attempt = try batch(alloc, transport, headers, base, case.name, case.rows);
                    if (attempt.status != 503) break :blk attempt;
                    attempt.deinit(alloc);
                    try ctx.io.sleep(.fromMilliseconds(10), .awake);
                }
                return error.TablePlacementTimeout;
            };
            defer inserted.deinit(alloc);
            if (inserted.status != 201) std.debug.print("PK setup batch table={s} status={d} body={s}\n", .{ case.name, inserted.status, inserted.body });
            try std.testing.expectEqual(@as(u16, 201), inserted.status);
            if (case.succeeds) {
                for ([_][]const u8{ "row-a", "row-b" }) |key| {
                    var fetched = try document(alloc, transport, headers, base, case.name, key);
                    defer fetched.deinit(alloc);
                    std.debug.print("PK source GET key={s} status={d} body={s}\n", .{ key, fetched.status, fetched.body });
                    if (fetched.status != 200) return error.UnexpectedSourceLookup;
                }
                try expectSqlPostimage(alloc, ctx.io, transport, headers, base, case.name);
            }
            const alter = if (case.generated)
                try std.fmt.allocPrint(alloc, "ALTER TABLE {s} ADD PRIMARY KEY (id)", .{case.name})
            else
                try std.fmt.allocPrint(alloc, "ALTER TABLE {s} ADD CONSTRAINT {s}_key PRIMARY KEY (id)", .{ case.name, case.name });
            defer alloc.free(alter);
            var forbidden = try sql(alloc, transport, reader_headers, metadata_uri, alter);
            defer forbidden.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 403), forbidden.status);
            var after_forbidden_response = try table(alloc, transport, headers, base, case.name);
            defer after_forbidden_response.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), after_forbidden_response.status);
            var after_forbidden = try std.json.parseFromSlice(std.json.Value, alloc, after_forbidden_response.body, .{});
            defer after_forbidden.deinit();
            try std.testing.expectEqualStrings(before_id, (try field(after_forbidden.value, "table_id")).string);
            try waitRelationPublication(alloc, ctx.io, metadata, case.name);
            var admitted = try sql(alloc, transport, rewrite_headers, metadata_uri, alter);
            defer admitted.deinit(alloc);
            if (admitted.status != 202 and admitted.status != 409) std.debug.print("PK ALTER table={s} status={d} body={s}\n", .{ case.name, admitted.status, admitted.body });
            try std.testing.expect(admitted.status == 202 or admitted.status == 409);
            var admitted_json = try std.json.parseFromSlice(std.json.Value, alloc, admitted.body, .{});
            defer admitted_json.deinit();
            const receipt = try field(admitted_json.value, "ddl_receipt");
            try std.testing.expectEqualStrings(if (admitted.status == 202) "pending" else "admission_unknown", (try field(receipt, "state")).string);
            const job_id = (try field(receipt, "restore_job_id")).string;
            if (admitted.status == 409) {
                // This is a read-only recovery handle. Never replay the ALTER.
                var first_status = try job(alloc, transport, rewrite_headers, metadata_uri, job_id);
                defer first_status.deinit(alloc);
                std.debug.print("PK uncertain admission job status table={s} status={d} body={s}\n", .{ case.name, first_status.status, first_status.body });
                if (first_status.status != 200) return error.RewriteAdmissionUnknownWithoutVisibleJob;
            }
            var terminal = false;
            var last_phase: [64]u8 = undefined;
            var last_phase_len: usize = 0;
            const started_ns = platform.time.monotonicNs();
            const deadline_ns = started_ns +| 30 * std.time.ns_per_s;
            for (0..300) |_| {
                if (platform.time.monotonicNs() >= deadline_ns) break;
                try ctx.checkDrivers();
                var response = try job(alloc, transport, rewrite_headers, metadata_uri, job_id);
                defer response.deinit(alloc);
                try std.testing.expectEqual(@as(u16, 200), response.status);
                var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
                defer parsed.deinit();
                const phase = (try field(parsed.value, "phase")).string;
                last_phase_len = @min(phase.len, last_phase.len);
                @memcpy(last_phase[0..last_phase_len], phase[0..last_phase_len]);
                if (std.mem.eql(u8, phase, if (case.succeeds) "succeeded" else "failed")) {
                    std.debug.print("PK job terminal table={s} body={s}\n", .{ case.name, response.body });
                    if (!case.succeeds) try std.testing.expectEqualStrings(
                        if (case.has_null) "BackupIntegrityFailure" else "ConstraintActivationFailed",
                        (try field(parsed.value, "error")).string,
                    );
                    terminal = true;
                    break;
                }
                if (std.mem.eql(u8, phase, if (case.succeeds) "failed" else "succeeded")) return error.UnexpectedRewriteOutcome;
                try ctx.io.sleep(.fromMilliseconds(10), .awake);
            }
            if (!terminal) std.debug.print("PK job nonterminal table={s} last_phase={s}\n", .{ case.name, last_phase[0..last_phase_len] });
            try std.testing.expect(terminal);
            var after_response = try table(alloc, transport, headers, base, case.name);
            defer after_response.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), after_response.status);
            var after = try std.json.parseFromSlice(std.json.Value, alloc, after_response.body, .{});
            defer after.deinit();
            const after_id = (try field(after.value, "table_id")).string;
            terminal_table_ids[case_index] = try std.fmt.parseUnsigned(u64, after_id, 10);
            try std.testing.expectEqual(case.succeeds, !std.mem.eql(u8, before_id, after_id));
            const schema = try field(after.value, "schema");
            const constraints = if (schema == .object) schema.object.get("unique_constraints") else null;
            try std.testing.expectEqual(case.succeeds, constraints != null and constraints.? == .array and constraints.?.array.items.len == 1);
            if (case.succeeds) {
                const expected_name = try std.fmt.allocPrint(alloc, "{s}_{s}", .{ case.name, if (case.generated) "pkey" else "key" });
                defer alloc.free(expected_name);
                try std.testing.expectEqualStrings(expected_name, (try field(constraints.?.array.items[0], "name")).string);
                var duplicate = try batch(alloc, transport, headers, base, case.name, "{\"inserts\":{\"row-duplicate\":{\"id\":1,\"note\":\"duplicate\"}},\"sync_level\":\"full_text\"}");
                defer duplicate.deinit(alloc);
                try std.testing.expectEqual(@as(u16, 409), duplicate.status);
                try std.testing.expect(std.mem.indexOf(u8, duplicate.body, "UniqueConstraintViolation") != null);
                try expectSqlPostimage(alloc, ctx.io, transport, headers, base, case.name);
            } else {
                const keys: []const []const u8 = if (case.has_null) &.{"row-null"} else &.{ "row-a", "row-b" };
                for (keys, 0..) |key, key_index| {
                    var fetched = try document(alloc, transport, headers, base, case.name, key);
                    defer fetched.deinit(alloc);
                    try std.testing.expectEqual(@as(u16, 200), fetched.status);
                    try expectDocumentPostimage(alloc, fetched.body, if (case.has_null) null else 7, if (case.has_null) "missing" else if (key_index == 0) "first" else "second");
                }
            }
        }
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        const alloc = ctx.alloc;
        const metadata = ctx.metadata;
        const data = ctx.data;
        const transport = ctx.transport;
        const headers = ctx.headers;
        const restarted_base = ctx.base;
        const terminal_table_ids = self.terminal_table_ids;
        for (cases, terminal_table_ids) |case, expected_table_id| {
            try waitForDonorRoute(alloc, ctx.io, metadata, data, expected_table_id);
            var response = try table(alloc, transport, headers, restarted_base, case.name);
            defer response.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(expected_table_id, try std.fmt.parseUnsigned(u64, (try field(parsed.value, "table_id")).string, 10));
            const schema = try field(parsed.value, "schema");
            const constraints = if (schema == .object) schema.object.get("unique_constraints") else null;
            try std.testing.expectEqual(case.succeeds, constraints != null and constraints.? == .array and constraints.?.array.items.len == 1);
            const key = if (case.has_null) "row-null" else "row-a";
            var fetched = try document(alloc, transport, headers, restarted_base, case.name, key);
            defer fetched.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), fetched.status);
            try expectDocumentPostimage(alloc, fetched.body, if (case.succeeds) 1 else if (case.has_null) null else 7, if (case.has_null) "missing" else "first");
            if (case.succeeds) {
                const expected_name = try std.fmt.allocPrint(alloc, "{s}_{s}", .{ case.name, if (case.generated) "pkey" else "key" });
                defer alloc.free(expected_name);
                try std.testing.expectEqualStrings(expected_name, (try field(constraints.?.array.items[0], "name")).string);
                var duplicate = try batch(alloc, transport, headers, restarted_base, case.name, "{\"inserts\":{\"restart-duplicate\":{\"id\":1}},\"sync_level\":\"full_text\"}");
                defer duplicate.deinit(alloc);
                try std.testing.expectEqual(@as(u16, 409), duplicate.status);
                try std.testing.expect(std.mem.indexOf(u8, duplicate.body, "UniqueConstraintViolation") != null);
                try expectSqlPostimage(alloc, ctx.io, transport, headers, restarted_base, case.name);
            } else if (!case.has_null) {
                var second = try document(alloc, transport, headers, restarted_base, case.name, "row-b");
                defer second.deinit(alloc);
                try std.testing.expectEqual(@as(u16, 200), second.status);
                try expectDocumentPostimage(alloc, second.body, 7, "second");
            }
        }
    }
};

test "mounted SQL ADD PRIMARY KEY publishes only validated fresh generation" {
    try fixture.run(Campaign);
}
