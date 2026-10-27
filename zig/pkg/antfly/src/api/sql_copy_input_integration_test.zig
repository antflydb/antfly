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

//! COPY input through authenticated pgwire and native transaction owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const frames = @import("../pgwire/cursor_reference_test_support.zig");
const protocol = @import("../pgwire/protocol.zig");
const SourceCampaign = @import("sql_document_mutation_lifecycle_integration_test.zig").SourceCampaign;
const Result = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const std.json.Value };
const Entry = struct {
    id: []const u8,
    sql: []const u8,
    format: []const u8,
    parameters: []const ?[]const u8 = &.{},
    reject_before_data: bool = false,
    hex: []const u8,
    tag: []const u8,
    output: Result,
    faults: []const struct { name: []const u8, hex: []const u8, sqlstate: []const u8 },
};
const Reference = struct { create: []const u8, read: []const u8, entries: []const Entry, advanced: []const Entry, predicates: []const Entry, predicate_errors: []const struct { sql: []const u8, sqlstate: []const u8 }, removed_oids: struct { id: []const u8, sql: []const u8, sqlstate: []const u8 }, identity: struct { create: []const u8, copy: []const u8, hex: []const u8, filter_sql: []const u8, filter_hex: []const u8, fault_hex: []const u8, fault_sqlstate: []const u8, after_hex: []const u8, output: Result }, rls_sqlstate: []const u8 };
const bytes = @embedFile("../pgwire/fixtures/copy_input_reference.json");

fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var response = try client.execute(ctx, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) std.debug.print("COPY setup sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
}

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    reference: Reference,
    retained: *std.heap.ArenaAllocator,
    baseline: ?SourceCampaign = null,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        for ([_][]const u8{ "pk-rewriter", "pk-reader" }) |username| inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", if (std.mem.eql(u8, username, "pk-rewriter")) .admin else .read);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser(username, permission);
        };
    }
    fn copy(ctx: *fixture.Context, entry: Entry, hex: []const u8, expected: ?[]const u8, reader_only: bool, rollback: bool, interrupted: bool) !void {
        const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const a = ctx.alloc;
        var input: std.Io.Writer.Allocating = .init(a);
        defer input.deinit();
        try frames.startup(a, &input.writer, if (reader_only) "pk-reader" else "pk-rewriter", if (reader_only) "pk-reader-durable-password" else "pk-rewrite-durable-password", "default");
        if (rollback) try frames.frame(&input.writer, 'Q', "BEGIN\x00");
        const statement = try std.fmt.allocPrint(a, "{s}\x00", .{entry.sql});
        defer a.free(statement);
        if (entry.parameters.len == 0) try frames.frame(&input.writer, 'Q', statement) else {
            var body: std.Io.Writer.Allocating = .init(a);
            defer body.deinit();
            try body.writer.writeByte(0);
            try body.writer.writeAll(statement);
            try body.writer.writeInt(u16, 0, .big);
            try frames.frame(&input.writer, 'P', body.written());
            body.writer.end = 0;
            try body.writer.writeAll("\x00\x00");
            try body.writer.writeInt(u16, 0, .big);
            try body.writer.writeInt(u16, @intCast(entry.parameters.len), .big);
            for (entry.parameters) |parameter| {
                if (parameter) |value| {
                    try body.writer.writeInt(i32, @intCast(value.len), .big);
                    try body.writer.writeAll(value);
                } else try body.writer.writeInt(i32, -1, .big);
            }
            try body.writer.writeInt(u16, 0, .big);
            try frames.frame(&input.writer, 'B', body.written());
            try frames.frame(&input.writer, 'E', "\x00\x00\x00\x00\x00");
            try frames.frame(&input.writer, 'S', "");
        }
        const payload = try a.alloc(u8, hex.len / 2);
        defer a.free(payload);
        _ = try std.fmt.hexToBytes(payload, hex);
        const early = entry.reject_before_data or reader_only or (expected != null and (std.mem.eql(u8, expected.?, "0A000") or std.mem.eql(u8, expected.?, "42601")));
        if (!early) {
            var offset: usize = 0;
            while (offset < payload.len) {
                const end = @min(offset + 7, payload.len);
                try frames.frame(&input.writer, 'd', payload[offset..end]);
                offset = end;
            }
            if (!interrupted) try frames.frame(&input.writer, 'c', "");
        }
        if (rollback) try frames.frame(&input.writer, 'Q', "ROLLBACK\x00");
        if (!interrupted) try frames.frame(&input.writer, 'X', "");
        var source = std.Io.Reader.fixed(input.written());
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
        var session: protocol.Session = .{ .alloc = a, .io = ctx.io, .source = adapter.backend(), .reader = &source, .writer = &output.writer };
        defer session.deinit();
        try session.run();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var errors: usize = 0;
        var completions: usize = 0;
        var accepted: usize = 0;
        while (cursor.offset != cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const length = try cursor.int(u32);
            var body: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
            if (tag == 'G') accepted += 1;
            if (tag == 'C') {
                const value = try body.string();
                if (std.mem.startsWith(u8, value, "COPY ")) {
                    try std.testing.expectEqualStrings(entry.tag, value);
                    completions += 1;
                }
            }
            if (tag == 'E') {
                errors += 1;
                var code: ?[]const u8 = null;
                var message: []const u8 = "";
                while (try body.int(u8) != 0) {
                    const field = body.bytes[body.offset - 1];
                    const value = try body.string();
                    if (field == 'C') code = value;
                    if (field == 'M') message = value;
                }
                if (expected) |want| try std.testing.expectEqualStrings(want, code orelse return error.MissingCopyDiagnostic) else if (!interrupted) {
                    std.debug.print("native COPY original={s} sql={s} accepted={d} code={s} message={s}\n", .{ entry.id, entry.sql, accepted, code orelse "missing", message });
                    return error.UnexpectedCopyFailure;
                }
            }
        }
        try std.testing.expectEqual(@as(usize, if (expected != null or interrupted) 1 else 0), errors);
        try std.testing.expectEqual(@as(usize, if (expected != null or interrupted) 0 else 1), completions);
        try std.testing.expectEqual(@as(usize, if (early) 0 else 1), accepted);
        try ctx.checkDrivers();
    }
    fn capture(self: *@This(), ctx: *fixture.Context, entry: Entry) !void {
        const a = self.retained.allocator();
        const tables = try a.alloc(@import("sql_document_mutation_lifecycle_integration_test.zig").SourceTable, 1);
        tables[0] = .{ .name = "usage_records", .create = self.reference.create, .seed = "", .read = self.reference.read, .final = .{ .columns = entry.output.columns, .column_oids = entry.output.column_oids, .rows = entry.output.rows } };
        self.baseline = .{ .retained = self.retained, .read_result_rows = 512, .entry = .{ .id = entry.id, .profile = "copy_input", .sqlstate = null, .command_tag = entry.tag, .affected = 300, .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} }, .tables = tables } };
        try self.baseline.?.verifyRows(ctx);
        try self.baseline.?.fingerprints(ctx, .initial);
    }
    fn unchanged(self: *@This(), ctx: *fixture.Context) !void {
        try self.baseline.?.verifyRows(ctx);
        try self.baseline.?.fingerprints(ctx, .prepared);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, self.reference.create);
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        var probe = self.reference.predicates[0];
        probe.tag = "COPY 1";
        try copy(ctx, probe, "302c6163746976650a", null, false, false, false);
        for (self.reference.entries) |entry| {
            try command(ctx, "DELETE FROM usage_records");
            try copy(ctx, entry, entry.hex, null, false, false, false);
            try self.capture(ctx, entry);
            for (entry.faults) |fault| {
                errdefer std.debug.print("COPY original={s} fault={s}\n", .{ entry.id, fault.name });
                try copy(ctx, entry, fault.hex, fault.sqlstate, false, false, false);
                try self.unchanged(ctx);
            }
            try copy(ctx, entry, entry.faults[3].hex, "22P04", false, true, false);
            try self.unchanged(ctx);
            try copy(ctx, entry, entry.hex, "42501", true, false, false);
            try self.unchanged(ctx);
            // A successful COPY inside an explicit transaction must remain
            // rollbackable. Delete first in a separate committed transaction.
            try command(ctx, "DELETE FROM usage_records");
            try copy(ctx, entry, entry.hex, null, false, true, false);
            var empty = try client.execute(ctx, "SELECT id FROM usage_records", .read);
            defer empty.deinit(ctx.alloc);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, empty.body, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 0), result.value.rows.len);
            // Even a complete set of CopyData frames cannot commit without
            // CopyDone. Disconnect must retire its staged native session.
            try copy(ctx, entry, entry.hex, null, false, false, true);
            try copy(ctx, entry, entry.hex, null, false, false, false);
            try self.capture(ctx, entry);
        }
        for (self.reference.advanced) |entry| {
            try command(ctx, "DELETE FROM usage_records");
            try copy(ctx, entry, entry.hex, null, false, false, false);
            try self.capture(ctx, entry);
            for (entry.faults) |fault| {
                errdefer std.debug.print("COPY advanced={s} fault={s}\n", .{ entry.id, fault.name });
                try copy(ctx, entry, fault.hex, fault.sqlstate, false, false, false);
                try self.unchanged(ctx);
            }
        }
        for (self.reference.predicates) |entry| {
            try command(ctx, "DELETE FROM usage_records");
            try copy(ctx, entry, entry.hex, null, false, false, false);
            try self.capture(ctx, entry);
            for (entry.faults) |fault| {
                errdefer std.debug.print("COPY predicate={s} fault={s}\n", .{ entry.id, fault.name });
                try copy(ctx, entry, fault.hex, fault.sqlstate, false, false, false);
                try self.unchanged(ctx);
            }
        }
        for (self.reference.predicate_errors) |failure| {
            const entry: Entry = .{ .id = "predicate-rejection", .sql = failure.sql, .format = "text", .hex = "", .tag = "", .output = self.reference.predicates[0].output, .faults = &.{}, .reject_before_data = true };
            try copy(ctx, entry, "", failure.sqlstate, false, false, false);
            try self.unchanged(ctx);
        }
        const removed = self.reference.removed_oids;
        const invalid: Entry = .{ .id = removed.id, .sql = removed.sql, .format = "csv", .hex = "", .tag = "", .output = self.reference.advanced[0].output, .faults = &.{} };
        try copy(ctx, invalid, "", removed.sqlstate, false, false, false);
        try self.unchanged(ctx);
        const identity = self.reference.identity;
        try command(ctx, identity.create);
        const identity_owner = try fixture.waitServingSchema(ctx, "copy_identity", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, identity_owner.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "copy_identity");
        var identity_entry: Entry = .{ .id = "identity", .sql = identity.copy, .format = "text", .hex = identity.hex, .tag = "COPY 1", .output = identity.output, .faults = &.{} };
        try copy(ctx, identity_entry, identity.hex, null, false, false, false);
        try command(ctx, "INSERT INTO copy_identity(status) VALUES('default')");
        identity_entry.sql = "COPY copy_identity(status) FROM STDIN";
        try copy(ctx, identity_entry, "6e6578740a", null, false, false, false);
        try copy(ctx, identity_entry, identity.fault_hex, identity.fault_sqlstate, false, false, false);
        try copy(ctx, identity_entry, identity.after_hex, null, false, false, false);
        identity_entry.sql = identity.filter_sql;
        try copy(ctx, identity_entry, identity.filter_hex, null, false, false, false);
        try self.checkIdentity(ctx);
        try command(ctx, "CREATE TABLE copy_rls(id BIGINT PRIMARY KEY,status TEXT)");
        const rls_owner = try fixture.waitServingSchema(ctx, "copy_rls", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, rls_owner.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "copy_rls");
        try command(ctx, "CREATE POLICY allow_all ON copy_rls FOR ALL USING(TRUE) WITH CHECK(TRUE)");
        try command(ctx, "ALTER TABLE copy_rls ENABLE ROW LEVEL SECURITY");
        _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, rls_owner.table_id, .active, 0);
        identity_entry.sql = "COPY copy_rls FROM STDIN";
        try copy(ctx, identity_entry, "", self.reference.rls_sqlstate, false, false, false);
        identity_entry.sql = "COPY copy_rls FROM STDIN WHERE FALSE";
        try copy(ctx, identity_entry, "", self.reference.rls_sqlstate, false, false, false);
        try self.baseline.?.fingerprints(ctx, .mutated);
    }
    fn checkIdentity(self: *@This(), ctx: *fixture.Context) !void {
        var response = try client.execute(ctx, "SELECT id,status FROM copy_identity ORDER BY id", .read);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        const wire = @import("antfly_metadata_openapi").types;
        var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        try std.testing.expectEqual(self.reference.identity.output.rows.len, result.value.rows.len);
        for (result.value.rows, self.reference.identity.output.rows) |actual, expected| try std.testing.expect(try @import("sql_parity_reference.zig").rowMatchesWithNulls(ctx.alloc, result.value.columns, actual, null, expected, null));
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try self.baseline.?.verify(ctx, true);
        try self.checkIdentity(ctx);
        const entry = self.reference.predicates[self.reference.predicates.len - 1];
        try copy(ctx, entry, entry.faults[0].hex, "23505", false, false, false);
        try self.unchanged(ctx);
    }
};

test "mounted SQL original COPY input preserves PostgreSQL atomic batches constraints and cold owners" {
    // sql-0370, sql-0371, sql-0379, sql-0380; sql-0381 rejects the removed OIDS option.
    // No statement rewriting or metadata-only credit.
    const a = std.testing.allocator;
    const reference = try std.json.parseFromSlice(Reference, a, bytes, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for (reference.value.entries) |entry| {
        const original = try corpus.get(entry.id);
        try std.testing.expectEqualStrings(original.sql, entry.sql);
    }
    const predicate_original = try corpus.get(reference.value.predicates[0].id);
    try std.testing.expectEqualStrings(predicate_original.sql, reference.value.predicates[0].sql);
    const removed = try corpus.get(reference.value.removed_oids.id);
    try std.testing.expectEqualStrings(removed.sql, reference.value.removed_oids.sql);
    var retained: std.heap.ArenaAllocator = .init(a);
    defer retained.deinit();
    try fixture.runWithState(Campaign, .{ .reference = reference.value, .retained = &retained });
}
