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

//! Exact server COPY originals over public SQL and cold native owners.
const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const Corpus = @import("antfly_local_sources").sql_parity_fixtures.Corpus;
const frames = @import("../pgwire/cursor_reference_test_support.zig");
const protocol = @import("../pgwire/protocol.zig");
const path = "/tmp/usage.csv";
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    authorization: [192]u8 = undefined,
    headers: [1]@import("../raft/transport/http_common.zig").RequestHeader = undefined,
    connection: [32]u8 = undefined,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        var permission = try users.Permission.initOwned(manager.alloc, .@"*", "*", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }
    fn activate(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
        var permission = try @import("../usermgr/mod.zig").Permission.initOwned(ctx.alloc, .@"*", "*", .admin);
        defer permission.deinit(ctx.alloc);
        var key = try manager.createApiKey("pk-rewriter", "server-copy", &.{permission}, &.{}, null);
        defer key.deinit(manager.alloc);
        self.headers[0] = .{ .name = "authorization", .value = try std.fmt.bufPrint(&self.authorization, "ApiKey {s}", .{key.encoded}) };
        ctx.headers = &self.headers;
        ctx.admin_headers = &self.headers;
        var opened = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}");
        defer opened.deinit(ctx.alloc);
        var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, opened.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        self.connection = parsed.value.connection_id[0..32].*;
    }
    fn execute(self: *@This(), ctx: *fixture.Context, sql: []const u8, expected: []const u8) !void {
        var response = try client.executeConnected(ctx, null, &self.connection, sql, if (std.mem.startsWith(u8, sql, "CREATE TABLE")) .write_admission else .never);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("server COPY sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
        const parsed = try std.json.parseFromSlice(struct { command_tag: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings(expected, parsed.value.command_tag);
    }
    fn rejected(self: *@This(), ctx: *fixture.Context, sql: []const u8, expected: []const u8) !void {
        var response = try client.executeConnected(ctx, null, &self.connection, sql, .never);
        defer response.deinit(ctx.alloc);
        const parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings(expected, parsed.value.code);
    }
    fn count(self: *@This(), ctx: *fixture.Context, expected: i64) !void {
        var response = try client.executeConnected(ctx, null, &self.connection, "SELECT count(*) FROM usage_records", .session_observation);
        defer response.deinit(ctx.alloc);
        errdefer std.debug.print("server COPY count status={d} body={s}\n", .{ response.status, response.body });
        const parsed = try std.json.parseFromSlice(struct { rows: []const []const std.json.Value }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const value = parsed.value.rows[0][0];
        try std.testing.expectEqual(expected, switch (value) {
            .integer => |number| number,
            .string => |text| try std.fmt.parseInt(i64, text, 10),
            else => return error.InvalidCopyCount,
        });
    }
    fn wireCopy(ctx: *fixture.Context, sql: []const u8, expected: []const u8, extended: bool) !void {
        return wireCopyCheck(ctx, sql, expected, extended, null, 15000);
    }
    fn wireCopyCheck(ctx: *fixture.Context, sql: []const u8, expected: []const u8, extended: bool, expected_code: ?[]const u8, timeout_ms: u32) !void {
        var input: std.Io.Writer.Allocating = .init(ctx.alloc);
        defer input.deinit();
        try frames.startup(ctx.alloc, &input.writer, "pk-rewriter", "pk-rewrite-durable-password", "default");
        const terminated = try std.fmt.allocPrint(ctx.alloc, "{s}\x00", .{sql});
        defer ctx.alloc.free(terminated);
        if (!extended) try frames.frame(&input.writer, 'Q', terminated) else {
            var body: std.Io.Writer.Allocating = .init(ctx.alloc);
            defer body.deinit();
            try body.writer.writeByte(0);
            try body.writer.writeAll(terminated);
            try body.writer.writeInt(u16, 0, .big);
            try frames.frame(&input.writer, 'P', body.written());
            try frames.frame(&input.writer, 'B', "\x00\x00\x00\x00\x00\x00\x00\x00");
            try frames.frame(&input.writer, 'E', "\x00\x00\x00\x00\x00");
            try frames.frame(&input.writer, 'S', "");
        }
        try frames.frame(&input.writer, 'X', "");
        var source = std.Io.Reader.fixed(input.written());
        var output: std.Io.Writer.Allocating = .init(ctx.alloc);
        defer output.deinit();
        var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = ctx.metadata.server.owned_public_http_server.? };
        var session: protocol.Session = .{ .alloc = ctx.alloc, .io = ctx.io, .source = adapter.backend(), .reader = &source, .writer = &output.writer, .limits = .{ .statement_timeout_ms = timeout_ms } };
        defer session.deinit();
        try session.run();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var completions: usize = 0;
        var errors: usize = 0;
        while (cursor.offset != cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const length = try cursor.int(u32);
            var body: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
            try std.testing.expect(tag != 'G' and tag != 'H' and tag != 'd');
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
                if (expected_code) |wanted| try std.testing.expectEqualStrings(wanted, code orelse return error.MissingCopyDiagnostic) else {
                    std.debug.print("server COPY wire code={s} message={s}\n", .{ code orelse "missing", message });
                    return error.UnexpectedCopyFailure;
                }
            }
            if (tag == 'C') {
                try std.testing.expectEqualStrings(expected, try body.string());
                completions += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, if (expected_code != null) 0 else 1), completions);
        try std.testing.expectEqual(@as(usize, if (expected_code != null) 1 else 0), errors);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.activate(ctx);
        var corpus = try Corpus.init(ctx.alloc);
        defer corpus.deinit();
        try self.execute(ctx, "CREATE TABLE usage_records(id TEXT PRIMARY KEY, status TEXT NOT NULL)", "CREATE TABLE");
        try self.count(ctx, 0);
        // Own the exact original path exclusively. Never replace or delete a
        // pre-existing user file. Delete only this same inode on completion.
        const owned = try std.Io.Dir.createFileAbsolute(ctx.io, path, .{ .exclusive = true, .read = true, .permissions = .fromMode(0o600) });
        defer owned.close(ctx.io);
        const inode = (try owned.stat(ctx.io)).inode;
        defer {
            if (std.Io.Dir.cwd().statFile(ctx.io, path, .{})) |stat| {
                if (stat.inode == inode) std.Io.Dir.deleteFileAbsolute(ctx.io, path) catch {};
            } else |_| {}
        }
        try owned.writePositionalAll(ctx.io, "a,queued\nb,ready\n", 0);
        try self.execute(ctx, (try corpus.get("sql-0375")).sql, "COPY");
        try self.count(ctx, 2);
        try self.execute(ctx, "DELETE FROM usage_records", "DELETE");
        try owned.setLength(ctx.io, 0);
        try owned.writePositionalAll(ctx.io, "a\tqueued\nb\tready\n", 0);
        try wireCopy(ctx, (try corpus.get("sql-0376")).sql, "COPY 2", false);
        try self.count(ctx, 2);
        try self.execute(ctx, (try corpus.get("sql-0377")).sql, "COPY");
        try wireCopy(ctx, (try corpus.get("sql-0377")).sql, "COPY 2", true);
        const exported = try std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.alloc, .limited(4096));
        defer ctx.alloc.free(exported);
        try std.testing.expect(std.mem.indexOf(u8, exported, "a,queued\n") != null);
        try std.testing.expect(std.mem.indexOf(u8, exported, "b,ready\n") != null);
        try self.execute(ctx, "COPY (SELECT 9007199254740993::bigint, 1.20::numeric, ARRAY[9007199254740993::bigint,NULL::bigint]) TO PROGRAM 'cat > /tmp/usage.csv' WITH (FORMAT csv)", "COPY");
        const precise = try std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.alloc, .limited(4096));
        defer ctx.alloc.free(precise);
        try std.testing.expectEqualStrings("9007199254740993,1.20,\"{9007199254740993,NULL}\"\n", precise);
        try self.execute(ctx, "CREATE TABLE typed_copy(id INTEGER PRIMARY KEY, status TEXT NOT NULL)", "CREATE TABLE");
        try self.execute(ctx, "COPY typed_copy FROM PROGRAM 'printf \"bad\\tqueued\\n1\\tready\\n\"' WITH (ON_ERROR ignore)", "COPY");
        try self.rejected(ctx, "COPY typed_copy FROM PROGRAM 'printf \"bad\\tqueued\\n\"'", "22P02");
        // Credential caps remain effective even when its managed user has a
        // global admin grant. No server file/program access through table caps.
        const privileged = ctx.headers;
        ctx.headers = &self.credentials.admin;
        {
            defer ctx.headers = privileged;
            var denied = try client.execute(ctx, "COPY usage_records FROM PROGRAM 'exit 99'", .never);
            defer denied.deinit(ctx.alloc);
            const diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, denied.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("42501", diagnostic.value.code);
        }
        try self.execute(ctx, "DELETE FROM usage_records", "DELETE");
        const started = @import("antfly_platform").time.monotonicNs();
        try wireCopyCheck(ctx, "COPY usage_records FROM PROGRAM 'printf producer-started > /tmp/usage.csv; printf \"cancelled\\tready\\n\"; sleep 10'", "", false, "57014", 1000);
        try std.testing.expect(@import("antfly_platform").time.monotonicNs() - started < 5 * std.time.ns_per_s);
        const progress = try std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.alloc, .limited(64));
        defer ctx.alloc.free(progress);
        try std.testing.expectEqualStrings("producer-started", progress);
        try self.count(ctx, 0);
        // More than one full typed batch is staged before the producer exits
        // unsuccessfully; all rows must roll back before reporting failure.
        try self.rejected(ctx, "COPY usage_records FROM PROGRAM 'i=0; while [ $i -lt 140 ]; do printf \"p%s\\tqueued\\n\" \"$i\"; i=$((i+1)); done; exit 7'", "38000");
        try self.count(ctx, 0);
        try self.rejected(ctx, "COPY usage_records FROM PROGRAM 'printf \"x\\tqueued\\nx\\tready\\n\"'", "23505");
        try self.count(ctx, 0);
        try self.execute(ctx, "BEGIN", "BEGIN");
        try self.execute(ctx, "SAVEPOINT child", "SAVEPOINT");
        try self.rejected(ctx, "COPY usage_records FROM PROGRAM 'printf \"x\\tqueued\\n\"; exit 7'", "38000");
        try self.rejected(ctx, "SELECT count(*) FROM usage_records", "25P02");
        try self.execute(ctx, "ROLLBACK TO child", "ROLLBACK");
        try self.count(ctx, 0);
        try self.execute(ctx, "COMMIT", "COMMIT");
        try self.execute(ctx, "COPY usage_records FROM PROGRAM 'printf \"kept\\tready\\n\"'", "COPY");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = &self.headers;
        ctx.admin_headers = &self.headers;
        try self.count(ctx, 1);
        try self.execute(ctx, "COPY (SELECT * FROM usage_records) TO PROGRAM 'cat > /dev/null'", "COPY");
        try self.rejected(ctx, "COPY usage_records TO PROGRAM 'cat > /dev/null; exit 9'", "38000");
        try self.count(ctx, 1);
    }
};

test "mounted SQL original server COPY completes files programs atomic batches credential caps and cold owners" {
    // sql-0375, sql-0376 and sql-0377 execute unchanged in beforeRestart;
    // afterRestart validates the committed postimage through cold owners.
    try fixture.run(Campaign);
}
