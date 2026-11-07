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

//! Cursor format and lifetime semantics through authenticated native owners.
const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const oracle = @import("../pgwire/cursor_reference_test_support.zig");
const protocol = @import("../pgwire/protocol.zig");

fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var response = try client.execute(ctx, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("cursor setup sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        return error.CursorSetupFailed;
    }
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    reference: oracle.Reference,
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn verify(self: *@This(), ctx: *fixture.Context) !void {
        const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        for (self.reference.profiles) |profile| {
            errdefer std.debug.print("native cursor binary={} scroll={} hold={}\n", .{ profile.binary, profile.scroll, profile.hold });
            var input = std.Io.Writer.Allocating.init(ctx.alloc);
            defer input.deinit();
            try oracle.input(ctx.alloc, &input.writer, profile, "pk-rewriter", "pk-rewrite-durable-password", "default");
            var reader = std.Io.Reader.fixed(input.written());
            var output = std.Io.Writer.Allocating.init(ctx.alloc);
            defer output.deinit();
            var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
            var session: protocol.Session = .{ .alloc = ctx.alloc, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer, .limits = .{ .result_rows = 16 } };
            defer session.deinit();
            try session.run();
            try oracle.verify(ctx.alloc, output.written(), profile);
            try ctx.checkDrivers();
        }
        for ([_]bool{ false, true }) |scroll| for ([_]bool{ false, true }) |failed| {
            var input = std.Io.Writer.Allocating.init(ctx.alloc);
            defer input.deinit();
            if (failed) try oracle.failedInput(ctx.alloc, &input.writer, scroll, "pk-rewriter", "pk-rewrite-durable-password", "default") else try oracle.suspendedInput(ctx.alloc, &input.writer, scroll, "pk-rewriter", "pk-rewrite-durable-password", "default");
            var reader = std.Io.Reader.fixed(input.written());
            var output = std.Io.Writer.Allocating.init(ctx.alloc);
            defer output.deinit();
            var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
            var session: protocol.Session = .{ .alloc = ctx.alloc, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer, .limits = .{ .result_rows = 16 } };
            defer session.deinit();
            try session.run();
            if (failed) try oracle.verifyFailed(output.written()) else try oracle.verifySuspended(output.written());
        };
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try command(ctx, "CREATE TABLE usage_records(id BIGINT PRIMARY KEY)");
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try command(ctx, "INSERT INTO usage_records(id) VALUES(0),(1),(2),(3),(4)");
        try self.verify(ctx);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try self.verify(ctx);
    }
};
test "mounted SQL original binary cursor preserves PostgreSQL scroll hold formats and cold owners" {
    const a = std.testing.allocator;
    const reference = try std.json.parseFromSlice(oracle.Reference, a, oracle.bytes, .{});
    defer reference.deinit();
    var inventory = try @import("antfly_local_sources").sql_parity_fixtures.Corpus.init(a);
    defer inventory.deinit();
    const original = try inventory.get("sql-0049");
    try std.testing.expectEqualStrings(original.sql, reference.value.original_sql);
    try std.testing.expectEqual(@as(usize, 0), original.params.len);
    var witnessed = false;
    for (reference.value.profiles) |profile| for (profile.steps) |step| {
        if (std.mem.eql(u8, step.sql, original.sql)) witnessed = true;
    };
    try std.testing.expect(witnessed);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
