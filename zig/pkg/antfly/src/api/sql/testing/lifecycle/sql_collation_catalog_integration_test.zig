// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const support = @import("../callable_lifecycle_reference.zig");
const records = local.sql_catalog.collations;

const Campaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    originals: [4][]const u8,
    bound: ?records.Bound = null,

    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
        var permission = try users.Permission.initOwned(manager.alloc, .namespace, "default.public", .admin);
        defer permission.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
    }

    fn inspect(ctx: *fixture.Context, name: []const u8) !?records.Bound {
        const server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const request: records.SnapshotRequest = .{ .database = "default", .targets = &.{.{ .table = name }} };
        const bytes = try server.source.systemCatalog(ctx.alloc, .{ .setting_admin = true }, .{ .sql_collation_snapshot = request });
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(records.Snapshot, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        try parsed.value.validate(request);
        return if (parsed.value.found) |found| found.record.bound() else null;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try support.observe(ctx, .{ .sql = self.originals[0], .command_tag = "CREATE COLLATION", .notices = &.{} }, false);
        self.bound = (try inspect(ctx, "case_insensitive")) orelse return error.MissingCollationAfterPublication;
        try support.observe(ctx, .{ .sql = self.originals[1], .command_tag = "ALTER COLLATION", .notices = &.{} }, false);
        try std.testing.expect((try inspect(ctx, "case_insensitive")) == null);
        try std.testing.expect(self.bound.?.eql((try inspect(ctx, "ci_text")).?));
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try std.testing.expect(self.bound.?.eql((try inspect(ctx, "ci_text")).?));
        try support.observe(ctx, .{ .sql = self.originals[2], .command_tag = "DROP COLLATION", .notices = &.{} }, false);
        try std.testing.expect((try inspect(ctx, "ci_text")) == null);
        try support.observe(ctx, .{ .sql = self.originals[3], .command_tag = "DROP COLLATION", .notices = &.{.{ .code = "00000", .message = "collation \"ci_text\" does not exist, skipping" }} }, false);
    }
};

test "mounted SQL ICU collation catalog originals preserve stable identity across rename and restart" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    var campaign: Campaign = .{ .originals = undefined };
    for ([_][]const u8{ "sql-0129", "sql-0130", "sql-0131", "sql-0132" }, &campaign.originals) |id, *sql| sql.* = (try corpus.get(id)).sql;
    try fixture.runWithState(Campaign, campaign);
}
