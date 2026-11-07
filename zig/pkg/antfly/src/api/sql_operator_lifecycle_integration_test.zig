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

//! Native operator catalog, typed row execution and cold dependency ownership.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const reference_support = @import("sql_callable_lifecycle_reference.zig");
const Reference = reference_support.Reference;
const Step = reference_support.Step;
const observe = reference_support.observe;
const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    reference: Reference,
    credentials: client.ApiKeyHeaders = .{},
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
        var usage = try users.Permission.initOwned(manager.alloc, .namespace, "*", .read);
        defer usage.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-reader", usage);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        for (self.reference.before) |step| {
            try observe(ctx, step, false);
            if (std.mem.startsWith(u8, step.sql, "CREATE TABLE operator_rows")) {
                const serving = try fixture.waitServingSchema(ctx, "operator_rows", 0);
                try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
                try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "operator_rows");
            }
        }
        for (self.reference.cold) |step| try observe(ctx, step, true);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "DROP OPERATOR ===(text,text)", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.reference.cold) |step| try observe(ctx, step, false);
        for (self.reference.after) |step| try observe(ctx, step, false);
    }
};
test "mounted SQL original operators preserve typed overloads stable function identities and cold dependency retirement" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.operator_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0133", "sql-0134", "sql-0135" };
    var checked: usize = 0;
    for ([_][]const Step{ reference.value.before, reference.value.after }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(originals[checked], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        checked += 1;
    };
    try std.testing.expectEqual(originals.len, checked);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
