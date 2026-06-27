// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const catalog_resources = @import("../api/catalog_resources.zig");
const sql_adapter = @import("mod.zig");

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    sessions: std.AutoHashMapUnmanaged(u64, Session) = .empty,

    const Entry = struct {
        name: []const u8,
        session: sql_adapter.OwnedSqlCatalogSession,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            alloc.free(@constCast(self.name));
            self.session.deinit(alloc);
            self.* = undefined;
        }
    };

    const Session = struct {
        entries: std.ArrayListUnmanaged(Entry) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            for (self.entries.items) |*entry| entry.deinit(alloc);
            self.entries.deinit(alloc);
            self.* = undefined;
        }

        fn truncateRetainingCapacity(self: *@This(), alloc: std.mem.Allocator, len: usize) void {
            var index = self.entries.items.len;
            while (index > len) {
                index -= 1;
                self.entries.items[index].deinit(alloc);
            }
            self.entries.shrinkRetainingCapacity(len);
        }
    };

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(self.alloc);
        self.sessions.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn apply(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.SavepointTransactionPlan,
        session: *sql_adapter.OwnedSqlCatalogSession,
    ) !void {
        if (session_id == 0) return error.InvalidSqlSession;
        switch (plan) {
            .savepoint => |savepoint_plan| try self.savepoint(session_id, savepoint_plan.savepoint_name, session),
            .rollback_to => |rollback| try self.rollbackTo(session_id, rollback.savepoint_name, session),
            .release => |release_plan| try self.release(session_id, release_plan.savepoint_name),
        }
    }

    pub fn clear(self: *@This(), session_id: u64) void {
        if (session_id == 0) return;
        if (self.sessions.fetchRemove(session_id)) |removed| {
            var value = removed.value;
            value.deinit(self.alloc);
        }
    }

    pub fn savepointCountForTest(self: *@This(), session_id: u64) usize {
        const session = self.sessions.getPtr(session_id) orelse return 0;
        return session.entries.items.len;
    }

    fn savepoint(
        self: *@This(),
        session_id: u64,
        name: []const u8,
        session: *const sql_adapter.OwnedSqlCatalogSession,
    ) !void {
        const result = try self.sessions.getOrPut(self.alloc, session_id);
        if (!result.found_existing) result.value_ptr.* = .{};
        const stack = result.value_ptr;
        if (lastIndexOfSavepoint(stack, name)) |index| {
            stack.truncateRetainingCapacity(self.alloc, index);
        }
        const owned_name = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned_name);
        var snapshot = try session.cloneAlloc(self.alloc);
        errdefer snapshot.deinit(self.alloc);
        try stack.entries.append(self.alloc, .{
            .name = owned_name,
            .session = snapshot,
        });
    }

    fn rollbackTo(
        self: *@This(),
        session_id: u64,
        name: []const u8,
        session: *sql_adapter.OwnedSqlCatalogSession,
    ) !void {
        const stack = self.sessions.getPtr(session_id) orelse return error.SavepointNotFound;
        const index = lastIndexOfSavepoint(stack, name) orelse return error.SavepointNotFound;
        const snapshot = try stack.entries.items[index].session.cloneAlloc(self.alloc);
        errdefer snapshot.deinit(self.alloc);
        const notification_session_id = session.notification_session_id;
        const request_read_only = session.request_read_only;
        session.deinit(self.alloc);
        session.* = snapshot;
        session.notification_session_id = notification_session_id;
        session.request_read_only = request_read_only;
        session.in_sql_transaction = true;
        stack.truncateRetainingCapacity(self.alloc, index + 1);
    }

    fn release(self: *@This(), session_id: u64, name: []const u8) !void {
        const stack = self.sessions.getPtr(session_id) orelse return error.SavepointNotFound;
        const index = lastIndexOfSavepoint(stack, name) orelse return error.SavepointNotFound;
        stack.truncateRetainingCapacity(self.alloc, index);
    }

    fn lastIndexOfSavepoint(session: *const Session, name: []const u8) ?usize {
        var index = session.entries.items.len;
        while (index > 0) {
            index -= 1;
            if (std.mem.eql(u8, session.entries.items[index].name, name)) return index;
        }
        return null;
    }
};

test "sql savepoint runtime restores owned session snapshots" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    const session_id: u64 = 77;
    var session = try sql_adapter.OwnedSqlCatalogSession.fromSessionAlloc(alloc, catalog_resources.SqlCatalogSession.default());
    defer session.deinit(alloc);
    session.notification_session_id = session_id;
    session.request_read_only = true;
    session.in_sql_transaction = true;

    try runtime.apply(session_id, .{ .savepoint = .{ .savepoint_name = "before_local" } }, &session);
    try std.testing.expectEqual(@as(usize, 1), runtime.savepointCountForTest(session_id));

    try session.setTransactionLocalSettingAlloc(alloc, "app.mode", "changed");
    try std.testing.expectEqual(@as(usize, 1), session.settings.len);
    try std.testing.expect(session.transaction_local_settings);

    try runtime.apply(session_id, .{ .rollback_to = .{ .savepoint_name = "before_local" } }, &session);
    try std.testing.expectEqual(session_id, session.notification_session_id);
    try std.testing.expect(session.request_read_only);
    try std.testing.expect(session.in_sql_transaction);
    try std.testing.expect(!session.transaction_local_settings);
    try std.testing.expectEqual(@as(usize, 0), session.settings.len);
    try std.testing.expectEqual(@as(usize, 1), runtime.savepointCountForTest(session_id));

    try runtime.apply(session_id, .{ .release = .{ .savepoint_name = "before_local" } }, &session);
    try std.testing.expectEqual(@as(usize, 0), runtime.savepointCountForTest(session_id));
    try std.testing.expectError(error.SavepointNotFound, runtime.apply(session_id, .{ .release = .{ .savepoint_name = "before_local" } }, &session));
}
