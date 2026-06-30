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
const ddl_plan = @import("ddl_plan.zig");

const sql_adapter = struct {
    const NotificationChannelPlan = ddl_plan.NotificationChannelPlan;
    const UnlistenNotificationPlan = ddl_plan.UnlistenNotificationPlan;
};

const SpinMutex = struct {
    inner: std.Io.Mutex = .init,

    fn lock(self: *@This()) void {
        self.inner.lockUncancelable(std.Options.debug_io);
    }

    fn unlock(self: *@This()) void {
        self.inner.unlock(std.Options.debug_io);
    }
};

pub const DeliveredEvent = struct {
    sequence: u64,
    channel_name: []u8,
    payload_json: ?[]u8 = null,
    delivered_session_ids: []u64 = &.{},
    created_at_ns: u64 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.channel_name);
        if (self.payload_json) |payload| alloc.free(payload);
        if (self.delivered_session_ids.len > 0) alloc.free(self.delivered_session_ids);
        self.* = undefined;
    }
};

const Subscription = struct {
    session_id: u64,
    channel_name: []u8,
    created_at_ns: u64,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.channel_name);
        self.* = undefined;
    }
};

pub const ApplyResult = struct {
    delivered: usize = 0,
};

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    mutex: SpinMutex = .{},
    next_session_id: u64 = 1,
    next_sequence: u64 = 1,
    subscriptions: std.ArrayListUnmanaged(Subscription) = .empty,
    events: std.ArrayListUnmanaged(DeliveredEvent) = .empty,

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.subscriptions.items) |*subscription| subscription.deinit(self.alloc);
        self.subscriptions.deinit(self.alloc);
        for (self.events.items) |*event| event.deinit(self.alloc);
        self.events.deinit(self.alloc);
    }

    pub fn allocateSessionId(self: *@This()) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const out = self.next_session_id;
        self.next_session_id += 1;
        return out;
    }

    pub fn apply(
        self: *@This(),
        plan: sql_adapter.NotificationChannelPlan,
        session_id: u64,
        timestamp_ns: u64,
    ) !ApplyResult {
        self.mutex.lock();
        defer self.mutex.unlock();
        return switch (plan) {
            .listen => |listen| try self.listenLocked(session_id, listen.channel_name, timestamp_ns),
            .notify => |notify| try self.notifyLocked(session_id, notify.channel_name, notify.payload_json, timestamp_ns),
            .unlisten => |unlisten| try self.unlistenLocked(session_id, unlisten),
        };
    }

    pub fn planAllowedInReadOnly(_: *@This(), plan: sql_adapter.NotificationChannelPlan) bool {
        return switch (plan) {
            .listen, .unlisten => true,
            .notify => false,
        };
    }

    pub fn clearSession(self: *@This(), session_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        clearSessionLocked(self, session_id);
    }

    pub fn subscriptionCountForTest(self: *@This(), session_id: u64, channel_name: []const u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        for (self.subscriptions.items) |subscription| {
            if (subscription.session_id == session_id and std.mem.eql(u8, subscription.channel_name, channel_name)) count += 1;
        }
        return count;
    }

    pub fn eventCountForTest(self: *@This()) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.events.items.len;
    }

    pub fn cloneLastEventForTest(self: *@This(), alloc: std.mem.Allocator) !?DeliveredEvent {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.events.items.len == 0) return null;
        return try cloneEventAlloc(alloc, self.events.items[self.events.items.len - 1]);
    }

    fn listenLocked(
        self: *@This(),
        session_id: u64,
        channel_name: []const u8,
        timestamp_ns: u64,
    ) !ApplyResult {
        try validateChannelName(channel_name);
        for (self.subscriptions.items) |subscription| {
            if (subscription.session_id == session_id and std.mem.eql(u8, subscription.channel_name, channel_name)) {
                return .{};
            }
        }
        try self.subscriptions.append(self.alloc, .{
            .session_id = session_id,
            .channel_name = try self.alloc.dupe(u8, channel_name),
            .created_at_ns = timestamp_ns,
        });
        return .{};
    }

    fn notifyLocked(
        self: *@This(),
        _: u64,
        channel_name: []const u8,
        payload_json: ?[]const u8,
        timestamp_ns: u64,
    ) !ApplyResult {
        try validateChannelName(channel_name);
        var delivered = std.ArrayListUnmanaged(u64).empty;
        errdefer delivered.deinit(self.alloc);
        for (self.subscriptions.items) |subscription| {
            if (std.mem.eql(u8, subscription.channel_name, channel_name)) {
                try delivered.append(self.alloc, subscription.session_id);
            }
        }
        const delivered_ids = try delivered.toOwnedSlice(self.alloc);
        errdefer if (delivered_ids.len > 0) self.alloc.free(delivered_ids);
        const owned_channel = try self.alloc.dupe(u8, channel_name);
        errdefer self.alloc.free(owned_channel);
        const owned_payload = if (payload_json) |payload| try self.alloc.dupe(u8, payload) else null;
        errdefer if (owned_payload) |payload| self.alloc.free(payload);
        const sequence = self.next_sequence;
        self.next_sequence += 1;
        try self.events.append(self.alloc, .{
            .sequence = sequence,
            .channel_name = owned_channel,
            .payload_json = owned_payload,
            .delivered_session_ids = delivered_ids,
            .created_at_ns = timestamp_ns,
        });
        return .{ .delivered = delivered_ids.len };
    }

    fn unlistenLocked(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.UnlistenNotificationPlan,
    ) !ApplyResult {
        if (plan.all) {
            clearSessionLocked(self, session_id);
            return .{};
        }
        const channel_name = plan.channel_name orelse return error.UnsupportedSqlShape;
        try validateChannelName(channel_name);
        var i: usize = 0;
        while (i < self.subscriptions.items.len) {
            if (self.subscriptions.items[i].session_id == session_id and
                std.mem.eql(u8, self.subscriptions.items[i].channel_name, channel_name))
            {
                var removed = self.subscriptions.orderedRemove(i);
                removed.deinit(self.alloc);
                continue;
            }
            i += 1;
        }
        return .{};
    }

    fn clearSessionLocked(self: *@This(), session_id: u64) void {
        var i: usize = 0;
        while (i < self.subscriptions.items.len) {
            if (self.subscriptions.items[i].session_id == session_id) {
                var removed = self.subscriptions.orderedRemove(i);
                removed.deinit(self.alloc);
                continue;
            }
            i += 1;
        }
    }
};

fn validateChannelName(channel_name: []const u8) !void {
    if (channel_name.len == 0) return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, channel_name, 0) != null) return error.UnsupportedSqlShape;
}

fn cloneEventAlloc(alloc: std.mem.Allocator, source: DeliveredEvent) !DeliveredEvent {
    const channel_name = try alloc.dupe(u8, source.channel_name);
    errdefer alloc.free(channel_name);
    const payload_json = if (source.payload_json) |payload| try alloc.dupe(u8, payload) else null;
    errdefer if (payload_json) |payload| alloc.free(payload);
    const delivered_session_ids = try alloc.dupe(u64, source.delivered_session_ids);
    errdefer if (delivered_session_ids.len > 0) alloc.free(delivered_session_ids);
    return .{
        .sequence = source.sequence,
        .channel_name = channel_name,
        .payload_json = payload_json,
        .delivered_session_ids = delivered_session_ids,
        .created_at_ns = source.created_at_ns,
    };
}

test "sql notification runtime records subscriptions and delivered events" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    const session_a = runtime.allocateSessionId();
    const session_b = runtime.allocateSessionId();
    try std.testing.expect(runtime.planAllowedInReadOnly(.{ .listen = .{ .channel_name = "usage_events" } }));
    try std.testing.expect(runtime.planAllowedInReadOnly(.{ .unlisten = .{ .channel_name = "usage_events" } }));
    try std.testing.expect(!runtime.planAllowedInReadOnly(.{ .notify = .{
        .channel_name = "usage_events",
        .payload_json = "\"updated\"",
    } }));
    _ = try runtime.apply(.{ .listen = .{ .channel_name = "usage_events" } }, session_a, 10);
    _ = try runtime.apply(.{ .listen = .{ .channel_name = "usage_events" } }, session_b, 11);
    _ = try runtime.apply(.{ .listen = .{ .channel_name = "usage_events" } }, session_b, 12);
    try std.testing.expectEqual(@as(usize, 1), runtime.subscriptionCountForTest(session_b, "usage_events"));

    const result = try runtime.apply(.{ .notify = .{
        .channel_name = "usage_events",
        .payload_json = "\"updated\"",
    } }, session_a, 20);
    try std.testing.expectEqual(@as(usize, 2), result.delivered);
    try std.testing.expectEqual(@as(usize, 1), runtime.eventCountForTest());

    var event = (try runtime.cloneLastEventForTest(alloc)) orelse return error.TestUnexpectedResult;
    defer event.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), event.sequence);
    try std.testing.expectEqualStrings("usage_events", event.channel_name);
    try std.testing.expectEqualStrings("\"updated\"", event.payload_json orelse return error.TestUnexpectedResult);
    try std.testing.expectEqual(@as(usize, 2), event.delivered_session_ids.len);

    _ = try runtime.apply(.{ .unlisten = .{ .all = true } }, session_b, 30);
    try std.testing.expectEqual(@as(usize, 0), runtime.subscriptionCountForTest(session_b, "usage_events"));
}
