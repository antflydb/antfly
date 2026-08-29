// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const raft_engine = @import("raft_engine");
const db_types = @import("../storage/db/types.zig");
const read_state_observer_mod = @import("state_machine/read_state_observer.zig");

/// Tracks quorum ReadIndex requests until the matching ReadState has crossed
/// this replica's state-machine apply boundary. Registration is request
/// bounded, cancellation removes ownership immediately, and context identity
/// is canonical rather than delegated to caller string conventions.
pub const AppliedReadTracker = struct {
    pub const context_prefix = "antfly-read-safe-v1:";

    pub const Token = struct {
        group_id: u64,
        request_id: u64,
    };

    pub const Registration = struct {
        token: Token,
        request_ctx: []const u8,
    };

    const Waiter = struct {
        target_index: ?u64 = null,
        complete: bool = false,
    };

    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    next_request_id: std.atomic.Value(u64) = .init(1),
    applied_indexes: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    waiters: std.AutoHashMapUnmanaged(Token, Waiter) = .empty,

    pub fn init(allocator: std.mem.Allocator) AppliedReadTracker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *AppliedReadTracker) void {
        self.applied_indexes.deinit(self.allocator);
        self.waiters.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn register(
        self: *AppliedReadTracker,
        group_id: u64,
        context_buffer: []u8,
    ) !Registration {
        var request_id = self.next_request_id.fetchAdd(1, .monotonic);
        if (request_id == 0) request_id = self.next_request_id.fetchAdd(1, .monotonic);
        const token = Token{ .group_id = group_id, .request_id = request_id };

        lock(&self.mutex);
        const result = self.waiters.getOrPut(self.allocator, token) catch |err| {
            self.mutex.unlock();
            return err;
        };
        if (result.found_existing) {
            self.mutex.unlock();
            return error.DuplicateAppliedReadToken;
        }
        result.value_ptr.* = .{};
        self.mutex.unlock();
        errdefer self.cancel(token);

        const request_ctx = std.fmt.bufPrint(
            context_buffer,
            context_prefix ++ "{x}",
            .{request_id},
        ) catch return error.ReadIndexContextTooLong;
        return .{ .token = token, .request_ctx = request_ctx };
    }

    pub fn cancel(self: *AppliedReadTracker, token: Token) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        _ = self.waiters.remove(token);
    }

    pub fn takeCompleted(self: *AppliedReadTracker, token: Token) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const waiter = self.waiters.get(token) orelse return false;
        if (!waiter.complete) return false;
        _ = self.waiters.remove(token);
        return true;
    }

    pub fn noteApplied(self: *AppliedReadTracker, group_id: u64, applied_index: u64) !void {
        if (applied_index == 0) return;
        lock(&self.mutex);
        defer self.mutex.unlock();
        const entry = try self.applied_indexes.getOrPut(self.allocator, group_id);
        if (!entry.found_existing or applied_index > entry.value_ptr.*) entry.value_ptr.* = applied_index;
        const visible_index = entry.value_ptr.*;
        var waiters = self.waiters.iterator();
        while (waiters.next()) |waiter| {
            if (waiter.key_ptr.group_id != group_id) continue;
            const target_index = waiter.value_ptr.target_index orelse continue;
            if (visible_index >= target_index) waiter.value_ptr.complete = true;
        }
    }

    pub fn observeReadStates(
        self: *AppliedReadTracker,
        group_id: u64,
        read_states: []const raft_engine.core.ReadState,
    ) void {
        if (read_states.len == 0) return;
        lock(&self.mutex);
        defer self.mutex.unlock();
        const applied_index = self.applied_indexes.get(group_id) orelse 0;
        for (read_states) |read_state| {
            if (!std.mem.startsWith(u8, read_state.request_ctx, context_prefix)) continue;
            const encoded_id = read_state.request_ctx[context_prefix.len..];
            if (encoded_id.len == 0) continue;
            const request_id = std.fmt.parseUnsigned(u64, encoded_id, 16) catch continue;
            const waiter = self.waiters.getPtr(.{
                .group_id = group_id,
                .request_id = request_id,
            }) orelse continue;
            waiter.target_index = read_state.index;
            waiter.complete = applied_index >= read_state.index;
        }
    }

    pub fn retireGroup(self: *AppliedReadTracker, group_id: u64) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        _ = self.applied_indexes.remove(group_id);
        var waiters = self.waiters.iterator();
        while (waiters.next()) |waiter| {
            if (waiter.key_ptr.group_id == group_id) self.waiters.removeByPtr(waiter.key_ptr);
        }
    }

    pub fn pendingCount(self: *AppliedReadTracker) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.waiters.count();
    }

    pub fn observer(self: *AppliedReadTracker) read_state_observer_mod.ReadStateObserver {
        return .{
            .ptr = self,
            .vtable = &.{ .on_read_states = onReadStates },
        };
    }

    fn onReadStates(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *AppliedReadTracker = @ptrCast(@alignCast(ptr));
        self.observeReadStates(group_id, read_states);
    }

    fn lock(mutex: *std.atomic.Mutex) void {
        while (!mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

pub const EnrichmentReadKind = enum {
    search,
    lookup,
    scan,
};

pub const ReadConsistency = enum {
    stale,
    leader_lease,
    read_index,
};

pub const ReadableLeaseRequester = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        request_readable_lease: *const fn (ptr: *anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void,
    };

    pub fn requestReadableLease(self: ReadableLeaseRequester, group_id: u64, request_ctx: []const u8) !void {
        try self.vtable.request_readable_lease(self.ptr, group_id, request_ctx);
    }
};

pub const ReadableLeaseCallback = *const fn (ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void;

pub const CallbackReadableLeaseRequester = struct {
    ctx: ?*anyopaque,
    callback: ReadableLeaseCallback,

    pub fn init(ctx: ?*anyopaque, callback: ReadableLeaseCallback) CallbackReadableLeaseRequester {
        return .{
            .ctx = ctx,
            .callback = callback,
        };
    }

    pub fn requester(self: *const CallbackReadableLeaseRequester) ReadableLeaseRequester {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .request_readable_lease = requestReadableLease,
            },
        };
    }

    fn requestReadableLease(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *const CallbackReadableLeaseRequester = @ptrCast(@alignCast(ptr));
        try self.callback(self.ctx, group_id, request_ctx);
    }
};

pub fn noopReadableLeaseRequester() ReadableLeaseRequester {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .request_readable_lease = requestReadableLeaseNoop,
        },
    };
}

fn requestReadableLeaseNoop(_: *anyopaque, _: u64, _: []const u8) !void {}

pub const EnrichmentReadGate = struct {
    requester: ReadableLeaseRequester,

    pub fn init(requester: ReadableLeaseRequester) EnrichmentReadGate {
        return .{ .requester = requester };
    }

    pub fn prepare(
        self: EnrichmentReadGate,
        group_id: u64,
        kind: EnrichmentReadKind,
        consistency: ReadConsistency,
    ) !void {
        if (consistency == .stale) return;

        var buf: [96]u8 = undefined;
        const request_ctx = try std.fmt.bufPrint(
            &buf,
            "enrichment:{s}:{s}",
            .{ @tagName(kind), @tagName(consistency) },
        );
        try self.requester.requestReadableLease(group_id, request_ctx);
    }

    pub fn prepareSearch(
        self: EnrichmentReadGate,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: ReadConsistency,
    ) !void {
        _ = req;
        try self.prepare(group_id, .search, consistency);
    }

    pub fn prepareLookup(
        self: EnrichmentReadGate,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: ReadConsistency,
    ) !void {
        _ = key;
        _ = opts;
        try self.prepare(group_id, .lookup, consistency);
    }

    pub fn prepareScan(
        self: EnrichmentReadGate,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: ReadConsistency,
    ) !void {
        _ = from_key;
        _ = to_key;
        _ = opts;
        try self.prepare(group_id, .scan, consistency);
    }
};

test "enrichment read gate supports explicit consistency modes" {
    const Recorder = struct {
        group_id: u64 = 0,
        request_ctx: [64]u8 = undefined,
        request_ctx_len: usize = 0,
        request_count: usize = 0,

        fn requester(self: *@This()) ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{
                    .request_readable_lease = requestReadableLease,
                },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.group_id = group_id;
            @memcpy(self.request_ctx[0..request_ctx.len], request_ctx);
            self.request_ctx_len = request_ctx.len;
            self.request_count += 1;
        }
    };

    var recorder = Recorder{};
    const gate = EnrichmentReadGate.init(recorder.requester());
    try gate.prepare(77, .search, .read_index);

    try std.testing.expectEqual(@as(u64, 77), recorder.group_id);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepare(77, .lookup, .leader_lease);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.request_ctx[0..recorder.request_ctx_len]);

    const requests_before_stale = recorder.request_count;
    try gate.prepare(77, .scan, .stale);
    try std.testing.expectEqual(requests_before_stale, recorder.request_count);

    try gate.prepareSearch(77, .{}, .read_index);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepareLookup(77, "doc:a", .{}, .leader_lease);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepareScan(77, "doc:a", "doc:z", .{}, .read_index);
    try std.testing.expectEqualStrings("enrichment:scan:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);
}

test "callback readable lease requester forwards calls" {
    const Recorder = struct {
        group_id: u64 = 0,
        request_ctx: [64]u8 = undefined,
        request_ctx_len: usize = 0,

        fn callback(ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.group_id = group_id;
            @memcpy(self.request_ctx[0..request_ctx.len], request_ctx);
            self.request_ctx_len = request_ctx.len;
        }
    };

    var recorder = Recorder{};
    const callback_requester = CallbackReadableLeaseRequester.init(&recorder, Recorder.callback);
    try callback_requester.requester().requestReadableLease(91, "enrichment:lookup");

    try std.testing.expectEqual(@as(u64, 91), recorder.group_id);
    try std.testing.expectEqualStrings("enrichment:lookup", recorder.request_ctx[0..recorder.request_ctx_len]);
}

test "applied read tracker completes only after matching ReadState and applied index" {
    var tracker = AppliedReadTracker.init(std.testing.allocator);
    defer tracker.deinit();

    var first_context: [96]u8 = undefined;
    const first = try tracker.register(7001, &first_context);
    try std.testing.expect(std.mem.startsWith(u8, first.request_ctx, AppliedReadTracker.context_prefix));
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingCount());

    try tracker.noteApplied(7001, 7);
    tracker.observeReadStates(7002, &.{.{
        .index = 7,
        .request_ctx = @constCast(first.request_ctx),
    }});
    try std.testing.expect(!tracker.takeCompleted(first.token));
    tracker.observeReadStates(7001, &.{.{
        .index = 7,
        .request_ctx = @constCast("unrelated-read-context"),
    }});
    try std.testing.expect(!tracker.takeCompleted(first.token));
    var removed_context_buffer: [96]u8 = undefined;
    const removed_context = try std.fmt.bufPrint(
        &removed_context_buffer,
        "lookup:read_index:vopr-read-v1:{x}",
        .{first.token.request_id},
    );
    tracker.observeReadStates(7001, &.{.{
        .index = 7,
        .request_ctx = @constCast(removed_context),
    }});
    try std.testing.expect(!tracker.takeCompleted(first.token));
    try tracker.observer().onReadStates(7001, &.{.{
        .index = 7,
        .request_ctx = @constCast(first.request_ctx),
    }});
    try std.testing.expect(tracker.takeCompleted(first.token));
    try std.testing.expect(!tracker.takeCompleted(first.token));

    var second_context: [96]u8 = undefined;
    const second = try tracker.register(7001, &second_context);
    tracker.observeReadStates(7001, &.{.{
        .index = 9,
        .request_ctx = @constCast(second.request_ctx),
    }});
    try std.testing.expect(!tracker.takeCompleted(second.token));
    try tracker.noteApplied(7001, 8);
    try std.testing.expect(!tracker.takeCompleted(second.token));
    try tracker.noteApplied(7001, 9);
    try std.testing.expect(tracker.takeCompleted(second.token));
}

test "applied read tracker cancellation retirement and context errors release ownership" {
    var tracker = AppliedReadTracker.init(std.testing.allocator);
    defer tracker.deinit();

    var canceled_context: [96]u8 = undefined;
    const canceled = try tracker.register(91, &canceled_context);
    tracker.cancel(canceled.token);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());

    var retired_context: [96]u8 = undefined;
    _ = try tracker.register(92, &retired_context);
    tracker.retireGroup(92);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());

    var too_short: [1]u8 = undefined;
    try std.testing.expectError(
        error.ReadIndexContextTooLong,
        tracker.register(93, &too_short),
    );
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());
}
