// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Volatile producer scheduling. Durable acceptance and receipts are owned by
//! storage; cursors and clocks select work and never discharge obligations.
const std = @import("std");
const obligations = @import("artifact_producer_obligations.zig");
const publication = @import("artifact_publication.zig");
const retry = @import("artifact_producer_retry.zig");
pub const Scheduler = struct {
    pub const poll_interval_ns = 5 * std.time.ns_per_s;
    pub const quantum_ns = 2 * std.time.ns_per_ms;
    running: std.atomic.Value(bool) = .init(false),
    pending: std.atomic.Value(bool) = .init(false),
    retry_after_ns: std.atomic.Value(u64) = .init(0),
    cursor: ?obligations.WorkCursor = null,
    producer_retry_after_ns: ?u64 = null,
    retry_round: ?struct { authority: publication.Authority, number: u64, more: bool = false } = null,

    pub const Port = struct {
        ptr: *anyopaque,
        now: *const fn (*anyopaque) u64,
        page: *const fn (*anyopaque) anyerror!bool,
    };
    pub fn advance(self: *Scheduler, port: Port) !bool {
        if (port.now(port.ptr) < self.retry_after_ns.load(.acquire)) return false;
        if (self.running.swap(true, .acq_rel)) return false;
        defer self.running.store(false, .release);
        errdefer {
            self.pending.store(false, .release);
            self.retry_after_ns.store(port.now(port.ptr) +| poll_interval_ns, .release);
        }
        self.pending.store(false, .release);
        self.retry_after_ns.store(port.now(port.ptr) +| poll_interval_ns, .release);
        return port.page(port.ptr);
    }
    pub fn active(self: *const Scheduler, now_ns: u64) bool {
        return self.pending.load(.acquire) and now_ns >= self.retry_after_ns.load(.acquire);
    }
    pub fn deinit(self: *Scheduler, alloc: std.mem.Allocator) void {
        std.debug.assert(!self.running.load(.acquire));
        self.clearCursor(alloc);
    }
    fn clearCursor(self: *Scheduler, alloc: std.mem.Allocator) void {
        if (self.cursor) |cursor| alloc.free(cursor.document);
        self.cursor = null;
    }
    pub fn cursorForAuthority(self: *Scheduler, alloc: std.mem.Allocator, authority: publication.Authority) ?obligations.WorkCursor {
        if (self.cursor) |cursor| if (!std.meta.eql(cursor.authority, authority)) self.clearCursor(alloc);
        return self.cursor;
    }
    pub fn observeAuthority(self: *Scheduler, authority: publication.Authority) void {
        if (self.retry_round) |round| if (!std.meta.eql(round.authority, authority)) {
            self.retry_round = null;
            self.producer_retry_after_ns = null;
        };
    }
    pub fn setRetryDelay(self: *Scheduler, now_ns: u64, delay_ns: u64) void {
        if (self.producer_retry_after_ns == null) self.producer_retry_after_ns = now_ns +| delay_ns;
    }
    pub fn needsRetryRound(self: *const Scheduler, now_ns: u64, has_work: bool, has_templates: bool) bool {
        return self.retry_round == null and has_work and has_templates and now_ns >= (self.producer_retry_after_ns orelse std.math.maxInt(u64));
    }
    pub fn needsRetryDelay(self: *const Scheduler) bool {
        return self.producer_retry_after_ns == null;
    }
    pub const RetryPage = struct { number: u64, first_template: u32 };
    pub fn retryPage(self: *const Scheduler, item: obligations.WorkPage.Item, has_templates: bool) ?RetryPage {
        const round = self.retry_round orelse return null;
        if (!item.dispatch_complete or !has_templates or (item.retry_round == round.number and item.retry_next_template == 0)) return null;
        return .{ .number = round.number, .first_template = if (item.retry_round == round.number) item.retry_next_template else 0 };
    }
    pub fn noteRetryProgress(self: *Scheduler, complete: bool) void {
        if (self.retry_round) |*round| round.more = round.more or !complete;
    }
    pub fn startRound(self: *Scheduler, authority: publication.Authority, number: u64) void {
        self.retry_round = .{ .authority = authority, .number = number };
    }
    pub fn commitCursor(self: *Scheduler, alloc: std.mem.Allocator, authority: publication.Authority, owned_document: []u8) void {
        self.clearCursor(alloc);
        self.cursor = .{ .authority = authority, .document = owned_document };
    }
    pub fn completePage(self: *Scheduler, alloc: std.mem.Allocator, at_end: bool, more_dispatch: bool, now_ns: u64) void {
        if (at_end) {
            self.clearCursor(alloc);
            if (self.retry_round) |*round| {
                if (round.more) round.more = false else {
                    self.retry_round = null;
                    self.producer_retry_after_ns = now_ns +| retry.interval_ns;
                }
            }
        }
        const more = self.cursor != null or more_dispatch or self.retry_round != null;
        self.pending.store(more, .release);
        if (more) self.retry_after_ns.store(0, .release);
    }
};

test "producer scheduler backs off failures and empty sweeps and prevents reentrant pages" {
    const Fixture = struct {
        scheduler: Scheduler = .{},
        now_ns: u64 = 10,
        fail: bool = true,
        calls: usize = 0,
        fn now(ptr: *anyopaque) u64 {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).now_ns;
        }
        fn page(ptr: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(!try self.scheduler.advance(self.port()));
            if (self.fail) return error.Refused;
            return false;
        }
        fn port(self: *@This()) Scheduler.Port {
            return .{ .ptr = self, .now = now, .page = page };
        }
    };
    var fixture: Fixture = .{};
    try std.testing.expectError(error.Refused, fixture.scheduler.advance(fixture.port()));
    fixture.fail = false;
    try std.testing.expect(!try fixture.scheduler.advance(fixture.port()));
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    fixture.now_ns += Scheduler.poll_interval_ns;
    try std.testing.expect(!try fixture.scheduler.advance(fixture.port()));
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expect(!fixture.scheduler.active(fixture.now_ns));
}

test "producer scheduler resumes partial retry sweeps and invalidates obsolete authority hints" {
    const alloc = std.testing.allocator;
    var scheduler: Scheduler = .{};
    defer scheduler.deinit(alloc);
    var authority: publication.Authority = undefined;
    @memset(std.mem.asBytes(&authority), 0);
    scheduler.setRetryDelay(10, 20);
    try std.testing.expect(!scheduler.needsRetryRound(29, true, true));
    try std.testing.expect(scheduler.needsRetryRound(30, true, true));
    scheduler.startRound(authority, 7);
    scheduler.retry_round.?.more = true;
    scheduler.commitCursor(alloc, authority, try alloc.dupe(u8, "last-document"));
    scheduler.completePage(alloc, false, false, 30);
    try std.testing.expect(scheduler.active(30));
    try std.testing.expectEqualStrings("last-document", scheduler.cursor.?.document);
    scheduler.completePage(alloc, true, false, 31);
    try std.testing.expect(scheduler.cursor == null);
    try std.testing.expect(scheduler.retry_round != null);
    try std.testing.expect(!scheduler.retry_round.?.more);
    scheduler.completePage(alloc, true, false, 32);
    try std.testing.expect(scheduler.retry_round == null);
    try std.testing.expect(!scheduler.pending.load(.acquire));
    scheduler.startRound(authority, 8);
    scheduler.commitCursor(alloc, authority, try alloc.dupe(u8, "obsolete"));
    var next_authority = authority;
    next_authority.namespace[0] = 1;
    try std.testing.expect(scheduler.cursorForAuthority(alloc, next_authority) == null);
    scheduler.observeAuthority(next_authority);
    try std.testing.expect(scheduler.retry_round == null);
    try std.testing.expect(scheduler.producer_retry_after_ns == null);
}
