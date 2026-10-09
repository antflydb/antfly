// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const httpx = @import("httpx");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const RequestCancellation = @import("antfly_local_sources").common_http_http_common.RequestCancellation;

/// Stable request authority retained through buffered HTTP output. Both server
/// protocols destroy the context after their synchronous response writer and
/// response allocation retire, including write/reset/disconnect failures.
pub const Owner = struct {
    allocator: std.mem.Allocator,
    cancellation: RequestCancellation,
    execution: scheduler.Execution = .{},
    transferred: bool = false,

    const context_key = "antfly.query-response-owner";

    pub fn create(allocator: std.mem.Allocator, original: RequestCancellation) !*Owner {
        const self = try allocator.create(Owner);
        self.* = .{ .allocator = allocator, .cancellation = original };
        return self;
    }

    pub fn releaseUnlessTransferred(self: *Owner) void {
        if (!self.transferred) destroy(self);
    }

    pub fn retainUntilContextRetires(self: *Owner, ctx: *httpx.Context) !void {
        if (self.execution.entry == null) return;
        if (self.transferred or ctx.getData(context_key) != null) return error.InvalidRunnableTransition;
        try self.execution.park(.output_blocked);
        try ctx.setData(context_key, self, destroy);
        self.transferred = true;
    }

    fn destroy(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const allocator = self.allocator;
        self.execution.finish();
        allocator.destroy(self);
    }
};

test "HTTP response owner retains original authority through context retirement" {
    const a = std.testing.allocator;
    for ([_]enum { success, cancel, expire, shutdown }{ .success, .cancel, .expire, .shutdown }) |outcome| {
        var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2 });
        var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/query");
        defer request.deinit();
        var ctx = httpx.Context.init(a, std.testing.io, &request);
        var context_live = true;
        defer if (context_live) ctx.deinit();
        const Clock = struct {
            var now_ns: u64 = 10;
            fn now() u64 {
                return now_ns;
            }
        };
        Clock.now_ns = 10;
        const owner = try Owner.create(a, .{ .execution_deadline_ns = 30 });
        var transferred = false;
        defer if (!transferred) owner.releaseUnlessTransferred();
        try owner.execution.start(&scheduler_owner, .{
            .io = std.testing.io,
            .native_now_ns = Clock.now,
            .deadline_ns = owner.cancellation.execution_deadline_ns,
            .retained_bytes = 23,
            .cancellation = .{ .ptr = &owner.cancellation, .requested = struct {
                fn requested(raw: *const anyopaque) bool {
                    return @as(*const RequestCancellation, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.requested },
        }, .query);
        try owner.execution.park(.resource_blocked);
        try owner.execution.enter(.inference);
        try owner.retainUntilContextRetires(&ctx);
        transferred = true;
        // The handler's normal scope retirement no longer owns this capsule.
        owner.releaseUnlessTransferred();
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().output_blocked);
        try std.testing.expectEqual(@as(u64, 23), scheduler_owner.stats().retained_bytes);
        try std.testing.expectEqual(scheduler.Class.inference, try owner.execution.retainedClass());
        var control: scheduler.Execution = .{};
        try control.start(&scheduler_owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
        try control.checkpoint();
        control.finish();
        const grants = scheduler_owner.stats().grants;
        switch (outcome) {
            .success => try owner.execution.checkLifetime(),
            .cancel => {
                owner.cancellation.cancel();
                try std.testing.expectError(error.Canceled, owner.execution.checkLifetime());
            },
            .expire => {
                Clock.now_ns = 31;
                try std.testing.expectError(error.DeadlineExceeded, owner.execution.checkLifetime());
            },
            .shutdown => {
                scheduler_owner.close();
                try std.testing.expectError(error.SchedulerClosed, owner.execution.checkLifetime());
            },
        }
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler_owner.stats().retained_bytes);
        try std.testing.expectEqual(grants, scheduler_owner.stats().grants);
        ctx.deinit();
        context_live = false;
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler_owner.stats().retained_bytes);
    }
}

test "HTTP response owner context allocation failure retires the untransferred root" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2 });
            defer std.debug.assert(scheduler_owner.stats().operations == 0 and scheduler_owner.stats().retained_bytes == 0);
            var request = try httpx.Request.init(std.testing.allocator, .POST, "http://localhost/db/v1/query");
            defer request.deinit();
            const owner = try Owner.create(a, .{});
            var ctx = httpx.Context.init(a, std.testing.io, &request);
            defer ctx.deinit();
            defer owner.releaseUnlessTransferred();
            try owner.execution.start(&scheduler_owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
            try owner.retainUntilContextRetires(&ctx);
            try std.testing.expectError(error.InvalidRunnableTransition, owner.retainUntilContextRetires(&ctx));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
