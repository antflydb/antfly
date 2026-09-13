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

//! Allocation-free intrusive admission waiters. The policy owner serializes
//! enqueue, removal, budget charging and grant under its existing queue lock.
//! Admission publication transfers lifetime back to the waiting caller: no
//! publisher may access the waiter after publish returns (or after its final
//! release store). Cancellation must rejoin that same lock before retirement.
const std = @import("std");
const time = @import("antfly_platform").time;

pub const Cancellation = struct {
    ptr: *const anyopaque,
    is_cancelled: *const fn (*const anyopaque) bool,
};

pub const Handoff = struct {
    io: ?std.Io,
    ready: std.Io.Event = .unset,
    admitted: std.atomic.Value(bool) = .init(false),

    pub fn isAdmitted(self: *const Handoff) bool {
        return self.admitted.load(.acquire);
    }

    pub fn publish(self: *Handoff) void {
        self.publishObserved(struct {
            fn before(_: @This(), _: *Handoff) void {}
            fn after(_: @This()) void {}
        }{});
    }

    // The observer is a compile-time-specialized test seam, not a runtime
    // callback or a field in production waiters. Copy it before publication.
    fn publishObserved(self: *Handoff, observer: anytype) void {
        const owned_observer = observer;
        if (self.io) |io| self.ready.set(io);
        owned_observer.before(self);
        self.admitted.store(true, .release); // FINAL access to self
        owned_observer.after();
    }

    /// A wakeup is only a hint; admission is the lifetime-transfer fence.
    /// An error does NOT retire the waiter. The caller must cancel it under
    /// the policy lock, returning any permit granted concurrently, exactly once.
    pub fn wait(self: *Handoff, cancellation: ?Cancellation) !void {
        while (!self.isAdmitted()) {
            if (cancellation) |token| if (token.is_cancelled(token.ptr)) return error.Cancelled;
            if (self.io) |io| {
                self.ready.waitTimeout(io, .{ .duration = .{
                    .raw = std.Io.Duration.fromMilliseconds(5),
                    .clock = .awake,
                } }) catch |err| switch (err) {
                    error.Timeout => {},
                    error.Canceled => return err,
                };
            } else time.yieldBriefly();
        }
    }
};

/// Queue mechanics only; byte weights, task limits and accounting stay with
/// each policy. Waiters remain stack-local and the fast path never creates one.
pub fn Fifo(comptime Payload: type) type {
    return struct {
        const Self = @This();
        pub const Waiter = struct {
            next: ?*Waiter = null,
            handoff: Handoff,
            payload: Payload,
        };
        head: ?*Waiter = null,
        tail: ?*Waiter = null,

        pub fn enqueue(self: *Self, waiter: *Waiter) void {
            std.debug.assert(waiter.next == null and !waiter.handoff.isAdmitted());
            if (self.tail) |tail| tail.next = waiter else self.head = waiter;
            self.tail = waiter;
        }

        pub fn pop(self: *Self) ?*Waiter {
            const waiter = self.head orelse return null;
            self.head = waiter.next;
            if (self.head == null) self.tail = null;
            waiter.next = null;
            return waiter;
        }

        pub fn remove(self: *Self, target: *Waiter) bool {
            var previous: ?*Waiter = null;
            var current = self.head;
            while (current) |waiter| : (current = waiter.next) {
                if (waiter == target) {
                    if (previous) |prior| prior.next = waiter.next else self.head = waiter.next;
                    if (self.tail == waiter) self.tail = previous;
                    waiter.next = null;
                    return true;
                }
                previous = waiter;
            }
            return false;
        }
    };
}

test "admission handoff signals before transfer and never touches a retired waiter" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.io();
    const Probe = struct {
        io: std.Io,
        waiter: *Handoff,
        signalled: std.Io.Event = .unset,
        allow_transfer: std.Io.Event = .unset,
        retired: std.Io.Event = .unset,
        err: ?anyerror = null,

        fn awaitEvent(self: *@This(), event: *std.Io.Event) void {
            event.waitTimeout(self.io, .{ .duration = .{
                .raw = std.Io.Duration.fromSeconds(5),
                .clock = .awake,
            } }) catch |err| {
                self.err = err;
            };
        }
        fn before(self: *@This(), _: *Handoff) void {
            self.signalled.set(self.io);
            self.awaitEvent(&self.allow_transfer);
        }
        fn after(self: *@This()) void {
            // Publisher is still executing after the caller frees the waiter.
            self.awaitEvent(&self.retired);
        }
        fn run(self: *@This()) void {
            self.waiter.publishObserved(self);
        }
    };
    for ([_]bool{ false, true }) |with_io| {
        const waiter = try std.testing.allocator.create(Handoff);
        var owned = true;
        defer if (owned) std.testing.allocator.destroy(waiter);
        waiter.* = .{ .io = if (with_io) io else null };
        var probe = Probe{ .io = io, .waiter = waiter };
        var group = std.Io.Group.init;
        defer {
            probe.allow_transfer.set(io);
            probe.retired.set(io);
            group.cancel(io);
        }
        try group.concurrent(io, Probe.run, .{&probe});
        try probe.signalled.waitTimeout(io, .{ .duration = .{
            .raw = std.Io.Duration.fromSeconds(5),
            .clock = .awake,
        } });
        try std.testing.expect(!waiter.isAdmitted());
        if (with_io) try std.testing.expect(waiter.ready.isSet());
        probe.allow_transfer.set(io);
        try waiter.wait(null);
        std.testing.allocator.destroy(waiter);
        owned = false;
        probe.retired.set(io);
        try group.await(io);
        try std.testing.expect(probe.err == null);
    }
}

test "admission FIFO detaches cancelled head middle and tail before reuse" {
    const Queue = Fifo(u64);
    var queue: Queue = .{};
    var a = Queue.Waiter{ .handoff = .{ .io = null }, .payload = 1 };
    var b = Queue.Waiter{ .handoff = .{ .io = null }, .payload = 2 };
    var c = Queue.Waiter{ .handoff = .{ .io = null }, .payload = 3 };
    queue.enqueue(&a);
    queue.enqueue(&b);
    queue.enqueue(&c);
    try std.testing.expect(queue.remove(&b));
    try std.testing.expect(b.next == null and queue.head == &a and queue.tail == &c);
    queue.enqueue(&b);
    try std.testing.expect(queue.remove(&b));
    try std.testing.expect(queue.tail == &c);
    try std.testing.expect(queue.remove(&a));
    try std.testing.expect(queue.head == &c);
    try std.testing.expect(queue.pop() == &c);
    try std.testing.expect(c.next == null and queue.head == null and queue.tail == null);
    try std.testing.expect(!queue.remove(&a));
}
