//! Coalesced, allocation-free notification with one joined std.Io consumer.
//! Producers retain the owning ResourceManager, never a DataServer callback.
const std = @import("std");

pub const Signal = struct {
    mutex: std.atomic.Mutex = .unlocked,
    epoch: std.atomic.Value(u64) = .init(0),
    ready: std.Io.Event = .unset,
    io: ?std.Io = null,

    fn lock(self: *Signal) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn bind(self: *Signal, io: std.Io) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.io != null) return error.MaintenanceConsumerAlreadyBound;
        self.io = io;
    }

    /// Join the consumer before detaching; the mutex fences producers before
    /// its runtime is destroyed. Later notifications retain only the epoch.
    pub fn unbind(self: *Signal) void {
        self.lock();
        defer self.mutex.unlock();
        self.io = null;
    }

    pub fn snapshot(self: *const Signal) u64 {
        return self.epoch.load(.acquire);
    }

    pub fn assertUnbound(self: *Signal) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.io == null);
    }

    pub fn notify(self: *Signal) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.epoch.fetchAdd(1, .release) == std.math.maxInt(u64))
            @panic("maintenance completion epoch exhausted");
        if (self.io) |io| self.ready.set(io);
    }

    /// Only the bound consumer waits/resets. Reset before rechecking the
    /// epoch, so a completion between observation and sleep cannot be lost.
    pub fn waitSince(self: *Signal, io: std.Io, observed: u64, timeout: std.Io.Timeout) std.Io.Cancelable!void {
        self.ready.reset();
        if (self.snapshot() != observed) return;
        self.ready.waitTimeout(io, timeout) catch |err| switch (err) {
            error.Timeout => {},
            error.Canceled => return error.Canceled,
        };
    }
};

test "maintenance completion survives an unbound consumer and coalesces notifications" {
    var signal: Signal = .{};
    signal.notify();
    signal.notify();
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.io();
    try signal.bind(io);
    defer signal.unbind();
    try std.testing.expectError(error.MaintenanceConsumerAlreadyBound, signal.bind(io));
    try signal.waitSince(io, 0, .none);
    try std.testing.expectEqual(@as(u64, 2), signal.snapshot());
}

test "maintenance completion wakes an Io consumer and detaches before runtime shutdown" {
    var signal: Signal = .{};
    {
        var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
        defer runtime.deinit();
        const io = runtime.io();
        try signal.bind(io);
        defer signal.unbind();
        const observed = signal.snapshot();
        const Worker = struct {
            fn run(s: *Signal) void {
                s.notify();
            }
        };
        var future = try io.concurrent(Worker.run, .{&signal});
        defer future.await(io);
        try signal.waitSince(io, observed, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        try std.testing.expectEqual(observed + 1, signal.snapshot());
    }
    // No callback or std.Io reference from the destroyed consumer survives.
    signal.notify();
    try std.testing.expectEqual(@as(u64, 2), signal.snapshot());
}
